package core

import (
	"context"
	"github.com/yaicerx0824-glitch/yaice/cache"
	"github.com/yaicerx0824-glitch/yaice/logger"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

// GlobalMQ 全局消息队列管理器
type GlobalMQ struct {
	ctx           context.Context
	cancel        context.CancelFunc // 用于主动关闭
	globalQueue   chan Message
	logicHandlers map[int32]HandlerFunc
	eventHandlers map[int]EventHandlerFunc
	mu            sync.RWMutex
	workerCount   int32
	maxWorkers    int32
	frameRate     time.Duration // 使用 time.Duration 更清晰

	// 性能指标
	msgProcessed int64
	droppedMsgs  int64
}

var (
	globalOnce sync.Once
	gMQ        *GlobalMQ
)

// InitGlobalMQ 初始化全局消息队列（单例）
// 应在程序启动时显式调用一次
func InitGlobalMQ(ctx context.Context, frameRate int, maxWorkers int32, queueSize int) {
	globalOnce.Do(func() {
		// 衍生一个可取消的 context，用于优雅关闭
		childCtx, cancel := context.WithCancel(ctx)
		gMQ = &GlobalMQ{
			ctx:           childCtx,
			cancel:        cancel,
			globalQueue:   make(chan Message, queueSize),
			logicHandlers: make(map[int32]HandlerFunc),
			eventHandlers: make(map[int]EventHandlerFunc),
			workerCount:   0, // 初始为0，由 StartWorker 设置
			maxWorkers:    maxWorkers,
			frameRate:     time.Millisecond * time.Duration(frameRate),
		}
		logger.Info("GlobalMQ initialized", zap.Int("queue_size", queueSize), zap.Int32("max_workers", maxWorkers))
	})
}

// GetGlobalMQ 获取全局消息队列实例
func GetGlobalMQ() *GlobalMQ {
	if gMQ == nil {
		// 如果未初始化，提供默认初始化，但建议显式调用 Init
		InitGlobalMQ(context.Background(), 30, 4, 10000)
	}
	return gMQ
}

// GetGlobalMQLen 获取全局消息队列长度
func (gmq *GlobalMQ) GetGlobalMQLen() int {
	return len(gMQ.globalQueue)
}

// Stop 优雅关闭
func (gmq *GlobalMQ) Stop() {
	if gmq != nil && gmq.cancel != nil {
		gmq.cancel()
		close(gmq.globalQueue) // 关闭通道
	}
}

// StartWorker 启动全局消息处理协程
func (gmq *GlobalMQ) StartWorker(workerCount int) {
	if workerCount <= 0 {
		workerCount = 1
	}

	targetCount := int32(workerCount)
	if targetCount > gmq.maxWorkers {
		targetCount = gmq.maxWorkers
	}

	// 简单的防重复启动检查
	current := atomic.LoadInt32(&gmq.workerCount)
	if current > 0 {
		logger.Warn("Workers are already running", zap.Int32("current", current))
		return
	}

	atomic.StoreInt32(&gmq.workerCount, targetCount)

	for i := 0; i < int(targetCount); i++ {
		gmq.startFrameRateProcessor()
	}
	logger.Info("Global message queue workers started", zap.Int32("count", targetCount))
}

// SendToGlobalQueue 发送消息到全局队列
func (gmq *GlobalMQ) SendToGlobalQueue(conn Connection, msgData interface{}, serverAck int64, msgId int32, packetType int) bool {
	globalMsg := &GlobalMessage{
		MsgId:      msgId,
		Data:       msgData,
		Connection: conn,
		PacketType: packetType,
		ServerAck:  serverAck,
	}

	select {
	case gmq.globalQueue <- globalMsg:
		return true
	default:
		atomic.AddInt64(&gmq.droppedMsgs, 1)
		logger.Warn("GlobalMQ queue full, message dropped",
			zap.Int32("msg_id", msgId),
			zap.Int64("conn_id", conn.GetSessionId()))
		return false
	}
}

// RegisterEventHandlerFunc 注册事件处理器
func (gmq *GlobalMQ) RegisterEventHandlerFunc(logicEventType int, handler EventHandlerFunc) {
	gmq.mu.Lock()
	defer gmq.mu.Unlock()
	gmq.eventHandlers[logicEventType] = handler
}

// RegisterRouterHandlerFunc 注册路由消息处理器
func (gmq *GlobalMQ) RegisterRouterHandlerFunc(msgID int32, handlerFunc HandlerFunc) {
	gmq.mu.Lock()
	defer gmq.mu.Unlock()
	gmq.logicHandlers[msgID] = handlerFunc
	logger.Info("Message handler registered to GlobalMQ", zap.Int32("msg_id", msgID))
}

// startFrameRateProcessor 启动帧率控制处理器
func (gmq *GlobalMQ) startFrameRateProcessor() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Processor panic recovered",
					zap.Any("error", r),
					zap.Stack("stack"))
				// Panic 后不再重启该 Worker，避免雪崩，由监控层决定是否重启
			}
		}()

		ticker := time.NewTicker(gmq.frameRate)
		defer ticker.Stop()

		for {
			select {
			case <-gmq.ctx.Done():
				logger.Info("Processor shutting down")
				return
			case msg, ok := <-gmq.globalQueue:
				if !ok {
					logger.Info("Global queue closed")
					return
				}
				gmq.processMessage(msg)
			case <-ticker.C:
				// 固定频率的心跳或帧逻辑
				gmq.processMessage(&GlobalMessage{
					PacketType: NetworkProtocolType_FIXED_RATE,
				})
			}
		}
	}()
}

// processMessage 处理消息的核心逻辑
func (gmq *GlobalMQ) processMessage(msg Message) {
	atomic.AddInt64(&gmq.msgProcessed, 1)

	// 1. 优先查找事件处理器
	gmq.mu.RLock()
	eventHandler, okEvent := gmq.eventHandlers[msg.GetPacketType()]
	// 查找逻辑处理器需要 MsgId，这里我们假设如果注册了 PacketType 的 Handler，则优先使用
	// 如果需要兼容旧逻辑，可以在这里增加判断逻辑
	gmq.mu.RUnlock()
	if okEvent {
		if err := eventHandler(msg); err != nil {
			logger.Error("Event handler failed",
				zap.Int("logic_type", msg.GetPacketType()),
				zap.Error(err))
			return
		}
	}

	// 2. 如果没有事件处理器，尝试使用旧的逻辑处理器
	// 注意：这里需要获取 MsgId
	gmq.mu.RLock()
	logicHandler, okLogic := gmq.logicHandlers[msg.GetMsgId()]
	gmq.mu.RUnlock()

	if !okLogic {
		logger.Warn("No handler found for message",
			zap.Int32("msg_id", msg.GetMsgId()),
			zap.Int("logic_type", msg.GetPacketType()))
		return
	}

	// 执行逻辑处理
	// 优化：在锁外执行耗时操作
	conn := msg.GetConnection()
	playerGuid := conn.GetPlayerGuid()

	entity, ok := cache.GetCacheMgr().Get(playerGuid)
	if !ok {
		logger.Warn("Entity not found in cache", zap.Int64("guid", playerGuid))
		return
	}

	// 安全的类型断言
	data, ok := msg.GetData().([]byte)
	if !ok {
		logger.Error("Invalid message data type, expected []byte",
			zap.Int32("msg_id", msg.GetMsgId()))
		return
	}

	if err := logicHandler(conn, entity, data); err != nil {
		logger.Error("Logic handler failed",
			zap.Int32("msg_id", msg.GetMsgId()),
			zap.Error(err))
	}
}
