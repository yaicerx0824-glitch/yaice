package tcp_net

import (
	"bytes"
	"context"
	"errors"
	"github.com/yaicerx0824-glitch/yaice/core"
	"github.com/yaicerx0824-glitch/yaice/logger"
	"github.com/yaicerx0824-glitch/yaice/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn TCP连接实现
type Conn struct {
	Type         core.ServeType
	sessionId    int64
	playerGuid   int64
	isClosed     int32
	pkg          core.IPacket
	conn         *net.TCPConn
	serve        interface{}
	data         interface{}
	sendQueue    chan interface{}
	opt          interface{}
	ctx          context.Context
	lastActivity int64
	cancel       context.CancelFunc
	logger       *logger.Logger

	// 并发控制锁
	mu sync.RWMutex

	// 消息补发队列（FIFO，用于可靠消息传输）
	// 修正：原代码注释说是栈，但UpdateServerAck逻辑是队列，这里统一为队列
	pendingMessages []interface{}
	// 已经确认收到的消息
	serverAck int64
	// 已经发送出去的消息总数
	hasBeenSentNum int64
	// 收到客户端的ACK数量
	clientAck int64
}

func (c *Conn) IncClientAck() {
	atomic.AddInt64(&c.clientAck, 1)
}

func (c *Conn) GetClientAck() int64 {
	return atomic.LoadInt64(&c.clientAck)
}

// NewConn 创建带GlobalMQ的TCP连接
func NewConn(ctx context.Context, conn *net.TCPConn, pkg core.IPacket, opt interface{}, type_ core.ServeType) core.Connection {
	// 内部创建子上下文
	connCtx, cancel := context.WithCancel(ctx)

	// 优化TCP连接参数
	_ = conn.SetNoDelay(true)
	_ = conn.SetReadBuffer(65535)
	_ = conn.SetWriteBuffer(65535)
	_ = conn.SetKeepAlive(true)
	_ = conn.SetKeepAlivePeriod(30 * time.Second)

	conn_ := &Conn{
		Type:            type_,
		sessionId:       core.GetGuidManager().GetGuid(),
		conn:            conn,
		pkg:             pkg,
		sendQueue:       make(chan interface{}, 5000),
		isClosed:        0,
		opt:             opt,
		ctx:             connCtx,
		cancel:          cancel,
		lastActivity:    time.Now().Unix(),
		pendingMessages: make([]interface{}, 0),
	}

	// 批量发送数据
	go conn_.batchWriteLoop()
	return conn_
}

// 实现core.Connection接口的方法
func (c *Conn) GetSessionId() int64 {
	return c.sessionId
}

func (c *Conn) GetRemoteAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

func (c *Conn) SetPlayerGuid(playerGuid int64) {
	c.mu.Lock()
	c.playerGuid = playerGuid
	c.mu.Unlock()
}

func (c *Conn) GetPlayerGuid() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.playerGuid
}

func (c *Conn) GetLocalAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn != nil {
		return c.conn.LocalAddr().String()
	}
	return ""
}

func (c *Conn) IsClosed() bool {
	return atomic.LoadInt32(&c.isClosed) == 1
}

// AsyncSend 异步发送数据
func (c *Conn) AsyncSend(data_ proto.Message) error {
	if c.IsClosed() {
		return errors.New("connection is closed")
	}

	msgData := core.ServerProto{}
	msgData.SetData(data_)
	// 使用原子读取确保一致性
	msgData.SetPlayerGuid(c.GetPlayerGuid())
	msgData.SetClientAck(c.GetClientAck())
	msgData.SetMessageType(3)
	msgData.SetRequestId(0)
	msgData.SetErrorCode(0)
	msgData.SetProtoType(core.ProtoTypeProto)
	msgData.SetMessageDataType(core.ProtoTypeProto)

	c.PushToPendingMessages(msgData)
	c.incHasBeenSendCount()

	// 实现发送逻辑
	select {
	case c.sendQueue <- msgData:
		return nil
	default:
		return errors.New("send queue is full")
	}
}

func (c *Conn) SyncSend(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return
	}

	if _, err := c.conn.Write(data); err != nil {
		logger.Error("TCP connection write error",
			zap.Int64("sessionId", c.sessionId),
			zap.String("remote_addr", c.GetRemoteAddr()),
			zap.Error(err))
		// 避免在持有锁时调用Close，Close内部也有锁操作，可能导致死锁
		_ = c.Close()
	}
}

func (c *Conn) Start() error {
	go c.readLoop()
	return nil
}

func (c *Conn) Close() error {
	// 使用原子操作确保只关闭一次
	if !atomic.CompareAndSwapInt32(&c.isClosed, 0, 1) {
		return nil
	}

	// 先取消上下文，通知所有协程退出
	if c.cancel != nil {
		c.cancel()
	}

	var err error
	c.mu.Lock()
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil // 防止重复使用
	}
	c.mu.Unlock()

	logger.Debug("TCP connection closed",
		zap.Int64("sessionId", c.sessionId),
		zap.Error(err))
	return err
}

func (c *Conn) GetPacket() core.IPacket {
	return c.pkg
}

func (c *Conn) GetLastActivity() int64 {
	return atomic.LoadInt64(&c.lastActivity)
}

func (c *Conn) UpdateLastActivity() {
	atomic.StoreInt64(&c.lastActivity, time.Now().Unix())
}

func (c *Conn) SetServerAck(ack int64) {
	c.mu.Lock()
	c.serverAck = ack
	c.mu.Unlock()
}

func (c *Conn) GetServerAck() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.serverAck
}

func (s *Conn) CheckServerAck(serverAck int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if serverAck > s.clientAck {
		return false
	}
	if serverAck > s.serverAck {
		var needRemoveNum = serverAck - s.serverAck
		if int64(len(s.pendingMessages)) < needRemoveNum {
			logger.Error("pending messages logic error: queue length < need remove num",
				zap.Int("pendingMessagesLen", len(s.pendingMessages)),
				zap.Int64("needRemoveNum", needRemoveNum))
			return false
		}
	}
	return true
}

func (c *Conn) UpdateServerAck(serverAck int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.hasBeenSentNum >= serverAck && serverAck > c.serverAck {
		var needRemoveNum = serverAck - c.serverAck
		// 边界检查
		if int(needRemoveNum) <= len(c.pendingMessages) {
			// 从消息队列头部移除已经同步的消息
			c.pendingMessages = c.pendingMessages[needRemoveNum:]
			c.serverAck = serverAck
		}
	}
}

func (c *Conn) PushToPendingMessages(msgData interface{}) {
	c.mu.Lock()
	c.pendingMessages = append(c.pendingMessages, msgData)
	c.mu.Unlock()
}

// ProcessPendingMessages 处理所有待补发消息
// 优化：避免阻塞，使用非阻塞发送
func (c *Conn) ProcessPendingMessages() {
	c.mu.RLock()
	// 复制一份切片以减少锁持有时间
	messages := make([]interface{}, len(c.pendingMessages))
	copy(messages, c.pendingMessages)
	c.mu.RUnlock()

	for _, msgData := range messages {
		select {
		case c.sendQueue <- msgData:
			// 发送成功
		default:
			// 队列满，丢弃或记录日志，避免阻塞
			logger.Warn("Send queue full while processing pending messages", zap.Int64("sessionId", c.sessionId))
			return
		}
	}
}

func (c *Conn) ClearPendingMessages() {
	c.mu.Lock()
	c.pendingMessages = nil
	c.mu.Unlock()
}

func (c *Conn) batchWriteLoop() {
	const batchSize = 8192
	const flushInterval = 5 * time.Millisecond

	batch := bytes.NewBuffer(make([]byte, 0, batchSize))

	// 使用 time.NewTimer 初始化
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			// 上下文取消，发送剩余数据并退出
			if batch.Len() > 0 {
				_, _ = c.conn.Write(batch.Bytes())
			}
			return

		case data, ok := <-c.sendQueue:
			if !ok {
				// 队列关闭
				if batch.Len() > 0 {
					_, _ = c.conn.Write(batch.Bytes())
				}
				return
			}

			c.UpdateLastActivity()

			var assembledData []byte
			if data_, ok_ := data.([]byte); ok_ {
				assembledData = data_
			} else if data_, ok := data.(core.ServerProto); ok {
				content := c.GetPacket().Pack(data_)
				if content != nil {
					assembledData = content
				} else {
					continue
				}
			} else {
				continue
			}

			// 检查是否需要立即发送
			if batch.Len()+len(assembledData) >= batchSize {
				if batch.Len() > 0 {
					if _, err := c.conn.Write(batch.Bytes()); err != nil {
						logger.Error("Failed to write batch data", zap.Int64("sessionId", c.sessionId), zap.Error(err))
						c.Close()
						return
					}
					batch.Reset()
				}
				// 发送新数据
				if _, err := c.conn.Write(assembledData); err != nil {
					logger.Error("Failed to write single data", zap.Int64("sessionId", c.sessionId), zap.Error(err))
					c.Close()
					return
				}
				// 重置 Timer
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(flushInterval)
			} else {
				batch.Write(assembledData)
			}

		case <-timer.C:
			// 超时，发送批量数据
			if batch.Len() > 0 {
				if _, err := c.conn.Write(batch.Bytes()); err != nil {
					logger.Error("Failed to write batch data on timeout", zap.Int64("sessionId", c.sessionId), zap.Error(err))
					c.Close()
					return
				}
				batch.Reset()
			}
			timer.Reset(flushInterval)
		}
	}
}

func (c *Conn) readLoop() {
	headSize := c.pkg.GetHeadLen()
	headData := make([]byte, headSize)
	msgBuffer := make([]byte, 0, 65536)

	// 优化：在循环外设置一次超时，或者使用更长的超时配合心跳
	// 这里保留每次设置，但建议配合应用层心跳
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if c.IsClosed() {
				return
			}
			if c.conn == nil {
				return
			}
			// 设置读取超时
			if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Minute)); err != nil {
				logger.Error("Failed to set read deadline", zap.Int64("sessionId", c.sessionId), zap.Error(err))
				return
			}
			// 读取消息头
			_, err := io.ReadFull(c.conn, headData)
			if err != nil {
				if c.handleReadError(err) != nil {
					return
				}
				continue
			}
			msgLen := utils.BytesToInt(headData)
			if msgLen <= 0 || msgLen > 1048576 {
				logger.Error("Invalid message length", zap.Int64("sessionId", c.sessionId), zap.Int32("length", msgLen))
				c.Close()
				return
			}
			// 调整消息体缓冲区大小
			if cap(msgBuffer) < int(msgLen) {
				msgBuffer = make([]byte, msgLen)
			} else {
				msgBuffer = msgBuffer[:msgLen]
			}
			// 读取消息体
			_, err = io.ReadFull(c.conn, msgBuffer)
			if err != nil {
				if c.handleReadError(err) != nil {
					return
				}
				continue
			}
			c.UpdateLastActivity()
			err, func_ := c.pkg.Unpack(c, msgBuffer)
			if err != nil {
				logger.Error("Failed to unpack message", zap.Int64("sessionId", c.sessionId), zap.Error(err))
				c.Close()
				return
			}
			if func_ != nil {
				func_(c)
			}
		}
	}
}

func (c *Conn) incHasBeenSendCount() {
	atomic.AddInt64(&c.hasBeenSentNum, 1)
}

func (c *Conn) handleReadError(err error) error {
	if errors.Is(err, io.EOF) {
		logger.Info("Connection closed by peer", zap.Int64("sessionId", c.sessionId))
		return nil
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		// 超时错误，通常可以继续等待或由心跳机制处理
		logger.Warn("Read timeout", zap.Int64("sessionId", c.sessionId))
		return nil
	} else {
		logger.Error("Error reading from connection", zap.Int64("sessionId", c.sessionId), zap.Error(err))
		c.Close()
		return err
	}
}
