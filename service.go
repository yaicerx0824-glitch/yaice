package yaice

import (
	"context"
	"errors"
	"fmt"
	"github.com/yaice-rx/yaice/cache"
	"github.com/yaice-rx/yaice/config"
	"github.com/yaice-rx/yaice/core"
	"github.com/yaice-rx/yaice/database"
	"github.com/yaice-rx/yaice/logger"
	"github.com/yaice-rx/yaice/network"
	"github.com/yaice-rx/yaice/network/tcp_net"
	"go.uber.org/zap"
	"sync"
	"time"
)

// State 服务状态枚举
type State int

const (
	StateStopped State = iota
	StateStarting
	StateRunning
	StateStopping
)

// String 获取状态字符串
func (s State) String() string {
	switch s {
	case StateStopped:
		return "Stopped"
	case StateStarting:
		return "Starting"
	case StateRunning:
		return "Running"
	case StateStopping:
		return "Stopping"
	default:
		return "Unknown"
	}
}

// LifecycleHook 定义服务生命周期钩子接口
type LifecycleHook interface {
	OnBoot()     // 启动前初始化
	OnStarting() // 启动中
	OnStarted()  // 启动完成
}

// Service 服务结构体
type Service struct {
	name       string
	version    string
	state      State
	stateMutex sync.RWMutex

	startTime time.Time
	config    *config.Config

	// 组件管理器
	networkMgr *network.Manager
	dbMgr      *database.Manager
	cacheMgr   *cache.LRUCache
	guidMgr    *core.GuidManager

	// 运行时控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 配置与钩子
	packet        core.IPacket
	lifecycleHook LifecycleHook
}

var (
	serverInstance     *Service
	serverInstanceOnce sync.Once
)

// GetService 获取服务单例
// 使用 sync.Once 确保线程安全
func GetService() *Service {
	if serverInstance == nil {
		logger.Panic("attempted to get service instance before initialization")
	}
	return serverInstance
}

// NewService 创建服务实例
func NewService(name, version string, cfg *config.Config, packet core.IPacket, hook LifecycleHook) *Service {
	serverInstanceOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		serverInstance = &Service{
			name:          name,
			version:       version,
			state:         StateStopped,
			startTime:     time.Now(), // 初始化时记录创建时间，启动时会更新
			config:        cfg,
			ctx:           ctx,
			cancel:        cancel,
			packet:        packet,
			lifecycleHook: hook,
		}
	})
	return serverInstance
}

// Start 启动服务
func (s *Service) Start() error {
	// 状态检查与锁定
	if err := s.checkAndSetState(StateStopped, StateStarting); err != nil {
		return err
	}

	logger.Info("Service starting...", zap.String("name", s.name), zap.String("version", s.version))

	// 初始化各个组件
	components, err := s.initComponents()
	if err != nil {
		// 如果初始化失败，尝试回滚已创建的资源
		s.cleanupComponents(components)
		s.setState(StateStopped)
		return fmt.Errorf("failed to initialize components: %w", err)
	}

	// 应用初始化好的组件
	s.applyComponents(components)

	// 执行启动前钩子
	if s.lifecycleHook != nil {
		s.lifecycleHook.OnBoot()
	}

	// 注册事件处理器
	if err := s.registerEventHandlers(); err != nil {
		s.cleanupComponents(components)
		s.setState(StateStopped)
		return fmt.Errorf("failed to register event handlers: %w", err)
	}

	// 启动组件
	if err := s.startComponents(); err != nil {
		s.cleanupComponents(components)
		s.setState(StateStopped)
		return fmt.Errorf("failed to start components: %w", err)
	}

	// 执行启动中钩子
	if s.lifecycleHook != nil {
		s.lifecycleHook.OnStarting()
	}

	// 启动监控
	s.startMonitoring()

	// 设置为运行状态
	s.setState(StateRunning)
	s.startTime = time.Now() // 更新为实际开始运行的时间

	// 执行启动完成钩子
	if s.lifecycleHook != nil {
		s.lifecycleHook.OnStarted()
	}

	logger.Info("Service started successfully",
		zap.String("name", s.name),
		zap.String("version", s.version),
		zap.Time("start_time", s.startTime))

	return nil
}

// Stop 停止服务
func (s *Service) Stop() error {
	if err := s.checkAndSetState(StateRunning, StateStopping); err != nil {
		return err
	}

	logger.Info("Service stopping...")

	// 1. 先通知所有组件停止（利用 context cancel）
	s.cancel()

	// 2. 停止有状态组件（网络、数据库等）
	// 注意：这里不依赖 ctx，而是显式调用 Stop 方法
	if s.networkMgr != nil {
		if err := s.networkMgr.Stop(s.ctx); err != nil {
			logger.Error("Failed to stop network manager", zap.Error(err))
		}
	}

	if s.dbMgr != nil {
		if err := s.dbMgr.Stop(s.ctx); err != nil {
			logger.Error("Failed to stop database manager", zap.Error(err))
		}
	}

	if core.GetGlobalMQ() != nil {
		core.GetGlobalMQ().Stop()
	}

	// 3. 等待所有后台 goroutine 退出
	s.wg.Wait()

	s.setState(StateStopped)
	logger.Info("Service stopped successfully")

	return nil
}

// --- 内部辅助方法 ---

// serviceComponents 用于存储初始化过程中的组件，以便在出错时回滚
type serviceComponents struct {
	guidMgr  *core.GuidManager
	cacheMgr *cache.LRUCache
	dbMgr    *database.Manager
	netMgr   *network.Manager
}

// initComponents 初始化所有核心组件
func (s *Service) initComponents() (*serviceComponents, error) {
	var comps serviceComponents
	var err error

	// 1. 初始化 GUID
	comps.guidMgr = core.NewGuidManager(s.config.GrpcGuidConfig)

	// 2. 初始化缓存
	comps.cacheMgr = cache.NewLRUCache(10, 100, nil)
	logger.Info("Cache initialized", zap.Int("min_size", 10), zap.Int("core_size", 100))

	// 3. 初始化 GlobalMQ
	if s.config.GlobalMQConfig != nil && s.config.GlobalMQConfig.Enabled {
		core.InitGlobalMQ(
			s.ctx,
			s.config.GlobalMQConfig.FrameRate,
			int32(s.config.GlobalMQConfig.MaxWorkers),
			s.config.GlobalMQConfig.QueueSize,
		)
		logger.Info("GlobalMQ initialized",
			zap.Int32("frame_rate", int32(s.config.GlobalMQConfig.FrameRate)),
			zap.Int32("max_workers", int32(s.config.GlobalMQConfig.MaxWorkers)),
			zap.Int("queue_size", s.config.GlobalMQConfig.QueueSize))
	} else {
		logger.Info("GlobalMQ is disabled")
	}

	// 4. 初始化数据库
	comps.dbMgr = database.NewManager(s.config.Database)
	comps.dbMgr.Init(s.ctx)
	// 5. 初始化网络管理器
	comps.netMgr = network.NewManager(s.ctx, s.config.Network, s.packet)

	return &comps, err
}

// applyComponents 将初始化好的组件应用到 Service 实例
func (s *Service) applyComponents(c *serviceComponents) {
	s.guidMgr = c.guidMgr
	s.cacheMgr = c.cacheMgr
	s.dbMgr = c.dbMgr
	s.networkMgr = c.netMgr
}

// cleanupComponents 清理部分初始化的组件
func (s *Service) cleanupComponents(c *serviceComponents) {
	if c.dbMgr != nil {
		_ = c.dbMgr.Stop(s.ctx)
	}
	if core.GetGlobalMQ() != nil {
		core.GetGlobalMQ().Stop()
	}
	// Cache 和 GuidManager 通常不需要显式的 Stop 清理
}

// startComponents 启动组件
func (s *Service) startComponents() error {
	// 启动 GlobalMQ
	if core.GetGlobalMQ() != nil {
		core.GetGlobalMQ().StartWorker(s.config.GlobalMQConfig.MaxWorkers)
	}

	// 启动网络管理器
	if err := s.networkMgr.Start(); err != nil {
		return fmt.Errorf("network manager start failed: %w", err)
	}
	logger.Info("Network manager started successfully")

	// 启动数据库
	if err := s.dbMgr.Start(); err != nil {
		return fmt.Errorf("database manager start failed: %w", err)
	}

	return nil
}

// registerEventHandlers 注册网络消息事件处理器
func (s *Service) registerEventHandlers() error {
	if core.GetGlobalMQ() == nil {
		return errors.New("GlobalMQ is not initialized, cannot register handlers")
	}

	// 注册 Token 检查处理器
	core.GetGlobalMQ().RegisterEventHandlerFunc(core.NetworkProtocolType_TOKEN_CHECK, s.handleTokenCheck)

	// 注册逻辑消息处理器
	core.GetGlobalMQ().RegisterEventHandlerFunc(core.NetworkProtocolType_LOGIC, s.handleLogicMessage)

	// 注册心跳处理器
	core.GetGlobalMQ().RegisterEventHandlerFunc(core.NetworkProtocolType_HEART_BEAT, s.handleHeartBeat)

	// 注册时间同步处理器
	core.GetGlobalMQ().RegisterEventHandlerFunc(core.NetworkProtocolType_SYNC_TIME, s.handleSyncTime)

	// 注册固定帧率处理器
	core.GetGlobalMQ().RegisterEventHandlerFunc(core.NetworkProtocolType_FIXED_RATE, s.handleFixedRate)

	return nil
}

// --- 事件处理逻辑 (提取为方法，提高可读性) ---

func (s *Service) handleTokenCheck(message core.Message) error {
	data, ok := message.GetData().(core.Token)
	if !ok {
		return errors.New("invalid token data type")
	}

	// 获取 TCP Server，增加类型安全检查
	serverRaw := s.GetNetworkServer(network.ServerTcpNetworkType)
	if serverRaw == nil {
		return errors.New("tcp server not found")
	}
	tcpServer, ok := serverRaw.(*tcp_net.Server)
	if !ok {
		return errors.New("invalid tcp server type")
	}

	conn := tcpServer.GetConn(data.GetGuid())
	currentConn := message.GetConnection()

	if conn != nil {
		// 已存在的连接处理
		if conn.CheckServerAck(message.GetServerAck()) {
			currentConn.UpdateServerAck(message.GetServerAck())
			conn.SetServerAck(message.GetServerAck())
			conn.ProcessPendingMessages()
		} else {
			conn.Close()
			return errors.New("server ack mismatch, connection closed")
		}
	} else {
		// 新连接处理
		tcpServer.AddConn(currentConn)
		conn = currentConn
	}

	// 回复 Token 验证成功
	replyPack := currentConn.GetPacket().ReplyTokenPack(
		currentConn.GetSessionId(),
		conn.GetClientAck(),
		core.KickOutType_Success,
		0,
	)
	conn.SyncSend(replyPack)
	return nil
}

func (s *Service) handleLogicMessage(message core.Message) error {
	conn := message.GetConnection()
	conn.IncClientAck()
	conn.UpdateServerAck(int64(message.GetServerAck()))
	return nil
}

func (s *Service) handleHeartBeat(message core.Message) error {
	conn := message.GetConnection()
	conn.UpdateLastActivity()

	pack := conn.GetPacket().HeartBeatPack(conn.GetSessionId())
	conn.SyncSend(pack)
	return nil
}

func (s *Service) handleSyncTime(message core.Message) error {
	conn := message.GetConnection()
	conn.UpdateLastActivity()

	pack := conn.GetPacket().SyncTimePack(conn.GetSessionId())
	conn.SyncSend(pack)
	return nil
}

func (s *Service) handleFixedRate(message core.Message) error {
	s.networkMgr.CheckServerConnections()
	return nil
}

// --- 监控相关 ---

func (s *Service) startMonitoring() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(10 * time.Second) // 建议调整为10秒，减少日志压力
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.monitorSystem()
			case <-s.ctx.Done():
				logger.Info("Monitoring goroutine stopped")
				return
			}
		}
	}()
}

func (s *Service) monitorSystem() {
	if core.GetGlobalMQ() != nil {
		queueLen := core.GetGlobalMQ().GetGlobalMQLen()
		logger.Debug("GlobalMQ status", zap.Int("queue_len", queueLen))
	}

	if s.cacheMgr != nil {
		cacheSize := s.cacheMgr.Len()
		expired := s.cacheMgr.CleanExpired()
		logger.Debug("Cache status", zap.Int("size", cacheSize), zap.Int("expired_cleaned", expired))
	}

	uptime := time.Since(s.startTime)
	logger.Debug("System status",
		zap.Duration("uptime", uptime),
		zap.String("state", s.State().String()))
}

// --- 状态管理辅助 ---

func (s *Service) checkAndSetState(oldState, newState State) error {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()
	if s.state != oldState {
		return fmt.Errorf("invalid state transition: expected %s, got %s", oldState, s.state)
	}
	s.state = newState
	return nil
}

func (s *Service) setState(newState State) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()
	s.state = newState
}

// --- Getter 方法 ---

func (s *Service) State() State {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()
	return s.state
}

func (s *Service) Name() string    { return s.name }
func (s *Service) Version() string { return s.version }

func (s *Service) Uptime() int64 {
	return int64(time.Since(s.startTime).Seconds())
}

func (s *Service) GetConfig() *config.Config     { return s.config }
func (s *Service) GetGuidMgr() *core.GuidManager { return s.guidMgr }
func (s *Service) GetMongoDB() *database.Manager { return s.dbMgr }
func (s *Service) GetCache() *cache.LRUCache     { return s.cacheMgr }

func (s *Service) GetNetworkServer(networkType string) interface{} {
	if s.networkMgr == nil {
		return nil
	}
	return s.networkMgr.GetServer(networkType)
}
