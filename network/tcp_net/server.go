package tcp_net

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaice-rx/yaice/config"
	"github.com/yaice-rx/yaice/core"
	"github.com/yaice-rx/yaice/logger"
	"go.uber.org/zap"
)

type Server struct {
	sync.RWMutex
	config          *config.TCPConfig
	type_           core.ServeType
	connCount       int32
	maxConnCount    int32
	listener        *net.TCPListener
	cancel          context.CancelFunc // 用于内部控制的 cancel 函数
	ctx             context.Context    // 传入的上下文
	activeConns     map[int64]core.Connection
	packet          core.IPacket
	isRunning       int32 // 使用 int32 代替 bool，支持原子操作
	isAllowConnFunc func(conn interface{}) bool
	wg              sync.WaitGroup // 用于优雅关闭，等待所有 goroutine 退出
}

func NewServer(ctx context.Context, packet core.IPacket, cfg interface{}) core.Server {
	s := &Server{
		type_:        core.Serve_Server,
		connCount:    0,
		maxConnCount: 10000,
		activeConns:  make(map[int64]core.Connection),
		ctx:          ctx,
		packet:       packet,
		config:       cfg.(*config.TCPConfig),
	}
	return s
}

func (s *Server) Listen(isAllowConnFunc func(conn interface{}) bool) int {
	// 原子操作检查状态，防止重复启动
	if !atomic.CompareAndSwapInt32(&s.isRunning, 0, 1) {
		return -1
	}

	s.isAllowConnFunc = isAllowConnFunc

	// 尝试绑定端口
	for port := s.config.StartPort; port < s.config.EndPort; port++ {
		tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			continue
		}
		listener, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			continue
		}
		s.listener = listener

		// 启动监听循环
		s.wg.Add(1)
		go s.acceptLoop()
		return port
	}

	// 没有找到可用端口，回滚状态
	atomic.StoreInt32(&s.isRunning, 0)
	return -1
}

func (s *Server) Close(ctx context.Context) error {
	// 原子操作检查状态
	if !atomic.CompareAndSwapInt32(&s.isRunning, 1, 0) {
		return errors.New("TCP Server is not running")
	}

	// 1. 关闭监听器，这会解除 Accept 的阻塞
	if s.listener != nil {
		s.listener.Close()
	}

	// 2. 关闭所有活跃连接
	// 获取所有连接的副本，避免在持有锁时调用 conn.Close() (如果 Close 内部有回调可能死锁)
	s.Lock()
	conns := make([]core.Connection, 0, len(s.activeConns))
	for _, conn := range s.activeConns {
		conns = append(conns, conn)
	}
	s.Unlock() // 尽快释放锁

	for _, conn := range conns {
		conn.Close()
	}

	// 3. 等待 acceptLoop 和所有连接处理 goroutine 退出
	// 注意：这里需要配合 context 的超时控制，防止无限等待
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.New("close server timeout")
	}
}

// CheckConns 检测玩家是否超时
// 优化：加锁保护 map 遍历
func (s *Server) CheckConns(ctx context.Context) {
	now := time.Now().Unix()
	timeout := s.config.PingTimeOut

	s.RLock()
	// 复制一份 key 列表，避免在锁内进行耗时操作或调用外部方法
	expiredGuids := make([]int64, 0)
	for guid, conn := range s.activeConns {
		if conn.GetLastActivity()+timeout < now {
			expiredGuids = append(expiredGuids, guid)
		}
	}
	s.RUnlock()

	// 在锁外关闭连接和移除 map
	for _, guid := range expiredGuids {
		if conn := s.GetConn(guid); conn != nil {
			conn.Close()
		}
		s.RemoveConn(guid)
	}
}

func (s *Server) AddConn(conn core.Connection) {
	s.Lock()
	s.activeConns[conn.GetSessionId()] = conn
	s.Unlock()
	atomic.AddInt32(&s.connCount, 1)
}

func (s *Server) ContainConn(guid int64) bool {
	s.RLock()
	_, ok := s.activeConns[guid]
	s.RUnlock()
	return ok
}

func (s *Server) GetConn(guid int64) core.Connection {
	s.RLock()
	defer s.RUnlock()
	return s.activeConns[guid]
}

func (s *Server) RemoveConn(guid int64) {
	s.Lock()
	delete(s.activeConns, guid)
	s.Unlock()
	// 防止计数器减为负数（虽然逻辑上不应发生，但防御性编程更好）
	atomic.AddInt32(&s.connCount, -1)
}

func (s *Server) GetConnCount() int32 {
	return atomic.LoadInt32(&s.connCount)
}

func (s *Server) SetMaxConnCount(count int32) {
	if count > 0 {
		s.maxConnCount = count
	}
}

func (s *Server) GetActiveConnections() int {
	return int(atomic.LoadInt32(&s.connCount))
}

func (s *Server) Health() error {
	return nil
}

// acceptLoop 接受连接的循环
func (s *Server) acceptLoop() {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			logger.Error("TCP Server acceptLoop panic recovered", zap.Any("recover", r))
		}
	}()

	for {
		// 直接阻塞 Accept，不再使用 SetDeadline 轮询
		// 当 listener.Close() 被调用时，Accept 会立即返回错误
		tcpConn, err := s.listener.AcceptTCP()
		if err != nil {
			// 如果是关闭导致的错误，正常退出
			if atomic.LoadInt32(&s.isRunning) == 0 {
				return
			}
			// 记录错误并继续（如果是临时性错误）或退出
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Error("TCP Server accept error", zap.Error(err))
			// 短暂休眠避免 CPU 飙升（如果是非关闭类严重错误）
			time.Sleep(time.Second)
			continue
		}

		// 检查连接数限制
		if atomic.LoadInt32(&s.connCount) >= s.maxConnCount {
			logger.Warn("Connection limit reached, closing new connection", zap.Int32("count", s.connCount))
			tcpConn.Close()
			continue
		}

		// 检查是否允许连接
		if s.isAllowConnFunc != nil {
			if !s.isAllowConnFunc(tcpConn) {
				tcpConn.Close()
				continue
			}
		}

		// 创建连接
		conn := NewConn(s.ctx, tcpConn, s.packet, s.config, core.Serve_Server)

		// 先添加到 map，再启动 goroutine
		s.AddConn(conn)

		// 启动连接处理
		s.wg.Add(1)
		go func(c core.Connection) {
			defer s.wg.Done()
			defer func() {
				// 连接处理结束后从活跃连接列表移除
				s.RemoveConn(c.GetSessionId())
				if r := recover(); r != nil {
					logger.Error("TCP Conn panic recovered", zap.Any("recover", r))
				}
			}()

			// 确保 Start 失败时连接被关闭
			if err := c.Start(); err != nil {
				logger.Error("Connection start failed", zap.Error(err), zap.Int64("sessionId", c.GetSessionId()))
				// Start 失败通常意味着连接已无效，确保关闭
				c.Close()
			}
		}(conn)
	}
}
