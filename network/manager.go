package network

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"sync"

	"github.com/yaice-rx/yaice/config"
	"github.com/yaice-rx/yaice/core"
	"github.com/yaice-rx/yaice/logger"
	"github.com/yaice-rx/yaice/network/http_net"
	"github.com/yaice-rx/yaice/network/tcp_net"
)

const (
	ServerTcpNetworkType  = "tcp_net"
	ServerHttpNetworkType = "http_net"
)

// Manager 网络管理器
type Manager struct {
	ctx     context.Context
	cancel  context.CancelFunc // 用于优雅关闭所有子服务
	name    string
	config  *config.NetworkConfig
	servers map[string]core.Server
	clients map[string]core.Client
	mu      sync.RWMutex
	isReady bool
	packet  core.IPacket
}

// NewManager 创建网络管理器
func NewManager(ctx context.Context, cfg *config.NetworkConfig, packet core.IPacket) *Manager {
	// 创建派生 context，以便在 Manager 停止时通知所有子服务
	childCtx, cancel := context.WithCancel(ctx)
	return &Manager{
		ctx:     childCtx,
		cancel:  cancel,
		name:    "network_manager",
		config:  cfg,
		servers: make(map[string]core.Server),
		clients: make(map[string]core.Client),
		packet:  packet,
	}
}

// Start 启动网络服务
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isReady {
		return nil
	}

	var startedServers []string // 用于记录已启动的服务，以便回滚

	// 启动TCP服务器
	if m.config.TCP != nil && m.config.TCP.Enabled {
		if err := m.startTCPServerLocked(m.ctx); err != nil {
			m.rollbackServers(startedServers) // 发生错误时回滚已启动的服务
			return fmt.Errorf("failed to start TCP server: %w", err)
		}
		startedServers = append(startedServers, ServerTcpNetworkType)
	}

	// 启动HTTP服务器
	if m.config.HTTP != nil && m.config.HTTP.Enabled {
		if err := m.startHTTPServerLocked(m.ctx); err != nil {
			m.rollbackServers(startedServers) // 发生错误时回滚已启动的服务
			return fmt.Errorf("failed to start HTTP server: %w", err)
		}
		startedServers = append(startedServers, ServerHttpNetworkType)
	}

	// 检查是否至少有一个服务器启动
	if len(m.servers) == 0 {
		logger.Warn("No network servers are enabled, network manager will run without serving capabilities")
	}

	m.isReady = true
	logger.Info("Network manager started successfully",
		zap.Int("servers_count", len(m.servers)))

	return nil
}

// rollbackServers 回滚已启动的服务
func (m *Manager) rollbackServers(serverNames []string) {
	for _, name := range serverNames {
		if server, exists := m.servers[name]; exists {
			logger.Warn("Rolling back server due to startup failure", zap.String("server", name))
			// 使用 context.Background() 防止父 context 已取消导致无法清理
			if err := server.Close(context.Background()); err != nil {
				logger.Error("Failed to rollback server", zap.String("server", name), zap.Error(err))
			}
			delete(m.servers, name)
		}
	}
}

// startTCPServerLocked 启动TCP服务器 (调用时需持有 m.mu.Lock)
func (m *Manager) startTCPServerLocked(ctx context.Context) error {
	tcpCfg := m.config.TCP

	// 如果未指定包处理器，使用默认的 TCP 包处理器
	if m.packet == nil {
		m.packet = tcp_net.NewPacket()
	}

	server := tcp_net.NewServer(ctx, m.packet, m.config.TCP)
	port := server.Listen(nil)
	if port <= 0 {
		return fmt.Errorf("start port %d end port %d", m.config.TCP.StartPort, m.config.TCP.EndPort)
	}

	m.servers[ServerTcpNetworkType] = server
	m.config.TCP.Port = port // 更新配置中的实际端口号

	logger.Info("TCP server started successfully",
		zap.String("host", tcpCfg.Host),
		zap.Int("port", port),
		zap.Int("max_conn", tcpCfg.MaxConn))

	return nil
}

// startHTTPServerLocked 启动HTTP服务器 (调用时需持有 m.mu.Lock)
func (m *Manager) startHTTPServerLocked(ctx context.Context) error {
	httpCfg := m.config.HTTP
	server, err := http_net.NewHTTPServer(ctx, httpCfg)
	if err != nil {
		return err
	}
	port := server.Listen(nil)
	if port <= 0 {
		return fmt.Errorf("listen on port %d failed", httpCfg.Port)
	}

	m.servers[ServerHttpNetworkType] = server
	m.config.HTTP.Port = port // 更新配置中的实际端口号

	logger.Info("HTTP server started successfully",
		zap.String("host", httpCfg.Host),
		zap.Int("port", port),
		zap.Bool("enable_tls", httpCfg.EnableTLS))

	return nil
}

// Stop 停止网络服务
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isReady {
		return nil
	}

	// 触发 context 取消，通知所有子服务
	m.cancel()

	// 记录停止前的数量
	serverCount := len(m.servers)
	clientCount := len(m.clients)

	// 停止所有服务器
	for name, server := range m.servers {
		logger.Info("Stopping server", zap.String("name", name))
		if err := server.Close(ctx); err != nil {
			logger.Error("Failed to stop server",
				zap.String("name", name), zap.Error(err))
		}
	}

	// 断开所有客户端连接
	for name, client := range m.clients {
		logger.Info("Disconnecting client", zap.String("name", name))
		if err := client.Close(); err != nil {
			logger.Error("Failed to disconnect client",
				zap.String("name", name), zap.Error(err))
		}
	}

	// 清空资源
	m.servers = make(map[string]core.Server)
	m.clients = make(map[string]core.Client)
	m.isReady = false

	logger.Info("Network manager stopped successfully",
		zap.Int("servers_stopped", serverCount),
		zap.Int("clients_disconnected", clientCount))

	return nil
}

// GetServer 获取指定类型的服务器
func (m *Manager) GetServer(serverType string) core.Server {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.servers[serverType]
}

// GetActiveConnections 获取所有服务器的活跃连接总数
func (m *Manager) GetActiveConnections() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, server := range m.servers {
		// 安全地检查服务器是否实现了GetActiveConnections方法
		if connServer, ok := server.(interface{ GetActiveConnections() int }); ok {
			total += connServer.GetActiveConnections()
		}
	}
	return total
}

// CheckServerConnections 检查所有服务器的连接状态
func (m *Manager) CheckServerConnections() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, server := range m.servers {
		// 安全地检查服务器是否实现了CheckConns方法
		if connServer, ok := server.(interface{ CheckConns(ctx context.Context) }); ok {
			connServer.CheckConns(m.ctx)
		}
	}
}

// IsServerEnabled 检查指定类型的服务器是否启用
func (m *Manager) IsServerEnabled(serverType string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	switch serverType {
	case ServerTcpNetworkType:
		return m.config.TCP != nil && m.config.TCP.Enabled
	case ServerHttpNetworkType:
		return m.config.HTTP != nil && m.config.HTTP.Enabled
	default:
		return false
	}
}

// GetServerStatus 获取所有服务器的状态信息
func (m *Manager) GetServerStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]interface{})
	for name, server := range m.servers {
		// 使用类型断言确保方法存在，避免 panic
		serverInfo := make(map[string]interface{})

		if connServer, ok := server.(interface{ GetActiveConnections() int }); ok {
			serverInfo["active_connections"] = connServer.GetActiveConnections()
		} else {
			serverInfo["active_connections"] = -1 // 表示不支持
		}

		if healthServer, ok := server.(interface{ Health() error }); ok {
			serverInfo["health"] = healthServer.Health() == nil
		} else {
			serverInfo["health"] = false
		}

		status[name] = serverInfo
	}
	return status
}

// Health 健康检查
func (m *Manager) Health() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, server := range m.servers {
		// 使用类型断言确保方法存在
		if healthServer, ok := server.(interface{ Health() error }); ok {
			if err := healthServer.Health(); err != nil {
				return fmt.Errorf("server %s health check failed: %w", name, err)
			}
		}
	}
	return nil
}

// Name 返回管理器名称
func (m *Manager) Name() string {
	return m.name
}
