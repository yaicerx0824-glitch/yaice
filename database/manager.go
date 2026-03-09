package database

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"sync"
	"time"

	"github.com/yaice-rx/yaice/config"
	"github.com/yaice-rx/yaice/logger"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

// Manager 数据库管理器
type Manager struct {
	name        string
	config      *config.DatabaseConfig
	mongoDB     *mongo.Client
	mu          sync.RWMutex
	databaseMgr *CollectionManager
	isReady     bool
}

// NewManager 创建数据库管理器实例（不进行连接）
func NewManager(cfg *config.DatabaseConfig) *Manager {
	return &Manager{
		name:   "database_manager",
		config: cfg,
	}
}

// Init 初始化数据库连接
func (m *Manager) Init(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isReady {
		return errors.New("database manager is already initialized")
	}

	// 连接MongoDB
	if m.config.MongoDB != nil && m.config.MongoDB.Enabled {
		if err := m.connectMongoDB(ctx); err != nil {
			return fmt.Errorf("failed to connect mongodb: %w", err)
		}
	}

	m.isReady = true
	return nil
}

// Start 启动数据库管理器（启动内部组件如队列等）
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isReady {
		return errors.New("database manager is not ready, please call Init first")
	}

	if m.databaseMgr != nil {
		m.databaseMgr.Start()
	}

	logger.Info("Database manager started successfully")
	return nil
}

// Stop 停止数据库连接
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isReady {
		return nil
	}

	// 停止数据库管理器
	if m.databaseMgr != nil {
		m.databaseMgr.Shutdown()
	}

	// 断开MongoDB连接
	if m.mongoDB != nil {
		// 设置一个超时上下文，防止Disconnect长时间阻塞
		disconnectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := m.mongoDB.Disconnect(disconnectCtx); err != nil {
			logger.Error("Failed to disconnect from MongoDB", zap.Error(err))
			// 即使断开失败，也继续清理状态
		}
		m.mongoDB = nil
	}

	m.isReady = false
	m.databaseMgr = nil
	logger.Info("Database manager stopped successfully")
	return nil
}

// Health 健康检查
func (m *Manager) Health(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.isReady {
		return errors.New("database manager is not ready")
	}

	// 检查MongoDB连接
	if m.mongoDB != nil {
		// 使用传入的 ctx，支持超时控制
		if err := m.mongoDB.Ping(ctx, nil); err != nil {
			return fmt.Errorf("mongodb connection is unhealthy: %w", err)
		}
	}

	return nil
}

// Name 获取组件名称
func (m *Manager) Name() string {
	return m.name
}

// GetMongoDB 获取MongoDB客户端
func (m *Manager) GetMongoDB() *mongo.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mongoDB
}

// connectMongoDB 连接MongoDB（内部方法，调用前需持有锁）
func (m *Manager) connectMongoDB(ctx context.Context) error {
	mongoCfg := m.config.MongoDB

	// 配置连接选项
	opts := options.Client().ApplyURI(mongoCfg.URI)
	// 建议在此处添加更多连接配置，例如最大连接池大小、连接超时等
	opts.SetMaxPoolSize(100)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetWriteConcern(writeconcern.W1())
	opts.SetReadConcern(readconcern.Majority())
	opts.SetReadPreference(readpref.PrimaryPreferred())
	opts.SetRetryWrites(true)
	opts.SetRetryReads(true)
	opts.SetMinPoolSize(2)
	opts.SetMaxPoolSize(3)
	opts.SetMaxConnIdleTime(60 * time.Minute)
	client, err := mongo.Connect(opts)
	if err != nil {
		return err
	}

	// 测试连接
	if err := client.Ping(ctx, nil); err != nil {
		// 尝试断开以清理资源
		if discErr := client.Disconnect(ctx); discErr != nil {
			logger.Error("Failed to disconnect broken mongodb client", zap.Error(discErr))
		}
		return fmt.Errorf("mongodb ping failed: %w", err)
	}

	m.mongoDB = client
	logger.Info("MongoDB connected successfully",
		zap.String("database", mongoCfg.Database))

	// 初始化 CollectionManager
	m.databaseMgr = NewCollectionManager(client.Database(mongoCfg.Database), m.config.MongoDB.BatchSize, m.config.MongoDB.QueueSize)

	return nil
}

// RegisterEntity 注册实体
func (m *Manager) RegisterEntity(entity interface{}) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.databaseMgr == nil {
		return errors.New("database manager is not initialized or mongodb is disabled")
	}

	return m.databaseMgr.GetOrCreateShardEntity(entity)
}

// GetCollect 获取分片管理器
func (m *Manager) GetCollect(entity interface{}) *ShardManager {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.databaseMgr == nil {
		logger.Error("Database manager is not initialized")
		return nil
	}

	shardMgr, err := m.databaseMgr.GetShardManager(entity)
	if err != nil {
		logger.Error("GetShardManager error", zap.Error(err))
		return nil
	}
	return shardMgr
}
