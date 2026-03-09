package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	ErrExecutorClosed = errors.New("executor is closed")
	ErrQueueFull      = errors.New("write queue is full")
)

// ShardExecutor 分片执行器
type ShardExecutor struct {
	collection *mongo.Collection
	queue      chan *WriteEvent
	wg         sync.WaitGroup
	// 使用 context 来控制协程生命周期，替代 stopCh channel
	ctx       context.Context
	cancel    context.CancelFunc
	batchSize int
}

// NewShardExecutor 创建分片执行器实例
func NewShardExecutor(
	col *mongo.Collection,
	batchSize int,
	queueSize int,
) *ShardExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	exec := &ShardExecutor{
		collection: col,
		queue:      make(chan *WriteEvent, queueSize),
		batchSize:  batchSize,
		ctx:        ctx,
		cancel:     cancel,
	}
	exec.wg.Add(1)
	go exec.loop()
	return exec
}

// Submit 提交写入事件
// 修改为非阻塞模式，并返回 error，避免死锁
func (s *ShardExecutor) Submit(evt *WriteEvent) error {
	select {
	case s.queue <- evt:
		return nil
	default:
		return ErrQueueFull
	}
}

// loop 执行循环
func (s *ShardExecutor) loop() {
	defer s.wg.Done()

	// 使用 ticker 实现超时刷新，防止数据积压太久
	ticker := time.NewTicker(500 * time.Millisecond) // 默认 500ms 刷新一次
	defer ticker.Stop()

	buffer := make([]mongo.WriteModel, 0, s.batchSize)
	events := make([]*WriteEvent, 0, s.batchSize)

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		// 传递 context，支持超时控制
		s.flush(s.ctx, buffer, events)
		buffer = buffer[:0]
		events = events[:0]
	}

	for {
		select {
		case evt, ok := <-s.queue:
			if !ok {
				// queue 已被关闭（通常不应发生，除非逻辑错误），处理剩余数据
				flush()
				return
			}
			buffer = append(buffer, evt.Model)
			events = append(events, evt)

			if len(buffer) >= s.batchSize {
				flush()
			}
		case <-ticker.C:
			// 定时刷新，降低延迟
			flush()
		case <-s.ctx.Done():
			// 收到停止信号，处理剩余事件并退出
			flush()
			return
		}
	}
}

// flush 刷新批量写入
func (s *ShardExecutor) flush(
	ctx context.Context,
	models []mongo.WriteModel,
	events []*WriteEvent,
) {
	// 使用传入的 ctx，允许外部控制超时
	_, err := s.collection.BulkWrite(ctx, models)

	// 回调处理
	for _, e := range events {
		if e.Callback != nil {
			// 使用 defer recover 防止用户回调 panic 导致整个写入循环崩溃
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("panic in write callback: %v\n", r)
					}
				}()
				e.Callback(err)
			}()
		}
	}
}

// Shutdown 关闭执行器
func (s *ShardExecutor) Shutdown() {
	// 发送停止信号
	s.cancel()
	// 不再关闭 queue，避免 Submit 向已关闭 channel 发送数据导致 panic
	// 等待 loop 结束
	s.wg.Wait()
}

// ShardManager 集合分片管理器
type ShardManager struct {
	collection     *mongo.Collection
	executors      map[int32]*ShardExecutor
	executorMutex  sync.RWMutex
	db             *mongo.Database
	collectionName string
	batchSize      int
	queueSize      int
}

// CollectionManager 集合管理器，管理所有集合的分片操作
type CollectionManager struct {
	managers      map[string]*ShardManager
	collects      map[string]Entity
	managersMutex sync.RWMutex
	db            *mongo.Database
	batchSize     int
	queueSize     int
}

// NewCollectionManager 创建集合管理器实例
func NewCollectionManager(db *mongo.Database, batchSize, queueSize int) *CollectionManager {
	return &CollectionManager{
		managers:  make(map[string]*ShardManager),
		db:        db,
		batchSize: batchSize,
		queueSize: queueSize,
		collects:  make(map[string]Entity),
	}
}

// GetOrCreateShardEntity 获取或创建集合的分片管理器
func (cm *CollectionManager) GetOrCreateShardEntity(collection interface{}) error {
	cm.managersMutex.Lock()
	defer cm.managersMutex.Unlock()

	entity, ok := collection.(Entity)
	if !ok {
		return fmt.Errorf("invalid entity type, must implement Entity interface")
	}

	name := entity.CollectionName()
	if _, exists := cm.collects[name]; exists {
		return fmt.Errorf("shard manager for collection %s already exists", name)
	}

	cm.collects[name] = entity
	return nil
}

func (cm *CollectionManager) GetShardManager(collection interface{}) (*ShardManager, error) {
	cm.managersMutex.RLock()
	defer cm.managersMutex.RUnlock()

	entity, ok := collection.(Entity)
	if !ok {
		return nil, fmt.Errorf("invalid entity type")
	}

	return cm.managers[entity.CollectionName()], nil
}

// Start 启动所有已注册的分片管理器
func (cm *CollectionManager) Start() error {
	cm.managersMutex.Lock()
	defer cm.managersMutex.Unlock()

	for name, entity := range cm.collects {
		if cm.managers[name] == nil {
			// 创建新的分片管理器
			manager := NewShardManager(cm.db, name, cm.batchSize, cm.queueSize, entity.Indexes())
			cm.managers[name] = manager
		}
	}
	return nil
}

// Shutdown 关闭所有集合的分片管理器
func (cm *CollectionManager) Shutdown() {
	cm.managersMutex.Lock()
	// 复制一份 map 的 key，避免在持有锁的情况下调用耗时操作
	managers := make([]*ShardManager, 0, len(cm.managers))
	for _, m := range cm.managers {
		managers = append(managers, m)
	}
	cm.managersMutex.Unlock()

	for _, manager := range managers {
		manager.Shutdown()
	}
}

// NewShardManager 创建分片管理器实例
func NewShardManager(
	db *mongo.Database,
	collection string,
	batchSize int,
	queueSize int,
	indexes []IndexDefinition,
) *ShardManager {
	col := db.Collection(collection)

	// 创建索引，增加错误处理
	if len(indexes) > 0 {
		if err := createIndexes(col, indexes); err != nil {
			// 实际场景中可能需要更复杂的处理，如重试或记录日志
			fmt.Printf("Warning: failed to create indexes for %s: %v\n", collection, err)
		}
	}

	return &ShardManager{
		collection:     col,
		executors:      make(map[int32]*ShardExecutor),
		db:             db,
		collectionName: collection,
		batchSize:      batchSize,
		queueSize:      queueSize,
	}
}

// createIndexes 为集合创建索引
func createIndexes(col *mongo.Collection, indexes []IndexDefinition) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 可以优化为批量创建索引
	models := make([]mongo.IndexModel, len(indexes))
	for i, idx := range indexes {
		models[i] = mongo.IndexModel{
			Keys:    idx.Keys,
			Options: idx.Options,
		}
	}

	_, err := col.Indexes().CreateMany(ctx, models)
	return err
}

// GetOrCreateExecutor 获取或创建分片执行器
// 使用 Double-Checked Locking 模式
func (m *ShardManager) GetOrCreateExecutor(shardKey int32) *ShardExecutor {
	m.executorMutex.RLock()
	exec, ok := m.executors[shardKey]
	m.executorMutex.RUnlock()

	if ok {
		return exec
	}

	m.executorMutex.Lock()
	defer m.executorMutex.Unlock()

	// 再次检查
	if exec, ok := m.executors[shardKey]; ok {
		return exec
	}

	exec = NewShardExecutor(m.collection, m.batchSize, m.queueSize)
	m.executors[shardKey] = exec
	return exec
}

// Submit 提交写入事件
func (m *ShardManager) Submit(shardKey int32, evt *WriteEvent) error {
	exec := m.GetOrCreateExecutor(shardKey)
	// 检查 error
	if err := exec.Submit(evt); err != nil {
		return fmt.Errorf("submit to shard %d failed: %w", shardKey, err)
	}
	return nil
}

// Query 同步查询方法
func (m *ShardManager) Query(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, error) {
	return m.collection.Find(ctx, filter, opts...)
}

// QueryOne 同步查询单个文档
func (m *ShardManager) QueryOne(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOneOptions]) *mongo.SingleResult {
	return m.collection.FindOne(ctx, filter, opts...)
}

// Update 同步更新方法
func (m *ShardManager) Update(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	return m.collection.UpdateOne(ctx, filter, update, opts...)
}

// Count 同步计数方法
func (m *ShardManager) Count(ctx context.Context, filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error) {
	return m.collection.CountDocuments(ctx, filter, opts...)
}

// Insert 同步插入
func (m *ShardManager) Insert(ctx context.Context, docs interface{}) (*mongo.InsertOneResult, error) {
	return m.collection.InsertOne(ctx, docs)
}

// Shutdown 关闭分片管理器
func (m *ShardManager) Shutdown() {
	m.executorMutex.Lock()
	// 复制 executors 列表以避免在锁内执行耗时操作
	executors := make([]*ShardExecutor, 0, len(m.executors))
	for _, e := range m.executors {
		executors = append(executors, e)
	}
	m.executorMutex.Unlock()

	for _, e := range executors {
		e.Shutdown()
	}
}

// GetCollection 获取集合实例
func (m *ShardManager) GetCollection() *mongo.Collection {
	return m.collection
}
