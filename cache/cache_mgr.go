package cache

import (
	"container/list"
	"sync"
	"time"
)

// 缓存单例
var cacheMgr *LRUCache

// FilterFunc 缓存项过滤函数类型
// 返回true表示可以剔除该元素，false表示不能剔除
type FilterFunc func(key int64, value interface{}) bool

// CacheItem 缓存项
type CacheItem struct {
	Key        int64
	Value      interface{}
	ExpireTime time.Time // 过期时间，为0表示永不过期
	Element    *list.Element
}

// LoadValueFromDbHook 从数据库加载值的钩子函数类型
type LoadValueFromDbHook func(key int64) (interface{}, time.Duration, error)

// LRUCache LRU缓存实现
type LRUCache struct {
	minSize             int // 最小缓存大小，小于此值不会剔除元素
	coreSize            int // 核心缓存大小，超过此大小且超时会剔除元素
	items               map[int64]*CacheItem
	accessOrder         *list.List
	filter              FilterFunc
	mu                  sync.RWMutex
	loadValueFromDbHook LoadValueFromDbHook // 从数据库加载值的钩子函数
}

func GetCacheMgr() *LRUCache {
	if cacheMgr == nil {
		cacheMgr = NewLRUCache(100, 1000, nil)
	}
	return cacheMgr
}

// NewLRUCache 创建LRU缓存实例
func NewLRUCache(minSize, coreSize int, filter FilterFunc) *LRUCache {
	cacheMgr = &LRUCache{
		minSize:     minSize,
		coreSize:    coreSize,
		items:       make(map[int64]*CacheItem),
		accessOrder: list.New(),
		filter:      filter,
	}
	return cacheMgr
}

// SetLoadValueFromDbHook 设置从数据库加载值的钩子函数
func (c *LRUCache) SetLoadValueFromDbHook(hook LoadValueFromDbHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadValueFromDbHook = hook
}

// Get 从缓存获取值
func (c *LRUCache) Get(key int64) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		// 缓存未命中，尝试从数据库加载
		if c.loadValueFromDbHook != nil {
			value, expire, err := c.loadValueFromDbHook(key)
			if err == nil && value != nil {
				// 从数据库加载成功，添加到缓存
				expireTime := time.Time{}
				if expire > 0 {
					expireTime = time.Now().Add(expire)
				}
				item := &CacheItem{
					Key:        key,
					Value:      value,
					ExpireTime: expireTime,
				}
				// 检查是否需要剔除元素（超过最小缓存大小）
				if len(c.items) >= c.minSize {
					// 尝试剔除最老的可剔除元素
					c.evictOldestEligible()
				}
				// 添加到访问顺序链表
				item.Element = c.accessOrder.PushFront(item)
				// 添加到映射
				c.items[key] = item

				// 检查是否超过核心缓存大小，清理过期且可剔除的元素
				if len(c.items) > c.coreSize {
					c.cleanExpiredEligible()
				}

				return value, true
			}
		}
		return nil, false
	}

	// 检查是否过期
	if !item.ExpireTime.IsZero() && time.Now().After(item.ExpireTime) {
		// 检查是否可以剔除
		if c.filter == nil || c.filter(item.Key, item.Value) {
			c.removeItem(item)
		}
		return nil, false
	}

	// 更新访问顺序（移到链表头部）
	c.accessOrder.MoveToFront(item.Element)
	return item.Value, true
}

// Put 设置缓存值
func (c *LRUCache) Put(key int64, value interface{}, expire time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查是否已存在
	if item, exists := c.items[key]; exists {
		// 更新值和过期时间
		item.Value = value
		if expire > 0 {
			item.ExpireTime = time.Now().Add(expire)
		} else {
			item.ExpireTime = time.Time{}
		}
		// 更新访问顺序
		c.accessOrder.MoveToFront(item.Element)
		return
	}

	// 检查是否需要剔除元素（超过最小缓存大小）
	if len(c.items) >= c.minSize {
		// 尝试剔除最老的可剔除元素
		c.evictOldestEligible()
	}

	// 创建新项
	expireTime := time.Time{}
	if expire > 0 {
		expireTime = time.Now().Add(expire)
	}

	item := &CacheItem{
		Key:        key,
		Value:      value,
		ExpireTime: expireTime,
	}

	// 添加到访问顺序链表
	item.Element = c.accessOrder.PushFront(item)
	// 添加到映射
	c.items[key] = item

	// 检查是否超过核心缓存大小，清理过期且可剔除的元素
	if len(c.items) > c.coreSize {
		c.cleanExpiredEligible()
	}
}

// Delete 从缓存删除值
func (c *LRUCache) Delete(key int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if item, exists := c.items[key]; exists {
		c.removeItem(item)
		return true
	}
	return false
}

// Clear 清空缓存
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[int64]*CacheItem)
	c.accessOrder.Init()
}

// Len 获取缓存项数量
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}

// 剔除最老的可剔除元素
func (c *LRUCache) evictOldestEligible() bool {
	if len(c.items) == 0 {
		return false
	}

	// 遍历链表从尾部开始（最老的元素）
	for e := c.accessOrder.Back(); e != nil; e = e.Prev() {
		item := e.Value.(*CacheItem)
		// 检查是否可以剔除
		if c.filter == nil || c.filter(item.Key, item.Value) {
			c.removeItem(item)
			return true
		}
	}

	return false
}

// 清理过期且可剔除的元素
func (c *LRUCache) cleanExpiredEligible() int {
	cleaned := 0
	now := time.Now()

	// 遍历所有项，检查是否过期且可剔除
	for key, item := range c.items {
		if !item.ExpireTime.IsZero() && now.After(item.ExpireTime) {
			// 检查是否可以剔除
			if c.filter == nil || c.filter(key, item.Value) {
				c.removeItem(item)
				cleaned++
			}
		}
	}

	return cleaned
}

// RemoveEligible 移除所有可剔除的元素
func (c *LRUCache) RemoveEligible() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	// 创建待移除项列表，避免在遍历过程中修改map
	toRemove := make([]int64, 0)

	for key, item := range c.items {
		// 检查是否可以剔除
		if c.filter == nil || c.filter(key, item.Value) {
			toRemove = append(toRemove, key)
		}
	}

	// 移除标记的项
	for _, key := range toRemove {
		if item, exists := c.items[key]; exists {
			c.removeItem(item)
			removed++
		}
	}

	return removed
}

// 移除指定项
func (c *LRUCache) removeItem(item *CacheItem) {
	// 从访问顺序链表移除
	c.accessOrder.Remove(item.Element)
	// 从映射移除
	delete(c.items, item.Key)
}

// CleanExpired 清理过期且可剔除的元素
func (c *LRUCache) CleanExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cleanExpiredEligible()
}
