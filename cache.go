package gocache

import (
	"context"
	"sync"
	"time"

	option "github.com/dfryer1193/gooptional"
)

type CacheKey[K any] interface {
	String() string
}

type Cache[K any, V any] interface {
	Get(context.Context, CacheKey[K]) (option.Optional[V], error)
	Set(key CacheKey[K], value V)
	Delete(key CacheKey[K])
}

var _ Cache[string, string] = (*ReadThroughCache[string, string])(nil)

type ReadThroughCache[K any, V any] struct {
	cache   map[string]V
	fetcher func(ctx context.Context, key K) (V, error)
}

func NewReadThroughCache[K any, V any](fetcher func(ctx context.Context, key K) (V, error)) *ReadThroughCache[K, V] {
	return &ReadThroughCache[K, V]{
		cache:   make(map[string]V),
		fetcher: fetcher,
	}
}

func (c *ReadThroughCache[K, V]) WithExpiry(ttl time.Duration) *ExpiringReadThroughCache[K, V] {
	return ExpiringReadThroughCacheFrom(c, ttl)
}

func (c *ReadThroughCache[K, V]) Get(ctx context.Context, key CacheKey[K]) (option.Optional[V], error) {
	value, exists := c.cache[key.String()]
	if exists {
		return option.Empty[V](), nil
	}

	value, err := c.fetcher(ctx, key.(K))
	if err != nil {
		return option.Empty[V](), err
	}

	c.cache[key.String()] = value
	return option.Of(value), nil
}

func (c *ReadThroughCache[K, V]) Set(key CacheKey[K], value V) {
	c.cache[key.String()] = value
}

func (c *ReadThroughCache[K, V]) Delete(key CacheKey[K]) {
	delete(c.cache, key.String())
}

type expiringCacheEntry[V any] struct {
	value  V
	cancel context.CancelFunc
}

type ExpiringReadThroughCache[K any, V any] struct {
	mu    sync.RWMutex
	cache map[string]expiringCacheEntry[V]

	fetcher func(ctx context.Context, key K) (V, error)
	ttl     time.Duration

	wg     sync.WaitGroup
	stopCh chan struct{}
}

func NewExpiringReadThroughCache[K any, V any](fetcher func(ctx context.Context, key K) (V, error), ttl time.Duration) *ExpiringReadThroughCache[K, V] {
	return &ExpiringReadThroughCache[K, V]{
		cache:   make(map[string]expiringCacheEntry[V]),
		fetcher: fetcher,
		ttl:     ttl,
	}
}

func ExpiringReadThroughCacheFrom[K any, V any](cache *ReadThroughCache[K, V], ttl time.Duration) *ExpiringReadThroughCache[K, V] {
	newCache := &ExpiringReadThroughCache[K, V]{
		cache:   make(map[string]expiringCacheEntry[V]),
		fetcher: cache.fetcher,
		ttl:     ttl,
	}

	for key, value := range cache.cache {
		newCache.setWithTTL(key, value)
	}

	return newCache
}

func (c *ExpiringReadThroughCache[K, V]) Get(ctx context.Context, key CacheKey[K]) (option.Optional[V], error) {
	c.mu.RLock()
	value, exists := c.cache[key.String()]
	c.mu.RUnlock()
	if exists {
		return option.Of(value.value), nil
	}

	retval, err := c.fetcher(ctx, key.(K))
	if err != nil {
		return option.Empty[V](), err
	}

	c.setWithTTL(key.String(), retval)

	return option.Of(retval), nil
}

func (c *ExpiringReadThroughCache[K, V]) Set(key CacheKey[K], value V) {
	c.setWithTTL(key.String(), value)
}

func (c *ExpiringReadThroughCache[K, V]) setWithTTL(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.cache[key]; exists {
		entry.cancel()
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.ttl)
	c.cache[key] = expiringCacheEntry[V]{
		value:  value,
		cancel: cancel,
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		<-ctx.Done()

		c.mu.Lock()
		defer c.mu.Unlock()
		if entry, exists := c.cache[key]; exists {
			entry.cancel()
			delete(c.cache, key)
		}
	}()
}

func (c *ExpiringReadThroughCache[K, V]) Delete(key CacheKey[K]) {
	c.mu.Lock()
	delete(c.cache, key.String())
	c.mu.Unlock()
}
