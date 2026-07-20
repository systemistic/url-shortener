// Package cache provides a small, mutex-guarded generic LRU cache. In this
// system it stands in for a Redis/Memcached tier on the redirect read path;
// the interface (Get/Put/Delete) maps 1:1 onto GET/SET/DEL.
package cache

import "sync"

// node is an entry in the intrusive doubly-linked recency list.
type node[K comparable, V any] struct {
	key        K
	value      V
	prev, next *node[K, V]
}

// LRU is a fixed-capacity least-recently-used cache, safe for concurrent
// use. Get promotes the entry to most-recently-used; Put evicts the
// least-recently-used entry when full.
type LRU[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	items    map[K]*node[K, V]
	// head/tail are sentinels: head.next is the most recently used entry,
	// tail.prev the least recently used.
	head, tail *node[K, V]
}

// NewLRU returns an LRU holding at most capacity entries. capacity must be
// positive; it panics otherwise (a misconfiguration, not a runtime error).
func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	if capacity <= 0 {
		panic("cache: LRU capacity must be positive")
	}
	h := &node[K, V]{}
	t := &node[K, V]{}
	h.next, t.prev = t, h
	return &LRU[K, V]{
		capacity: capacity,
		items:    make(map[K]*node[K, V], capacity),
		head:     h,
		tail:     t,
	}
}

// Get returns the value for key and marks it most recently used.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.moveToFront(n)
	return n.value, true
}

// Put inserts or updates key, marking it most recently used. When the cache
// is at capacity, the least recently used entry is evicted.
func (c *LRU[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.items[key]; ok {
		n.value = value
		c.moveToFront(n)
		return
	}
	if len(c.items) >= c.capacity {
		lru := c.tail.prev
		c.unlink(lru)
		delete(c.items, lru.key)
	}
	n := &node[K, V]{key: key, value: value}
	c.items[key] = n
	c.pushFront(n)
}

// Delete deletes key from the cache, reporting whether it was present.
func (c *LRU[K, V]) Delete(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.items[key]
	if !ok {
		return false
	}
	c.unlink(n)
	delete(c.items, key)
	return true
}

// Len returns the number of cached entries.
func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *LRU[K, V]) moveToFront(n *node[K, V]) {
	c.unlink(n)
	c.pushFront(n)
}

func (c *LRU[K, V]) pushFront(n *node[K, V]) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

func (c *LRU[K, V]) unlink(n *node[K, V]) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev, n.next = nil, nil
}
