// Package ratelimit implements a per-key token-bucket rate limiter used to
// protect the write path (URL creation) from abuse. Each key (client IP)
// gets a bucket of `burst` tokens refilled at `rate` tokens/second. In a
// multi-node deployment this state would live in Redis (INCR+EXPIRE or a
// Lua token bucket); the local version has identical semantics per node.
package ratelimit

import (
	"sync"
	"time"
)

// pruneThreshold caps tracked buckets: once exceeded, idle full buckets are
// dropped so one scan of the IPv4 space cannot grow memory unboundedly.
const pruneThreshold = 16384

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter is a per-key token bucket limiter, safe for concurrent use.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens per second
	burst   float64 // bucket capacity
	now     func() time.Time
}

// New returns a Limiter allowing `burst` immediate requests per key and a
// sustained `rate` requests/second thereafter. Non-positive inputs are
// clamped to minimally permissive values.
func New(rate float64, burst int) *Limiter {
	if rate <= 0 {
		rate = 1
	}
	if burst < 1 {
		burst = 1
	}
	return &Limiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   float64(burst),
		now:     time.Now,
	}
}

// Allow reports whether key may proceed, consuming one token if so.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= pruneThreshold {
			l.pruneLocked(now)
		}
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	} else {
		b.tokens += now.Sub(b.last).Seconds() * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// pruneLocked drops buckets that have fully refilled (idle long enough to
// be indistinguishable from a fresh bucket). Callers hold l.mu.
func (l *Limiter) pruneLocked(now time.Time) {
	for k, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Seconds()*l.rate >= l.burst {
			delete(l.buckets, k)
		}
	}
}
