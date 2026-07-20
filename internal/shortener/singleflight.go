package shortener

import (
	"sync"
	"sync/atomic"

	"github.com/systemistic/url-shortener/internal/store"
)

// flightGroup is a minimal singleflight: concurrent callers for the same
// key share one execution of fn. On a cache miss for a hot code this
// collapses N simultaneous lookups into a single store read, preventing a
// thundering herd against the database (cache stampede protection).
type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
	// waiters counts followers piggybacking on this call (observability;
	// also lets tests deterministically wait for followers to join).
	waiters atomic.Int64
	rec     store.URLRecord
	err     error
}

func newFlightGroup() *flightGroup {
	return &flightGroup{calls: make(map[string]*flightCall)}
}

// do executes fn for key, deduplicating concurrent calls. shared reports
// whether this caller piggybacked on another caller's execution.
func (g *flightGroup) do(key string, fn func() (store.URLRecord, error)) (rec store.URLRecord, err error, shared bool) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		c.waiters.Add(1)
		g.mu.Unlock()
		<-c.done
		return c.rec, c.err, true
	}
	c := &flightCall{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	c.rec, c.err = fn()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	close(c.done)
	return c.rec, c.err, false
}
