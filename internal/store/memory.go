package store

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// defaultSweepInterval is how often the background sweeper reclaims
// expired records when none is configured.
const defaultSweepInterval = time.Minute

// Memory is a concurrency-safe in-memory Store. Expiry is lazy on read;
// Run adds background reclamation so expired entries do not accumulate
// unread.
type Memory struct {
	mu   sync.Mutex
	recs map[string]*URLRecord
	now  func() time.Time

	// SweepInterval is the background sweep cadence used by Run. Set it
	// before calling Run; zero means defaultSweepInterval (1 minute).
	SweepInterval time.Duration
	// Log, when non-nil, receives a debug line per non-empty sweep.
	Log *slog.Logger
}

// NewMemory returns an empty in-memory store using the injected clock
// (nil defaults to time.Now).
func NewMemory(now func() time.Time) *Memory {
	if now == nil {
		now = time.Now
	}
	return &Memory{recs: make(map[string]*URLRecord), now: now}
}

// Create implements Store.Create as an atomic put-if-absent. An expired
// record does not block reuse of its code.
func (m *Memory) Create(_ context.Context, rec URLRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.recs[rec.Code]; ok && !cur.Expired(m.now()) {
		return ErrConflict
	}
	r := rec
	m.recs[rec.Code] = &r
	return nil
}

// Get implements Store.Get with lazy expiry: an expired record is removed
// on first read and reported as ErrNotFound. The returned record is a
// snapshot copy (Clicks included).
func (m *Memory) Get(_ context.Context, code string) (URLRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.recs[code]
	if !ok {
		return URLRecord{}, ErrNotFound
	}
	if rec.Expired(m.now()) {
		delete(m.recs, code)
		return URLRecord{}, ErrNotFound
	}
	return *rec, nil
}

// Delete implements Store.Delete.
func (m *Memory) Delete(_ context.Context, code string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.recs[code]
	if !ok {
		return ErrNotFound
	}
	expired := rec.Expired(m.now())
	delete(m.recs, code)
	if expired {
		return ErrNotFound
	}
	return nil
}

// IncrClicks implements Store.IncrClicks. Increments on expired records
// are refused the same way reads are.
func (m *Memory) IncrClicks(_ context.Context, code string, n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.recs[code]
	if !ok || rec.Expired(m.now()) {
		return ErrNotFound
	}
	rec.Clicks += n
	return nil
}

// Len returns the number of stored records, including not-yet-swept expired
// ones. Intended for tests and diagnostics.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.recs)
}

// Sweep removes every expired record and returns how many were reclaimed.
func (m *Memory) Sweep() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	n := 0
	for code, rec := range m.recs {
		if rec.Expired(now) {
			delete(m.recs, code)
			n++
		}
	}
	return n
}

// Run sweeps expired records every SweepInterval until ctx is canceled.
// Run it in its own goroutine; it returns when ctx is done.
func (m *Memory) Run(ctx context.Context) {
	interval := m.SweepInterval
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := m.Sweep(); n > 0 && m.Log != nil {
				m.Log.Debug("ttl sweep", slog.Int("expired", n))
			}
		}
	}
}
