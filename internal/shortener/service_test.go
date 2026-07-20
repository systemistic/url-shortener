package shortener

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/systemistic/url-shortener/internal/analytics"
	"github.com/systemistic/url-shortener/internal/base62"
	"github.com/systemistic/url-shortener/internal/store"
)

// --- fakes -----------------------------------------------------------------

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// seqIDs is a deterministic IDGen: 1, 2, 3, ...
type seqIDs struct{ n atomic.Uint64 }

func (s *seqIDs) Next() (uint64, error) { return s.n.Add(1), nil }

// failIDs always errors.
type failIDs struct{}

func (failIDs) Next() (uint64, error) { return 0, errors.New("idgen down") }

// countingStore wraps the in-memory store, counting calls and optionally
// failing the first Create calls with queued errors.
type countingStore struct {
	*store.Memory
	gets       atomic.Int64
	creates    atomic.Int64
	createErrs []error // consumed front-to-back before delegating
	mu         sync.Mutex
	getGate    chan struct{} // when non-nil, Get blocks on it
}

func (c *countingStore) Get(ctx context.Context, code string) (store.URLRecord, error) {
	c.gets.Add(1)
	if c.getGate != nil {
		<-c.getGate
	}
	return c.Memory.Get(ctx, code)
}

func (c *countingStore) Create(ctx context.Context, rec store.URLRecord) error {
	c.creates.Add(1)
	c.mu.Lock()
	if len(c.createErrs) > 0 {
		err := c.createErrs[0]
		c.createErrs = c.createErrs[1:]
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	return c.Memory.Create(ctx, rec)
}

// sinkCounts is a ClickSink capturing flushed clicks.
type sinkCounts struct {
	mu     sync.Mutex
	counts map[string]int64
}

func (s *sinkCounts) IncrClicks(_ context.Context, code string, n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts == nil {
		s.counts = make(map[string]int64)
	}
	s.counts[code] += n
	return nil
}

type env struct {
	clk *fakeClock
	st  *countingStore
	rec *analytics.Recorder
	snk *sinkCounts
	svc *Service
}

func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	clk := newFakeClock()
	st := &countingStore{Memory: store.NewMemory(clk.now)}
	snk := &sinkCounts{}
	rec := analytics.NewRecorder(snk, analytics.RecorderConfig{BufferSize: 128}, nil)
	cfg.Now = clk.now
	svc := New(st, &seqIDs{}, rec, cfg, slog.New(slog.DiscardHandler))
	return &env{clk: clk, st: st, rec: rec, snk: snk, svc: svc}
}

// drainClicks runs the recorder with a canceled ctx so it flushes exactly
// what has been buffered so far.
func (e *env) drainClicks() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e.rec.Run(ctx)
}

// --- Create ----------------------------------------------------------------

func TestCreateGenerated(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{LongURL: "https://example.com/x"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rec.Code == "" {
		t.Fatal("empty code")
	}
	if _, err := base62.Decode(rec.Code); err != nil {
		t.Errorf("code %q is not base62: %v", rec.Code, err)
	}
	if !rec.ExpiresAt.IsZero() {
		t.Errorf("ExpiresAt = %v, want zero (never)", rec.ExpiresAt)
	}
	got, err := e.st.Memory.Get(context.Background(), rec.Code)
	if err != nil || got.LongURL != "https://example.com/x" {
		t.Errorf("stored record = %+v, %v", got, err)
	}
}

func TestCreateWithTTL(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{
		LongURL: "https://example.com",
		TTL:     time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := e.clk.now().Add(time.Hour)
	if !rec.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", rec.ExpiresAt, want)
	}
}

func TestCreateCustomAlias(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{
		LongURL:     "https://example.com",
		CustomAlias: "my-link",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != "my-link" {
		t.Errorf("Code = %q, want my-link", rec.Code)
	}
}

func TestCreateCustomAliasConflict(t *testing.T) {
	e := newEnv(t, Config{})
	req := CreateReq{LongURL: "https://example.com", CustomAlias: "taken"}
	if _, err := e.svc.Create(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Create(context.Background(), req); !errors.Is(err, ErrAliasTaken) {
		t.Fatalf("second Create = %v, want ErrAliasTaken", err)
	}
}

func TestCreateValidation(t *testing.T) {
	e := newEnv(t, Config{SelfHost: "sho.rt"})
	tests := []struct {
		name    string
		req     CreateReq
		wantErr error
	}{
		{"empty url", CreateReq{}, ErrInvalidURL},
		{"bad scheme", CreateReq{LongURL: "ftp://x.com"}, ErrInvalidURL},
		{"own domain", CreateReq{LongURL: "https://sho.rt/abc"}, ErrInvalidURL},
		{"bad alias", CreateReq{LongURL: "https://x.com", CustomAlias: "a!"}, ErrInvalidAlias},
		{"reserved alias", CreateReq{LongURL: "https://x.com", CustomAlias: "healthz"}, ErrInvalidAlias},
		{"negative ttl", CreateReq{LongURL: "https://x.com", TTL: -time.Second}, ErrInvalidTTL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := e.svc.Create(context.Background(), tt.req); !errors.Is(err, tt.wantErr) {
				t.Errorf("Create error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreateRetriesOnGeneratedCollision(t *testing.T) {
	e := newEnv(t, Config{})
	e.st.createErrs = []error{store.ErrConflict, store.ErrConflict}
	rec, err := e.svc.Create(context.Background(), CreateReq{LongURL: "https://example.com"})
	if err != nil {
		t.Fatalf("Create with 2 collisions: %v", err)
	}
	if e.st.creates.Load() != 3 {
		t.Errorf("store.Create called %d times, want 3", e.st.creates.Load())
	}
	if rec.Code != base62.Encode(3) {
		t.Errorf("Code = %q, want third generated id %q", rec.Code, base62.Encode(3))
	}
}

func TestCreateGivesUpAfterRetries(t *testing.T) {
	e := newEnv(t, Config{})
	e.st.createErrs = []error{store.ErrConflict, store.ErrConflict, store.ErrConflict}
	if _, err := e.svc.Create(context.Background(), CreateReq{LongURL: "https://example.com"}); err == nil {
		t.Fatal("Create succeeded despite persistent collisions")
	}
}

func TestCreatePropagatesIDGenError(t *testing.T) {
	e := newEnv(t, Config{})
	svc := New(e.st, failIDs{}, e.rec, Config{Now: e.clk.now}, slog.New(slog.DiscardHandler))
	if _, err := svc.Create(context.Background(), CreateReq{LongURL: "https://example.com"}); err == nil {
		t.Fatal("Create succeeded with failing idgen")
	}
}

// --- Resolve ---------------------------------------------------------------

func TestResolveMissFillsCacheThenHits(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{LongURL: "https://example.com/y"})
	if err != nil {
		t.Fatal(err)
	}

	long, err := e.svc.Resolve(context.Background(), rec.Code)
	if err != nil || long != "https://example.com/y" {
		t.Fatalf("Resolve = %q, %v", long, err)
	}
	if e.st.gets.Load() != 1 {
		t.Fatalf("store.Get calls = %d, want 1", e.st.gets.Load())
	}

	// Second resolve served from cache: no extra store read.
	long, err = e.svc.Resolve(context.Background(), rec.Code)
	if err != nil || long != "https://example.com/y" {
		t.Fatalf("cached Resolve = %q, %v", long, err)
	}
	if e.st.gets.Load() != 1 {
		t.Errorf("store.Get calls = %d after cache hit, want 1", e.st.gets.Load())
	}
}

func TestResolveNegativeCache(t *testing.T) {
	e := newEnv(t, Config{NegativeTTL: 30 * time.Second})
	if _, err := e.svc.Resolve(context.Background(), "ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resolve(ghost) = %v, want ErrNotFound", err)
	}
	if e.st.gets.Load() != 1 {
		t.Fatalf("store.Get calls = %d, want 1", e.st.gets.Load())
	}
	// Repeat misses are absorbed by the negative cache.
	for range 10 {
		if _, err := e.svc.Resolve(context.Background(), "ghost"); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	}
	if e.st.gets.Load() != 1 {
		t.Errorf("store.Get calls = %d after negative-cached misses, want 1", e.st.gets.Load())
	}
	// After the negative TTL the store is consulted again.
	e.clk.advance(31 * time.Second)
	if _, err := e.svc.Resolve(context.Background(), "ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if e.st.gets.Load() != 2 {
		t.Errorf("store.Get calls = %d after negative TTL, want 2", e.st.gets.Load())
	}
}

func TestCreateClearsNegativeEntry(t *testing.T) {
	e := newEnv(t, Config{})
	if _, err := e.svc.Resolve(context.Background(), "my-page"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := e.svc.Create(context.Background(), CreateReq{
		LongURL:     "https://example.com",
		CustomAlias: "my-page",
	}); err != nil {
		t.Fatal(err)
	}
	// Immediately resolvable: the negative entry must not shadow the create.
	long, err := e.svc.Resolve(context.Background(), "my-page")
	if err != nil || long != "https://example.com" {
		t.Fatalf("Resolve after create = %q, %v", long, err)
	}
}

func TestResolveExpiredRecord(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{
		LongURL: "https://example.com",
		TTL:     time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Resolve(context.Background(), rec.Code); err != nil {
		t.Fatal(err) // cached now
	}
	e.clk.advance(2 * time.Minute)
	// Cached record is expired: cache self-invalidates, store says gone.
	if _, err := e.svc.Resolve(context.Background(), rec.Code); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resolve after expiry = %v, want ErrNotFound", err)
	}
}

func TestResolveRecordsClicks(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{LongURL: "https://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := e.svc.Resolve(context.Background(), rec.Code); err != nil {
			t.Fatal(err)
		}
	}
	// Failed resolves must not record clicks.
	e.svc.Resolve(context.Background(), "ghost")

	e.drainClicks()
	e.snk.mu.Lock()
	defer e.snk.mu.Unlock()
	if e.snk.counts[rec.Code] != 3 {
		t.Errorf("clicks flushed = %d, want 3", e.snk.counts[rec.Code])
	}
	if e.snk.counts["ghost"] != 0 {
		t.Errorf("ghost clicks = %d, want 0", e.snk.counts["ghost"])
	}
}

func TestResolveSingleflightCollapsesStampede(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{LongURL: "https://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	e.st.getGate = gate // every store.Get blocks until the gate opens

	const followers = 15
	var wg sync.WaitGroup
	start := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if long, err := e.svc.Resolve(context.Background(), rec.Code); err != nil || long != "https://example.com" {
				t.Errorf("Resolve = %q, %v", long, err)
			}
		}()
	}
	start() // leader enters the store read and blocks on the gate
	deadline := time.Now().Add(5 * time.Second)
	for e.st.gets.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("leader never reached the store")
		}
		runtime.Gosched()
	}
	e.svc.flight.mu.Lock()
	call := e.svc.flight.calls[rec.Code]
	e.svc.flight.mu.Unlock()
	if call == nil {
		t.Fatal("no in-flight call registered")
	}
	for range followers {
		start()
	}
	for call.waiters.Load() != followers {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d followers joined the flight", call.waiters.Load(), followers)
		}
		runtime.Gosched()
	}
	close(gate)
	wg.Wait()
	if got := e.st.gets.Load(); got != 1 {
		t.Errorf("store.Get calls = %d for %d concurrent resolves, want 1", got, followers+1)
	}
}

// --- Stats / Delete ----------------------------------------------------------

func TestStats(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{
		LongURL: "https://example.com",
		TTL:     time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.st.Memory.IncrClicks(context.Background(), rec.Code, 5); err != nil {
		t.Fatal(err)
	}
	got, err := e.svc.Stats(context.Background(), rec.Code)
	if err != nil {
		t.Fatal(err)
	}
	if got.Code != rec.Code || got.LongURL != "https://example.com" || got.Clicks != 5 {
		t.Errorf("Stats = %+v", got)
	}
	if !got.CreatedAt.Equal(rec.CreatedAt) || !got.ExpiresAt.Equal(rec.ExpiresAt) {
		t.Errorf("Stats times = %v/%v, want %v/%v", got.CreatedAt, got.ExpiresAt, rec.CreatedAt, rec.ExpiresAt)
	}
	if _, err := e.svc.Stats(context.Background(), "ghost"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Stats(ghost) = %v, want ErrNotFound", err)
	}
}

func TestDeleteInvalidatesCache(t *testing.T) {
	e := newEnv(t, Config{})
	rec, err := e.svc.Create(context.Background(), CreateReq{LongURL: "https://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Resolve(context.Background(), rec.Code); err != nil {
		t.Fatal(err) // now cached
	}
	if err := e.svc.Delete(context.Background(), rec.Code); err != nil {
		t.Fatal(err)
	}
	// Must not serve the deleted mapping from cache.
	if _, err := e.svc.Resolve(context.Background(), rec.Code); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resolve after delete = %v, want ErrNotFound", err)
	}
	if err := e.svc.Delete(context.Background(), rec.Code); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete = %v, want ErrNotFound", err)
	}
}
