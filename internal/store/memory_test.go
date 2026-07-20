package store

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// fakeClock is a settable clock for deterministic TTL tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newFakeClock() *fakeClock               { return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }
func rec(code, url string, exp time.Time) URLRecord {
	return URLRecord{Code: code, LongURL: url, ExpiresAt: exp}
}

func TestCreateGetDelete(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	ctx := context.Background()

	if err := m.Create(ctx, rec("abc", "https://example.com", time.Time{})); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := m.Get(ctx, "abc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LongURL != "https://example.com" {
		t.Errorf("LongURL = %q", got.LongURL)
	}
	if err := m.Delete(ctx, "abc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := m.Get(ctx, "abc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
	if err := m.Delete(ctx, "abc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete = %v, want ErrNotFound", err)
	}
}

func TestCreateConflict(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	ctx := context.Background()

	if err := m.Create(ctx, rec("abc", "https://a.com", time.Time{})); err != nil {
		t.Fatal(err)
	}
	if err := m.Create(ctx, rec("abc", "https://b.com", time.Time{})); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate Create = %v, want ErrConflict", err)
	}
	// The original record must be untouched by the failed create.
	got, _ := m.Get(ctx, "abc")
	if got.LongURL != "https://a.com" {
		t.Errorf("record overwritten by conflicting create: %q", got.LongURL)
	}
}

func TestExpiredCodeIsReusable(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	ctx := context.Background()

	if err := m.Create(ctx, rec("abc", "https://a.com", clk.t.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Minute)
	if err := m.Create(ctx, rec("abc", "https://b.com", time.Time{})); err != nil {
		t.Fatalf("Create over expired record = %v, want nil", err)
	}
	got, err := m.Get(ctx, "abc")
	if err != nil || got.LongURL != "https://b.com" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
}

func TestLazyExpiryOnGet(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	ctx := context.Background()

	if err := m.Create(ctx, rec("ttl", "https://a.com", clk.t.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get(ctx, "ttl"); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	clk.advance(time.Minute) // exactly at deadline: expired
	if _, err := m.Get(ctx, "ttl"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get at expiry = %v, want ErrNotFound", err)
	}
	if m.Len() != 0 {
		t.Errorf("expired record not lazily removed, Len = %d", m.Len())
	}
}

func TestIncrClicks(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	ctx := context.Background()

	if err := m.Create(ctx, rec("abc", "https://a.com", time.Time{})); err != nil {
		t.Fatal(err)
	}
	if err := m.IncrClicks(ctx, "abc", 3); err != nil {
		t.Fatal(err)
	}
	if err := m.IncrClicks(ctx, "abc", 2); err != nil {
		t.Fatal(err)
	}
	got, _ := m.Get(ctx, "abc")
	if got.Clicks != 5 {
		t.Errorf("Clicks = %d, want 5", got.Clicks)
	}
	if err := m.IncrClicks(ctx, "nope", 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("IncrClicks(unknown) = %v, want ErrNotFound", err)
	}
}

func TestClicksSnapshotIsolation(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	ctx := context.Background()

	if err := m.Create(ctx, rec("abc", "https://a.com", time.Time{})); err != nil {
		t.Fatal(err)
	}
	snap, _ := m.Get(ctx, "abc")
	if err := m.IncrClicks(ctx, "abc", 7); err != nil {
		t.Fatal(err)
	}
	if snap.Clicks != 0 {
		t.Errorf("snapshot mutated by later IncrClicks: %d", snap.Clicks)
	}
}

func TestSweep(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	ctx := context.Background()

	for _, r := range []URLRecord{
		rec("live", "https://a.com", time.Time{}),
		rec("soon", "https://b.com", clk.t.Add(time.Minute)),
		rec("later", "https://c.com", clk.t.Add(time.Hour)),
	} {
		if err := m.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	clk.advance(30 * time.Minute)
	if n := m.Sweep(); n != 1 {
		t.Fatalf("Sweep reclaimed %d, want 1", n)
	}
	if m.Len() != 2 {
		t.Fatalf("Len after sweep = %d, want 2", m.Len())
	}
	if _, err := m.Get(ctx, "soon"); !errors.Is(err, ErrNotFound) {
		t.Errorf("swept record still readable")
	}
	if _, err := m.Get(ctx, "later"); err != nil {
		t.Errorf("unexpired record swept: %v", err)
	}
}

func TestRunSweeperStopsOnCancelAndSweeps(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.now)
	m.SweepInterval = time.Millisecond
	ctx := context.Background()

	if err := m.Create(ctx, rec("gone", "https://a.com", clk.t.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Hour)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.Run(runCtx)
		close(done)
	}()

	// Poll (bounded) until the background sweeper reclaims the record.
	deadline := time.Now().Add(5 * time.Second)
	for m.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("sweeper never reclaimed the expired record")
		}
		runtime.Gosched()
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
