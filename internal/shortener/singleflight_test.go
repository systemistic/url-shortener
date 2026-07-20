package shortener

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/systemistic/url-shortener/internal/store"
)

func TestFlightGroupCollapsesConcurrentCalls(t *testing.T) {
	g := newFlightGroup()
	const followers = 31

	var calls atomic.Int64
	leaderIn := make(chan struct{})
	release := make(chan struct{})

	// Leader: enters fn and blocks until every follower has joined.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec, err, shared := g.do("hot", func() (store.URLRecord, error) {
			calls.Add(1)
			close(leaderIn)
			<-release
			return store.URLRecord{Code: "hot", LongURL: "https://a.com"}, nil
		})
		if err != nil || shared || rec.LongURL != "https://a.com" {
			t.Errorf("leader got rec=%+v err=%v shared=%v", rec, err, shared)
		}
	}()
	<-leaderIn

	// The in-flight call is registered while the leader blocks in fn.
	g.mu.Lock()
	call := g.calls["hot"]
	g.mu.Unlock()
	if call == nil {
		t.Fatal("no in-flight call registered")
	}

	for range followers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, err, shared := g.do("hot", func() (store.URLRecord, error) {
				calls.Add(1)
				return store.URLRecord{}, nil
			})
			if err != nil || !shared || rec.LongURL != "https://a.com" {
				t.Errorf("follower got rec=%+v err=%v shared=%v", rec, err, shared)
			}
		}()
	}

	// Deterministic: release only after every follower is blocked on the
	// leader's call.
	deadline := time.Now().Add(5 * time.Second)
	for call.waiters.Load() != followers {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d followers joined", call.waiters.Load(), followers)
		}
		runtime.Gosched()
	}
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("fn executed %d times, want exactly 1", got)
	}
}

func TestFlightGroupPropagatesError(t *testing.T) {
	g := newFlightGroup()
	sentinel := errors.New("boom")
	_, err, _ := g.do("k", func() (store.URLRecord, error) {
		return store.URLRecord{}, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}

func TestFlightGroupSequentialCallsRunSeparately(t *testing.T) {
	g := newFlightGroup()
	var calls atomic.Int64
	fn := func() (store.URLRecord, error) {
		calls.Add(1)
		return store.URLRecord{}, nil
	}
	if _, _, sh := g.do("k", fn); sh {
		t.Error("first call reported shared")
	}
	if _, _, sh := g.do("k", fn); sh {
		t.Error("second sequential call reported shared")
	}
	if calls.Load() != 2 {
		t.Errorf("fn called %d times, want 2 (no dedup across time)", calls.Load())
	}
}

func TestFlightGroupDistinctKeysRunIndependently(t *testing.T) {
	g := newFlightGroup()
	var calls atomic.Int64
	var wg sync.WaitGroup
	for _, k := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.do(k, func() (store.URLRecord, error) {
				calls.Add(1)
				return store.URLRecord{Code: k}, nil
			})
		}()
	}
	wg.Wait()
	if calls.Load() != 3 {
		t.Errorf("fn called %d times, want 3", calls.Load())
	}
}
