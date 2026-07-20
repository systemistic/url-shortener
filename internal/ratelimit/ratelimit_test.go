package ratelimit

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// withClock installs a settable fake clock on the limiter.
func withClock(l *Limiter) func(d time.Duration) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	current := base
	var mu sync.Mutex
	l.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	return func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		current = current.Add(d)
	}
}

func TestBurstThenDeny(t *testing.T) {
	l := New(1, 3)
	withClock(l)

	admitted := 0
	for range 10 {
		if l.Allow("ip1") {
			admitted++
		}
	}
	if admitted != 3 {
		t.Fatalf("admitted %d of 10 with burst 3, want 3", admitted)
	}
}

func TestRefillOverTime(t *testing.T) {
	l := New(2, 2) // 2 tokens/s, burst 2
	advance := withClock(l)

	if !l.Allow("k") || !l.Allow("k") {
		t.Fatal("burst not admitted")
	}
	if l.Allow("k") {
		t.Fatal("admitted with empty bucket")
	}
	advance(500 * time.Millisecond) // refills 1 token
	if !l.Allow("k") {
		t.Fatal("denied after refill")
	}
	if l.Allow("k") {
		t.Fatal("admitted more than the refilled token")
	}
}

func TestRefillCapsAtBurst(t *testing.T) {
	l := New(10, 2)
	advance := withClock(l)

	l.Allow("k")
	l.Allow("k")
	advance(time.Hour) // would refill thousands of tokens; cap at burst
	admitted := 0
	for range 10 {
		if l.Allow("k") {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("admitted %d after long idle, want burst 2", admitted)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l := New(1, 1)
	withClock(l)

	if !l.Allow("a") {
		t.Fatal("first request for a denied")
	}
	if l.Allow("a") {
		t.Fatal("second request for a admitted")
	}
	if !l.Allow("b") {
		t.Fatal("b penalized for a's traffic")
	}
}

func TestClampsBadConfig(t *testing.T) {
	l := New(-5, 0)
	withClock(l)
	if !l.Allow("k") {
		t.Fatal("minimally-permissive limiter denied the first request")
	}
}

func TestPruneBoundsMemory(t *testing.T) {
	l := New(1000, 1)
	advance := withClock(l)

	for i := range pruneThreshold {
		l.Allow(fmt.Sprintf("ip%d", i))
	}
	advance(time.Minute) // every bucket fully refills → prunable
	l.Allow("straw")     // insertion beyond threshold triggers the prune
	l.mu.Lock()
	n := len(l.buckets)
	l.mu.Unlock()
	if n > 2 {
		t.Fatalf("tracked buckets = %d after prune, want ≤ 2", n)
	}
}

func TestConcurrentAllow(t *testing.T) {
	l := New(1, 100)
	var wg sync.WaitGroup
	admitted := make(chan bool, 8*50)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				admitted <- l.Allow("shared")
			}
		}()
	}
	wg.Wait()
	close(admitted)
	n := 0
	for ok := range admitted {
		if ok {
			n++
		}
	}
	// 400 requests against burst 100 at 1 rps: at most ~101 admitted.
	if n < 100 || n > 102 {
		t.Fatalf("admitted %d of 400, want ≈100", n)
	}
}
