package idgen

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewRejectsBadShard(t *testing.T) {
	if _, err := New(MaxShard+1, nil); err == nil {
		t.Fatal("New(MaxShard+1) succeeded, want error")
	}
	if _, err := New(MaxShard, nil); err != nil {
		t.Fatalf("New(MaxShard) error: %v", err)
	}
}

func TestNextUniqueAndOrdered(t *testing.T) {
	g, err := New(3, nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 10000
	prev := uint64(0)
	seen := make(map[uint64]bool, n)
	for range n {
		id, err := g.Next()
		if err != nil {
			t.Fatalf("Next error: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate ID %d", id)
		}
		seen[id] = true
		if id <= prev {
			t.Fatalf("IDs not strictly increasing: %d after %d", id, prev)
		}
		prev = id
		if (id >> seqBits & MaxShard) != 3 {
			t.Fatalf("ID %d does not carry shard 3", id)
		}
	}
}

func TestNextConcurrentUnique(t *testing.T) {
	g, err := New(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	const workers, per = 8, 2000
	ids := make(chan uint64, workers*per)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range per {
				id, err := g.Next()
				if err != nil {
					t.Error(err)
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[uint64]bool, workers*per)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate ID %d under concurrency", id)
		}
		seen[id] = true
	}
}

func TestSmallClockRegressionWaits(t *testing.T) {
	base := time.Now()
	current := base
	g, err := New(1, func() time.Time { return current })
	if err != nil {
		t.Fatal(err)
	}

	first, err := g.Next()
	if err != nil {
		t.Fatal(err)
	}
	// Clock steps back 3ms (≤ 5ms threshold): Next should spin until the
	// clock catches up, not fail. The fake clock advances 1ms per call.
	current = base.Add(-3 * time.Millisecond)
	orig := g.now
	g.now = func() time.Time {
		current = current.Add(time.Millisecond)
		return orig()
	}
	second, err := g.Next()
	if err != nil {
		t.Fatalf("Next after small regression: %v", err)
	}
	if second <= first {
		t.Fatalf("ID went backwards after small clock regression: %d then %d", first, second)
	}
}

func TestLargeClockRegressionErrors(t *testing.T) {
	base := time.Now()
	current := base
	g, err := New(1, func() time.Time { return current })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Next(); err != nil {
		t.Fatal(err)
	}
	current = base.Add(-6 * time.Millisecond) // > 5ms threshold
	if _, err := g.Next(); !errors.Is(err, ErrClockBackwards) {
		t.Fatalf("Next error = %v, want ErrClockBackwards", err)
	}
	// Clock recovers: IDs flow again.
	current = base.Add(time.Millisecond)
	if _, err := g.Next(); err != nil {
		t.Fatalf("Next after recovery: %v", err)
	}
}

func TestSequenceOverflowWaitsForNextMillisecond(t *testing.T) {
	base := time.Now()
	calls := 0
	g, err := New(0, func() time.Time {
		calls++
		// Advance one millisecond only after enough polls, exercising the
		// spin-wait path.
		return base.Add(time.Duration(calls/3) * time.Millisecond)
	})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool)
	for range (maxSeq + 1) * 2 {
		id, err := g.Next()
		if err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("duplicate ID %d across sequence overflow", id)
		}
		seen[id] = true
	}
}
