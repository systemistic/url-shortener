package analytics

import (
	"context"
	"sync"
	"testing"
	"time"
)

// memSink is a thread-safe ClickSink recording every increment.
type memSink struct {
	mu     sync.Mutex
	counts map[string]int64
	calls  int
	notify chan struct{} // closed-loop signal: one send per IncrClicks
}

func newMemSink() *memSink {
	return &memSink{counts: make(map[string]int64), notify: make(chan struct{}, 1024)}
}

func (s *memSink) IncrClicks(_ context.Context, code string, n int64) error {
	s.mu.Lock()
	s.counts[code] += n
	s.calls++
	s.mu.Unlock()
	s.notify <- struct{}{}
	return nil
}

func (s *memSink) count(code string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[code]
}

func (s *memSink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// waitCalls blocks until the sink has served n IncrClicks calls in total.
func (s *memSink) waitCalls(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-s.notify:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for sink call %d/%d", i+1, n)
		}
	}
}

func TestBatchSizeTriggersFlush(t *testing.T) {
	sink := newMemSink()
	r := NewRecorder(sink, RecorderConfig{BufferSize: 64, BatchSize: 4, FlushInterval: time.Hour}, nil)
	tick := make(chan time.Time)
	r.tick = tick

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	for range 4 {
		r.Record("abc")
	}
	sink.waitCalls(t, 1) // one aggregated call: 4 clicks on one code
	if got := sink.count("abc"); got != 4 {
		t.Errorf("clicks = %d, want 4", got)
	}
	// Counters are bumped just after the sink call: poll briefly.
	deadline := time.Now().Add(5 * time.Second)
	for r.Batches() != 1 || r.Flushed() != 4 {
		if time.Now().After(deadline) {
			t.Fatalf("Batches = %d, Flushed = %d, want 1 and 4", r.Batches(), r.Flushed())
		}
	}
	cancel()
	<-done
}

func TestTickerTriggersFlush(t *testing.T) {
	sink := newMemSink()
	r := NewRecorder(sink, RecorderConfig{BufferSize: 64, BatchSize: 1000}, nil)
	tick := make(chan time.Time)
	r.tick = tick

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	r.Record("a")
	r.Record("b")
	// The worker must consume both events before the tick, otherwise the
	// flush races the enqueue. Poke the tick until the flush shows up.
	deadline := time.Now().Add(5 * time.Second)
	for r.Batches() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("ticker flush never happened")
		}
		select {
		case tick <- time.Now():
		default:
		}
	}
	sink.waitCalls(t, 2) // two distinct codes → two sink calls
	if sink.count("a") != 1 || sink.count("b") != 1 {
		t.Errorf("counts = a:%d b:%d, want 1/1", sink.count("a"), sink.count("b"))
	}
	cancel()
	<-done
}

func TestEmptyTickDoesNotFlush(t *testing.T) {
	sink := newMemSink()
	r := NewRecorder(sink, RecorderConfig{BufferSize: 8, BatchSize: 8}, nil)
	tick := make(chan time.Time)
	r.tick = tick

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	tick <- time.Now() // nothing pending: no batch
	tick <- time.Now() // second send proves the first was fully handled
	if r.Batches() != 0 {
		t.Errorf("Batches = %d after empty ticks, want 0", r.Batches())
	}
	if sink.callCount() != 0 {
		t.Errorf("sink called %d times on empty flush", sink.callCount())
	}
	cancel()
	<-done
}

func TestCancelDrainsRemainder(t *testing.T) {
	sink := newMemSink()
	// Worker not yet running: events accumulate in the buffer.
	r := NewRecorder(sink, RecorderConfig{BufferSize: 64, BatchSize: 1000, FlushInterval: time.Hour}, nil)
	for range 7 {
		r.Record("xyz")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled: Run must still drain the buffer
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if got := sink.count("xyz"); got != 7 {
		t.Errorf("drained clicks = %d, want 7", got)
	}
	if r.Flushed() != 7 {
		t.Errorf("Flushed = %d, want 7", r.Flushed())
	}
}

func TestOverflowDropsAndCounts(t *testing.T) {
	sink := newMemSink()
	r := NewRecorder(sink, RecorderConfig{BufferSize: 2, BatchSize: 1000, FlushInterval: time.Hour}, nil)
	// No worker running: the buffer fills at 2, the rest drop.
	for range 5 {
		r.Record("hot")
	}
	if got := r.Dropped(); got != 3 {
		t.Fatalf("Dropped = %d, want 3", got)
	}
	// The buffered 2 still flush on drain.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	<-done
	if got := sink.count("hot"); got != 2 {
		t.Errorf("flushed clicks = %d, want 2", got)
	}
}

func TestAggregationOnePerCode(t *testing.T) {
	sink := newMemSink()
	r := NewRecorder(sink, RecorderConfig{BufferSize: 64, BatchSize: 6, FlushInterval: time.Hour}, nil)
	tick := make(chan time.Time)
	r.tick = tick

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	for range 3 {
		r.Record("a")
	}
	for range 3 {
		r.Record("b")
	}
	sink.waitCalls(t, 2)
	if sink.callCount() != 2 {
		t.Errorf("sink calls = %d, want 2 (aggregated per code)", sink.callCount())
	}
	if sink.count("a") != 3 || sink.count("b") != 3 {
		t.Errorf("counts = a:%d b:%d, want 3/3", sink.count("a"), sink.count("b"))
	}
	cancel()
	<-done
}
