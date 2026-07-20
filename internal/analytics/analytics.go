// Package analytics implements asynchronous click tracking: redirect
// handlers enqueue events on a buffered channel and return immediately; a
// batching worker drains the channel and applies aggregated increments to a
// ClickSink (the store). This mirrors the production shape (Kafka topic +
// consumer batching into an OLAP store). Backpressure policy is
// load-shedding: when the buffer is full the event is dropped and counted
// rather than slowing down redirects — analytics are best-effort, redirect
// latency is not.
package analytics

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// ClickSink receives aggregated click increments. In this system it is the
// URL store's IncrClicks; in production it would be a bulk UPDATE / OLAP
// ingest.
type ClickSink interface {
	IncrClicks(ctx context.Context, code string, n int64) error
}

// RecorderConfig tunes the batching worker. Zero values take defaults.
type RecorderConfig struct {
	// BufferSize is the event channel capacity absorbing redirect bursts
	// (default 4096).
	BufferSize int
	// BatchSize triggers a flush once this many events accumulate
	// (default 256).
	BatchSize int
	// FlushInterval triggers a flush of whatever has accumulated
	// (default 1s).
	FlushInterval time.Duration
}

func (c *RecorderConfig) withDefaults() {
	if c.BufferSize < 1 {
		c.BufferSize = 4096
	}
	if c.BatchSize < 1 {
		c.BatchSize = 256
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
}

// Recorder ingests click events without blocking and batches them into a
// ClickSink. Construct with NewRecorder, then start the worker with Run in
// its own goroutine.
type Recorder struct {
	sink   ClickSink
	events chan string
	cfg    RecorderConfig
	log    *slog.Logger

	dropped atomic.Uint64
	batches atomic.Uint64
	flushed atomic.Uint64

	// tick, when non-nil, replaces the internal flush ticker (tests).
	tick <-chan time.Time
}

// NewRecorder builds a Recorder that flushes into st. It never starts
// goroutines; call Run to start the worker.
func NewRecorder(st ClickSink, cfg RecorderConfig, log *slog.Logger) *Recorder {
	cfg.withDefaults()
	return &Recorder{
		sink:   st,
		events: make(chan string, cfg.BufferSize),
		cfg:    cfg,
		log:    log,
	}
}

// Record enqueues a click without blocking. If the buffer is full the event
// is dropped and the drop counter bumped.
func (r *Recorder) Record(code string) {
	select {
	case r.events <- code:
	default:
		r.dropped.Add(1)
	}
}

// Dropped returns the total number of events shed due to backpressure.
func (r *Recorder) Dropped() uint64 { return r.dropped.Load() }

// Batches returns the number of flushes applied to the sink.
func (r *Recorder) Batches() uint64 { return r.batches.Load() }

// Flushed returns the total number of events applied to the sink.
func (r *Recorder) Flushed() uint64 { return r.flushed.Load() }

// Run is the batching worker: it accumulates events and flushes when the
// batch is full or on the flush interval. On ctx cancel it drains whatever
// is still buffered, flushes the final batch, and returns.
func (r *Recorder) Run(ctx context.Context) {
	tick := r.tick
	if tick == nil {
		ticker := time.NewTicker(r.cfg.FlushInterval)
		defer ticker.Stop()
		tick = ticker.C
	}

	// pending aggregates per-code counts so one flush is one sink call per
	// distinct code, not per event.
	pending := make(map[string]int64, r.cfg.BatchSize)
	size := 0
	for {
		select {
		case code := <-r.events:
			pending[code]++
			size++
			if size >= r.cfg.BatchSize {
				r.flush(pending, size)
				pending = make(map[string]int64, r.cfg.BatchSize)
				size = 0
			}
		case <-tick:
			if size > 0 {
				r.flush(pending, size)
				pending = make(map[string]int64, r.cfg.BatchSize)
				size = 0
			}
		case <-ctx.Done():
			// Graceful drain: consume everything already buffered, then
			// flush the remainder. The HTTP server has stopped by now, so
			// the channel can only shrink.
			for {
				select {
				case code := <-r.events:
					pending[code]++
					size++
				default:
					if size > 0 {
						r.flush(pending, size)
					}
					return
				}
			}
		}
	}
}

// flush applies one aggregated batch to the sink. Sink errors (e.g. the
// code was deleted between click and flush) are logged, not retried —
// clicks are best-effort.
func (r *Recorder) flush(pending map[string]int64, size int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for code, n := range pending {
		if err := r.sink.IncrClicks(ctx, code, n); err != nil && r.log != nil {
			r.log.Debug("click flush skipped",
				slog.String("code", code),
				slog.Int64("clicks", n),
				slog.Any("err", err),
			)
		}
	}
	r.batches.Add(1)
	r.flushed.Add(uint64(size))
	if r.log != nil {
		r.log.Debug("analytics batch flushed",
			slog.Int("events", size),
			slog.Int("codes", len(pending)),
		)
	}
}
