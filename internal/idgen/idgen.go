// Package idgen implements a "snowflake-lite" unique 64-bit ID generator:
//
//	63           22        14         0
//	 | timestamp  | shard   | sequence |
//	 |  41 bits   | 8 bits  | 14 bits  |
//
// 41 bits of milliseconds since a custom epoch (~69 years of range),
// 8 bits of shard/node ID (256 generators without coordination), and a
// 14-bit per-millisecond sequence (16384 IDs/ms/shard ≈ 16M IDs/s/shard).
// IDs are roughly time-ordered, which keeps hot writes append-friendly in
// range-partitioned stores and makes codes non-guessable enough for a
// shortener without needing a central counter.
package idgen

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	shardBits = 8
	seqBits   = 14

	// MaxShard is the largest valid shard ID (inclusive).
	MaxShard = 1<<shardBits - 1

	maxSeq = 1<<seqBits - 1

	// maxBackwardsMs is how far the clock may step backwards before Next
	// refuses to issue IDs instead of spin-waiting for it to catch up.
	maxBackwardsMs = 5
)

// ErrClockBackwards is returned when the wall clock has moved backwards by
// more than 5 ms (a large NTP step); waiting it out inline would stall all
// writers, so the caller gets an error instead.
var ErrClockBackwards = errors.New("idgen: clock moved backwards")

// epoch is 2024-01-01T00:00:00Z in Unix milliseconds. A custom epoch keeps
// the timestamp field small so it fits 41 bits for decades.
var epoch = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// Generator produces unique, roughly monotonic 64-bit IDs. It is safe for
// concurrent use.
type Generator struct {
	mu     sync.Mutex
	shard  uint64
	lastMs int64
	seq    uint64
	now    func() time.Time
}

// New returns a Generator for the given shard (0..MaxShard). now is the
// injected clock; nil defaults to time.Now.
func New(shard uint16, now func() time.Time) (*Generator, error) {
	if shard > MaxShard {
		return nil, fmt.Errorf("idgen: shard %d out of range [0,%d]", shard, MaxShard)
	}
	if now == nil {
		now = time.Now
	}
	return &Generator{shard: uint64(shard), now: now}, nil
}

// Next returns the next unique ID. If the per-millisecond sequence is
// exhausted it spins until the next millisecond. If the wall clock steps
// backwards by at most 5 ms it waits for it to catch up; beyond that it
// returns ErrClockBackwards.
func (g *Generator) Next() (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := g.now().UnixMilli() - epoch
	if ms < g.lastMs {
		if g.lastMs-ms > maxBackwardsMs {
			return 0, fmt.Errorf("%w: %dms behind last issued timestamp", ErrClockBackwards, g.lastMs-ms)
		}
		// Small regression (NTP slew): wait for the clock to catch up.
		for ms < g.lastMs {
			ms = g.now().UnixMilli() - epoch
		}
	}
	if ms == g.lastMs {
		g.seq++
		if g.seq > maxSeq {
			// Sequence exhausted for this millisecond: wait for the next one.
			for ms <= g.lastMs {
				ms = g.now().UnixMilli() - epoch
			}
			g.seq = 0
		}
	} else {
		g.seq = 0
	}
	g.lastMs = ms
	return uint64(ms)<<(shardBits+seqBits) | g.shard<<seqBits | g.seq, nil
}
