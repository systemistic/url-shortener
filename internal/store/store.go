// Package store defines the persistence contract for URL mappings and an
// in-memory implementation. The Store interface is the seam where a real
// deployment plugs in DynamoDB (code as partition key) or Postgres; the
// in-memory version mirrors the semantics production code would rely on:
// conditional create (put-if-absent), point lookup, delete, batched click
// increments, and TTL expiry (lazy on read plus a background sweeper,
// exactly how DynamoDB TTL / Redis expiry behave).
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when no live record exists for a code
// (including records that exist but have expired).
var ErrNotFound = errors.New("url: not found")

// ErrConflict is returned by Create when the code is already taken.
var ErrConflict = errors.New("url: code already exists")

// URLRecord is one short-code → long-URL mapping. Clicks is a plain int64
// snapshot on reads; the store implementation owns all increments (via
// IncrClicks), so callers never mutate it.
type URLRecord struct {
	Code      string
	LongURL   string
	CreatedAt time.Time
	// ExpiresAt is the TTL deadline; the zero value means "never expires".
	ExpiresAt time.Time
	Clicks    int64
}

// Expired reports whether the record's TTL has passed at time now.
func (r URLRecord) Expired(now time.Time) bool {
	return !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt)
}

// Store is the persistence interface for URL mappings.
type Store interface {
	// Create inserts rec if rec.Code is free, returning ErrConflict
	// otherwise (conditional put). An expired record does not block its code.
	Create(ctx context.Context, rec URLRecord) error
	// Get returns the live record for code, or ErrNotFound (including when
	// the record exists but has expired — lazy expiry).
	Get(ctx context.Context, code string) (URLRecord, error)
	// Delete removes the record for code, or returns ErrNotFound.
	Delete(ctx context.Context, code string) error
	// IncrClicks adds n to the click counter for code. Used by the
	// analytics batcher; returns ErrNotFound for unknown/expired codes.
	IncrClicks(ctx context.Context, code string, n int64) error
}
