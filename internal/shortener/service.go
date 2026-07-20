// Package shortener is the core application service: it owns create /
// resolve / stats / delete semantics and composes the ID generator, store,
// read-path cache (cache-aside with a negative cache and singleflight),
// and the async click-analytics recorder.
package shortener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/systemistic/url-shortener/internal/analytics"
	"github.com/systemistic/url-shortener/internal/base62"
	"github.com/systemistic/url-shortener/internal/cache"
	"github.com/systemistic/url-shortener/internal/store"
)

// Service-level sentinel errors; the HTTP layer maps these to status codes
// in one place.
var (
	ErrNotFound     = errors.New("shortener: short url not found")
	ErrInvalidURL   = errors.New("shortener: invalid url")
	ErrInvalidAlias = errors.New("shortener: invalid alias")
	ErrInvalidTTL   = errors.New("shortener: invalid ttl")
	ErrAliasTaken   = errors.New("shortener: alias already taken")
)

// IDGen yields unique 64-bit IDs (consumer-side interface; the concrete
// implementation is idgen's snowflake-lite generator).
type IDGen interface{ Next() (uint64, error) }

// Clock is the injectable time source.
type Clock = func() time.Time

// createRetries bounds retry attempts when a generated code collides
// (possible only against a custom alias; generated IDs are unique).
const createRetries = 3

// Config tunes the service. Zero values take defaults.
type Config struct {
	// CacheSize is the positive LRU capacity (default 10000).
	CacheSize int
	// NegCacheSize is the negative LRU capacity (default 1024).
	NegCacheSize int
	// NegativeTTL is how long a known-missing code is remembered
	// (default 30s).
	NegativeTTL time.Duration
	// SelfHost, when set, is this service's public host; long URLs
	// pointing at it are rejected to prevent redirect loops.
	SelfHost string
	// Now is the injected clock (default time.Now).
	Now Clock
}

func (c *Config) withDefaults() {
	if c.CacheSize <= 0 {
		c.CacheSize = 10_000
	}
	if c.NegCacheSize <= 0 {
		c.NegCacheSize = 1024
	}
	if c.NegativeTTL <= 0 {
		c.NegativeTTL = 30 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// CreateReq is a request to shorten a URL.
type CreateReq struct {
	LongURL     string
	CustomAlias string        // optional; validated when set
	TTL         time.Duration // 0 = never expires
}

// StatsResp aggregates mapping metadata with the click counter.
type StatsResp struct {
	Code      string
	LongURL   string
	Clicks    int64
	CreatedAt time.Time
	ExpiresAt time.Time // zero = never
}

// Service implements the URL-shortener use cases. Safe for concurrent use.
type Service struct {
	store store.Store
	// pos caches live records on the redirect hot path (Redis stand-in).
	pos *cache.LRU[string, store.URLRecord]
	// neg remembers known-missing codes until the stored deadline, so
	// floods of dead-code lookups never reach the store.
	neg       *cache.LRU[string, time.Time]
	ids       IDGen
	analytics *analytics.Recorder
	flight    *flightGroup
	negTTL    time.Duration
	selfHost  string
	now       Clock
	log       *slog.Logger
}

// New wires a Service from its dependencies. It starts no goroutines.
func New(st store.Store, ids IDGen, an *analytics.Recorder, cfg Config, log *slog.Logger) *Service {
	cfg.withDefaults()
	return &Service{
		store:     st,
		pos:       cache.NewLRU[string, store.URLRecord](cfg.CacheSize),
		neg:       cache.NewLRU[string, time.Time](cfg.NegCacheSize),
		ids:       ids,
		analytics: an,
		flight:    newFlightGroup(),
		negTTL:    cfg.NegativeTTL,
		selfHost:  cfg.SelfHost,
		now:       cfg.Now,
		log:       log,
	}
}

// Create validates req and persists a new mapping, either under the
// requested custom alias or under a freshly generated base62 code.
func (s *Service) Create(ctx context.Context, req CreateReq) (store.URLRecord, error) {
	if err := validateLongURL(req.LongURL, s.selfHost); err != nil {
		return store.URLRecord{}, err
	}
	if err := validateTTL(req.TTL); err != nil {
		return store.URLRecord{}, err
	}
	now := s.now()
	rec := store.URLRecord{
		LongURL:   req.LongURL,
		CreatedAt: now,
	}
	if req.TTL > 0 {
		rec.ExpiresAt = now.Add(req.TTL)
	}

	if req.CustomAlias != "" {
		if err := validateAlias(req.CustomAlias); err != nil {
			return store.URLRecord{}, err
		}
		rec.Code = req.CustomAlias
		if err := s.store.Create(ctx, rec); err != nil {
			if errors.Is(err, store.ErrConflict) {
				return store.URLRecord{}, fmt.Errorf("%w: %q", ErrAliasTaken, req.CustomAlias)
			}
			return store.URLRecord{}, err
		}
		s.neg.Delete(rec.Code) // the code exists now: clear any negative entry
		return rec, nil
	}

	var lastErr error
	for range createRetries {
		id, err := s.ids.Next()
		if err != nil {
			return store.URLRecord{}, fmt.Errorf("generating id: %w", err)
		}
		rec.Code = base62.Encode(id)
		if err := s.store.Create(ctx, rec); err != nil {
			lastErr = err
			if errors.Is(err, store.ErrConflict) {
				continue // collided with a custom alias; regenerate
			}
			return store.URLRecord{}, err
		}
		s.neg.Delete(rec.Code)
		return rec, nil
	}
	return store.URLRecord{}, fmt.Errorf("could not allocate a unique code: %w", lastErr)
}

// Resolve returns the long URL for code and records the click
// asynchronously. This is the hot path: negative cache, then positive LRU,
// then a singleflight-deduplicated store read that repopulates the cache.
func (s *Service) Resolve(ctx context.Context, code string) (string, error) {
	rec, err := s.lookup(ctx, code)
	if err != nil {
		return "", err
	}
	s.analytics.Record(code)
	return rec.LongURL, nil
}

// Stats reads straight from the store (control-plane path, no cache) so
// clicks and TTL are as fresh as the last analytics flush.
func (s *Service) Stats(ctx context.Context, code string) (StatsResp, error) {
	rec, err := s.store.Get(ctx, code)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return StatsResp{}, ErrNotFound
		}
		return StatsResp{}, err
	}
	return StatsResp{
		Code:      rec.Code,
		LongURL:   rec.LongURL,
		Clicks:    rec.Clicks,
		CreatedAt: rec.CreatedAt,
		ExpiresAt: rec.ExpiresAt,
	}, nil
}

// Delete removes the mapping and invalidates its cache entry.
func (s *Service) Delete(ctx context.Context, code string) error {
	if err := s.store.Delete(ctx, code); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}
	s.pos.Delete(code)
	return nil
}

// lookup is the cache-aside read: a fresh negative entry or a live positive
// entry wins; otherwise one singleflight leader per code reads the store
// and repopulates the caches while followers wait for its result.
func (s *Service) lookup(ctx context.Context, code string) (store.URLRecord, error) {
	now := s.now()
	if deadline, ok := s.neg.Get(code); ok {
		if now.Before(deadline) {
			return store.URLRecord{}, ErrNotFound
		}
		s.neg.Delete(code) // stale negative entry
	}
	if rec, ok := s.pos.Get(code); ok {
		if !rec.Expired(now) {
			return rec, nil
		}
		s.pos.Delete(code) // cached record hit its own TTL: re-read
	}

	rec, err, _ := s.flight.do(code, func() (store.URLRecord, error) {
		rec, err := s.store.Get(ctx, code)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				// Negative cache: absorb repeat lookups of dead codes.
				s.neg.Put(code, s.now().Add(s.negTTL))
				return store.URLRecord{}, ErrNotFound
			}
			return store.URLRecord{}, err
		}
		s.pos.Put(code, rec)
		return rec, nil
	})
	return rec, err
}
