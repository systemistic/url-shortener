// Command urlshortener runs the URL-shortener service: HTTP API + redirect
// hot path, in-memory store with TTL sweeper, cache-aside LRU with negative
// caching and singleflight, per-IP rate limiting on creation, and an async
// click-analytics pipeline. All state is in-process; see the README for how
// each piece maps to real infrastructure.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/systemistic/go-system-design/pkg/httpkit"
	"github.com/systemistic/go-system-design/pkg/logkit"
	"github.com/systemistic/url-shortener/internal/analytics"
	"github.com/systemistic/url-shortener/internal/api"
	"github.com/systemistic/url-shortener/internal/idgen"
	"github.com/systemistic/url-shortener/internal/ratelimit"
	"github.com/systemistic/url-shortener/internal/shortener"
	"github.com/systemistic/url-shortener/internal/store"
)

type config struct {
	port      string
	baseURL   string
	selfHost  string
	shardID   uint16
	cacheSize int
	rateRPS   float64
	rateBurst int
}

func loadConfig() (config, error) {
	cfg := config{
		port:      envStr("PORT", "8081"),
		cacheSize: 10_000,
		rateRPS:   5,
		rateBurst: 10,
	}
	cfg.baseURL = envStr("BASE_URL", "http://localhost:"+cfg.port)
	if u, err := url.Parse(cfg.baseURL); err == nil {
		cfg.selfHost = u.Host
	}

	shard, err := envInt("SHARD_ID", 0)
	if err != nil || shard < 0 || shard > idgen.MaxShard {
		return cfg, fmt.Errorf("SHARD_ID must be an integer in [0,%d]: %v", idgen.MaxShard, err)
	}
	cfg.shardID = uint16(shard)

	if v, err := envInt("CACHE_SIZE", cfg.cacheSize); err != nil || v <= 0 {
		return cfg, fmt.Errorf("CACHE_SIZE must be a positive integer: %v", err)
	} else {
		cfg.cacheSize = v
	}
	if v, err := envInt("RATE_LIMIT_RPS", int(cfg.rateRPS)); err != nil || v <= 0 {
		return cfg, fmt.Errorf("RATE_LIMIT_RPS must be a positive integer: %v", err)
	} else {
		cfg.rateRPS = float64(v)
	}
	if v, err := envInt("RATE_LIMIT_BURST", cfg.rateBurst); err != nil || v <= 0 {
		return cfg, fmt.Errorf("RATE_LIMIT_BURST must be a positive integer: %v", err)
	} else {
		cfg.rateBurst = v
	}
	return cfg, nil
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	return strconv.Atoi(v)
}

func main() {
	log := logkit.New("urlshortener")
	if err := run(log); err != nil {
		log.Error("fatal", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	clock := time.Now
	ids, err := idgen.New(cfg.shardID, clock)
	if err != nil {
		return err
	}

	st := store.NewMemory(clock)
	st.SweepInterval = time.Minute
	st.Log = log

	recorder := analytics.NewRecorder(st, analytics.RecorderConfig{}, log)

	svc := shortener.New(st, ids, recorder, shortener.Config{
		CacheSize: cfg.cacheSize,
		SelfHost:  cfg.selfHost,
		Now:       clock,
	}, log)
	limiter := ratelimit.New(cfg.rateRPS, cfg.rateBurst)
	handler := api.New(svc, limiter, recorder, cfg.baseURL, log)

	// Background workers (TTL sweeper, analytics batcher) share one ctx so
	// they stop together after the HTTP server has drained.
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); st.Run(workerCtx) }()
	go func() { defer workers.Done(); recorder.Run(workerCtx) }()

	srv := httpkit.NewServer(":"+cfg.port, handler, log)
	log.Info("urlshortener starting",
		slog.String("port", cfg.port),
		slog.String("base_url", cfg.baseURL),
		slog.Int("shard_id", int(cfg.shardID)),
	)
	runErr := srv.Run(context.Background())

	// Shutdown order: the HTTP server has drained, so no new clicks arrive;
	// cancel the workers — the recorder flushes its final batch on the way
	// out — and wait for them to finish.
	stopWorkers()
	workers.Wait()
	log.Info("analytics drained",
		slog.Uint64("flushed", recorder.Flushed()),
		slog.Uint64("batches", recorder.Batches()),
		slog.Uint64("dropped", recorder.Dropped()),
	)
	return runErr
}
