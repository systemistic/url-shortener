// Package api is the HTTP transport for the URL shortener: routing,
// request/response DTOs, error mapping, and the per-IP rate limit on the
// write path. Business rules live in the shortener package.
package api

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/systemistic/go-system-design/pkg/httpkit"
	"github.com/systemistic/url-shortener/internal/analytics"
	"github.com/systemistic/url-shortener/internal/ratelimit"
	"github.com/systemistic/url-shortener/internal/shortener"
)

// maxBodyBytes bounds create-request bodies (URL + alias + ttl is tiny).
const maxBodyBytes = 8 << 10

// Handler serves the shortener HTTP API.
type Handler struct {
	svc      *shortener.Service
	limiter  *ratelimit.Limiter
	recorder *analytics.Recorder
	baseURL  string
	log      *slog.Logger
}

// New builds the routed handler. baseURL is the public prefix used to
// render short links (e.g. "https://sho.rt"); rec feeds the debug
// analytics endpoint.
func New(svc *shortener.Service, limiter *ratelimit.Limiter, rec *analytics.Recorder, baseURL string, log *slog.Logger) http.Handler {
	h := &Handler{
		svc:      svc,
		limiter:  limiter,
		recorder: rec,
		baseURL:  strings.TrimRight(baseURL, "/"),
		log:      log,
	}
	mux := http.NewServeMux()

	// Liveness/readiness from the shared kit. These literal patterns take
	// precedence over the "GET /{code}" wildcard below.
	health := httpkit.Health(func() bool { return true })
	mux.Handle("GET /healthz", health)
	mux.Handle("GET /readyz", health)

	mux.HandleFunc("POST /api/v1/urls", h.create)
	mux.HandleFunc("GET /api/v1/urls/{code}/stats", h.stats)
	mux.HandleFunc("DELETE /api/v1/urls/{code}", h.delete)
	mux.HandleFunc("GET /api/v1/debug/analytics", h.debugAnalytics)

	// The redirect route: single path segment, everything else 404s.
	mux.HandleFunc("GET /{code}", h.redirect)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		httpkit.JSON(w, http.StatusOK, map[string]string{
			"service": "urlshortener",
			"create":  "POST /api/v1/urls",
		})
	})
	return mux
}

type createRequest struct {
	LongURL     string `json:"long_url"`
	CustomAlias string `json:"custom_alias,omitempty"`
	TTLSeconds  int64  `json:"ttl_seconds,omitempty"`
}

type createResponse struct {
	Code      string `json:"code"`
	ShortURL  string `json:"short_url"`
	LongURL   string `json:"long_url"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	if !h.limiter.Allow(clientIP(r)) {
		w.Header().Set("Retry-After", "1")
		httpkit.Error(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}
	var req createRequest
	if err := httpkit.DecodeJSON(w, r, &req, maxBodyBytes); err != nil {
		httpkit.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	rec, err := h.svc.Create(r.Context(), shortener.CreateReq{
		LongURL:     req.LongURL,
		CustomAlias: req.CustomAlias,
		TTL:         time.Duration(req.TTLSeconds) * time.Second,
	})
	if err != nil {
		h.writeError(w, err)
		return
	}
	resp := createResponse{
		Code:     rec.Code,
		ShortURL: h.baseURL + "/" + rec.Code,
		LongURL:  rec.LongURL,
	}
	if !rec.ExpiresAt.IsZero() {
		resp.ExpiresAt = rec.ExpiresAt.UTC().Format(time.RFC3339)
	}
	httpkit.JSON(w, http.StatusCreated, resp)
}

func (h *Handler) redirect(w http.ResponseWriter, r *http.Request) {
	long, err := h.svc.Resolve(r.Context(), r.PathValue("code"))
	if err != nil {
		h.writeError(w, err)
		return
	}
	// 302 (Found), not 301: permanent redirects get cached by browsers and
	// CDNs indefinitely, which would bypass this hop — losing click
	// analytics and making deletes/TTLs ineffective. no-store keeps shared
	// caches from pinning the answer for the same reason.
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, long, http.StatusFound)
}

type statsResponse struct {
	Code      string `json:"code"`
	LongURL   string `json:"long_url"`
	Clicks    int64  `json:"clicks"`
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.Stats(r.Context(), r.PathValue("code"))
	if err != nil {
		h.writeError(w, err)
		return
	}
	resp := statsResponse{
		Code:      res.Code,
		LongURL:   res.LongURL,
		Clicks:    res.Clicks,
		CreatedAt: res.CreatedAt.UTC().Format(time.RFC3339),
	}
	if !res.ExpiresAt.IsZero() {
		resp.ExpiresAt = res.ExpiresAt.UTC().Format(time.RFC3339)
	}
	httpkit.JSON(w, http.StatusOK, resp)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Delete(r.Context(), r.PathValue("code")); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// debugAnalytics surfaces the click pipeline's load-shedding counters.
func (h *Handler) debugAnalytics(w http.ResponseWriter, r *http.Request) {
	httpkit.JSON(w, http.StatusOK, map[string]uint64{
		"dropped": h.recorder.Dropped(),
		"batches": h.recorder.Batches(),
		"flushed": h.recorder.Flushed(),
	})
}

// writeError maps service sentinels to HTTP statuses in one place.
func (h *Handler) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, shortener.ErrNotFound):
		httpkit.Error(w, http.StatusNotFound, "short url not found")
	case errors.Is(err, shortener.ErrAliasTaken):
		httpkit.Error(w, http.StatusConflict, err.Error())
	case errors.Is(err, shortener.ErrInvalidURL),
		errors.Is(err, shortener.ErrInvalidAlias),
		errors.Is(err, shortener.ErrInvalidTTL):
		httpkit.Error(w, http.StatusUnprocessableEntity, err.Error())
	default:
		h.log.Error("internal error", slog.Any("err", err))
		httpkit.Error(w, http.StatusInternalServerError, "internal error")
	}
}

// clientIP extracts the caller's IP for rate limiting. It trusts the first
// X-Forwarded-For hop when present (this service is designed to sit behind
// a load balancer; in an untrusted topology, key on RemoteAddr only).
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if first, _, ok := strings.Cut(xff, ","); ok {
			return strings.TrimSpace(first)
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
