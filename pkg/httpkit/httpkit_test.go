package httpkit

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestMiddlewareChain(t *testing.T) {
	var gotID string
	h := Recover(discardLogger())(RequestID(AccessLog(discardLogger())(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotID = RequestIDFromContext(r.Context())
			JSON(w, http.StatusTeapot, map[string]string{"ok": "yes"})
		}))))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
	if gotID == "" || rec.Header().Get("X-Request-Id") != gotID {
		t.Fatalf("request id not propagated: ctx=%q header=%q", gotID, rec.Header().Get("X-Request-Id"))
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestRequestIDPropagatesIncoming(t *testing.T) {
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-Id", "abc123")
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); got != "abc123" {
		t.Fatalf("X-Request-Id = %q, want abc123", got)
	}
}

func TestRecoverTurnsPanicInto500(t *testing.T) {
	h := Recover(discardLogger())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestDecodeJSONRejectsUnknownFieldsAndOversize(t *testing.T) {
	type in struct {
		Name string `json:"name"`
	}
	var v in
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"nope":1}`))
	if err := DecodeJSON(rec, req, &v, 1<<10); err == nil {
		t.Fatal("want error for unknown field")
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"`+strings.Repeat("x", 2048)+`"}`))
	if err := DecodeJSON(rec, req, &v, 64); err == nil {
		t.Fatal("want error for oversized body")
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"ok"}`))
	if err := DecodeJSON(rec, req, &v, 1<<10); err != nil || v.Name != "ok" {
		t.Fatalf("decode: err=%v v=%+v", err, v)
	}
}

func TestHealthEndpoints(t *testing.T) {
	ready := false
	mux := Health(func() bool { return ready })

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before ready = %d, want 503", rec.Code)
	}
	ready = true
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz after ready = %d, want 200", rec.Code)
	}
}
