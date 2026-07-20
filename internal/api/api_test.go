package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/systemistic/url-shortener/internal/analytics"
	"github.com/systemistic/url-shortener/internal/ratelimit"
	"github.com/systemistic/url-shortener/internal/shortener"
	"github.com/systemistic/url-shortener/internal/store"
)

type ids struct{ n uint64 }

func (i *ids) Next() (uint64, error) { i.n++; return i.n, nil }

// newTestHandler wires a full stack against in-memory infrastructure.
// rateBurst controls how many creates the limiter admits.
func newTestHandler(t *testing.T, rateBurst int) (http.Handler, *store.Memory) {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	mem := store.NewMemory(nil)
	rec := analytics.NewRecorder(mem, analytics.RecorderConfig{}, log)
	svc := shortener.New(mem, &ids{}, rec, shortener.Config{SelfHost: "sho.rt"}, log)
	limiter := ratelimit.New(1, rateBurst)
	return New(svc, limiter, rec, "https://sho.rt", log), mem
}

func doJSON(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func createURL(t *testing.T, h http.Handler, body string) map[string]any {
	t.Helper()
	w := doJSON(t, h, http.MethodPost, "/api/v1/urls", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", w.Code, w.Body)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("create response not JSON: %v", err)
	}
	return resp
}

func TestCreateEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{"valid", `{"long_url":"https://example.com/a"}`, http.StatusCreated},
		{"valid with alias", `{"long_url":"https://example.com/b","custom_alias":"docs-page"}`, http.StatusCreated},
		{"valid with ttl", `{"long_url":"https://example.com/c","ttl_seconds":60}`, http.StatusCreated},
		{"bad url scheme", `{"long_url":"ftp://example.com"}`, http.StatusUnprocessableEntity},
		{"missing url", `{}`, http.StatusUnprocessableEntity},
		{"own domain loop", `{"long_url":"https://sho.rt/x"}`, http.StatusUnprocessableEntity},
		{"bad alias", `{"long_url":"https://example.com","custom_alias":"a b"}`, http.StatusUnprocessableEntity},
		{"negative ttl", `{"long_url":"https://example.com","ttl_seconds":-5}`, http.StatusUnprocessableEntity},
		{"malformed json", `{"long_url":`, http.StatusBadRequest},
		{"unknown field", `{"long_url":"https://example.com","nope":1}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler(t, 100)
			w := doJSON(t, h, http.MethodPost, "/api/v1/urls", tt.body)
			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d (body %s)", w.Code, tt.wantStatus, w.Body)
			}
		})
	}
}

func TestCreateResponseShape(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	resp := createURL(t, h, `{"long_url":"https://example.com/page","custom_alias":"my-alias"}`)
	if resp["code"] != "my-alias" {
		t.Errorf("code = %v", resp["code"])
	}
	if resp["short_url"] != "https://sho.rt/my-alias" {
		t.Errorf("short_url = %v", resp["short_url"])
	}
	if resp["long_url"] != "https://example.com/page" {
		t.Errorf("long_url = %v", resp["long_url"])
	}
}

func TestCreateAliasConflict409(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	body := `{"long_url":"https://example.com","custom_alias":"taken-one"}`
	createURL(t, h, body)
	w := doJSON(t, h, http.MethodPost, "/api/v1/urls", body)
	if w.Code != http.StatusConflict {
		t.Fatalf("duplicate alias status = %d, want 409", w.Code)
	}
}

func TestCreateRateLimited429(t *testing.T) {
	h, _ := newTestHandler(t, 2)
	statuses := []int{}
	for i := range 4 {
		w := doJSON(t, h, http.MethodPost, "/api/v1/urls",
			fmt.Sprintf(`{"long_url":"https://example.com/%d"}`, i))
		statuses = append(statuses, w.Code)
	}
	if statuses[0] != http.StatusCreated || statuses[1] != http.StatusCreated {
		t.Fatalf("burst not admitted: %v", statuses)
	}
	if statuses[2] != http.StatusTooManyRequests || statuses[3] != http.StatusTooManyRequests {
		t.Fatalf("flood not limited: %v", statuses)
	}
	// Denied responses carry Retry-After.
	w := doJSON(t, h, http.MethodPost, "/api/v1/urls", `{"long_url":"https://example.com"}`)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
		t.Errorf("expected 429 with Retry-After, got %d %q", w.Code, w.Header().Get("Retry-After"))
	}
}

func TestRedirect302(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	resp := createURL(t, h, `{"long_url":"https://example.com/target"}`)
	code := resp["code"].(string)

	w := doJSON(t, h, http.MethodGet, "/"+code, "")
	if w.Code != http.StatusFound {
		t.Fatalf("redirect status = %d, want 302", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "https://example.com/target" {
		t.Errorf("Location = %q", loc)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
}

func TestRedirectUnknown404(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	if w := doJSON(t, h, http.MethodGet, "/n0such", ""); w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestStatsEndpoint(t *testing.T) {
	h, mem := newTestHandler(t, 100)
	resp := createURL(t, h, `{"long_url":"https://example.com/s","ttl_seconds":3600}`)
	code := resp["code"].(string)

	// Clicks arrive via the analytics batcher; simulate a completed flush.
	if err := mem.IncrClicks(context.Background(), code, 2); err != nil {
		t.Fatal(err)
	}

	w := doJSON(t, h, http.MethodGet, "/api/v1/urls/"+code+"/stats", "")
	if w.Code != http.StatusOK {
		t.Fatalf("stats status = %d", w.Code)
	}
	var stats struct {
		Code      string `json:"code"`
		LongURL   string `json:"long_url"`
		Clicks    int64  `json:"clicks"`
		CreatedAt string `json:"created_at"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Code != code || stats.LongURL != "https://example.com/s" || stats.Clicks != 2 {
		t.Errorf("stats = %+v", stats)
	}
	if _, err := time.Parse(time.RFC3339, stats.CreatedAt); err != nil {
		t.Errorf("created_at %q not RFC3339: %v", stats.CreatedAt, err)
	}
	if _, err := time.Parse(time.RFC3339, stats.ExpiresAt); err != nil {
		t.Errorf("expires_at %q not RFC3339: %v", stats.ExpiresAt, err)
	}

	if w := doJSON(t, h, http.MethodGet, "/api/v1/urls/n0such/stats", ""); w.Code != http.StatusNotFound {
		t.Errorf("unknown stats status = %d, want 404", w.Code)
	}
}

func TestDeleteEndpoint(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	resp := createURL(t, h, `{"long_url":"https://example.com/d"}`)
	code := resp["code"].(string)

	if w := doJSON(t, h, http.MethodDelete, "/api/v1/urls/"+code, ""); w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", w.Code)
	}
	if w := doJSON(t, h, http.MethodGet, "/"+code, ""); w.Code != http.StatusNotFound {
		t.Fatalf("redirect after delete = %d, want 404", w.Code)
	}
	if w := doJSON(t, h, http.MethodDelete, "/api/v1/urls/"+code, ""); w.Code != http.StatusNotFound {
		t.Fatalf("second delete = %d, want 404", w.Code)
	}
}

func TestDebugAnalyticsEndpoint(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	w := doJSON(t, h, http.MethodGet, "/api/v1/debug/analytics", "")
	if w.Code != http.StatusOK {
		t.Fatalf("debug status = %d", w.Code)
	}
	var body map[string]uint64
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"dropped", "batches", "flushed"} {
		if _, ok := body[k]; !ok {
			t.Errorf("debug body missing %q: %v", k, body)
		}
	}
}

func TestHealthEndpoints(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	for _, path := range []string{"/healthz", "/readyz"} {
		if w := doJSON(t, h, http.MethodGet, path, ""); w.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200", path, w.Code)
		}
	}
}

func TestRootIndex(t *testing.T) {
	h, _ := newTestHandler(t, 100)
	if w := doJSON(t, h, http.MethodGet, "/", ""); w.Code != http.StatusOK {
		t.Errorf("root status = %d, want 200", w.Code)
	}
}
