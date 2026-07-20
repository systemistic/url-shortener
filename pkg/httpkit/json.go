package httpkit

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// JSON writes v as a JSON response with the given status code.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Error writes a standard JSON error envelope.
func Error(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"error": msg})
}

// DecodeJSON strictly decodes a request body into v, rejecting unknown
// fields and bodies over maxBytes (protects against oversized payloads).
func DecodeJSON(w http.ResponseWriter, r *http.Request, v any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return fmt.Errorf("body exceeds %d bytes", maxErr.Limit)
		}
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if dec.More() {
		return errors.New("body must contain a single JSON object")
	}
	_, _ = io.Copy(io.Discard, r.Body)
	return nil
}

// Health returns a mux serving /healthz (liveness) and /readyz (readiness).
// ready is polled on each /readyz request so services can gate readiness on
// dependencies (leader election, cache warmup, ...).
func Health(ready func() bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready == nil || ready() {
			JSON(w, http.StatusOK, map[string]string{"status": "ready"})
			return
		}
		Error(w, http.StatusServiceUnavailable, "not ready")
	})
	return mux
}
