// Package httpkit is the shared HTTP serving kit for all services in this
// monorepo: a production-configured http.Server with graceful shutdown,
// standard middleware (request ID, structured access logs, panic recovery),
// JSON response helpers, and health/readiness endpoints.
package httpkit

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Server wraps http.Server with production defaults and graceful shutdown.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// NewServer builds a server with sane production timeouts. The handler is
// wrapped with the standard middleware chain (outermost first): panic
// recovery, request ID, access logging.
func NewServer(addr string, h http.Handler, log *slog.Logger) *Server {
	return &Server{
		log: log,
		srv: &http.Server{
			Addr:              addr,
			Handler:           Recover(log)(RequestID(AccessLog(log)(h))),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    1 << 20,
		},
	}
}

// Run serves until ctx is canceled or SIGINT/SIGTERM arrives, then drains
// in-flight requests for up to 15s before returning.
func (s *Server) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", slog.String("addr", s.srv.Addr))
		if err := s.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	s.log.Info("shutting down, draining connections")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return s.srv.Shutdown(shutdownCtx)
}
