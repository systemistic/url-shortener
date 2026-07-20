// Package logkit provides the shared structured-logging setup for all
// services in this monorepo. It standardizes on log/slog with a JSON
// handler so logs are machine-parseable in aggregation pipelines
// (ELK, Loki, CloudWatch, ...).
package logkit

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON slog.Logger tagged with the service name. The level
// is read from LOG_LEVEL (debug|info|warn|error), defaulting to info.
func New(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: levelFromEnv(),
	})
	return slog.New(h).With(slog.String("service", service))
}

func levelFromEnv() slog.Level {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
