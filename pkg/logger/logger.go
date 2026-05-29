package logger

import (
	"log/slog"
	"os"
)

// New creates a structured JSON logger writing to stderr.
func New() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler)
}

// WithNode returns a logger that includes the node ID on every record.
func WithNode(log *slog.Logger, nodeID string) *slog.Logger {
	return log.With(slog.String("node_id", nodeID))
}
