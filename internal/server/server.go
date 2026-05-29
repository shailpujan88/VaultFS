package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/yourusername/vaultfs/internal/health"
	"github.com/yourusername/vaultfs/internal/metrics"
	"github.com/yourusername/vaultfs/internal/replication"
	"github.com/yourusername/vaultfs/internal/store"
	"github.com/yourusername/vaultfs/internal/wal"
)

const shutdownTimeout = 10 * time.Second

// Server exposes the VaultFS HTTP API.
type Server struct {
	log            *slog.Logger
	store          *store.Store
	wal            *wal.WAL
	repl           *replication.Manager
	monitor        *health.Monitor
	metricsHandler http.Handler
	startedAt      time.Time
	http           *http.Server
}

// New wires dependencies, routes, and middleware.
func New(addr string, log *slog.Logger, st *store.Store, w *wal.WAL, repl *replication.Manager, mon *health.Monitor) *Server {
	s := &Server{
		log:       log,
		store:     st,
		wal:       w,
		repl:      repl,
		monitor:        mon,
		metricsHandler: promhttp.Handler(),
		startedAt:      time.Now().UTC(),
	}

	metrics.SetKeysTotal(st.Len())

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/keys/{key}", s.handleGetKey)
	mux.HandleFunc("PUT /v1/keys/{key}", s.handlePutKey)
	mux.HandleFunc("DELETE /v1/keys/{key}", s.handleDeleteKey)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("POST /internal/replicate", s.handleReplicate)
	mux.HandleFunc("GET /replication/status", s.handleReplicationStatus)
	mux.HandleFunc("GET /cluster/status", s.handleClusterStatus)

	s.http = &http.Server{
		Addr:         addr,
		Handler:      s.withMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// Start listens until ctx is cancelled, then gracefully shuts down with a 10s drain timeout.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", slog.String("addr", s.http.Addr))
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		s.log.Info("graceful shutdown started", slog.Duration("timeout", shutdownTimeout))
		if err := s.http.Shutdown(shutdownCtx); err != nil {
			return err
		}
		s.log.Info("http server stopped")
		return nil
	case err := <-errCh:
		return err
	}
}
