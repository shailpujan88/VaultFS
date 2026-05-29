package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yourusername/vaultfs/internal/config"
	"github.com/yourusername/vaultfs/internal/health"
	"github.com/yourusername/vaultfs/internal/replication"
	"github.com/yourusername/vaultfs/internal/server"
	"github.com/yourusername/vaultfs/internal/store"
	"github.com/yourusername/vaultfs/internal/wal"
	"github.com/yourusername/vaultfs/pkg/logger"
)

func main() {
	baseLog := logger.New()

	cfg, err := config.Load()
	if err != nil {
		baseLog.Error("failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	log := logger.WithNode(baseLog, cfg.NodeID)
	log.Info("starting vaultfs",
		slog.Int("port", cfg.Port),
		slog.String("data_dir", cfg.DataDir),
		slog.Int("peer_count", len(cfg.Peers)),
		slog.Bool("leader", cfg.IsLeader),
		slog.String("leader_id", cfg.LeaderID),
	)

	st := store.New()
	defer st.Close()

	w, err := wal.Open(cfg.DataDir)
	if err != nil {
		log.Error("failed to open wal", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		if err := w.Close(); err != nil {
			log.Error("failed to close wal", slog.String("error", err.Error()))
		}
	}()

	if err := wal.Replay(cfg.DataDir, func(entry wal.Entry) error {
		switch entry.OperationType {
		case wal.OperationPut:
			st.Put(entry.Key, entry.Value)
		case wal.OperationDelete:
			_ = st.Delete(entry.Key)
		}
		return nil
	}); err != nil {
		log.Error("failed to replay wal", slog.String("error", err.Error()))
		os.Exit(1)
	}

	repl := replication.New(cfg.NodeID, cfg.Peers, cfg.IsLeader, log, st, w)
	monitor := health.NewMonitor(cfg.NodeID, cfg.Peers, cfg.LeaderID, repl, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	monitor.Start(ctx)
	go runReplicationLagRefresh(ctx, repl)

	srv := server.New(cfg.Addr(), log, st, w, repl, monitor)
	if err := srv.Start(ctx); err != nil {
		log.Error("server stopped with error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	log.Info("vaultfs stopped cleanly")
}

func runReplicationLagRefresh(ctx context.Context, repl *replication.Manager) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			repl.RefreshReplicationLagMetrics()
		}
	}
}
