package health

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/vaultfs/internal/replication"
	"github.com/yourusername/vaultfs/internal/store"
	"github.com/yourusername/vaultfs/internal/wal"
)

type clusterNode struct {
	id       string
	isLeader bool
	store    *store.Store
	wal      *wal.WAL
	repl     *replication.Manager
	monitor  *Monitor
	server   *httptest.Server
}

func TestLeaderFailoverPromotion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes := map[string]*clusterNode{}

	for _, id := range []string{"node-a", "node-b", "node-c"} {
		nodes[id] = newClusterNode(t, id, id == "node-a", log)
	}

	urls := make(map[string]string, len(nodes))
	for id, n := range nodes {
		urls[id] = n.server.URL
	}

	for id, n := range nodes {
		var peers []string
		for pid, u := range urls {
			if pid != id {
				peers = append(peers, u)
			}
		}
		leaderID := "node-a"
		n.monitor = NewMonitor(
			id, peers, leaderID, n.repl, log,
			WithHeartbeatInterval(200*time.Millisecond),
			WithMissedThreshold(3),
		)
		n.monitor.Start(ctx)
	}

	// Allow heartbeats to establish peer node IDs.
	time.Sleep(800 * time.Millisecond)

	// Kill the leader HTTP server (simulate crash).
	nodes["node-a"].server.Close()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if nodes["node-b"].repl.IsLeader() {
			if nodes["node-c"].repl.IsLeader() {
				t.Fatal("node-c should not promote when node-b is lower")
			}
			st := nodes["node-b"].monitor.ClusterStatus()
			if st.LeaderNodeID != "node-b" {
				t.Fatalf("cluster leader = %q, want node-b", st.LeaderNodeID)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("node-b did not promote to leader within 10s")
}

func newClusterNode(t *testing.T, id string, isLeader bool, log *slog.Logger) *clusterNode {
	t.Helper()

	dir := t.TempDir()
	st := store.New()
	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	repl := replication.New(id, nil, isLeader, log, st, w)

	var mu sync.RWMutex
	started := time.Now()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			mu.RLock()
			leader := repl.IsLeader()
			mu.RUnlock()
			_ = json.NewEncoder(w).Encode(PingResponse{
				Status:   "ok",
				NodeID:   id,
				IsLeader: leader,
				KeyCount: st.Len(),
			})
		default:
			http.NotFound(w, r)
		}
	}))

	t.Cleanup(func() {
		srv.Close()
		st.Close()
		_ = w.Close()
	})

	n := &clusterNode{
		id:       id,
		isLeader: isLeader,
		store:    st,
		wal:      w,
		repl:     repl,
		server:   srv,
	}
	_ = started
	return n
}
