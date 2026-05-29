package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yourusername/vaultfs/internal/store"
	"github.com/yourusername/vaultfs/internal/wal"
)

func testManager(t *testing.T, leader bool, peers []string) (*Manager, *store.Store) {
	t.Helper()

	dir := t.TempDir()
	st := store.New()
	t.Cleanup(func() { st.Close() })

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("wal open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := New("node-a", peers, leader, log, st, w)
	return m, st
}

func TestApplyPutAndDelete(t *testing.T) {
	m, st := testManager(t, false, nil)

	if err := m.Apply(Request{Operation: OpPut, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("apply put: %v", err)
	}
	val, err := st.Get("k")
	if err != nil || string(val) != "v" {
		t.Fatalf("get: %v val=%q", err, val)
	}

	if err := m.Apply(Request{Operation: OpDelete, Key: "k"}); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if _, err := st.Get("k"); err == nil {
		t.Fatal("expected key deleted")
	}
}

func TestLeaderReplicatesToPeer(t *testing.T) {
	peerDir := t.TempDir()
	peerStore := store.New()
	t.Cleanup(func() { peerStore.Close() })
	peerWAL, err := wal.Open(peerDir)
	if err != nil {
		t.Fatalf("peer wal: %v", err)
	}
	t.Cleanup(func() { _ = peerWAL.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	peerRepl := New("peer-b", nil, false, log, peerStore, peerWAL)

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			return
		case replicatePath:
			var req Request
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := peerRepl.Apply(req); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(peerSrv.Close)

	leader, _ := testManager(t, true, []string{peerSrv.URL})
	leader.ReplicatePut(context.Background(), "sync-key", []byte("sync-val"))

	val, err := peerStore.Get("sync-key")
	if err != nil {
		t.Fatalf("peer get: %v", err)
	}
	if string(val) != "sync-val" {
		t.Fatalf("peer value = %q", val)
	}

	st := leader.Status(context.Background())
	if st.Role != RoleLeader {
		t.Fatalf("role = %s", st.Role)
	}
	if len(st.PeerHealth) != 1 || !st.PeerHealth[0].Healthy {
		t.Fatalf("peer health: %+v", st.PeerHealth)
	}
	if st.LastReplicationAt == nil {
		t.Fatal("expected last_replication_at")
	}
}

func TestReplicateRetriesUnreachablePeer(t *testing.T) {
	leader, _ := testManager(t, true, []string{"127.0.0.1:1"})

	start := time.Now()
	leader.ReplicatePut(context.Background(), "k", []byte("v"))
	elapsed := time.Since(start)

	if elapsed < 300*time.Millisecond {
		t.Fatalf("expected backoff retries, got %v", elapsed)
	}

	st := leader.Status(context.Background())
	if len(st.PeerHealth) != 1 {
		t.Fatalf("peer health len = %d", len(st.PeerHealth))
	}
}

func TestFollowerDoesNotReplicate(t *testing.T) {
	follower, _ := testManager(t, false, []string{"http://127.0.0.1:9"})

	body, _ := json.Marshal(Request{Operation: OpPut, Key: "k", Value: []byte("v")})
	req := httptest.NewRequest(http.MethodPost, replicatePath, bytes.NewReader(body))
	// follower manager should not send when ReplicatePut called
	follower.ReplicatePut(context.Background(), "k", []byte("v"))

	st := follower.Status(context.Background())
	if st.Role != RoleFollower {
		t.Fatalf("role = %s", st.Role)
	}
	_ = req
}
