package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/yourusername/vaultfs/internal/metrics"
	"github.com/yourusername/vaultfs/internal/store"
	"github.com/yourusername/vaultfs/internal/wal"
)

// Manager coordinates leader/follower replication to peer nodes.
type Manager struct {
	nodeID   string
	peers    []string
	isLeader bool
	log      *slog.Logger
	store    *store.Store
	wal      *wal.WAL
	client   *http.Client

	mu           sync.RWMutex
	lastReplAt   time.Time
	peerLastRepl map[string]time.Time
	peerHealth   map[string]PeerStatus
}

// New creates a replication manager. Nodes default to follower unless isLeader is true.
func New(nodeID string, peers []string, isLeader bool, log *slog.Logger, st *store.Store, w *wal.WAL) *Manager {
	normalized := make([]string, 0, len(peers))
	for _, peer := range peers {
		peer = strings.TrimSpace(peer)
		if peer != "" && peer != nodeID {
			normalized = append(normalized, peer)
		}
	}

	m := &Manager{
		nodeID:     nodeID,
		peers:      normalized,
		isLeader:   isLeader,
		log:        log,
		store:      st,
		wal:        w,
		peerHealth:   make(map[string]PeerStatus),
		peerLastRepl: make(map[string]time.Time),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}

	for _, peer := range normalized {
		m.peerHealth[peer] = PeerStatus{Address: peer, Healthy: false}
		metrics.SetReplicationLag(peer, 0)
	}

	role := RoleFollower
	if isLeader {
		role = RoleLeader
	}
	log.Info("replication initialized",
		slog.String("role", role),
		slog.Int("peer_count", len(normalized)),
		slog.Any("peers", normalized),
	)

	return m
}

// NodeID returns the local node identifier.
func (m *Manager) NodeID() string {
	return m.nodeID
}

// PeerCount returns how many peers are configured.
func (m *Manager) PeerCount() int {
	return len(m.peers)
}

// IsLeader reports whether this node is the designated leader.
func (m *Manager) IsLeader() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isLeader
}

// PromoteLeader marks this node as the cluster leader after failover.
func (m *Manager) PromoteLeader() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isLeader {
		return
	}
	m.isLeader = true
	m.log.Info("leader promotion event",
		slog.String("event", "leader_promoted"),
		slog.String("node_id", m.nodeID),
	)
}

// Role returns "leader" or "follower".
func (m *Manager) Role() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.isLeader {
		return RoleLeader
	}
	return RoleFollower
}

// ReplicatePut forwards a PUT to all peers (leader only).
func (m *Manager) ReplicatePut(ctx context.Context, key string, value []byte) {
	if !m.IsLeader() {
		return
	}
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	m.replicateToAll(ctx, Request{Operation: OpPut, Key: key, Value: valCopy})
}

// ReplicateDelete forwards a DELETE to all peers (leader only).
func (m *Manager) ReplicateDelete(ctx context.Context, key string) {
	if !m.IsLeader() {
		return
	}
	m.replicateToAll(ctx, Request{Operation: OpDelete, Key: key})
}

func (m *Manager) replicateToAll(ctx context.Context, req Request) {
	for _, peer := range m.peers {
		m.replicateToPeer(ctx, peer, req)
	}
}

func (m *Manager) replicateToPeer(ctx context.Context, peer string, req Request) {
	var lastErr error
	backoff := 100 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				m.setPeerHealth(peer, false, lastErr)
				return
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		err := m.sendReplicate(ctx, peer, req)
		if err == nil {
			m.setPeerHealth(peer, true, nil)
			m.markPeerReplicationSuccess(peer)
			return
		}
		lastErr = err
		m.log.Warn("replication attempt failed",
			slog.String("peer", peer),
			slog.Int("attempt", attempt),
			slog.String("operation", req.Operation),
			slog.String("key", req.Key),
			slog.String("error", err.Error()),
		)
	}

	m.setPeerHealth(peer, false, lastErr)
	m.refreshReplicationLag(peer)
	m.log.Error("replication failed after retries",
		slog.String("peer", peer),
		slog.String("operation", req.Operation),
		slog.String("key", req.Key),
		slog.Int("retries", maxRetries),
		slog.String("error", lastErr.Error()),
	)
}

func (m *Manager) sendReplicate(ctx context.Context, peer string, req Request) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal replicate request: %w", err)
	}

	url := peerReplicateURL(peer)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create replicate request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("replicate to %s: %w", peer, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("replicate to %s: status %d: %s", peer, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// Apply handles an incoming replication request on a peer node.
func (m *Manager) Apply(req Request) error {
	switch req.Operation {
	case OpPut:
		if err := m.wal.AppendPut(req.Key, req.Value); err != nil {
			return fmt.Errorf("wal append put: %w", err)
		}
		m.store.Put(req.Key, req.Value)
		return nil
	case OpDelete:
		if err := m.wal.AppendDelete(req.Key); err != nil {
			return fmt.Errorf("wal append delete: %w", err)
		}
		if err := m.store.Delete(req.Key); err != nil && !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("store delete: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown operation: %s", req.Operation)
	}
}

// Status returns replication role, peer health, and last successful replication time.
func (m *Manager) Status(ctx context.Context) Status {
	peers := make([]string, len(m.peers))
	copy(peers, m.peers)

	health := make([]PeerStatus, 0, len(peers))
	for _, peer := range peers {
		health = append(health, m.probePeer(ctx, peer))
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var lastRepl *time.Time
	if !m.lastReplAt.IsZero() {
		t := m.lastReplAt.UTC()
		lastRepl = &t
	}

	return Status{
		Role:              m.Role(),
		PeerHealth:        health,
		LastReplicationAt: lastRepl,
	}
}

func (m *Manager) probePeer(ctx context.Context, peer string) PeerStatus {
	m.mu.RLock()
	cached, ok := m.peerHealth[peer]
	m.mu.RUnlock()

	status := PeerStatus{Address: peer, Healthy: false}
	if ok {
		status.LastSuccess = cached.LastSuccess
		status.LastError = cached.LastError
	}

	url := peerHealthURL(peer)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		status.LastError = err.Error()
		return status
	}

	resp, err := m.client.Do(req)
	if err != nil {
		status.LastError = err.Error()
		return status
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		status.Healthy = true
		status.LastError = ""
		now := time.Now().UTC()
		status.LastSuccess = &now
		return status
	}

	status.LastError = fmt.Sprintf("health check status %d", resp.StatusCode)
	return status
}

func (m *Manager) markPeerReplicationSuccess(peer string) {
	now := time.Now().UTC()
	m.mu.Lock()
	m.lastReplAt = now
	m.peerLastRepl[peer] = now
	m.mu.Unlock()
	metrics.SetReplicationLag(peer, 0)
}

func (m *Manager) refreshReplicationLag(peer string) {
	m.mu.RLock()
	last, ok := m.peerLastRepl[peer]
	m.mu.RUnlock()
	if !ok {
		metrics.SetReplicationLag(peer, -1)
		return
	}
	metrics.SetReplicationLag(peer, time.Since(last).Seconds())
}

// RefreshReplicationLagMetrics updates lag gauges for all peers (call periodically on leaders).
func (m *Manager) RefreshReplicationLagMetrics() {
	if !m.IsLeader() {
		return
	}
	m.mu.RLock()
	peers := make([]string, len(m.peers))
	copy(peers, m.peers)
	m.mu.RUnlock()
	for _, peer := range peers {
		m.refreshReplicationLag(peer)
	}
}

func (m *Manager) setPeerHealth(peer string, healthy bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st := PeerStatus{Address: peer, Healthy: healthy}
	if healthy {
		now := time.Now().UTC()
		st.LastSuccess = &now
		st.LastError = ""
	} else if err != nil {
		st.LastError = err.Error()
	}
	if prev, ok := m.peerHealth[peer]; ok && !healthy && prev.LastSuccess != nil {
		st.LastSuccess = prev.LastSuccess
	}
	m.peerHealth[peer] = st
}

func peerReplicateURL(peer string) string {
	return peerBaseURL(peer) + replicatePath
}

func peerHealthURL(peer string) string {
	return peerBaseURL(peer) + "/health"
}

func peerBaseURL(peer string) string {
	peer = strings.TrimSpace(peer)
	if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
		return strings.TrimRight(peer, "/")
	}
	return "http://" + strings.TrimRight(peer, "/")
}
