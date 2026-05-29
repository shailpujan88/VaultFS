package health

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// LeaderPromoter is implemented by the replication manager for failover.
type LeaderPromoter interface {
	NodeID() string
	IsLeader() bool
	PromoteLeader()
}

type peerState struct {
	address         string
	nodeID          string
	status          string
	missedHeartbeats int
	lastSeen        time.Time
}

// Monitor runs heartbeat pings and leader failover.
type Monitor struct {
	nodeID       string
	peers        []string
	leaderNodeID string
	repl         LeaderPromoter
	log          *slog.Logger
	client       *http.Client

	interval  time.Duration
	threshold int

	mu           sync.RWMutex
	peerByAddr   map[string]*peerState
	localStatus  string
	localLastSeen time.Time
}

// Option configures a Monitor.
type Option func(*Monitor)

// WithHeartbeatInterval overrides the default 2s heartbeat interval.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(m *Monitor) {
		if d > 0 {
			m.interval = d
		}
	}
}

// WithMissedThreshold overrides the default of 3 missed heartbeats.
func WithMissedThreshold(n int) Option {
	return func(m *Monitor) {
		if n > 0 {
			m.threshold = n
		}
	}
}

// NewMonitor creates a heartbeat monitor.
func NewMonitor(nodeID string, peers []string, leaderNodeID string, repl LeaderPromoter, log *slog.Logger, opts ...Option) *Monitor {
	normalized := make([]string, 0, len(peers))
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p != "" {
			normalized = append(normalized, p)
		}
	}

	m := &Monitor{
		nodeID:       nodeID,
		peers:        normalized,
		leaderNodeID: leaderNodeID,
		repl:         repl,
		log:          log,
		client:       &http.Client{Timeout: 1500 * time.Millisecond},
		interval:     defaultHeartbeatInterval,
		threshold:    defaultMissedThreshold,
		peerByAddr:   make(map[string]*peerState),
		localStatus:  StatusUp,
		localLastSeen: time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(m)
	}
	for _, addr := range normalized {
		m.peerByAddr[addr] = &peerState{
			address: addr,
			status:  StatusDown,
		}
	}
	return m
}

// Start begins the heartbeat loop until ctx is cancelled.
func (m *Monitor) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()

		m.runRound(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.runRound(ctx)
			}
		}
	}()
}

func (m *Monitor) runRound(ctx context.Context) {
	m.mu.Lock()
	m.localLastSeen = time.Now().UTC()
	m.localStatus = StatusUp
	m.mu.Unlock()

	for _, addr := range m.peers {
		m.pingPeer(ctx, addr)
	}

	m.maybePromote()
}

func (m *Monitor) pingPeer(ctx context.Context, address string) {
	resp, err := m.fetchHealth(ctx, address)
	if err != nil {
		m.recordMiss(address, "", err)
		return
	}

	m.recordSuccess(address, resp.NodeID)
	if resp.IsLeader && resp.NodeID != "" {
		m.setLeaderNodeID(resp.NodeID)
	}
}

func (m *Monitor) fetchHealth(ctx context.Context, address string) (*PingResponse, error) {
	url := peerHealthURL(address)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	httpResp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("health status %d", httpResp.StatusCode)
	}

	var resp PingResponse
	if err := json.NewDecoder(io.LimitReader(httpResp.Body, 4096)).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode health: %w", err)
	}
	return &resp, nil
}

func (m *Monitor) recordSuccess(address, nodeID string) {
	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.peerByAddr[address]
	if st == nil {
		st = &peerState{address: address}
		m.peerByAddr[address] = st
	}
	st.nodeID = nodeID
	st.missedHeartbeats = 0
	st.status = StatusUp
	st.lastSeen = now
}

func (m *Monitor) recordMiss(address, nodeID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.peerByAddr[address]
	if st == nil {
		st = &peerState{address: address, status: StatusDown}
		m.peerByAddr[address] = st
	}
	if nodeID != "" {
		st.nodeID = nodeID
	}
	st.missedHeartbeats++
	if st.missedHeartbeats >= m.threshold {
		if st.status != StatusDown {
			st.status = StatusDown
			m.log.Error("peer marked down",
				slog.String("alert", "peer_down"),
				slog.String("node_id", st.nodeID),
				slog.String("address", address),
				slog.Int("missed_heartbeats", st.missedHeartbeats),
				slog.String("error", err.Error()),
			)
		}
	}
}

func (m *Monitor) setLeaderNodeID(id string) {
	m.mu.Lock()
	m.leaderNodeID = id
	m.mu.Unlock()
}

func (m *Monitor) maybePromote() {
	m.mu.RLock()
	leaderID := m.leaderNodeID
	leaderDown := m.isLeaderDownLocked(leaderID)
	m.mu.RUnlock()

	if leaderID == "" || !leaderDown || m.repl.IsLeader() {
		return
	}

	candidate := m.lowestUpFollowerID(leaderID)
	if candidate == "" || candidate != m.nodeID {
		return
	}

	m.repl.PromoteLeader()
	m.setLeaderNodeID(m.nodeID)
	m.log.Info("leader promotion event",
		slog.String("event", "leader_promoted"),
		slog.String("node_id", m.nodeID),
		slog.String("previous_leader", leaderID),
	)
}

func (m *Monitor) isLeaderDownLocked(leaderID string) bool {
	if leaderID == "" || leaderID == m.nodeID {
		return false
	}
	for _, st := range m.peerByAddr {
		if st.nodeID == leaderID {
			return st.status == StatusDown && st.missedHeartbeats >= m.threshold
		}
	}
	return false
}

func (m *Monitor) lowestUpFollowerID(leaderID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []string
	if m.nodeID != leaderID && m.localStatus == StatusUp {
		ids = append(ids, m.nodeID)
	}
	for _, st := range m.peerByAddr {
		if st.nodeID == "" || st.nodeID == leaderID {
			continue
		}
		if st.status == StatusUp {
			ids = append(ids, st.nodeID)
		}
	}
	if len(ids) == 0 {
		return ""
	}
	sort.Strings(ids)
	return ids[0]
}

// ClusterStatus returns the current cluster view.
func (m *Monitor) ClusterStatus() ClusterStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	leaderID := m.leaderNodeID
	if m.repl.IsLeader() {
		leaderID = m.nodeID
	}

	nodes := []NodeInfo{{
		NodeID:   m.nodeID,
		Status:   m.localStatus,
		LastSeen: timePtr(m.localLastSeen),
	}}

	seen := map[string]bool{m.nodeID: true}
	for _, st := range m.peerByAddr {
		id := st.nodeID
		if id == "" {
			id = st.address
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		var lastSeen *time.Time
		if !st.lastSeen.IsZero() {
			t := st.lastSeen.UTC()
			lastSeen = &t
		}
		nodes = append(nodes, NodeInfo{
			NodeID:   id,
			Address:  st.address,
			Status:   st.status,
			LastSeen: lastSeen,
		})
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})

	return ClusterStatus{
		LeaderNodeID: leaderID,
		Nodes:        nodes,
	}
}

// LeaderNodeID returns the currently believed leader node ID.
func (m *Monitor) LeaderNodeID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.repl.IsLeader() {
		return m.nodeID
	}
	return m.leaderNodeID
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func peerHealthURL(peer string) string {
	peer = strings.TrimSpace(peer)
	if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
		return strings.TrimRight(peer, "/") + "/health"
	}
	return "http://" + strings.TrimRight(peer, "/") + "/health"
}
