package health

import "time"

const (
	StatusUp   = "UP"
	StatusDown = "DOWN"

	defaultHeartbeatInterval = 2 * time.Second
	defaultMissedThreshold   = 3
)

// PingResponse is returned by GET /health on each node.
type PingResponse struct {
	Status    string `json:"status"`
	NodeID    string `json:"node_id"`
	IsLeader  bool   `json:"is_leader"`
	KeyCount  int    `json:"key_count"`
}

// NodeInfo describes a member of the cluster.
type NodeInfo struct {
	NodeID   string     `json:"node_id"`
	Address  string     `json:"address,omitempty"`
	Status   string     `json:"status"`
	LastSeen *time.Time `json:"last_seen,omitempty"`
}

// ClusterStatus is returned by GET /cluster/status.
type ClusterStatus struct {
	LeaderNodeID string     `json:"leader"`
	Nodes        []NodeInfo `json:"nodes"`
}
