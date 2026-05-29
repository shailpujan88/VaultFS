package replication

import "time"

const (
	RoleLeader   = "leader"
	RoleFollower = "follower"

	OpPut    = "put"
	OpDelete = "delete"

	replicatePath = "/internal/replicate"
	maxRetries    = 3
)

// Request is the payload for POST /internal/replicate.
type Request struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
}

// PeerStatus describes reachability of a single peer.
type PeerStatus struct {
	Address     string     `json:"address"`
	Healthy     bool       `json:"healthy"`
	LastSuccess *time.Time `json:"last_success,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
}

// Status is returned by GET /replication/status.
type Status struct {
	Role                string       `json:"role"`
	PeerHealth          []PeerStatus `json:"peer_health"`
	LastReplicationAt   *time.Time   `json:"last_replication_at,omitempty"`
}
