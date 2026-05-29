package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	envNodeID   = "VAULTFS_NODE_ID"
	envPort     = "VAULTFS_PORT"
	envPeers    = "VAULTFS_PEERS"
	envDataDir  = "VAULTFS_DATA_DIR"
	envLeader   = "LEADER"
	envLeaderID = "VAULTFS_LEADER_ID"
	defaultPort = 8080
	defaultData = "./data"
)

// Config holds runtime settings for a VaultFS node.
type Config struct {
	NodeID   string
	Port     int
	Peers    []string
	DataDir  string
	IsLeader bool
	LeaderID string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	nodeID := strings.TrimSpace(os.Getenv(envNodeID))
	if nodeID == "" {
		return nil, fmt.Errorf("%s is required", envNodeID)
	}

	port := defaultPort
	if raw := strings.TrimSpace(os.Getenv(envPort)); raw != "" {
		p, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be a valid integer: %w", envPort, err)
		}
		if p < 1 || p > 65535 {
			return nil, fmt.Errorf("%s must be between 1 and 65535", envPort)
		}
		port = p
	}

	dataDir := defaultData
	if raw := strings.TrimSpace(os.Getenv(envDataDir)); raw != "" {
		dataDir = raw
	}

	var peers []string
	if raw := strings.TrimSpace(os.Getenv(envPeers)); raw != "" {
		for _, peer := range strings.Split(raw, ",") {
			peer = strings.TrimSpace(peer)
			if peer != "" {
				peers = append(peers, peer)
			}
		}
	}

	isLeader := parseBoolEnv(envLeader)

	leaderID := strings.TrimSpace(os.Getenv(envLeaderID))
	if isLeader {
		leaderID = nodeID
	}

	return &Config{
		NodeID:   nodeID,
		Port:     port,
		Peers:    peers,
		DataDir:  dataDir,
		IsLeader: isLeader,
		LeaderID: leaderID,
	}, nil
}

func parseBoolEnv(name string) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	return strings.EqualFold(raw, "true") || raw == "1" || strings.EqualFold(raw, "yes")
}

// Addr returns the listen address for this node.
func (c *Config) Addr() string {
	return fmt.Sprintf(":%d", c.Port)
}
