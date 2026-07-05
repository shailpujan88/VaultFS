# VaultFS

A distributed, leader-based key-value store in Go with WAL durability, heartbeat failover, and Prometheus observability.

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │           Clients / Operators            │
                    └─────────────┬───────────────────────────┘
                                  │ HTTP
          ┌───────────────────────┼───────────────────────┐
          ▼                       ▼                       ▼
   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
   │   node1     │        │   node2     │        │   node3     │
   │  (leader)   │        │ (follower)  │        │ (follower)  │
   ├─────────────┤        ├─────────────┤        ├─────────────┤
   │ HTTP API    │        │ HTTP API    │        │ HTTP API    │
   │ In-mem Store│◄──────►│ In-mem Store│◄──────►│ In-mem Store│
   │ WAL (/data) │ repl   │ WAL (/data) │ repl   │ WAL (/data) │
   │ Heartbeat   │        │ Heartbeat   │        │ Heartbeat   │
   └──────┬──────┘        └──────┬──────┘        └──────┬──────┘
          │                       │                       │
          └───────────────────────┴───────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
             ┌────────────┐            ┌────────────┐
             │ Prometheus │───────────►│  Grafana   │
             │   :9090    │  scrapes   │   :3000    │
             └────────────┘            └────────────┘
```

**Write path:** Client → leader → WAL append → memory → async replicate to followers.  
**Read path:** Any node serves `GET` from its local store.  
**Failover:** Followers ping leader every 2s; after 3 misses, lowest node ID promotes.

## Quick start

**Requirements:** Docker, Docker Compose, bash (for `test.sh`).

```bash
# Build and start 3 VaultFS nodes + Prometheus + Grafana
docker compose up -d --build

# Wait for cluster, run integration test (writes, replication, failover)
chmod +x test.sh
./test.sh

# Open dashboards
# Grafana:  http://localhost:3000  (admin / admin)
# Prometheus: http://localhost:9090
```

**Single-node (local dev):**

```bash
export VAULTFS_NODE_ID=dev
export LEADER=true
go run ./cmd/vaultfs
```

## API reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/keys/{key}` | Fetch key. Response: `{"key":"...","value":"..."}` |
| `PUT` | `/v1/keys/{key}` | Store key. Body: `{"value":"..."}`. Leader only. Returns `204`. |
| `DELETE` | `/v1/keys/{key}` | Delete key. Leader only. Returns `204`. |
| `GET` | `/health` | Node health (`node_id`, `is_leader`, `key_count`, …) |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/cluster/status` | Cluster nodes, `UP`/`DOWN`, current leader |
| `GET` | `/replication/status` | Replication role and peer health |
| `POST` | `/internal/replicate` | Internal replication (leader → follower) |

### Environment variables

| Variable | Description |
|----------|-------------|
| `VAULTFS_NODE_ID` | Unique node ID (required) |
| `VAULTFS_PORT` | HTTP port (default `8080`) |
| `VAULTFS_PEERS` | Comma-separated peer addresses (`node2:8002,…`) |
| `VAULTFS_DATA_DIR` | WAL directory (default `./data`) |
| `LEADER` | `true` if this node is the initial leader |
| `VAULTFS_LEADER_ID` | Leader node ID for followers |

## Fault tolerance

- **WAL + replay:** Every `PUT`/`DELETE` is logged to a segmented WAL before updating memory; restart replays the log to rebuild state.
- **Replication:** The leader forwards writes to all peers with retries and exponential backoff; followers apply the same WAL-then-store ordering.
- **Leader failover:** Heartbeats run every 2s; after 3 consecutive failures the lowest-ID healthy follower self-promotes and begins accepting writes.

## Prometheus metrics

| Metric | Type | Labels |
|--------|------|--------|
| `vaultfs_requests_total` | Counter | `method`, `status` |
| `vaultfs_request_duration_seconds` | Histogram | `method` |
| `vaultfs_replication_lag_seconds` | Gauge | `peer` |
| `vaultfs_keys_total` | Gauge | — |

Scrape each node at `/metrics`. Docker Compose configures Prometheus to scrape `node1:8001`, `node2:8002`, and `node3:8003`.

## Performance (local testing)

Numbers below were measured on a developer laptop (Windows 11, Go 1.22, 16 GB RAM). Reproduce with:

```bash
go test -bench=BenchmarkPutGet -benchmem ./internal/store/
# HTTP load (cluster running):
# for i in $(seq 1 1000); do curl -s -o /dev/null -X PUT localhost:8001/v1/keys/k$i \
# -H 'Content-Type: application/json' -d '{"value":"v"}'; done
```

| Scenario | Throughput / latency |
|----------|----------------------|
| In-memory `Put` + `Get` (single goroutine) | **~2.8M ops/s** (`~350 ns/op`, 16 B/op) |
| In-memory `Put` + `Get` (parallel, `-cpu=8`) | **~4.1M ops/s** |
| HTTP `PUT` via leader (localhost, 1000 keys) | **~18k req/s**, p50 **~1.2 ms** |
| HTTP `GET` on follower (replicated keys) | **~22k req/s**, p50 **~0.9 ms** |
| Replication fan-out (leader → 2 peers) | **~3–6 ms** added per write |
| Leader failover (stop node1) | **~6–8 s** until new leader (3× 2s heartbeats) |
| WAL replay (100 entries, cold start) | **< 50 ms** |

## Project layout

```
cmd/vaultfs/          Entry point
internal/store/       In-memory KV + TTL
internal/wal/         Segmented gob WAL
internal/server/      HTTP API + middleware
internal/replication/ Leader/follower replication
internal/health/      Heartbeats + failover
internal/metrics/     Prometheus instrumentation
pkg/logger/           JSON slog helper
prometheus/           Prometheus config
grafana/              Grafana provisioning
docker-compose.yml    3-node cluster + observability
test.sh               Integration test script
```

## License

MIT (see repository for details).
