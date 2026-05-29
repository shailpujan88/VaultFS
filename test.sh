#!/usr/bin/env bash
# End-to-end test for the 3-node VaultFS Docker cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

NODE1=http://localhost:8001
NODE2=http://localhost:8002
NODE3=http://localhost:8003

json_value() {
  # Extract a JSON string field value: json_value '{"value":"x"}' value -> x
  local body="$1" key="$2"
  printf '%s' "$body" | tr ',' '\n' | grep "\"${key}\"" | head -1 | sed 's/.*:"\([^"]*\)".*/\1/'
}

json_leader() {
  json_value "$1" leader
}

wait_cluster() {
  echo "==> Waiting for cluster health..."
  local i
  for i in $(seq 1 60); do
    if curl -sf "${NODE1}/health" >/dev/null \
      && curl -sf "${NODE2}/health" >/dev/null \
      && curl -sf "${NODE3}/health" >/dev/null; then
      echo "    All nodes healthy."
      return 0
    fi
    sleep 2
  done
  echo "ERROR: cluster did not become healthy in time" >&2
  docker compose ps
  return 1
}

write_keys() {
  local base_url="$1"
  echo "==> Writing 10 keys to ${base_url}..."
  local i
  for i in $(seq 0 9); do
    curl -sf -X PUT "${base_url}/v1/keys/key-${i}" \
      -H "Content-Type: application/json" \
      -d "{\"value\":\"value-${i}\"}" >/dev/null
  done
}

read_keys() {
  local base_url="$1" label="$2"
  echo "==> Reading 10 keys from ${label} (${base_url})..."
  local i val expected
  for i in $(seq 0 9); do
    expected="value-${i}"
    body="$(curl -sf "${base_url}/v1/keys/key-${i}")"
    val="$(json_value "$body" value)"
    if [[ "$val" != "$expected" ]]; then
      echo "ERROR: ${label} key-${i}: got '${val}', want '${expected}'" >&2
      exit 1
    fi
  done
  echo "    OK"
}

wait_for_new_leader() {
  echo "==> Waiting for leader failover (node2 or node3)..."
  local i leader
  for i in $(seq 1 30); do
    for port in 8002 8003; do
      body="$(curl -sf "http://localhost:${port}/cluster/status")"
      leader="$(json_leader "$body")"
      if [[ "$leader" == "node2" || "$leader" == "node3" ]]; then
        echo "    New leader: ${leader} (reported by localhost:${port})"
        NEW_LEADER_PORT="$port"
        NEW_LEADER_ID="$leader"
        return 0
      fi
    done
    sleep 2
  done
  echo "ERROR: no failover leader detected within 60s" >&2
  curl -sf "${NODE2}/cluster/status" || true
  curl -sf "${NODE3}/cluster/status" || true
  return 1
}

echo "==> Stopping any existing cluster..."
docker compose down -v --remove-orphans 2>/dev/null || true

echo "==> Building and starting 3-node cluster..."
docker compose up -d --build

wait_cluster

write_keys "$NODE1"
read_keys "$NODE2" "node2"
read_keys "$NODE3" "node3"

echo "==> Stopping node1 (leader)..."
docker compose stop node1

wait_for_new_leader

READ_URL="http://localhost:${NEW_LEADER_PORT}"
read_keys "$READ_URL" "${NEW_LEADER_ID}"

# Also verify the other follower still serves reads.
if [[ "$NEW_LEADER_ID" == "node2" ]]; then
  read_keys "$NODE3" "node3"
else
  read_keys "$NODE2" "node2"
fi

echo ""
echo "==> All tests passed."
echo "    Leader failover: node1 -> ${NEW_LEADER_ID}"
echo "    Data durable across ${NEW_LEADER_ID} and remaining follower."
