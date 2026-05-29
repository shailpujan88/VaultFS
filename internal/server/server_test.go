package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yourusername/vaultfs/internal/health"
	"github.com/yourusername/vaultfs/internal/replication"
	"github.com/yourusername/vaultfs/internal/store"
	"github.com/yourusername/vaultfs/internal/wal"
)

func testServer(t *testing.T) (*Server, *store.Store) {
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
	repl := replication.New("node-test", nil, true, log, st, w)
	mon := health.NewMonitor("node-test", nil, "node-test", repl, log)
	srv := New("127.0.0.1:0", log, st, w, repl, mon)
	return srv, st
}

func TestPutGetDeleteKey(t *testing.T) {
	srv, _ := testServer(t)
	ts := httptest.NewServer(srv.http.Handler)
	t.Cleanup(ts.Close)

	body, _ := json.Marshal(putRequest{Value: "hello"})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/keys/greet", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put status = %d", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/v1/keys/greet")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d", resp.StatusCode)
	}
	var got getResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Value != "hello" {
		t.Fatalf("value = %q", got.Value)
	}
	if resp.Header.Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID header")
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/v1/keys/greet", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", resp.StatusCode)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	srv, st := testServer(t)
	st.Put("k", []byte("v"))

	ts := httptest.NewServer(srv.http.Handler)
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer resp.Body.Close()

	var health healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health.Status != "ok" || health.KeyCount != 1 {
		t.Fatalf("health: %+v", health)
	}
	if health.UptimeSeconds < 0 {
		t.Fatal("expected non-negative uptime")
	}

	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	text := string(data)
	if !bytes.Contains([]byte(text), []byte("vaultfs_requests_total")) {
		t.Fatalf("metrics missing counter: %s", text)
	}
	if !bytes.Contains([]byte(text), []byte("vaultfs_request_duration_seconds")) {
		t.Fatalf("metrics missing histogram: %s", text)
	}
	if !bytes.Contains([]byte(text), []byte("vaultfs_keys_total")) {
		t.Fatalf("metrics missing keys gauge: %s", text)
	}
}

func TestGracefulShutdown(t *testing.T) {
	srv, _ := testServer(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		_ = srv.http.Serve(ln)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- srv.http.Shutdown(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(shutdownTimeout + time.Second):
		t.Fatal("shutdown timed out")
	}
}
