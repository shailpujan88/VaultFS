package server

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/yourusername/vaultfs/internal/store"
)

type putRequest struct {
	Value string `json:"value"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type healthResponse struct {
	Status        string `json:"status"`
	NodeID        string `json:"node_id"`
	IsLeader      bool   `json:"is_leader"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	KeyCount      int    `json:"key_count"`
	PeerCount     int    `json:"peer_count"`
}

func (s *Server) handleGetKey(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	val, err := s.store.Get(key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		s.log.Error("store get failed", slog.String("key", key), slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, getResponse{Key: key, Value: string(val)})
}

func (s *Server) handlePutKey(w http.ResponseWriter, r *http.Request) {
	if !s.repl.IsLeader() {
		writeError(w, http.StatusForbidden, "not the leader")
		return
	}

	key, err := keyFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var req putRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	value := []byte(req.Value)
	if err := s.applyPut(key, value); err != nil {
		s.log.Error("put failed", slog.String("key", key), slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.repl.ReplicatePut(r.Context(), key, value)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	if !s.repl.IsLeader() {
		writeError(w, http.StatusForbidden, "not the leader")
		return
	}

	key, err := keyFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if _, err := s.store.Get(key); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		s.log.Error("store get failed", slog.String("key", key), slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if err := s.applyDelete(key); err != nil {
		s.log.Error("delete failed", slog.String("key", key), slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.repl.ReplicateDelete(r.Context(), key)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) applyPut(key string, value []byte) error {
	if err := s.wal.AppendPut(key, value); err != nil {
		return err
	}
	s.store.Put(key, value)
	return nil
}

func (s *Server) applyDelete(key string) error {
	if err := s.wal.AppendDelete(key); err != nil {
		return err
	}
	return s.store.Delete(key)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{
		Status:        "ok",
		NodeID:        s.repl.NodeID(),
		IsLeader:      s.repl.IsLeader(),
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		KeyCount:      s.store.Len(),
		PeerCount:     s.repl.PeerCount(),
	})
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.monitor.ClusterStatus())
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.metricsHandler.ServeHTTP(w, r)
}

func keyFromRequest(r *http.Request) (string, error) {
	raw := r.PathValue("key")
	if raw == "" {
		return "", errors.New("missing key")
	}
	key, err := url.PathUnescape(raw)
	if err != nil {
		return "", errors.New("invalid key")
	}
	if key == "" {
		return "", errors.New("missing key")
	}
	return key, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
