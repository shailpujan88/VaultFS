package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/yourusername/vaultfs/internal/replication"
)

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var req replication.Request
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := s.repl.Apply(req); err != nil {
		s.log.Error("apply replication failed", slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.repl.Status(r.Context()))
}
