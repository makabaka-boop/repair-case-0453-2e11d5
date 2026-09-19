// Package api exposes the batch/chunk HTTP handlers.
package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"batchseal/internal/store"
)

// Limits fixed by the protocol.
const (
	MaxExpectedChunks = 10000
	MaxPayloadBytes   = 65536
	// Generous envelope ceiling: payload plus JSON framing.
	MaxChunkBodyBytes = MaxPayloadBytes + 4096
)

// Server wires the store to HTTP routes.
type Server struct {
	store *store.Store
	mux   *http.ServeMux
	log   *log.Logger
}

// NewServer builds the router.
func NewServer(s *store.Store, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	srv := &Server{store: s, mux: http.NewServeMux(), log: logger}
	srv.routes()
	return srv
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /api/v1/batches", s.handleCreateBatch)
	s.mux.HandleFunc("GET /api/v1/batches/{id}", s.handleGetBatch)
	s.mux.HandleFunc("POST /api/v1/batches/{id}/chunks", s.handleSubmitChunk)
	s.mux.HandleFunc("POST /api/v1/batches/{id}/seal", s.handleSeal)
}

// Handler returns the root handler with request logging.
func (s *Server) Handler() http.Handler {
	return s.recoverPanic(s.logRequests(s.mux))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type createBatchRequest struct {
	ExpectedChunks int `json:"expectedChunks"`
}

func (s *Server) handleCreateBatch(w http.ResponseWriter, r *http.Request) {
	var req createBatchRequest
	if ok := decodeJSON(w, r, &req); !ok {
		return
	}
	if req.ExpectedChunks < 1 || req.ExpectedChunks > MaxExpectedChunks {
		writeError(w, http.StatusBadRequest, "INVALID_EXPECTED_CHUNKS",
			"expectedChunks must be an integer between 1 and 10000")
		return
	}
	b, err := s.store.CreateBatch(r.Context(), req.ExpectedChunks)
	if err != nil {
		s.log.Printf("create batch: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, batchJSON(b))
}

func (s *Server) handleSubmitChunk(w http.ResponseWriter, r *http.Request) {
	batchID, ok := batchIDFromPath(w, r)
	if !ok {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, MaxChunkBodyBytes)
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE",
				"payload must be at most 65536 UTF-8 bytes")
			return
		}
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST",
			"request body must be JSON with integer seq and string payload")
		return
	}

	payloadRaw, hasPayload := raw["payload"]
	seqRaw, hasSeq := raw["seq"]
	if !hasPayload || !hasSeq {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST",
			"seq and payload are required")
		return
	}
	var payload string
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PAYLOAD",
			"payload must be a string")
		return
	}
	var seq int
	if err := json.Unmarshal(seqRaw, &seq); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_SEQ",
			"seq must be an integer")
		return
	}
	payloadBytes := []byte(payload) // Go JSON strings are unescaped to raw UTF-8 bytes
	if len(payloadBytes) > MaxPayloadBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE",
			"payload must be at most 65536 UTF-8 bytes")
		return
	}
	if seq < 1 {
		writeError(w, http.StatusBadRequest, "INVALID_SEQ",
			"seq must be between 1 and expectedChunks")
		return
	}

	result, err := s.store.SubmitChunk(r.Context(), batchID, seq, payloadBytes)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "BATCH_NOT_FOUND", "batch does not exist")
	case errors.Is(err, store.ErrSeqRange):
		writeError(w, http.StatusBadRequest, "SEQ_OUT_OF_RANGE",
			"seq must be between 1 and expectedChunks")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "CHUNK_CONFLICT",
			"chunk seq already exists with a different payload")
	case errors.Is(err, store.ErrSealed):
		writeError(w, http.StatusConflict, "BATCH_SEALED",
			"batch is sealed; only byte-identical retransmission is allowed")
	case err != nil:
		s.log.Printf("submit chunk: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
	default:
		status := http.StatusCreated
		if !result.Created {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]any{
			"batchId":    batchID,
			"seq":        result.Seq,
			"size":       result.Size,
			"duplicate":  !result.Created,
			"receivedAt": result.ReceivedAt.UTC().Format(time.RFC3339Nano),
		})
	}
}

func (s *Server) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	batchID, ok := batchIDFromPath(w, r)
	if !ok {
		return
	}
	snap, err := s.store.Snapshot(r.Context(), batchID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "BATCH_NOT_FOUND", "batch does not exist")
		return
	}
	if err != nil {
		s.log.Printf("get batch: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, snapshotJSON(snap))
}

func (s *Server) handleSeal(w http.ResponseWriter, r *http.Request) {
	batchID, ok := batchIDFromPath(w, r)
	if !ok {
		return
	}
	snap, err := s.store.SealBatch(r.Context(), batchID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "BATCH_NOT_FOUND", "batch does not exist")
	case errors.Is(err, store.ErrIncomplete):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":    "INCOMPLETE",
			"message":  "batch is missing chunks",
			"batchId":  snap.ID,
			"status":   snap.Status,
			"expected": snap.ExpectedChunks,
			"received": snap.Received,
			"gaps":     snap.Gaps,
		})
	case err != nil:
		s.log.Printf("seal batch: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
	default:
		// Repeated seal of an already SEALED batch is idempotent.
		writeJSON(w, http.StatusOK, snapshotJSON(snap))
	}
}

// --- helpers ---

func batchIDFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if len(id) != 32 || strings.ToLower(id) != id || !isHex(id) {
		writeError(w, http.StatusNotFound, "BATCH_NOT_FOUND", "batch does not exist")
		return "", false
	}
	return id, true
}

func isHex(s string) bool {
	for _, c := range []byte(s) {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

func batchJSON(b *store.Batch) map[string]any {
	resp := map[string]any{
		"batchId":        b.ID,
		"expectedChunks": b.ExpectedChunks,
		"status":         b.Status,
		"createdAt":      b.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	return resp
}

func snapshotJSON(s *store.Snapshot) map[string]any {
	gaps := s.Gaps
	if gaps == nil {
		gaps = []int{}
	}
	resp := map[string]any{
		"batchId":  s.ID,
		"status":   s.Status,
		"expected": s.ExpectedChunks,
		"received": s.Received,
		"gaps":     gaps,
	}
	if s.SealedAt != nil {
		resp["sealedAt"] = s.SealedAt.UTC().Format(time.RFC3339Nano)
	}
	return resp
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", "request body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "malformed JSON request")
		return false
	}
	return true
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		s.log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, sw.status, time.Since(start).Round(time.Millisecond))
	})
}

func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Printf("panic: %v", rec)
				writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
