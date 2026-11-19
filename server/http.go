// Package server exposes a node over HTTP: Raft RPCs between nodes, and a
// small key-value API for clients.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"raftkv/kv"
	"raftkv/raft"
)

// HTTPTransport sends Raft RPCs as JSON over HTTP.
type HTTPTransport struct {
	Peers  map[int]string // node ID -> "host:port"
	Client *http.Client
}

func NewHTTPTransport(peers map[int]string) *HTTPTransport {
	return &HTTPTransport{Peers: peers, Client: &http.Client{}}
}

func (t *HTTPTransport) RequestVote(ctx context.Context, peer int, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	err := t.post(ctx, peer, "/raft/vote", args, &reply)
	return reply, err
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, peer int, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	err := t.post(ctx, peer, "/raft/append", args, &reply)
	return reply, err
}

func (t *HTTPTransport) post(ctx context.Context, peer int, path string, args, reply any) error {
	addr, ok := t.Peers[peer]
	if !ok {
		return fmt.Errorf("unknown peer %d", peer)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer %d returned %s", peer, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}

// Server handles both Raft traffic and client requests for one node.
type Server struct {
	Node  *raft.Node
	Store *kv.Store
	Peers map[int]string
}

const maxValueBytes = 1 << 20 // 1 MiB

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var args raft.RequestVoteArgs
		if !decode(w, r, &args) {
			return
		}
		writeJSON(w, http.StatusOK, s.Node.HandleRequestVote(args))
	})
	mux.HandleFunc("POST /raft/append", func(w http.ResponseWriter, r *http.Request) {
		var args raft.AppendEntriesArgs
		if !decode(w, r, &args) {
			return
		}
		writeJSON(w, http.StatusOK, s.Node.HandleAppendEntries(args))
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Node.Status())
	})
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, found, err := s.Store.Get(r.PathValue("key"))
		if err != nil {
			s.writeError(w, err)
			return
		}
		if !found {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, value)
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
		if err != nil {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "value too large"})
			return
		}
		if err := s.Store.Put(r.PathValue("key"), string(value)); err != nil {
			s.writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Delete(r.PathValue("key")); err != nil {
			s.writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// writeError maps store errors to HTTP. A follower answers 421 and names the
// leader, so the client can retry there.
func (s *Server) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, kv.ErrNotLeader):
		body := map[string]string{"error": "not the leader"}
		if leader := s.Node.Status().Leader; leader > 0 {
			body["leader"] = s.Peers[leader]
		}
		writeJSON(w, http.StatusMisdirectedRequest, body)
	case errors.Is(err, kv.ErrLeaderChanged), errors.Is(err, kv.ErrTimeout), errors.Is(err, kv.ErrStopped):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(v); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// Timeouts used by the real nodes. HTTP between processes is slower than the
// in-memory test network, so these are longer than in the raft tests.
const (
	ElectionTimeout = 300 * time.Millisecond
	Heartbeat       = 50 * time.Millisecond
	RequestTimeout  = 3 * time.Second
)
