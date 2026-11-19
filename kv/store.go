// Package kv is a key-value store whose every operation goes through Raft.
package kv

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"raftkv/raft"
)

var (
	ErrNotLeader     = raft.ErrNotLeader
	ErrLeaderChanged = errors.New("kv: leadership changed before the operation was committed")
	ErrTimeout       = errors.New("kv: operation was not committed in time")
	ErrStopped       = errors.New("kv: store stopped")
)

// Command is what gets written into the Raft log.
type Command struct {
	Op    string `json:"op"` // "put", "delete" or "get"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	ID    string `json:"id"` // tells us if the entry at our index is really ours
}

type result struct {
	id    string
	value string
	found bool
}

type Store struct {
	node    *raft.Node
	timeout time.Duration

	mu      sync.Mutex
	data    map[string]string      // the actual key-value data
	waiters map[uint64]chan result // log index -> request waiting for it
	stopped bool
}

func New(node *raft.Node, applyCh <-chan raft.ApplyMsg, timeout time.Duration) *Store {
	s := &Store{node: node, timeout: timeout, data: map[string]string{}, waiters: map[uint64]chan result{}}
	go s.run(applyCh)
	return s
}

// run applies every committed command, in order, and wakes up whoever
// is waiting for that index.
func (s *Store) run(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		s.mu.Lock()
		res := s.applyLocked(msg)
		if ch, ok := s.waiters[msg.Index]; ok { // someone is waiting for this index
			ch <- res
			delete(s.waiters, msg.Index)
		}
		s.mu.Unlock()
	}
	// Raft stopped: wake everyone still waiting.
	s.mu.Lock()
	s.stopped = true
	for index, ch := range s.waiters {
		close(ch)
		delete(s.waiters, index)
	}
	s.mu.Unlock()
}

func (s *Store) applyLocked(msg raft.ApplyMsg) result {
	if msg.Command == nil {
		return result{} // a new leader's no-op: nothing to do
	}
	var cmd Command
	if err := json.Unmarshal(msg.Command, &cmd); err != nil {
		return result{} // every node skips the same bad entry, so they stay equal
	}
	switch cmd.Op {
	case "put":
		s.data[cmd.Key] = cmd.Value
	case "delete":
		delete(s.data, cmd.Key)
	}
	value, found := s.data[cmd.Key]
	return result{id: cmd.ID, value: value, found: found}
}

func (s *Store) Put(key, value string) error {
	_, err := s.submit(Command{Op: "put", Key: key, Value: value})
	return err
}

func (s *Store) Delete(key string) error {
	_, err := s.submit(Command{Op: "delete", Key: key})
	return err
}

// Get also goes through the log. Slower than reading local memory, but the
// answer is guaranteed to include every write committed before it.
func (s *Store) Get(key string) (string, bool, error) {
	res, err := s.submit(Command{Op: "get", Key: key})
	return res.value, res.found, err
}

func (s *Store) submit(cmd Command) (result, error) {
	cmd.ID = newID()
	data, err := json.Marshal(cmd)
	if err != nil {
		return result{}, err
	}

	// Hold s.mu while proposing, so the entry can't be applied
	// before our waiter is registered.
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return result{}, ErrStopped
	}
	index, _, err := s.node.Propose(data)
	if err != nil {
		s.mu.Unlock()
		return result{}, err
	}
	ch := make(chan result, 1)
	s.waiters[index] = ch
	s.mu.Unlock()

	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	select {
	case res, ok := <-ch:
		if !ok {
			return result{}, ErrStopped
		}
		if res.id != cmd.ID {
			return result{}, ErrLeaderChanged
		}
		return res, nil
	case <-timer.C:
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return result{}, ErrTimeout
	}
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("kv: random id: %v", err))
	}
	return hex.EncodeToString(b)
}
