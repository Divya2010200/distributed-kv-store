// Package raft implements leader election and log replication from the Raft
// consensus algorithm. Log compaction and membership changes are not included.
package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

// Transport sends RPCs to other nodes. Implementations must respect ctx.
type Transport interface {
	RequestVote(ctx context.Context, peer int, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer int, args AppendEntriesArgs) (AppendEntriesReply, error)
}

type Config struct {
	ID int
	// Peers lists every node in the cluster, including this one.
	Peers     []int
	Transport Transport
	Storage   Storage
	// ElectionTimeout is the minimum; each wait is random in [T, 2T).
	ElectionTimeout time.Duration
	Heartbeat       time.Duration
	Seed            int64
}

type Status struct {
	ID           int    `json:"id"`
	Role         string `json:"role"`
	Term         uint64 `json:"term"`
	Leader       int    `json:"leader"`
	CommitIndex  uint64 `json:"commit_index"`
	LastApplied  uint64 `json:"last_applied"`
	LastLogIndex uint64 `json:"last_log_index"`
}

type Node struct {
	cfg Config

	mu       sync.Mutex
	role     Role
	term     uint64
	votedFor int     // -1 means no vote in this term
	log      []Entry // log[0] is a sentinel with term 0
	commit   uint64
	applied  uint64
	leader   int // -1 when unknown
	deadline time.Time
	next     map[int]uint64
	match    map[int]uint64
	rng      *rand.Rand
	stopped  bool

	applyCond *sync.Cond
	applyCh   chan ApplyMsg
	notify    map[int]chan struct{}
	stop      chan struct{}
	wg        sync.WaitGroup
}

// Start loads saved state and starts the node. Committed entries arrive on the
// returned channel, which is closed when the node stops.
func Start(cfg Config) (*Node, <-chan ApplyMsg, error) {
	if cfg.ElectionTimeout <= 0 || cfg.Heartbeat <= 0 || cfg.Heartbeat >= cfg.ElectionTimeout {
		return nil, nil, fmt.Errorf("raft: heartbeat must be positive and shorter than the election timeout")
	}
	state, found, err := cfg.Storage.Load()
	if err != nil {
		return nil, nil, err
	}
	n := &Node{
		cfg:      cfg,
		votedFor: -1,
		leader:   -1,
		log:      []Entry{{Term: 0}},
		next:     map[int]uint64{},
		match:    map[int]uint64{},
		rng:      rand.New(rand.NewSource(cfg.Seed + int64(cfg.ID))),
		applyCh:  make(chan ApplyMsg, 256),
		notify:   map[int]chan struct{}{},
		stop:     make(chan struct{}),
	}
	if found {
		n.term, n.votedFor = state.Term, state.VotedFor
		n.log = append(n.log, state.Log...)
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.resetDeadlineLocked()

	// Create every channel before starting any goroutine that reads the map.
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.notify[p] = make(chan struct{}, 1)
		}
	}
	for p := range n.notify {
		n.wg.Add(1)
		go n.peerLoop(p)
	}
	n.wg.Add(2)
	go n.electionLoop()
	go n.applyLoop()
	return n, n.applyCh, nil
}

// Stop halts the node and waits for its goroutines.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stop)
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// Propose appends a command if this node is the leader. A returned index is
// not a promise: the entry may still be lost if leadership changes.
func (n *Node) Propose(command []byte) (index, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.stopped {
		return 0, 0, ErrNotLeader
	}
	n.log = append(n.log, Entry{Term: n.term, Command: append([]byte(nil), command...)})
	n.persistLocked()
	n.match[n.cfg.ID] = n.lastIndexLocked()
	n.advanceCommitLocked()
	n.notifyAllLocked()
	return n.lastIndexLocked(), n.term, nil
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.cfg.ID, Role: n.role.String(), Term: n.term, Leader: n.leader,
		CommitIndex: n.commit, LastApplied: n.applied, LastLogIndex: n.lastIndexLocked(),
	}
}

// --- helpers ----------------------------------------------------------------

func (n *Node) lastIndexLocked() uint64 { return uint64(len(n.log) - 1) }

func (n *Node) hasMajority(count int) bool { return count*2 > len(n.cfg.Peers) }

func (n *Node) resetDeadlineLocked() {
	jitter := time.Duration(n.rng.Int63n(int64(n.cfg.ElectionTimeout)))
	n.deadline = time.Now().Add(n.cfg.ElectionTimeout + jitter)
}

func (n *Node) rpcContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
}

func (n *Node) notifyLocked(peer int) {
	select {
	case n.notify[peer] <- struct{}{}:
	default:
	}
}

func (n *Node) notifyAllLocked() {
	for p := range n.notify {
		n.notifyLocked(p)
	}
}

// persistLocked saves state before the node acts on it. If the disk fails the
// node stops (fail-stop) rather than risk breaking Raft's guarantees.
func (n *Node) persistLocked() {
	state := HardState{Term: n.term, VotedFor: n.votedFor, Log: n.log[1:]}
	if err := n.cfg.Storage.Save(state); err != nil {
		panic(fmt.Sprintf("raft: node %d cannot persist state: %v", n.cfg.ID, err))
	}
}

func copyEntries(entries []Entry) []Entry {
	out := make([]Entry, len(entries))
	for i, e := range entries {
		out[i] = Entry{Term: e.Term, Command: append([]byte(nil), e.Command...)}
	}
	return out
}
