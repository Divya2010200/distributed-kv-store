package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// network is a fake network for tests. It can cut a node off, and it adds
// small random delays so messages arrive in unpredictable orders.
type network struct {
	mu        sync.Mutex
	nodes     map[int]*Node
	connected map[int]bool
	rng       *rand.Rand
}

type netTransport struct {
	net  *network
	from int
}

var errUnreachable = errors.New("unreachable")

func (net *network) target(from, to int) (*Node, time.Duration) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delay := time.Duration(net.rng.Intn(5)) * time.Millisecond
	if !net.connected[from] || !net.connected[to] || net.nodes[to] == nil {
		return nil, delay
	}
	return net.nodes[to], delay
}

func (net *network) stillConnected(from, to int) bool {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.connected[from] && net.connected[to]
}

func (t netTransport) RequestVote(ctx context.Context, peer int, args RequestVoteArgs) (RequestVoteReply, error) {
	node, delay := t.net.target(t.from, peer)
	time.Sleep(delay)
	if node == nil {
		return RequestVoteReply{}, errUnreachable
	}
	reply := node.HandleRequestVote(args)
	if !t.net.stillConnected(t.from, peer) || ctx.Err() != nil {
		return RequestVoteReply{}, errUnreachable // the reply was lost
	}
	return reply, nil
}

func (t netTransport) AppendEntries(ctx context.Context, peer int, args AppendEntriesArgs) (AppendEntriesReply, error) {
	node, delay := t.net.target(t.from, peer)
	time.Sleep(delay)
	if node == nil {
		return AppendEntriesReply{}, errUnreachable
	}
	reply := node.HandleAppendEntries(args)
	if !t.net.stillConnected(t.from, peer) || ctx.Err() != nil {
		return AppendEntriesReply{}, errUnreachable
	}
	return reply, nil
}

const (
	testElectionTimeout = 150 * time.Millisecond
	testHeartbeat       = 30 * time.Millisecond
)

// cluster runs n nodes on the fake network and records what each applies.
type cluster struct {
	t       *testing.T
	ids     []int
	net     *network
	storage map[int]*MemoryStorage

	mu      sync.Mutex
	applied map[int]map[uint64][]byte // node -> index -> command
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		net:     &network{nodes: map[int]*Node{}, connected: map[int]bool{}, rng: rand.New(rand.NewSource(1))},
		storage: map[int]*MemoryStorage{},
		applied: map[int]map[uint64][]byte{},
	}
	for id := 1; id <= n; id++ {
		c.ids = append(c.ids, id)
		c.storage[id] = &MemoryStorage{}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) start(id int) {
	node, applyCh, err := Start(Config{
		ID: id, Peers: c.ids, Transport: netTransport{net: c.net, from: id},
		Storage: c.storage[id], ElectionTimeout: testElectionTimeout,
		Heartbeat: testHeartbeat, Seed: time.Now().UnixNano(),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.applied[id] = map[uint64][]byte{} // applied state is rebuilt after a restart
	c.mu.Unlock()
	c.net.mu.Lock()
	c.net.nodes[id] = node
	c.net.connected[id] = true
	c.net.mu.Unlock()
	go c.collect(id, applyCh)
}

func (c *cluster) collect(id int, applyCh <-chan ApplyMsg) {
	var last uint64
	for msg := range applyCh {
		if msg.Index != last+1 {
			c.t.Errorf("node %d applied index %d after %d", id, msg.Index, last)
		}
		last = msg.Index
		c.mu.Lock()
		c.applied[id][msg.Index] = msg.Command
		c.mu.Unlock()
	}
}

func (c *cluster) node(id int) *Node {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.nodes[id]
}

func (c *cluster) disconnect(id int) {
	c.net.mu.Lock()
	c.net.connected[id] = false
	c.net.mu.Unlock()
}

func (c *cluster) connect(id int) {
	c.net.mu.Lock()
	c.net.connected[id] = true
	c.net.mu.Unlock()
}

// crash stops a node. Its MemoryStorage is kept, like a disk.
func (c *cluster) crash(id int) {
	c.disconnect(id)
	node := c.node(id)
	node.Stop()
	c.net.mu.Lock()
	c.net.nodes[id] = nil
	c.net.mu.Unlock()
}

func (c *cluster) restart(id int) { c.start(id) }

func (c *cluster) shutdown() {
	for _, id := range c.ids {
		if node := c.node(id); node != nil {
			node.Stop()
		}
	}
}

func (c *cluster) isConnected(id int) bool {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.connected[id] && c.net.nodes[id] != nil
}

// checkOneLeader waits for exactly one leader among connected nodes and fails
// if two nodes ever claim leadership in the same term.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for try := 0; try < 20; try++ {
		time.Sleep(100 * time.Millisecond)
		leadersByTerm := map[uint64][]int{}
		for _, id := range c.ids {
			if !c.isConnected(id) {
				continue
			}
			if s := c.node(id).Status(); s.Role == "leader" {
				leadersByTerm[s.Term] = append(leadersByTerm[s.Term], id)
			}
		}
		var newestTerm uint64
		for term, leaders := range leadersByTerm {
			if len(leaders) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(leaders), leaders)
			}
			newestTerm = max(newestTerm, term)
		}
		if len(leadersByTerm) > 0 {
			return leadersByTerm[newestTerm][0]
		}
	}
	c.t.Fatal("no leader was elected")
	return -1
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for _, id := range c.ids {
		if c.isConnected(id) && c.node(id).Status().Role == "leader" {
			c.t.Fatalf("node %d is leader but should not be", id)
		}
	}
}

// checkTermsAgree returns the term shared by all connected nodes.
func (c *cluster) checkTermsAgree() uint64 {
	c.t.Helper()
	var term uint64
	for _, id := range c.ids {
		if !c.isConnected(id) {
			continue
		}
		t := c.node(id).Status().Term
		if term != 0 && t != term {
			c.t.Fatalf("nodes disagree on the term: %d vs %d", term, t)
		}
		term = t
	}
	return term
}
