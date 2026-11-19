package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// nCommitted reports how many nodes have applied index, and fails the test if
// two nodes applied different commands at the same index.
func (c *cluster) nCommitted(index uint64) (int, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd []byte
	for _, id := range c.ids {
		got, ok := c.applied[id][index]
		if !ok {
			continue
		}
		if count > 0 && !bytes.Equal(cmd, got) {
			c.t.Fatalf("index %d: nodes applied different commands %q and %q", index, cmd, got)
		}
		count++
		cmd = got
	}
	return count, cmd
}

// checkAgreement compares every index any node has applied.
func (c *cluster) checkAgreement() {
	c.t.Helper()
	c.mu.Lock()
	var highest uint64
	for _, m := range c.applied {
		for index := range m {
			highest = max(highest, index)
		}
	}
	c.mu.Unlock()
	for index := uint64(1); index <= highest; index++ {
		c.nCommitted(index)
	}
}

// one submits cmd to whichever node is leader and waits until at least
// `expected` nodes have applied it. It retries if leadership changes.
func (c *cluster) one(cmd string, expected int) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var index uint64
		found := false
		for _, id := range c.ids {
			if !c.isConnected(id) {
				continue
			}
			if i, _, err := c.node(id).Propose([]byte(cmd)); err == nil {
				index, found = i, true
				break
			}
		}
		if !found {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for wait := time.Now().Add(2 * time.Second); time.Now().Before(wait); {
			n, got := c.nCommitted(index)
			if n >= expected && string(got) == cmd {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	c.t.Fatalf("command %q was not committed on %d nodes", cmd, expected)
	return 0
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3)
	for i := 1; i <= 5; i++ {
		c.one(fmt.Sprintf("set x %d", i), 3)
	}
	c.checkAgreement()
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3)
	first := c.one("a", 3)
	leader := c.checkOneLeader()
	follower := leader%3 + 1

	// One follower is cut off. 2 of 3 is still a majority.
	c.disconnect(follower)
	c.one("b", 2)
	c.one("c", 2)

	// It comes back and must catch up on what it missed.
	c.connect(follower)
	last := c.one("d", 3)
	for index := first; index <= last; index++ {
		if n, _ := c.nCommitted(index); n != 3 && n != 0 {
			t.Fatalf("index %d is on %d nodes after catch-up, want 3", index, n)
		}
	}
	c.checkAgreement()
}

func TestNoCommitWithoutMajority(t *testing.T) {
	c := newCluster(t, 5)
	c.one("start", 5)
	leader := c.checkOneLeader()

	// Cut off 3 of the 4 followers: the leader is left with 2 of 5.
	cut := 0
	for _, id := range c.ids {
		if id != leader && cut < 3 {
			c.disconnect(id)
			cut++
		}
	}
	index, _, err := c.node(leader).Propose([]byte("lonely"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * testElectionTimeout)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("entry was committed by %d nodes without a majority", n)
	}

	for _, id := range c.ids {
		c.connect(id)
	}
	c.one("after", 5)
	c.checkAgreement()
}

// An old leader that wrote entries nobody else saw must have them replaced.
func TestOldLeaderEntriesReplaced(t *testing.T) {
	c := newCluster(t, 3)
	c.one("101", 3)
	leader1 := c.checkOneLeader()

	c.disconnect(leader1)
	for _, cmd := range []string{"lost-1", "lost-2", "lost-3"} {
		c.node(leader1).Propose([]byte(cmd)) // can never commit
	}

	c.one("103", 2) // the other two elect a new leader and carry on
	leader2 := c.checkOneLeader()

	c.disconnect(leader2)
	c.connect(leader1) // old leader + the remaining follower
	c.one("104", 2)

	c.connect(leader2)
	c.one("105", 3)
	c.checkAgreement()
	for index := uint64(1); index < 20; index++ {
		if _, cmd := c.nCommitted(index); bytes.HasPrefix(cmd, []byte("lost")) {
			t.Fatalf("uncommitted entry %q was applied at index %d", cmd, index)
		}
	}
}

func TestRestartAll(t *testing.T) {
	c := newCluster(t, 3)
	first := c.one("before-crash", 3)
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	c.one("after-crash", 3)
	if n, cmd := c.nCommitted(first); n != 3 || string(cmd) != "before-crash" {
		t.Fatalf("after restart index %d is %q on %d nodes", first, cmd, n)
	}
	c.checkAgreement()
}

// A delayed, older AppendEntries must not remove entries a follower already has.
func TestOldMessageDoesNotTruncate(t *testing.T) {
	disk := &MemoryStorage{}
	node := startAlone(t, disk)
	defer node.Stop()
	full := AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: []Entry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}, {Term: 1, Command: []byte("c")}}}
	if !node.HandleAppendEntries(full).Success {
		t.Fatal("first append failed")
	}
	old := AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: full.Entries[:1]}
	if !node.HandleAppendEntries(old).Success {
		t.Fatal("old append failed")
	}
	if got := node.Status().LastLogIndex; got != 3 {
		t.Fatalf("log length is %d after an old message, want 3", got)
	}
}

// Random disconnects, crashes and restarts while commands keep arriving.
func TestChaos(t *testing.T) {
	c := newCluster(t, 5)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	stopAt := time.Now().Add(4 * time.Second)
	for i := 0; time.Now().Before(stopAt); i++ {
		id := c.ids[rng.Intn(len(c.ids))]
		switch rng.Intn(4) {
		case 0:
			c.disconnect(id)
		case 1:
			c.connect(id)
		case 2:
			if c.node(id) != nil {
				c.crash(id)
			}
		case 3:
			if c.node(id) == nil {
				c.restart(id)
			}
		}
		for _, id := range c.ids {
			if c.isConnected(id) {
				c.node(id).Propose([]byte(fmt.Sprintf("cmd-%d", i)))
			}
		}
		time.Sleep(time.Duration(rng.Intn(80)) * time.Millisecond)
		c.checkAgreement()
	}
	for _, id := range c.ids {
		if c.node(id) == nil {
			c.restart(id)
		}
		c.connect(id)
	}
	c.one("final", 5)
	c.checkAgreement()
}

// A follower may only commit entries it has confirmed match the leader.
// Here entries 2 and 3 came from an old leader and are not checked yet.
func TestFollowerCommitsOnlyCheckedEntries(t *testing.T) {
	disk := &MemoryStorage{}
	disk.Save(HardState{Term: 1, VotedFor: -1, Log: []Entry{{Term: 1}, {Term: 1}, {Term: 1}}})
	node := startAlone(t, disk)
	defer node.Stop()
	heartbeat := AppendEntriesArgs{Term: 2, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3}
	if !node.HandleAppendEntries(heartbeat).Success {
		t.Fatal("heartbeat was rejected")
	}
	if got := node.Status().CommitIndex; got != 1 {
		t.Fatalf("follower committed up to %d, but only index 1 was checked", got)
	}
}

// A follower whose log does not match tells the leader where to retry.
func TestMismatchRejected(t *testing.T) {
	disk := &MemoryStorage{}
	disk.Save(HardState{Term: 3, VotedFor: -1, Log: []Entry{{Term: 1}, {Term: 2}, {Term: 2}}})
	node := startAlone(t, disk)
	defer node.Stop()
	reply := node.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 3})
	if reply.Success {
		t.Fatal("accepted entries although index 3 has term 2, not 3")
	}
	if reply.ConflictIndex != 2 {
		t.Fatalf("ConflictIndex = %d, want 2 (first index of term 2)", reply.ConflictIndex)
	}
	reply = node.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 2, PrevLogIndex: 9, PrevLogTerm: 3})
	if reply.Success || reply.ConflictIndex != 4 {
		t.Fatalf("got %+v for a gap, want failure with ConflictIndex 4", reply)
	}
}
