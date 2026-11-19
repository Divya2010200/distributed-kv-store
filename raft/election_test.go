package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()

	// Followers hear heartbeats, so nobody should start a new election.
	time.Sleep(100 * time.Millisecond)
	term1 := c.checkTermsAgree()
	if term1 < 1 {
		t.Fatalf("term is %d, expected at least 1", term1)
	}
	time.Sleep(2 * testElectionTimeout * 3)
	if term2 := c.checkTermsAgree(); term2 != term1 {
		t.Fatalf("term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	leader1 := c.checkOneLeader()

	// The leader is cut off, so the other two must elect a new one.
	c.disconnect(leader1)
	leader2 := c.checkOneLeader()
	if leader2 == leader1 {
		t.Fatal("the disconnected node is still the only leader")
	}

	// The old leader comes back. It sees a higher term and steps down.
	c.connect(leader1)
	c.checkOneLeader()

	// With 2 of 3 nodes gone there is no majority, so no leader.
	leader3 := c.checkOneLeader()
	other := 1
	for other == leader3 {
		other++
	}
	c.disconnect(leader3)
	c.disconnect(other)
	time.Sleep(4 * testElectionTimeout)
	c.checkNoLeader()

	// One comes back: a majority again, so a leader again.
	c.connect(other)
	c.checkOneLeader()
}

// deadTransport fails every call, so a single node can be tested on its own.
type deadTransport struct{}

func (deadTransport) RequestVote(context.Context, int, RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, errors.New("no network")
}
func (deadTransport) AppendEntries(context.Context, int, AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, errors.New("no network")
}

func startAlone(t *testing.T, storage Storage) *Node {
	t.Helper()
	node, _, err := Start(Config{
		ID: 1, Peers: []int{1, 2, 3}, Transport: deadTransport{}, Storage: storage,
		ElectionTimeout: 10 * time.Second, Heartbeat: time.Second, // never times out during the test
	})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

// A node must never vote twice in one term, even if it restarts in between.
func TestVoteSurvivesRestart(t *testing.T) {
	disk := &MemoryStorage{}
	node := startAlone(t, disk)
	if !node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 2}).VoteGranted {
		t.Fatal("first vote in term 5 should be granted")
	}
	node.Stop()

	node = startAlone(t, disk) // "restart" with the same disk
	defer node.Stop()
	if node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 3}).VoteGranted {
		t.Fatal("voted twice in term 5: two leaders would be possible")
	}
	if !node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 2}).VoteGranted {
		t.Fatal("repeating the same vote (a resent request) should be allowed")
	}
}

// A candidate whose log is behind must not win, or it could erase committed data.
func TestCandidateLogMustBeUpToDate(t *testing.T) {
	disk := &MemoryStorage{}
	disk.Save(HardState{Term: 2, VotedFor: -1, Log: []Entry{{Term: 1}, {Term: 2}}})
	node := startAlone(t, disk)
	defer node.Stop()

	behind := RequestVoteArgs{Term: 3, CandidateID: 2, LastLogIndex: 9, LastLogTerm: 1}
	if node.HandleRequestVote(behind).VoteGranted {
		t.Fatal("granted a vote to a candidate with an older last term")
	}
	shorter := RequestVoteArgs{Term: 4, CandidateID: 3, LastLogIndex: 1, LastLogTerm: 2}
	if node.HandleRequestVote(shorter).VoteGranted {
		t.Fatal("granted a vote to a candidate with a shorter log in the same term")
	}
	ok := RequestVoteArgs{Term: 5, CandidateID: 2, LastLogIndex: 2, LastLogTerm: 2}
	if !node.HandleRequestVote(ok).VoteGranted {
		t.Fatal("refused a candidate whose log is as up to date")
	}
}

// A request from an old term is refused and tells the sender the newer term.
func TestOldTermRejected(t *testing.T) {
	disk := &MemoryStorage{}
	disk.Save(HardState{Term: 7, VotedFor: -1})
	node := startAlone(t, disk)
	defer node.Stop()
	reply := node.HandleRequestVote(RequestVoteArgs{Term: 6, CandidateID: 2})
	if reply.VoteGranted || reply.Term != 7 {
		t.Fatalf("got %+v, want a refusal carrying term 7", reply)
	}
}
