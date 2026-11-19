package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"raftkv/raft"
)

type noNetwork struct{}

func (noNetwork) RequestVote(context.Context, int, raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	return raft.RequestVoteReply{}, errors.New("no network")
}
func (noNetwork) AppendEntries(context.Context, int, raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	return raft.AppendEntriesReply{}, errors.New("no network")
}

func start(t *testing.T, peers []int) *Store {
	t.Helper()
	node, applyCh, err := raft.Start(raft.Config{
		ID: 1, Peers: peers, Transport: noNetwork{}, Storage: &raft.MemoryStorage{},
		ElectionTimeout: 50 * time.Millisecond, Heartbeat: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Stop)
	return New(node, applyCh, time.Second)
}

func TestPutGetDelete(t *testing.T) {
	s := start(t, []int{1}) // a one-node cluster elects itself
	time.Sleep(200 * time.Millisecond)
	if err := s.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := s.Get("a"); err != nil || !ok || v != "1" {
		t.Fatalf("Get = %q %v %v", v, ok, err)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Get("a"); err != nil || ok {
		t.Fatalf("after delete: found=%v err=%v", ok, err)
	}
}

func TestNotLeader(t *testing.T) {
	s := start(t, []int{1, 2, 3}) // no network, so it can never win
	if err := s.Put("a", "1"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v, want ErrNotLeader", err)
	}
}
