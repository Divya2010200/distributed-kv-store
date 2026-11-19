package raft

import "errors"

// Entry is one record in the log. Each entry remembers the term
// (election round) in which the leader created it.
type Entry struct {
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

// ApplyMsg is sent to the key-value store once an entry is committed.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// A candidate asks for a vote.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  int    `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// The leader sends new entries (or an empty heartbeat).
type AppendEntriesArgs struct {
	Term         uint64  `json:"term"`
	LeaderID     int     `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index"`
}

// HardState is what MUST be saved to disk before a node answers anyone.
// If a node crashes and forgets these, it could vote twice in one
// election or lose confirmed writes.
type HardState struct {
	Term     uint64  `json:"term"`
	VotedFor int     `json:"voted_for"`
	Log      []Entry `json:"log"`
}

var ErrNotLeader = errors.New("raft: not the leader")
