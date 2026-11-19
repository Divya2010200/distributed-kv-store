package raft

import "time"

// HandleRequestVote runs on a node that is asked for its vote.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	// A newer term exists: move into it as a follower (this clears my vote).
	if args.Term > n.term {
		n.becomeFollowerLocked(args.Term)
	}
	reply := RequestVoteReply{Term: n.term}
	if args.Term < n.term { // candidate is from an OLD term
		return reply
	}
	// Rule 3: is the candidate's log at least as up to date as mine?
	lastTerm := n.log[n.lastIndexLocked()].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= n.lastIndexLocked())
	// Rule 2: haven't voted yet, or already voted for this same candidate.
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.persistLocked() // save the vote BEFORE replying
		n.resetDeadlineLocked()
		reply.VoteGranted = true
	}
	return reply
}

// electionLoop checks the timer 10 times per timeout period.
func (n *Node) electionLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.ElectionTimeout / 10)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
		}
		n.mu.Lock()
		if n.role != Leader && time.Now().After(n.deadline) {
			n.startElectionLocked()
		}
		n.mu.Unlock()
	}
}

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leader = -1
	n.persistLocked()
	n.resetDeadlineLocked()

	term := n.term
	votes := 1
	if n.hasMajority(votes) { // a 1-node cluster wins at once
		n.becomeLeaderLocked()
		return
	}
	args := RequestVoteArgs{
		Term: term, CandidateID: n.cfg.ID,
		LastLogIndex: n.lastIndexLocked(), LastLogTerm: n.log[n.lastIndexLocked()].Term,
	}
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.wg.Add(1)
		go func(peer int) { // ask each peer in parallel
			defer n.wg.Done()
			ctx, cancel := n.rpcContext()
			defer cancel()
			reply, err := n.cfg.Transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.term {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			// Ignore late replies from an election that's already over.
			if n.role != Candidate || n.term != term || !reply.VoteGranted {
				return
			}
			votes++
			if n.hasMajority(votes) {
				n.becomeLeaderLocked()
			}
		}(peer)
	}
}

func (n *Node) becomeFollowerLocked(term uint64) {
	n.role = Follower
	n.term = term
	n.votedFor = -1
	n.leader = -1
	n.persistLocked()
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leader = n.cfg.ID
	// A no-op entry in the new term (explained in Step 4).
	n.log = append(n.log, Entry{Term: n.term})
	n.persistLocked()
	last := n.lastIndexLocked()
	for _, p := range n.cfg.Peers {
		n.next[p] = last + 1
		n.match[p] = 0
	}
	n.match[n.cfg.ID] = last
	n.advanceCommitLocked()
	n.notifyAllLocked()
}
