package raft

import "time"

// HandleAppendEntries runs on a follower when the leader sends entries.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.term {
		return AppendEntriesReply{Term: n.term} // an old leader: refuse
	}
	if args.Term > n.term {
		n.becomeFollowerLocked(args.Term)
	} else if n.role != Follower {
		n.role = Follower // a leader exists for this term, so stop campaigning
	}
	n.leader = args.LeaderID
	n.resetDeadlineLocked()
	reply := AppendEntriesReply{Term: n.term}

	// Check 1: do I even have an entry at PrevLogIndex?
	last := n.lastIndexLocked()
	if args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1 // "my log is too short, start from here"
		return reply
	}
	// Check 2: does that entry have the term the leader expects?
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Skip back over the whole wrong term at once, not one entry at a time.
		conflictTerm := n.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 1 && n.log[i-1].Term == conflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// The logs match up to PrevLogIndex. Add the new entries, but only cut
	// my log on a REAL conflict: a delayed old message must not delete entries.
	changed := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + uint64(i)
		if index <= n.lastIndexLocked() {
			if n.log[index].Term == entry.Term {

				continue // no conflict: this entry is already in my log
			}
			n.log = n.log[:index] // conflict: delete it and everything after
		}
		n.log = append(n.log, copyEntries(args.Entries[i:])...)
		changed = true
		break
	}
	if changed {
		n.persistLocked()
	}

	// Commit, but only up to the last entry this message CHECKED.
	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if args.LeaderCommit > n.commit {
		n.commit = min(args.LeaderCommit, lastNew)
		n.applyCond.Broadcast()
	}
	reply.Success = true
	return reply
}

// peerLoop: one loop per follower. It sends when there's something new
// (notify) or when it's time for a heartbeat (ticker).
func (n *Node) peerLoop(peer int) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-n.notify[peer]:
		case <-ticker.C:
		}
		n.sendAppend(peer)
	}
}

// sendAppend sends one AppendEntries to one follower and handles the reply.
func (n *Node) sendAppend(peer int) {
	n.mu.Lock()
	if n.role != Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	prev := n.next[peer] - 1
	args := AppendEntriesArgs{
		Term: n.term, LeaderID: n.cfg.ID,
		PrevLogIndex: prev, PrevLogTerm: n.log[prev].Term,
		Entries:      copyEntries(n.log[prev+1:]), // everything from next onwards
		LeaderCommit: n.commit,
	}
	n.mu.Unlock() // never hold the lock during a network call

	ctx, cancel := n.rpcContext()
	defer cancel()
	reply, err := n.cfg.Transport.AppendEntries(ctx, peer, args)
	if err != nil {
		return // the heartbeat ticker will retry
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.term {
		n.becomeFollowerLocked(reply.Term) // there's a newer leader
		return
	}
	if n.role != Leader || n.term != args.Term {
		return // I'm not the leader of that term any more: ignore
	}
	if reply.Success {
		matched := args.PrevLogIndex + uint64(len(args.Entries))
		if matched > n.match[peer] {
			n.match[peer] = matched
		}
		n.next[peer] = n.match[peer] + 1
		n.advanceCommitLocked()
	} else {
		next := n.next[peer] - 1
		if reply.ConflictIndex > 0 {
			next = reply.ConflictIndex
		}
		n.next[peer] = max(1, min(next, n.lastIndexLocked()+1))
	}
	if n.next[peer] <= n.lastIndexLocked() {
		n.notifyLocked(peer) // still behind: send again straight away
	}
}

// advanceCommitLocked commits the highest index stored on a majority.
// It only counts entries from the CURRENT term (Raft's commitment rule).
func (n *Node) advanceCommitLocked() {
	for index := n.lastIndexLocked(); index > n.commit; index-- {
		if n.log[index].Term != n.term {
			break
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.match[p] >= index {
				count++
			}
		}
		if n.hasMajority(count) {
			n.commit = index
			n.applyCond.Broadcast()
			n.notifyAllLocked()
			return
		}
	}
}

// applyLoop hands committed entries to the key-value store, in order.
func (n *Node) applyLoop() {
	defer n.wg.Done()
	defer close(n.applyCh)
	for {
		n.mu.Lock()
		for !n.stopped && n.applied >= n.commit {
			n.applyCond.Wait() // sleep until something new is committed
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		start := n.applied + 1
		entries := copyEntries(n.log[start : n.commit+1])
		n.applied = n.commit
		n.mu.Unlock()

		for i, e := range entries {
			select {
			case n.applyCh <- ApplyMsg{Index: start + uint64(i), Term: e.Term, Command: e.Command}:
			case <-n.stop:
				return
			}
		}
	}
}
