# distributed-kv-store

A replicated key-value store in Go, built on my own implementation of the
[Raft consensus algorithm](https://raft.github.io/raft.pdf). Three nodes keep
identical copies of the data. Writes are confirmed only once a majority has
them, and the cluster keeps working if one node fails.

Standard library only: no Raft or RPC frameworks.

## What it does

- **Leader election** with randomised timeouts, terms and the "up-to-date log" voting rule
- **Log replication** with the log-matching check, conflict skipping and the
  current-term commitment rule
- **Crash recovery:** term, vote and log are saved to disk (write to a temp
  file, fsync, atomic rename) before a node answers any request
- **Key-value store** on top of the log: `PUT`, `GET`, `DELETE`. Reads also go
  through the log, so they always see every committed write
- **HTTP API** with leader redirection: followers answer `421` and name the leader
- **`kvctl` client** that finds the leader and retries during elections

## Architecture

```
            kvctl put city London
                     |
                     v
   +---------- node 2 (leader) ----------+
   |  HTTP API -> kv.Store -> raft.Node  |
   +-------------------------------------+
        | AppendEntries      | AppendEntries
        v                    v
   node 1 (follower)    node 3 (follower)
   committed once 2 of 3 nodes have the entry
```

| Package | Purpose |
|---|---|
| `raft` | Elections, replication, commit and apply, persistence |
| `kv` | Applies committed commands to a map and wakes the waiting request |
| `server` | Raft RPCs and the client API over HTTP/JSON |
| `cmd/kvnode` | Runs one node |
| `cmd/kvctl` | Command-line client |

## Run a 3-node cluster (Windows PowerShell)

```powershell
go build -o bin\kvnode.exe .\cmd\kvnode
go build -o bin\kvctl.exe .\cmd\kvctl

$peers = "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
foreach ($i in 1,2,3) { Start-Process powershell -WorkingDirectory $PWD -ArgumentList "-NoExit","-Command",".\bin\kvnode.exe -id $i -peers $peers" }

.\bin\kvctl.exe status
.\bin\kvctl.exe put city London
.\bin\kvctl.exe get city
```

On Linux or macOS, start `./bin/kvnode -id N -peers ...` in three terminals.

## Failure experiment

Run on a laptop with three local processes:

| Step | Result |
|---|---|
| Start 3 nodes | Node 2 elected leader in term 1 |
| Write `city`, `food` | Committed on all 3 nodes |
| Close the leader | Node 3 elected in term 2; all data still readable |
| Write `city=Hyderabad` with a node down | Committed on the 2 remaining nodes |
| Restart the old leader | Rejoined as a follower and caught up (all at commit 7) |
| Leave only 1 of 3 nodes | Writes refused with `503`: no majority, no commit |

## Tests

```
go test -race ./...
```

- **Election:** a single leader, re-election after failure, no leader without a
  majority, a vote that survives a restart, the up-to-date log rule
- **Replication:** follower failure and catch-up, no commit without a
  majority, an isolated leader's uncommitted entries replaced, full-cluster
  restart, stale messages that must not truncate the log, followers only
  committing checked entries
- **Chaos:** 5 nodes with random disconnects, crashes and restarts while
  commands arrive, checking that no two nodes ever apply different commands
  at the same index
- **End to end:** 3 nodes over real HTTP, leader killed, data verified on the new leader

The tests use an in-memory network that can cut nodes off and adds random delays.

## Limitations

- **No log compaction or snapshots.** The log grows forever, and every save
  rewrites the whole state file.
- **No membership changes.** The cluster is fixed at start-up.
- **No CheckQuorum.** An isolated leader keeps its role. It can't commit
  anything, so it's safe, but clients wait for a timeout instead of being
  redirected quickly. I found this in the failure experiment.
- **Reads go through the log**, which is correct but slow. ReadIndex or lease
  reads would be faster.
- **No client request IDs for deduplication.** `PUT` and `DELETE` are
  idempotent, so retrying them is safe, but a non-idempotent operation would need them.
- No TLS or authentication, and tested only on one machine.

## Built with AI assistance

Built step by step with an AI assistant. I typed the election, replication and
key-value code and completed the key logic in each, ran the test suite and the
failure experiments, and can explain each part.
