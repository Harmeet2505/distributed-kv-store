# Distributed Key-Value Store (C++, Raft)

A distributed, fault-tolerant key-value store built from scratch in C++ — implementing durable single-node storage, leader-follower replication, and a full Raft consensus protocol for automatic leader election and log replication.

Built to understand, hands-on, how systems like Redis, etcd, and CockroachDB actually work underneath their APIs — not to replace them.

## Why this exists

I use key-value stores and distributed databases constantly as a developer, but only ever at the `GET`/`SET` level — a black box underneath. This project was built to open that box: implement the actual mechanics of durability, replication, and consensus myself, and hit the real edge cases (race conditions, split-brain, log divergence, partial network failures) that theory alone glosses over.

## Architecture overview

Each node in the cluster runs the same binary and can be a **follower**, **candidate**, or **leader** at any time — roles are not fixed, and leadership changes automatically via Raft's election protocol.

```
Client → connects to whichever node is currently leader
Leader → appends write to its own log → replicates to followers → waits for majority ack → commits → applies to store
Followers → receive replicated entries → append to own log → apply to own store → ack
```

A client library (`kv_client`) holds the full list of cluster node addresses and automatically retries against other nodes if the one it's connected to is not the leader or becomes unreachable — so a leader failure does not require manual reconnection.

## Build phases

The project was built incrementally, each phase adding one layer of the "what happens when X fails" problem:

**Phase 1 — Single-node store**
- Thread-safe in-memory key-value store (`std::unordered_map` + `std::shared_mutex` reader-writer locking)
- Custom newline-delimited text protocol over raw TCP sockets (POSIX), similar in spirit to Redis's RESP protocol
- Multi-threaded server: one thread per client connection

**Phase 2 — Persistence**
- Write-ahead log (WAL): every write is logged to disk *before* being applied in memory, guaranteeing crash recovery
- Snapshotting: periodic compaction of the WAL into a full state dump (current key → value pairs only), keeping recovery time bounded regardless of total historical writes
- Crash-safe snapshotting via temp-file-then-atomic-rename, so a crash mid-snapshot can never corrupt state

**Phase 3 — Manual leader-follower replication**
- Leader forwards every write to all followers and waits for a majority acknowledgment before confirming to the client
- Hardened against slow/dead followers: acknowledgment timeouts, and a distinction between a broken connection (dropped permanently) versus a timeout (retried next round)
- Superseded by Raft in Phase 4, but kept in the codebase as it captures the exact data-replication mechanism Raft's `AppendEntries` RPC builds on

**Phase 4 — Raft consensus**
- Full leader election: randomized election timeouts, term numbers, majority voting, log-completeness checks before granting a vote
- Log replication via `AppendEntries`, including the consistency check (`prevLogIndex`/`prevLogTerm`) that repairs a follower's log after it diverges (e.g., after being disconnected during a leadership change)
- Automatic failover: losing any minority of nodes (e.g., 1 of 3) triggers automatic re-election and the cluster continues serving writes with no manual intervention
- Persistent `currentTerm`/`votedFor` state, surviving process crashes, to prevent double-voting in the same term after a restart

## Tech stack

- **C++20** — core implementation
- **CMake** — build system
- Raw **POSIX sockets** for all networking (client-facing and node-to-node) — no networking libraries, to demonstrate direct understanding of the socket API, threading model, and framing over a raw TCP byte stream

## Running a 3-node cluster locally

```bash
mkdir -p build data && cd build && cmake .. && make

# Terminal 1
./bin/kvstore --node-id=node1 --client-port=9090 --raft-port=9190 --total-nodes=3 --peers=node2:127.0.0.1:9191,node3:127.0.0.1:9192

# Terminal 2
./bin/kvstore --node-id=node2 --client-port=9091 --raft-port=9191 --total-nodes=3 --peers=node1:127.0.0.1:9190,node3:127.0.0.1:9192

# Terminal 3
./bin/kvstore --node-id=node3 --client-port=9092 --raft-port=9192 --total-nodes=3 --peers=node1:127.0.0.1:9190,node2:127.0.0.1:9191
```

One node will print `Became leader` within a few hundred milliseconds. Connect with the included client, which automatically finds and follows the current leader:

```bash
./bin/kv_client 127.0.0.1:9090,127.0.0.1:9091,127.0.0.1:9092
SET name harmeet
GET name
```

**Tested failure scenario:** kill whichever node is currently leader mid-session. The remaining two nodes elect a new leader automatically within ~300ms, and the client transparently reconnects to it without the user doing anything. Killing a second node (leaving only 1 of 3 alive) correctly makes the cluster refuse to elect a leader — since a minority partition can never safely accept writes without risking split-brain.

## Benchmarks

Measured on a 3-node local cluster (all nodes on localhost, no real network latency), 10 concurrent client threads, 5,000 requests per operation type.

| Operation | Throughput | Avg latency | p99 latency |
|---|---|---|---|
| `SET` (majority-replicated write) | ~18,200 ops/sec | 0.54ms | 1.69ms |
| `GET` (local read, no replication) | ~89,000 ops/sec | 0.11ms | 0.34ms |

**Two real bugs this benchmarking process caught, worth noting:**
- Initial `SET` latency measured ~55ms — tracing it down showed writes were only replicated on the next scheduled heartbeat tick (every 50ms) rather than immediately on write, since the leader passively waited for its heartbeat loop instead of proactively triggering replication. Fixing this to replicate immediately on every write dropped latency by roughly two orders of magnitude.
- A follow-up run under concurrent load showed duplicate entries appearing across node logs. This traced to multiple client threads calling into the same peer's `AppendEntries` send path concurrently, with no synchronization — interleaved writes on one shared socket could corrupt message framing between leader and follower. Fixed with a per-peer mutex serializing outbound replication, ensuring only one `AppendEntries` round-trip to a given peer is ever in flight at a time.

Verified correctness after the fix by diffing write-ahead logs across all three nodes post-benchmark (`sort wal.log | uniq -d` — confirmed no duplicate entries).

## Known limitations (deliberate, documented tradeoffs)

- **WAL durability uses `flush()`, not `fsync()`.** `flush()` guarantees data survives a process crash but not an OS-level crash or power loss before the OS itself writes buffered data to physical disk. A production version would use raw POSIX file descriptors with `fsync()` for the stronger guarantee — deferred to keep early phases focused, planned as a hardening pass.
- **Commit-index safety across terms is simplified.** Raft's full specification requires a leader to only advance its commit index using entries from its *own* current term, to handle a subtle edge case around old, uncommitted entries from a previous failed leader. The core mechanism here handles the common case correctly but doesn't implement every edge case in Raft's formal safety proof.
- **No log compaction/snapshotting at the Raft layer.** The KV store snapshots its own state (Phase 2), but the Raft log itself grows unboundedly over the life of a leader. A production system would periodically snapshot the Raft log too.

## What I'd add next

- Raw `fsync()`-based WAL writes for full crash durability
- Raft log snapshotting (independent of the KV store's own snapshotting)
- Dynamic cluster membership changes (adding/removing nodes without a restart)