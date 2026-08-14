# design rationale

## why raft + crdts together

the standard approach to distributed consensus is to choose either cp (raft, paxos) or ap (crdts, dynamo-style). concord deliberately layers both:

- **raft** provides a totally-ordered, crash-durable log. every committed entry is guaranteed to be replicated to a majority. this gives us strong consistency for the ordering of operations and durability across node failures.

- **crdts** sit on top of the committed log as the state machine. this matters because it makes the system robust to a specific class of issues that raw raft doesn't handle well: **concurrent proposals within the same term**.

when two agents submit writes to the leader simultaneously, raft serializes them into the log in some order. but the agents don't know which order. with a plain key-value store, the second write blindly overwrites the first. with crdts, both writes are preserved and merged according to mathematically-proven merge semantics.

## crdt type selection

### lww-register

used for scalar agent state: "agent-3 is currently running task X."

merge rule: highest (timestamp, node_id) wins. the node_id tiebreaker ensures a total order even with synchronized clocks, making convergence deterministic.

the main tradeoff: lww discards the "losing" write entirely. this is correct for status fields where only the latest value matters, but wrong for accumulative state. that's what the other two types are for.

### or-set (observed-remove set)

used for accumulating facts: completed subtasks, discovered entities, tool call results.

the key insight: every `add` operation generates a globally unique tag (uuid + node_id). a `remove` only deletes the tags that were *observed* at remove time. so if node A adds "task-1" and node B concurrently removes "task-1," node A's add survives — because its tag was never observed by B's remove.

this eliminates the "zombie re-add" problem in plain grow-only sets and the "lost add" problem in naive add/remove sets.

### rga sequence (replicated growable array)

used for ordered plan steps: "first search, then analyze, then report."

each element has a unique id (timestamp, node_id) and references a parent (the element it was inserted after). when concurrent inserts target the same parent, the one with the higher id sorts first. the key correctness property: this ordering function is deterministic regardless of the order nodes apply operations, which is what makes merge commutative.

the implementation uses tombstones for deletion rather than physical removal, preserving the causal structure needed for correct merging across replicas.

## raft implementation details

### leader election

randomized timeouts between 150-300ms prevent election storms. a candidate increments its term, votes for itself, and broadcasts RequestVote RPCs. the election restriction (§5.4.1 in the raft paper) ensures a candidate's log is at least as up-to-date as any voter's, preventing committed entries from being overwritten.

### log replication

the leader maintains a `next_index` and `match_index` per follower. on conflict (term mismatch at prev_log_index), the follower rejects and the leader decrements next_index. this is the simple O(n) backtracking approach rather than the optimized batch rollback — simpler to verify correct.

### commit advancement

an entry is committed once replicated to a majority. critically: only entries from the *current* term advance the commit index (§5.4.2). entries from previous terms are committed indirectly when a current-term entry that follows them is committed. the initial noop entry after election handles this.

### snapshot and log compaction

snapshots serialize the full crdt state machine. when a follower is too far behind (its next_index is before the snapshot point), the leader sends an InstallSnapshot RPC instead of individual log entries.

## why hand-rolled instead of a library

using a raft library (e.g., `raft-rs`, `openraft`) would reduce the implementation to glue code. the value of this project is demonstrating understanding of:

1. how leader election prevents split-brain
2. how the log consistency check guarantees linearizability
3. how the commit rule (majority + current term) prevents stale data
4. where the cp/ap tradeoff sits and how crdts relax it

you can't explain these in an interview by saying "I called a library function."

## what the crdt layer buys on top of raft

raft alone handles the "two agents write different keys simultaneously" case fine — the leader serializes them. but consider:

- agent A writes `plan.steps[2].status = "complete"` 
- agent B writes `plan.steps[3].status = "in-progress"`

with a naive state machine, the second write might clobber the first if they target the same document. with crdts, both updates merge correctly because they operate on different parts of the state via structurally-aware merge functions.

the crdt guarantee — commutativity, associativity, idempotency — means that even if log replay happens in a different order (e.g., after a snapshot restore), the final state is identical.
