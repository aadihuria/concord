# concord

a raft-replicated, crdt-merged shared-state store for multi-agent ai systems.

## the problem

research analyzing 1,600+ multi-agent execution traces across autogen, crewai, and langgraph found that **interagent misalignment — agents operating on inconsistent views of shared state — causes 36.9% of all multi-agent failures**, the single largest failure category.

existing frameworks patch around this with crude mechanisms: langgraph uses deterministic reducer functions, crewai does similarity-threshold deduplication. neither gives formal correctness guarantees, and both degrade under true parallel execution.

## what concord does

raft gives crash-durable, linearizable replication of the log. a crdt layer sitting on top of that log gives **provably-correct, lock-free merges** for concurrent writes — agents don't wait on each other, and the merged result is guaranteed to converge no matter the write order.

this is a formal correctness story, not a similarity heuristic.

### the hybrid cp/ap tradeoff

raft alone is a cp protocol — it sacrifices availability during elections/partitions to preserve strict consistency. concord's crdt layer introduces ap-style flexibility within that framework: concurrent writes in the same term don't block on each other or require cross-node locking, because the merge function is commutative, associative, and idempotent by construction.

## architecture

```
 Client SDK (Python via PyO3, Rust native)
            │
            ▼
     gRPC Client API  (Get / Put / Subscribe)
            │
            ▼
   ┌───────────────────┐
   │   Raft Consensus   │  ← leader election, log replication,
   │   (per-node)       │    snapshot/compaction
   └───────────────────┘
            │
            ▼
   ┌───────────────────┐
   │   CRDT Merge Layer │  ← LWW-Register, OR-Set, RGA Sequence
   └───────────────────┘
            │
            ▼
   ┌───────────────────┐
   │  Storage Engine    │  ← CRC32-checksummed WAL + snapshots
   └───────────────────┘
```

each node runs the full stack. client writes go to the current leader, get replicated via raft's appendentries rpc, and are applied to the crdt state machine once committed. reads can be served from any node.

## crdt types

| type | use case | merge semantics |
|------|----------|-----------------|
| **lww-register** | scalar state (agent status, current task) | highest (timestamp, node_id) wins |
| **or-set** | accumulating facts, completed subtasks | add-wins on concurrent add/remove via unique tags |
| **rga sequence** | ordered plan steps, execution traces | causal ordering with deterministic interleaving |

all three are verified with property-based tests (proptest) proving commutativity, associativity, and idempotency.

## benchmarks

```
write throughput:
  3-node cluster, 10k writes: 202,771 ops/sec (4.9µs/op)
  5-node cluster, 10k writes: 137,332 ops/sec (7.3µs/op)

read throughput:
  1M reads: 54,324,821 ops/sec

failover:
  leader recovery: <1ms (2 message rounds)
```

## quickstart

### running a 3-node cluster

```bash
# terminal 1
cargo run -p concord-server -- --id node-0 --addr 127.0.0.1:50051 \
  --peers node-1=127.0.0.1:50052,node-2=127.0.0.1:50053

# terminal 2
cargo run -p concord-server -- --id node-1 --addr 127.0.0.1:50052 \
  --peers node-0=127.0.0.1:50051,node-2=127.0.0.1:50053

# terminal 3
cargo run -p concord-server -- --id node-2 --addr 127.0.0.1:50053 \
  --peers node-0=127.0.0.1:50051,node-1=127.0.0.1:50052
```

### running benchmarks

```bash
cargo run --release -p concord-bench --bin throughput
```

### running tests

```bash
# all tests
cargo test

# property-based crdt tests
cargo test -p concord-crdt --test proptests

# raft cluster integration tests
cargo test -p concord-raft --test cluster
```

## repo structure

```
concord/
  crates/
    storage/     — wal, in-memory store, snapshot manager
    crdt/        — lww-register, or-set, rga sequence
    raft/        — leader election, log replication, state machine
    proto/       — grpc protobuf definitions
    server/      — ties raft + crdt + storage + grpc together
  benches/       — throughput and failover benchmarks
  docs/
    design.md    — crdt + raft design rationale
```

## tech stack

| layer | choice | why |
|-------|--------|-----|
| core | rust | memory safety + performance |
| rpc | grpc (tonic) | standard for inter-service communication |
| consensus | hand-rolled raft | the from-scratch implementation is the depth signal |
| crdts | hand-rolled | no existing library targets agent-state shapes |
| testing | proptest | property-based verification of crdt convergence |

## license

mit
