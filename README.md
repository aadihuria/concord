# Concord

A Raft-replicated, CRDT-merged shared-state store for multi-agent AI systems.

[![CI](https://github.com/aadihuria/concord/actions/workflows/ci.yml/badge.svg)](https://github.com/aadihuria/concord/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## The Problem

Research analyzing 1,600+ multi-agent execution traces across AutoGen, CrewAI, and LangGraph found that **interagent misalignment — agents operating on inconsistent views of shared state — causes 36.9% of all multi-agent failures**, the single largest failure category.

Existing frameworks patch around this with crude mechanisms: LangGraph uses deterministic reducer functions, CrewAI does similarity-threshold deduplication. Neither gives formal correctness guarantees, and both degrade under true parallel execution.

## What Concord Does

Raft gives crash-durable, linearizable replication of the log. A CRDT layer sitting on top of that log gives **provably-correct, lock-free merges** for concurrent writes — agents don't wait on each other, and the merged result is guaranteed to converge no matter the write order.

This is a formal correctness story, not a similarity heuristic.

### The Hybrid CP/AP Tradeoff

Raft alone is a CP protocol — it sacrifices availability during elections/partitions to preserve strict consistency. Concord's CRDT layer introduces AP-style flexibility within that framework: concurrent writes in the same term don't block on each other or require cross-node locking, because the merge function is commutative, associative, and idempotent by construction.

## Architecture

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

Each node runs the full stack. Client writes go to the current leader, get replicated via Raft's AppendEntries RPC, and are applied to the CRDT state machine once committed. Reads can be served from any node.

## CRDT Types

| Type | Use Case | Merge Semantics |
|------|----------|-----------------|
| **LWW-Register** | Scalar state (agent status, current task) | Highest (timestamp, node_id) wins |
| **OR-Set** | Accumulating facts, completed subtasks | Add-wins on concurrent add/remove via unique tags |
| **RGA Sequence** | Ordered plan steps, execution traces | Causal ordering with deterministic interleaving |

All three are verified with property-based tests (proptest) proving commutativity, associativity, and idempotency.

## Benchmarks

```
Write throughput:
  3-node cluster, 10k writes: 202,771 ops/sec (4.9µs/op)
  5-node cluster, 10k writes: 137,332 ops/sec (7.3µs/op)

Read throughput:
  1M reads: 54,324,821 ops/sec

Failover:
  Leader recovery: <1ms (2 message rounds)
```

## Quickstart

### Option 1: Docker Compose (recommended)

```bash
docker compose up --build
```

This starts a 3-node cluster. The leader is accessible at `localhost:50051`.

### Option 2: Run from source

```bash
# Terminal 1
cargo run -p concord-server -- --id node-0 --addr 127.0.0.1:50051 \
  --peers node-1=127.0.0.1:50052,node-2=127.0.0.1:50053

# Terminal 2
cargo run -p concord-server -- --id node-1 --addr 127.0.0.1:50052 \
  --peers node-0=127.0.0.1:50051,node-2=127.0.0.1:50053

# Terminal 3
cargo run -p concord-server -- --id node-2 --addr 127.0.0.1:50053 \
  --peers node-0=127.0.0.1:50051,node-1=127.0.0.1:50052
```

### Python SDK

Install the Python client (requires Rust toolchain):

```bash
cd crates/client-py
pip install maturin
maturin develop --release
```

Then use it:

```python
from concord import ConcordClient

client = ConcordClient("127.0.0.1:50051", "my-agent")

# LWW-Register: last-writer-wins scalar state
client.put("agents", "status", {"state": "running", "task": "search"})
status = client.get("agents", "status")

# OR-Set: add-wins set for accumulating facts
client.add_to_set("knowledge", "facts", "python-3.12-released")
client.add_to_set("knowledge", "facts", "rust-1.75-stable")
facts = client.get_set("knowledge", "facts")

# RGA Sequence: ordered list with deterministic merge
client.insert_into_sequence("plan", "steps", 0, {"action": "search"})
client.insert_into_sequence("plan", "steps", 1, {"action": "analyze"})
steps = client.get_sequence("plan", "steps")
```

See [`examples/concurrent_agents.py`](examples/concurrent_agents.py) for a full multi-agent demo.

### Running Tests

```bash
# All tests
cargo test

# Property-based CRDT tests
cargo test -p concord-crdt --test proptests

# Raft cluster integration tests (leader election, failover, partitions)
cargo test -p concord-raft --test cluster
```

### Running Benchmarks

```bash
cargo run --release -p concord-bench --bin throughput
```

## Repo Structure

```
concord/
  crates/
    crdt/        — LWW-Register, OR-Set, RGA Sequence
    raft/        — leader election, log replication, state machine
    storage/     — WAL, in-memory store, snapshot manager
    proto/       — gRPC protobuf definitions
    server/      — ties Raft + CRDT + storage + gRPC together
    client-py/   — Python SDK via PyO3
  benches/       — throughput and failover benchmarks
  examples/      — multi-agent demo
  docs/
    design.md    — CRDT + Raft design rationale
```

## Tech Stack

| Layer | Choice | Why |
|-------|--------|-----|
| Core | Rust | Memory safety without GC overhead |
| RPC | gRPC (tonic) | Standard for inter-service communication |
| Consensus | Hand-rolled Raft | Full implementation of leader election, log replication, snapshotting |
| CRDTs | Hand-rolled | No existing library targets agent-state shapes |
| Python SDK | PyO3 + maturin | Native performance with idiomatic Python API |
| Testing | proptest | Property-based verification of CRDT convergence laws |
| Observability | OpenTelemetry | Optional distributed tracing via `--otel` flag |

## License

MIT
