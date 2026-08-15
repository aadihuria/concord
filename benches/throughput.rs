use std::time::Instant;

use serde_json::json;
use tokio::time::Duration;

use concord_crdt::state::StateKey;
use concord_crdt::CrdtOp;
use concord_raft::message::RaftMessage;
use concord_raft::node::{NodeRole, RaftConfig, RaftNode};

fn make_cluster(n: usize) -> Vec<RaftNode> {
    let ids: Vec<String> = (0..n).map(|i| format!("node-{}", i)).collect();
    ids.iter()
        .map(|id| {
            let peers: Vec<String> = ids.iter().filter(|p| *p != id).cloned().collect();
            let mut config = RaftConfig::new(id, peers);
            config.election_timeout_min = Duration::from_millis(100);
            config.election_timeout_max = Duration::from_millis(200);
            RaftNode::new(config)
        })
        .collect()
}

fn deliver_messages(nodes: &mut [RaftNode]) {
    let mut all_msgs: Vec<(String, RaftMessage)> = Vec::new();
    for node in nodes.iter_mut() {
        all_msgs.extend(node.take_outbox());
    }
    for (to, msg) in all_msgs {
        if let Some(node) = nodes.iter_mut().find(|n| n.id() == to) {
            node.handle_message(msg);
        }
    }
}

fn elect_leader(nodes: &mut [RaftNode]) -> usize {
    let far_future = tokio::time::Instant::now() + Duration::from_secs(10);
    nodes[0].tick(far_future);
    for _ in 0..10 {
        deliver_messages(nodes);
    }
    nodes
        .iter()
        .position(|n| n.role() == NodeRole::Leader)
        .unwrap()
}

fn bench_write_throughput(cluster_size: usize, num_writes: usize) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut nodes = make_cluster(cluster_size);
        let leader_idx = elect_leader(&mut nodes);

        let start = Instant::now();

        for i in 0..num_writes {
            let op = CrdtOp::SetRegister {
                key: StateKey::new("bench", &format!("k{}", i)),
                value: json!(i),
                timestamp: (i + 1) as u64,
                node_id: "bench-client".into(),
            };
            nodes[leader_idx].propose(op).unwrap();

            if i % 10 == 0 {
                for _ in 0..3 {
                    deliver_messages(&mut nodes);
                }
            }
        }

        // drain remaining messages
        for _ in 0..50 {
            deliver_messages(&mut nodes);
        }

        let elapsed = start.elapsed();
        let ops_per_sec = num_writes as f64 / elapsed.as_secs_f64();
        let us_per_op = elapsed.as_micros() as f64 / num_writes as f64;

        println!(
            "  {}-node cluster, {} writes: {:.0} ops/sec ({:.1}µs/op) in {:.1}ms",
            cluster_size,
            num_writes,
            ops_per_sec,
            us_per_op,
            elapsed.as_secs_f64() * 1000.0
        );
    });
}

fn bench_read_throughput(num_reads: usize) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut nodes = make_cluster(3);
        let leader_idx = elect_leader(&mut nodes);

        // seed data
        for i in 0..100 {
            let op = CrdtOp::SetRegister {
                key: StateKey::new("bench", &format!("k{}", i)),
                value: json!({"data": i, "nested": {"value": true}}),
                timestamp: (i + 1) as u64,
                node_id: "setup".into(),
            };
            nodes[leader_idx].propose(op).unwrap();
        }
        for _ in 0..30 {
            deliver_messages(&mut nodes);
        }

        let key = StateKey::new("bench", "k50");
        let start = Instant::now();

        for _ in 0..num_reads {
            let _ = nodes[0].state_machine().state().get_register(&key);
        }

        let elapsed = start.elapsed();
        let ops_per_sec = num_reads as f64 / elapsed.as_secs_f64();

        println!(
            "  {} reads: {:.0} ops/sec in {:.1}ms",
            num_reads,
            ops_per_sec,
            elapsed.as_secs_f64() * 1000.0
        );
    });
}

fn bench_failover_recovery() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut nodes = make_cluster(5);
        let leader_idx = elect_leader(&mut nodes);

        // write data
        for i in 0..50 {
            let op = CrdtOp::SetRegister {
                key: StateKey::new("failover", &format!("k{}", i)),
                value: json!(i),
                timestamp: (i + 1) as u64,
                node_id: "client".into(),
            };
            nodes[leader_idx].propose(op).unwrap();
        }
        for _ in 0..30 {
            deliver_messages(&mut nodes);
        }

        // simulate leader failure by triggering election on another node
        let candidate = if leader_idx == 0 { 1 } else { 0 };

        let start = Instant::now();

        let far_future = tokio::time::Instant::now() + Duration::from_secs(10);
        nodes[candidate].tick(far_future);

        let mut rounds = 0;
        loop {
            deliver_messages(&mut nodes);
            rounds += 1;
            if nodes
                .iter()
                .any(|n| n.role() == NodeRole::Leader && n.id() != nodes[leader_idx].id())
            {
                break;
            }
            if rounds > 100 {
                println!("  failover did not complete within 100 rounds");
                return;
            }
        }

        let elapsed = start.elapsed();
        println!(
            "  failover recovery: {:.1}ms ({} message rounds)",
            elapsed.as_secs_f64() * 1000.0,
            rounds
        );
    });
}

fn main() {
    println!("=== concord benchmarks ===\n");

    println!("write throughput:");
    bench_write_throughput(3, 1000);
    bench_write_throughput(3, 10000);
    bench_write_throughput(5, 1000);
    bench_write_throughput(5, 10000);

    println!("\nread throughput:");
    bench_read_throughput(100_000);
    bench_read_throughput(1_000_000);

    println!("\nfailover:");
    bench_failover_recovery();
}
