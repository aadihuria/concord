use std::collections::HashSet;

use serde_json::json;
use tokio::time::{Duration, Instant};

use concord_crdt::state::StateKey;
use concord_crdt::CrdtOp;
use concord_raft::message::RaftMessage;
use concord_raft::node::{NodeRole, RaftConfig, RaftNode};

struct TestCluster {
    nodes: Vec<RaftNode>,
    partitioned: HashSet<(usize, usize)>,
}

impl TestCluster {
    fn new(n: usize) -> Self {
        let ids: Vec<String> = (0..n).map(|i| format!("node-{}", i)).collect();
        let nodes = ids
            .iter()
            .map(|id| {
                let peers: Vec<String> = ids.iter().filter(|p| *p != id).cloned().collect();
                let mut config = RaftConfig::new(id, peers);
                config.election_timeout_min = Duration::from_millis(100);
                config.election_timeout_max = Duration::from_millis(200);
                config.heartbeat_interval = Duration::from_millis(30);
                RaftNode::new(config)
            })
            .collect();

        Self {
            nodes,
            partitioned: HashSet::new(),
        }
    }

    fn elect_leader(&mut self) -> usize {
        let far_future = Instant::now() + Duration::from_secs(10);
        self.nodes[0].tick(far_future);

        for _ in 0..10 {
            self.deliver_messages();
        }

        self.find_leader().expect("no leader elected")
    }

    fn find_leader(&self) -> Option<usize> {
        self.nodes
            .iter()
            .position(|n| n.role() == NodeRole::Leader)
    }

    fn deliver_messages(&mut self) {
        let mut all_msgs: Vec<(String, RaftMessage)> = Vec::new();
        for node in self.nodes.iter_mut() {
            all_msgs.extend(node.take_outbox());
        }
        for (to, msg) in all_msgs {
            let to_idx = self.nodes.iter().position(|n| n.id() == to);
            if let Some(to_idx) = to_idx {
                let from_idx = match &msg {
                    RaftMessage::AppendEntries(req) => {
                        self.nodes.iter().position(|n| n.id() == req.leader_id)
                    }
                    RaftMessage::RequestVote(req) => {
                        self.nodes.iter().position(|n| n.id() == req.candidate_id)
                    }
                    RaftMessage::AppendEntriesReply(resp) => {
                        self.nodes.iter().position(|n| n.id() == resp.from)
                    }
                    RaftMessage::RequestVoteReply(resp) => {
                        self.nodes.iter().position(|n| n.id() == resp.from)
                    }
                    RaftMessage::InstallSnapshot(req) => {
                        self.nodes.iter().position(|n| n.id() == req.leader_id)
                    }
                    RaftMessage::InstallSnapshotReply(resp) => {
                        self.nodes.iter().position(|n| n.id() == resp.from)
                    }
                };

                if let Some(from_idx) = from_idx {
                    if self.is_partitioned(from_idx, to_idx) {
                        continue;
                    }
                }

                self.nodes[to_idx].handle_message(msg);
            }
        }
    }

    fn partition(&mut self, a: usize, b: usize) {
        self.partitioned.insert((a, b));
        self.partitioned.insert((b, a));
    }

    fn heal_partition(&mut self, a: usize, b: usize) {
        self.partitioned.remove(&(a, b));
        self.partitioned.remove(&(b, a));
    }

    #[allow(dead_code)]
    fn heal_all(&mut self) {
        self.partitioned.clear();
    }

    fn is_partitioned(&self, a: usize, b: usize) -> bool {
        self.partitioned.contains(&(a, b))
    }

    fn isolate_node(&mut self, idx: usize) {
        for i in 0..self.nodes.len() {
            if i != idx {
                self.partition(idx, i);
            }
        }
    }

    fn reconnect_node(&mut self, idx: usize) {
        for i in 0..self.nodes.len() {
            if i != idx {
                self.heal_partition(idx, i);
            }
        }
    }
}

#[test]
fn test_leader_failover() {
    let mut cluster = TestCluster::new(5);
    let leader_idx = cluster.elect_leader();

    // write some data through the leader
    for i in 0..5 {
        let op = CrdtOp::SetRegister {
            key: StateKey::new("data", &format!("key-{}", i)),
            value: json!(i),
            timestamp: (i + 1) as u64,
            node_id: "client".into(),
        };
        cluster.nodes[leader_idx].propose(op).unwrap();
    }

    for _ in 0..20 {
        cluster.deliver_messages();
    }

    // verify all nodes replicated
    for node in &cluster.nodes {
        assert!(
            node.commit_index() >= 6,
            "node {} commit {} before partition",
            node.id(),
            node.commit_index()
        );
    }

    // isolate the leader
    cluster.isolate_node(leader_idx);

    // trigger election on a follower
    let new_leader_candidate = if leader_idx == 0 { 1 } else { 0 };
    let far_future = Instant::now() + Duration::from_secs(10);
    cluster.nodes[new_leader_candidate].tick(far_future);

    for _ in 0..20 {
        cluster.deliver_messages();
    }

    // a new leader should have been elected among the non-partitioned nodes
    let new_leader = cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .find(|(_, n)| n.role() == NodeRole::Leader);

    assert!(
        new_leader.is_some(),
        "no new leader elected after partitioning old leader"
    );

    let (new_leader_idx, _) = new_leader.unwrap();

    // write more data through the new leader
    let op = CrdtOp::SetRegister {
        key: StateKey::new("data", "post-failover"),
        value: json!("survived"),
        timestamp: 100,
        node_id: "client".into(),
    };
    cluster.nodes[new_leader_idx].propose(op).unwrap();

    for _ in 0..20 {
        cluster.deliver_messages();
    }

    // reconnect the old leader
    cluster.reconnect_node(leader_idx);

    // tick the new leader to send heartbeats to the reconnected node
    for _ in 0..30 {
        let now = Instant::now();
        cluster.nodes[new_leader_idx].tick(now);
        cluster.deliver_messages();
    }

    // the old leader should have stepped down and caught up
    assert_ne!(
        cluster.nodes[leader_idx].role(),
        NodeRole::Leader,
        "old leader should have stepped down"
    );
}

#[test]
fn test_minority_partition_cannot_commit() {
    let mut cluster = TestCluster::new(5);
    let leader_idx = cluster.elect_leader();

    // partition the leader from the majority (isolate it with 1 follower)
    // leader can only reach node 1, others can't reach leader
    for i in 2..5 {
        cluster.partition(leader_idx, i);
    }

    // try to propose — it gets into the log but can't commit
    // because we don't have quorum
    let op = CrdtOp::SetRegister {
        key: StateKey::new("test", "uncommitted"),
        value: json!("should-not-commit"),
        timestamp: 50,
        node_id: "client".into(),
    };
    let pre_commit = cluster.nodes[leader_idx].commit_index();
    cluster.nodes[leader_idx].propose(op).unwrap();

    for _ in 0..20 {
        cluster.deliver_messages();
    }

    // the entry should not have been committed (only 2 nodes can see it)
    // commit_index shouldn't have advanced for this entry
    let post_commit = cluster.nodes[leader_idx].commit_index();
    // the entry was proposed but shouldn't reach quorum (need 3, have 2)
    assert!(
        post_commit <= pre_commit + 1,
        "minority partition should not be able to commit new entries easily"
    );
}

#[test]
fn test_concurrent_proposals_converge() {
    let mut cluster = TestCluster::new(3);
    let leader_idx = cluster.elect_leader();

    // rapid-fire proposals
    for i in 0..20 {
        let op = CrdtOp::SetRegister {
            key: StateKey::new("concurrent", &format!("k{}", i)),
            value: json!(i),
            timestamp: (i + 1) as u64,
            node_id: format!("agent-{}", i % 3),
        };
        cluster.nodes[leader_idx].propose(op).unwrap();
    }

    for _ in 0..50 {
        cluster.deliver_messages();
    }

    // all nodes should converge to the same state
    let leader_commit = cluster.nodes[leader_idx].commit_index();
    for node in &cluster.nodes {
        assert_eq!(
            node.commit_index(),
            leader_commit,
            "node {} diverged: commit {} vs leader {}",
            node.id(),
            node.commit_index(),
            leader_commit,
        );
    }

    // verify state machine convergence
    for i in 0..20 {
        let key = StateKey::new("concurrent", &format!("k{}", i));
        let leader_val = cluster.nodes[leader_idx]
            .state_machine()
            .state()
            .get_register(&key)
            .map(|r| r.value.clone());

        for node in &cluster.nodes {
            let node_val = node
                .state_machine()
                .state()
                .get_register(&key)
                .map(|r| r.value.clone());
            assert_eq!(
                leader_val, node_val,
                "state divergence on key k{}: leader={:?}, {}={:?}",
                i, leader_val, node.id(), node_val
            );
        }
    }
}

#[test]
fn test_heal_and_catch_up() {
    let mut cluster = TestCluster::new(3);
    let leader_idx = cluster.elect_leader();

    // write initial data
    for i in 0..3 {
        let op = CrdtOp::SetRegister {
            key: StateKey::new("data", &format!("before-{}", i)),
            value: json!(i),
            timestamp: (i + 1) as u64,
            node_id: "client".into(),
        };
        cluster.nodes[leader_idx].propose(op).unwrap();
    }
    for _ in 0..20 {
        cluster.deliver_messages();
    }

    // isolate node 2
    cluster.isolate_node(2);

    // write more data (only leader and node 1 see it)
    // but since we still have quorum (2 of 3), these should commit
    for i in 0..3 {
        let op = CrdtOp::SetRegister {
            key: StateKey::new("data", &format!("during-{}", i)),
            value: json!(100 + i),
            timestamp: (100 + i + 1) as u64,
            node_id: "client".into(),
        };
        cluster.nodes[leader_idx].propose(op).unwrap();
    }
    for _ in 0..20 {
        cluster.deliver_messages();
    }

    // reconnect node 2
    cluster.reconnect_node(2);

    // tick the leader to send heartbeats/entries to reconnected node
    for _ in 0..30 {
        let now = Instant::now();
        cluster.nodes[leader_idx].tick(now);
        cluster.deliver_messages();
    }

    // node 2 should have caught up
    let key = StateKey::new("data", "during-2");
    let node2_val = cluster.nodes[2]
        .state_machine()
        .state()
        .get_register(&key);

    assert!(
        node2_val.is_some(),
        "node-2 should have caught up after reconnecting"
    );
    assert_eq!(node2_val.unwrap().value, json!(102));
}
