use std::collections::HashMap;

use rand::Rng;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

use concord_crdt::CrdtOp;

use crate::error::RaftError;
use crate::log::{Command, LogEntry, RaftLog};
use crate::message::*;
use crate::state_machine::StateMachine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub id: String,
    pub peers: Vec<String>,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
}

impl RaftConfig {
    pub fn new(id: &str, peers: Vec<String>) -> Self {
        Self {
            id: id.to_string(),
            peers,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}

pub struct RaftNode {
    config: RaftConfig,

    // persistent state
    current_term: u64,
    voted_for: Option<String>,
    log: RaftLog,

    // volatile state
    role: NodeRole,
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<String>,

    // leader-only volatile state
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,

    // election state
    votes_received: usize,
    election_deadline: Instant,

    // state machine
    state_machine: StateMachine,

    // outbound message channel
    outbox: Vec<(String, RaftMessage)>,
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for peer in &config.peers {
            next_index.insert(peer.clone(), 1);
            match_index.insert(peer.clone(), 0);
        }

        let election_deadline = Self::random_election_deadline(
            config.election_timeout_min,
            config.election_timeout_max,
        );

        Self {
            config,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            role: NodeRole::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            next_index,
            match_index,
            votes_received: 0,
            election_deadline,
            state_machine: StateMachine::new(),
            outbox: Vec::new(),
        }
    }

    pub fn id(&self) -> &str {
        &self.config.id
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.current_term
    }

    pub fn leader_id(&self) -> Option<&str> {
        self.leader_id.as_deref()
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn state_machine(&self) -> &StateMachine {
        &self.state_machine
    }

    pub fn take_outbox(&mut self) -> Vec<(String, RaftMessage)> {
        std::mem::take(&mut self.outbox)
    }

    // --- client interface ---

    pub fn propose(&mut self, op: CrdtOp) -> Result<u64, RaftError> {
        if self.role != NodeRole::Leader {
            return Err(RaftError::NotLeader {
                leader: self.leader_id.clone(),
            });
        }

        let index = self.log.last_index() + 1;
        let entry = LogEntry {
            index,
            term: self.current_term,
            command: Command::CrdtOp(op),
        };

        self.log.append(entry);
        self.replicate_to_all();
        Ok(index)
    }

    // --- tick: called periodically to drive timeouts ---

    pub fn tick(&mut self, now: Instant) {
        match self.role {
            NodeRole::Follower | NodeRole::Candidate => {
                if now >= self.election_deadline {
                    self.start_election();
                }
            }
            NodeRole::Leader => {
                self.send_heartbeats();
            }
        }
    }

    // --- message handlers ---

    pub fn handle_message(&mut self, msg: RaftMessage) {
        match msg {
            RaftMessage::AppendEntries(req) => self.handle_append_entries(req),
            RaftMessage::AppendEntriesReply(resp) => self.handle_append_entries_response(resp),
            RaftMessage::RequestVote(req) => self.handle_request_vote(req),
            RaftMessage::RequestVoteReply(resp) => self.handle_vote_response(resp),
            RaftMessage::InstallSnapshot(req) => self.handle_install_snapshot(req),
            RaftMessage::InstallSnapshotReply(resp) => self.handle_install_snapshot_response(resp),
        }
    }

    fn handle_append_entries(&mut self, req: AppendEntriesRequest) {
        if req.term < self.current_term {
            self.send(
                &req.leader_id,
                RaftMessage::AppendEntriesReply(AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                    from: self.config.id.clone(),
                }),
            );
            return;
        }

        self.maybe_step_down(req.term);
        self.leader_id = Some(req.leader_id.clone());
        self.reset_election_timeout();

        // log consistency check
        if req.prev_log_index > 0 {
            match self.log.term_at(req.prev_log_index) {
                Some(term) if term != req.prev_log_term => {
                    self.log.truncate_from(req.prev_log_index);
                    self.send(
                        &req.leader_id,
                        RaftMessage::AppendEntriesReply(AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                            from: self.config.id.clone(),
                        }),
                    );
                    return;
                }
                None if req.prev_log_index > self.log.snapshot_last_index() => {
                    self.send(
                        &req.leader_id,
                        RaftMessage::AppendEntriesReply(AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                            from: self.config.id.clone(),
                        }),
                    );
                    return;
                }
                _ => {}
            }
        }

        if !req.entries.is_empty() {
            self.log.append_entries(req.prev_log_index, req.entries);
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(req.leader_commit, self.log.last_index());
            self.apply_committed();
        }

        self.send(
            &req.leader_id,
            RaftMessage::AppendEntriesReply(AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: self.log.last_index(),
                from: self.config.id.clone(),
            }),
        );
    }

    fn handle_append_entries_response(&mut self, resp: AppendEntriesResponse) {
        if resp.term > self.current_term {
            self.maybe_step_down(resp.term);
            return;
        }

        if self.role != NodeRole::Leader {
            return;
        }

        if resp.success {
            self.next_index
                .insert(resp.from.clone(), resp.match_index + 1);
            self.match_index.insert(resp.from.clone(), resp.match_index);
            self.try_advance_commit();
        } else {
            // decrement next_index and retry
            let ni = self.next_index.get(&resp.from).copied().unwrap_or(1);
            let new_ni = if ni > 1 { ni - 1 } else { 1 };
            self.next_index.insert(resp.from.clone(), new_ni);
            self.replicate_to(&resp.from);
        }
    }

    fn handle_request_vote(&mut self, req: VoteRequest) {
        if req.term < self.current_term {
            self.send(
                &req.candidate_id,
                RaftMessage::RequestVoteReply(VoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                    from: self.config.id.clone(),
                }),
            );
            return;
        }

        self.maybe_step_down(req.term);

        let can_vote =
            self.voted_for.is_none() || self.voted_for.as_deref() == Some(&req.candidate_id);

        let log_up_to_date = req.last_log_term > self.log.last_term()
            || (req.last_log_term == self.log.last_term()
                && req.last_log_index >= self.log.last_index());

        let grant = can_vote && log_up_to_date;

        if grant {
            self.voted_for = Some(req.candidate_id.clone());
            self.reset_election_timeout();
            debug!(
                "{}: granting vote to {} for term {}",
                self.config.id, req.candidate_id, req.term
            );
        }

        self.send(
            &req.candidate_id,
            RaftMessage::RequestVoteReply(VoteResponse {
                term: self.current_term,
                vote_granted: grant,
                from: self.config.id.clone(),
            }),
        );
    }

    fn handle_vote_response(&mut self, resp: VoteResponse) {
        if resp.term > self.current_term {
            self.maybe_step_down(resp.term);
            return;
        }

        if self.role != NodeRole::Candidate || resp.term != self.current_term {
            return;
        }

        if resp.vote_granted {
            self.votes_received += 1;
            debug!(
                "{}: received vote from {} ({}/{})",
                self.config.id,
                resp.from,
                self.votes_received,
                self.quorum_size()
            );

            if self.votes_received >= self.quorum_size() {
                self.become_leader();
            }
        }
    }

    fn handle_install_snapshot(&mut self, req: InstallSnapshotRequest) {
        if req.term < self.current_term {
            self.send(
                &req.leader_id,
                RaftMessage::InstallSnapshotReply(InstallSnapshotResponse {
                    term: self.current_term,
                    from: self.config.id.clone(),
                }),
            );
            return;
        }

        self.maybe_step_down(req.term);
        self.leader_id = Some(req.leader_id.clone());
        self.reset_election_timeout();

        if let Ok(snapshot) =
            serde_json::from_slice::<crate::state_machine::StateMachineSnapshot>(&req.data)
        {
            self.state_machine.restore(snapshot);
            self.log
                .set_snapshot_meta(req.last_included_index, req.last_included_term);
            self.log
                .compact(req.last_included_index, req.last_included_term);
            self.commit_index = std::cmp::max(self.commit_index, req.last_included_index);
            self.last_applied = req.last_included_index;

            info!(
                "{}: installed snapshot through index {}",
                self.config.id, req.last_included_index
            );
        } else {
            warn!("{}: failed to deserialize snapshot", self.config.id);
        }

        self.send(
            &req.leader_id,
            RaftMessage::InstallSnapshotReply(InstallSnapshotResponse {
                term: self.current_term,
                from: self.config.id.clone(),
            }),
        );
    }

    fn handle_install_snapshot_response(&mut self, resp: InstallSnapshotResponse) {
        if resp.term > self.current_term {
            self.maybe_step_down(resp.term);
        }
    }

    // --- internal helpers ---

    fn start_election(&mut self) {
        self.current_term += 1;
        self.role = NodeRole::Candidate;
        self.voted_for = Some(self.config.id.clone());
        self.votes_received = 1; // vote for self
        self.reset_election_timeout();

        info!(
            "{}: starting election for term {}",
            self.config.id, self.current_term
        );

        let req = VoteRequest {
            term: self.current_term,
            candidate_id: self.config.id.clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        for peer in self.config.peers.clone() {
            self.send(&peer, RaftMessage::RequestVote(req.clone()));
        }

        // single-node cluster: immediately become leader
        if self.config.peers.is_empty() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = NodeRole::Leader;
        self.leader_id = Some(self.config.id.clone());

        let next = self.log.last_index() + 1;
        for peer in &self.config.peers {
            self.next_index.insert(peer.clone(), next);
            self.match_index.insert(peer.clone(), 0);
        }

        info!(
            "{}: became leader for term {}",
            self.config.id, self.current_term
        );

        // append a noop entry to commit entries from previous terms
        let entry = LogEntry {
            index: self.log.last_index() + 1,
            term: self.current_term,
            command: Command::Noop,
        };
        self.log.append(entry);
        self.replicate_to_all();
    }

    fn maybe_step_down(&mut self, term: u64) {
        if term > self.current_term {
            debug!(
                "{}: stepping down, saw term {} (was {})",
                self.config.id, term, self.current_term
            );
            self.current_term = term;
            self.role = NodeRole::Follower;
            self.voted_for = None;
            self.leader_id = None;
            self.reset_election_timeout();
        }
    }

    fn replicate_to_all(&mut self) {
        let peers: Vec<String> = self.config.peers.clone();
        for peer in peers {
            self.replicate_to(&peer);
        }
    }

    fn replicate_to(&mut self, peer: &str) {
        let next = self.next_index.get(peer).copied().unwrap_or(1);

        // if peer is too far behind and we have a snapshot, send snapshot instead
        if next <= self.log.snapshot_last_index() && self.log.snapshot_last_index() > 0 {
            let snapshot = self.state_machine.snapshot();
            let data = serde_json::to_vec(&snapshot).unwrap_or_default();
            self.send(
                peer,
                RaftMessage::InstallSnapshot(InstallSnapshotRequest {
                    term: self.current_term,
                    leader_id: self.config.id.clone(),
                    last_included_index: self.log.snapshot_last_index(),
                    last_included_term: self.log.snapshot_last_term(),
                    data,
                }),
            );
            return;
        }

        let prev_index = if next > 0 { next - 1 } else { 0 };
        let prev_term = self.log.term_at(prev_index).unwrap_or(0);

        let entries: Vec<LogEntry> = self.log.entries_from(next).to_vec();

        self.send(
            peer,
            RaftMessage::AppendEntries(AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.config.id.clone(),
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit_index,
            }),
        );
    }

    fn send_heartbeats(&mut self) {
        self.replicate_to_all();
    }

    fn try_advance_commit(&mut self) {
        let cluster_size = self.config.peers.len() + 1;
        let old_commit = self.commit_index;

        for n in (self.commit_index + 1)..=self.log.last_index() {
            if self.log.term_at(n) != Some(self.current_term) {
                continue;
            }

            let mut match_count = 1; // leader has it
            for peer in &self.config.peers {
                if self.match_index.get(peer).copied().unwrap_or(0) >= n {
                    match_count += 1;
                }
            }

            if match_count > cluster_size / 2 {
                self.commit_index = n;
            }
        }

        self.apply_committed();

        if self.commit_index > old_commit {
            self.replicate_to_all();
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.entry_at(self.last_applied) {
                let cmd = entry.command.clone();
                self.state_machine.apply(self.last_applied, &cmd);
            }
        }
    }

    fn reset_election_timeout(&mut self) {
        self.election_deadline = Self::random_election_deadline(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
    }

    fn quorum_size(&self) -> usize {
        (self.config.peers.len() + 1).div_ceil(2)
    }

    fn random_election_deadline(min: Duration, max: Duration) -> Instant {
        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(min..=max);
        Instant::now() + timeout
    }

    fn send(&mut self, to: &str, msg: RaftMessage) {
        self.outbox.push((to.to_string(), msg));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use concord_crdt::state::StateKey;
    use serde_json::json;

    fn make_cluster(n: usize) -> Vec<RaftNode> {
        let ids: Vec<String> = (0..n).map(|i| format!("node-{}", i)).collect();
        ids.iter()
            .map(|id| {
                let peers: Vec<String> = ids.iter().filter(|p| *p != id).cloned().collect();
                let mut config = RaftConfig::new(id, peers);
                config.election_timeout_min = Duration::from_millis(100);
                config.election_timeout_max = Duration::from_millis(200);
                config.heartbeat_interval = Duration::from_millis(30);
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
        // trigger election on node 0 by advancing past its deadline
        let far_future = Instant::now() + Duration::from_secs(10);
        nodes[0].tick(far_future);

        // deliver vote requests and responses
        for _ in 0..5 {
            deliver_messages(nodes);
        }

        nodes
            .iter()
            .position(|n| n.role() == NodeRole::Leader)
            .expect("no leader elected")
    }

    #[test]
    fn test_leader_election() {
        let mut nodes = make_cluster(3);
        let leader_idx = elect_leader(&mut nodes);
        assert_eq!(nodes[leader_idx].role(), NodeRole::Leader);

        let followers: Vec<_> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader_idx)
            .map(|(_, n)| n)
            .collect();

        for f in followers {
            assert_eq!(f.role(), NodeRole::Follower);
        }
    }

    #[test]
    fn test_log_replication() {
        let mut nodes = make_cluster(3);
        let leader_idx = elect_leader(&mut nodes);

        let op = CrdtOp::SetRegister {
            key: StateKey::new("agents", "status"),
            value: json!("running"),
            timestamp: 1,
            node_id: "client".into(),
        };
        nodes[leader_idx].propose(op).unwrap();

        // deliver entries and responses
        for _ in 0..10 {
            deliver_messages(&mut nodes);
        }

        for node in &nodes {
            assert!(
                node.commit_index() >= 2,
                "node {} commit_index = {} (expected >= 2)",
                node.id(),
                node.commit_index()
            );
        }

        // verify state machine convergence
        let key = StateKey::new("agents", "status");
        for node in &nodes {
            let reg = node
                .state_machine()
                .state()
                .get_register(&key)
                .expect("register not found");
            assert_eq!(reg.value, json!("running"));
        }
    }

    #[test]
    fn test_proposal_rejected_by_follower() {
        let mut nodes = make_cluster(3);
        elect_leader(&mut nodes);

        let follower_idx = nodes
            .iter()
            .position(|n| n.role() == NodeRole::Follower)
            .unwrap();

        let op = CrdtOp::SetRegister {
            key: StateKey::new("test", "key"),
            value: json!("val"),
            timestamp: 1,
            node_id: "client".into(),
        };
        let result = nodes[follower_idx].propose(op);
        assert!(matches!(result, Err(RaftError::NotLeader { .. })));
    }

    #[test]
    fn test_leader_stepdown_on_higher_term() {
        let mut nodes = make_cluster(3);
        elect_leader(&mut nodes);

        let leader_idx = nodes
            .iter()
            .position(|n| n.role() == NodeRole::Leader)
            .unwrap();

        let higher_term_msg = RaftMessage::AppendEntries(AppendEntriesRequest {
            term: nodes[leader_idx].term() + 10,
            leader_id: "phantom".into(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        });

        nodes[leader_idx].handle_message(higher_term_msg);
        assert_eq!(nodes[leader_idx].role(), NodeRole::Follower);
    }

    #[test]
    fn test_single_node_cluster() {
        let config = RaftConfig::new("solo", vec![]);
        let mut node = RaftNode::new(config);

        let far_future = Instant::now() + Duration::from_secs(10);
        node.tick(far_future);

        assert_eq!(node.role(), NodeRole::Leader);

        let op = CrdtOp::SetRegister {
            key: StateKey::new("test", "solo"),
            value: json!(42),
            timestamp: 1,
            node_id: "client".into(),
        };
        node.propose(op).unwrap();

        // single node commits immediately — just need to advance commit
        // (no peers to wait for)
        // For a single-node cluster, we need the leader to check commit
        // We'll trigger a tick which sends heartbeats (no peers, so no-op)
        // and the commit should advance in try_advance_commit
        for _ in 0..5 {
            deliver_messages(std::slice::from_mut(&mut node));
        }
    }

    #[test]
    fn test_five_node_cluster() {
        let mut nodes = make_cluster(5);
        let leader_idx = elect_leader(&mut nodes);

        for i in 0..10 {
            let op = CrdtOp::SetRegister {
                key: StateKey::new("data", &format!("key-{}", i)),
                value: json!(i),
                timestamp: i as u64 + 1,
                node_id: "client".into(),
            };
            nodes[leader_idx].propose(op).unwrap();
        }

        for _ in 0..20 {
            deliver_messages(&mut nodes);
        }

        for node in &nodes {
            let key = StateKey::new("data", "key-9");
            let reg = node.state_machine().state().get_register(&key);
            assert!(reg.is_some(), "node {} missing key-9", node.id());
            assert_eq!(reg.unwrap().value, json!(9));
        }
    }

    #[test]
    fn test_log_conflict_resolution() {
        let mut nodes = make_cluster(3);
        elect_leader(&mut nodes);

        let leader_idx = nodes
            .iter()
            .position(|n| n.role() == NodeRole::Leader)
            .unwrap();

        // write some entries
        for i in 0..3 {
            let op = CrdtOp::SetRegister {
                key: StateKey::new("test", &format!("k{}", i)),
                value: json!(i),
                timestamp: i as u64 + 1,
                node_id: "client".into(),
            };
            nodes[leader_idx].propose(op).unwrap();
        }

        for _ in 0..15 {
            deliver_messages(&mut nodes);
        }

        // all nodes should have the same commit index
        let leader_commit = nodes[leader_idx].commit_index();
        for node in &nodes {
            assert_eq!(
                node.commit_index(),
                leader_commit,
                "node {} has commit_index {} != leader's {}",
                node.id(),
                node.commit_index(),
                leader_commit,
            );
        }
    }
}
