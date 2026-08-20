use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::time::{Duration, Instant};
use tonic::transport::Server;
use tracing::{debug, info};

use concord_proto::concord::client_service_server::ClientServiceServer;
use concord_proto::concord::raft_service_server::RaftServiceServer;
use concord_proto::concord::{
    AppendEntriesReq, InstallSnapshotReq, LogEntryProto, VoteReq, WatchEvent,
};
use concord_raft::message::{
    AppendEntriesResponse, InstallSnapshotResponse, RaftMessage, VoteResponse,
};
use concord_raft::node::{RaftConfig, RaftNode};

use crate::client_service::ClientServiceImpl;
use crate::raft_service::{log_entry_to_proto, RaftServiceImpl};
use crate::transport::PeerTransport;

pub struct ConcordServer {
    node_id: String,
    addr: SocketAddr,
    peer_addrs: HashMap<String, String>,
}

impl ConcordServer {
    pub fn new(node_id: &str, addr: SocketAddr, peer_addrs: HashMap<String, String>) -> Self {
        Self {
            node_id: node_id.to_string(),
            addr,
            peer_addrs,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let peers: Vec<String> = self.peer_addrs.keys().cloned().collect();
        let config = RaftConfig::new(&self.node_id, peers);
        let node = Arc::new(tokio::sync::Mutex::new(RaftNode::new(config)));

        let transport = PeerTransport::new(self.peer_addrs.clone());
        let (watch_tx, _) = broadcast::channel::<WatchEvent>(1024);

        let raft_svc = RaftServiceImpl::new(Arc::clone(&node));
        let client_svc = ClientServiceImpl::new(Arc::clone(&node), watch_tx);

        let node_for_tick = Arc::clone(&node);
        let transport_for_tick = transport.clone();
        let tick_handle = tokio::spawn(async move {
            Self::tick_loop(node_for_tick, transport_for_tick).await;
        });

        info!("concord node {} listening on {}", self.node_id, self.addr);

        Server::builder()
            .add_service(RaftServiceServer::new(raft_svc))
            .add_service(ClientServiceServer::new(client_svc))
            .serve(self.addr)
            .await?;

        tick_handle.abort();
        Ok(())
    }

    async fn tick_loop(node: Arc<tokio::sync::Mutex<RaftNode>>, transport: PeerTransport) {
        let mut interval = tokio::time::interval(Duration::from_millis(10));

        loop {
            interval.tick().await;
            let now = Instant::now();

            let outbox = {
                let mut n = node.lock().await;
                n.tick(now);
                n.take_outbox()
            };

            Self::dispatch_messages(outbox, &transport, &node);
        }
    }

    fn dispatch_messages(
        messages: Vec<(String, RaftMessage)>,
        transport: &PeerTransport,
        node: &Arc<tokio::sync::Mutex<RaftNode>>,
    ) {
        for (to, msg) in messages {
            let transport = transport.clone();
            let node = Arc::clone(node);
            tokio::spawn(async move {
                if let Err(e) = Self::send_message(&to, msg, &transport, &node).await {
                    debug!("failed to send to {}: {}", to, e);
                }
            });
        }
    }

    async fn send_message(
        to: &str,
        msg: RaftMessage,
        transport: &PeerTransport,
        node: &Arc<tokio::sync::Mutex<RaftNode>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut client = transport.get_client(to).await?;

        let reply: Option<RaftMessage> = match msg {
            RaftMessage::AppendEntries(req) => {
                let entries: Vec<LogEntryProto> =
                    req.entries.iter().map(log_entry_to_proto).collect();

                let proto_req = AppendEntriesReq {
                    term: req.term,
                    leader_id: req.leader_id,
                    prev_log_index: req.prev_log_index,
                    prev_log_term: req.prev_log_term,
                    entries,
                    leader_commit: req.leader_commit,
                };

                let resp = client.append_entries(proto_req).await?.into_inner();
                Some(RaftMessage::AppendEntriesReply(AppendEntriesResponse {
                    term: resp.term,
                    success: resp.success,
                    match_index: resp.match_index,
                    from: resp.from,
                }))
            }
            RaftMessage::RequestVote(req) => {
                let proto_req = VoteReq {
                    term: req.term,
                    candidate_id: req.candidate_id,
                    last_log_index: req.last_log_index,
                    last_log_term: req.last_log_term,
                };

                let resp = client.request_vote(proto_req).await?.into_inner();
                Some(RaftMessage::RequestVoteReply(VoteResponse {
                    term: resp.term,
                    vote_granted: resp.vote_granted,
                    from: resp.from,
                }))
            }
            RaftMessage::InstallSnapshot(req) => {
                let proto_req = InstallSnapshotReq {
                    term: req.term,
                    leader_id: req.leader_id,
                    last_included_index: req.last_included_index,
                    last_included_term: req.last_included_term,
                    data: req.data,
                };

                let resp = client.install_snapshot(proto_req).await?.into_inner();
                Some(RaftMessage::InstallSnapshotReply(InstallSnapshotResponse {
                    term: resp.term,
                    from: resp.from,
                }))
            }
            _ => None,
        };

        if let Some(reply) = reply {
            let mut n = node.lock().await;
            n.handle_message(reply);
        }

        Ok(())
    }
}
