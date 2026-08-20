use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::instrument;

use concord_proto::concord::raft_service_server::RaftService;
use concord_proto::concord::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, LogEntryProto,
    VoteReq, VoteResp,
};
use concord_raft::log::{Command, LogEntry};
use concord_raft::message::{AppendEntriesRequest, RaftMessage, VoteRequest};
use concord_raft::node::RaftNode;

pub struct RaftServiceImpl {
    node: Arc<Mutex<RaftNode>>,
}

impl RaftServiceImpl {
    pub fn new(node: Arc<Mutex<RaftNode>>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    #[instrument(skip(self, request), fields(otel.kind = "server"))]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesReq>,
    ) -> Result<Response<AppendEntriesResp>, Status> {
        let req = request.into_inner();

        let entries: Vec<LogEntry> = req
            .entries
            .into_iter()
            .map(|e| {
                let command: Command = serde_json::from_slice(&e.data).unwrap_or(Command::Noop);
                LogEntry {
                    index: e.index,
                    term: e.term,
                    command,
                }
            })
            .collect();

        let msg = RaftMessage::AppendEntries(AppendEntriesRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries,
            leader_commit: req.leader_commit,
        });

        let mut node = self.node.lock().await;
        node.handle_message(msg);

        let outbox = node.take_outbox();
        drop(node);

        for (_, out_msg) in outbox {
            if let RaftMessage::AppendEntriesReply(resp) = out_msg {
                return Ok(Response::new(AppendEntriesResp {
                    term: resp.term,
                    success: resp.success,
                    match_index: resp.match_index,
                    from: resp.from,
                }));
            }
        }

        Err(Status::internal("no response generated"))
    }

    #[instrument(skip(self, request), fields(otel.kind = "server"))]
    async fn request_vote(&self, request: Request<VoteReq>) -> Result<Response<VoteResp>, Status> {
        let req = request.into_inner();

        let msg = RaftMessage::RequestVote(VoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        });

        let mut node = self.node.lock().await;
        node.handle_message(msg);

        let outbox = node.take_outbox();
        drop(node);

        for (_, out_msg) in outbox {
            if let RaftMessage::RequestVoteReply(resp) = out_msg {
                return Ok(Response::new(VoteResp {
                    term: resp.term,
                    vote_granted: resp.vote_granted,
                    from: resp.from,
                }));
            }
        }

        Err(Status::internal("no response generated"))
    }

    #[instrument(skip(self, request), fields(otel.kind = "server"))]
    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotReq>,
    ) -> Result<Response<InstallSnapshotResp>, Status> {
        let req = request.into_inner();

        let msg = RaftMessage::InstallSnapshot(concord_raft::message::InstallSnapshotRequest {
            term: req.term,
            leader_id: req.leader_id,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            data: req.data,
        });

        let mut node = self.node.lock().await;
        node.handle_message(msg);

        let outbox = node.take_outbox();
        drop(node);

        for (_, out_msg) in outbox {
            if let RaftMessage::InstallSnapshotReply(resp) = out_msg {
                return Ok(Response::new(InstallSnapshotResp {
                    term: resp.term,
                    from: resp.from,
                }));
            }
        }

        Err(Status::internal("no response generated"))
    }
}

pub fn log_entry_to_proto(entry: &LogEntry) -> LogEntryProto {
    let data = serde_json::to_vec(&entry.command).unwrap_or_default();
    LogEntryProto {
        index: entry.index,
        term: entry.term,
        data,
    }
}
