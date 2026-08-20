//! End-to-end tests for the gRPC service layer against a real single-node
//! `RaftNode`. These exercise `ClientServiceImpl` and `RaftServiceImpl`
//! through their public tonic-generated request/response types, the same
//! path a real client or peer would take, without needing a bound socket.

use std::sync::Arc;

use tokio::sync::{broadcast, Mutex};
use tonic::Request;

use concord_proto::concord::client_service_server::ClientService;
use concord_proto::concord::raft_service_server::RaftService;
use concord_proto::concord::*;
use concord_raft::node::{RaftConfig, RaftNode};
use concord_server::client_service::ClientServiceImpl;
use concord_server::raft_service::RaftServiceImpl;

/// A single-node `RaftNode` becomes its own leader as soon as it's ticked
/// past its election deadline (see `RaftNode::start_election`), so this
/// gives tests a leader to propose against without a multi-node cluster.
fn leader_node(id: &str) -> Arc<Mutex<RaftNode>> {
    let config = RaftConfig::new(id, vec![]);
    let mut node = RaftNode::new(config);
    let far_future = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    node.tick(far_future);
    Arc::new(Mutex::new(node))
}

fn client_service(node: Arc<Mutex<RaftNode>>) -> ClientServiceImpl {
    let (watch_tx, _) = broadcast::channel(16);
    ClientServiceImpl::new(node, watch_tx)
}

#[tokio::test]
async fn put_then_get_round_trips_through_the_leader() {
    let node = leader_node("solo");
    let svc = client_service(node);

    let put_resp = svc
        .put(Request::new(PutRequest {
            namespace: "agents".into(),
            key: "status".into(),
            value_json: "\"running\"".into(),
            node_id: "client-1".into(),
            timestamp: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(put_resp.success);

    let get_resp = svc
        .get(Request::new(GetRequest {
            namespace: "agents".into(),
            key: "status".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(get_resp.found);
    assert_eq!(get_resp.value_json, "\"running\"");
}

#[tokio::test]
async fn get_on_missing_key_reports_not_found() {
    let node = leader_node("solo");
    let svc = client_service(node);

    let resp = svc
        .get(Request::new(GetRequest {
            namespace: "agents".into(),
            key: "does-not-exist".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.found);
    assert_eq!(resp.version, 0);
}

#[tokio::test]
async fn delete_overwrites_value_with_null() {
    let node = leader_node("solo");
    let svc = client_service(node);

    svc.put(Request::new(PutRequest {
        namespace: "ns".into(),
        key: "k".into(),
        value_json: "1".into(),
        node_id: "client-1".into(),
        timestamp: 1,
    }))
    .await
    .unwrap();

    let del_resp = svc
        .delete(Request::new(DeleteRequest {
            namespace: "ns".into(),
            key: "k".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(del_resp.success);

    let get_resp = svc
        .get(Request::new(GetRequest {
            namespace: "ns".into(),
            key: "k".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(get_resp.found);
    assert_eq!(get_resp.value_json, "null");
}

#[tokio::test]
async fn writes_before_leadership_return_leader_hint_instead_of_success() {
    // Node is constructed but never ticked, so it's still a Follower with
    // no known leader — propose() must reject the write rather than accept
    // it locally, since a follower's log isn't replicated to anyone.
    let config = RaftConfig::new("solo", vec![]);
    let node = Arc::new(Mutex::new(RaftNode::new(config)));
    let svc = client_service(node);

    let resp = svc
        .put(Request::new(PutRequest {
            namespace: "ns".into(),
            key: "k".into(),
            value_json: "1".into(),
            node_id: "client-1".into(),
            timestamp: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success);
    assert_eq!(resp.index, 0);
}

#[tokio::test]
async fn put_rejects_malformed_json() {
    let node = leader_node("solo");
    let svc = client_service(node);

    let result = svc
        .put(Request::new(PutRequest {
            namespace: "ns".into(),
            key: "k".into(),
            value_json: "{not valid json".into(),
            node_id: "client-1".into(),
            timestamp: 1,
        }))
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn set_add_and_remove_round_trip() {
    let node = leader_node("solo");
    let svc = client_service(node);

    svc.add_to_set(Request::new(AddToSetRequest {
        namespace: "tasks".into(),
        key: "completed".into(),
        element: "task-1".into(),
        node_id: "agent-a".into(),
    }))
    .await
    .unwrap();

    svc.add_to_set(Request::new(AddToSetRequest {
        namespace: "tasks".into(),
        key: "completed".into(),
        element: "task-2".into(),
        node_id: "agent-a".into(),
    }))
    .await
    .unwrap();

    let set_resp = svc
        .get_set(Request::new(GetSetRequest {
            namespace: "tasks".into(),
            key: "completed".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(set_resp.found);
    assert_eq!(set_resp.elements.len(), 2);
    assert!(set_resp.elements.contains(&"task-1".to_string()));

    let remove_resp = svc
        .remove_from_set(Request::new(RemoveFromSetRequest {
            namespace: "tasks".into(),
            key: "completed".into(),
            element: "task-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(remove_resp.success);

    let set_resp = svc
        .get_set(Request::new(GetSetRequest {
            namespace: "tasks".into(),
            key: "completed".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(set_resp.elements, vec!["task-2".to_string()]);
}

#[tokio::test]
async fn remove_from_set_on_absent_element_reports_failure_not_error() {
    let node = leader_node("solo");
    let svc = client_service(node);

    let resp = svc
        .remove_from_set(Request::new(RemoveFromSetRequest {
            namespace: "tasks".into(),
            key: "completed".into(),
            element: "never-added".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success);
}

#[tokio::test]
async fn sequence_insert_and_delete_round_trip_in_order() {
    let node = leader_node("solo");
    let svc = client_service(node);

    for (pos, word) in ["research", "analyze", "report"].iter().enumerate() {
        svc.insert_into_sequence(Request::new(InsertIntoSequenceRequest {
            namespace: "plan".into(),
            key: "steps".into(),
            position: pos as u32,
            value_json: format!("\"{}\"", word),
            node_id: "agent-a".into(),
            timestamp: pos as u64 + 1,
        }))
        .await
        .unwrap();
    }

    let seq_resp = svc
        .get_sequence(Request::new(GetSequenceRequest {
            namespace: "plan".into(),
            key: "steps".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        seq_resp.values_json,
        vec!["\"research\"", "\"analyze\"", "\"report\""]
    );

    svc.delete_from_sequence(Request::new(DeleteFromSequenceRequest {
        namespace: "plan".into(),
        key: "steps".into(),
        position: 0,
    }))
    .await
    .unwrap();

    let seq_resp = svc
        .get_sequence(Request::new(GetSequenceRequest {
            namespace: "plan".into(),
            key: "steps".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(seq_resp.values_json, vec!["\"analyze\"", "\"report\""]);
}

// --- RaftServiceImpl: the inter-node RPC surface ---

#[tokio::test]
async fn request_vote_grants_when_candidate_log_is_up_to_date() {
    let config = RaftConfig::new("follower", vec!["candidate".into()]);
    let node = Arc::new(Mutex::new(RaftNode::new(config)));
    let svc = RaftServiceImpl::new(node);

    let resp = svc
        .request_vote(Request::new(VoteReq {
            term: 1,
            candidate_id: "candidate".into(),
            last_log_index: 0,
            last_log_term: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.vote_granted);
    assert_eq!(resp.term, 1);
}

#[tokio::test]
async fn request_vote_rejects_stale_term() {
    let config = RaftConfig::new("follower", vec!["candidate".into()]);
    let mut raw_node = RaftNode::new(config);
    // Bump the follower's term via a heartbeat from a fictitious current leader
    // so the subsequent vote request (still at term 1) is stale.
    raw_node.handle_message(concord_raft::RaftMessage::AppendEntries(
        concord_raft::AppendEntriesRequest {
            term: 5,
            leader_id: "leader".into(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    ));
    let node = Arc::new(Mutex::new(raw_node));
    let svc = RaftServiceImpl::new(node);

    let resp = svc
        .request_vote(Request::new(VoteReq {
            term: 1,
            candidate_id: "candidate".into(),
            last_log_index: 0,
            last_log_term: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.vote_granted);
    assert_eq!(resp.term, 5);
}

#[tokio::test]
async fn append_entries_advances_follower_log_and_reports_match_index() {
    let config = RaftConfig::new("follower", vec!["leader".into()]);
    let node = Arc::new(Mutex::new(RaftNode::new(config)));
    let svc = RaftServiceImpl::new(node);

    let entry_data = serde_json::to_vec(&concord_raft::log::Command::Noop).unwrap();
    let resp = svc
        .append_entries(Request::new(AppendEntriesReq {
            term: 1,
            leader_id: "leader".into(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntryProto {
                index: 1,
                term: 1,
                data: entry_data,
            }],
            leader_commit: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert_eq!(resp.match_index, 1);
    assert_eq!(resp.term, 1);
}

#[tokio::test]
async fn append_entries_rejects_mismatched_prev_log_term() {
    let config = RaftConfig::new("follower", vec!["leader".into()]);
    let node = Arc::new(Mutex::new(RaftNode::new(config)));
    let svc = RaftServiceImpl::new(node);

    // Prime the follower with one entry at term 1, then have the leader
    // claim `prev_log_term` 2 at that same index — the consistency check
    // in `handle_append_entries` must reject this rather than silently
    // accepting a divergent history.
    let entry_data = serde_json::to_vec(&concord_raft::log::Command::Noop).unwrap();
    svc.append_entries(Request::new(AppendEntriesReq {
        term: 1,
        leader_id: "leader".into(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![LogEntryProto {
            index: 1,
            term: 1,
            data: entry_data,
        }],
        leader_commit: 0,
    }))
    .await
    .unwrap();

    let resp = svc
        .append_entries(Request::new(AppendEntriesReq {
            term: 2,
            leader_id: "leader".into(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success);
}
