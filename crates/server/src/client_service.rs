use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::instrument;

use concord_crdt::state::StateKey;
use concord_crdt::CrdtOp;
use concord_proto::concord::client_service_server::ClientService;
use concord_proto::concord::*;
use concord_raft::node::RaftNode;

pub struct ClientServiceImpl {
    node: Arc<Mutex<RaftNode>>,
    watch_tx: broadcast::Sender<WatchEvent>,
}

impl ClientServiceImpl {
    pub fn new(node: Arc<Mutex<RaftNode>>, watch_tx: broadcast::Sender<WatchEvent>) -> Self {
        Self { node, watch_tx }
    }

    fn leader_hint(node: &RaftNode) -> String {
        node.leader_id().unwrap_or("unknown").to_string()
    }
}

#[tonic::async_trait]
impl ClientService for ClientServiceImpl {
    #[instrument(skip(self, request), fields(otel.kind = "server"))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let key = StateKey::new(&req.namespace, &req.key);

        let node = self.node.lock().await;
        let state = node.state_machine().state();

        if let Some(reg) = state.get_register(&key) {
            Ok(Response::new(GetResponse {
                value_json: serde_json::to_string(&reg.value).unwrap_or_default(),
                version: reg.timestamp,
                found: true,
            }))
        } else {
            Ok(Response::new(GetResponse {
                value_json: String::new(),
                version: 0,
                found: false,
            }))
        }
    }

    #[instrument(skip(self, request), fields(otel.kind = "server"))]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        let value: serde_json::Value = serde_json::from_str(&req.value_json)
            .map_err(|e| Status::invalid_argument(format!("invalid json: {}", e)))?;

        let op = CrdtOp::SetRegister {
            key: StateKey::new(&req.namespace, &req.key),
            value: value.clone(),
            timestamp: req.timestamp,
            node_id: req.node_id.clone(),
        };

        let mut node = self.node.lock().await;
        match node.propose(op) {
            Ok(index) => {
                let _ = self.watch_tx.send(WatchEvent {
                    namespace: req.namespace,
                    key: req.key,
                    value_json: req.value_json,
                    version: req.timestamp,
                });
                Ok(Response::new(PutResponse {
                    index,
                    success: true,
                    leader_hint: String::new(),
                }))
            }
            Err(concord_raft::RaftError::NotLeader { .. }) => {
                let hint = Self::leader_hint(&node);
                Ok(Response::new(PutResponse {
                    index: 0,
                    success: false,
                    leader_hint: hint,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    #[instrument(skip(self, request), fields(otel.kind = "server"))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        let op = CrdtOp::SetRegister {
            key: StateKey::new(&req.namespace, &req.key),
            value: serde_json::Value::Null,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            node_id: "system".into(),
        };

        let mut node = self.node.lock().await;
        match node.propose(op) {
            Ok(_) => Ok(Response::new(DeleteResponse {
                success: true,
                leader_hint: String::new(),
            })),
            Err(concord_raft::RaftError::NotLeader { .. }) => {
                let hint = Self::leader_hint(&node);
                Ok(Response::new(DeleteResponse {
                    success: false,
                    leader_hint: hint,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    type SubscribeStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<WatchEvent, Status>> + Send>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let rx = self.watch_tx.subscribe();
        let stream = BroadcastStream::new(rx).map(|result| {
            result.map_err(|e| Status::internal(e.to_string()))
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn add_to_set(
        &self,
        request: Request<AddToSetRequest>,
    ) -> Result<Response<AddToSetResponse>, Status> {
        let req = request.into_inner();
        let key = StateKey::new(&req.namespace, &req.key);

        let mut node = self.node.lock().await;

        let op = {
            let state = node.state_machine().state().clone();
            let mut state_copy = state;
            state_copy.add_to_set(key, req.element, &req.node_id)
        };

        match node.propose(op) {
            Ok(_) => Ok(Response::new(AddToSetResponse {
                success: true,
                leader_hint: String::new(),
            })),
            Err(concord_raft::RaftError::NotLeader { .. }) => {
                let hint = Self::leader_hint(&node);
                Ok(Response::new(AddToSetResponse {
                    success: false,
                    leader_hint: hint,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn remove_from_set(
        &self,
        request: Request<RemoveFromSetRequest>,
    ) -> Result<Response<RemoveFromSetResponse>, Status> {
        let req = request.into_inner();
        let key = StateKey::new(&req.namespace, &req.key);

        let mut node = self.node.lock().await;

        let op = {
            let state = node.state_machine().state().clone();
            let mut state_copy = state;
            match state_copy.remove_from_set(key, &req.element) {
                Some(op) => op,
                None => {
                    return Ok(Response::new(RemoveFromSetResponse {
                        success: false,
                        leader_hint: String::new(),
                    }));
                }
            }
        };

        match node.propose(op) {
            Ok(_) => Ok(Response::new(RemoveFromSetResponse {
                success: true,
                leader_hint: String::new(),
            })),
            Err(concord_raft::RaftError::NotLeader { .. }) => {
                let hint = Self::leader_hint(&node);
                Ok(Response::new(RemoveFromSetResponse {
                    success: false,
                    leader_hint: hint,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn insert_into_sequence(
        &self,
        request: Request<InsertIntoSequenceRequest>,
    ) -> Result<Response<InsertIntoSequenceResponse>, Status> {
        let req = request.into_inner();
        let key = StateKey::new(&req.namespace, &req.key);

        let value: serde_json::Value = serde_json::from_str(&req.value_json)
            .map_err(|e| Status::invalid_argument(format!("invalid json: {}", e)))?;

        let mut node = self.node.lock().await;

        let op = {
            let state = node.state_machine().state().clone();
            let mut state_copy = state;
            state_copy.insert_into_sequence(
                key,
                req.position as usize,
                value,
                req.timestamp,
                &req.node_id,
            )
        };

        match node.propose(op) {
            Ok(_) => Ok(Response::new(InsertIntoSequenceResponse {
                success: true,
                leader_hint: String::new(),
            })),
            Err(concord_raft::RaftError::NotLeader { .. }) => {
                let hint = Self::leader_hint(&node);
                Ok(Response::new(InsertIntoSequenceResponse {
                    success: false,
                    leader_hint: hint,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_from_sequence(
        &self,
        request: Request<DeleteFromSequenceRequest>,
    ) -> Result<Response<DeleteFromSequenceResponse>, Status> {
        let req = request.into_inner();
        let key = StateKey::new(&req.namespace, &req.key);

        let mut node = self.node.lock().await;

        let op = {
            let state = node.state_machine().state().clone();
            let mut state_copy = state;
            match state_copy.delete_from_sequence(key, req.position as usize) {
                Some(op) => op,
                None => {
                    return Ok(Response::new(DeleteFromSequenceResponse {
                        success: false,
                        leader_hint: String::new(),
                    }));
                }
            }
        };

        match node.propose(op) {
            Ok(_) => Ok(Response::new(DeleteFromSequenceResponse {
                success: true,
                leader_hint: String::new(),
            })),
            Err(concord_raft::RaftError::NotLeader { .. }) => {
                let hint = Self::leader_hint(&node);
                Ok(Response::new(DeleteFromSequenceResponse {
                    success: false,
                    leader_hint: hint,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_sequence(
        &self,
        request: Request<GetSequenceRequest>,
    ) -> Result<Response<GetSequenceResponse>, Status> {
        let req = request.into_inner();
        let key = StateKey::new(&req.namespace, &req.key);

        let node = self.node.lock().await;
        let state = node.state_machine().state();

        if let Some(seq) = state.get_sequence(&key) {
            let values: Vec<String> = seq
                .values()
                .into_iter()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .collect();
            Ok(Response::new(GetSequenceResponse {
                values_json: values,
                found: true,
            }))
        } else {
            Ok(Response::new(GetSequenceResponse {
                values_json: vec![],
                found: false,
            }))
        }
    }

    async fn get_set(
        &self,
        request: Request<GetSetRequest>,
    ) -> Result<Response<GetSetResponse>, Status> {
        let req = request.into_inner();
        let key = StateKey::new(&req.namespace, &req.key);

        let node = self.node.lock().await;
        let state = node.state_machine().state();

        if let Some(set) = state.get_set(&key) {
            let elements: Vec<String> = set.elements().into_iter().map(|s| s.to_string()).collect();
            Ok(Response::new(GetSetResponse {
                elements,
                found: true,
            }))
        } else {
            Ok(Response::new(GetSetResponse {
                elements: vec![],
                found: false,
            }))
        }
    }
}
