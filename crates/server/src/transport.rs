use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tonic::transport::Channel;

use concord_proto::concord::raft_service_client::RaftServiceClient;

#[derive(Clone)]
pub struct PeerTransport {
    peers: Arc<RwLock<HashMap<String, RaftServiceClient<Channel>>>>,
    addrs: Arc<HashMap<String, String>>,
}

impl PeerTransport {
    pub fn new(peer_addrs: HashMap<String, String>) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            addrs: Arc::new(peer_addrs),
        }
    }

    pub async fn get_client(
        &self,
        peer_id: &str,
    ) -> Result<RaftServiceClient<Channel>, tonic::transport::Error> {
        if let Some(client) = self.peers.read().get(peer_id) {
            return Ok(client.clone());
        }

        let addr = self.addrs.get(peer_id).cloned().unwrap_or_default();
        let endpoint = tonic::transport::Endpoint::from_shared(addr)?
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(5));

        let channel = endpoint.connect().await?;
        let client = RaftServiceClient::new(channel);

        self.peers.write().insert(peer_id.to_string(), client.clone());
        Ok(client)
    }

    pub fn remove_client(&self, peer_id: &str) {
        self.peers.write().remove(peer_id);
    }
}
