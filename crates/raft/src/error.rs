use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("not the leader (leader is {leader:?})")]
    NotLeader { leader: Option<String> },

    #[error("storage error: {0}")]
    Storage(#[from] concord_storage::StorageError),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("node {0} not found in cluster")]
    UnknownNode(String),

    #[error("log compacted past index {0}")]
    Compacted(u64),
}
