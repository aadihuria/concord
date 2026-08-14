use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("corrupted wal entry at offset {offset}: {reason}")]
    CorruptedEntry { offset: u64, reason: String },

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("snapshot error: {0}")]
    Snapshot(String),
}
