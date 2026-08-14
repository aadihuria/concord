use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Last-Writer-Wins Register.
///
/// Resolves concurrent writes by comparing (timestamp, node_id) pairs.
/// The write with the highest timestamp wins; ties are broken by node_id
/// to guarantee a total order (deterministic convergence).
///
/// HLC (hybrid logical clock) timestamps are used in practice — the caller
/// is responsible for providing monotonically increasing timestamps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LwwRegister {
    pub value: Value,
    pub timestamp: u64,
    pub node_id: String,
}

impl LwwRegister {
    pub fn new(value: Value, timestamp: u64, node_id: String) -> Self {
        Self {
            value,
            timestamp,
            node_id,
        }
    }

    pub fn update(&mut self, value: Value, timestamp: u64, node_id: String) -> bool {
        if self.should_accept(timestamp, &node_id) {
            self.value = value;
            self.timestamp = timestamp;
            self.node_id = node_id;
            true
        } else {
            false
        }
    }

    pub fn merge(&mut self, other: &LwwRegister) -> bool {
        if self.should_accept(other.timestamp, &other.node_id) {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.node_id = other.node_id.clone();
            true
        } else {
            false
        }
    }

    fn should_accept(&self, timestamp: u64, node_id: &str) -> bool {
        timestamp > self.timestamp
            || (timestamp == self.timestamp && node_id > &self.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_higher_timestamp_wins() {
        let mut reg = LwwRegister::new(json!("old"), 1, "node-a".into());
        assert!(reg.update(json!("new"), 2, "node-b".into()));
        assert_eq!(reg.value, json!("new"));
    }

    #[test]
    fn test_lower_timestamp_rejected() {
        let mut reg = LwwRegister::new(json!("current"), 5, "node-a".into());
        assert!(!reg.update(json!("stale"), 3, "node-b".into()));
        assert_eq!(reg.value, json!("current"));
    }

    #[test]
    fn test_tie_broken_by_node_id() {
        let mut reg = LwwRegister::new(json!("from-a"), 10, "node-a".into());
        assert!(reg.update(json!("from-b"), 10, "node-b".into()));
        assert_eq!(reg.value, json!("from-b"));

        // reverse direction should not overwrite
        assert!(!reg.update(json!("from-a-again"), 10, "node-a".into()));
        assert_eq!(reg.value, json!("from-b"));
    }

    #[test]
    fn test_merge_commutative() {
        let a = LwwRegister::new(json!("a-val"), 5, "node-a".into());
        let b = LwwRegister::new(json!("b-val"), 7, "node-b".into());

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value, ba.value);
        assert_eq!(ab.timestamp, ba.timestamp);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut reg = LwwRegister::new(json!("val"), 5, "node-a".into());
        let other = LwwRegister::new(json!("other"), 3, "node-b".into());

        reg.merge(&other);
        let after_first = reg.clone();
        reg.merge(&other);
        assert_eq!(reg, after_first);
    }
}
