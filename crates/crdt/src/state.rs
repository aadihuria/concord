use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lww::LwwRegister;
use crate::orset::{OrSet, OrSetOp};
use crate::sequence::{RgaSequence, SequenceOp};

/// A typed key in the CRDT state. The type determines which CRDT governs merges.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StateKey {
    pub namespace: String,
    pub key: String,
}

impl StateKey {
    pub fn new(namespace: &str, key: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            key: key.to_string(),
        }
    }
}

/// Operations that can be applied to the CRDT state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrdtOp {
    SetRegister {
        key: StateKey,
        value: Value,
        timestamp: u64,
        node_id: String,
    },
    AddToSet {
        key: StateKey,
        op: OrSetOp,
    },
    RemoveFromSet {
        key: StateKey,
        op: OrSetOp,
    },
    InsertIntoSequence {
        key: StateKey,
        op: SequenceOp,
    },
    DeleteFromSequence {
        key: StateKey,
        op: SequenceOp,
    },
}

/// The full CRDT state machine. Each key maps to one of three CRDT types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtState {
    registers: HashMap<StateKey, LwwRegister>,
    sets: HashMap<StateKey, OrSet>,
    sequences: HashMap<StateKey, RgaSequence>,
}

impl CrdtState {
    pub fn new() -> Self {
        Self {
            registers: HashMap::new(),
            sets: HashMap::new(),
            sequences: HashMap::new(),
        }
    }

    pub fn apply(&mut self, op: &CrdtOp) {
        match op {
            CrdtOp::SetRegister {
                key,
                value,
                timestamp,
                node_id,
            } => {
                let reg = self.registers.entry(key.clone()).or_insert_with(|| {
                    LwwRegister::new(Value::Null, 0, String::new())
                });
                reg.update(value.clone(), *timestamp, node_id.clone());
            }
            CrdtOp::AddToSet { key, op } | CrdtOp::RemoveFromSet { key, op } => {
                let set = self.sets.entry(key.clone()).or_insert_with(OrSet::new);
                set.apply(op);
            }
            CrdtOp::InsertIntoSequence { key, op }
            | CrdtOp::DeleteFromSequence { key, op } => {
                let seq = self
                    .sequences
                    .entry(key.clone())
                    .or_insert_with(RgaSequence::new);
                seq.apply(op);
            }
        }
    }

    pub fn get_register(&self, key: &StateKey) -> Option<&LwwRegister> {
        self.registers.get(key)
    }

    pub fn get_set(&self, key: &StateKey) -> Option<&OrSet> {
        self.sets.get(key)
    }

    pub fn get_sequence(&self, key: &StateKey) -> Option<&RgaSequence> {
        self.sequences.get(key)
    }

    pub fn set_register(
        &mut self,
        key: StateKey,
        value: Value,
        timestamp: u64,
        node_id: &str,
    ) -> CrdtOp {
        let op = CrdtOp::SetRegister {
            key: key.clone(),
            value: value.clone(),
            timestamp,
            node_id: node_id.to_string(),
        };
        self.apply(&op);
        op
    }

    pub fn add_to_set(&mut self, key: StateKey, element: String, node_id: &str) -> CrdtOp {
        let set = self.sets.entry(key.clone()).or_insert_with(OrSet::new);
        let inner_op = set.add(element, node_id);
        CrdtOp::AddToSet {
            key,
            op: inner_op,
        }
    }

    pub fn remove_from_set(&mut self, key: StateKey, element: &str) -> Option<CrdtOp> {
        let set = self.sets.get_mut(&key)?;
        let inner_op = set.remove(element)?;
        Some(CrdtOp::RemoveFromSet {
            key,
            op: inner_op,
        })
    }

    pub fn insert_into_sequence(
        &mut self,
        key: StateKey,
        position: usize,
        value: Value,
        timestamp: u64,
        node_id: &str,
    ) -> CrdtOp {
        let seq = self
            .sequences
            .entry(key.clone())
            .or_insert_with(RgaSequence::new);
        let inner_op = seq.insert(position, value, timestamp, node_id);
        CrdtOp::InsertIntoSequence {
            key,
            op: inner_op,
        }
    }

    pub fn delete_from_sequence(&mut self, key: StateKey, position: usize) -> Option<CrdtOp> {
        let seq = self.sequences.get_mut(&key)?;
        let inner_op = seq.delete(position)?;
        Some(CrdtOp::DeleteFromSequence {
            key,
            op: inner_op,
        })
    }

    pub fn merge(&mut self, other: &CrdtState) {
        for (key, other_reg) in &other.registers {
            let reg = self.registers.entry(key.clone()).or_insert_with(|| {
                LwwRegister::new(Value::Null, 0, String::new())
            });
            reg.merge(other_reg);
        }
        for (key, other_set) in &other.sets {
            let set = self.sets.entry(key.clone()).or_insert_with(OrSet::new);
            set.merge(other_set);
        }
        for (key, other_seq) in &other.sequences {
            let seq = self
                .sequences
                .entry(key.clone())
                .or_insert_with(RgaSequence::new);
            seq.merge(other_seq);
        }
    }
}

impl Default for CrdtState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_register_ops() {
        let mut state = CrdtState::new();
        let key = StateKey::new("agents", "agent-1-status");

        state.set_register(key.clone(), json!("running"), 1, "node-a");
        state.set_register(key.clone(), json!("done"), 2, "node-a");

        let reg = state.get_register(&key).unwrap();
        assert_eq!(reg.value, json!("done"));
    }

    #[test]
    fn test_set_ops() {
        let mut state = CrdtState::new();
        let key = StateKey::new("tasks", "completed");

        state.add_to_set(key.clone(), "task-1".into(), "agent-a");
        state.add_to_set(key.clone(), "task-2".into(), "agent-b");

        let set = state.get_set(&key).unwrap();
        assert!(set.contains("task-1"));
        assert!(set.contains("task-2"));
        assert_eq!(set.len(), 2);

        state.remove_from_set(key.clone(), "task-1");
        let set = state.get_set(&key).unwrap();
        assert!(!set.contains("task-1"));
    }

    #[test]
    fn test_sequence_ops() {
        let mut state = CrdtState::new();
        let key = StateKey::new("plan", "steps");

        state.insert_into_sequence(key.clone(), 0, json!("research"), 1, "agent-a");
        state.insert_into_sequence(key.clone(), 1, json!("analyze"), 2, "agent-a");
        state.insert_into_sequence(key.clone(), 2, json!("report"), 3, "agent-a");

        let seq = state.get_sequence(&key).unwrap();
        let vals = seq.values();
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], &json!("research"));
    }

    #[test]
    fn test_state_merge() {
        let mut state_a = CrdtState::new();
        let mut state_b = CrdtState::new();

        let key = StateKey::new("agents", "status");
        state_a.set_register(key.clone(), json!("idle"), 1, "node-a");
        state_b.set_register(key.clone(), json!("active"), 2, "node-b");

        state_a.merge(&state_b);
        let reg = state_a.get_register(&key).unwrap();
        assert_eq!(reg.value, json!("active"));
    }

    #[test]
    fn test_op_serialization_roundtrip() {
        let op = CrdtOp::SetRegister {
            key: StateKey::new("ns", "k"),
            value: json!({"nested": true}),
            timestamp: 42,
            node_id: "node-1".into(),
        };

        let serialized = serde_json::to_string(&op).unwrap();
        let deserialized: CrdtOp = serde_json::from_str(&serialized).unwrap();

        if let CrdtOp::SetRegister {
            key, timestamp, ..
        } = deserialized
        {
            assert_eq!(key.namespace, "ns");
            assert_eq!(timestamp, 42);
        } else {
            panic!("wrong variant");
        }
    }
}
