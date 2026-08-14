use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// RGA (Replicated Growable Array) — a sequence CRDT for ordered elements.
///
/// Each element is identified by a unique (timestamp, node_id) pair. Inserts
/// reference a "parent" ID — the element after which the new element should
/// appear. When concurrent inserts target the same parent, the one with the
/// higher (timestamp, node_id) sorts first (leftmost wins for recency).
///
/// Deletions are tombstoned rather than physically removed, preserving the
/// causal structure needed for correct merging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RgaSequence {
    nodes: Vec<RgaNode>,
    index: HashMap<ElementId, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ElementId {
    pub timestamp: u64,
    pub node_id: String,
}

impl ElementId {
    pub fn root() -> Self {
        Self {
            timestamp: 0,
            node_id: String::new(),
        }
    }

    fn is_root(&self) -> bool {
        self.timestamp == 0 && self.node_id.is_empty()
    }
}

impl PartialOrd for ElementId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ElementId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RgaNode {
    id: ElementId,
    parent: ElementId,
    value: Value,
    tombstone: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SequenceOp {
    Insert {
        id: ElementId,
        parent: ElementId,
        value: Value,
    },
    Delete {
        id: ElementId,
    },
}

impl RgaSequence {
    pub fn new() -> Self {
        let root = RgaNode {
            id: ElementId::root(),
            parent: ElementId::root(),
            value: Value::Null,
            tombstone: false,
        };
        let mut index = HashMap::new();
        index.insert(ElementId::root(), 0);
        Self {
            nodes: vec![root],
            index,
        }
    }

    /// Insert a value after the element at the given visible position (0-indexed).
    /// Position 0 means "insert at the beginning" (after root).
    pub fn insert(
        &mut self,
        position: usize,
        value: Value,
        timestamp: u64,
        node_id: &str,
    ) -> SequenceOp {
        let parent = self.id_at_visible_position(position);

        let id = ElementId {
            timestamp,
            node_id: node_id.to_string(),
        };

        let op = SequenceOp::Insert {
            id: id.clone(),
            parent: parent.clone(),
            value: value.clone(),
        };

        self.apply_insert(id, parent, value);
        op
    }

    /// Delete the element at the given visible position (0-indexed).
    pub fn delete(&mut self, position: usize) -> Option<SequenceOp> {
        let visible: Vec<_> = self
            .nodes
            .iter()
            .filter(|n| !n.tombstone && !n.id.is_root())
            .collect();

        let node = visible.get(position)?;
        let id = node.id.clone();

        let op = SequenceOp::Delete { id: id.clone() };
        self.apply_delete(&id);
        Some(op)
    }

    pub fn apply(&mut self, op: &SequenceOp) {
        match op {
            SequenceOp::Insert { id, parent, value } => {
                if !self.index.contains_key(id) {
                    self.apply_insert(id.clone(), parent.clone(), value.clone());
                }
            }
            SequenceOp::Delete { id } => {
                self.apply_delete(id);
            }
        }
    }

    pub fn merge(&mut self, other: &RgaSequence) {
        for node in &other.nodes {
            if node.id.is_root() {
                continue;
            }
            if !self.index.contains_key(&node.id) {
                self.apply_insert(node.id.clone(), node.parent.clone(), node.value.clone());
            }
            if node.tombstone {
                self.apply_delete(&node.id);
            }
        }
    }

    pub fn values(&self) -> Vec<&Value> {
        self.nodes
            .iter()
            .filter(|n| !n.tombstone && !n.id.is_root())
            .map(|n| &n.value)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| !n.tombstone && !n.id.is_root())
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn id_at_visible_position(&self, position: usize) -> ElementId {
        if position == 0 {
            return ElementId::root();
        }

        let visible: Vec<_> = self
            .nodes
            .iter()
            .filter(|n| !n.tombstone && !n.id.is_root())
            .collect();

        if position <= visible.len() {
            visible[position - 1].id.clone()
        } else {
            visible
                .last()
                .map(|n| n.id.clone())
                .unwrap_or_else(ElementId::root)
        }
    }

    fn apply_insert(&mut self, id: ElementId, parent: ElementId, value: Value) {
        if self.index.contains_key(&id) {
            return;
        }

        let parent_idx = self
            .index
            .get(&parent)
            .copied()
            .unwrap_or(0);

        // RGA insert: scan right from parent, skipping nodes whose subtree
        // belongs to a sibling with a higher ID than ours. Stop when we find
        // a sibling with a lower ID, or leave the parent's subtree entirely.
        let mut insert_idx = parent_idx + 1;

        while insert_idx < self.nodes.len() {
            let existing = &self.nodes[insert_idx];

            if existing.parent == parent {
                // direct sibling — higher IDs sort first (leftmost)
                if existing.id > id {
                    // this sibling has priority, skip it and its descendants
                    insert_idx += 1;
                    continue;
                } else {
                    // we have priority over this sibling, insert before it
                    break;
                }
            }

            // not a direct sibling — check if it's a descendant of a
            // sibling we already skipped past (i.e., still in a higher-
            // priority sibling's subtree)
            if self.is_descendant_of(insert_idx, &parent) {
                insert_idx += 1;
                continue;
            }

            // left the parent's subtree entirely
            break;
        }

        let node = RgaNode {
            id: id.clone(),
            parent,
            value,
            tombstone: false,
        };

        self.nodes.insert(insert_idx, node);
        self.rebuild_index();
    }

    fn apply_delete(&mut self, id: &ElementId) {
        if let Some(&idx) = self.index.get(id) {
            self.nodes[idx].tombstone = true;
        }
    }

    fn is_descendant_of(&self, node_idx: usize, ancestor_id: &ElementId) -> bool {
        let mut current = &self.nodes[node_idx].parent;
        let mut depth = 0;
        while depth < self.nodes.len() {
            if current == ancestor_id {
                return true;
            }
            if current.is_root() {
                break;
            }
            if let Some(&idx) = self.index.get(current) {
                current = &self.nodes[idx].parent;
            } else {
                break;
            }
            depth += 1;
        }
        false
    }

    fn rebuild_index(&mut self) {
        self.index.clear();
        for (i, node) in self.nodes.iter().enumerate() {
            self.index.insert(node.id.clone(), i);
        }
    }
}

impl Default for RgaSequence {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_insert_and_read() {
        let mut seq = RgaSequence::new();
        seq.insert(0, json!("step-1"), 1, "agent-a");
        seq.insert(1, json!("step-2"), 2, "agent-a");
        seq.insert(2, json!("step-3"), 3, "agent-a");

        let vals = seq.values();
        assert_eq!(vals, vec![&json!("step-1"), &json!("step-2"), &json!("step-3")]);
    }

    #[test]
    fn test_insert_at_beginning() {
        let mut seq = RgaSequence::new();
        seq.insert(0, json!("second"), 1, "a");
        seq.insert(0, json!("first"), 2, "a");

        let vals = seq.values();
        assert_eq!(vals, vec![&json!("first"), &json!("second")]);
    }

    #[test]
    fn test_delete() {
        let mut seq = RgaSequence::new();
        seq.insert(0, json!("a"), 1, "n");
        seq.insert(1, json!("b"), 2, "n");
        seq.insert(2, json!("c"), 3, "n");

        seq.delete(1); // delete "b"

        let vals = seq.values();
        assert_eq!(vals, vec![&json!("a"), &json!("c")]);
        assert_eq!(seq.len(), 2);
    }

    #[test]
    fn test_concurrent_inserts_converge() {
        // two agents concurrently insert after the same position
        let mut state_a = RgaSequence::new();
        let mut state_b = RgaSequence::new();

        // shared initial state
        let op0 = state_a.insert(0, json!("root-step"), 1, "init");
        state_b.apply(&op0);

        // concurrent inserts after position 1 (after "root-step")
        let op_a = state_a.insert(1, json!("agent-a-step"), 2, "agent-a");
        let op_b = state_b.insert(1, json!("agent-b-step"), 2, "agent-b");

        // cross-apply
        state_a.apply(&op_b);
        state_b.apply(&op_a);

        // both should converge to the same order
        let vals_a: Vec<_> = state_a.values().into_iter().cloned().collect();
        let vals_b: Vec<_> = state_b.values().into_iter().cloned().collect();
        assert_eq!(vals_a, vals_b);
    }

    #[test]
    fn test_merge_commutative() {
        let mut a = RgaSequence::new();
        let mut b = RgaSequence::new();

        a.insert(0, json!("a1"), 1, "node-a");
        a.insert(1, json!("a2"), 3, "node-a");
        b.insert(0, json!("b1"), 2, "node-b");
        b.insert(1, json!("b2"), 4, "node-b");

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        let vals_ab: Vec<_> = ab.values().into_iter().cloned().collect();
        let vals_ba: Vec<_> = ba.values().into_iter().cloned().collect();

        assert_eq!(vals_ab, vals_ba);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut seq = RgaSequence::new();
        seq.insert(0, json!("x"), 1, "a");

        let snapshot = seq.clone();
        seq.merge(&snapshot);
        seq.merge(&snapshot);

        assert_eq!(seq.len(), 1);
    }

    #[test]
    fn test_delete_then_merge_preserves_tombstone() {
        let mut a = RgaSequence::new();
        let op = a.insert(0, json!("item"), 1, "n");

        let mut b = RgaSequence::new();
        b.apply(&op);

        a.delete(0);
        b.merge(&a);

        assert_eq!(b.len(), 0);
    }
}
