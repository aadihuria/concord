use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Observed-Remove Set (OR-Set / Add-Remove Set).
///
/// Each add operation generates a globally unique tag. A remove operation
/// only removes the tags that were _observed_ at the time of the remove.
/// This eliminates the add-remove anomaly present in simpler set CRDTs:
/// if two nodes concurrently add and remove the same element, the add wins
/// (because the concurrent add's tag was never observed by the remover).
///
/// Internal representation: element -> set of active tags.
/// A tag is a (Uuid, node_id) pair for uniqueness without coordination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSet {
    elements: HashMap<String, HashSet<Tag>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tag {
    pub id: String,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrSetOp {
    Add {
        element: String,
        tag: Tag,
    },
    Remove {
        element: String,
        observed_tags: HashSet<Tag>,
    },
}

impl OrSet {
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
        }
    }

    pub fn add(&mut self, element: String, node_id: &str) -> OrSetOp {
        let tag = Tag {
            id: Uuid::new_v4().to_string(),
            node_id: node_id.to_string(),
        };

        self.elements
            .entry(element.clone())
            .or_default()
            .insert(tag.clone());

        OrSetOp::Add { element, tag }
    }

    pub fn remove(&mut self, element: &str) -> Option<OrSetOp> {
        let tags = self.elements.get(element)?;
        if tags.is_empty() {
            return None;
        }

        let observed_tags = tags.clone();
        self.elements.remove(element);

        Some(OrSetOp::Remove {
            element: element.to_string(),
            observed_tags,
        })
    }

    pub fn apply(&mut self, op: &OrSetOp) {
        match op {
            OrSetOp::Add { element, tag } => {
                self.elements
                    .entry(element.clone())
                    .or_default()
                    .insert(tag.clone());
            }
            OrSetOp::Remove {
                element,
                observed_tags,
            } => {
                if let Some(tags) = self.elements.get_mut(element) {
                    for t in observed_tags {
                        tags.remove(t);
                    }
                    if tags.is_empty() {
                        self.elements.remove(element);
                    }
                }
            }
        }
    }

    pub fn merge(&mut self, other: &OrSet) {
        for (element, other_tags) in &other.elements {
            let entry = self.elements.entry(element.clone()).or_default();
            for tag in other_tags {
                entry.insert(tag.clone());
            }
        }
    }

    pub fn contains(&self, element: &str) -> bool {
        self.elements
            .get(element)
            .map(|tags| !tags.is_empty())
            .unwrap_or(false)
    }

    pub fn elements(&self) -> Vec<&str> {
        self.elements
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .map(|(k, _)| k.as_str())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.elements
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for OrSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_contains() {
        let mut set = OrSet::new();
        set.add("task-1".into(), "node-a");
        assert!(set.contains("task-1"));
        assert!(!set.contains("task-2"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_remove() {
        let mut set = OrSet::new();
        set.add("task-1".into(), "node-a");
        set.remove("task-1");
        assert!(!set.contains("task-1"));
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_concurrent_add_remove_add_wins() {
        // node-a adds "item", node-b concurrently removes "item"
        // but node-b only observed tags from before node-a's add
        let mut state_a = OrSet::new();
        let mut state_b = OrSet::new();

        // initial state: both have "item"
        let op_init = state_a.add("item".into(), "node-a");
        state_b.apply(&op_init);

        // node-b removes "item" (observes the initial tag)
        let remove_op = state_b.remove("item").unwrap();

        // concurrently, node-a adds "item" again with a new tag
        let add_op = state_a.add("item".into(), "node-a");

        // apply both ops to both replicas
        state_a.apply(&remove_op);
        state_b.apply(&add_op);

        // both should still contain "item" because the concurrent add
        // introduced a tag that the remove didn't observe
        assert!(state_a.contains("item"));
        assert!(state_b.contains("item"));
    }

    #[test]
    fn test_merge_commutative() {
        let mut a = OrSet::new();
        let mut b = OrSet::new();

        a.add("x".into(), "node-a");
        a.add("y".into(), "node-a");
        b.add("y".into(), "node-b");
        b.add("z".into(), "node-b");

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        let mut ab_elems: Vec<_> = ab.elements().iter().map(|s| s.to_string()).collect();
        let mut ba_elems: Vec<_> = ba.elements().iter().map(|s| s.to_string()).collect();
        ab_elems.sort();
        ba_elems.sort();

        assert_eq!(ab_elems, ba_elems);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut a = OrSet::new();
        a.add("x".into(), "node-a");

        let mut merged = a.clone();
        merged.merge(&a);
        merged.merge(&a);

        assert_eq!(merged.len(), 1);
        assert!(merged.contains("x"));
    }

    #[test]
    fn test_op_replay_convergence() {
        let mut state_a = OrSet::new();
        let mut state_b = OrSet::new();

        let ops: Vec<OrSetOp> = vec![
            state_a.add("task-1".into(), "agent-1"),
            state_a.add("task-2".into(), "agent-1"),
            state_b.add("task-3".into(), "agent-2"),
        ];

        // apply all ops to both replicas in different orders
        for op in &ops {
            state_a.apply(op);
        }
        for op in ops.iter().rev() {
            state_b.apply(op);
        }

        let mut a_elems: Vec<_> = state_a.elements().iter().map(|s| s.to_string()).collect();
        let mut b_elems: Vec<_> = state_b.elements().iter().map(|s| s.to_string()).collect();
        a_elems.sort();
        b_elems.sort();

        assert_eq!(a_elems, b_elems);
    }
}
