use std::collections::BTreeMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::StorageError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Document {
    pub value: Value,
    pub version: u64,
    pub last_modified_by: String,
}

pub struct MemStore {
    data: RwLock<BTreeMap<String, Document>>,
    version: RwLock<u64>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            version: RwLock::new(0),
        }
    }

    pub fn get(&self, key: &str) -> Option<Document> {
        self.data.read().get(key).cloned()
    }

    pub fn put(&self, key: String, value: Value, modified_by: String) -> u64 {
        let mut ver = self.version.write();
        *ver += 1;
        let doc = Document {
            value,
            version: *ver,
            last_modified_by: modified_by,
        };
        self.data.write().insert(key, doc);
        *ver
    }

    pub fn delete(&self, key: &str) -> bool {
        self.data.write().remove(key).is_some()
    }

    pub fn get_nested(&self, key: &str, path: &[&str]) -> Option<Value> {
        let data = self.data.read();
        let doc = data.get(key)?;
        let mut current = &doc.value;
        for segment in path {
            current = current.get(segment)?;
        }
        Some(current.clone())
    }

    pub fn put_nested(
        &self,
        key: &str,
        path: &[&str],
        value: Value,
        modified_by: String,
    ) -> Result<u64, StorageError> {
        let mut data = self.data.write();
        let mut ver = self.version.write();

        let doc = data
            .get_mut(key)
            .ok_or_else(|| StorageError::KeyNotFound(key.to_string()))?;

        let mut current = &mut doc.value;
        for (i, segment) in path.iter().enumerate() {
            if i == path.len() - 1 {
                if let Some(obj) = current.as_object_mut() {
                    obj.insert(segment.to_string(), value.clone());
                } else {
                    return Err(StorageError::KeyNotFound(format!(
                        "path segment '{}' is not an object",
                        segment
                    )));
                }
            } else {
                current = current
                    .get_mut(segment)
                    .ok_or_else(|| StorageError::KeyNotFound(segment.to_string()))?;
            }
        }

        *ver += 1;
        doc.version = *ver;
        doc.last_modified_by = modified_by;

        Ok(*ver)
    }

    pub fn keys(&self) -> Vec<String> {
        self.data.read().keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    pub fn snapshot(&self) -> BTreeMap<String, Document> {
        self.data.read().clone()
    }

    pub fn restore(&self, data: BTreeMap<String, Document>) {
        let max_ver = data.values().map(|d| d.version).max().unwrap_or(0);
        *self.data.write() = data;
        *self.version.write() = max_ver;
    }

    pub fn current_version(&self) -> u64 {
        *self.version.read()
    }

    pub fn clear(&self) {
        self.data.write().clear();
        *self.version.write() = 0;
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_crud() {
        let store = MemStore::new();

        store.put(
            "agent-1".into(),
            json!({"status": "running"}),
            "agent-1".into(),
        );
        let doc = store.get("agent-1").unwrap();
        assert_eq!(doc.value, json!({"status": "running"}));
        assert_eq!(doc.version, 1);

        store.put(
            "agent-1".into(),
            json!({"status": "done"}),
            "agent-1".into(),
        );
        let doc = store.get("agent-1").unwrap();
        assert_eq!(doc.value, json!({"status": "done"}));
        assert_eq!(doc.version, 2);

        assert!(store.delete("agent-1"));
        assert!(store.get("agent-1").is_none());
    }

    #[test]
    fn test_nested_operations() {
        let store = MemStore::new();

        store.put(
            "plan".into(),
            json!({
                "steps": {
                    "step-1": {"status": "pending", "tool": "search"},
                    "step-2": {"status": "pending", "tool": "analyze"}
                },
                "metadata": {"owner": "agent-0"}
            }),
            "agent-0".into(),
        );

        let tool = store
            .get_nested("plan", &["steps", "step-1", "tool"])
            .unwrap();
        assert_eq!(tool, json!("search"));

        store
            .put_nested(
                "plan",
                &["steps", "step-1", "status"],
                json!("complete"),
                "agent-1".into(),
            )
            .unwrap();

        let status = store
            .get_nested("plan", &["steps", "step-1", "status"])
            .unwrap();
        assert_eq!(status, json!("complete"));
    }

    #[test]
    fn test_snapshot_and_restore() {
        let store = MemStore::new();
        store.put("a".into(), json!(1), "test".into());
        store.put("b".into(), json!(2), "test".into());

        let snap = store.snapshot();

        let store2 = MemStore::new();
        store2.restore(snap);

        assert_eq!(store2.get("a").unwrap().value, json!(1));
        assert_eq!(store2.get("b").unwrap().value, json!(2));
        assert_eq!(store2.current_version(), 2);
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(MemStore::new());
        store.put("shared".into(), json!(42), "setup".into());

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let s = Arc::clone(&store);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        let _ = s.get("shared");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
