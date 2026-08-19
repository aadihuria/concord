//! Integration tests exercising the WAL, snapshot manager, and in-memory
//! store together the way the server crate actually wires them: writes go
//! through the WAL first, get applied to the store, and get periodically
//! compacted into a snapshot so the WAL doesn't grow unbounded. The unit
//! tests inside each module already cover each piece in isolation; these
//! cover the composition, since that's where recovery bugs actually hide.

use serde::{Deserialize, Serialize};
use serde_json::json;

use concord_storage::snapshot::Snapshot;
use concord_storage::wal::WalEntry;
use concord_storage::{MemStore, SnapshotManager, Wal};

/// Stand-in for what the real system stores inside `WalEntry::data` (there
/// it's a serialized Raft `Command`; here a minimal put/delete is enough to
/// prove WAL + snapshot + store recover correctly together).
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Op {
    Put { key: String, value: serde_json::Value },
    Delete { key: String },
}

fn apply(store: &MemStore, op: &Op) {
    match op {
        Op::Put { key, value } => {
            store.put(key.clone(), value.clone(), "test".into());
        }
        Op::Delete { key } => {
            store.delete(key);
        }
    }
}

#[test]
fn wal_replay_rebuilds_store_state_after_restart() {
    let tmp = tempfile::TempDir::new().unwrap();

    {
        let mut wal = Wal::open(tmp.path()).unwrap();
        let ops = [
            Op::Put {
                key: "a".into(),
                value: json!(1),
            },
            Op::Put {
                key: "b".into(),
                value: json!(2),
            },
            Op::Delete { key: "a".into() },
        ];
        for (i, op) in ops.iter().enumerate() {
            let data = serde_json::to_vec(op).unwrap();
            wal.append(&WalEntry {
                index: i as u64 + 1,
                term: 1,
                data,
            })
            .unwrap();
        }
        wal.sync().unwrap();
    }

    // Simulate a process restart: reopen the WAL and rebuild the store from
    // nothing but what's on disk.
    let wal = Wal::open(tmp.path()).unwrap();
    let store = MemStore::new();
    for entry in wal.replay().unwrap() {
        let op: Op = serde_json::from_slice(&entry.data).unwrap();
        apply(&store, &op);
    }

    assert!(store.get("a").is_none(), "a was deleted, should not survive replay");
    assert_eq!(store.get("b").unwrap().value, json!(2));
}

#[test]
fn snapshot_plus_wal_tail_reconstructs_state_after_compaction() {
    let tmp = tempfile::TempDir::new().unwrap();
    let snap_mgr = SnapshotManager::new(&tmp.path().join("snapshots"));

    // Entries 1..=3 get compacted into a snapshot.
    let compacted = MemStore::new();
    compacted.put("a".into(), json!(1), "test".into());
    compacted.put("b".into(), json!(2), "test".into());
    compacted.put("c".into(), json!(3), "test".into());

    snap_mgr
        .save(&Snapshot {
            last_included_index: 3,
            last_included_term: 1,
            data: compacted.snapshot(),
        })
        .unwrap();

    // Entries 4..=5 arrive after the snapshot and stay in the WAL tail.
    let mut wal = Wal::open(&tmp.path().join("wal")).unwrap();
    let tail_ops: Vec<(u64, Op)> = vec![
        (
            4,
            Op::Put {
                key: "d".into(),
                value: json!(4),
            },
        ),
        (5, Op::Delete { key: "b".into() }),
    ];
    for (index, op) in &tail_ops {
        let data = serde_json::to_vec(op).unwrap();
        wal.append(&WalEntry {
            index: *index,
            term: 1,
            data,
        })
        .unwrap();
    }
    wal.sync().unwrap();

    // Recovery: load the snapshot, then replay only the WAL entries that
    // came after it — a real node must skip anything the snapshot already
    // covers, or a delete like this one would double-apply harmlessly but
    // a non-idempotent op would not.
    let loaded = snap_mgr.load_latest().unwrap().unwrap();
    let recovered = MemStore::new();
    recovered.restore(loaded.data);

    let wal = Wal::open(&tmp.path().join("wal")).unwrap();
    for entry in wal.replay().unwrap() {
        if entry.index <= loaded.last_included_index {
            continue;
        }
        let op: Op = serde_json::from_slice(&entry.data).unwrap();
        apply(&recovered, &op);
    }

    assert_eq!(recovered.get("a").unwrap().value, json!(1));
    assert_eq!(recovered.get("d").unwrap().value, json!(4));
    assert!(
        recovered.get("b").is_none(),
        "b should have been deleted by the WAL tail"
    );
}

#[test]
fn wal_truncate_after_matches_a_raft_log_conflict_rollback() {
    // Mirrors what a Raft follower does when the leader's prev-log-term
    // check fails: keep everything up to the divergence point, drop the rest.
    let tmp = tempfile::TempDir::new().unwrap();
    let mut wal = Wal::open(tmp.path()).unwrap();

    for i in 1..=5u64 {
        let op = Op::Put {
            key: format!("k{i}"),
            value: json!(i),
        };
        let data = serde_json::to_vec(&op).unwrap();
        wal.append(&WalEntry {
            index: i,
            term: 1,
            data,
        })
        .unwrap();
    }
    wal.sync().unwrap();

    wal.truncate_after(2).unwrap();

    let store = MemStore::new();
    for entry in wal.replay().unwrap() {
        let op: Op = serde_json::from_slice(&entry.data).unwrap();
        apply(&store, &op);
    }

    assert_eq!(store.len(), 2);
    assert!(store.get("k3").is_none());
    assert_eq!(store.get("k1").unwrap().value, json!(1));
}
