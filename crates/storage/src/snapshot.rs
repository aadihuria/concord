use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::store::Document;
use crate::StorageError;

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: BTreeMap<String, Document>,
}

const SNAPSHOT_MAGIC: &[u8; 4] = b"CSNP";

pub struct SnapshotManager {
    dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(dir: &Path) -> Self {
        fs::create_dir_all(dir).expect("failed to create snapshot dir");
        Self {
            dir: dir.to_path_buf(),
        }
    }

    pub fn save(&self, snapshot: &Snapshot) -> Result<PathBuf, StorageError> {
        let filename = format!(
            "snapshot-{:016x}-{:016x}.snap",
            snapshot.last_included_term, snapshot.last_included_index
        );
        let path = self.dir.join(&filename);
        let tmp_path = self.dir.join(format!("{}.tmp", filename));

        let payload = serde_json::to_vec(snapshot)
            .map_err(|e| StorageError::Serialize(e.to_string()))?;

        let crc = crc32fast::hash(&payload);

        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);

        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&crc.to_le_bytes())?;
        writer.write_all(&(payload.len() as u64).to_le_bytes())?;
        writer.write_all(&payload)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;

        fs::rename(&tmp_path, &path)?;

        Ok(path)
    }

    pub fn load_latest(&self) -> Result<Option<Snapshot>, StorageError> {
        let mut snapshots: Vec<_> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "snap")
                    .unwrap_or(false)
            })
            .collect();

        snapshots.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

        for entry in snapshots {
            match self.load_snapshot(&entry.path()) {
                Ok(snap) => return Ok(Some(snap)),
                Err(e) => {
                    tracing::warn!("skipping corrupted snapshot {:?}: {}", entry.path(), e);
                    continue;
                }
            }
        }

        Ok(None)
    }

    fn load_snapshot(&self, path: &Path) -> Result<Snapshot, StorageError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != SNAPSHOT_MAGIC {
            return Err(StorageError::Snapshot("invalid magic bytes".into()));
        }

        let mut crc_bytes = [0u8; 4];
        reader.read_exact(&mut crc_bytes)?;
        let stored_crc = u32::from_le_bytes(crc_bytes);

        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;

        let computed_crc = crc32fast::hash(&payload);
        if computed_crc != stored_crc {
            return Err(StorageError::Snapshot("crc mismatch".into()));
        }

        let snapshot: Snapshot = serde_json::from_slice(&payload)
            .map_err(|e| StorageError::Deserialize(e.to_string()))?;

        Ok(snapshot)
    }

    pub fn cleanup_old(&self, keep: usize) -> Result<usize, StorageError> {
        let mut snapshots: Vec<_> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "snap")
                    .unwrap_or(false)
            })
            .collect();

        snapshots.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

        let mut removed = 0;
        for entry in snapshots.into_iter().skip(keep) {
            fs::remove_file(entry.path())?;
            removed += 1;
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_snapshot(index: u64, term: u64) -> Snapshot {
        let mut data = BTreeMap::new();
        data.insert(
            "key1".into(),
            Document {
                value: json!({"status": "active"}),
                version: index,
                last_modified_by: "test".into(),
            },
        );
        Snapshot {
            last_included_index: index,
            last_included_term: term,
            data,
        }
    }

    #[test]
    fn test_save_and_load() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path());

        let snap = make_snapshot(100, 3);
        mgr.save(&snap).unwrap();

        let loaded = mgr.load_latest().unwrap().unwrap();
        assert_eq!(loaded.last_included_index, 100);
        assert_eq!(loaded.last_included_term, 3);
        assert_eq!(loaded.data.len(), 1);
    }

    #[test]
    fn test_loads_latest() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path());

        mgr.save(&make_snapshot(50, 2)).unwrap();
        mgr.save(&make_snapshot(100, 3)).unwrap();
        mgr.save(&make_snapshot(75, 2)).unwrap();

        let loaded = mgr.load_latest().unwrap().unwrap();
        assert_eq!(loaded.last_included_index, 100);
    }

    #[test]
    fn test_cleanup_old() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path());

        for i in 1..=5 {
            mgr.save(&make_snapshot(i * 10, 1)).unwrap();
        }

        let removed = mgr.cleanup_old(2).unwrap();
        assert_eq!(removed, 3);

        let snap_count = fs::read_dir(tmp.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .map(|ext| ext == "snap")
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(snap_count, 2);
    }

    #[test]
    fn test_corrupted_snapshot_skipped() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path());

        mgr.save(&make_snapshot(50, 1)).unwrap();
        let latest_path = mgr.save(&make_snapshot(100, 2)).unwrap();

        // corrupt the latest snapshot
        let mut contents = fs::read(&latest_path).unwrap();
        if contents.len() > 10 {
            contents[8] ^= 0xff;
        }
        fs::write(&latest_path, &contents).unwrap();

        let loaded = mgr.load_latest().unwrap().unwrap();
        assert_eq!(loaded.last_included_index, 50);
    }

    #[test]
    fn test_empty_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path());
        assert!(mgr.load_latest().unwrap().is_none());
    }
}
