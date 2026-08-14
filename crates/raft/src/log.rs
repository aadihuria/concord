use serde::{Deserialize, Serialize};

use concord_crdt::CrdtOp;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    CrdtOp(CrdtOp),
    Noop,
}

/// In-memory Raft log with support for truncation and compaction.
pub struct RaftLog {
    entries: Vec<LogEntry>,
    snapshot_last_index: u64,
    snapshot_last_term: u64,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            snapshot_last_index: 0,
            snapshot_last_term: 0,
        }
    }

    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_last_index)
    }

    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_last_term)
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_last_index {
            return Some(self.snapshot_last_term);
        }
        self.entry_at(index).map(|e| e.term)
    }

    pub fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.snapshot_last_index || index == 0 {
            return None;
        }
        let offset = (index - self.snapshot_last_index - 1) as usize;
        self.entries.get(offset)
    }

    pub fn entries_from(&self, start_index: u64) -> &[LogEntry] {
        if start_index <= self.snapshot_last_index {
            return &self.entries;
        }
        let offset = (start_index - self.snapshot_last_index - 1) as usize;
        if offset >= self.entries.len() {
            return &[];
        }
        &self.entries[offset..]
    }

    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    pub fn append_entries(&mut self, _prev_index: u64, entries: Vec<LogEntry>) {
        for entry in entries {
            let offset = if entry.index <= self.snapshot_last_index {
                continue;
            } else {
                (entry.index - self.snapshot_last_index - 1) as usize
            };

            if offset < self.entries.len() {
                if self.entries[offset].term != entry.term {
                    self.entries.truncate(offset);
                    self.entries.push(entry);
                }
            } else {
                self.entries.push(entry);
            }
        }
    }

    pub fn truncate_from(&mut self, index: u64) {
        if index <= self.snapshot_last_index {
            self.entries.clear();
            return;
        }
        let offset = (index - self.snapshot_last_index - 1) as usize;
        self.entries.truncate(offset);
    }

    pub fn compact(&mut self, through_index: u64, through_term: u64) {
        let offset = if through_index <= self.snapshot_last_index {
            return;
        } else {
            (through_index - self.snapshot_last_index) as usize
        };

        if offset <= self.entries.len() {
            self.entries = self.entries.split_off(offset);
        } else {
            self.entries.clear();
        }

        self.snapshot_last_index = through_index;
        self.snapshot_last_term = through_term;
    }

    pub fn snapshot_last_index(&self) -> u64 {
        self.snapshot_last_index
    }

    pub fn snapshot_last_term(&self) -> u64 {
        self.snapshot_last_term
    }

    pub fn set_snapshot_meta(&mut self, last_index: u64, last_term: u64) {
        self.snapshot_last_index = last_index;
        self.snapshot_last_term = last_term;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use concord_crdt::{CrdtOp, state::StateKey};

    fn make_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            command: Command::Noop,
        }
    }

    #[allow(dead_code)]
    fn make_crdt_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            command: Command::CrdtOp(CrdtOp::SetRegister {
                key: StateKey::new("test", "key"),
                value: json!(index),
                timestamp: index,
                node_id: "test".into(),
            }),
        }
    }

    #[test]
    fn test_append_and_read() {
        let mut log = RaftLog::new();
        log.append(make_entry(1, 1));
        log.append(make_entry(2, 1));
        log.append(make_entry(3, 2));

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(3), Some(2));
    }

    #[test]
    fn test_truncate() {
        let mut log = RaftLog::new();
        for i in 1..=5 {
            log.append(make_entry(i, 1));
        }

        log.truncate_from(4);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn test_compact() {
        let mut log = RaftLog::new();
        for i in 1..=10 {
            log.append(make_entry(i, 1));
        }

        log.compact(5, 1);
        assert_eq!(log.snapshot_last_index(), 5);
        assert_eq!(log.len(), 5);
        assert!(log.entry_at(5).is_none());
        assert!(log.entry_at(6).is_some());
        assert_eq!(log.entry_at(6).unwrap().index, 6);
    }

    #[test]
    fn test_append_entries_conflict_resolution() {
        let mut log = RaftLog::new();
        log.append(make_entry(1, 1));
        log.append(make_entry(2, 1));
        log.append(make_entry(3, 1));

        // leader sends entries with different term at index 2
        let new_entries = vec![
            LogEntry {
                index: 2,
                term: 2,
                command: Command::Noop,
            },
            LogEntry {
                index: 3,
                term: 2,
                command: Command::Noop,
            },
        ];

        log.append_entries(1, new_entries);
        assert_eq!(log.term_at(2), Some(2));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn test_entries_from() {
        let mut log = RaftLog::new();
        for i in 1..=5 {
            log.append(make_entry(i, 1));
        }

        let from_3 = log.entries_from(3);
        assert_eq!(from_3.len(), 3);
        assert_eq!(from_3[0].index, 3);
    }
}
