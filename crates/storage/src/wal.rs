use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::StorageError;

/// On-disk frame layout:
///   [4 bytes] crc32 of (len ++ payload)
///   [4 bytes] payload length (little-endian u32)
///   [N bytes] payload (bincode-encoded WalEntry)
const HEADER_SIZE: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

pub struct Wal {
    dir: PathBuf,
    writer: BufWriter<File>,
    size: u64,
    entry_count: u64,
}

impl Wal {
    pub fn open(dir: &Path) -> Result<Self, StorageError> {
        fs::create_dir_all(dir)?;
        let path = dir.join("wal.log");

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let size = file.metadata()?.len();
        let entry_count = Self::count_entries(&path)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            writer: BufWriter::new(file),
            size,
            entry_count,
        })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<(), StorageError> {
        let payload =
            bincode::serialize(entry).map_err(|e| StorageError::Serialize(e.to_string()))?;

        let len = payload.len() as u32;
        let mut to_checksum = Vec::with_capacity(4 + payload.len());
        to_checksum.extend_from_slice(&len.to_le_bytes());
        to_checksum.extend_from_slice(&payload);

        let crc = crc32fast::hash(&to_checksum);

        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;

        self.size += (HEADER_SIZE + payload.len()) as u64;
        self.entry_count += 1;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        use std::io::Write;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<WalEntry>, StorageError> {
        let path = self.dir.join("wal.log");
        Self::read_entries(&path)
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn size_bytes(&self) -> u64 {
        self.size
    }

    pub fn truncate_after(&mut self, last_index: u64) -> Result<(), StorageError> {
        let path = self.dir.join("wal.log");
        let entries = Self::read_entries(&path)?;

        let keep: Vec<_> = entries
            .into_iter()
            .filter(|e| e.index <= last_index)
            .collect();

        drop(std::mem::replace(
            &mut self.writer,
            BufWriter::new(File::create(path.with_extension("tmp"))?),
        ));

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        self.writer = BufWriter::new(file);
        self.size = 0;
        self.entry_count = 0;

        for entry in &keep {
            self.append(entry)?;
        }
        self.sync()?;

        let tmp = path.with_extension("tmp");
        if tmp.exists() {
            let _ = fs::remove_file(tmp);
        }

        Ok(())
    }

    fn read_entries(path: &Path) -> Result<Vec<WalEntry>, StorageError> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };

        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Ok(vec![]);
        }

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut offset: u64 = 0;

        loop {
            if offset >= file_len {
                break;
            }

            let remaining = file_len - offset;
            if remaining < HEADER_SIZE as u64 {
                // partial header at end of file — truncated write, skip
                break;
            }

            let mut header = [0u8; HEADER_SIZE];
            match reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

            if len == 0 || len > 64 * 1024 * 1024 {
                // sanity check: skip obviously corrupted length
                break;
            }

            if remaining < (HEADER_SIZE + len) as u64 {
                // incomplete payload — truncated write
                break;
            }

            let mut payload = vec![0u8; len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let mut to_checksum = Vec::with_capacity(4 + len);
            to_checksum.extend_from_slice(&(len as u32).to_le_bytes());
            to_checksum.extend_from_slice(&payload);
            let computed_crc = crc32fast::hash(&to_checksum);

            if computed_crc != stored_crc {
                // crc mismatch — stop here, everything after is suspect
                break;
            }

            let entry: WalEntry =
                bincode::deserialize(&payload).map_err(|e| StorageError::CorruptedEntry {
                    offset,
                    reason: e.to_string(),
                })?;

            entries.push(entry);
            offset += (HEADER_SIZE + len) as u64;
        }

        Ok(entries)
    }

    fn count_entries(path: &Path) -> Result<u64, StorageError> {
        Ok(Self::read_entries(path)?.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use tempfile::TempDir;

    fn make_entry(index: u64, term: u64, data: &[u8]) -> WalEntry {
        WalEntry {
            index,
            term,
            data: data.to_vec(),
        }
    }

    #[test]
    fn test_append_and_replay() {
        let tmp = TempDir::new().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        let entries = vec![
            make_entry(1, 1, b"set x=1"),
            make_entry(2, 1, b"set y=2"),
            make_entry(3, 2, b"del x"),
        ];

        for e in &entries {
            wal.append(e).unwrap();
        }
        wal.sync().unwrap();

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed, entries);
    }

    #[test]
    fn test_crash_recovery_truncated_write() {
        let tmp = TempDir::new().unwrap();

        {
            let mut wal = Wal::open(tmp.path()).unwrap();
            wal.append(&make_entry(1, 1, b"good")).unwrap();
            wal.sync().unwrap();
        }

        // simulate a torn write by appending garbage
        let path = tmp.path().join("wal.log");
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xff; 20]).unwrap();
        file.sync_all().unwrap();

        let wal = Wal::open(tmp.path()).unwrap();
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data, b"good");
    }

    #[test]
    fn test_crc_corruption_detected() {
        let tmp = TempDir::new().unwrap();

        {
            let mut wal = Wal::open(tmp.path()).unwrap();
            wal.append(&make_entry(1, 1, b"first")).unwrap();
            wal.append(&make_entry(2, 1, b"second")).unwrap();
            wal.sync().unwrap();
        }

        // corrupt the crc of the second entry
        let path = tmp.path().join("wal.log");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        // read first entry to find its size
        let mut header = [0u8; HEADER_SIZE];
        file.read_exact(&mut header).unwrap();
        let first_payload_len =
            u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as u64;
        let second_entry_offset = HEADER_SIZE as u64 + first_payload_len;

        // flip a bit in the second entry's crc
        file.seek(SeekFrom::Start(second_entry_offset)).unwrap();
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes).unwrap();
        crc_bytes[0] ^= 0x01;
        file.seek(SeekFrom::Start(second_entry_offset)).unwrap();
        file.write_all(&crc_bytes).unwrap();
        file.sync_all().unwrap();

        let wal = Wal::open(tmp.path()).unwrap();
        let entries = wal.replay().unwrap();
        // should recover only the first entry
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data, b"first");
    }

    #[test]
    fn test_truncate_after() {
        let tmp = TempDir::new().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        for i in 1..=5 {
            wal.append(&make_entry(i, 1, format!("entry-{}", i).as_bytes()))
                .unwrap();
        }
        wal.sync().unwrap();

        wal.truncate_after(3).unwrap();
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.last().unwrap().index, 3);
    }

    #[test]
    fn test_empty_wal_replay() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::open(tmp.path()).unwrap();
        let entries = wal.replay().unwrap();
        assert!(entries.is_empty());
        assert_eq!(wal.entry_count(), 0);
    }

    #[test]
    fn test_reopen_preserves_entries() {
        let tmp = TempDir::new().unwrap();

        {
            let mut wal = Wal::open(tmp.path()).unwrap();
            wal.append(&make_entry(1, 1, b"persistent")).unwrap();
            wal.sync().unwrap();
        }

        {
            let mut wal = Wal::open(tmp.path()).unwrap();
            assert_eq!(wal.entry_count(), 1);
            wal.append(&make_entry(2, 1, b"another")).unwrap();
            wal.sync().unwrap();
        }

        let wal = Wal::open(tmp.path()).unwrap();
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 2);
    }
}
