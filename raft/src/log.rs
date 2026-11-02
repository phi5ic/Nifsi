use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use async_trait::async_trait;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("Database error: {0}")]
    DbError(#[from] rusqlite::Error),
    #[error("Serialization error: {0}")]
    SerdeError(#[from] bincode::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

#[async_trait]
pub trait LogStore: Send + Sync {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), LogError>;
    async fn get_entry(&self, index: u64) -> Result<Option<LogEntry>, LogError>;
    async fn last_index_and_term(&self) -> Result<(u64, u64), LogError>;
    async fn delete_from(&self, index: u64) -> Result<(), LogError>;
}

#[derive(Clone)]
pub struct SQLiteLogStore {
    conn: Arc<Mutex<Connection>>,
}

impl SQLiteLogStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, LogError> {
        let conn = Connection::open(path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS log (
                index_num INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                data BLOB NOT NULL
            )",
            [],
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

#[async_trait]
impl LogStore for SQLiteLogStore {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), LogError> {
        let conn = self.conn.clone();
        let entries = entries.to_vec();
        tokio::task::spawn_blocking(move || -> Result<(), LogError> {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction()?;
            for entry in entries {
                tx.execute(
                    "INSERT OR REPLACE INTO log (index_num, term, data) VALUES (?1, ?2, ?3)",
                    params![entry.index, entry.term, entry.data],
                )?;
            }
            tx.commit()?;
            Ok(())
        }).await.unwrap()
    }

    async fn get_entry(&self, index: u64) -> Result<Option<LogEntry>, LogError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<LogEntry>, LogError> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare("SELECT term, data FROM log WHERE index_num = ?1")?;
            let entry = stmt.query_row(params![index], |row| {
                Ok(LogEntry {
                    index,
                    term: row.get(0)?,
                    data: row.get(1)?,
                })
            }).optional()?;
            Ok(entry)
        }).await.unwrap()
    }

    async fn last_index_and_term(&self) -> Result<(u64, u64), LogError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<(u64, u64), LogError> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT index_num, term FROM log ORDER BY index_num DESC LIMIT 1"
            )?;
            let result = stmt.query_row([], |row| {
                Ok((row.get(0)?, row.get(1)?))
            }).optional()?;

            Ok(result.unwrap_or((0, 0)))
        }).await.unwrap()
    }

    async fn delete_from(&self, index: u64) -> Result<(), LogError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<(), LogError> {
            let conn = conn.lock().unwrap();
            conn.execute("DELETE FROM log WHERE index_num >= ?1", params![index])?;
            Ok(())
        }).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store_with(n: u64) -> SQLiteLogStore {
        let store = SQLiteLogStore::new(":memory:").unwrap();
        let entries: Vec<LogEntry> = (1..=n)
            .map(|i| LogEntry { term: 1, index: i, data: vec![i as u8] })
            .collect();
        store.append(&entries).await.unwrap();
        store
    }

    #[tokio::test]
    async fn test_empty_store_sentinel() {
        let store = SQLiteLogStore::new(":memory:").unwrap();

        assert_eq!(store.last_index_and_term().await.unwrap(), (0, 0),
            "empty store must return (0,0)");
        assert!(store.get_entry(0).await.unwrap().is_none(),
            "index 0 always None — log is 1-indexed");
        assert!(store.get_entry(1).await.unwrap().is_none(),
            "entry 1 must not exist before any append");
    }

    #[tokio::test]
    async fn test_single_entry_readback() {
        let store = SQLiteLogStore::new(":memory:").unwrap();
        store.append(&[LogEntry { term: 3, index: 1, data: b"hello".to_vec() }])
            .await.unwrap();

        assert_eq!(store.last_index_and_term().await.unwrap(), (1, 3));

        let e = store.get_entry(1).await.unwrap().expect("entry 1 must exist");
        assert_eq!(e.index, 1,         "index field");
        assert_eq!(e.term,  3,         "term field");
        assert_eq!(e.data,  b"hello",  "payload");

        // Adjacent slots must be absent
        assert!(store.get_entry(0).await.unwrap().is_none(), "index 0 always None");
        assert!(store.get_entry(2).await.unwrap().is_none(), "index 2 not written");
    }

    #[tokio::test]
    async fn test_sequential_every_index() {
        const N: u64 = 20;
        let store = store_with(N).await;

        assert_eq!(store.last_index_and_term().await.unwrap(), (N, 1));

        for i in 1..=N {
            let e = store.get_entry(i).await.unwrap()
                .unwrap_or_else(|| panic!("entry {} missing", i));
            assert_eq!(e.index, i,            "entry {} index field", i);
            assert_eq!(e.term,  1,            "entry {} term",        i);
            assert_eq!(e.data,  vec![i as u8], "entry {} data",        i);
        }

        assert!(store.get_entry(N + 1).await.unwrap().is_none(),
            "one past end must be absent (off-by-one check)");
    }

    #[tokio::test]
    async fn test_batch_append_all_slots() {
        let store = SQLiteLogStore::new(":memory:").unwrap();
        let batch = vec![
            LogEntry { term: 1, index: 1, data: b"a".to_vec() },
            LogEntry { term: 1, index: 2, data: b"b".to_vec() },
            LogEntry { term: 2, index: 3, data: b"c".to_vec() },
            LogEntry { term: 2, index: 4, data: b"d".to_vec() },
            LogEntry { term: 3, index: 5, data: b"e".to_vec() },
        ];
        store.append(&batch).await.unwrap();

        assert_eq!(store.last_index_and_term().await.unwrap(), (5, 3));

        let checks: &[(u64, u64, &[u8])] = &[
            (1, 1, b"a"), (2, 1, b"b"), (3, 2, b"c"), (4, 2, b"d"), (5, 3, b"e"),
        ];
        for &(idx, term, data) in checks {
            let e = store.get_entry(idx).await.unwrap()
                .unwrap_or_else(|| panic!("batch entry {} missing", idx));
            assert_eq!(e.index, idx,  "batch[{}] index", idx);
            assert_eq!(e.term,  term, "batch[{}] term",  idx);
            assert_eq!(e.data,  data, "batch[{}] data",  idx);
        }
    }

    #[tokio::test]
    async fn test_delete_from_every_split_point() {
        const N: u64 = 8;

        for split in 1..=(N + 1) {
            let store = store_with(N).await;
            store.delete_from(split).await.unwrap();

            for i in 1..split {
                let e = store.get_entry(i).await.unwrap()
                    .unwrap_or_else(|| panic!(
                        "split={}: entry {} should survive but was deleted", split, i));
                assert_eq!(e.index, i,
                    "split={}: entry {} index wrong after delete", split, i);
            }

            for i in split..=(N + 1) {
                assert!(
                    store.get_entry(i).await.unwrap().is_none(),
                    "split={}: entry {} should be absent but still exists", split, i
                );
            }

            let expected_last = split.saturating_sub(1);
            let (last_idx, _) = store.last_index_and_term().await.unwrap();
            assert_eq!(last_idx, expected_last,
                "split={}: last_index wrong after delete_from", split);
        }
    }

    #[tokio::test]
    async fn test_delete_all_restores_empty_sentinel() {
        let store = store_with(10).await;
        store.delete_from(1).await.unwrap();

        assert_eq!(store.last_index_and_term().await.unwrap(), (0, 0),
            "after delete_from(1) store must look empty");

        for i in 0..=11 {
            assert!(store.get_entry(i).await.unwrap().is_none(),
                "entry {} must not exist after full truncation", i);
        }
    }

    #[tokio::test]
    async fn test_delete_beyond_end_is_noop() {
        let store = store_with(5).await;
        store.delete_from(6).await.unwrap();    // one past end
        store.delete_from(100).await.unwrap();  // well past end

        assert_eq!(store.last_index_and_term().await.unwrap(), (5, 1),
            "out-of-range delete must be a no-op");

        for i in 1..=5 {
            assert!(store.get_entry(i).await.unwrap().is_some(),
                "entry {} must still exist", i);
        }
    }

    #[tokio::test]
    async fn test_overwrite_single_entry() {
        let store = store_with(5).await;

        store.append(&[LogEntry { term: 9, index: 3, data: b"new".to_vec() }])
            .await.unwrap();

        let e = store.get_entry(3).await.unwrap().unwrap();
        assert_eq!(e.term,  9,       "overwritten: new term");
        assert_eq!(e.data,  b"new",  "overwritten: new payload");
        assert_eq!(e.index, 3,       "overwritten: index unchanged");

        let e2 = store.get_entry(2).await.unwrap().unwrap();
        assert_eq!(e2.term, 1, "entry 2 must keep original term");
        assert_eq!(e2.data, vec![2], "entry 2 must keep original data");

        let e4 = store.get_entry(4).await.unwrap().unwrap();
        assert_eq!(e4.term, 1, "entry 4 must keep original term");
        assert_eq!(e4.data, vec![4], "entry 4 must keep original data");
    }

    #[tokio::test]
    async fn test_last_index_tracks_terms_and_truncations() {
        let store = SQLiteLogStore::new(":memory:").unwrap();

        // Append three term-1 entries
        store.append(&[
            LogEntry { term: 1, index: 1, data: vec![1] },
            LogEntry { term: 1, index: 2, data: vec![2] },
            LogEntry { term: 1, index: 3, data: vec![3] },
        ]).await.unwrap();
        assert_eq!(store.last_index_and_term().await.unwrap(), (3, 1),
            "after term-1 batch");

        // Append two term-2 entries
        store.append(&[
            LogEntry { term: 2, index: 4, data: vec![4] },
            LogEntry { term: 2, index: 5, data: vec![5] },
        ]).await.unwrap();
        assert_eq!(store.last_index_and_term().await.unwrap(), (5, 2),
            "after term-2 batch");

        // Append one term-3 entry
        store.append(&[LogEntry { term: 3, index: 6, data: vec![6] }])
            .await.unwrap();
        assert_eq!(store.last_index_and_term().await.unwrap(), (6, 3),
            "after term-3 entry");

        // Truncate back to index 3 — crosses two term boundaries
        store.delete_from(4).await.unwrap();
        assert_eq!(store.last_index_and_term().await.unwrap(), (3, 1),
            "after truncation back into term-1, last must be (3, 1)");
    }

    #[tokio::test]
    async fn test_truncate_then_reappend_coherence() {
        let store = store_with(10).await;

        store.delete_from(6).await.unwrap();
        assert_eq!(store.last_index_and_term().await.unwrap(), (5, 1));

        let new: Vec<LogEntry> = (6..=10)
            .map(|i| LogEntry { term: 2, index: i, data: vec![i as u8 + 100] })
            .collect();
        store.append(&new).await.unwrap();
        assert_eq!(store.last_index_and_term().await.unwrap(), (10, 2));

        for i in 1..=5u64 {
            let e = store.get_entry(i).await.unwrap()
                .unwrap_or_else(|| panic!("entry {} lost after re-append", i));
            assert_eq!(e.term, 1,             "entry {} should be term 1 (original)", i);
            assert_eq!(e.data, vec![i as u8], "entry {} data should be original",    i);
        }

        for i in 6..=10u64 {
            let e = store.get_entry(i).await.unwrap()
                .unwrap_or_else(|| panic!("new entry {} missing", i));
            assert_eq!(e.term, 2,                    "new entry {} should be term 2",   i);
            assert_eq!(e.data, vec![i as u8 + 100],  "new entry {} data should be new", i);
        }
    }

    #[tokio::test]
    async fn test_sparse_indexes_and_gaps() {
        let store = SQLiteLogStore::new(":memory:").unwrap();
        store.append(&[
            LogEntry { term: 1, index: 1, data: b"one".to_vec()   },
            LogEntry { term: 1, index: 3, data: b"three".to_vec() },
            LogEntry { term: 1, index: 5, data: b"five".to_vec()  },
        ]).await.unwrap();

        assert!(store.get_entry(2).await.unwrap().is_none(), "gap at index 2");
        assert!(store.get_entry(4).await.unwrap().is_none(), "gap at index 4");

        assert_eq!(store.get_entry(1).await.unwrap().unwrap().data, b"one");
        assert_eq!(store.get_entry(3).await.unwrap().unwrap().data, b"three");
        assert_eq!(store.get_entry(5).await.unwrap().unwrap().data, b"five");

        assert_eq!(store.last_index_and_term().await.unwrap(), (5, 1));
    }
}
