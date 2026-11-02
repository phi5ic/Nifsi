use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<String>,
}

// HardStateStore — crash-safe persistence of (current_term, voted_for)
pub trait HardStateStore: Send + Sync {
    fn save(&self, term: u64, voted_for: Option<&str>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn load(&self) -> Result<(u64, Option<String>), Box<dyn std::error::Error + Send + Sync>>;
}
#[derive(Clone, Default)]
pub struct MemHardStateStore {
    inner: Arc<Mutex<(u64, Option<String>)>>,
}

impl MemHardStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HardStateStore for MemHardStateStore {
    fn save(&self, term: u64, voted_for: Option<&str>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut g = self.inner.lock().unwrap();
        *g = (term, voted_for.map(|s| s.to_owned()));
        Ok(())
    }

    fn load(&self) -> Result<(u64, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.inner.lock().unwrap().clone())
    }
}
// MetadataStore — the replicated state machine interface

pub trait MetadataStore: Send + Sync {
    fn apply(&self, index: u64, entry: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn last_applied(&self) -> u64;
}
pub struct NoopMetadataStore;
impl MetadataStore for NoopMetadataStore {
    fn apply(&self, _index: u64, _entry: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
    fn last_applied(&self) -> u64 { 0 }
}
// SqliteHardStateStore — durable crash-safe persistence
#[derive(Clone)]
pub struct SqliteHardStateStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteHardStateStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS hard_state (
                 key   TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl HardStateStore for SqliteHardStateStore {
    fn save(
        &self,
        term: u64,
        voted_for: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO hard_state (key, value) VALUES ('term', ?1)",
            params![term.to_string()],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO hard_state (key, value) VALUES ('voted_for', ?1)",
            params![voted_for.unwrap_or("")],
        )?;
        Ok(())
    }

    fn load(&self) -> Result<(u64, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.conn.lock().unwrap();

        let term: u64 = conn
            .query_row(
                "SELECT value FROM hard_state WHERE key = 'term'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let voted_for: Option<String> = conn
            .query_row(
                "SELECT value FROM hard_state WHERE key = 'voted_for'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|s| if s.is_empty() { None } else { Some(s) });

        Ok((term, voted_for))
    }
}

