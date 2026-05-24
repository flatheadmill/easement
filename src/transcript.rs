// Transcript authority. Wicket owns the canonical transcript as a Vec in
// memory with a UUID index. Persistence is append-only to a JSONL file.
//
// Easement streams the entire CLI transcript from the first line. For each
// entry: if its UUID is in our index, it is replay — skip. If its UUID is
// not in our index, assert that its parentUuid is the UUID of the last
// chained entry in our Vec. If it is, append. If it is not, crash.
//
// Entries without UUIDs (queue-operation, last-prompt) do not participate
// in the chain. They are accepted silently.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::normalize::try_normalize;
use crate::parser::parse_line;
use crate::protocol::NormalizedEntry;

pub struct Transcript {
    entries: Vec<serde_json::Value>,
    uuid_index: HashMap<String, usize>,
    seq: u64,
    path: PathBuf,
}

impl Transcript {
    pub fn new(slug: &str, timestamp: Option<&str>) -> Self {
        let home = std::env::var("HOME").expect("HOME not set");
        let dir = std::path::Path::new(&home)
            .join(".local")
            .join("state")
            .join("wicket")
            .join(slug);
        let _ = fs::create_dir_all(&dir);
        let filename = match timestamp {
            Some(ts) => format!("{}.jsonl", ts),
            None => "transcript.jsonl".to_string(),
        };
        let path = dir.join(filename);

        Self {
            entries: Vec::new(),
            uuid_index: HashMap::new(),
            seq: 0,
            path,
        }
    }

    pub fn load_history(&mut self) -> Vec<NormalizedEntry> {
        let content = match fs::read_to_string(&self.path) {
            Ok(c) => c,
            Err(_) => return vec![],
        };

        let mut results = vec![];
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(line) {
                Ok(data) => {
                    self.index_entry(&data);
                    results.extend(self.ingest(data));
                }
                Err(e) => {
                    tracing::warn!("transcript line parse error: {}", e);
                }
            }
        }

        tracing::info!(
            entries = self.entries.len(),
            indexed = self.uuid_index.len(),
            normalized = results.len(),
            "transcript loaded from disk"
        );
        results
    }

    pub fn handle_entry(&mut self, data: serde_json::Value) -> Vec<NormalizedEntry> {
        let entry_uuid = data.get("uuid").and_then(|v| v.as_str()).map(|s| s.to_string());
        let entry_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("unknown");

        match &entry_uuid {
            Some(uuid) if self.uuid_index.contains_key(uuid) => {
                tracing::debug!(uuid = %uuid, entry_type = %entry_type, "replay, skipping");
                vec![]
            }
            Some(uuid) => {
                let parent_uuid = data.get("parentUuid").and_then(|v| v.as_str());
                let chain_head = self.chain_head();

                match (parent_uuid, chain_head) {
                    (Some(parent), Some(head)) if parent == head => {
                        tracing::debug!(
                            uuid = %uuid,
                            parent = %parent,
                            entry_type = %entry_type,
                            "new entry, chain valid"
                        );
                    }
                    (None, None) => {
                        tracing::debug!(
                            uuid = %uuid,
                            entry_type = %entry_type,
                            "new root entry"
                        );
                    }
                    (None, Some(head)) => {
                        tracing::debug!(
                            uuid = %uuid,
                            chain_head = %head,
                            entry_type = %entry_type,
                            "new entry without parentUuid, chain head exists"
                        );
                    }
                    (Some(parent), chain_head) => {
                        tracing::error!(
                            uuid = %uuid,
                            parent_uuid = %parent,
                            chain_head = ?chain_head,
                            entry_type = %entry_type,
                            "CHAIN BREAK: parentUuid does not match chain head"
                        );
                        panic!(
                            "transcript chain break: entry {} parentUuid {} does not match chain head {:?}",
                            uuid, parent, chain_head
                        );
                    }
                }

                self.index_entry(&data);
                self.persist(&data);
                self.ingest(data)
            }
            None => {
                tracing::debug!(entry_type = %entry_type, "entry without uuid, dropping");
                vec![]
            }
        }
    }

    pub fn entries(&self) -> &[serde_json::Value] {
        &self.entries
    }

    fn chain_head(&self) -> Option<&str> {
        self.entries.iter().rev().find_map(|e| {
            e.get("uuid").and_then(|v| v.as_str())
        })
    }

    fn index_entry(&mut self, data: &serde_json::Value) {
        if let Some(uuid) = data.get("uuid").and_then(|v| v.as_str()) {
            self.uuid_index.insert(uuid.to_string(), self.entries.len());
        }
    }

    fn ingest(&mut self, data: serde_json::Value) -> Vec<NormalizedEntry> {
        self.entries.push(data.clone());

        let json = serde_json::to_string(&data).unwrap_or_default();
        if let Some(entry) = parse_line(&json) {
            self.seq += 1;
            if let Some(normalized) = try_normalize(entry, self.seq) {
                return vec![normalized];
            }
        }

        vec![]
    }

    fn persist(&self, data: &serde_json::Value) {
        let mut line = match serde_json::to_string(data) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("transcript persist error: {}", e);
                return;
            }
        };
        line.push('\n');

        if let Some(parent) = self.path.parent() {
            let _ = fs::create_dir_all(parent);
        }

        match fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(mut file) => {
                if let Err(e) = file.write_all(line.as_bytes()) {
                    tracing::warn!("transcript write error: {}", e);
                }
            }
            Err(e) => {
                tracing::warn!("transcript open error: {}", e);
            }
        }
    }
}

// -- Session tracking --
//
// Sessions are keyed by target type: local sessions persist to disk, remote
// sessions are held in memory for the Wicket process lifetime.

pub struct Sessions {
    local: Option<String>,
    remote: Option<String>,
    slug: String,
    state_dir: PathBuf,
}

impl Sessions {
    pub fn new(slug: &str) -> Self {
        let home = std::env::var("HOME").expect("HOME not set");
        let state_dir = std::path::Path::new(&home)
            .join(".local")
            .join("state")
            .join("wicket");

        // Load the most recent local session from disk.
        let local = Self::read_latest(&state_dir, slug);

        Self {
            local,
            remote: None,
            slug: slug.to_string(),
            state_dir,
        }
    }

    pub fn local(&self) -> Option<&str> {
        self.local.as_deref()
    }

    pub fn remote(&self) -> Option<&str> {
        self.remote.as_deref()
    }

    pub fn set_local(&mut self, id: String) {
        self.record(&id);
        self.local = Some(id);
    }

    pub fn clear_local(&mut self) {
        tracing::info!("clearing stale local session id");
        self.local = None;
    }

    pub fn set_remote(&mut self, id: String) {
        self.remote = Some(id);
    }

    fn record(&self, session_id: &str) {
        let path = self.state_dir.join(&self.slug).join("sessions.jsonl");
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }

        let entry = serde_json::json!({
            "session_id": session_id,
            "timestamp": timestamp()
        });

        let mut line = serde_json::to_string(&entry).unwrap();
        line.push('\n');

        match fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(mut file) => {
                let _ = file.write_all(line.as_bytes());
            }
            Err(e) => {
                tracing::warn!("session record error: {}", e);
            }
        }
    }

    fn read_latest(state_dir: &std::path::Path, slug: &str) -> Option<String> {
        let path = state_dir.join(slug).join("sessions.jsonl");
        let content = fs::read_to_string(&path).ok()?;
        let last_line = content.lines().rev().find(|l| !l.trim().is_empty())?;
        let entry: serde_json::Value = serde_json::from_str(last_line).ok()?;
        entry
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }
}

fn timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
