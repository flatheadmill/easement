// Transcript authority. Wicket owns the canonical transcript — deduplication,
// persistence, history replay, and normalization all live here. The transcript
// is stored at ~/.local/state/puzzle/<slug>/transcript.jsonl and survives
// across windows and machine migrations.
//
// The deduplication strategy uses a boundary UUID. When Easement replays
// transcript entries at the start of a round, the transcript layer buffers
// them until the boundary UUID (captured from the first assistant stdout
// event) is found. Everything before the boundary is history replay and is
// skipped. Everything from the boundary forward is new content.
//
// On the first round (empty transcript), there is no history to skip, so
// all entries pass through as new.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::normalize::try_normalize;
use crate::parser::parse_line;
use crate::protocol::NormalizedEntry;

pub struct Transcript {
    entries: Vec<serde_json::Value>,
    seq: u64,
    boundary_uuid: Option<String>,
    boundary_found: bool,
    buffer: Vec<serde_json::Value>,
    path: PathBuf,
}

impl Transcript {
    pub fn new(slug: &str) -> Self {
        let home = std::env::var("HOME").expect("HOME not set");
        let dir = std::path::Path::new(&home)
            .join(".local")
            .join("state")
            .join("puzzle")
            .join(slug);
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("transcript.jsonl");

        Self {
            entries: Vec::new(),
            seq: 0,
            boundary_uuid: None,
            boundary_found: false,
            buffer: Vec::new(),
            path,
        }
    }

    /// Load existing transcript from disk. Returns normalized entries for
    /// streaming to the client on connect.
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
                    // Load into memory without persisting — the data is
                    // already on disk.
                    results.extend(self.ingest(data));
                }
                Err(e) => {
                    tracing::warn!("transcript line parse error: {}", e);
                }
            }
        }

        tracing::info!(
            loaded = self.entries.len(),
            normalized = results.len(),
            "transcript loaded from disk"
        );
        results
    }

    /// Call at the start of each round, before spawning Easement.
    pub fn begin_round(&mut self) {
        self.boundary_uuid = None;
        self.boundary_found = self.entries.is_empty();
        self.buffer.clear();
        tracing::info!(
            entries = self.entries.len(),
            boundary_found = self.boundary_found,
            "begin round (first={})",
            self.entries.is_empty()
        );
    }

    /// Set the boundary UUID from the first assistant stdout event.
    /// Processes any buffered entries and returns new normalized entries.
    pub fn set_boundary(&mut self, uuid: String) -> Vec<NormalizedEntry> {
        tracing::info!(
            uuid = %uuid,
            buffered = self.buffer.len(),
            "boundary uuid set"
        );
        self.boundary_uuid = Some(uuid);
        self.process_buffer()
    }

    /// Handle a transcript entry from Easement. Returns new normalized
    /// entries if the data contains new content past the boundary.
    pub fn handle_entry(&mut self, data: serde_json::Value) -> Vec<NormalizedEntry> {
        let entry_type = data
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        if self.boundary_found {
            tracing::debug!(
                entry_type = %entry_type,
                entries = self.entries.len(),
                "accepting transcript entry"
            );
            return self.accept(data);
        }

        self.buffer.push(data);
        tracing::debug!(
            entry_type = %entry_type,
            buffered = self.buffer.len(),
            has_boundary = self.boundary_uuid.is_some(),
            "buffering transcript entry"
        );

        if self.boundary_uuid.is_some() {
            return self.process_buffer();
        }

        vec![]
    }

    /// The raw entries for transcript transfer to Easement when forking
    /// to a new machine (no session ID yet).
    pub fn entries(&self) -> &[serde_json::Value] {
        &self.entries
    }

    fn accept(&mut self, data: serde_json::Value) -> Vec<NormalizedEntry> {
        self.persist(&data);
        self.ingest(data)
    }

    /// Add to in-memory state and normalize, without persisting.
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

    fn process_buffer(&mut self) -> Vec<NormalizedEntry> {
        let uuid = match &self.boundary_uuid {
            Some(u) => u.clone(),
            None => return vec![],
        };

        let boundary_pos = self.buffer.iter().position(|data| {
            data.get("uuid").and_then(|v| v.as_str()) == Some(uuid.as_str())
        });

        let boundary_pos = match boundary_pos {
            Some(pos) => pos,
            None => {
                tracing::debug!(
                    buffered = self.buffer.len(),
                    boundary_uuid = %uuid,
                    "boundary not in buffer yet, waiting"
                );
                return vec![];
            }
        };

        self.boundary_found = true;

        // The new turn includes a user entry before the assistant boundary.
        let start = self.buffer[..boundary_pos]
            .iter()
            .rposition(|data| {
                data.get("type").and_then(|v| v.as_str()) == Some("user")
            })
            .unwrap_or(boundary_pos);

        let skipped = start;
        let new_content: Vec<_> = self.buffer.drain(start..).collect();
        let accepted_count = new_content.len();
        self.buffer.clear();

        tracing::info!(
            skipped,
            accepted = accepted_count,
            "boundary found, processing new content"
        );

        let mut results = vec![];
        for data in new_content {
            results.extend(self.accept(data));
        }
        results
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
            .join("puzzle");

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
