// Transcript authority. Easement owns the canonical transcript as a Vec in memory with a UUID
// index. Persistence is append-only to a JSONL file.
//
// On load, the chain is validated. If our own file has a broken chain, that is a fatal error.
//
// After each round, the CLI's transcript file is reconciled against ours. The CLI gets one chance
// to prune — its cleanup transforms may shorten the chain. We accept the pruning, log the cut, and
// continue from the CLI's chain point. After reconciliation, any chain break is fatal.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

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
            .join("easement")
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
        let mut prev_uuid: Option<String> = None;
        let mut validating = true;

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let data: serde_json::Value = match serde_json::from_str(line) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("transcript line parse error: {}", e);
                    continue;
                }
            };

            let uuid = data
                .get("uuid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let parent = data
                .get("parentUuid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let entrypoint = data
                .get("entrypoint")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if entrypoint == "cli" {
                validating = false;
            }

            if validating {
                if let Some(ref u) = uuid {
                    match (&parent, &prev_uuid) {
                        (Some(p), Some(prev)) if p != prev => {
                            panic!(
                                "transcript file corrupt: entry {} parentUuid {} does not follow {}",
                                u, p, prev
                            );
                        }
                        (Some(_), None) if self.entries.is_empty() => {}
                        _ => {}
                    }
                }
            }

            self.index_entry(&data);
            results.extend(self.ingest(data));

            if uuid.is_some() {
                prev_uuid = uuid;
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

    /// Reconcile the CLI's transcript against ours after a round. The CLI
    /// may have pruned entries from the tail (cleanup transforms). We accept
    /// the pruning, back up what was cut, and append the genuinely new entries.
    pub fn reconcile_cli_file(
        &mut self,
        cli_path: &Path,
        backup_dir: &Path,
    ) -> Vec<NormalizedEntry> {
        let content = match fs::read_to_string(cli_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "cannot read CLI transcript");
                return vec![];
            }
        };

        // Back up our transcript before reconciliation.
        let backup_path = backup_dir.join("pre-reconcile.jsonl");
        if let Err(e) = fs::copy(&self.path, &backup_path) {
            tracing::warn!(error = %e, "cannot back up transcript");
        }

        // Collect new entries from the CLI file — entries not in our UUID index.
        let mut new_entries: Vec<serde_json::Value> = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let data: serde_json::Value = match serde_json::from_str(line) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let uuid = data.get("uuid").and_then(|v| v.as_str());
            match uuid {
                Some(u) if self.uuid_index.contains_key(u) => continue,
                Some(_) => new_entries.push(data),
                None => continue,
            }
        }

        if new_entries.is_empty() {
            return vec![];
        }

        // Find where the first new entry chains from.
        let first_parent = new_entries[0]
            .get("parentUuid")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let chain_head = self.chain_head().unwrap_or("");

        let results = if first_parent == chain_head {
            // Clean append — CLI's new entries chain from our head.
            let mut results = vec![];
            for data in new_entries {
                self.index_entry(&data);
                self.persist(&data);
                results.extend(self.ingest(data));
            }
            results
        } else if let Some(rewind_pos) = self.uuid_index.get(first_parent).copied() {
            // The CLI pruned entries from our tail. The new entries chain
            // from an earlier point in our transcript.
            let cut_count = self.entries.len() - rewind_pos - 1;
            tracing::warn!(
                rewind_to = %first_parent,
                cut_count,
                "CLI pruned entries, rewinding transcript"
            );

            // Truncate our in-memory state back to the rewind point.
            let cut_entries: Vec<serde_json::Value> =
                self.entries.drain(rewind_pos + 1..).collect();
            for entry in &cut_entries {
                if let Some(u) = entry.get("uuid").and_then(|v| v.as_str()) {
                    self.uuid_index.remove(u);
                }
            }

            // Save the cut entries.
            let cut_path = backup_dir.join("cut-entries.jsonl");
            if let Ok(mut file) = fs::File::create(&cut_path) {
                for entry in &cut_entries {
                    if let Ok(line) = serde_json::to_string(entry) {
                        let _ = writeln!(file, "{}", line);
                    }
                }
            }

            // Rewrite our transcript file from the rewound state.
            self.rewrite_file();

            // Now append the new entries.
            let mut results = vec![];
            for data in new_entries {
                self.index_entry(&data);
                self.persist(&data);
                results.extend(self.ingest(data));
            }
            results
        } else {
            // The first new entry chains from a UUID we've never seen.
            panic!(
                "CLI transcript has entries from unknown provenance: parentUuid {}",
                first_parent
            );
        };

        results
    }

    /// Strict entry handler for live operation. Any chain break is fatal.
    #[allow(dead_code)]
    pub fn handle_entry(&mut self, data: serde_json::Value) -> Vec<NormalizedEntry> {
        let entry_uuid = data
            .get("uuid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let entry_type = data
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

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
        self.entries
            .iter()
            .rev()
            .find_map(|e| e.get("uuid").and_then(|v| v.as_str()))
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

    fn rewrite_file(&self) {
        let tmp = self.path.with_extension("jsonl.tmp");
        match fs::File::create(&tmp) {
            Ok(mut file) => {
                for entry in &self.entries {
                    if let Ok(line) = serde_json::to_string(entry) {
                        let _ = writeln!(file, "{}", line);
                    }
                }
                if let Err(e) = fs::rename(&tmp, &self.path) {
                    tracing::warn!(error = %e, "cannot rename rewritten transcript");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot create temp file for transcript rewrite");
            }
        }
    }
}
