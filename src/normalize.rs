// Normalization layer. Sits between parser.rs (raw JSONL schema) and protocol.rs (wire types).
// Filters to the main conversation chain, drops sidechains and noise entry types, converts to
// NormalizedEntry.

use crate::parser::{
    AssistantContentBlock, AssistantEntry, Entry, TextBlock, ThinkingBlock, ToolResultBlock,
    ToolResultContent, ToolUseBlock, UserContent, UserContentBlock, UserEntry,
};
use crate::protocol::{ContentBlock, NormalizedEntry};

/// Filter and convert a raw parsed Entry into a NormalizedEntry for the client. Returns None for
/// entries that should be skipped (progress, system, sidechains, empty content). The seq number is
/// assigned by the caller (transcript layer) — this function just does the conversion.
pub fn try_normalize(entry: Entry, seq: u64) -> Option<NormalizedEntry> {
    match entry {
        Entry::User(user) if !user.is_sidechain => normalize_user(user, seq),
        Entry::Assistant(assistant) if !assistant.is_sidechain => {
            normalize_assistant(assistant, seq)
        }
        _ => None,
    }
}

fn normalize_user(entry: UserEntry, seq: u64) -> Option<NormalizedEntry> {
    let blocks = match entry.message.content {
        UserContent::Text(text) => vec![ContentBlock::Text { text }],
        UserContent::Blocks(content_blocks) => {
            let mut blocks = Vec::new();
            for block in content_blocks {
                match block {
                    UserContentBlock::ToolResult(ToolResultBlock {
                        content, is_error, ..
                    }) => {
                        let text = match content {
                            Some(ToolResultContent::Text(s)) => s,
                            Some(ToolResultContent::Blocks(blocks)) => blocks
                                .iter()
                                .filter_map(|b| {
                                    b.get("text")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string())
                                })
                                .collect::<Vec<_>>()
                                .join("\n"),
                            None => String::new(),
                        };
                        blocks.push(ContentBlock::ToolResult {
                            content: text,
                            is_error,
                        });
                    }
                    UserContentBlock::Text(TextBlock { text }) => {
                        blocks.push(ContentBlock::Text { text });
                    }
                    UserContentBlock::Unknown => {}
                }
            }
            blocks
        }
    };

    if blocks.is_empty() {
        return None;
    }

    Some(NormalizedEntry {
        kind: "user",
        blocks,
        uuid: entry.uuid,
        seq,
        timestamp: entry.timestamp,
        input_tokens: None,
        output_tokens: None,
    })
}

fn normalize_assistant(entry: AssistantEntry, seq: u64) -> Option<NormalizedEntry> {
    let mut blocks = Vec::new();

    for block in entry.message.content {
        match block {
            AssistantContentBlock::Thinking(ThinkingBlock { thinking, .. }) => {
                blocks.push(ContentBlock::Thinking { text: thinking });
            }
            AssistantContentBlock::Text(TextBlock { text }) => {
                blocks.push(ContentBlock::Text { text });
            }
            AssistantContentBlock::ToolUse(ToolUseBlock { name, input, .. }) => {
                let summary = summarize_tool_input(&name, &input);
                blocks.push(ContentBlock::ToolUse {
                    name,
                    input_summary: summary,
                    input,
                });
            }
            AssistantContentBlock::Unknown => {}
        }
    }

    if blocks.is_empty() {
        return None;
    }

    let (input_tokens, output_tokens) = entry
        .message
        .usage
        .map(|u| (u.input_tokens, u.output_tokens))
        .unwrap_or((None, None));

    Some(NormalizedEntry {
        kind: "assistant",
        blocks,
        uuid: entry.uuid,
        seq,
        timestamp: entry.timestamp,
        input_tokens,
        output_tokens,
    })
}

fn summarize_tool_input(tool_name: &str, input: &serde_json::Value) -> String {
    match tool_name {
        "Bash" => input
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "Read" | "Write" | "Edit" => input
            .get("file_path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "Glob" | "Grep" => input
            .get("pattern")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "Task" => input
            .get("prompt")
            .and_then(|v| v.as_str())
            .map(|s| truncate(s, 80))
            .unwrap_or_default(),
        _ => {
            let keys: Vec<&str> = input
                .as_object()
                .map(|m| m.keys().map(|k| k.as_str()).collect())
                .unwrap_or_default();
            keys.join(", ")
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let boundary = s
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i <= max)
            .last()
            .unwrap_or(0);
        format!("{}...", &s[..boundary])
    }
}
