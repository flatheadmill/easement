// NDJSON and LPJSON framing for the client-facing edge. The Easement-facing
// edge is always NDJSON. Chrome native messaging requires LPJSON (4-byte LE
// length prefix). Terminal clients use NDJSON (newline-delimited).
//
// Auto-detection from the first byte is not viable: a 123-byte JSON payload
// produces 0x7B as the first LPJSON length byte, which is '{' in ASCII.

use std::io::{self, Write};
use std::sync::OnceLock;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Framing {
    Ndjson,
    Lpjson,
}

static FRAMING: OnceLock<Framing> = OnceLock::new();

pub fn set_framing(f: Framing) {
    let _ = FRAMING.set(f);
}

pub fn init_default() {
    let _ = FRAMING.get_or_init(|| Framing::Ndjson);
}

pub fn framing() -> Framing {
    FRAMING.get().copied().unwrap_or(Framing::Ndjson)
}

/// Write a JSON message to the client, framed appropriately.
pub fn write_client(lock: &mut io::StdoutLock, bytes: &[u8]) {
    match framing() {
        Framing::Ndjson => {
            let _ = lock.write_all(bytes);
            let _ = lock.write_all(b"\n");
        }
        Framing::Lpjson => {
            let _ = lock.write_all(&(bytes.len() as u32).to_le_bytes());
            let _ = lock.write_all(bytes);
        }
    }
    let _ = lock.flush();
}

/// Read one LPJSON message: 4-byte LE length followed by that many bytes.
pub async fn read_lpjson<R: AsyncReadExt + Unpin>(reader: &mut R) -> Option<String> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(_) => return None,
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    match reader.read_exact(&mut buf).await {
        Ok(_) => {}
        Err(_) => return None,
    }
    String::from_utf8(buf).ok()
}

/// Read one message from stdin according to the current framing.
pub async fn read_client_message<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
) -> Option<String> {
    match framing() {
        Framing::Ndjson => {
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => return None,
                    Ok(_) => {
                        let trimmed = line.trim().to_string();
                        if !trimmed.is_empty() {
                            return Some(trimmed);
                        }
                        // Skip blank lines.
                    }
                    Err(_) => return None,
                }
            }
        }
        Framing::Lpjson => read_lpjson(reader).await,
    }
}
