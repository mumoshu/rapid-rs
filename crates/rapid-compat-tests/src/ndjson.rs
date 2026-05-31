//! NDJSON trace reader/writer.
//!
//! Wire format (one record per line, no trailing comma):
//!
//! ```text
//! {"ts_ms": 1779845333609, "dst": "127.0.0.1:22000", "req_b64": "Ci..."}
//! ```
//!
//! Authoritative producer: the Java patch in
//! `references/rapid-java/rapid/src/main/java/com/vrg/rapid/messaging/impl/NdjsonTraceWriter.java`.

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::Path;

use prost::Message;
use rapid::pb;

/// A single replay record.
#[derive(Debug, Clone)]
pub struct TraceRecord {
    /// Epoch milliseconds (`System.currentTimeMillis()` on the Java side).
    pub ts_ms: u64,
    /// The endpoint that received the request.
    pub dst: SocketAddr,
    /// Decoded request payload.
    pub request: pb::RapidRequest,
}

/// Errors raised while parsing or producing NDJSON traces.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wraps any `std::io::Error` raised by the underlying file handle.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The record's JSON was malformed (missing field, bad type, etc).
    #[error("malformed record at line {line}: {detail}")]
    Malformed {
        /// 1-based line number where the error was detected.
        line: usize,
        /// Free-form parsing diagnostic.
        detail: String,
    },
    /// Base64 decode failed.
    #[error("base64 decode failed at line {line}")]
    Base64 {
        /// 1-based line number.
        line: usize,
    },
    /// The base64-decoded bytes were not a valid `RapidRequest`.
    #[error("protobuf decode failed at line {0}: {1}")]
    Protobuf(usize, prost::DecodeError),
}

/// Read every NDJSON record from `path`. Lines that don't parse return
/// `Err`. Empty / whitespace-only lines are skipped.
///
/// # Errors
///
/// See [`Error`] variants.
pub fn read<P: AsRef<Path>>(path: P) -> Result<Vec<TraceRecord>, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(parse_line(trimmed, idx + 1)?);
    }
    Ok(out)
}

/// Append `records` to `path` in NDJSON format.
///
/// # Errors
///
/// Returns any `std::io::Error` raised by the underlying file.
pub fn append<P: AsRef<Path>>(path: P, records: &[TraceRecord]) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    for r in records {
        let b64 = base64_encode(&encode_request(&r.request));
        writeln!(
            file,
            r#"{{"ts_ms":{},"dst":"{}","req_b64":"{}"}}"#,
            r.ts_ms, r.dst, b64
        )?;
    }
    Ok(())
}

fn parse_line(line: &str, lineno: usize) -> Result<TraceRecord, Error> {
    let ts_ms = extract_u64(line, "\"ts_ms\":", lineno, "ts_ms missing")?;
    let dst_raw = extract_string(line, "\"dst\":\"", lineno, "dst missing")?;
    let req_b64 = extract_string(line, "\"req_b64\":\"", lineno, "req_b64 missing")?;
    let dst: SocketAddr = dst_raw.parse().map_err(|e| Error::Malformed {
        line: lineno,
        detail: format!("dst not a SocketAddr: {e}"),
    })?;
    let bytes = base64_decode(&req_b64).ok_or(Error::Base64 { line: lineno })?;
    let request =
        pb::RapidRequest::decode(bytes.as_slice()).map_err(|e| Error::Protobuf(lineno, e))?;
    Ok(TraceRecord {
        ts_ms,
        dst,
        request,
    })
}

fn extract_u64(line: &str, key: &str, lineno: usize, detail: &str) -> Result<u64, Error> {
    let i = line.find(key).ok_or_else(|| Error::Malformed {
        line: lineno,
        detail: detail.into(),
    })?;
    let after = &line[i + key.len()..];
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    after[..end].parse().map_err(|_| Error::Malformed {
        line: lineno,
        detail: format!("{detail} (not a u64)"),
    })
}

fn extract_string(line: &str, key: &str, lineno: usize, detail: &str) -> Result<String, Error> {
    let i = line.find(key).ok_or_else(|| Error::Malformed {
        line: lineno,
        detail: detail.into(),
    })?;
    let after = &line[i + key.len()..];
    let end = after.find('"').ok_or_else(|| Error::Malformed {
        line: lineno,
        detail: format!("{detail} (unterminated)"),
    })?;
    Ok(after[..end].to_string())
}

fn encode_request(req: &pb::RapidRequest) -> Vec<u8> {
    let mut buf = Vec::with_capacity(req.encoded_len());
    req.encode(&mut buf).expect("Vec encode infallible");
    buf
}

// Tiny standalone base64 implementations — base64 is the only output
// format used by Java's `Base64.getEncoder()` so we mirror the standard
// alphabet without padding-removal quirks.

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(b2 & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut table = [255u8; 256];
    for (i, &c) in ALPHABET.iter().enumerate() {
        table[c as usize] = u8::try_from(i).ok()?;
    }
    let mut bytes: Vec<u8> = s
        .bytes()
        .filter(|b| *b != b'=' && !b.is_ascii_whitespace())
        .collect();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut decoded = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut buf = [0u8; 4];
    while !bytes.is_empty() {
        let take = bytes.len().min(4);
        let chunk: Vec<u8> = bytes.drain(..take).collect();
        for (i, b) in chunk.iter().enumerate() {
            let v = table[*b as usize];
            if v == 255 {
                return None;
            }
            buf[i] = v;
        }
        if chunk.len() >= 2 {
            decoded.push((buf[0] << 2) | (buf[1] >> 4));
        }
        if chunk.len() >= 3 {
            decoded.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if chunk.len() >= 4 {
            decoded.push((buf[2] << 6) | buf[3]);
        }
    }
    Some(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_one_record() {
        let r = TraceRecord {
            ts_ms: 1,
            dst: "127.0.0.1:7000".parse().unwrap(),
            request: pb::RapidRequest {
                content: Some(pb::rapid_request::Content::ProbeMessage(
                    pb::ProbeMessage::default(),
                )),
            },
        };
        let tmp = tempdir();
        let path = tmp.join("trace.ndjson");
        append(&path, std::slice::from_ref(&r)).unwrap();
        let read_back = read(&path).unwrap();
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].ts_ms, 1);
        assert_eq!(read_back[0].dst, r.dst);
        std::fs::remove_file(&path).ok();
        std::fs::remove_dir(&tmp).ok();
    }

    #[test]
    fn base64_alphabet_matches_java() {
        // Spot-check: "hello" → "aGVsbG8=" via java.util.Base64.
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
    }

    fn tempdir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("rapid-ndjson-{}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
