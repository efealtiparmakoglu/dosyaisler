//! Queue file format: JSONL, one job per line.
//!
//! Canonical line (exactly these 8 fields, in this order, `": "` and `", "`
//! separators; payload is serialized compactly):
//!
//! `{"id": "j-000001", "type": "demo", "payload": {"n":1}, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 1}`
//!
//! Parsing is lenient:
//! - unknown extra fields are ignored (and dropped on rewrite, since the
//!   worker always writes the canonical 8-field form);
//! - missing optional fields fall back to defaults (`status=pending`,
//!   `attempts=0`, `max_attempts=3`, `last_error=null`, `queued_seq=0`);
//! - `id` and `type` are required (a line without them is a format error).
//!
//! Whole-file rewrites are atomic: write a temp file in the same directory,
//! fsync, rename over the target.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Minimal JSON (order-preserving objects, no external crates)
// ---------------------------------------------------------------------------

/// JSON value. Objects preserve key order so a parse/serialize round trip of a
/// canonical line is byte-exact.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(items) => items.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Integer value; accepts integral floats (`3.0`) for leniency.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Json::Int(i) => Some(*i),
            Json::Float(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
            _ => None,
        }
    }
}

const MAX_DEPTH: usize = 128;

/// Parse a complete JSON document (no trailing garbage allowed).
pub fn parse_json(input: &str) -> Result<Json, String> {
    let mut p = Parser {
        b: input.as_bytes(),
        pos: 0,
        depth: 0,
    };
    p.skip_ws();
    let v = p.value()?;
    p.skip_ws();
    if p.pos != p.b.len() {
        return Err(format!(
            "unexpected trailing input at byte {}",
            p.pos
        ));
    }
    Ok(v)
}

struct Parser<'a> {
    b: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if matches!(c, b' ' | b'\t' | b'\n' | b'\r') {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn next_byte(&mut self) -> Result<u8, String> {
        let c = self.peek().ok_or("unexpected end of input")?;
        self.pos += 1;
        Ok(c)
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        let got = self.next_byte()?;
        if got == c {
            Ok(())
        } else {
            Err(format!("expected '{}' found '{}'", c as char, got as char))
        }
    }

    fn text(&self, start: usize, end: usize) -> Result<&'a str, String> {
        std::str::from_utf8(&self.b[start..end]).map_err(|_| "invalid UTF-8".to_string())
    }

    fn value(&mut self) -> Result<Json, String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err("JSON nesting too deep".to_string());
        }
        let v = match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => self.string().map(Json::Str),
            Some(b't') => self.literal("true", Json::Bool(true)),
            Some(b'f') => self.literal("false", Json::Bool(false)),
            Some(b'n') => self.literal("null", Json::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            _ => Err("expected a JSON value".to_string()),
        };
        self.depth -= 1;
        v
    }

    fn literal(&mut self, word: &str, v: Json) -> Result<Json, String> {
        if self.b[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(v)
        } else {
            Err(format!("invalid literal, expected {word}"))
        }
    }

    fn object(&mut self) -> Result<Json, String> {
        self.pos += 1; // '{'
        let mut items: Vec<(String, Json)> = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Obj(items));
        }
        loop {
            self.skip_ws();
            let key = self.string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            let val = self.value()?;
            items.push((key, val));
            self.skip_ws();
            match self.next_byte()? {
                b',' => continue,
                b'}' => return Ok(Json::Obj(items)),
                c => Err(format!("expected ',' or '}}', found '{}'", c as char)),
            }?;
        }
    }

    fn array(&mut self) -> Result<Json, String> {
        self.pos += 1; // '['
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Arr(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value()?);
            self.skip_ws();
            match self.next_byte()? {
                b',' => continue,
                b']' => return Ok(Json::Arr(items)),
                c => Err(format!("expected ',' or ']', found '{}'", c as char)),
            }?;
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let start = self.pos;
            while let Some(c) = self.peek() {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.pos += 1;
            }
            out.push_str(self.text(start, self.pos)?);
            match self.next_byte()? {
                b'"' => return Ok(out),
                b'\\' => self.escape(&mut out)?,
                _ => return Err("control character in string".to_string()),
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), String> {
        let e = self.next_byte()?;
        match e {
            b'"' => out.push('"'),
            b'\\' => out.push('\\'),
            b'/' => out.push('/'),
            b'b' => out.push('\u{8}'),
            b'f' => out.push('\u{c}'),
            b'n' => out.push('\n'),
            b'r' => out.push('\r'),
            b't' => out.push('\t'),
            b'u' => {
                let hi = self.hex4()?;
                let cp = if (0xD800..0xDC00).contains(&hi) {
                    // High surrogate: require a following \uXXXX low surrogate.
                    if self.next_byte()? != b'\\' || self.next_byte()? != b'u' {
                        return Err("unpaired surrogate in \\u escape".to_string());
                    }
                    let lo = self.hex4()?;
                    if !(0xDC00..0xE000).contains(&lo) {
                        return Err("invalid low surrogate in \\u escape".to_string());
                    }
                    0x1_0000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                } else if (0xDC00..0xE000).contains(&hi) {
                    return Err("unpaired low surrogate in \\u escape".to_string());
                } else {
                    hi
                };
                out.push(char::from_u32(cp).ok_or("invalid \\u escape")?);
            }
            _ => return Err(format!("invalid escape '\\{}'", e as char)),
        }
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let mut v: u32 = 0;
        for _ in 0..4 {
            let c = self.next_byte()?;
            let d = (c as char)
                .to_digit(16)
                .ok_or_else(|| format!("invalid hex digit '{}'", c as char))?;
            v = v * 16 + d;
        }
        Ok(v)
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos += 1;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        let s = self.text(start, self.pos)?;
        if !is_float {
            if let Ok(i) = s.parse::<i64>() {
                return Ok(Json::Int(i));
            }
        }
        s.parse::<f64>()
            .map(Json::Float)
            .map_err(|_| format!("invalid number '{s}'"))
    }
}

/// Serialize JSON compactly (no spaces).
pub fn json_to_compact(v: &Json) -> String {
    let mut out = String::new();
    write_json(v, &mut out);
    out
}

fn write_json(v: &Json, out: &mut String) {
    match v {
        Json::Null => out.push_str("null"),
        Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Json::Int(i) => out.push_str(&i.to_string()),
        Json::Float(f) => {
            if f.is_finite() {
                out.push_str(&format!("{f:?}"));
            } else {
                out.push_str("null");
            }
        }
        Json::Str(s) => write_json_string(s, out),
        Json::Arr(items) => {
            out.push('[');
            for (i, x) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(x, out);
            }
            out.push(']');
        }
        Json::Obj(items) => {
            out.push('{');
            for (i, (k, x)) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(k, out);
                out.push(':');
                write_json(x, out);
            }
            out.push('}');
        }
    }
}

fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    write_json_string(s, &mut out);
    out
}

// ---------------------------------------------------------------------------
// Jobs and queue file
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pending,
    Claimed,
    Done,
    Failed,
}

impl Status {
    pub fn parse(s: &str) -> Result<Status, String> {
        match s {
            "pending" => Ok(Status::Pending),
            "claimed" => Ok(Status::Claimed),
            "done" => Ok(Status::Done),
            "failed" => Ok(Status::Failed),
            other => Err(format!("invalid status '{other}' (expected pending|claimed|done|failed)")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Claimed => "claimed",
            Status::Done => "done",
            Status::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub job_type: String,
    pub payload: Json,
    pub status: Status,
    pub attempts: i64,
    pub max_attempts: i64,
    pub last_error: Option<String>,
    pub queued_seq: i64,
}

impl Job {
    /// Canonical single-line serialization (exact 8-field contract format).
    pub fn to_line(&self) -> String {
        format!(
            "{{\"id\": {}, \"type\": {}, \"payload\": {}, \"status\": {}, \"attempts\": {}, \
             \"max_attempts\": {}, \"last_error\": {}, \"queued_seq\": {}}}",
            json_escape(&self.id),
            json_escape(&self.job_type),
            json_to_compact(&self.payload),
            json_escape(self.status.as_str()),
            self.attempts,
            self.max_attempts,
            match &self.last_error {
                Some(e) => json_escape(e),
                None => "null".to_string(),
            },
            self.queued_seq,
        )
    }

    pub fn is_claimable(&self) -> bool {
        self.status == Status::Pending && self.attempts < self.max_attempts
    }

    pub fn is_exhausted(&self) -> bool {
        self.status == Status::Pending && self.attempts >= self.max_attempts
    }
}

/// Parse one JSON object into a [`Job`], leniently.
pub fn job_from_json(v: &Json) -> Result<Job, String> {
    let obj = match v {
        Json::Obj(_) => v,
        _ => return Err("line is not a JSON object".to_string()),
    };
    let id = obj
        .get("id")
        .and_then(Json::as_str)
        .ok_or("missing or non-string field 'id'")?
        .to_string();
    let job_type = obj
        .get("type")
        .and_then(Json::as_str)
        .ok_or("missing or non-string field 'type'")?
        .to_string();
    let payload = obj.get("payload").cloned().unwrap_or(Json::Null);
    let status = match obj.get("status") {
        None => Status::Pending,
        Some(Json::Str(s)) => Status::parse(s)?,
        Some(_) => return Err("field 'status' is not a string".to_string()),
    };
    let int_field = |name: &str, default: i64| -> Result<i64, String> {
        match obj.get(name) {
            None => Ok(default),
            Some(v) => v
                .as_int()
                .ok_or_else(|| format!("field '{name}' is not an integer")),
        }
    };
    let attempts = int_field("attempts", 0)?;
    let max_attempts = int_field("max_attempts", 3)?;
    let queued_seq = int_field("queued_seq", 0)?;
    let last_error = match obj.get("last_error") {
        None | Some(Json::Null) => None,
        Some(Json::Str(s)) => Some(s.clone()),
        Some(_) => return Err("field 'last_error' is not a string or null".to_string()),
    };
    Ok(Job {
        id,
        job_type,
        payload,
        status,
        attempts,
        max_attempts,
        last_error,
        queued_seq,
    })
}

/// Queue parse error with 1-based line number.
#[derive(Debug)]
pub struct QueueParseError {
    pub line: usize,
    pub msg: String,
}

impl std::fmt::Display for QueueParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.msg)
    }
}

/// Parse a whole queue file. Blank lines are skipped.
pub fn parse_queue(text: &str) -> Result<Vec<Job>, QueueParseError> {
    let mut jobs = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let v = parse_json(line)
            .map_err(|e| QueueParseError { line: i + 1, msg: format!("invalid JSON: {e}") })?;
        let job = job_from_json(&v).map_err(|msg| QueueParseError { line: i + 1, msg })?;
        jobs.push(job);
    }
    Ok(jobs)
}

/// Canonical serialization: one line per job, each terminated by '\n'.
pub fn serialize_queue(jobs: &[Job]) -> String {
    let mut out = String::new();
    for j in jobs {
        out.push_str(&j.to_line());
        out.push('\n');
    }
    out
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Atomic whole-file write: temp file in the same directory + fsync + rename.
pub fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".to_string());
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{name}.tmp.{}-{n}", std::process::id()));
    let write = || -> io::Result<()> {
        use std::io::Write;
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, path)?;
        // Best-effort directory fsync so the rename is durable.
        if let Ok(d) = fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    };
    match write() {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "dosyaisler-qtest-{tag}-{}-{}",
            std::process::id(),
            C.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn roundtrip_canonical_lines_exact() {
        let a = r#"{"id": "j-000001", "type": "mail", "payload": {"to":"a@b.c","n":1,"tags":["x","y"]}, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 1}"#;
        let b = r#"{"id": "j-000002", "type": "build", "payload": {"cmd":"make"}, "status": "failed", "attempts": 2, "max_attempts": 3, "last_error": "exit 1: oops", "queued_seq": 2}"#;
        let text = format!("{a}\n{b}\n");
        let jobs = parse_queue(&text).unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].id, "j-000001");
        assert_eq!(jobs[0].status, Status::Pending);
        assert_eq!(jobs[1].last_error.as_deref(), Some("exit 1: oops"));
        // Whole-file round trip must be byte-exact for canonical input.
        assert_eq!(serialize_queue(&jobs), text);
        // Single-line round trip too.
        assert_eq!(jobs[0].to_line(), a);
    }

    #[test]
    fn lenient_defaults_and_unknown_fields_ignored() {
        let line = r#"{"id": "j-9", "type": "t", "note": "extra", "queued_seq": 7}"#;
        let jobs = parse_queue(line).unwrap();
        let j = &jobs[0];
        assert_eq!(j.status, Status::Pending);
        assert_eq!(j.attempts, 0);
        assert_eq!(j.max_attempts, 3);
        assert_eq!(j.last_error, None);
        assert_eq!(j.queued_seq, 7);
        assert_eq!(j.payload, Json::Null);
        // Rewrite canonicalizes: unknown fields are dropped.
        assert_eq!(
            j.to_line(),
            r#"{"id": "j-9", "type": "t", "payload": null, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 7}"#
        );
    }

    #[test]
    fn payload_escapes_roundtrip() {
        let payload = Json::Obj(vec![
            ("msg".to_string(), Json::Str("he said \"hi\"\nbye\u{e9}".to_string())),
            ("back".to_string(), Json::Str("a\\b".to_string())),
            ("tab".to_string(), Json::Str("\tx".to_string())),
            ("ok".to_string(), Json::Bool(true)),
        ]);
        let j = Job {
            id: "j-1".into(),
            job_type: "escape".into(),
            payload: payload.clone(),
            status: Status::Pending,
            attempts: 0,
            max_attempts: 3,
            last_error: Some("line1\nline2 \"q\"".into()),
            queued_seq: 1,
        };
        let parsed = parse_queue(&j.to_line()).unwrap().pop().unwrap();
        assert_eq!(parsed.payload, payload);
        assert_eq!(parsed.last_error.as_deref(), Some("line1\nline2 \"q\""));
        assert_eq!(parsed.to_line(), j.to_line());
    }

    #[test]
    fn parse_errors_report_line_and_bad_status() {
        let text = "{\"id\": \"a\", \"type\": \"t\"}\n\n{\"id\": broken}\n";
        let err = parse_queue(text).unwrap_err();
        assert_eq!(err.line, 3);
        assert!(err.msg.contains("invalid JSON"));

        let bad_status = r#"{"id": "a", "type": "t", "status": "weird"}"#;
        let err = parse_queue(bad_status).unwrap_err();
        assert!(err.msg.contains("invalid status"), "got: {}", err.msg);

        let no_id = r#"{"type": "t"}"#;
        assert!(parse_queue(no_id).is_err());
    }

    #[test]
    fn write_atomic_replaces_content_without_temp_litter() {
        let dir = temp_dir("atomic");
        let f = dir.join("queue.jsonl");
        write_atomic(&f, b"first\n").unwrap();
        assert_eq!(fs::read_to_string(&f).unwrap(), "first\n");
        write_atomic(&f, b"second\nlonger\n").unwrap();
        assert_eq!(fs::read_to_string(&f).unwrap(), "second\nlonger\n");
        let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
        assert_eq!(entries.len(), 1, "no temp files should remain");
    }
}
