//! Command-template runner.
//!
//! `--exec TEMPLATE` is split on whitespace (no quoting support). Tokens are
//! substituted per argument *after* splitting, so values never re-split:
//! - `{id}`          -> job id
//! - `{type}`        -> job type
//! - `{payload}`     -> compact JSON payload as a single argument
//! - `{payload_file}`-> payload written to a temp file; its path is substituted
//!   and the file is deleted as soon as the command exits
//!
//! A job succeeds iff the command exits 0. Unknown `{tokens}` are left as
//! literal text.

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::qfile::{json_to_compact, Job};

/// Last N characters of stderr kept for `last_error`.
pub const STDERR_TAIL_CHARS: usize = 500;

/// Split a template into argv words on whitespace (no quoting).
pub fn split_template(t: &str) -> Vec<String> {
    t.split_whitespace().map(str::to_string).collect()
}

/// Per-run scratch space for `{payload_file}` temp files.
pub struct ExecSlot {
    pub dir: PathBuf,
    counter: AtomicU64,
}

impl ExecSlot {
    pub fn create(base: &Path, tag: &str) -> io::Result<ExecSlot> {
        let dir = base.join(format!("dosyaisler-{tag}"));
        std::fs::create_dir_all(&dir)?;
        Ok(ExecSlot {
            dir,
            counter: AtomicU64::new(0),
        })
    }

    fn next_payload_path(&self) -> PathBuf {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        self.dir.join(format!("payload-{n}.json"))
    }

    /// Remove the scratch dir (best effort); deletes any leaked payload files.
    pub fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Substitute template tokens. Returns the final argv and the payload files
/// that must be deleted after the command exits.
pub fn substitute(argv: &[String], job: &Job, slot: &ExecSlot) -> io::Result<(Vec<String>, Vec<PathBuf>)> {
    let mut out = Vec::with_capacity(argv.len());
    let mut files = Vec::new();
    for arg in argv {
        let mut s = arg.replace("{id}", &job.id).replace("{type}", &job.job_type);
        if s.contains("{payload}") {
            s = s.replace("{payload}", &json_to_compact(&job.payload));
        }
        if s.contains("{payload_file}") {
            let path = slot.next_payload_path();
            std::fs::write(&path, json_to_compact(&job.payload).as_bytes())?;
            s = s.replace("{payload_file}", &path.to_string_lossy());
            files.push(path);
        }
        out.push(s);
    }
    Ok((out, files))
}

/// Keep the last `max` characters of `s` (char-boundary safe).
pub fn tail_chars(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    s.chars().skip(count - max).collect()
}

/// Result of running one job's command.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    /// Process exit code; `None` on spawn failure or if killed by a signal.
    pub exit: Option<i32>,
    pub spawn_error: Option<String>,
    /// Trailing [`STDERR_TAIL_CHARS`] chars of stderr (trimmed).
    pub stderr_tail: String,
}

impl RunOutcome {
    pub fn success(&self) -> bool {
        self.spawn_error.is_none() && self.exit == Some(0)
    }

    /// Short reason for the progress line: `exit 3`, `spawn` or `signal`.
    pub fn reason(&self) -> String {
        if self.spawn_error.is_some() {
            return "spawn".to_string();
        }
        self.exit
            .map(|c| format!("exit {c}"))
            .unwrap_or_else(|| "signal".to_string())
    }

    /// Text stored in the job's `last_error` (exit code + stderr tail).
    pub fn error_text(&self) -> String {
        if let Some(e) = &self.spawn_error {
            return format!("spawn failed: {e}");
        }
        match self.exit {
            Some(c) => format!("exit {c}: {}", self.stderr_tail),
            None => format!("terminated by signal: {}", self.stderr_tail),
        }
    }

    pub fn ok() -> RunOutcome {
        RunOutcome {
            exit: Some(0),
            spawn_error: None,
            stderr_tail: String::new(),
        }
    }
}

/// Execution backend. The engine depends on this trait so tests can inject a
/// deterministic fake (counters, barriers, fake clock) instead of processes.
pub trait Runner: Send + Sync {
    fn run(&self, job: &Job, slot: &ExecSlot) -> RunOutcome;
}

/// Real runner: spawns the substituted argv as a subprocess.
pub struct TemplateRunner {
    pub argv: Vec<String>,
}

impl TemplateRunner {
    pub fn from_template(t: &str) -> Option<TemplateRunner> {
        let argv = split_template(t);
        if argv.is_empty() {
            None
        } else {
            Some(TemplateRunner { argv })
        }
    }
}

impl Runner for TemplateRunner {
    fn run(&self, job: &Job, slot: &ExecSlot) -> RunOutcome {
        let (argv, payload_files) = match substitute(&self.argv, job, slot) {
            Ok(v) => v,
            Err(e) => {
                return RunOutcome {
                    exit: None,
                    spawn_error: Some(format!("cannot write payload file: {e}")),
                    stderr_tail: String::new(),
                };
            }
        };
        let out = Command::new(&argv[0])
            .args(&argv[1..])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output();
        // Payload files are deleted right after the command exits, success or not.
        for f in &payload_files {
            let _ = std::fs::remove_file(f);
        }
        match out {
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                RunOutcome {
                    exit: o.status.code(),
                    spawn_error: None,
                    stderr_tail: tail_chars(stderr.trim_end(), STDERR_TAIL_CHARS),
                }
            }
            Err(e) => RunOutcome {
                exit: None,
                spawn_error: Some(format!("'{}': {e}", argv[0])),
                stderr_tail: String::new(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qfile::Status;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn temp_dir(tag: &str) -> PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "dosyaisler-etest-{tag}-{}-{}",
            std::process::id(),
            C.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn demo_job() -> Job {
        Job {
            id: "j-000007".into(),
            job_type: "mail".into(),
            payload: crate::qfile::parse_json(r#"{"to":"a@b.c","n":1}"#).unwrap(),
            status: Status::Pending,
            attempts: 0,
            max_attempts: 3,
            last_error: None,
            queued_seq: 1,
        }
    }

    #[test]
    fn substitution_replaces_all_tokens_payload_stays_one_arg() {
        let dir = temp_dir("subst");
        let slot = ExecSlot::create(&dir, "slot").unwrap();
        let job = demo_job();
        let template = vec![
            "run-job".to_string(),
            "{id}".to_string(),
            "{type}".to_string(),
            "{payload}".to_string(),
            "{payload_file}".to_string(),
            "{unknown}".to_string(),
        ];
        let (argv, files) = substitute(&template, &job, &slot).unwrap();
        assert_eq!(argv[0], "run-job");
        assert_eq!(argv[1], "j-000007");
        assert_eq!(argv[2], "mail");
        assert_eq!(argv[3], r#"{"to":"a@b.c","n":1}"#); // compact, single arg
        assert_eq!(argv[5], "{unknown}"); // unknown tokens stay literal
        let pf = PathBuf::from(&argv[4]);
        assert_eq!(files, vec![pf.clone()]);
        assert_eq!(
            std::fs::read_to_string(&pf).unwrap(),
            r#"{"to":"a@b.c","n":1}"#
        );
        slot.cleanup();
        assert!(!slot.dir.exists(), "cleanup removes the scratch dir");
    }

    #[test]
    fn payload_file_is_deleted_after_the_command_exits() {
        let dir = temp_dir("lifecycle");
        let slot = ExecSlot::create(&dir, "slot").unwrap();
        let job = demo_job();
        let runner = TemplateRunner {
            argv: vec!["/usr/bin/true".into(), "{payload_file}".into()],
        };
        let outcome = runner.run(&job, &slot);
        assert!(outcome.success(), "outcome: {:?}", outcome);
        // The only file created for {payload_file} must be gone again.
        let left: Vec<_> = std::fs::read_dir(&slot.dir).unwrap().collect();
        assert!(left.is_empty(), "payload files must be deleted after the run");
        slot.cleanup();
    }

    #[test]
    fn spawn_failure_is_a_clear_error_and_tail_is_bounded() {
        let dir = temp_dir("spawn");
        let slot = ExecSlot::create(&dir, "slot").unwrap();
        let job = demo_job();
        let runner = TemplateRunner {
            argv: vec!["definitely-not-a-command-xyz".into()],
        };
        let outcome = runner.run(&job, &slot);
        assert!(!outcome.success());
        assert!(outcome.spawn_error.is_some());
        assert!(outcome.error_text().starts_with("spawn failed: 'definitely-not-a-command-xyz':"));

        assert_eq!(tail_chars("abc", 500), "abc");
        let long: String = std::iter::repeat_n('a', 600).collect();
        let tail = tail_chars(&long, STDERR_TAIL_CHARS);
        assert_eq!(tail.chars().count(), 500);
        assert!(tail.chars().all(|c| c == 'a'));

        // Nonzero exit: reason and error_text carry the exit code.
        let r = TemplateRunner {
            argv: vec!["/usr/bin/false".into()],
        };
        let outcome = r.run(&job, &slot);
        assert_eq!(outcome.exit, Some(1));
        assert!(!outcome.success());
        assert_eq!(outcome.reason(), "exit 1");
        assert_eq!(outcome.error_text(), "exit 1: ");
        slot.cleanup();
    }
}
