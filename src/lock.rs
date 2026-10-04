//! Advisory sidecar lock for the queue file.
//!
//! Rust's std has no `flock`, and we avoid external crates, so mutual
//! exclusion uses an atomic-create sidecar file: `<QFILE>.lock` is created
//! with `create_new(true)`. Exactly one process/thread can create it; the
//! holder performs its read-modify-write of the queue file and then removes
//! the sidecar on drop. Contenders retry with exponential backoff until
//! `timeout` elapses, then fail.
//!
//! Crash orphans: if a holder dies before removing the sidecar, the lock file
//! stays behind. The holder embeds its pid and the acquisition time
//! (`pid=<pid> unix_ms=<ms>`, taken from the injected clock); a contender that
//! finds the sidecar older than `stale` removes it and takes over. If the
//! embedded timestamp is unreadable, the file mtime is used as a fallback.
//!
//! This is the same advisory contract as an flock-based scheme: only one
//! process mutates the queue at a time. Tests enforce the invariant (no lost
//! updates, no double claims), not the syscall.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crate::clock::Clock;

#[derive(Debug, Clone, Copy)]
pub struct LockConfig {
    /// How long to wait for the lock before giving up.
    pub timeout: Duration,
    /// Age after which an existing sidecar is considered an orphan and removed.
    pub stale: Duration,
}

impl Default for LockConfig {
    fn default() -> Self {
        LockConfig {
            timeout: Duration::from_secs(5),
            stale: Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
pub enum LockError {
    Timeout { path: PathBuf, timeout: Duration },
    Io(std::io::Error),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Timeout { path, timeout } => {
                write!(f, "lock timeout after {timeout:?}: {}", path.display())
            }
            LockError::Io(e) => write!(f, "lock io error: {e}"),
        }
    }
}

/// Held lock. Removing the sidecar file on drop releases it.
#[derive(Debug)]
pub struct FileLock {
    path: PathBuf,
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn lock_path_for(qfile: &Path) -> PathBuf {
    let mut s = qfile.as_os_str().to_os_string();
    s.push(".lock");
    PathBuf::from(s)
}

/// Acquire the sidecar lock, retrying with backoff. Stale orphan locks older
/// than `cfg.stale` (measured against the injected clock) are removed and
/// taken over.
pub fn acquire(qfile: &Path, cfg: &LockConfig, clock: &dyn Clock) -> Result<FileLock, LockError> {
    let lock_path = lock_path_for(qfile);
    let started = clock.now_mono_ms();
    let mut backoff = Duration::from_millis(1);
    loop {
        match OpenOptions::new().write(true).create_new(true).open(&lock_path) {
            Ok(mut f) => {
                let content = format!(
                    "pid={} unix_ms={}\n",
                    std::process::id(),
                    clock.now_unix_ms()
                );
                let _ = f.write_all(content.as_bytes());
                let _ = f.sync_all();
                return Ok(FileLock { path: lock_path });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if is_stale(&lock_path, cfg.stale, clock) {
                    // Orphaned by a crashed holder: remove and race again.
                    let _ = fs::remove_file(&lock_path);
                    continue;
                }
            }
            Err(e) => return Err(LockError::Io(e)),
        }
        let elapsed = clock.now_mono_ms().saturating_sub(started);
        if Duration::from_millis(elapsed) >= cfg.timeout {
            return Err(LockError::Timeout {
                path: lock_path,
                timeout: cfg.timeout,
            });
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_millis(25));
    }
}

/// A sidecar is stale when its embedded acquisition timestamp (or, failing
/// that, its mtime) is at least `stale` old.
fn is_stale(lock_path: &Path, stale: Duration, clock: &dyn Clock) -> bool {
    let stale_ms = stale.as_millis() as u64;
    if let Ok(content) = fs::read_to_string(lock_path) {
        for tok in content.split_whitespace() {
            if let Some(ms) = tok.strip_prefix("unix_ms=") {
                if let Ok(ms) = ms.parse::<u64>() {
                    return clock.now_unix_ms().saturating_sub(ms) >= stale_ms;
                }
            }
        }
    }
    if let Ok(meta) = fs::metadata(lock_path) {
        if let Ok(modified) = meta.modified() {
            if let Ok(age) = modified.elapsed() {
                return age >= stale;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::RealClock;

    fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static C: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "dosyaisler-ltest-{tag}-{}-{}",
            std::process::id(),
            C.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }

    /// Two threads each increment a counter file 50 times under the sidecar
    /// lock. Lost updates would mean the lock failed; the total must be 100.
    #[test]
    fn sidecar_lock_gives_mutual_exclusion_under_contention() {
        let dir = temp_dir("mutex");
        let data = dir.join("counter.jsonl");
        fs::write(&data, "0\n").unwrap();

        let work = |data: PathBuf| {
            for _ in 0..50 {
                let guard = acquire(&data, &LockConfig::default(), &RealClock).unwrap();
                let n: i64 = fs::read_to_string(&data)
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                fs::write(&data, format!("{}\n", n + 1)).unwrap();
                drop(guard);
            }
        };
        let d1 = data.clone();
        let t1 = thread::spawn(move || work(d1));
        let d2 = data.clone();
        let t2 = thread::spawn(move || work(d2));
        t1.join().unwrap();
        t2.join().unwrap();

        let total: i64 = fs::read_to_string(&data).unwrap().trim().parse().unwrap();
        assert_eq!(total, 100, "no increment may be lost");
        assert!(!lock_path_for(&data).exists(), "lock released at the end");
    }

    #[test]
    fn acquire_times_out_when_lock_is_held() {
        let dir = temp_dir("timeout");
        let data = dir.join("q.jsonl");
        fs::write(&data, "").unwrap();
        let lp = lock_path_for(&data);
        // A freshly-created lock (per the real clock) must not be stolen.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        fs::write(&lp, format!("pid=1 unix_ms={now_ms}\n")).unwrap();

        let err = acquire(
            &data,
            &LockConfig {
                timeout: Duration::from_millis(40),
                stale: Duration::from_secs(60),
            },
            &RealClock,
        )
        .unwrap_err();
        match err {
            LockError::Timeout { path, timeout } => {
                assert_eq!(path, lp);
                assert_eq!(timeout, Duration::from_millis(40));
            }
            other => panic!("expected timeout, got {other:?}"),
        }
        // The pre-existing sidecar must still be there (we never steal a fresh lock).
        assert!(lp.exists());
    }

    #[test]
    fn stale_orphan_is_taken_over_and_release_removes_sidecar() {
        let dir = temp_dir("stale");
        let data = dir.join("q.jsonl");
        fs::write(&data, "").unwrap();
        let lp = lock_path_for(&data);

        // Orphaned holder from long ago (per the fake clock).
        let clock = crate::clock::FakeClock::new(1_000_000);
        fs::write(&lp, "pid=999999 unix_ms=1000\n").unwrap(); // age: 999s
        let guard = acquire(
            &data,
            &LockConfig {
                timeout: Duration::from_millis(50),
                stale: Duration::from_secs(60),
            },
            &clock,
        )
        .unwrap();
        assert!(lp.exists(), "holder recreated the sidecar");
        let holder = fs::read_to_string(&lp).unwrap();
        assert!(holder.contains(&format!("pid={}", std::process::id())));

        // A fresh-looking sidecar must NOT be stolen (timeout instead).
        drop(guard);
        assert!(!lp.exists(), "release removes the sidecar file");
    }
}
