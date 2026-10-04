//! Claim/execute/finish engine: worker threads, telemetry and reports.
//!
//! Two-phase protocol per job:
//! 1. **Claim** — under the sidecar lock: scan the file, record exhausted
//!    skips, pick the first claimable job (pending with attempts < max),
//!    set `status=claimed` and `attempts+=1`, atomically rewrite the file.
//!    Execution happens *outside* the lock.
//! 2. **Finish** — re-acquire the lock: set `status=done` (`last_error=null`)
//!    or `status=failed` (`last_error=exit code + stderr tail`), atomically
//!    rewrite.
//!
//! `--max-jobs N` is enforced inside the claim critical section (a shared
//! started counter is checked under the lock), so at most N jobs are ever
//! claimed.
//!
//! Determinism: lifecycle lines are buffered per job and emitted at the end
//! sorted by job id, so reports never interleave even with N threads.

use std::collections::BTreeMap;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::clock::Clock;
use crate::exec::{ExecSlot, RunOutcome, Runner};
use crate::lock::{self, LockConfig, LockError};
use crate::qfile::{write_atomic, Job, Json, QueueParseError, Status};

#[derive(Debug, Clone)]
pub struct RunOptions {
    /// OS threads, 1..=32 (validated by the CLI).
    pub workers: usize,
    /// Stop once this many jobs have been claimed/processed.
    pub max_jobs: Option<usize>,
    pub lock: LockConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    Done,
    Failed,
    Skipped,
}

#[derive(Debug, Clone)]
pub struct JobResult {
    pub id: String,
    pub worker: String,
    pub outcome: JobOutcome,
    /// Short reason: `exit 3`, `spawn` or `signal`.
    pub reason: String,
    pub duration_ms: u64,
    pub exit: Option<i32>,
    pub error: Option<String>,
    pub attempts_after: i64,
}

#[derive(Debug, Clone)]
pub struct Telemetry {
    pub jobs_started: usize,
    pub max_concurrent: usize,
    pub total_busy_ms: u64,
}

#[derive(Debug)]
pub struct RunReport {
    pub queue: PathBuf,
    pub done: usize,
    pub failed: usize,
    pub skipped: usize,
    pub pending_left: usize,
    pub claimed_left: usize,
    pub started_at_unix_ms: u64,
    pub wall_ms: u64,
    pub telemetry: Telemetry,
    pub jobs: Vec<JobResult>,
}

#[derive(Debug)]
pub enum RunError {
    LockTimeout { path: PathBuf, timeout: Duration },
    Format(QueueParseError),
    Io(io::Error),
}

impl From<LockError> for RunError {
    fn from(e: LockError) -> Self {
        match e {
            LockError::Timeout { path, timeout } => RunError::LockTimeout { path, timeout },
            LockError::Io(e) => RunError::Io(e),
        }
    }
}

impl From<QueueParseError> for RunError {
    fn from(e: QueueParseError) -> Self {
        RunError::Format(e)
    }
}

impl From<io::Error> for RunError {
    fn from(e: io::Error) -> Self {
        RunError::Io(e)
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::LockTimeout { path, timeout } => {
                write!(f, "lock timeout after {timeout:?}: {}", path.display())
            }
            RunError::Format(e) => write!(f, "queue format error: {e}"),
            RunError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

fn read_jobs(qfile: &Path) -> Result<Vec<Job>, RunError> {
    let text = std::fs::read_to_string(qfile)?;
    Ok(crate::qfile::parse_queue(&text)?)
}

fn write_jobs(qfile: &Path, jobs: &[Job]) -> Result<(), RunError> {
    write_atomic(qfile, crate::qfile::serialize_queue(jobs).as_bytes())?;
    Ok(())
}

struct Shared {
    started: AtomicUsize,
    done: AtomicUsize,
    failed: AtomicUsize,
    skipped: AtomicUsize,
    cur_concurrent: AtomicUsize,
    max_concurrent: AtomicUsize,
    busy_ms: AtomicU64,
    results: Mutex<BTreeMap<String, JobResult>>,
    fatal: Mutex<Option<RunError>>,
    slot: ExecSlot,
}

impl Shared {
    fn set_fatal(&self, e: RunError) {
        let mut f = self.fatal.lock().unwrap();
        if f.is_none() {
            *f = Some(e);
        }
    }

    fn has_fatal(&self) -> bool {
        self.fatal.lock().unwrap().is_some()
    }

    fn bump_max_concurrent(&self) {
        let mut cur = self.cur_concurrent.load(Ordering::SeqCst);
        loop {
            let max = self.max_concurrent.load(Ordering::SeqCst);
            if cur <= max {
                return;
            }
            match self
                .max_concurrent
                .compare_exchange(max, cur, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(_) => cur = self.cur_concurrent.load(Ordering::SeqCst),
            }
        }
    }

    fn record_result(&self, r: JobResult) {
        match r.outcome {
            JobOutcome::Done => self.done.fetch_add(1, Ordering::SeqCst),
            JobOutcome::Failed => self.failed.fetch_add(1, Ordering::SeqCst),
            JobOutcome::Skipped => self.skipped.fetch_add(1, Ordering::SeqCst),
        };
        self.results.lock().unwrap().insert(r.id.clone(), r);
    }
}

/// Claim the next job under the sidecar lock, or `None` to stop (queue
/// exhausted, quota reached, or fatal error).
fn claim_next(
    qfile: &Path,
    opts: &RunOptions,
    clock: &dyn Clock,
    shared: &Shared,
) -> Option<Job> {
    let guard = match lock::acquire(qfile, &opts.lock, clock) {
        Ok(g) => g,
        Err(e) => {
            shared.set_fatal(e.into());
            return None;
        }
    };
    let claimed = (|| {
        let mut jobs = read_jobs(qfile)?;
        // Record exhausted pending jobs exactly once (they stay pending).
        for j in jobs.iter().filter(|j| j.is_exhausted()) {
            {
                let mut rs = shared.results.lock().unwrap();
                if rs.contains_key(&j.id) {
                    continue;
                }
                rs.insert(
                    j.id.clone(),
                    JobResult {
                        id: j.id.clone(),
                        worker: "-".to_string(),
                        outcome: JobOutcome::Skipped,
                        reason: "exhausted".to_string(),
                        duration_ms: 0,
                        exit: None,
                        error: None,
                        attempts_after: j.attempts,
                    },
                );
            }
            shared.skipped.fetch_add(1, Ordering::SeqCst);
        }
        // Quota is checked inside the critical section: at most --max-jobs
        // claims can ever happen.
        if let Some(m) = opts.max_jobs {
            if shared.started.load(Ordering::SeqCst) >= m {
                return Ok(None);
            }
        }
        let idx = match jobs.iter().position(Job::is_claimable) {
            Some(i) => i,
            None => return Ok(None),
        };
        let mut job = jobs[idx].clone();
        job.status = Status::Claimed;
        job.attempts += 1;
        jobs[idx] = job.clone();
        write_jobs(qfile, &jobs)?;
        shared.started.fetch_add(1, Ordering::SeqCst);
        Ok(Some(job))
    })();
    drop(guard);
    match claimed {
        Ok(Some(job)) => Some(job),
        Ok(None) => None,
        Err(e) => {
            shared.set_fatal(e);
            None
        }
    }
}

/// Phase two: re-acquire the lock and write the terminal status.
fn finish_job(
    qfile: &Path,
    opts: &RunOptions,
    clock: &dyn Clock,
    job: &Job,
    success: bool,
    outcome: &RunOutcome,
) -> Result<(), RunError> {
    let guard = lock::acquire(qfile, &opts.lock, clock)?;
    let r = (|| {
        let mut jobs = read_jobs(qfile)?;
        let idx = match jobs.iter().position(|j| j.id == job.id) {
            Some(i) => i,
            None => {
                return Err(RunError::Io(io::Error::other(format!(
                    "job {} vanished from the queue file",
                    job.id
                ))));
            }
        };
        if success {
            jobs[idx].status = Status::Done;
            jobs[idx].last_error = None;
        } else {
            jobs[idx].status = Status::Failed;
            jobs[idx].last_error = Some(outcome.error_text());
        }
        write_jobs(qfile, &jobs)
    })();
    drop(guard);
    r
}

fn worker_main(
    wid: usize,
    qfile: PathBuf,
    opts: RunOptions,
    runner: Arc<dyn Runner>,
    clock: Arc<dyn Clock>,
    shared: Arc<Shared>,
) {
    let wname = format!("w{wid}");
    loop {
        if shared.has_fatal() {
            return;
        }
        let Some(job) = claim_next(&qfile, &opts, &*clock, &shared) else {
            return;
        };
        // ---- execute outside the lock -------------------------------------
        shared.cur_concurrent.fetch_add(1, Ordering::SeqCst);
        shared.bump_max_concurrent();
        let t0 = clock.now_mono_ms();
        let outcome = catch_unwind(AssertUnwindSafe(|| runner.run(&job, &shared.slot)))
            .unwrap_or_else(|_| RunOutcome {
                exit: None,
                spawn_error: Some("job runner panicked".to_string()),
                stderr_tail: String::new(),
            });
        let duration_ms = clock.now_mono_ms().saturating_sub(t0);
        shared.cur_concurrent.fetch_sub(1, Ordering::SeqCst);
        shared.busy_ms.fetch_add(duration_ms, Ordering::SeqCst);

        let success = outcome.success();
        // ---- finish under the lock -----------------------------------------
        if let Err(e) = finish_job(&qfile, &opts, &*clock, &job, success, &outcome) {
            shared.set_fatal(e);
            return;
        }
        shared.record_result(JobResult {
            id: job.id.clone(),
            worker: wname.clone(),
            outcome: if success { JobOutcome::Done } else { JobOutcome::Failed },
            reason: if success {
                "exit 0".to_string()
            } else {
                outcome.reason()
            },
            duration_ms,
            exit: outcome.exit,
            error: if success {
                None
            } else {
                Some(outcome.error_text())
            },
            attempts_after: job.attempts,
        });
    }
}

/// Run the queue until no claimable jobs remain (or the quota is reached).
pub fn run_queue(
    qfile: &Path,
    opts: &RunOptions,
    runner: Arc<dyn Runner>,
    clock: Arc<dyn Clock>,
) -> Result<RunReport, RunError> {
    let started_at = clock.now_unix_ms();
    let wall0 = clock.now_mono_ms();
    // Validate the file up front so bad format exits before any claim.
    read_jobs(qfile)?;

    let slot = ExecSlot::create(
        &std::env::temp_dir(),
        &format!("run-{}-{started_at}", std::process::id()),
    )?;
    let shared = Arc::new(Shared {
        started: AtomicUsize::new(0),
        done: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
        skipped: AtomicUsize::new(0),
        cur_concurrent: AtomicUsize::new(0),
        max_concurrent: AtomicUsize::new(0),
        busy_ms: AtomicU64::new(0),
        results: Mutex::new(BTreeMap::new()),
        fatal: Mutex::new(None),
        slot,
    });

    let mut handles = Vec::new();
    for wid in 1..=opts.workers.max(1) {
        let q = qfile.to_path_buf();
        let o = opts.clone();
        let r = Arc::clone(&runner);
        let c = Arc::clone(&clock);
        let s = Arc::clone(&shared);
        handles.push(thread::spawn(move || worker_main(wid, q, o, r, c, s)));
    }
    for h in handles {
        let _ = h.join();
    }
    shared.slot.cleanup();
    if let Some(e) = shared.fatal.lock().unwrap().take() {
        return Err(e);
    }

    let final_jobs = read_jobs(qfile)?;
    let jobs: Vec<JobResult> = shared
        .results
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect(); // BTreeMap iteration is already sorted by job id
    Ok(RunReport {
        queue: qfile.to_path_buf(),
        done: shared.done.load(Ordering::SeqCst),
        failed: shared.failed.load(Ordering::SeqCst),
        skipped: shared.skipped.load(Ordering::SeqCst),
        pending_left: final_jobs.iter().filter(|j| j.status == Status::Pending).count(),
        claimed_left: final_jobs.iter().filter(|j| j.status == Status::Claimed).count(),
        started_at_unix_ms: started_at,
        wall_ms: clock.now_mono_ms().saturating_sub(wall0),
        telemetry: Telemetry {
            jobs_started: shared.started.load(Ordering::SeqCst),
            max_concurrent: shared.max_concurrent.load(Ordering::SeqCst),
            total_busy_ms: shared.busy_ms.load(Ordering::SeqCst),
        },
        jobs,
    })
}

fn fmt_secs(ms: u64) -> String {
    format!("{}.{:02}s", ms / 1000, (ms % 1000) / 10)
}

fn sanitize_log(s: &str) -> String {
    const MAX: usize = 160;
    let flat: String = s
        .chars()
        .map(|c| if c == '\n' || c == '\r' || c == '\t' { ' ' } else { c })
        .collect();
    if flat.chars().count() <= MAX {
        flat
    } else {
        let mut t: String = flat.chars().take(MAX).collect();
        t.push_str("...");
        t
    }
}

impl RunReport {
    /// Deterministic progress lines: buffered per job, sorted by job id.
    pub fn job_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for j in &self.jobs {
            match j.outcome {
                JobOutcome::Skipped => lines.push(format!("SKIP {} exhausted", j.id)),
                JobOutcome::Done => {
                    lines.push(format!("CLAIM {} by {}", j.id, j.worker));
                    lines.push(format!("OK {} ({})", j.id, fmt_secs(j.duration_ms)));
                }
                JobOutcome::Failed => {
                    lines.push(format!("CLAIM {} by {}", j.id, j.worker));
                    let err_txt = j.error.as_deref().unwrap_or("");
                    let err_txt = match j.exit {
                        Some(c) => err_txt
                            .strip_prefix(&format!("exit {c}: "))
                            .unwrap_or(err_txt),
                        None => err_txt,
                    };
                    lines.push(format!(
                        "FAIL {} ({}) err={}",
                        j.id,
                        j.reason,
                        sanitize_log(err_txt)
                    ));
                }
            }
        }
        lines
    }

    pub fn summary_line(&self) -> String {
        format!(
            "summary: done={} failed={} skipped={} pending={} claimed_left={}",
            self.done, self.failed, self.skipped, self.pending_left, self.claimed_left
        )
    }

    pub fn telemetry_line(&self) -> String {
        format!(
            "telemetry: jobs={} max_concurrent={} total_busy_ms={} wall_ms={}",
            self.telemetry.jobs_started,
            self.telemetry.max_concurrent,
            self.telemetry.total_busy_ms,
            self.wall_ms
        )
    }

    /// 0 when no job failed this run, 1 otherwise.
    pub fn exit_code(&self) -> i32 {
        if self.failed > 0 {
            1
        } else {
            0
        }
    }

    /// Machine-readable report (--json-report).
    pub fn to_json(&self) -> Json {
        let jobs = Json::Arr(
            self.jobs
                .iter()
                .map(|j| {
                    Json::Obj(vec![
                        ("id".into(), Json::Str(j.id.clone())),
                        ("worker".into(), Json::Str(j.worker.clone())),
                        (
                            "outcome".into(),
                            Json::Str(
                                match j.outcome {
                                    JobOutcome::Done => "done",
                                    JobOutcome::Failed => "failed",
                                    JobOutcome::Skipped => "skipped",
                                }
                                .to_string(),
                            ),
                        ),
                        ("reason".into(), Json::Str(j.reason.clone())),
                        ("duration_ms".into(), Json::Int(j.duration_ms as i64)),
                        ("exit".into(), j.exit.map(|c| Json::Int(c as i64)).unwrap_or(Json::Null)),
                        (
                            "error".into(),
                            match &j.error {
                                Some(e) => Json::Str(e.clone()),
                                None => Json::Null,
                            },
                        ),
                        ("attempts".into(), Json::Int(j.attempts_after)),
                    ])
                })
                .collect(),
        );
        Json::Obj(vec![
            ("queue".into(), Json::Str(self.queue.display().to_string())),
            ("started_at_unix_ms".into(), Json::Int(self.started_at_unix_ms as i64)),
            ("wall_ms".into(), Json::Int(self.wall_ms as i64)),
            (
                "summary".into(),
                Json::Obj(vec![
                    ("done".into(), Json::Int(self.done as i64)),
                    ("failed".into(), Json::Int(self.failed as i64)),
                    ("skipped".into(), Json::Int(self.skipped as i64)),
                    ("pending".into(), Json::Int(self.pending_left as i64)),
                    ("claimed_left".into(), Json::Int(self.claimed_left as i64)),
                ]),
            ),
            (
                "telemetry".into(),
                Json::Obj(vec![
                    ("jobs".into(), Json::Int(self.telemetry.jobs_started as i64)),
                    ("max_concurrent".into(), Json::Int(self.telemetry.max_concurrent as i64)),
                    ("total_busy_ms".into(), Json::Int(self.telemetry.total_busy_ms as i64)),
                ]),
            ),
            ("jobs".into(), jobs),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{FakeClock, RealClock};
    use crate::exec::STDERR_TAIL_CHARS;
    use crate::qfile::parse_queue;
    use std::sync::Barrier;

    fn temp_dir(tag: &str) -> PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "dosyaisler-wtest-{tag}-{}-{}",
            std::process::id(),
            C.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Wrapper around a plain function pointer; non-capturing closures coerce
    /// to `fn` pointers with the required higher-ranked lifetimes.
    struct FnRunner(fn(&Job, &ExecSlot) -> RunOutcome);
    impl Runner for FnRunner {
        fn run(&self, job: &Job, slot: &ExecSlot) -> RunOutcome {
            (self.0)(job, slot)
        }
    }

    /// Always fails with a fixed exit code and stderr payload.
    struct FailRunner {
        code: i32,
        stderr: String,
    }
    impl Runner for FailRunner {
        fn run(&self, _job: &Job, _slot: &ExecSlot) -> RunOutcome {
            fail_outcome(self.code, &self.stderr)
        }
    }

    /// Blocks all workers on a barrier so `max_concurrent` provably reaches
    /// the worker count; a second barrier guarantees every worker's +10ms fake
    /// clock advance happened before any worker returns, making durations
    /// deterministic (each job's duration == workers * 10ms).
    struct BarrierRunner {
        arrive: Arc<Barrier>,
        done: Arc<Barrier>,
        clock: Arc<FakeClock>,
    }
    impl Runner for BarrierRunner {
        fn run(&self, _job: &Job, _slot: &ExecSlot) -> RunOutcome {
            self.arrive.wait();
            self.clock.advance_ms(10);
            self.done.wait();
            ok_outcome()
        }
    }

    fn ok_outcome() -> RunOutcome {
        RunOutcome::ok()
    }

    fn fail_outcome(code: i32, stderr: &str) -> RunOutcome {
        RunOutcome {
            exit: Some(code),
            spawn_error: None,
            stderr_tail: stderr.to_string(),
        }
    }

    fn job_line(id: &str, status: &str, attempts: i64, max: i64, last_error: Option<&str>) -> String {
        let err = match last_error {
            Some(e) => format!("\"{e}\""),
            None => "null".to_string(),
        };
        format!(
            r#"{{"id": "{id}", "type": "demo", "payload": {{"n":1}}, "status": "{status}", "attempts": {attempts}, "max_attempts": {max}, "last_error": {err}, "queued_seq": 1}}"#
        )
    }

    fn write_q(path: &Path, lines: &[String]) {
        let mut text = lines.join("\n");
        text.push('\n');
        std::fs::write(path, text).unwrap();
    }

    fn read_q(path: &Path) -> Vec<Job> {
        parse_queue(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn default_opts(workers: usize, max_jobs: Option<usize>) -> RunOptions {
        RunOptions {
            workers,
            max_jobs,
            lock: LockConfig::default(),
        }
    }

    fn run(
        qfile: &Path,
        workers: usize,
        max_jobs: Option<usize>,
        runner: Arc<dyn Runner>,
    ) -> RunReport {
        run_queue(
            qfile,
            &default_opts(workers, max_jobs),
            runner,
            Arc::new(RealClock),
        )
        .unwrap()
    }

    #[test]
    fn two_phase_claim_then_done() {
        let dir = temp_dir("twophase");
        let q = dir.join("q.jsonl");
        write_q(&q, &[job_line("j-1", "pending", 0, 3, None)]);
        let report = run(&q, 1, None, Arc::new(FnRunner(|_, _| ok_outcome())));

        assert_eq!(report.done, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(report.telemetry.jobs_started, 1);
        let j = &read_q(&q)[0];
        assert_eq!(j.status, Status::Done, "phase 2 sets done");
        assert_eq!(j.attempts, 1, "claim incremented attempts");
        assert_eq!(j.last_error, None, "done clears last_error");
        let lines = report.job_lines();
        assert_eq!(
            lines,
            vec!["CLAIM j-1 by w1".to_string(), "OK j-1 (0.00s)".to_string()]
        );
    }

    #[test]
    fn failure_records_exit_code_and_stderr_tail_bounded_at_500() {
        let dir = temp_dir("fail");
        let q = dir.join("q.jsonl");
        write_q(&q, &[job_line("j-1", "pending", 0, 3, Some("stale error"))]);
        let long_err: String = std::iter::repeat_n('x', 600).collect();
        let report = run(
            &q,
            1,
            None,
            Arc::new(FailRunner {
                code: 2,
                stderr: crate::exec::tail_chars(&long_err, STDERR_TAIL_CHARS),
            }),
        );

        assert_eq!(report.failed, 1);
        assert_eq!(report.exit_code(), 1, "failed jobs mean exit 1");
        let j = &read_q(&q)[0];
        assert_eq!(j.status, Status::Failed);
        assert_eq!(j.attempts, 1);
        let err = j.last_error.clone().unwrap();
        assert!(err.starts_with("exit 2: "), "got: {err}");
        let tail = err.strip_prefix("exit 2: ").unwrap();
        assert_eq!(tail.chars().count(), STDERR_TAIL_CHARS);
        assert!(tail.chars().all(|c| c == 'x'));
        assert!(report.job_lines()[1].starts_with("FAIL j-1 (exit 2) err="));
    }

    #[test]
    fn exhausted_pending_job_is_skipped_and_stays_pending() {
        let dir = temp_dir("exhausted");
        let q = dir.join("q.jsonl");
        write_q(
            &q,
            &[
                job_line("j-1", "pending", 3, 3, None), // exhausted
                job_line("j-2", "pending", 0, 3, None), // claimable
            ],
        );
        let report = run(&q, 1, None, Arc::new(FnRunner(|_, _| ok_outcome())));

        assert_eq!(report.skipped, 1);
        assert_eq!(report.done, 1);
        let jobs = read_q(&q);
        let skipped = jobs.iter().find(|j| j.id == "j-1").unwrap();
        assert_eq!(skipped.status, Status::Pending, "skipped jobs stay pending");
        assert_eq!(skipped.attempts, 3, "skip does not touch the job");
        assert!(jobs.iter().find(|j| j.id == "j-2").unwrap().status == Status::Done);
        assert_eq!(report.job_lines()[0], "SKIP j-1 exhausted");
    }

    #[test]
    fn mutual_exclusion_no_job_is_ever_claimed_twice() {
        let dir = temp_dir("mutex");
        let q = dir.join("q.jsonl");
        let lines: Vec<String> = (1..=100)
            .map(|i| job_line(&format!("j-{i:03}"), "pending", 0, 3, None))
            .collect();
        write_q(&q, &lines);
        let report = run(&q, 2, None, Arc::new(FnRunner(|_, _| ok_outcome())));

        assert_eq!(report.done, 100);
        let jobs = read_q(&q);
        for j in &jobs {
            assert_eq!(j.status, Status::Done);
            assert_eq!(j.attempts, 1, "job {} claimed more than once", j.id);
        }
        let ids: std::collections::HashSet<&String> = report.jobs.iter().map(|r| &r.id).collect();
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn parallelism_max_concurrent_two_with_barriers() {
        let dir = temp_dir("par2");
        let q = dir.join("q.jsonl");
        let lines: Vec<String> = (1..=4)
            .map(|i| job_line(&format!("j-{i}"), "pending", 0, 3, None))
            .collect();
        write_q(&q, &lines);
        let clock = Arc::new(FakeClock::new(1_000));
        let runner = Arc::new(BarrierRunner {
            arrive: Arc::new(Barrier::new(2)),
            done: Arc::new(Barrier::new(2)),
            clock: Arc::clone(&clock),
        });
        let report = run_queue(&q, &default_opts(2, None), runner, clock).unwrap();

        assert_eq!(report.telemetry.max_concurrent, 2);
        assert_eq!(report.telemetry.jobs_started, 4);
        // Fake clock: each job's duration covers both +10ms advances per pair.
        assert_eq!(report.telemetry.total_busy_ms, 80);
        assert!(report.jobs.iter().all(|j| j.duration_ms == 20));
    }

    #[test]
    fn parallelism_four_workers_reach_four_concurrent() {
        let dir = temp_dir("par4");
        let q = dir.join("q.jsonl");
        let lines: Vec<String> = (1..=4)
            .map(|i| job_line(&format!("j-{i}"), "pending", 0, 3, None))
            .collect();
        write_q(&q, &lines);
        let clock = Arc::new(FakeClock::new(0));
        let runner = Arc::new(BarrierRunner {
            arrive: Arc::new(Barrier::new(4)),
            done: Arc::new(Barrier::new(4)),
            clock: Arc::clone(&clock),
        });
        let report = run_queue(&q, &default_opts(4, None), runner, clock).unwrap();

        assert_eq!(report.telemetry.max_concurrent, 4);
        assert_eq!(report.telemetry.total_busy_ms, 160);
    }

    #[test]
    fn max_jobs_limits_claimed_jobs() {
        let dir = temp_dir("quota");
        let q = dir.join("q.jsonl");
        let lines: Vec<String> = (1..=5)
            .map(|i| job_line(&format!("j-{i}"), "pending", 0, 3, None))
            .collect();
        write_q(&q, &lines);
        let report = run(&q, 2, Some(2), Arc::new(FnRunner(|_, _| ok_outcome())));

        assert_eq!(report.telemetry.jobs_started, 2);
        assert_eq!(report.done + report.failed, 2);
        assert_eq!(report.pending_left, 3);
        let jobs = read_q(&q);
        assert_eq!(jobs.iter().filter(|j| j.status == Status::Done).count(), 2);
        assert_eq!(jobs.iter().filter(|j| j.status == Status::Pending).count(), 3);
    }

    #[test]
    fn empty_or_terminal_queue_exits_immediately_untouched() {
        let dir = temp_dir("empty");
        let q = dir.join("q.jsonl");
        let before = format!(
            "{}\n{}\n",
            job_line("j-1", "done", 1, 3, None),
            job_line("j-2", "failed", 1, 3, Some("exit 1: x"))
        );
        std::fs::write(&q, &before).unwrap();
        let report = run(&q, 4, None, Arc::new(FnRunner(|_, _| panic!("must not run"))));

        assert_eq!(report.telemetry.jobs_started, 0);
        assert_eq!(report.done + report.failed + report.skipped, 0);
        assert_eq!(report.exit_code(), 0);
        assert_eq!(std::fs::read_to_string(&q).unwrap(), before, "file untouched");

        // A completely empty file behaves the same.
        let q2 = dir.join("empty.jsonl");
        std::fs::write(&q2, "").unwrap();
        let report = run(&q2, 2, None, Arc::new(FnRunner(|_, _| panic!("must not run"))));
        assert_eq!(report.telemetry.jobs_started, 0);
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn error_isolation_failing_jobs_do_not_affect_others() {
        let dir = temp_dir("isolation");
        let q = dir.join("q.jsonl");
        write_q(
            &q,
            &[
                job_line("j-1", "pending", 0, 3, None),
                job_line("j-2", "pending", 0, 3, None),
                job_line("j-3", "pending", 0, 3, None),
                job_line("j-4", "pending", 0, 3, None),
            ],
        );
        let report = run(
            &q,
            2,
            None,
            Arc::new(FnRunner(|job, _| {
                if job.id == "j-2" {
                    fail_outcome(7, "kaboom")
                } else if job.id == "j-4" {
                    RunOutcome {
                        exit: None,
                        spawn_error: Some("'nope': No such file or directory (os error 2)".into()),
                        stderr_tail: String::new(),
                    }
                } else {
                    ok_outcome()
                }
            })),
        );

        assert_eq!(report.done, 2);
        assert_eq!(report.failed, 2);
        let jobs = read_q(&q);
        let by_id = |id: &str| jobs.iter().find(|j| j.id == id).unwrap();
        assert_eq!(by_id("j-1").status, Status::Done);
        assert_eq!(by_id("j-3").status, Status::Done);
        assert_eq!(by_id("j-2").status, Status::Failed);
        assert_eq!(by_id("j-2").last_error.as_deref(), Some("exit 7: kaboom"));
        assert_eq!(by_id("j-4").status, Status::Failed);
        assert!(by_id("j-4")
            .last_error
            .as_deref()
            .unwrap()
            .starts_with("spawn failed: 'nope':"));
        // Results are reported sorted by job id regardless of thread order.
        let ids: Vec<&str> = report.jobs.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["j-1", "j-2", "j-3", "j-4"]);
    }

    #[test]
    fn json_report_shape_is_complete_and_sorted() {
        let dir = temp_dir("report");
        let q = dir.join("q.jsonl");
        write_q(
            &q,
            &[
                job_line("j-1", "pending", 0, 3, None),
                job_line("j-2", "pending", 0, 3, None),
            ],
        );
        let report = run(
            &q,
            1,
            None,
            Arc::new(FnRunner(|job, _| {
                if job.id == "j-2" {
                    fail_outcome(3, "boom")
                } else {
                    ok_outcome()
                }
            })),
        );
        let text = crate::qfile::json_to_compact(&report.to_json());
        let v = crate::qfile::parse_json(&text).unwrap();
        let summary = v.get("summary").unwrap();
        assert_eq!(summary.get("done").unwrap().as_int(), Some(1));
        assert_eq!(summary.get("failed").unwrap().as_int(), Some(1));
        assert_eq!(summary.get("skipped").unwrap().as_int(), Some(0));
        assert_eq!(summary.get("pending").unwrap().as_int(), Some(0));
        let tel = v.get("telemetry").unwrap();
        assert_eq!(tel.get("jobs").unwrap().as_int(), Some(2));
        assert_eq!(tel.get("max_concurrent").unwrap().as_int(), Some(1));
        let jobs = match v.get("jobs").unwrap() {
            Json::Arr(a) => a,
            other => panic!("jobs not an array: {other:?}"),
        };
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].get("id").unwrap().as_str(), Some("j-1"));
        assert_eq!(jobs[0].get("outcome").unwrap().as_str(), Some("done"));
        assert_eq!(jobs[1].get("outcome").unwrap().as_str(), Some("failed"));
        assert_eq!(jobs[1].get("exit").unwrap().as_int(), Some(3));
        assert_eq!(jobs[1].get("error").unwrap().as_str(), Some("exit 3: boom"));
    }
}
