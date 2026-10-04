//! End-to-end CLI tests: run the real binary against real queue files.
//!
//! Job commands are tiny `#!/bin/sh` scripts (no sleeps): one that fails when
//! the payload contains `"fail":true`, exercising success, failure, exit codes
//! and the JSON report.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use dosyaisler::qfile::{parse_json, parse_queue, Status};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_dosyaisler")
}

fn temp_dir(tag: &str) -> PathBuf {
    static C: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "dosyaisler-cli-{tag}-{}-{}",
        std::process::id(),
        C.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&d).unwrap();
    d
}

fn run_cli(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin()).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

fn job_line(id: &str, payload: &str) -> String {
    format!(
        r#"{{"id": "{id}", "type": "demo", "payload": {payload}, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 1}}"#
    )
}

/// `#!/bin/sh` script: exits 2 with "boom" on stderr when the payload file
/// (arg 2) contains `"fail":true`, else exits 0.
fn write_worker_script(dir: &Path) -> String {
    let path = dir.join("worker.sh");
    fs::write(
        &path,
        "#!/bin/sh\npf=\"$2\"\nif grep -q '\"fail\":true' \"$pf\"; then echo boom >&2; exit 2; fi\nexit 0\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().to_string()
}

#[test]
fn usage_and_missing_file_exit_2() {
    let dir = temp_dir("usage");
    let q = dir.join("q.jsonl");
    fs::write(&q, "").unwrap();

    // No command at all.
    let (code, _, _) = run_cli(&[]);
    assert_eq!(code, 2);
    // Unknown command.
    let (code, _, _) = run_cli(&["frobnicate"]);
    assert_eq!(code, 2);
    // run without --exec.
    let (code, _, stderr) = run_cli(&["run", q.to_str().unwrap()]);
    assert_eq!(code, 2);
    assert!(stderr.contains("--exec"), "stderr: {stderr}");
    // run with a missing queue file.
    let missing = dir.join("nope.jsonl");
    let (code, _, stderr) = run_cli(&["run", missing.to_str().unwrap(), "--exec", "true"]);
    assert_eq!(code, 2);
    assert!(stderr.contains("not found"), "stderr: {stderr}");
    // inspect without a file.
    let (code, _, _) = run_cli(&["inspect"]);
    assert_eq!(code, 2);
    // Invalid option values.
    let (code, _, _) = run_cli(&["run", q.to_str().unwrap(), "--exec", "true", "--workers", "0"]);
    assert_eq!(code, 2);
    let (code, _, _) = run_cli(&["run", q.to_str().unwrap(), "--exec", "true", "--workers", "33"]);
    assert_eq!(code, 2);
    let (code, _, _) = run_cli(&["run", q.to_str().unwrap(), "--exec", "true", "--bogus"]);
    assert_eq!(code, 2);
    // Help exits 0.
    let (code, stdout, _) = run_cli(&["--help"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("dosyaisler"));
}

#[test]
fn e2e_run_mixed_success_and_failure() {
    let dir = temp_dir("mixed");
    let q = dir.join("q.jsonl");
    fs::write(
        &q,
        format!(
            "{}\n{}\n{}\n",
            job_line("j-000001", r#"{"n":1}"#),
            job_line("j-000002", r#"{"fail":true}"#),
            job_line("j-000003", r#"{"n":3}"#)
        ),
    )
    .unwrap();
    let script = write_worker_script(&dir);
    let template = format!("{script} {{id}} {{payload_file}}");

    let (code, stdout, stderr) = run_cli(&[
        "run",
        q.to_str().unwrap(),
        "--exec",
        &template,
        "--workers",
        "1",
    ]);

    assert_eq!(code, 1, "a failed job means exit 1 (stderr: {stderr})");
    assert!(stdout.is_empty(), "run writes progress to stderr");
    // Deterministic, id-sorted lifecycle lines (single worker).
    let lines: Vec<&str> = stderr.lines().collect();
    assert_eq!(lines[0], "CLAIM j-000001 by w1");
    assert!(lines[1].starts_with("OK j-000001 ("));
    assert_eq!(lines[2], "CLAIM j-000002 by w1");
    assert_eq!(lines[3], "FAIL j-000002 (exit 2) err=boom");
    assert_eq!(lines[4], "CLAIM j-000003 by w1");
    assert!(lines[5].starts_with("OK j-000003 ("));
    assert_eq!(lines[6], "summary: done=2 failed=1 skipped=0 pending=0 claimed_left=0");
    assert!(lines[7].starts_with("telemetry: jobs=3 max_concurrent=1 total_busy_ms="));

    // Final queue state: results recorded, failures keep last_error.
    let jobs = parse_queue(&fs::read_to_string(&q).unwrap()).unwrap();
    let by_id = |id: &str| jobs.iter().find(|j| j.id == id).unwrap();
    assert_eq!(by_id("j-000001").status, Status::Done);
    assert_eq!(by_id("j-000001").last_error, None);
    assert_eq!(by_id("j-000002").status, Status::Failed);
    assert_eq!(by_id("j-000002").last_error.as_deref(), Some("exit 2: boom"));
    assert_eq!(by_id("j-000002").attempts, 1);
    assert_eq!(by_id("j-000003").status, Status::Done);
    // The sidecar lock is gone when the run finishes.
    assert!(!dir.join("q.jsonl.lock").exists());
}

#[test]
fn e2e_json_report_shape_and_max_jobs() {
    let dir = temp_dir("report");
    let q = dir.join("q.jsonl");
    let lines: Vec<String> = (1..=3)
        .map(|i| job_line(&format!("j-{i}"), r#"{"n":9}"#))
        .collect();
    fs::write(&q, format!("{}\n", lines.join("\n"))).unwrap();
    let script = write_worker_script(&dir);
    let template = format!("{script} {{payload_file}}");
    let report_path = dir.join("report.json");

    let (code, _, stderr) = run_cli(&[
        "run",
        q.to_str().unwrap(),
        "--exec",
        &template,
        "--max-jobs",
        "2",
        "--json-report",
        report_path.to_str().unwrap(),
    ]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stderr.contains("summary: done=2 failed=0 skipped=0 pending=1"));

    let v = parse_json(&fs::read_to_string(&report_path).unwrap()).unwrap();
    let summary = v.get("summary").unwrap();
    assert_eq!(summary.get("done").unwrap().as_int(), Some(2));
    assert_eq!(summary.get("pending").unwrap().as_int(), Some(1));
    let tel = v.get("telemetry").unwrap();
    assert_eq!(tel.get("jobs").unwrap().as_int(), Some(2));
    assert_eq!(tel.get("max_concurrent").unwrap().as_int(), Some(1));
    assert!(tel.get("total_busy_ms").unwrap().as_int().unwrap() >= 0);
    let jobs = match v.get("jobs").unwrap() {
        dosyaisler::qfile::Json::Arr(a) => a,
        other => panic!("jobs is not an array: {other:?}"),
    };
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].get("id").unwrap().as_str(), Some("j-1"));
    assert_eq!(jobs[1].get("id").unwrap().as_str(), Some("j-2"));
    assert_eq!(jobs[0].get("outcome").unwrap().as_str(), Some("done"));

    // Queue still holds the third pending job.
    let jobs = parse_queue(&fs::read_to_string(&q).unwrap()).unwrap();
    assert_eq!(jobs.iter().filter(|j| j.status == Status::Pending).count(), 1);
}

#[test]
fn inspect_counts_head_and_error_paths() {
    let dir = temp_dir("inspect");
    let q = dir.join("q.jsonl");
    let failed_line = r#"{"id": "j-3", "type": "demo", "payload": {"n":3}, "status": "failed", "attempts": 1, "max_attempts": 3, "last_error": "exit 2: boom", "queued_seq": 3}"#;
    let lines = [
        job_line("j-1", r#"{"n":1}"#),
        job_line("j-2", r#"{"n":2}"#),
        failed_line.to_string(),
    ];
    fs::write(&q, format!("{}\n", lines.join("\n"))).unwrap();

    let (code, stdout, _) = run_cli(&["inspect", q.to_str().unwrap()]);
    assert_eq!(code, 0);
    for expected in [
        "file:",
        "total: 3",
        "pending: 2",
        "claimed: 0",
        "done: 0",
        "failed: 1",
        "head:",
    ] {
        assert!(stdout.contains(expected), "stdout missing '{expected}': {stdout}");
    }
    assert!(stdout.contains(r#""id": "j-1""#), "head shows raw lines: {stdout}");

    // Malformed queue file: inspect and run both exit 2.
    let bad = dir.join("bad.jsonl");
    fs::write(&bad, "{\"id\": \"a\", \"type\": \"t\"}\nnot json\n").unwrap();
    let (code, _, stderr) = run_cli(&["inspect", bad.to_str().unwrap()]);
    assert_eq!(code, 2);
    assert!(stderr.contains("line 2"), "stderr: {stderr}");
    let (code, _, _) = run_cli(&["run", bad.to_str().unwrap(), "--exec", "true"]);
    assert_eq!(code, 2);

    // Held sidecar lock: run exits 1 after the (zero) lock timeout. The
    // sidecar embeds a fresh timestamp so it must NOT be stolen as stale.
    let held = dir.join("held.jsonl");
    fs::write(&held, format!("{}\n", job_line("j-1", r#"{}"#))).unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    fs::write(
        dir.join("held.jsonl.lock"),
        format!("pid=1 unix_ms={now_ms}\n"),
    )
    .unwrap();
    let (code, _, stderr) = run_cli(&[
        "run",
        held.to_str().unwrap(),
        "--exec",
        "true",
        "--lock-timeout",
        "0",
    ]);
    assert_eq!(code, 1);
    assert!(stderr.contains("lock"), "stderr: {stderr}");
}
