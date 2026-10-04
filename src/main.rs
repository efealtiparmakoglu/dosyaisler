//! dosyaisler — persistent JSONL file-queue worker.
//!
//! Thin CLI on top of the library modules: argument parsing, exit codes and
//! output. All logic lives in `lib.rs` modules.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use dosyaisler::clock::RealClock;
use dosyaisler::exec::TemplateRunner;
use dosyaisler::lock::LockConfig;
use dosyaisler::qfile::{self, json_to_compact, parse_queue, write_atomic};
use dosyaisler::worker::{run_queue, RunError, RunOptions};

const VERSION: &str = env!("CARGO_PKG_VERSION");

const USAGE: &str = "\
dosyaisler — persistent JSONL file-queue worker

USAGE:
    dosyaisler run QFILE --exec TEMPLATE [OPTIONS]
    dosyaisler inspect QFILE
    dosyaisler (--help | --version)

COMMANDS:
    run        claim and execute jobs from the queue file until it is empty
    inspect    show status counts and the head lines of the queue file

OPTIONS (run):
    --exec TEMPLATE     command template; split on spaces, tokens {id},
                        {type}, {payload} (compact JSON, one arg) and
                        {payload_file} (temp file, deleted after the run)
    --workers N         parallel workers, 1..=32 (default 1)
    --max-jobs N        stop after N claimed jobs (exit 0)
    --lock-timeout S    seconds to wait for the queue lock (default 5)
    --stale-lock S      seconds before an orphaned lock file is removed (default 60)
    --json-report FILE  write a machine-readable run report (JSON)
    -h, --help          show this help

EXIT CODES:
    0  clean run (no failed jobs)
    1  lock timeout, or at least one job failed
    2  usage error, missing/unreadable queue file, or malformed queue file
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    code_to_exit(real_main(&args))
}

fn code_to_exit(code: i32) -> ExitCode {
    ExitCode::from(code as u8)
}

fn usage_err(msg: &str) -> i32 {
    eprintln!("dosyaisler: {msg}");
    eprintln!("run 'dosyaisler --help' for usage");
    2
}

/// Split `--flag=value` into (`--flag`, Some(`value`)).
fn split_inline(arg: &str) -> (&str, Option<String>) {
    match arg.split_once('=') {
        Some((name, value)) => (name, Some(value.to_string())),
        None => (arg, None),
    }
}

fn take_value(
    args: &[String],
    i: &mut usize,
    name: &str,
    inline: Option<String>,
) -> Result<String, String> {
    if let Some(v) = inline {
        return Ok(v);
    }
    if *i < args.len() {
        let v = args[*i].clone();
        *i += 1;
        return Ok(v);
    }
    Err(format!("missing value for {name}"))
}

fn real_main(args: &[String]) -> i32 {
    match args.first().map(String::as_str) {
        Some("run") => cmd_run(&args[1..]),
        Some("inspect") => cmd_inspect(&args[1..]),
        Some("-h" | "--help" | "help") => {
            print!("{USAGE}");
            0
        }
        Some("-V" | "--version" | "version") => {
            println!("dosyaisler {VERSION}");
            0
        }
        Some(other) => usage_err(&format!("unknown command '{other}'")),
        None => usage_err("missing command (run|inspect)"),
    }
}

fn cmd_run(args: &[String]) -> i32 {
    let mut qfile: Option<String> = None;
    let mut exec: Option<String> = None;
    let mut workers: usize = 1;
    let mut max_jobs: Option<usize> = None;
    let mut lock_timeout = 5.0f64;
    let mut stale_lock = 60.0f64;
    let mut json_report: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        let raw = args[i].clone();
        let (name, inline) = split_inline(&raw);
        i += 1;
        match name {
            "--exec" => match take_value(args, &mut i, name, inline) {
                Ok(v) => exec = Some(v),
                Err(e) => return usage_err(&e),
            },
            "--workers" => match take_value(args, &mut i, name, inline) {
                Ok(v) => match v.parse::<usize>() {
                    Ok(n) => workers = n,
                    Err(_) => return usage_err(&format!("--workers expects a number, got '{v}'")),
                },
                Err(e) => return usage_err(&e),
            },
            "--max-jobs" => match take_value(args, &mut i, name, inline) {
                Ok(v) => match v.parse::<usize>() {
                    Ok(n) => max_jobs = Some(n),
                    Err(_) => return usage_err(&format!("--max-jobs expects a number, got '{v}'")),
                },
                Err(e) => return usage_err(&e),
            },
            "--lock-timeout" => match take_value(args, &mut i, name, inline) {
                Ok(v) => match v.parse::<f64>() {
                    Ok(s) if s >= 0.0 => lock_timeout = s,
                    _ => return usage_err("--lock-timeout expects a non-negative number of seconds"),
                },
                Err(e) => return usage_err(&e),
            },
            "--stale-lock" => match take_value(args, &mut i, name, inline) {
                Ok(v) => match v.parse::<f64>() {
                    Ok(s) if s >= 0.0 => stale_lock = s,
                    _ => return usage_err("--stale-lock expects a non-negative number of seconds"),
                },
                Err(e) => return usage_err(&e),
            },
            "--json-report" => match take_value(args, &mut i, name, inline) {
                Ok(v) => json_report = Some(v),
                Err(e) => return usage_err(&e),
            },
            "-h" | "--help" => {
                print!("{USAGE}");
                return 0;
            }
            _ => {
                if raw.starts_with('-') && raw.len() > 1 {
                    return usage_err(&format!("unknown option '{raw}'"));
                }
                if qfile.is_some() {
                    return usage_err(&format!("unexpected extra argument '{raw}'"));
                }
                qfile = Some(raw);
            }
        }
    }

    let Some(qfile) = qfile else {
        return usage_err("missing QFILE argument");
    };
    let Some(template) = exec else {
        return usage_err("missing required option --exec TEMPLATE");
    };
    let Some(runner) = TemplateRunner::from_template(&template) else {
        return usage_err("--exec template is empty");
    };
    if !(1..=32).contains(&workers) {
        return usage_err("--workers must be between 1 and 32");
    }
    if max_jobs == Some(0) {
        return usage_err("--max-jobs must be at least 1");
    }

    let qpath = Path::new(&qfile);
    if !qpath.exists() {
        return usage_err(&format!("queue file not found: {qfile}"));
    }
    if !qpath.is_file() {
        return usage_err(&format!("queue path is not a regular file: {qfile}"));
    }

    let opts = RunOptions {
        workers,
        max_jobs,
        lock: LockConfig {
            timeout: Duration::from_secs_f64(lock_timeout),
            stale: Duration::from_secs_f64(stale_lock),
        },
    };
    let clock = Arc::new(RealClock);
    match run_queue(qpath, &opts, Arc::new(runner), clock) {
        Ok(report) => {
            // Deterministic report: per-job lines (sorted by id), then the summary.
            for line in report.job_lines() {
                eprintln!("{line}");
            }
            eprintln!("{}", report.summary_line());
            eprintln!("{}", report.telemetry_line());
            if let Some(path) = json_report {
                let body = format!("{}\n", json_to_compact(&report.to_json()));
                if let Err(e) = write_atomic(Path::new(&path), body.as_bytes()) {
                    eprintln!("dosyaisler: cannot write json report '{path}': {e}");
                    return 1;
                }
            }
            report.exit_code()
        }
        Err(RunError::Format(e)) => {
            eprintln!("dosyaisler: queue format error in '{qfile}': {e}");
            2
        }
        Err(RunError::LockTimeout { path, timeout }) => {
            eprintln!(
                "dosyaisler: could not acquire queue lock within {timeout:?}: {} (another worker running?)",
                path.display()
            );
            1
        }
        Err(e) => {
            eprintln!("dosyaisler: {e}");
            1
        }
    }
}

fn cmd_inspect(args: &[String]) -> i32 {
    let mut positional: Vec<&String> = Vec::new();
    for a in args {
        match a.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return 0;
            }
            s if s.starts_with('-') && s.len() > 1 => {
                return usage_err(&format!("unknown option '{s}' (inspect takes only QFILE)"));
            }
            _ => positional.push(a),
        }
    }
    let [qfile] = positional.as_slice() else {
        return usage_err("usage: dosyaisler inspect QFILE");
    };
    let qpath = Path::new(qfile);
    if !qpath.exists() {
        return usage_err(&format!("queue file not found: {qfile}"));
    }
    let text = match std::fs::read_to_string(qpath) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("dosyaisler: cannot read queue file '{qfile}': {e}");
            return 2;
        }
    };
    let jobs = match parse_queue(&text) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("dosyaisler: queue format error in '{qfile}': {e}");
            return 2;
        }
    };
    let count = |s: qfile::Status| jobs.iter().filter(|j| j.status == s).count();
    println!("file: {qfile}");
    println!("total: {}", jobs.len());
    println!("pending: {}", count(qfile::Status::Pending));
    println!("claimed: {}", count(qfile::Status::Claimed));
    println!("done: {}", count(qfile::Status::Done));
    println!("failed: {}", count(qfile::Status::Failed));
    let head: Vec<&str> = text.lines().map(str::trim).filter(|l| !l.is_empty()).take(5).collect();
    if !head.is_empty() {
        println!("head:");
        for line in head {
            println!("{line}");
        }
    }
    0
}
