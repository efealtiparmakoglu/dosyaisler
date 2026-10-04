//! dosyaisler — a persistent file-queue worker.
//!
//! Reads jobs from a JSONL queue file, claims them under an advisory sidecar
//! lock, executes them in parallel via a command-template runner, and writes
//! results (done/failed + telemetry) back with atomic whole-file rewrites.
//!
//! Layout:
//! - [`qfile`]: queue file format (JSON parse/serialize, canonical output, atomic rewrite)
//! - [`lock`]: `QFILE.lock` sidecar lock with stale takeover
//! - [`exec`]: command-template runner (`{id}`, `{type}`, `{payload}`, `{payload_file}`)
//! - [`worker`]: claim/execute/finish engine, thread pool, telemetry, reports
//! - [`clock`]: injectable clock (real for the CLI, fake for tests)

pub mod clock;
pub mod exec;
pub mod lock;
pub mod qfile;
pub mod worker;
