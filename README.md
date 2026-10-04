# dosyaisler

`dosyaisler` ("işler" = works/jobs) is a queue **worker**: it takes jobs from a
persistent JSONL queue file, executes them as external commands in parallel,
and writes results and telemetry back into the same file. No daemon, no
database, no dependencies — the queue is a plain text file you can inspect,
edit, and version.

Typical uses: batch jobs that must survive a crash (the queue file is the
state), running a folder of tasks with bounded parallelism, or gluing a
producer script to a consumer without infrastructure.

## Queue file format

JSONL, one job per line. Each line has exactly these fields:

```
{"id": "j-000001", "type": "email", "payload": {"to":"ada@example.com"}, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 1}
```

- `status` is one of `pending | claimed | done | failed`.
- `payload` is arbitrary JSON; it is always serialized compactly.
- Parsing is lenient: unknown extra fields are ignored (and dropped on
  rewrite, since the worker always writes the canonical 8-field form), and
  missing optional fields fall back to defaults (`status=pending`,
  `attempts=0`, `max_attempts=3`, `last_error=null`, `queued_seq=0`).
  `id` and `type` are required; a line without them is a format error (exit 2).
- The whole file is rewritten atomically (temp file + fsync + rename), so a
  crash mid-write never corrupts the queue.

## Build

```
cargo build --release
```

The binary is `target/release/dosyaisler`. No external crates are used.

## Usage

```
dosyaisler run QFILE --exec TEMPLATE [--workers N] [--max-jobs N]
                [--lock-timeout S] [--stale-lock S] [--json-report FILE]
dosyaisler inspect QFILE
```

- `--exec TEMPLATE` — command template, split on spaces (no quoting). Tokens:
  `{id}`, `{type}`, `{payload}` (compact JSON, one argument), `{payload_file}`
  (payload written to a temp file; the path is substituted and the file is
  deleted right after the command exits). A job succeeds iff the command
  exits 0. Unknown `{tokens}` are left literal.
- `--workers N` — parallel worker threads, 1..=32 (default 1).
- `--max-jobs N` — stop after N claimed jobs (exit 0). The quota is enforced
  inside the claim critical section, so at most N jobs are ever claimed.
- `--lock-timeout S` — seconds to wait for the queue lock before giving up
  (default 5).
- `--stale-lock S` — seconds before an orphaned lock file is removed and
  taken over (default 60).
- `--json-report FILE` — write a machine-readable run report.

`inspect` prints status counts plus the head lines of the file.

Exit codes: `0` clean run, `1` lock timeout or at least one job failed,
`2` usage error, missing/unreadable queue file, or malformed queue file.

## Example

Queue file (`examples/queue.jsonl`, shipped with this repo):

```
{"id": "j-000001", "type": "email", "payload": {"to":"ada@example.com","subject":"hello"}, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 1}
{"id": "j-000002", "type": "email", "payload": {"to":"bob@example.com","fail":true}, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 2}
{"id": "j-000003", "type": "resize", "payload": {"src":"a.png","w":128}, "status": "pending", "attempts": 0, "max_attempts": 3, "last_error": null, "queued_seq": 3}
{"id": "j-000004", "type": "resize", "payload": {"src":"b.png","w":256}, "status": "pending", "attempts": 3, "max_attempts": 3, "last_error": "exit 1: source not found", "queued_seq": 4}
```

`examples/job.sh` fails (exit 2) when the payload contains `"fail":true`.
Run the worker with two parallel workers:

```
$ dosyaisler run examples/queue.jsonl \
      --exec "$(pwd)/examples/job.sh {id} {payload_file}" --workers 2
CLAIM j-000001 by w2
OK j-000001 (0.26s)
CLAIM j-000002 by w1
FAIL j-000002 (exit 2) err=simulated failure for j-000002
CLAIM j-000003 by w2
OK j-000003 (0.01s)
SKIP j-000004 exhausted
summary: done=2 failed=1 skipped=1 pending=1 claimed_left=0
telemetry: jobs=3 max_concurrent=2 total_busy_ms=526 wall_ms=367
$ echo $?
1
```

Afterwards the file records the outcome of every job: `j-000001`/`j-000003`
are `done` with `last_error: null`, `j-000002` is `failed` with
`"last_error": "exit 2: simulated failure for j-000002"`, and `j-000004`
(queued with `attempts == max_attempts`) was skipped as exhausted and stays
`pending`. Exit code is 1 because one job failed.

Progress goes to stderr: one line per job lifecycle (`CLAIM`, `OK`, `FAIL`,
`SKIP`), then the summary and telemetry. To keep reports deterministic
regardless of thread interleaving, the per-job lines are buffered and printed
at the end sorted by job id; a job's `CLAIM` line is always adjacent to its
completion line.

`dosyaisler inspect examples/queue.jsonl` prints `total/pending/claimed/
done/failed` counts plus the head lines, and exits 0 even when jobs have
failed.

## How it works

### Two-phase claim/finish

1. **Claim** — under the lock: scan the file, record exhausted skips, pick
   the first claimable job (`pending` with `attempts < max_attempts`), set
   `status=claimed` and `attempts+=1`, atomically rewrite the file. The
   command then runs *outside* the lock.
2. **Finish** — re-acquire the lock and write the terminal state: `done`
   with `last_error=null`, or `failed` with `last_error` set to the exit
   code plus the last 500 characters of stderr (e.g. `exit 2: boom`). A
   command that cannot even spawn (missing binary) is a failure with a
   clear `spawn failed: 'name': ...` message.

This worker never re-queues failed jobs (only `pending` jobs are claimable),
and it does not wait for external producers: when a scan finds no claimable
job, it prints the summary and exits. Jobs already in `claimed` state from a
crashed earlier run are left untouched. Pending jobs that arrive with
`attempts >= max_attempts` are reported as `SKIP <id> exhausted` and remain
pending; they never block termination.

### Locking: sidecar file with stale takeover

std has no `flock` and the project uses no external crates, so mutual
exclusion uses an atomic-create sidecar: `QFILE.lock` is created with
`create_new(true)`. Exactly one process/thread can create it; the holder
does its read-modify-write and removes the file on release. Contenders
retry with exponential backoff (1–25 ms) until `--lock-timeout` elapses,
then exit 1.

If a holder dies before releasing, the sidecar stays behind as an orphan.
The holder embeds its pid and acquisition time inside the lock file, and a
contender that finds it older than `--stale-lock` removes it and takes over
(the file mtime is the fallback if the content is unreadable). The result is
the same advisory contract as flock-based locking — only one process mutates
the queue at a time; the test suite enforces the invariant (100 concurrent
claims produce exactly 100 recorded claims, and two threads incrementing a
counter 50 times each under the lock lose nothing).

### Parallelism and telemetry

`--workers N` runs N OS threads, each looping claim → execute → finish. A
failing job affects no other job (error isolation). Telemetry observes the
true overlap: every execution bumps a live concurrency counter, and the
report contains `jobs`, `max_concurrent` (peak observed) and
`total_busy_ms` (sum of per-job execution time), plus wall time. All
timestamps and durations come from an injectable clock — the CLI uses the
real clock, the tests use a fake one.

### Reports

`--json-report FILE` writes a deterministic JSON document:

```
{"queue":"...","started_at_unix_ms":...,"wall_ms":...,
 "summary":{"done":2,"failed":1,"skipped":0,"pending":0,"claimed_left":0},
 "telemetry":{"jobs":3,"max_concurrent":2,"total_busy_ms":526},
 "jobs":[{"id":"j-000001","worker":"w2","outcome":"done","reason":"exit 0",
          "duration_ms":264,"exit":0,"error":null,"attempts":1}, ...]}
```

`jobs` is sorted by id, so the file is byte-stable between identical runs.

## Tests

```
cargo test --release        # 27 tests: format, locking, exec, worker, e2e CLI
cargo clippy --all-targets -- -D warnings
```

Highlights of what the suite pins down: canonical round-trip byte-exactness,
lenient parsing, no double-claiming under thread contention, stale lock
takeover, lock timeout, `max_concurrent` measured exactly on a fake clock,
`--max-jobs` quota, `{payload_file}` lifecycle, error isolation, exit codes.

## Layout

- `src/qfile.rs` — JSON parser/serializer (order-preserving), job format, atomic rewrite
- `src/lock.rs` — sidecar lock with stale takeover
- `src/exec.rs` — template runner (`Runner` trait; tests inject fakes)
- `src/worker.rs` — claim/execute/finish engine, thread pool, telemetry, reports
- `src/clock.rs` — injectable clock (real / fake)
- `src/main.rs` — CLI: argument parsing, exit codes, output
- `tests/cli.rs` — end-to-end tests against the real binary
