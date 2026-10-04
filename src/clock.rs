//! Injectable clock.
//!
//! The CLI uses [`RealClock`]. Tests inject [`FakeClock`] so durations,
//! timestamps and lock staleness are deterministic.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Source of time for the worker: wall time (unix ms) for timestamps and
/// monotonic time (ms since an arbitrary epoch) for durations and timeouts.
pub trait Clock: Send + Sync {
    fn now_unix_ms(&self) -> u64;
    fn now_mono_ms(&self) -> u64;
}

/// Real system clock. Monotonic readings are ms since process start.
pub struct RealClock;

impl Clock for RealClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn now_mono_ms(&self) -> u64 {
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        START.get_or_init(Instant::now).elapsed().as_millis() as u64
    }
}

/// Deterministic fake clock for tests. `advance_ms` moves both wall and
/// monotonic time forward.
pub struct FakeClock {
    wall_ms: AtomicU64,
    mono_ms: AtomicU64,
}

impl FakeClock {
    pub fn new(wall_ms: u64) -> FakeClock {
        FakeClock {
            wall_ms: AtomicU64::new(wall_ms),
            mono_ms: AtomicU64::new(0),
        }
    }

    pub fn advance_ms(&self, n: u64) {
        self.wall_ms.fetch_add(n, Ordering::SeqCst);
        self.mono_ms.fetch_add(n, Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now_unix_ms(&self) -> u64 {
        self.wall_ms.load(Ordering::SeqCst)
    }

    fn now_mono_ms(&self) -> u64 {
        self.mono_ms.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_clock_advance_moves_wall_and_mono() {
        let c = FakeClock::new(1_000);
        assert_eq!(c.now_unix_ms(), 1_000);
        assert_eq!(c.now_mono_ms(), 0);
        c.advance_ms(5);
        c.advance_ms(2);
        assert_eq!(c.now_unix_ms(), 1_007);
        assert_eq!(c.now_mono_ms(), 7);
    }

    #[test]
    fn real_clock_is_monotonic_and_sane() {
        let c = RealClock;
        let a = c.now_mono_ms();
        let b = c.now_mono_ms();
        assert!(b >= a);
        // Any plausible unix timestamp after 2020-01-01 (ms).
        assert!(c.now_unix_ms() > 1_577_836_800_000);
    }
}
