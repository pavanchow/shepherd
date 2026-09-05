//! An injected logical clock.
//!
//! Time in Shepherd is a monotonically increasing tick counter, never wall
//! clock time. Failure detection (missed heartbeats) is expressed purely in
//! ticks, which keeps the whole simulation deterministic and lets tests fast
//! forward through arbitrary spans of time.

/// A monotonic logical clock measured in ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clock {
    now: u64,
}

impl Clock {
    /// Create a clock started at the given tick.
    #[must_use]
    pub fn new(start: u64) -> Self {
        Clock { now: start }
    }

    /// The current tick.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.now
    }

    /// Advance the clock by `dt` ticks and return the new time.
    pub fn advance(&mut self, dt: u64) -> u64 {
        self.now = self.now.saturating_add(dt);
        self.now
    }
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advances_monotonically() {
        let mut c = Clock::new(0);
        assert_eq!(c.now(), 0);
        assert_eq!(c.advance(1), 1);
        assert_eq!(c.advance(5), 6);
        assert_eq!(c.now(), 6);
    }
}
