//! Deterministic pseudo random number generator (`SplitMix64`).
//!
//! The whole point of Shepherd is reproducibility. A given seed must always
//! produce the same workload and therefore the same placement, so the engine
//! never reaches for the operating system entropy source. This is the only
//! source of randomness in the crate.

/// A small, fast, fully deterministic PRNG.
#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// Create a generator from a seed. The same seed always yields the same
    /// stream of values.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    /// Next raw 64 bit value.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniformly pick a value in `0..n`. Panics if `n` is zero.
    ///
    /// Uses rejection sampling so every value is equally likely even when `n`
    /// is not a power of two (a plain `next_u64() % n` is biased).
    ///
    /// # Panics
    /// Panics when `n` is zero, which is never a meaningful bound.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below requires a positive bound");
        // `threshold` is 2^64 mod n, the count of values at the bottom of the
        // range that would bias the modulo. Values below it are discarded.
        let threshold = n.wrapping_neg() % n;
        loop {
            let v = self.next_u64();
            if v >= threshold {
                return v % n;
            }
        }
    }

    /// Uniformly pick a value in the inclusive range `lo..=hi`.
    ///
    /// # Panics
    /// Panics when `hi < lo`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(hi >= lo, "range requires hi >= lo");
        let span = hi - lo;
        // `span + 1` overflows when `hi` is `u64::MAX`; fall back to the full
        // 64 bit range, which is exactly `lo..=hi` shifted by `lo`.
        let r = if span == u64::MAX {
            self.next_u64()
        } else {
            self.below(span + 1)
        };
        lo + r
    }

    /// Return true with roughly the given percent probability.
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seed_diverges() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn range_is_bounded() {
        let mut r = Rng::new(7);
        for _ in 0..10_000 {
            let v = r.range(3, 9);
            assert!((3..=9).contains(&v));
        }
    }

    #[test]
    fn range_handles_u64_max() {
        let mut r = Rng::new(11);
        let mut low_half = 0u32;
        for _ in 0..1_000 {
            if r.range(0, u64::MAX) < u64::MAX / 2 {
                low_half += 1;
            }
        }
        // A full range sample must land in both halves, never panic.
        assert!(low_half > 100 && low_half < 900, "low_half={low_half}");
    }

    #[test]
    fn below_is_uniform_for_odd_bounds() {
        let mut r = Rng::new(5);
        let mut counts = [0u32; 3];
        for _ in 0..30_000 {
            let idx = usize::try_from(r.below(3)).expect("bound is 3");
            counts[idx] += 1;
        }
        for c in counts {
            assert!(c > 8_000 && c < 12_000, "counts={counts:?}");
        }
    }
}
