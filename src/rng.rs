//! Deterministic pseudo random number generator (SplitMix64).
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
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below requires a positive bound");
        self.next_u64() % n
    }

    /// Uniformly pick a value in the inclusive range `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(hi >= lo, "range requires hi >= lo");
        lo + self.below(hi - lo + 1)
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
}
