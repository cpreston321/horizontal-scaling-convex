//! Deterministic randomness for the simulation.
//!
//! Every random choice in a run flows from a single seed so a failure can be
//! reproduced exactly by re-running with the same seed. We use ChaCha8 (via
//! `rand_chacha`) rather than the thread RNG so the stream is fully determined
//! by the seed and independent of platform or thread scheduling.

use rand::{
    Rng as _,
    SeedableRng,
};
use rand_chacha::ChaCha8Rng;

/// Seeded source of all randomness in a simulation run.
pub struct Rng {
    inner: ChaCha8Rng,
}

impl Rng {
    pub fn from_seed(seed: u64) -> Self {
        Self {
            inner: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// Return `true` with probability `p` (clamped to `[0, 1]`).
    pub fn chance(&mut self, p: f64) -> bool {
        self.inner.random::<f64>() < p.clamp(0.0, 1.0)
    }

    /// Uniform integer in `[lo, hi]` inclusive. Returns `lo` when `lo >= hi`.
    pub fn int_in(&mut self, lo: u64, hi: u64) -> u64 {
        if lo >= hi {
            return lo;
        }
        self.inner.random_range(lo..=hi)
    }

    /// Uniform index in `[0, len)`. Caller must ensure `len > 0`.
    pub fn index(&mut self, len: usize) -> usize {
        debug_assert!(len > 0, "index() requires a non-empty range");
        self.inner.random_range(0..len)
    }

    /// Choose a value from a non-empty slice.
    pub fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.index(items.len())]
    }
}
