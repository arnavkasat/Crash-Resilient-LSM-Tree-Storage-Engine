//! Deterministic key/value generation shared by the tests, the benchmarks and
//! the `lsmctl` crash-test driver, so a writer and a verifier in different
//! processes always agree on what the data should look like.

/// `key_for(7)` -> `b"key00000000000007"`.
pub fn key_for(i: u64) -> Vec<u8> {
    format!("key{i:014}").into_bytes()
}

/// A value whose contents are derived from `i`, so a verifier can detect a
/// value that belongs to a different key.
pub fn value_for(i: u64, size: usize) -> Vec<u8> {
    let head = format!("{i:016}");
    let mut v = Vec::with_capacity(size.max(head.len()));
    v.extend_from_slice(head.as_bytes());
    let filler = (i % 251) as u8;
    while v.len() < size {
        v.push(filler ^ (v.len() as u8));
    }
    v.truncate(size.max(head.len()));
    v
}

/// xorshift64*: a tiny, reproducible PRNG. Good enough to pick keys and crash
/// timings without pulling in a dependency.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform-ish value in `[0, n)`.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_deterministic_and_distinct() {
        assert_eq!(value_for(1, 64), value_for(1, 64));
        assert_ne!(value_for(1, 64), value_for(2, 64));
        assert_eq!(value_for(5, 64).len(), 64);
        // A size smaller than the embedded header still keeps the header.
        assert_eq!(value_for(5, 4).len(), 16);
    }

    #[test]
    fn keys_sort_in_numeric_order() {
        assert!(key_for(9) < key_for(10));
        assert!(key_for(0) < key_for(u32::MAX as u64));
    }

    #[test]
    fn rng_is_reproducible() {
        let a: Vec<u64> = (0..5)
            .scan(Rng::new(42), |r, _| Some(r.next_u64()))
            .collect();
        let b: Vec<u64> = (0..5)
            .scan(Rng::new(42), |r, _| Some(r.next_u64()))
            .collect();
        assert_eq!(a, b);
        assert!(Rng::new(7).below(10) < 10);
    }
}
