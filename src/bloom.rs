//! Bloom filters. One is embedded in every SSTable so a point lookup can skip
//! a file without touching its index or data blocks.

/// 64-bit mix used as the base hash; the two 32-bit halves drive double hashing.
pub fn hash_key(key: &[u8]) -> u64 {
    // FNV-1a, then an avalanche step so the low bits are well mixed.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 29;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 32;
    h
}

/// Builds the filter block for a set of keys.
pub struct BloomBuilder {
    bits_per_key: usize,
    hashes: Vec<u64>,
}

impl BloomBuilder {
    pub fn new(bits_per_key: usize) -> Self {
        BloomBuilder {
            bits_per_key: bits_per_key.max(1),
            hashes: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        self.hashes.push(hash_key(key));
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Encodes the filter: the bit array followed by a single byte holding `k`.
    pub fn finish(&self) -> Vec<u8> {
        // k = ln(2) * bits_per_key minimises the false-positive rate.
        let k = ((self.bits_per_key as f64 * 0.69) as usize).clamp(1, 30) as u8;
        let mut bits = (self.hashes.len() * self.bits_per_key).max(64);
        bits = bits.div_ceil(8) * 8;
        let mut out = vec![0u8; bits / 8 + 1];
        for &h in &self.hashes {
            set_bits(&mut out[..bits / 8], h, k, bits);
        }
        let last = out.len() - 1;
        out[last] = k;
        out
    }
}

fn set_bits(buf: &mut [u8], h: u64, k: u8, bits: usize) {
    let mut pos = (h & 0xffff_ffff) as u32;
    let delta = (h >> 32) as u32;
    for _ in 0..k {
        let bitpos = (pos as usize) % bits;
        buf[bitpos / 8] |= 1 << (bitpos % 8);
        pos = pos.wrapping_add(delta);
    }
}

/// A decoded filter block. `may_contain` never returns false for a key that was
/// added, so a `false` answer is proof the key is absent from the file.
#[derive(Debug)]
pub struct BloomFilter {
    data: Vec<u8>,
    k: u8,
    bits: usize,
}

impl BloomFilter {
    pub fn decode(block: &[u8]) -> Option<BloomFilter> {
        if block.len() < 2 {
            return None;
        }
        let k = block[block.len() - 1];
        let data = block[..block.len() - 1].to_vec();
        let bits = data.len() * 8;
        if k == 0 || k > 30 || bits == 0 {
            return None;
        }
        Some(BloomFilter { data, k, bits })
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.may_contain_hash(hash_key(key))
    }

    pub fn may_contain_hash(&self, h: u64) -> bool {
        let mut pos = (h & 0xffff_ffff) as u32;
        let delta = (h >> 32) as u32;
        for _ in 0..self.k {
            let bitpos = (pos as usize) % self.bits;
            if self.data[bitpos / 8] & (1 << (bitpos % 8)) == 0 {
                return false;
            }
            pos = pos.wrapping_add(delta);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(n: usize, bits_per_key: usize) -> BloomFilter {
        let mut b = BloomBuilder::new(bits_per_key);
        for i in 0..n {
            b.add(format!("key{i:08}").as_bytes());
        }
        BloomFilter::decode(&b.finish()).unwrap()
    }

    #[test]
    fn no_false_negatives() {
        let f = build(5000, 10);
        for i in 0..5000 {
            assert!(
                f.may_contain(format!("key{i:08}").as_bytes()),
                "missing {i}"
            );
        }
    }

    #[test]
    fn false_positive_rate_is_low() {
        let f = build(5000, 10);
        let mut fp = 0;
        for i in 5000..15000 {
            if f.may_contain(format!("key{i:08}").as_bytes()) {
                fp += 1;
            }
        }
        // 10 bits/key should land near 1%; allow slack for hash luck.
        assert!(fp < 400, "false positive rate too high: {fp}/10000");
    }

    #[test]
    fn empty_filter_decodes() {
        let b = BloomBuilder::new(10);
        let f = BloomFilter::decode(&b.finish()).unwrap();
        assert!(!f.may_contain(b"anything"));
    }
}
