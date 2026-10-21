//! CRC-32C (Castagnoli), the checksum guarding every record and block on disk.

const POLY: u32 = 0x82f6_3b78;

static TABLE: [[u32; 256]; 8] = build_tables();

const fn build_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            j += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    // Slicing-by-8 derived tables.
    let mut i = 0;
    while i < 256 {
        let mut crc = tables[0][i];
        let mut t = 1;
        while t < 8 {
            crc = tables[0][(crc & 0xff) as usize] ^ (crc >> 8);
            tables[t][i] = crc;
            t += 1;
        }
        i += 1;
    }
    tables
}

/// CRC-32C over `data`.
pub fn crc32c(data: &[u8]) -> u32 {
    extend(0, data)
}

/// Continues a CRC-32C over another chunk of data.
pub fn extend(prev: u32, data: &[u8]) -> u32 {
    let mut crc = !prev;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        let a = u32::from_le_bytes(c[0..4].try_into().unwrap()) ^ crc;
        let b = u32::from_le_bytes(c[4..8].try_into().unwrap());
        crc = TABLE[7][(a & 0xff) as usize]
            ^ TABLE[6][((a >> 8) & 0xff) as usize]
            ^ TABLE[5][((a >> 16) & 0xff) as usize]
            ^ TABLE[4][((a >> 24) & 0xff) as usize]
            ^ TABLE[3][(b & 0xff) as usize]
            ^ TABLE[2][((b >> 8) & 0xff) as usize]
            ^ TABLE[1][((b >> 16) & 0xff) as usize]
            ^ TABLE[0][((b >> 24) & 0xff) as usize];
    }
    for &byte in chunks.remainder() {
        crc = TABLE[0][((crc ^ byte as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // Standard CRC-32C test vectors.
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"a"), 0xc1d0_4330);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8a91_36aa);
    }

    #[test]
    fn extend_matches_oneshot() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1000).collect();
        let once = crc32c(&data);
        let split = extend(crc32c(&data[..37]), &data[37..]);
        assert_eq!(once, split);
    }

    #[test]
    fn detects_single_bit_flip() {
        let mut data = vec![7u8; 64];
        let before = crc32c(&data);
        data[31] ^= 0x04;
        assert_ne!(before, crc32c(&data));
    }
}
