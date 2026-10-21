//! Small encoding helpers shared by the on-disk formats.

use crate::error::{corrupt, Result};

/// One record as it travels through the engine: a key and either a value or
/// `None`, which is a tombstone.
pub type Entry = (Vec<u8>, Option<Vec<u8>>);

pub fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn get_u32(buf: &[u8], off: usize) -> Result<u32> {
    if off + 4 > buf.len() {
        return corrupt("truncated u32");
    }
    Ok(u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()))
}

pub fn get_u64(buf: &[u8], off: usize) -> Result<u64> {
    if off + 8 > buf.len() {
        return corrupt("truncated u64");
    }
    Ok(u64::from_le_bytes(buf[off..off + 8].try_into().unwrap()))
}

/// LEB128-style varint, used for the per-entry headers inside a block.
pub fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Decodes a varint at `off`, returning the value and the new offset.
pub fn get_varint(buf: &[u8], off: usize) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut i = off;
    loop {
        if i >= buf.len() {
            return corrupt("truncated varint");
        }
        if shift > 63 {
            return corrupt("varint overflow");
        }
        let byte = buf[i];
        result |= ((byte & 0x7f) as u64) << shift;
        i += 1;
        if byte < 0x80 {
            return Ok((result, i));
        }
        shift += 7;
    }
}

/// Length of the prefix `a` and `b` have in common.
pub fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    let max = a.len().min(b.len());
    let mut i = 0;
    while i < max && a[i] == b[i] {
        i += 1;
    }
    i
}

pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

pub fn from_hex(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return corrupt("odd-length hex string");
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16);
        let lo = (bytes[i + 1] as char).to_digit(16);
        match (hi, lo) {
            (Some(h), Some(l)) => out.push(((h << 4) | l) as u8),
            _ => return corrupt("invalid hex digit"),
        }
        i += 2;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            put_varint(&mut buf, v);
            let (got, off) = get_varint(&buf, 0).unwrap();
            assert_eq!(got, v);
            assert_eq!(off, buf.len());
        }
    }

    #[test]
    fn hex_roundtrip() {
        let data = vec![0u8, 1, 15, 16, 200, 255];
        assert_eq!(from_hex(&to_hex(&data)).unwrap(), data);
        assert_eq!(to_hex(&[]), "");
        assert!(from_hex("abc").is_err());
        assert!(from_hex("zz").is_err());
    }

    #[test]
    fn prefix() {
        assert_eq!(shared_prefix(b"abcd", b"abce"), 3);
        assert_eq!(shared_prefix(b"", b"abc"), 0);
        assert_eq!(shared_prefix(b"abc", b"abc"), 3);
    }
}
