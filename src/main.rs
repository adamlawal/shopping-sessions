// Shopping Sessions pipeline. See rules.md for the exact spec.
//
// Stages:
//   1. mmap every input file, split into newline-aligned chunks.
//   2. links -> union-find -> each ID's person name (byte-wise smallest ID).
//   3. events (parallel): parse + validate each row, partition by event_id key.
//   4. per event_id partition: keep the copy with the smallest ingest_ts, drop
//      it if invalid, map user -> person, repartition by person key.
//   5. clicks (parallel): parse + validate, map user -> person, partition by person.
//   6. per person partition: sort, sessionize, credit + assists, aggregate.
//   7. write Parquet.
//
// IDs are turned into 128-bit keys: an ID of up to 15 bytes is packed exactly
// (bytes + length), a longer one is a 127-bit hash with the top bit set, so
// short and long keys never meet. Equality of keys is equality of IDs.

use memmap2::Mmap;

use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const CHUNK: usize = 4 << 20;
const NPART: usize = 128;
const BLK: usize = 128;

const K_INVALID: u8 = 0;
const K_VALID: u8 = 1;
const K_PURCHASE: u8 = 3;

// ---------------------------------------------------------------- keys and hashing

const P0: u64 = 0xa076_1d64_78bd_642f;
const P1: u64 = 0xe703_7ed1_a0b4_28db;
const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
const P3: u64 = 0x5899_65cc_7537_4cc3;

#[inline(always)]
fn fold(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline]
fn hash_bytes(b: &[u8], seed: u64) -> u64 {
    let mut h = seed ^ (b.len() as u64).wrapping_mul(P0);
    let mut i = 0;
    while i + 8 <= b.len() {
        let w = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        h = fold(h ^ w, P1);
        i += 8;
    }
    if i < b.len() {
        let mut t = [0u8; 8];
        t[..b.len() - i].copy_from_slice(&b[i..]);
        h = fold(h ^ u64::from_le_bytes(t), P2);
    }
    fold(h, P3)
}

#[inline(always)]
fn ld8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}
#[inline(always)]
fn ld4(b: &[u8], i: usize) -> u64 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
}

#[inline(always)]
fn key128(b: &[u8]) -> u128 {
    let l = b.len();
    if l <= 15 {
        // exact packing: bytes little-endian, zero padded, length in byte 15
        let (lo, hi) = if l >= 8 {
            let lo = ld8(b, 0);
            let hi = if l == 8 { 0 } else { ld8(b, l - 8) >> (8 * (16 - l)) };
            (lo, hi)
        } else if l >= 4 {
            let a = ld4(b, 0);
            let c = ld4(b, l - 4) >> (8 * (8 - l));
            (a | (c << 32), 0)
        } else {
            let mut x = 0u64;
            for (j, &c) in b.iter().enumerate() {
                x |= (c as u64) << (8 * j);
            }
            (x, 0)
        };
        (lo as u128) | ((hi as u128) << 64) | ((l as u128) << 120)
    } else {
        let h1 = hash_bytes(b, P1);
        let h2 = hash_bytes(b, P3);
        ((h1 as u128) << 64 | h2 as u128) | (1u128 << 127)
    }
}

#[inline(always)]
fn mix(k: u128) -> u64 {
    fold((k as u64) ^ P0, ((k >> 64) as u64) ^ P2)
}

/// Partition from the LOW 32 bits of mix(); hash tables and filters use the HIGH bits,
/// so keys that share a partition still spread over the whole table.
#[inline(always)]
fn part_of(k: u128) -> usize {
    ((mix(k) as u32 as usize) * NPART) >> 32
}

#[derive(Default)]
struct KHasher(u64);
impl Hasher for KHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, b: &[u8]) {
        self.0 = hash_bytes(b, self.0 ^ P1);
    }
    #[inline]
    fn write_u128(&mut self, k: u128) {
        self.0 = mix(k);
    }
}
type KMap<V> = HashMap<u128, V, BuildHasherDefault<KHasher>>;

// ---------------------------------------------------------------- files

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

/// Drop the page mappings of the whole pages inside `c` (file-backed, read-only: the
/// data stays in the page cache, and any later access would simply fault it back in).
fn release(c: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        let pg = 4096usize;
        let a = (c.as_ptr() as usize + pg - 1) & !(pg - 1);
        let b = (c.as_ptr() as usize + c.len()) & !(pg - 1);
        if b > a {
            libc::madvise(a as *mut libc::c_void, b - a, libc::MADV_DONTNEED);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = c;
}

/// Map items to results in parallel; workers claim items in index order from a counter.
fn ordered_map<T: Sync, R: Send>(items: &[T], nthreads: usize, f: impl Fn(usize, &T) -> R + Sync) -> Vec<R> {
    let next = std::sync::atomic::AtomicUsize::new(0);
    let mut parts: Vec<Vec<(usize, R)>> = (0..nthreads.max(1))
        .into_par_iter()
        .map(|_| {
            let mut out = Vec::new();
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= items.len() {
                    break;
                }
                out.push((i, f(i, &items[i])));
            }
            out
        })
        .collect();
    let mut all: Vec<(usize, R)> = parts.drain(..).flatten().collect();
    all.sort_unstable_by_key(|x| x.0);
    all.into_iter().map(|x| x.1).collect()
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let mut o = memmap2::MmapOptions::new();
    if std::env::var("SS_POP").is_ok() {
        o.populate();
    }
    unsafe { o.map(&f).ok() }
}

/// Body of a CSV file (after the header line), split into newline-aligned chunks.
fn chunks_of<'a>(data: &'a [u8], out: &mut Vec<&'a [u8]>) {
    let body = match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => return,
    };
    let mut start = 0;
    while start < body.len() {
        let mut end = (start + CHUNK).min(body.len());
        if end < body.len() {
            match memchr::memchr(b'\n', &body[end..]) {
                Some(i) => end += i + 1,
                None => end = body.len(),
            }
        }
        out.push(&body[start..end]);
        start = end;
    }
}

/// Iterate lines of a chunk ('\n' terminated; a final unterminated line counts).
#[inline]
fn for_lines<'a>(chunk: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut start = 0;
    for i in memchr::memchr_iter(b'\n', chunk) {
        f(&chunk[start..i]);
        start = i + 1;
    }
    if start < chunk.len() {
        f(&chunk[start..]);
    }
}

/// Bytes equal to zero in x get 0x80, all others 0 (exact, no false positives).
#[inline(always)]
fn zero_bytes(x: u64) -> u64 {
    const LO7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    !(((x & LO7).wrapping_add(LO7)) | x | LO7)
}
const COMMAS: u64 = 0x2c2c_2c2c_2c2c_2c2c;
const QUOTES: u64 = 0x2222_2222_2222_2222;

/// Split a line into exactly N fields, unquoting. Returns false if malformed.
#[inline]
fn split_fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    if len == 0 {
        return false;
    }
    let mut n = 0usize;
    let mut start = 0usize;
    let mut quotes = 0u64;
    let mut i = 0usize;
    while i + 8 <= len {
        let w = u64::from_le_bytes(line[i..i + 8].try_into().unwrap());
        quotes |= zero_bytes(w ^ QUOTES);
        let mut m = zero_bytes(w ^ COMMAS);
        while m != 0 {
            let pos = i + (m.trailing_zeros() >> 3) as usize;
            if n + 1 >= N {
                return false;
            }
            out[n] = &line[start..pos];
            n += 1;
            start = pos + 1;
            m &= m - 1;
        }
        i += 8;
    }
    while i < len {
        let c = line[i];
        if c == b',' {
            if n + 1 >= N {
                return false;
            }
            out[n] = &line[start..i];
            n += 1;
            start = i + 1;
        } else if c == b'"' {
            quotes = 1;
        }
        i += 1;
    }
    if n + 1 != N {
        return false;
    }
    out[n] = &line[start..len];
    if quotes != 0 {
        for v in out.iter_mut() {
            let raw = *v;
            let l = raw.len();
            if l >= 2 && raw[0] == b'"' && raw[l - 1] == b'"' {
                let inner = &raw[1..l - 1];
                if memchr::memchr(b'"', inner).is_some() {
                    return false;
                }
                *v = inner;
            } else if memchr::memchr(b'"', raw).is_some() {
                return false;
            }
        }
    }
    true
}

const NLS: u64 = 0x0a0a_0a0a_0a0a_0a0a;

/// Bit masks for 64 bytes at p: (commas, newlines, quotes), bit i = byte i.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let cm = _mm256_set1_epi8(b',' as i8);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let qt = _mm256_set1_epi8(b'"' as i8);
        let m = |x: __m256i, y: __m256i| -> u64 {
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, y)) as u32 as u64)
        };
        (
            m(a, cm) | (m(b, cm) << 32),
            m(a, nl) | (m(b, nl) << 32),
            m(a, qt) | (m(b, qt) << 32),
        )
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::aarch64::*;
    unsafe {
        let v0 = vld1q_u8(p);
        let v1 = vld1q_u8(p.add(16));
        let v2 = vld1q_u8(p.add(32));
        let v3 = vld1q_u8(p.add(48));
        const W: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let w = vld1q_u8(W.as_ptr());
        let mk = |c: u8| -> u64 {
            let cc = vdupq_n_u8(c);
            let m0 = vandq_u8(vceqq_u8(v0, cc), w);
            let m1 = vandq_u8(vceqq_u8(v1, cc), w);
            let m2 = vandq_u8(vceqq_u8(v2, cc), w);
            let m3 = vandq_u8(vceqq_u8(v3, cc), w);
            let s01 = vpaddq_u8(m0, m1);
            let s23 = vpaddq_u8(m2, m3);
            let s = vpaddq_u8(s01, s23);
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        (mk(b','), mk(b'\n'), mk(b'"'))
    }
}

#[cfg(not(any(all(target_arch = "x86_64", target_feature = "avx2"), target_arch = "aarch64")))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let b = unsafe { std::slice::from_raw_parts(p, 64) };
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for (i, &x) in b.iter().enumerate() {
        c |= ((x == b',') as u64) << i;
        n |= ((x == b'\n') as u64) << i;
        q |= ((x == b'"') as u64) << i;
    }
    (c, n, q)
}

/// Tokenize a whole chunk in one pass: for every well-formed line with exactly N
/// fields, call f with the unquoted fields. Malformed and empty lines are skipped.
/// A line's quotes are valid exactly when every quote is part of a field wrapped
/// in quotes, i.e. when the line's quote count is twice the number of wrapped fields.
#[inline(always)]
fn for_rows<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = chunk.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut n = 0usize;
    let mut field_start = 0usize;
    let mut q_total = 0u32; // quotes before the current block
    let mut q_line = 0u32; // running quote count at line start

    #[inline(always)]
    fn finish<'a, const N: usize>(fields: &mut [&'a [u8]; N], nq: u32) -> bool {
        if nq == 0 {
            return true;
        }
        let mut wrapped = 0u32;
        for v in fields.iter_mut() {
            let raw = *v;
            let l = raw.len();
            if l >= 2 && raw[0] == b'"' && raw[l - 1] == b'"' {
                wrapped += 2;
                *v = &raw[1..l - 1];
            }
        }
        wrapped == nq
    }

    let mut tail = [0u8; 64];
    let mut base = 0usize;
    while base < len {
        let (cm, nl, qt) = if base + 64 <= len {
            unsafe { masks64(chunk.as_ptr().add(base)) }
        } else {
            let r = len - base;
            tail[..r].copy_from_slice(&chunk[base..]);
            tail[r..].fill(0);
            unsafe { masks64(tail.as_ptr()) }
        };
        let mut m = cm | nl;
        while m != 0 {
            let bit = m.trailing_zeros();
            let pos = base + bit as usize;
            if (nl >> bit) & 1 == 0 {
                if n < N {
                    fields[n] = unsafe { chunk.get_unchecked(field_start..pos) };
                }
                n += 1;
                field_start = pos + 1;
            } else {
                let q_here = q_total + (qt & ((1u64 << bit) - 1)).count_ones();
                if n + 1 == N {
                    fields[n] = unsafe { chunk.get_unchecked(field_start..pos) };
                    if finish(&mut fields, q_here - q_line) {
                        f(&fields);
                    }
                }
                n = 0;
                field_start = pos + 1;
                q_line = q_here;
            }
            m &= m - 1;
        }
        q_total += qt.count_ones();
        base += 64;
    }
    // final line without a trailing newline
    if field_start < len && n + 1 == N {
        fields[n] = &chunk[field_start..len];
        if finish(&mut fields, q_total - q_line) {
            f(&fields);
        }
    }
}

// ---------------------------------------------------------------- parsing values

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some(a as u32 * 10 + c as u32)
    } else {
        None
    }
}

#[inline]
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days from 1970-01-01 to the first of each month, years 1970..2100 (index (y-1970)*12 + m-1).
static MONTH_DAYS: [i32; 130 * 12] = {
    let mut v = [0i32; 130 * 12];
    let mut y = 1970i64;
    while y < 2100 {
        let mut m = 1i64;
        while m <= 12 {
            // days_from_civil(y, m, 1), written out for const evaluation
            let yy = if m <= 2 { y - 1 } else { y };
            let era = yy / 400;
            let yoe = yy - era * 400;
            let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5;
            let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            v[((y - 1970) * 12 + m - 1) as usize] = (era * 146097 + doe - 719468) as i32;
            m += 1;
        }
        y += 1;
    }
    v
};

#[inline(always)]
fn dim(y: u32, m: u32) -> u32 {
    match m {
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// 8 ASCII digits (already checked) little-endian in x -> value (first char most significant).
#[inline(always)]
fn parse8(x: u64) -> u64 {
    let x = x.wrapping_sub(0x3030_3030_3030_3030);
    let x = (x.wrapping_mul(10)).wrapping_add(x >> 8) & 0x00FF_00FF_00FF_00FF;
    let x = (x.wrapping_mul(100)).wrapping_add(x >> 16) & 0x0000_FFFF_0000_FFFF;
    (x.wrapping_mul(10000)).wrapping_add(x >> 32) & 0xFFFF_FFFF
}

/// True if every byte of x selected by `lanes` (0xFF per lane) is an ASCII digit.
#[inline(always)]
fn digits_in(x: u64, lanes: u64) -> bool {
    const HI: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    let hi = HI & lanes;
    let want = 0x3030_3030_3030_3030 & lanes;
    // high nibble 3, and adding 6 keeps it 3 (so the low nibble is 0..9); no carry leaves a byte
    (x & hi) == want && (x.wrapping_add(0x0606_0606_0606_0606) & hi) == want
}

/// ingest_ts is always valid F1 (rules.md). Fast path for exactly that shape, with
/// the full parser as the fallback for anything else, so the result never changes.
#[inline(always)]
fn parse_ingest(v: &[u8]) -> i64 {
    if v.len() == 20 && v[19] == b'Z' && v[10] == b'T' {
        let a = ld8(v, 0);
        let b = ld8(v, 8);
        if digits_in(a, 0x00FF_FF00_FFFF_FFFF) && digits_in(b, 0xFFFF_00FF_FF00_FFFF) {
            let d = |x: u64, i: u32| ((x >> (8 * i)) as u32 & 0xFF) - 0x30;
            let y = d(a, 0) * 1000 + d(a, 1) * 100 + d(a, 2) * 10 + d(a, 3);
            let mo = d(a, 5) * 10 + d(a, 6);
            let dd = d(b, 0) * 10 + d(b, 1);
            let h = d(b, 3) * 10 + d(b, 4);
            let mi = d(b, 6) * 10 + d(b, 7);
            let (s1, s2) = (v[17].wrapping_sub(b'0') as u32, v[18].wrapping_sub(b'0') as u32);
            if (1970..2100).contains(&y) && (1..=12).contains(&mo) && dd >= 1 && dd <= dim(y, mo) && h < 24 && mi < 60 && s1 < 6 && s2 < 10
                && v[4] == b'-' && v[7] == b'-' && v[13] == b':' && v[16] == b':'
            {
                let days = MONTH_DAYS[((y - 1970) * 12 + mo - 1) as usize] as i64 + dd as i64 - 1;
                return (days * 86400 + (h * 3600 + mi * 60 + s1 * 10 + s2) as i64) * 1000;
            }
        }
    }
    parse_ts(v).unwrap_or(i64::MAX)
}

/// F1 / F2 / F3 timestamp to epoch ms; None if bad. (Window checked by caller.)
#[inline(always)]
fn parse_ts(v: &[u8]) -> Option<i64> {
    let l = v.len();
    if (l == 20 || l == 25) && v[4] == b'-' {
        // YYYY-MM-DDTHH:MM:SS then Z or +HH:MM / -HH:MM
        let a = ld8(v, 0); // Y Y Y Y - M M -
        let b = ld8(v, 8); // D D T H H : M M
        let c = ld8(v, l - 8); // tail; for F1: ...M:SSZ ; we read seconds directly below
        let _ = c;
        // separators: a[4]='-' a[7]='-' b[2]='T' b[5]=':' v[16]=':'
        const SEP_A: u64 = 0x2D00_0000_2D00_0000; // bytes 4 and 7 = '-'
        const SEP_A_M: u64 = 0xFF00_00FF_0000_0000;
        if a & SEP_A_M != (0x2D00_002D_0000_0000) {
            let _ = SEP_A;
            return None;
        }
        if (b >> 16) as u8 != b'T' || (b >> 40) as u8 != b':' || v[16] != b':' {
            return None;
        }
        if !digits_in(a, 0x00FF_FF00_FFFF_FFFF) || !digits_in(b, 0xFFFF_00FF_FF00_FFFF) {
            return None;
        }
        let (s1, s2) = (v[17].wrapping_sub(b'0'), v[18].wrapping_sub(b'0'));
        if s1 > 5 || s2 > 9 {
            return None;
        }
        let d = |x: u64, i: u32| ((x >> (8 * i)) as u32 & 0xFF) - 0x30;
        let y = d(a, 0) * 1000 + d(a, 1) * 100 + d(a, 2) * 10 + d(a, 3);
        let mo = d(a, 5) * 10 + d(a, 6);
        let dd = d(b, 0) * 10 + d(b, 1);
        let h = d(b, 3) * 10 + d(b, 4);
        let mi = d(b, 6) * 10 + d(b, 7);
        let sec = s1 as u32 * 10 + s2 as u32;
        if !(1..=12).contains(&mo) || dd == 0 || h > 23 || mi > 59 || dd > dim(y, mo) {
            return None;
        }
        let days = if (1970..2100).contains(&y) {
            MONTH_DAYS[((y - 1970) * 12 + mo - 1) as usize] as i64 + dd as i64 - 1
        } else {
            days_from_civil(y as i64, mo, dd)
        };
        let mut secs = days * 86400 + (h * 3600 + mi * 60 + sec) as i64;
        if l == 20 {
            if v[19] != b'Z' {
                return None;
            }
        } else {
            let sign = v[19];
            if (sign != b'+' && sign != b'-') || v[22] != b':' {
                return None;
            }
            let oh = d2(v, 20)?;
            let om = d2(v, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 3600 + om * 60) as i64;
            if sign == b'+' {
                secs -= off;
            } else {
                secs += off;
            }
        }
        return Some(secs * 1000);
    }
    if l == 13 {
        // the common F3 form: 13 digits, epoch ms
        let a = ld8(v, 0);
        let b = ld8(v, 5); // digits 5..12 (overlaps a by 3)
        if digits_in(a, u64::MAX) && digits_in(b, u64::MAX) {
            return Some((parse8(a) * 100_000 + parse8(b) % 100_000) as i64);
        }
        return None;
    }
    if l == 0 {
        return None;
    }
    // other F3 lengths: digits only, leading zeros allowed
    let mut x: i64 = 0;
    let mut sig = 0usize;
    for &c in v {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
        }
        if sig <= 16 {
            x = x * 10 + d as i64;
        }
    }
    if sig > 15 {
        return Some(i64::MAX); // valid form, far outside any window
    }
    Some(x)
}

#[inline]
fn parse_price(v: &[u8]) -> Option<u32> {
    if v.is_empty() {
        return None;
    }
    let mut x: u64 = 0;
    for &c in v {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        x = x * 10 + d as u64;
        if x > 10_000_000 {
            // keep scanning for non-digits? any non-digit is invalid too, and so is > 1e7
            return None;
        }
    }
    if x >= 1 { Some(x as u32) } else { None }
}

// ---------------------------------------------------------------- records

/// Parsed event row, before link lookup.
#[derive(Clone, Copy)]
struct Row<'a> {
    ukey: u128,
    eid: u128,
    user: &'a [u8],
    ingest: i32,
    ts: u32,
    price: u32,
    rowid: u32,
    cat: u8,
    kind: u8,
}

/// Dedup record: one per parsed event row.
#[derive(Clone, Copy)]
struct D {
    eid: u128,
    ingest: i32, // seconds relative to window start
    rowid: u32,  // chunk << 20 | row in chunk
    ppart: u16,  // person partition of the row, or NO_PART if the row is invalid
}
const NO_PART: u16 = u16::MAX;

#[derive(Clone, Copy)]
struct Ev {
    lid: u32, // person-local id within the partition
    ts: u32,
    pc: u32, // price << 8 | category; 0 for views and carts
    rowid: u32,
}

#[derive(Clone, Copy)]
struct Cl<'a> {
    ts: i64,
    cid: &'a [u8],
    lid: u32,
    camp: u16,
}

struct URow<'a> {
    name: &'a [u8],
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u8>,
}

#[derive(Clone, Copy, Default)]
struct CAgg {
    sessions: i64,
    purch: i64,
    revenue: i64,
    assisted: i64,
}

/// Open-addressing table of linked IDs: key128 -> index. Key 0 marks an empty slot
/// (only the empty string packs to 0, and empty IDs are never stored or looked up).
struct LinkTable {
    keys: Vec<u128>,
    vals: Vec<u32>,
    mask: usize,
    shift: u32,
}

impl LinkTable {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2 + 16).next_power_of_two();
        LinkTable { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1, shift: 64 - cap.trailing_zeros() }
    }
    #[inline(always)]
    fn get(&self, k: u128) -> u32 {
        let mut h = (mix(k) >> self.shift) as usize;
        loop {
            let x = self.keys[h];
            if x == k {
                return self.vals[h];
            }
            if x == 0 {
                return u32::MAX;
            }
            h = (h + 1) & self.mask;
        }
    }
    #[inline]
    fn get_or_insert(&mut self, k: u128, mut f: impl FnMut() -> u32) -> u32 {
        let mut h = (mix(k) >> self.shift) as usize;
        loop {
            let x = self.keys[h];
            if x == k {
                return self.vals[h];
            }
            if x == 0 {
                let v = f();
                self.keys[h] = k;
                self.vals[h] = v;
                return v;
            }
            h = (h + 1) & self.mask;
        }
    }
}

/// Growable open-addressing map u128 -> u32 for person-local ids.
struct PTable {
    keys: Vec<u128>,
    vals: Vec<u32>,
    mask: usize,
    len: usize,
    shift: u32,
}

impl PTable {
    fn new(n: usize) -> Self {
        let cap = (n * 2 + 16).next_power_of_two();
        PTable { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1, len: 0, shift: 64 - cap.trailing_zeros() }
    }
    #[inline(always)]
    fn get(&self, k: u128) -> u32 {
        let mut h = (mix(k) >> self.shift) as usize;
        loop {
            let x = self.keys[h];
            if x == k {
                return self.vals[h];
            }
            if x == 0 {
                return u32::MAX;
            }
            h = (h + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let cap = (self.mask + 1) * 2;
        let mut keys = vec![0u128; cap];
        let mut vals = vec![0u32; cap];
        let mask = cap - 1;
        let shift = 64 - cap.trailing_zeros();
        for (i, &k) in self.keys.iter().enumerate() {
            if k != 0 {
                let mut h = (mix(k) >> shift) as usize;
                while keys[h] != 0 {
                    h = (h + 1) & mask;
                }
                keys[h] = k;
                vals[h] = self.vals[i];
            }
        }
        self.keys = keys;
        self.vals = vals;
        self.mask = mask;
        self.shift = shift;
    }
    #[inline]
    fn get_or_insert(&mut self, k: u128, f: impl FnOnce() -> u32) -> u32 {
        let mut h = (mix(k) >> self.shift) as usize;
        loop {
            let x = self.keys[h];
            if x == k {
                return self.vals[h];
            }
            if x == 0 {
                let v = f();
                self.keys[h] = k;
                self.vals[h] = v;
                self.len += 1;
                if self.len * 2 > self.mask {
                    self.grow();
                }
                return v;
            }
            h = (h + 1) & self.mask;
        }
    }
}

/// Second 64-bit hash of a key, independent of mix(); never 0.
#[inline(always)]
fn fp(k: u128) -> u64 {
    fold((k as u64) ^ P3, ((k >> 64) as u64) ^ P1) | 1
}

/// Lock-free table: user-ID fingerprint -> person code (partition << 25 | local id).
/// Filled with every linked ID before the events are read; unlinked users are
/// added the first time a valid event names them.
#[repr(C)]
struct Slot {
    key: std::sync::atomic::AtomicU64,
    val: std::sync::atomic::AtomicU32,
    _pad: u32,
}

/// Lock-free table: user-ID fingerprint -> person code (partition << 25 | local id).
/// Key and value share one 16-byte slot, so a lookup touches one cache line.
struct UserTable {
    slots: Vec<Slot>,
    shift: u32,
    mask: usize,
}

impl UserTable {
    fn new(n: usize) -> Self {
        let cap = (n + 1024).next_power_of_two();
        // Slot is two zeroed words with the layout of [u64; 2]: allocate zeroed memory directly
        let slots: Vec<Slot> = unsafe { std::mem::transmute(vec![[0u64; 2]; cap]) };
        UserTable { slots, shift: 64 - cap.trailing_zeros(), mask: cap - 1 }
    }
    #[inline(always)]
    fn val(&self, h: usize) -> u32 {
        use std::sync::atomic::Ordering::Acquire;
        loop {
            let v = self.slots[h].val.load(Acquire);
            if v != 0 {
                return v - 1;
            }
            std::hint::spin_loop();
        }
    }
    #[inline(always)]
    fn get(&self, f: u64) -> u32 {
        use std::sync::atomic::Ordering::Acquire;
        let mut h = (f >> self.shift) as usize;
        loop {
            let k = self.slots[h].key.load(Acquire);
            if k == f {
                return self.val(h);
            }
            if k == 0 {
                return u32::MAX;
            }
            h = (h + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get_or_insert(&self, f: u64, make: impl FnOnce() -> u32) -> u32 {
        use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
        let mut h = (f >> self.shift) as usize;
        let mut probes = 0usize;
        loop {
            probes += 1;
            assert!(probes <= self.mask, "user table full");
            let k = self.slots[h].key.load(Acquire);
            if k == f {
                return self.val(h);
            }
            if k == 0 {
                match self.slots[h].key.compare_exchange(0, f, AcqRel, Acquire) {
                    Ok(_) => {
                        let v = make();
                        self.slots[h].val.store(v + 1, Release);
                        return v;
                    }
                    Err(cur) => {
                        if cur == f {
                            return self.val(h);
                        }
                    }
                }
            }
            h = (h + 1) & self.mask;
        }
    }
}

const LID_BITS: u32 = 25;
#[inline(always)]
fn code(pp: usize, lid: u32) -> u32 {
    ((pp as u32) << LID_BITS) | lid
}

struct Defer<F: FnMut()>(F);
impl<F: FnMut()> Drop for Defer<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

struct Persons<'a> {
    shards: Vec<LinkTable>,
    offs: Vec<u32>,
    person: Vec<(u128, &'a [u8])>,
}

impl<'a> Persons<'a> {
    /// Person of a user ID, given its key and the slot lookup result.
    #[inline(always)]
    fn of(&self, ukey: u128, user: &'a [u8], idx: u32) -> (u128, &'a [u8]) {
        if idx == u32::MAX { (ukey, user) } else { self.person[idx as usize] }
    }
    #[inline(always)]
    fn lookup(&self, ukey: u128) -> u32 {
        if self.person.is_empty() {
            return u32::MAX;
        }
        let s = part_of(ukey);
        let l = self.shards[s].get(ukey);
        if l == u32::MAX { u32::MAX } else { self.offs[s] + l }
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = p;
    }
    x
}

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

fn new_parts<T>(chunk_len: usize, bytes_per_row: usize) -> Vec<Vec<T>> {
    let rows = chunk_len / bytes_per_row + 16;
    let cap = rows / NPART + rows / NPART / 3 + 8;
    (0..NPART).map(|_| Vec::with_capacity(cap)).collect()
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    // run_fixtures passes DATA OUT as the last two args; env vars otherwise.
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (PathBuf::from(env_or("DATA_DIR", "/data")), PathBuf::from(env_or("OUTPUT_DIR", "/output")))
    };
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();
    let verbose = std::env::var("SS_QUIET").is_err();

    // CPU seconds used so far by the whole process (Linux: /proc/self/stat utime+stime)
    fn cpu_now() -> f64 {
        if let Ok(st) = std::fs::read_to_string("/proc/self/stat") {
            if let Some(i) = st.rfind(')') {
                let f: Vec<&str> = st[i + 2..].split(' ').collect();
                if f.len() > 13 {
                    let u: f64 = f[11].parse().unwrap_or(0.0);
                    let s: f64 = f[12].parse().unwrap_or(0.0);
                    return (u + s) / 100.0;
                }
            }
        }
        0.0
    }
    let cpu_last = std::cell::Cell::new(0.0f64);
    macro_rules! lap {
        ($s:expr) => {
            if verbose {
                let c = cpu_now();
                eprintln!("[{:>8.3}s cpu+{:.2}] {}", t0.elapsed().as_secs_f64(), c - cpu_last.get(), $s);
                cpu_last.set(c);
            }
        };
    }

    // ---------------- manifest
    let manifest = std::fs::read(data_dir.join("manifest.json")).expect("manifest");
    let get = |key: &str| -> i64 {
        let s = std::str::from_utf8(&manifest).unwrap();
        let i = s.find(key).expect("key");
        let rest = &s[i + key.len()..];
        let q1 = rest.find(':').unwrap();
        let rest = &rest[q1 + 1..];
        let a = rest.find('"').unwrap();
        let b = rest[a + 1..].find('"').unwrap();
        parse_ts(rest[a + 1..a + 1 + b].as_bytes()).expect("manifest ts")
    };
    let ws = get("\"window_start\"");
    let we = get("\"window_end\"");
    let cws = ws - WEEK_MS;
    let ws_s = ws.div_euclid(1000);
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day0 + 1).max(1) as usize;

    // ---------------- map files
    let ev_maps: Vec<Mmap> = list_csv(&data_dir.join("events")).par_iter().filter_map(|p| map_file(p)).collect();
    let cl_maps: Vec<Mmap> = list_csv(&data_dir.join("clicks")).par_iter().filter_map(|p| map_file(p)).collect();
    let ln_maps: Vec<Mmap> = list_csv(&data_dir.join("links")).par_iter().filter_map(|p| map_file(p)).collect();
    // Ask the kernel to start reading every input file now, asynchronously and with many
    // requests in flight, so a cold page cache is filled while we parse links and metadata
    // instead of each worker stalling on small synchronous faults later.
    if std::env::var("SS_NOPF").is_err() {
        let regions: Vec<(usize, usize)> = ln_maps
            .iter()
            .chain(ev_maps.iter())
            .chain(cl_maps.iter())
            .map(|m| (m.as_ptr() as usize, m.len()))
            .collect();
        // split into ~8 MB pieces, issued in order by 4 threads
        let mut pieces: Vec<(usize, usize)> = Vec::new();
        for (p, l) in regions {
            let mut o = 0;
            while o < l {
                let n = (l - o).min(8 << 20);
                pieces.push((p + o, n));
                o += n;
            }
        }
        let pieces = std::sync::Arc::new(pieces);
        let next = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..4 {
            let (pieces, next) = (pieces.clone(), next.clone());
            std::thread::spawn(move || loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= pieces.len() {
                    break;
                }
                let (p, l) = pieces[i];
                let a = p & !4095;
                unsafe {
                    libc::madvise(a as *mut libc::c_void, l + (p - a), libc::MADV_WILLNEED);
                }
            });
        }
    }
    let prod_map = map_file(&data_dir.join("products.csv"));
    let camp_map = map_file(&data_dir.join("campaigns.csv"));

    // ---------------- products / categories
    let mut cat_names: Vec<&[u8]> = Vec::new();
    let mut cat_idx: KMap<u8> = KMap::default();
    let mut products: KMap<u8> = KMap::default();
    if let Some(m) = &prod_map {
        let mut ch = Vec::new();
        chunks_of(m, &mut ch);
        for c in ch {
            for_lines(c, |line| {
                let mut f = [&[][..]; 2];
                if !split_fields(line, &mut f) {
                    return;
                }
                let ci = *cat_idx.entry(key128(f[1])).or_insert_with(|| {
                    cat_names.push(f[1]);
                    (cat_names.len() - 1) as u8
                });
                products.insert(key128(f[0]), ci);
            });
        }
    }
    let unknown_cat: u8 = match cat_idx.get(&key128(b"unknown")) {
        Some(&i) => i,
        None => {
            cat_names.push(&b"unknown"[..]);
            (cat_names.len() - 1) as u8
        }
    };
    let ncat = cat_names.len();
    assert!(ncat < 256);
    let mut order: Vec<usize> = (0..ncat).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(cat_names[b]));
    let mut cat_rank = vec![0u16; ncat];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u16;
    }

    // ---------------- campaigns
    let mut camp_ids: Vec<&[u8]> = Vec::new();
    let mut camp_chan: Vec<&[u8]> = Vec::new();
    let mut campaigns: KMap<u16> = KMap::default();
    if let Some(m) = &camp_map {
        let mut ch = Vec::new();
        chunks_of(m, &mut ch);
        for c in ch {
            for_lines(c, |line| {
                let mut f = [&[][..]; 2];
                if !split_fields(line, &mut f) || f[0].is_empty() {
                    return;
                }
                let k = key128(f[0]);
                if !campaigns.contains_key(&k) {
                    campaigns.insert(k, camp_ids.len() as u16);
                    camp_ids.push(f[0]);
                    camp_chan.push(f[1]);
                }
            });
        }
    }
    let organic = camp_ids.len() as u16;
    let ncamp = camp_ids.len() + 1;
    lap!("metadata");

    // ---------------- links
    // 1. parse pairs (parallel), 2. shard every ID by key and give it a dense id
    // (parallel per shard), 3. map pairs to dense ids (parallel), 4. union-find on
    // plain u32 arrays (sequential, cache friendly), 5. name each person.
    let mut ln_chunks = Vec::new();
    for m in &ln_maps {
        chunks_of(m, &mut ln_chunks);
    }
    let pairs: Vec<(Vec<(u128, u128)>, Vec<Vec<(u128, &[u8])>>)> = ln_chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::with_capacity(c.len() / 20);
            let mut sh: Vec<Vec<(u128, &[u8])>> = new_parts(c.len(), 10);
            for_rows(c, |f: &[&[u8]; 2]| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    let (ka, kb) = (key128(f[0]), key128(f[1]));
                    v.push((ka, kb));
                    sh[part_of(ka)].push((ka, f[0]));
                    sh[part_of(kb)].push((kb, f[1]));
                }
            });
            (v, sh)
        })
        .collect();
    struct Shard<'a> {
        tab: LinkTable,
        keys: Vec<u128>,
        names: Vec<&'a [u8]>,
    }
    let shards: Vec<Shard> = (0..NPART)
        .into_par_iter()
        .map(|s| {
            let n: usize = pairs.iter().map(|c| c.1[s].len()).sum();
            let mut tab = LinkTable::with_capacity(n);
            let mut keys = Vec::with_capacity(n / 2 + 16);
            let mut names = Vec::with_capacity(n / 2 + 16);
            for c in &pairs {
                for &(k, nm) in &c.1[s] {
                    tab.get_or_insert(k, || {
                        keys.push(k);
                        names.push(nm);
                        (keys.len() - 1) as u32
                    });
                }
            }
            Shard { tab, keys, names }
        })
        .collect();
    let mut offs = vec![0u32; NPART + 1];
    for s in 0..NPART {
        offs[s + 1] = offs[s] + shards[s].keys.len() as u32;
    }
    let n_ids = offs[NPART] as usize;
    let gid = |k: u128| -> u32 {
        let s = part_of(k);
        offs[s] + shards[s].tab.get(k)
    };
    let edges: Vec<Vec<(u32, u32)>> = pairs.par_iter().map(|c| c.0.iter().map(|&(a, b)| (gid(a), gid(b))).collect()).collect();
    let mut parent: Vec<u32> = (0..n_ids as u32).collect();
    for v in &edges {
        for &(a, b) in v {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    let mut root: Vec<u32> = vec![0; n_ids];
    for g in 0..n_ids as u32 {
        root[g as usize] = find(&mut parent, g);
    }
    // per root: (smallest name, its key); names are never empty, so empty = unset
    let mut best: Vec<(&[u8], u128)> = vec![(&[][..], 0); n_ids];
    {
        let mut g = 0usize;
        for sh in &shards {
            for (l, &nm) in sh.names.iter().enumerate() {
                let b = &mut best[root[g] as usize];
                if b.0.is_empty() || nm < b.0 {
                    *b = (nm, sh.keys[l]);
                }
                g += 1;
            }
        }
    }

    // every person gets (partition, local id); linked persons first, in root order
    let counters: Vec<std::sync::atomic::AtomicU32> = (0..NPART).map(|_| std::sync::atomic::AtomicU32::new(0)).collect();
    let mut link_names: Vec<Vec<&[u8]>> = (0..NPART).map(|_| Vec::new()).collect();
    let mut root_code: Vec<u32> = vec![u32::MAX; n_ids];
    for g in 0..n_ids {
        let r = root[g] as usize;
        if root_code[r] == u32::MAX {
            let (nm, k) = best[r];
            let pp = part_of(k);
            let lid = link_names[pp].len() as u32;
            link_names[pp].push(nm);
            root_code[r] = code(pp, lid);
        }
    }
    for pp in 0..NPART {
        counters[pp].store(link_names[pp].len() as u32, std::sync::atomic::Ordering::Relaxed);
    }
    let ev_bytes: usize = ev_maps.iter().map(|m| m.len()).sum();
    let utab = UserTable::new(n_ids + ev_bytes / 1200 + 1024);
    (0..NPART).into_par_iter().for_each(|s| {
        for (l, &k) in shards[s].keys.iter().enumerate() {
            let g = offs[s] as usize + l;
            let c = root_code[root[g] as usize];
            utab.get_or_insert(fp(k), || c);
        }
    });
    lap!(format!("links: {} ids", n_ids));

    // ---------------- events: parse (valid rows go straight to their person partition)
    let mut ev_chunks = Vec::new();
    for m in &ev_maps {
        chunks_of(m, &mut ev_chunks);
    }
    assert!(ev_chunks.len() < 4096);
    let products = &products;
    let utab = &utab;
    let counters = &counters[..];
    #[inline(always)]
    fn flush_rows<'a>(
        rows: &mut Vec<Row<'a>>,
        utab: &UserTable,
        counters: &[std::sync::atomic::AtomicU32],
        dparts: &mut [Vec<D>],
        eparts: &mut [Vec<Ev>],
        nparts: &mut [Vec<(u32, u32, u32)>],
        arena: &mut Vec<u8>,
    ) {
        let mut cd = [u32::MAX; BLK];
        for (i, r) in rows.iter().enumerate() {
            if r.kind != K_INVALID {
                cd[i] = utab.get_or_insert(fp(r.ukey), || {
                    // a user seen for the first time and not linked: a person of their own
                    let pp = part_of(r.ukey);
                    let lid = counters[pp].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // copy the name: the event text is released right after this chunk
                    nparts[pp].push((lid, arena.len() as u32, r.user.len() as u32));
                    arena.extend_from_slice(r.user);
                    code(pp, lid)
                });
            }
        }
        for (i, r) in rows.iter().enumerate() {
            let mut ppart = NO_PART;
            if r.kind != K_INVALID {
                let pp = (cd[i] >> LID_BITS) as usize;
                let lid = cd[i] & ((1 << LID_BITS) - 1);
                ppart = pp as u16;
                eparts[pp].push(Ev { lid, ts: r.ts, pc: (r.price << 8) | r.cat as u32, rowid: r.rowid });
            }
            dparts[part_of(r.eid)].push(D { eid: r.eid, ingest: r.ingest, rowid: r.rowid, ppart });
        }
        rows.clear();
    }
    type EvOut = (Vec<Vec<D>>, Vec<Vec<Ev>>, Vec<Vec<(u32, u32, u32)>>, Vec<u8>);
    let parse_chunk = |ci: usize, c: &&[u8]| -> EvOut {
            let mut dparts: Vec<Vec<D>> = new_parts(c.len(), 70);
            let mut eparts: Vec<Vec<Ev>> = new_parts(c.len(), 70);
            let mut nparts: Vec<Vec<(u32, u32, u32)>> = (0..NPART).map(|_| Vec::new()).collect();
            let mut arena: Vec<u8> = Vec::new();
            let mut li: u32 = 0;
            let base = (ci as u32) << 20;
            let mut rows: Vec<Row> = Vec::with_capacity(BLK);
            for_rows(c, |f: &[&[u8]; 7]| {
                if f[0].is_empty() {
                    return;
                }
                let rowid = base | li;
                li += 1;
                let user = f[1];
                let ingest_ms = parse_ingest(f[3]);
                let ingest = if ingest_ms == i64::MAX {
                    i32::MAX
                } else {
                    (ingest_ms / 1000 - ws_s).clamp(i32::MIN as i64 + 1, i32::MAX as i64 - 1) as i32
                };
                let mut kind = K_INVALID;
                let mut ts = 0u32;
                let mut price = 0u32;
                let mut cat = 0u8;
                let mut ukey = 0u128;
                if !user.is_empty() && !f[5].is_empty() {
                    if let Some(t) = parse_ts(f[2]) {
                        if t >= ws && t < we && ingest_ms - t <= DAY_MS {
                            kind = match f[4] {
                                b"view" | b"cart" => K_VALID,
                                b"purchase" => match parse_price(f[6]) {
                                    Some(p) => {
                                        price = p;
                                        cat = *products.get(&key128(f[5])).unwrap_or(&unknown_cat);
                                        K_PURCHASE
                                    }
                                    None => K_INVALID,
                                },
                                _ => K_INVALID,
                            };
                            if kind != K_INVALID {
                                ts = (t - ws) as u32;
                                ukey = key128(user);
                            }
                        }
                    }
                }
                rows.push(Row { ukey, eid: key128(f[0]), user, ingest, ts, price, rowid, cat, kind });
                if rows.len() == BLK {
                    flush_rows(&mut rows, utab, counters, &mut dparts, &mut eparts, &mut nparts, &mut arena);
                }
            });
            flush_rows(&mut rows, utab, counters, &mut dparts, &mut eparts, &mut nparts, &mut arena);
            assert!(li < (1 << 20));
            // nothing references this chunk's text any more: drop its page mappings now,
            // in parallel, instead of in the single-threaded teardown at exit
            release(c);
            (dparts, eparts, nparts, arena)
        };
    // Workers take chunks in file order from a shared counter: together they read the
    // input front to back, right behind the kernel's prefetch, and stay load-balanced.
    let ev_parsed: Vec<EvOut> = ordered_map(&ev_chunks, ncpu, parse_chunk);
    lap!(format!("events parsed: {} chunks", ev_chunks.len()));

    // ---------------- events: dedup on the small records -> per person partition, rows to drop
    let losers: Vec<Vec<Vec<u32>>> = (0..NPART)
        .into_par_iter()
        .map(|p| {
            // Most event_ids are unique. A two-bit-per-slot filter (seen once / seen
            // twice) finds the rows whose id might repeat; only those are sorted.
            let n: usize = ev_parsed.iter().map(|c| c.0[p].len()).sum();
            let bits = (n * 16 + 1024).next_power_of_two();
            let shift = 64 - bits.trailing_zeros();
            let mut seen1 = vec![0u64; bits / 64];
            let mut seen2 = vec![0u64; bits / 64];
            for c in &ev_parsed {
                for d in &c.0[p] {
                    let b = (mix(d.eid) >> shift) as usize;
                    let (w, m) = (b >> 6, 1u64 << (b & 63));
                    seen2[w] |= seen1[w] & m;
                    seen1[w] |= m;
                }
            }
            let mut v: Vec<D> = Vec::with_capacity(n / 8 + 16);
            for c in &ev_parsed {
                for d in &c.0[p] {
                    let b = (mix(d.eid) >> shift) as usize;
                    if seen2[b >> 6] & (1u64 << (b & 63)) != 0 {
                        v.push(*d);
                    }
                }
            }
            v.sort_unstable_by_key(|r| (r.eid, r.ingest));
            let mut out: Vec<Vec<u32>> = (0..NPART).map(|_| Vec::new()).collect();
            let mut i = 0;
            while i < v.len() {
                let e = v[i].eid;
                i += 1;
                // every later copy of this event_id loses to the first (smallest ingest_ts)
                while i < v.len() && v[i].eid == e {
                    if v[i].ppart != NO_PART {
                        out[v[i].ppart as usize].push(v[i].rowid);
                    }
                    i += 1;
                }
            }
            out
        })
        .collect();
    lap!(format!("user table: {} persons, {} slots", counters.iter().map(|c| c.load(std::sync::atomic::Ordering::Relaxed) as usize).sum::<usize>(), utab.mask + 1));
    lap!("events deduped");

    // ---------------- clicks
    let mut cl_chunks = Vec::new();
    for m in &cl_maps {
        chunks_of(m, &mut cl_chunks);
    }
    let campaigns = &campaigns;
    let cl_parsed: Vec<Vec<Vec<Cl>>> = cl_chunks
        .par_iter()
        .map(|c| {
            let mut parts: Vec<Vec<Cl>> = new_parts(c.len(), 45);
            let mut rows: Vec<(u128, &[u8], i64, u16)> = Vec::with_capacity(c.len() / 45 + 16);
            for_rows(c, |f: &[&[u8]; 4]| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let camp = match campaigns.get(&key128(f[2])) {
                    Some(&c) => c,
                    None => return,
                };
                let ts = match parse_ts(f[3]) {
                    Some(t) if t >= cws && t < we => t,
                    _ => return,
                };
                rows.push((key128(f[1]), f[0], ts, camp));
            });
            // a click whose user never had a valid event cannot credit anything: drop it
            let cds: Vec<u32> = rows.iter().map(|r| utab.get(fp(r.0))).collect();
            for (r, &cd) in rows.iter().zip(cds.iter()) {
                if cd != u32::MAX {
                    let pp = (cd >> LID_BITS) as usize;
                    parts[pp].push(Cl { ts: r.2, cid: r.1, lid: cd & ((1 << LID_BITS) - 1), camp: r.3 });
                }
            }
            parts
        })
        .collect();
    lap!("clicks parsed");

    // ---------------- sessions per person partition
    struct PartOut<'a> {
        rows: Vec<URow<'a>>,
        camp: Vec<CAgg>,
    }
    let pmax = std::sync::atomic::AtomicU64::new(0);
    let psum = std::sync::atomic::AtomicU64::new(0);
    let results: Vec<PartOut> = (0..NPART)
        .into_par_iter()
        .map(|p| {
            let pt = Instant::now();
            let _guard = Defer(|| { let ns = pt.elapsed().as_nanos() as u64; pmax.fetch_max(ns, std::sync::atomic::Ordering::Relaxed); psum.fetch_add(ns, std::sync::atomic::Ordering::Relaxed); });
            // rows to drop (later copies of a deduped event), as an open-addressing set
            let nl: usize = losers.iter().map(|c| c[p].len()).sum();
            let cap = (nl * 2 + 16).next_power_of_two();
            let mask = cap - 1;
            let mut set = vec![u32::MAX; cap];
            for c in &losers {
                for &r in &c[p] {
                    let mut h = (r.wrapping_mul(0x9E37_79B1) >> 7) as usize & mask;
                    while set[h] != u32::MAX && set[h] != r {
                        h = (h + 1) & mask;
                    }
                    set[h] = r;
                }
            }
            let is_loser = |r: u32| -> bool {
                let mut h = (r.wrapping_mul(0x9E37_79B1) >> 7) as usize & mask;
                loop {
                    let x = set[h];
                    if x == u32::MAX {
                        return false;
                    }
                    if x == r {
                        return true;
                    }
                    h = (h + 1) & mask;
                }
            };

            // person-local ids are dense: group by counting sort, no hash tables
            let np = counters[p].load(std::sync::atomic::Ordering::Relaxed) as usize;
            let mut names: Vec<&[u8]> = link_names[p].clone();
            names.resize(np, &[][..]);
            for c in &ev_parsed {
                for &(lid, off, len) in &c.2[p] {
                    names[lid as usize] = &c.3[off as usize..(off + len) as usize];
                }
            }
            let mut estart = vec![0u32; np + 1];
            for c in &ev_parsed {
                for e in &c.1[p] {
                    if nl > 0 && is_loser(e.rowid) {
                        continue;
                    }
                    estart[e.lid as usize + 1] += 1;
                }
            }
            for l in 0..np {
                estart[l + 1] += estart[l];
            }
            let total = estart[np] as usize;
            let mut packed: Vec<u64> = vec![0; total];
            let mut pos: Vec<u32> = estart[..np].to_vec();
            for c in &ev_parsed {
                for e in &c.1[p] {
                    if nl > 0 && is_loser(e.rowid) {
                        continue;
                    }
                    let at = pos[e.lid as usize];
                    pos[e.lid as usize] = at + 1;
                    packed[at as usize] = ((e.ts as u64) << 32) | e.pc as u64;
                }
            }
            let mut cstart = vec![0u32; np + 1];
            for c in &cl_parsed {
                for x in &c[p] {
                    cstart[x.lid as usize + 1] += 1;
                }
            }
            for l in 0..np {
                cstart[l + 1] += cstart[l];
            }
            let mut carr: Vec<(i64, &[u8], u16)> = vec![(0, &[][..], 0); cstart[np] as usize];
            let mut cpos: Vec<u32> = cstart[..np].to_vec();
            for c in &cl_parsed {
                for x in &c[p] {
                    let at = cpos[x.lid as usize];
                    cpos[x.lid as usize] = at + 1;
                    carr[at as usize] = (x.ts, x.cid, x.camp);
                }
            }

            let mut rows: Vec<URow> = Vec::with_capacity(np * 2 + 16);
            let mut camp = vec![CAgg::default(); ncamp * ndays];
            let mut cnt = vec![0u32; ncamp];
            let mut active: Vec<u16> = Vec::new();
            let mut catsum = vec![0i64; ncat];
            let mut touched: Vec<u8> = Vec::new();

            let top_of = |catsum: &mut Vec<i64>, touched: &mut Vec<u8>| -> Option<u8> {
                let mut top: Option<u8> = None;
                for &c in touched.iter() {
                    let better = match top {
                        None => true,
                        Some(t) => {
                            let (sc, st) = (catsum[c as usize], catsum[t as usize]);
                            sc > st || (sc == st && cat_rank[c as usize] < cat_rank[t as usize])
                        }
                    };
                    if better {
                        top = Some(c);
                    }
                }
                for &c in touched.iter() {
                    catsum[c as usize] = 0;
                }
                touched.clear();
                top
            };

            for lid in 0..np {
                if estart[lid] == estart[lid + 1] {
                    continue; // a linked person with no surviving events
                }
                let name = names[lid];
                let pe = &mut packed[estart[lid] as usize..estart[lid + 1] as usize];
                if pe.len() > 1 {
                    pe.sort_unstable();
                }
                let pc = &mut carr[cstart[lid] as usize..cstart[lid + 1] as usize];
                if pc.len() > 1 {
                    pc.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));
                }
                let pe = &*pe;
                let pc = &*pc;

                let (mut lo, mut hi) = (0usize, 0usize);
                let mut cur_day: i64 = i64::MIN;
                let mut u_sess = 0i64;
                let mut u_ev = 0i64;
                let mut u_rev = 0i64;

                let mut k = 0usize;
                while k < pe.len() {
                    let start_rel = (pe[k] >> 32) as u32;
                    let start = ws + start_rel as i64;
                    let mut last = start_rel;
                    let mut rev = 0i64;
                    let mut nev = 0i64;
                    let mut purch = false;
                    let day = start.div_euclid(DAY_MS);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            let top = top_of(&mut catsum, &mut touched);
                            rows.push(URow { name, day: cur_day as i32, sessions: u_sess, events: u_ev, revenue: u_rev, top });
                        }
                        cur_day = day;
                        u_sess = 0;
                        u_ev = 0;
                        u_rev = 0;
                    }
                    while k < pe.len() {
                        let v = pe[k];
                        let t = (v >> 32) as u32;
                        if (t - last) as i64 > GAP_MS {
                            break;
                        }
                        last = t;
                        nev += 1;
                        let price = ((v >> 8) & 0xFF_FFFF) as i64;
                        if price > 0 {
                            let cat = (v & 0xFF) as usize;
                            purch = true;
                            rev += price;
                            if catsum[cat] == 0 {
                                touched.push(cat as u8);
                            }
                            catsum[cat] += price;
                        }
                        k += 1;
                    }
                    u_sess += 1;
                    u_ev += nev;
                    u_rev += rev;

                    // clicks window [start - 7d, start]
                    while hi < pc.len() && pc[hi].0 <= start {
                        let c = pc[hi].2 as usize;
                        if cnt[c] == 0 {
                            active.push(c as u16);
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && pc[lo].0 < start - WEEK_MS {
                        cnt[pc[lo].2 as usize] -= 1;
                        lo += 1;
                    }
                    let di = (day - day0) as usize;
                    let credit = if hi > lo {
                        let mt = pc[hi - 1].0;
                        let mut q = hi - 1;
                        while q > lo && pc[q - 1].0 == mt {
                            q -= 1;
                        }
                        pc[q].2
                    } else {
                        organic
                    };
                    let a = &mut camp[credit as usize * ndays + di];
                    a.sessions += 1;
                    a.revenue += rev;
                    if purch {
                        a.purch += 1;
                    }
                    if !active.is_empty() {
                        active.retain(|&c| {
                            if cnt[c as usize] > 0 {
                                camp[c as usize * ndays + di].assisted += 1;
                                true
                            } else {
                                false
                            }
                        });
                    }
                }
                if cur_day != i64::MIN {
                    let top = top_of(&mut catsum, &mut touched);
                    rows.push(URow { name, day: cur_day as i32, sessions: u_sess, events: u_ev, revenue: u_rev, top });
                }
                for &c in &active {
                    cnt[c as usize] = 0;
                }
                active.clear();
            }
            PartOut { rows, camp }
        })
        .collect();
    lap!(format!("sessions (slowest partition {:.1} ms, mean {:.2} ms)", pmax.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e6, psum.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e6 / NPART as f64));

    // ---------------- outputs
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();

    let nfiles = ncpu.max(1);
    let per = results.len().div_ceil(nfiles);
    let groups: Vec<&[PartOut]> = results.chunks(per.max(1)).collect();
    let cat_names = &cat_names;
    rayon::scope(|s| {
        for (gi, g) in groups.iter().enumerate() {
            let ud = &ud;
            s.spawn(move |_| {
                let rows: Vec<&URow> = g.iter().flat_map(|p| p.rows.iter()).collect();
                if rows.is_empty() && gi != 0 {
                    return;
                }
                write_user_daily(&ud.join(format!("part-{gi:03}.parquet")), &rows, cat_names);
            });
        }
        let results = &results;
        let cd = &cd;
        let camp_ids = &camp_ids;
        let camp_chan = &camp_chan;
        s.spawn(move |_| {
            let mut camp = vec![CAgg::default(); ncamp * ndays];
            for r in results.iter() {
                for (a, b) in camp.iter_mut().zip(r.camp.iter()) {
                    a.sessions += b.sessions;
                    a.purch += b.purch;
                    a.revenue += b.revenue;
                    a.assisted += b.assisted;
                }
            }
            let mut crow: Vec<(&[u8], &[u8], i32, CAgg)> = Vec::new();
            for c in 0..ncamp {
                let (id, ch): (&[u8], &[u8]) = if c == organic as usize {
                    (&b"organic"[..], &b"organic"[..])
                } else {
                    (camp_ids[c], camp_chan[c])
                };
                for d in 0..ndays {
                    let a = camp[c * ndays + d];
                    if a.sessions > 0 || a.assisted > 0 {
                        crow.push((id, ch, (day0 + d as i64) as i32, a));
                    }
                }
            }
            write_campaign_daily(&cd.join("part-000.parquet"), &crow);
        });
    });
    lap!("written");
    // Skip freeing the big structures: the process is about to exit.
    std::mem::forget(results);
    std::mem::forget(ev_parsed);
    std::mem::forget(losers);
    std::mem::forget(cl_parsed);
    std::process::exit(0);
}

// ---------------------------------------------------------------- parquet

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

macro_rules! col {
    ($rg:expr, $t:ty, $vals:expr, $def:expr) => {{
        let mut c = $rg.next_column().unwrap().unwrap();
        c.typed::<$t>().write_batch($vals, $def, None).unwrap();
        c.close().unwrap();
    }};
}

fn write_user_daily(path: &Path, rows: &[&URow], cats: &[&[u8]]) {
    let schema = Arc::new(
        parse_message_type(
            "message s { required binary user_id (UTF8); required int32 day (DATE); required int64 sessions; \
             required int64 events; required int64 revenue_cents; optional binary top_category (UTF8); }",
        )
        .unwrap(),
    );
    let f = std::io::BufWriter::with_capacity(1 << 20, File::create(path).unwrap());
    let mut w = SerializedFileWriter::new(f, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    // all names in one buffer, then zero-copy slices of it
    let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 12);
    let mut ends: Vec<usize> = Vec::with_capacity(rows.len());
    for r in rows {
        buf.extend_from_slice(r.name);
        ends.push(buf.len());
    }
    let buf = bytes::Bytes::from(buf);
    let mut st = 0;
    let ids: Vec<ByteArray> = ends
        .iter()
        .map(|&e| {
            let b = ByteArray::from(buf.slice(st..e));
            st = e;
            b
        })
        .collect();
    col!(rg, ByteArrayType, &ids, None);
    let days: Vec<i32> = rows.iter().map(|r| r.day).collect();
    col!(rg, Int32Type, &days, None);
    let v: Vec<i64> = rows.iter().map(|r| r.sessions).collect();
    col!(rg, Int64Type, &v, None);
    let v: Vec<i64> = rows.iter().map(|r| r.events).collect();
    col!(rg, Int64Type, &v, None);
    let v: Vec<i64> = rows.iter().map(|r| r.revenue).collect();
    col!(rg, Int64Type, &v, None);
    let def: Vec<i16> = rows.iter().map(|r| r.top.is_some() as i16).collect();
    let catb: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(c.to_vec())).collect();
    let tops: Vec<ByteArray> = rows.iter().filter_map(|r| r.top.map(|c| catb[c as usize].clone())).collect();
    col!(rg, ByteArrayType, &tops, Some(&def));
    rg.close().unwrap();
    w.close().unwrap();
}

fn write_campaign_daily(path: &Path, rows: &[(&[u8], &[u8], i32, CAgg)]) {
    let schema = Arc::new(
        parse_message_type(
            "message s { required binary campaign_id (UTF8); required binary channel (UTF8); required int32 day (DATE); \
             required int64 sessions; required int64 purchasing_sessions; required int64 revenue_cents; \
             required int64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let f = std::io::BufWriter::new(File::create(path).unwrap());
    let mut w = SerializedFileWriter::new(f, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.0.to_vec())).collect();
    col!(rg, ByteArrayType, &v, None);
    let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.1.to_vec())).collect();
    col!(rg, ByteArrayType, &v, None);
    let v: Vec<i32> = rows.iter().map(|r| r.2).collect();
    col!(rg, Int32Type, &v, None);
    let v: Vec<i64> = rows.iter().map(|r| r.3.sessions).collect();
    col!(rg, Int64Type, &v, None);
    let v: Vec<i64> = rows.iter().map(|r| r.3.purch).collect();
    col!(rg, Int64Type, &v, None);
    let v: Vec<i64> = rows.iter().map(|r| r.3.revenue).collect();
    col!(rg, Int64Type, &v, None);
    let v: Vec<i64> = rows.iter().map(|r| r.3.assisted).collect();
    col!(rg, Int64Type, &v, None);
    rg.close().unwrap();
    w.close().unwrap();
}
