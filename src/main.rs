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

#[inline]
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

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    unsafe { Mmap::map(&f).ok() }
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
    let mut line_start = 0usize;
    let mut q_total = 0u32; // quotes before the current word
    let mut q_line = 0u32; // value of the running count at line start

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

    let mut i = 0usize;
    while i + 8 <= len {
        let w = ld8(chunk, i);
        let qm = zero_bytes(w ^ QUOTES);
        let mut m = zero_bytes(w ^ COMMAS) | zero_bytes(w ^ NLS);
        while m != 0 {
            let bit = m.trailing_zeros();
            let pos = i + (bit >> 3) as usize;
            if chunk[pos] == b',' {
                if n < N {
                    fields[n] = &chunk[field_start..pos];
                }
                n += 1;
                field_start = pos + 1;
            } else {
                let q_here = q_total + (qm & ((1u64 << bit) - 1)).count_ones();
                if n + 1 == N {
                    fields[n] = &chunk[field_start..pos];
                    if finish(&mut fields, q_here - q_line) {
                        f(&fields);
                    }
                }
                let _ = line_start;
                n = 0;
                field_start = pos + 1;
                line_start = pos + 1;
                q_line = q_here;
            }
            m &= m - 1;
        }
        q_total += qm.count_ones();
        i += 8;
    }
    while i < len {
        let c = chunk[i];
        if c == b',' {
            if n < N {
                fields[n] = &chunk[field_start..i];
            }
            n += 1;
            field_start = i + 1;
        } else if c == b'\n' {
            if n + 1 == N {
                fields[n] = &chunk[field_start..i];
                if finish(&mut fields, q_total - q_line) {
                    f(&fields);
                }
            }
            n = 0;
            field_start = i + 1;
            line_start = i + 1;
            q_line = q_total;
        } else if c == b'"' {
            q_total += 1;
        }
        i += 1;
    }
    if line_start < len && n + 1 == N {
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

/// F1 / F2 / F3 timestamp to epoch ms; None if bad. (Window checked by caller.)
#[inline]
fn parse_ts(v: &[u8]) -> Option<i64> {
    let l = v.len();
    if l == 0 {
        return None;
    }
    if l != 20 && l != 25 || v[4] != b'-' {
        // F3: digits only
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
        return Some(x);
    }
    if v[7] != b'-' || v[10] != b'T' || v[13] != b':' || v[16] != b':' {
        return None;
    }
    let y = (d2(v, 0)? * 100 + d2(v, 2)?) as i64;
    let mo = d2(v, 5)?;
    let d = d2(v, 8)?;
    let h = d2(v, 11)?;
    let mi = d2(v, 14)?;
    let s = d2(v, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d == 0 {
        return None;
    }
    let dim = match mo {
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let mut secs = days_from_civil(y, mo, d) * 86400 + (h * 3600 + mi * 60 + s) as i64;
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
    Some(secs * 1000)
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
struct Ev<'a> {
    pk: u128,
    name: &'a [u8],
    ts: u32,
    price: u32,
    rowid: u32,
    cat: u8,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Cl<'a> {
    pk: u128,
    cid: &'a [u8],
    ts: i64,
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

    macro_rules! lap {
        ($s:expr) => {
            if verbose {
                eprintln!("[{:>8.3}s] {}", t0.elapsed().as_secs_f64(), $s)
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
    let person: Vec<(u128, &[u8])> = (0..n_ids)
        .into_par_iter()
        .map(|g| {
            let (nm, k) = best[root[g] as usize];
            (k, nm)
        })
        .collect();
    let persons = Persons { shards: shards.into_iter().map(|s| s.tab).collect(), offs, person };
    lap!(format!("links: {} ids", n_ids));

    // ---------------- events: parse (valid rows go straight to their person partition)
    let mut ev_chunks = Vec::new();
    for m in &ev_maps {
        chunks_of(m, &mut ev_chunks);
    }
    assert!(ev_chunks.len() < 4096);
    let products = &products;
    let persons = &persons;
    let ev_parsed: Vec<(Vec<Vec<D>>, Vec<Vec<Ev>>)> = ev_chunks
        .par_iter()
        .enumerate()
        .map(|(ci, c)| {
            let mut dparts: Vec<Vec<D>> = new_parts(c.len(), 70);
            let mut eparts: Vec<Vec<Ev>> = new_parts(c.len(), 70);
            let mut li: u32 = 0;
            let base = (ci as u32) << 20;
            let mut rows: Vec<Row> = Vec::with_capacity(c.len() / 70 + 16);
            for_rows(c, |f: &[&[u8]; 7]| {
                if f[0].is_empty() {
                    return;
                }
                let rowid = base | li;
                li += 1;
                let user = f[1];
                let ingest_ms = parse_ts(f[3]).unwrap_or(i64::MAX);
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
            });
            // batched link lookups: a tight loop lets the CPU overlap the cache misses
            let idx: Vec<u32> = rows.iter().map(|r| if r.kind != K_INVALID { persons.lookup(r.ukey) } else { u32::MAX }).collect();
            for (r, &ix) in rows.iter().zip(idx.iter()) {
                let mut ppart = NO_PART;
                if r.kind != K_INVALID {
                    let (pk, name) = persons.of(r.ukey, r.user, ix);
                    let pp = part_of(pk);
                    ppart = pp as u16;
                    eparts[pp].push(Ev { pk, name, ts: r.ts, price: r.price, rowid: r.rowid, cat: r.cat, kind: r.kind });
                }
                dparts[part_of(r.eid)].push(D { eid: r.eid, ingest: r.ingest, rowid: r.rowid, ppart });
            }
            assert!(li < (1 << 20));
            (dparts, eparts)
        })
        .collect();
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
            let idx: Vec<u32> = rows.iter().map(|r| persons.lookup(r.0)).collect();
            for (r, &ix) in rows.iter().zip(idx.iter()) {
                let pk = if ix == u32::MAX { r.0 } else { persons.person[ix as usize].0 };
                parts[part_of(pk)].push(Cl { pk, cid: r.1, ts: r.2, camp: r.3 });
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
    let results: Vec<PartOut> = (0..NPART)
        .into_par_iter()
        .map(|p| {
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

            // pass 1: person-local ids and counts
            let n: usize = ev_parsed.iter().map(|c| c.1[p].len()).sum();
            let mut tab = PTable::new(n / 16 + 64);
            let mut lids: Vec<u32> = Vec::with_capacity(n);
            let mut counts: Vec<u32> = Vec::with_capacity(n / 16 + 64);
            let mut pnames: Vec<&[u8]> = Vec::with_capacity(n / 16 + 64);
            for c in &ev_parsed {
                for e in &c.1[p] {
                    if nl > 0 && is_loser(e.rowid) {
                        lids.push(u32::MAX);
                        continue;
                    }
                    let lid = tab.get_or_insert(e.pk, || {
                        counts.push(0);
                        pnames.push(e.name);
                        (counts.len() - 1) as u32
                    });
                    counts[lid as usize] += 1;
                    lids.push(lid);
                }
            }
            let np = counts.len();
            let mut estart = vec![0u32; np + 1];
            for l in 0..np {
                estart[l + 1] = estart[l] + counts[l];
            }
            // pass 2: scatter packed events (ts << 32 | price << 8 | cat) by person
            let total = estart[np] as usize;
            let mut packed: Vec<u64> = vec![0; total];
            let mut pos: Vec<u32> = estart[..np].to_vec();
            let mut li = 0usize;
            for c in &ev_parsed {
                for e in &c.1[p] {
                    let lid = lids[li];
                    li += 1;
                    if lid == u32::MAX {
                        continue;
                    }
                    let at = pos[lid as usize];
                    pos[lid as usize] = at + 1;
                    packed[at as usize] = ((e.ts as u64) << 32) | ((e.price as u64) << 8) | e.cat as u64;
                }
            }
            // clicks: same grouping; clicks of people with no events are dropped
            let mut ccount = vec![0u32; np + 1];
            let ncl: usize = cl_parsed.iter().map(|c| c[p].len()).sum();
            let mut clids: Vec<u32> = Vec::with_capacity(ncl);
            for c in &cl_parsed {
                for x in &c[p] {
                    let lid = tab.get(x.pk);
                    clids.push(lid);
                    if lid != u32::MAX {
                        ccount[lid as usize] += 1;
                    }
                }
            }
            let mut cstart = vec![0u32; np + 1];
            for l in 0..np {
                cstart[l + 1] = cstart[l] + ccount[l];
            }
            let mut carr: Vec<(i64, &[u8], u16)> = vec![(0, &[][..], 0); cstart[np] as usize];
            let mut cpos: Vec<u32> = cstart[..np].to_vec();
            let mut li = 0usize;
            for c in &cl_parsed {
                for x in &c[p] {
                    let lid = clids[li];
                    li += 1;
                    if lid == u32::MAX {
                        continue;
                    }
                    let at = cpos[lid as usize];
                    cpos[lid as usize] = at + 1;
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
                let name = pnames[lid];
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
    lap!("sessions");

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
    let ids: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.name.to_vec())).collect();
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
    let tops: Vec<ByteArray> = rows.iter().filter_map(|r| r.top.map(|c| ByteArray::from(cats[c as usize].to_vec()))).collect();
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
