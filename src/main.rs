// Shopping Sessions pipeline. See rules.md for the exact spec.
//
// Stages:
//   1. mmap every input file, split into newline-aligned chunks.
//   2. links -> union-find -> each ID's person name (byte-wise smallest ID).
//   3. events (parallel): parse + validate each row, partition by event_id hash.
//   4. per event_id partition: keep the copy with the smallest ingest_ts, drop
//      it if invalid, map user -> person, repartition by person hash.
//   5. clicks (parallel): parse + validate, map user -> person, partition by person.
//   6. per person partition: sort, sessionize, credit + assists, aggregate.
//   7. write Parquet.

use ahash::{AHashMap, RandomState};
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const CHUNK: usize = 4 << 20;
const NPART: usize = 128;

const K_INVALID: u8 = 0;
const K_VIEW: u8 = 1;
const K_CART: u8 = 2;
const K_PURCHASE: u8 = 3;

static HS: RandomState = RandomState::with_seeds(
    0x243f6a8885a308d3,
    0x13198a2e03707344,
    0xa4093822299f31d0,
    0x082efa98ec4e6c89,
);

#[inline]
fn h64(b: &[u8]) -> u64 {
    HS.hash_one(b)
}

#[inline]
fn part_of(h: u64) -> usize {
    // use high bits: low bits are used by hash maps
    ((h >> 32) as usize * NPART) >> 32
}

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

/// Split a line into exactly N fields, unquoting. Returns false if malformed.
#[inline]
fn split_fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let has_quote = memchr::memchr(b'"', line).is_some();
    let mut n = 0;
    let mut start = 0;
    let mut it = memchr::memchr_iter(b',', line);
    loop {
        let end = match it.next() {
            Some(i) => i,
            None => line.len(),
        };
        if n == N {
            return false;
        }
        let raw = &line[start..end];
        let v = if has_quote {
            let l = raw.len();
            if l >= 2 && raw[0] == b'"' && raw[l - 1] == b'"' {
                let inner = &raw[1..l - 1];
                if memchr::memchr(b'"', inner).is_some() {
                    return false;
                }
                inner
            } else {
                if memchr::memchr(b'"', raw).is_some() {
                    return false;
                }
                raw
            }
        } else {
            raw
        };
        out[n] = v;
        n += 1;
        if end == line.len() {
            break;
        }
        start = end + 1;
    }
    n == N
}

// ---------------------------------------------------------------- parsing values

#[inline]
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
    if v[0].is_ascii_digit() && v.iter().all(|c| c.is_ascii_digit()) {
        // F3
        let mut i = 0;
        while i < l && v[i] == b'0' {
            i += 1;
        }
        if l - i > 15 {
            return None; // far outside any window
        }
        let mut x: i64 = 0;
        for &c in &v[i..] {
            x = x * 10 + (c - b'0') as i64;
        }
        return Some(x);
    }
    if l != 20 && l != 25 {
        return None;
    }
    if v[4] != b'-' || v[7] != b'-' || v[10] != b'T' || v[13] != b':' || v[16] != b':' {
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
    if v.is_empty() || !v.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < v.len() && v[i] == b'0' {
        i += 1;
    }
    if v.len() - i > 8 {
        return None;
    }
    let mut x: u32 = 0;
    for &c in &v[i..] {
        x = x * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&x) {
        Some(x)
    } else {
        None
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct EvRaw<'a> {
    h: u64,
    ingest: i64,
    ts: i64,
    eid: &'a [u8],
    user: &'a [u8],
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Ev<'a> {
    nh: u64,
    ts: i64,
    name: &'a [u8],
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Cl<'a> {
    nh: u64,
    ts: i64,
    name: &'a [u8],
    cid: &'a [u8],
    camp: u16,
}

struct URow<'a> {
    name: &'a [u8],
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

#[derive(Clone, Copy, Default)]
struct CAgg {
    sessions: i64,
    purch: i64,
    revenue: i64,
    assisted: i64,
}

struct Persons<'a> {
    ids: AHashMap<&'a [u8], u32>,
    person: Vec<(&'a [u8], u64)>,
}

impl<'a> Persons<'a> {
    #[inline]
    fn resolve(&self, user: &'a [u8]) -> (&'a [u8], u64) {
        match self.ids.get(user) {
            Some(&i) => self.person[i as usize],
            None => (user, h64(user)),
        }
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

    macro_rules! lap {
        ($s:expr) => {
            eprintln!("[{:>8.3}s] {}", t0.elapsed().as_secs_f64(), $s)
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
    let mut cat_idx: AHashMap<&[u8], u16> = AHashMap::new();
    let mut products: AHashMap<&[u8], u16> = AHashMap::new();
    if let Some(m) = &prod_map {
        let mut ch = Vec::new();
        chunks_of(m, &mut ch);
        for c in ch {
            for_lines(c, |line| {
                let mut f = [&[][..]; 2];
                if !split_fields(line, &mut f) {
                    return;
                }
                let ci = *cat_idx.entry(f[1]).or_insert_with(|| {
                    cat_names.push(f[1]);
                    (cat_names.len() - 1) as u16
                });
                products.insert(f[0], ci);
            });
        }
    }
    let unknown_cat: u16 = match cat_idx.get(&b"unknown"[..]) {
        Some(&i) => i,
        None => {
            cat_names.push(&b"unknown"[..]);
            (cat_names.len() - 1) as u16
        }
    };
    let ncat = cat_names.len();
    let mut order: Vec<usize> = (0..ncat).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(cat_names[b]));
    let mut cat_rank = vec![0u16; ncat];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u16;
    }

    // ---------------- campaigns
    let mut camp_ids: Vec<&[u8]> = Vec::new();
    let mut camp_chan: Vec<&[u8]> = Vec::new();
    let mut campaigns: AHashMap<&[u8], u16> = AHashMap::new();
    if let Some(m) = &camp_map {
        let mut ch = Vec::new();
        chunks_of(m, &mut ch);
        for c in ch {
            for_lines(c, |line| {
                let mut f = [&[][..]; 2];
                if !split_fields(line, &mut f) || f[0].is_empty() {
                    return;
                }
                if !campaigns.contains_key(f[0]) {
                    campaigns.insert(f[0], camp_ids.len() as u16);
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
    let mut ln_chunks = Vec::new();
    for m in &ln_maps {
        chunks_of(m, &mut ln_chunks);
    }
    let pairs: Vec<Vec<(&[u8], &[u8])>> = ln_chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            for_lines(c, |line| {
                let mut f = [&[][..]; 2];
                if split_fields(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                    v.push((f[0], f[1]));
                }
            });
            v
        })
        .collect();
    let mut ids: AHashMap<&[u8], u32> = AHashMap::new();
    let mut names: Vec<&[u8]> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    for v in &pairs {
        for &(a, b) in v {
            let ia = *ids.entry(a).or_insert_with(|| {
                names.push(a);
                parent.push(parent.len() as u32);
                (names.len() - 1) as u32
            });
            let ib = *ids.entry(b).or_insert_with(|| {
                names.push(b);
                parent.push(parent.len() as u32);
                (names.len() - 1) as u32
            });
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    let n_ids = names.len();
    let mut best: Vec<u32> = (0..n_ids as u32).collect(); // per root: index of smallest name
    for i in 0..n_ids as u32 {
        let r = find(&mut parent, i);
        if names[i as usize] < names[best[r as usize] as usize] {
            best[r as usize] = i;
        }
    }
    let mut person: Vec<(&[u8], u64)> = Vec::with_capacity(n_ids);
    for i in 0..n_ids as u32 {
        let r = find(&mut parent, i);
        let nm = names[best[r as usize] as usize];
        person.push((nm, 0));
    }
    person.par_iter_mut().for_each(|p| p.1 = h64(p.0));
    let persons = Persons { ids, person };
    lap!(format!("links: {} ids", n_ids));

    // ---------------- events: parse
    let mut ev_chunks = Vec::new();
    for m in &ev_maps {
        chunks_of(m, &mut ev_chunks);
    }
    let products = &products;
    let ev_parsed: Vec<Vec<Vec<EvRaw>>> = ev_chunks
        .par_iter()
        .map(|c| {
            let mut parts: Vec<Vec<EvRaw>> = (0..NPART).map(|_| Vec::new()).collect();
            for_lines(c, |line| {
                let mut f = [&[][..]; 7];
                if !split_fields(line, &mut f) || f[0].is_empty() {
                    return;
                }
                let eid = f[0];
                let user = f[1];
                let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                let mut kind = K_INVALID;
                let mut ts = 0i64;
                let mut price = 0u32;
                let mut cat = 0u16;
                if !user.is_empty() && !f[5].is_empty() {
                    if let Some(t) = parse_ts(f[2]) {
                        if t >= ws && t < we && ingest - t <= DAY_MS {
                            ts = t;
                            kind = match f[4] {
                                b"view" => K_VIEW,
                                b"cart" => K_CART,
                                b"purchase" => match parse_price(f[6]) {
                                    Some(p) => {
                                        price = p;
                                        cat = *products.get(f[5]).unwrap_or(&unknown_cat);
                                        K_PURCHASE
                                    }
                                    None => K_INVALID,
                                },
                                _ => K_INVALID,
                            };
                        }
                    }
                }
                let h = h64(eid);
                parts[part_of(h)].push(EvRaw { h, ingest, ts, eid, user, price, cat, kind });
            });
            parts
        })
        .collect();
    lap!(format!("events parsed: {} chunks", ev_chunks.len()));

    // ---------------- events: dedup, resolve person, repartition
    let persons = &persons;
    let ev_by_person: Vec<Vec<Vec<Ev>>> = (0..NPART)
        .into_par_iter()
        .map(|p| {
            let n: usize = ev_parsed.iter().map(|c| c[p].len()).sum();
            let mut v: Vec<EvRaw> = Vec::with_capacity(n);
            for c in &ev_parsed {
                v.extend_from_slice(&c[p]);
            }
            v.sort_unstable_by(|a, b| a.h.cmp(&b.h).then_with(|| a.eid.cmp(b.eid)).then(a.ingest.cmp(&b.ingest)));
            let mut out: Vec<Vec<Ev>> = (0..NPART).map(|_| Vec::new()).collect();
            let mut i = 0;
            while i < v.len() {
                let r = v[i];
                let mut j = i + 1;
                while j < v.len() && v[j].h == r.h && v[j].eid == r.eid {
                    j += 1;
                }
                i = j;
                if r.kind == K_INVALID {
                    continue;
                }
                let (name, nh) = persons.resolve(r.user);
                out[part_of(nh)].push(Ev { nh, ts: r.ts, name, price: r.price, cat: r.cat, kind: r.kind });
            }
            out
        })
        .collect();
    drop(ev_parsed);
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
            let mut parts: Vec<Vec<Cl>> = (0..NPART).map(|_| Vec::new()).collect();
            for_lines(c, |line| {
                let mut f = [&[][..]; 4];
                if !split_fields(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let camp = match campaigns.get(f[2]) {
                    Some(&c) => c,
                    None => return,
                };
                let ts = match parse_ts(f[3]) {
                    Some(t) if t >= cws && t < we => t,
                    _ => return,
                };
                let (name, nh) = persons.resolve(f[1]);
                parts[part_of(nh)].push(Cl { nh, ts, name, cid: f[0], camp });
            });
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
            let n: usize = ev_by_person.iter().map(|c| c[p].len()).sum();
            let mut ev: Vec<Ev> = Vec::with_capacity(n);
            for c in &ev_by_person {
                ev.extend_from_slice(&c[p]);
            }
            let n: usize = cl_parsed.iter().map(|c| c[p].len()).sum();
            let mut cl: Vec<Cl> = Vec::with_capacity(n);
            for c in &cl_parsed {
                cl.extend_from_slice(&c[p]);
            }
            ev.sort_unstable_by(|a, b| a.nh.cmp(&b.nh).then_with(|| a.name.cmp(b.name)).then(a.ts.cmp(&b.ts)));
            cl.sort_unstable_by(|a, b| {
                a.nh.cmp(&b.nh).then_with(|| a.name.cmp(b.name)).then(a.ts.cmp(&b.ts)).then_with(|| a.cid.cmp(b.cid))
            });

            let mut rows: Vec<URow> = Vec::new();
            let mut camp = vec![CAgg::default(); ncamp * ndays];
            let mut cnt = vec![0u32; ncamp];
            let mut active: Vec<u16> = Vec::new();
            let mut catsum = vec![0i64; ncat];
            let mut touched: Vec<u16> = Vec::new();

            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let nh = ev[i].nh;
                let name = ev[i].name;
                let mut j = i + 1;
                while j < ev.len() && ev[j].nh == nh && ev[j].name == name {
                    j += 1;
                }
                // clicks of this person
                while ci < cl.len() && (cl[ci].nh, cl[ci].name) < (nh, name) {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cl.len() && cl[cj].nh == nh && cl[cj].name == name {
                    cj += 1;
                }
                let pc = &cl[ci..cj];
                ci = cj;
                let pe = &ev[i..j];
                i = j;

                let (mut lo, mut hi) = (0usize, 0usize);
                // user-day accumulator
                let mut cur_day: i64 = i64::MIN;
                let mut u_sess = 0i64;
                let mut u_ev = 0i64;
                let mut u_rev = 0i64;

                let mut k = 0usize;
                while k < pe.len() {
                    let start = pe[k].ts;
                    let mut last = start;
                    let mut rev = 0i64;
                    let mut nev = 0i64;
                    let mut purch = false;
                    let day = start.div_euclid(DAY_MS);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            let mut top: Option<u16> = None;
                            for &c in &touched {
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
                            for &c in &touched {
                                catsum[c as usize] = 0;
                            }
                            touched.clear();
                            rows.push(URow { name, day: cur_day as i32, sessions: u_sess, events: u_ev, revenue: u_rev, top });
                        }
                        cur_day = day;
                        u_sess = 0;
                        u_ev = 0;
                        u_rev = 0;
                    }
                    while k < pe.len() && pe[k].ts - last <= GAP_MS {
                        let e = &pe[k];
                        last = e.ts;
                        nev += 1;
                        if e.kind == K_PURCHASE {
                            purch = true;
                            rev += e.price as i64;
                            if catsum[e.cat as usize] == 0 {
                                touched.push(e.cat);
                            }
                            catsum[e.cat as usize] += e.price as i64;
                        }
                        k += 1;
                    }
                    u_sess += 1;
                    u_ev += nev;
                    u_rev += rev;

                    // clicks window [start - 7d, start]
                    while hi < pc.len() && pc[hi].ts <= start {
                        let c = pc[hi].camp as usize;
                        if cnt[c] == 0 {
                            active.push(c as u16);
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && pc[lo].ts < start - WEEK_MS {
                        let c = pc[lo].camp as usize;
                        cnt[c] -= 1;
                        lo += 1;
                    }
                    let di = (day - day0) as usize;
                    let credit = if hi > lo {
                        let mt = pc[hi - 1].ts;
                        let mut q = hi - 1;
                        while q > lo && pc[q - 1].ts == mt {
                            q -= 1;
                        }
                        pc[q].camp
                    } else {
                        organic
                    };
                    let a = &mut camp[credit as usize * ndays + di];
                    a.sessions += 1;
                    a.revenue += rev;
                    if purch {
                        a.purch += 1;
                    }
                    {
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
                    let mut top: Option<u16> = None;
                    for &c in &touched {
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
                    for &c in &touched {
                        catsum[c as usize] = 0;
                    }
                    touched.clear();
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
    groups.par_iter().enumerate().for_each(|(gi, g)| {
        let rows: Vec<&URow> = g.iter().flat_map(|p| p.rows.iter()).collect();
        if rows.is_empty() && gi != 0 {
            return;
        }
        write_user_daily(&ud.join(format!("part-{gi:03}.parquet")), &rows, &cat_names);
    });

    let mut camp = vec![CAgg::default(); ncamp * ndays];
    for r in &results {
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
    lap!("written");
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
