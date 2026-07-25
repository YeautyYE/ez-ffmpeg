//! Parity gate and ignored micro-benchmark for the Annex-B start-code scan
//! (`nal_framing::find_startcode`).
//!
//! The shipping scan is a stride-3 probe. The parity tests — which run on
//! every test pass — pin it byte-for-byte to the plain byte-by-byte
//! reference (and pin the SWAR candidate kept below for benchmarking) over
//! an exhaustive small-buffer sweep, constructed boundary fixtures and
//! seeded-random access units. The benchmark is ignored; run it in release:
//!
//! ```text
//! cargo test --release bench_nal_startcode_scan -- --ignored --nocapture
//! ```
//!
//! Payload bytes reach this scan from remote senders, so a scan variant
//! must not fall below the byte-by-byte reference on ANY corpus, not just
//! on realistic entropy. The corpora therefore include the degradation
//! faces of the value-dependent variants: zero-dense payloads (25% zeros,
//! ~50% isolated zeros, an unbroken zero run) collapse SWAR zero-byte
//! skipping below the reference, 0x01-dense payloads do the same to
//! memchr-style candidate search (memchr is not vendored here; the SWAR
//! candidate is kept so any architecture can reproduce the comparison),
//! and separator-dense streams of tiny NALs are the degradation face of
//! striding itself — every hit sits a few bytes from the scan origin, so
//! a stride buys nothing and only its prelude keeps it at the byte scan's
//! cost there.
//!
//! Measurement discipline: two row families, both sampled in interleaved
//! rotated rounds keeping per-cell minima. The harness family drives every
//! finder variant through the same injected walker copy (identical walker
//! shape and call surface) for cross-variant screening. The
//! production-grade family is the release gate: two CONCRETE walkers —
//! the parent's composition with the byte scan compiled in, and the real
//! `walk_annexb` — compared pairwise per benchmark invocation, because
//! per-instantiation codegen scatter on boundary-bound corpora is larger
//! than the effects under test.

use super::nal_framing::{
    find_startcode, push_length_prefixed, walk_annexb, AuScan, NAL_LENGTH_SIZE,
};
use std::hint::black_box;
use std::time::Instant;

/// Byte-by-byte reference scan: the previously shipped implementation,
/// byte-for-byte. The parity tests treat this as ground truth.
fn find_startcode_reference(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < 3 {
        return None;
    }
    let i = (from..data.len() - 2)
        .find(|&i| data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1)?;
    if i > from && data[i - 1] == 0 {
        Some(i - 1)
    } else {
        Some(i)
    }
}

/// SWAR candidate (benchmark reference, NOT shipped): skip 8-byte words
/// containing no zero byte via the classic `(w - 0x0101..) & !w & 0x8080..`
/// test, then verify byte-wise at zero candidates. On zero-dense payloads
/// every word contains zeros, the skip never fires, and the per-byte word
/// reload drives throughput below the byte-by-byte reference — which is
/// why the stride-3 probe ships instead. Kept here so the verdict can be
/// re-measured on any architecture.
fn find_startcode_swar(data: &[u8], from: usize) -> Option<usize> {
    let n = data.len();
    if n < 3 {
        return None;
    }
    let end = n - 2;
    let mut i = from;
    while i < end {
        if i + 8 <= n {
            let word = u64::from_le_bytes(data[i..i + 8].try_into().expect("8-byte chunk"));
            let zeros =
                word.wrapping_sub(0x0101_0101_0101_0101) & !word & 0x8080_8080_8080_8080;
            if zeros == 0 {
                // No zero byte in the word: no triple can start within it.
                i += 8;
                continue;
            }
            // Jump to the first zero byte. The little-endian interpretation
            // puts the lowest-addressed byte in the least significant bits
            // on every host, so trailing_zeros() >> 3 is its byte offset.
            i += (zeros.trailing_zeros() >> 3) as usize;
            if i >= end {
                // The zero sits in the final two bytes: no room for a
                // triple, and every byte before it in the word is non-zero.
                return None;
            }
        }
        if data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 {
            return if i > from && data[i - 1] == 0 {
                Some(i - 1)
            } else {
                Some(i)
            };
        }
        i += 1;
    }
    None
}

/// Byte-for-byte copy of `nal_framing::walk_annexb` with the start-code
/// finder injected, so walker-level parity and the benchmark drive any
/// finder through the exact walker shape. Keep in sync with the original —
/// the parity tests fail on any behavioral drift.
fn walk_annexb_with<'a, F>(
    data: &'a [u8],
    find: F,
    mut on_nal: impl FnMut(&'a [u8]),
) -> Result<AuScan, String>
where
    F: Fn(&[u8], usize) -> Option<usize>,
{
    if data.len() < 4 {
        return Err(format!("Annex-B payload too short ({} bytes)", data.len()));
    }
    let mut pos = 0;
    while pos < data.len() && data[pos] == 0 {
        pos += 1;
    }
    if pos < 2 || pos >= data.len() || data[pos] != 1 {
        return Err("payload does not begin with an Annex-B start code".to_string());
    }
    pos += 1;

    let mut scan = AuScan::default();
    loop {
        let boundary = find(data, pos).unwrap_or(data.len());
        let mut end = boundary;
        while end > pos && data[end - 1] == 0 {
            end -= 1;
        }
        if end == pos {
            return Err("empty NAL unit".to_string());
        }
        scan.note(data[pos]);
        on_nal(&data[pos..end]);
        if !data[boundary..].iter().any(|&b| b != 0) {
            break;
        }
        let mut next = boundary;
        while next < data.len() && data[next] == 0 {
            next += 1;
        }
        if next >= data.len() || data[next] != 1 {
            return Err("malformed start code between NAL units".to_string());
        }
        pos = next + 1;
        if pos >= data.len() {
            return Err("trailing start code without a NAL unit".to_string());
        }
    }
    Ok(scan)
}

/// Concrete twin of `walk_annexb` with the byte-by-byte reference scan
/// compiled in directly — the parent's production composition. Kept
/// concrete (not finder-generic) so the paired production rows compare
/// the two shipped forms on equal codegen footing: closure-generic only,
/// exactly like the real walker. Keep in sync with the original — the
/// parity tests fail on any behavioral drift.
fn walk_annexb_reference<'a>(
    data: &'a [u8],
    mut on_nal: impl FnMut(&'a [u8]),
) -> Result<AuScan, String> {
    if data.len() < 4 {
        return Err(format!("Annex-B payload too short ({} bytes)", data.len()));
    }
    let mut pos = 0;
    while pos < data.len() && data[pos] == 0 {
        pos += 1;
    }
    if pos < 2 || pos >= data.len() || data[pos] != 1 {
        return Err("payload does not begin with an Annex-B start code".to_string());
    }
    pos += 1;

    let mut scan = AuScan::default();
    loop {
        let boundary = find_startcode_reference(data, pos).unwrap_or(data.len());
        let mut end = boundary;
        while end > pos && data[end - 1] == 0 {
            end -= 1;
        }
        if end == pos {
            return Err("empty NAL unit".to_string());
        }
        scan.note(data[pos]);
        on_nal(&data[pos..end]);
        if !data[boundary..].iter().any(|&b| b != 0) {
            break;
        }
        let mut next = boundary;
        while next < data.len() && data[next] == 0 {
            next += 1;
        }
        if next >= data.len() || data[next] != 1 {
            return Err("malformed start code between NAL units".to_string());
        }
        pos = next + 1;
        if pos >= data.len() {
            return Err("trailing start code without a NAL unit".to_string());
        }
    }
    Ok(scan)
}

/// Deterministic xorshift32 so every generated corpus is reproducible.
struct Xorshift(u32);

impl Xorshift {
    fn next(&mut self) -> u8 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.0 = x;
        (x >> 24) as u8
    }
}

/// NAL payload: xorshift entropy with `boost` injected at ~25% density
/// (0xFF = no boost), a fixed header byte, then sanitized so no raw
/// `00 00 0x` (x <= 3) run survives — emulation prevention makes those
/// impossible inside real NAL payloads — and a non-zero final byte (real
/// NAL units end on the RBSP stop bit).
fn gen_nal(header: u8, len: usize, rng: &mut Xorshift, boost: u8) -> Vec<u8> {
    let mut v = vec![0u8; len];
    for b in v.iter_mut() {
        let x = rng.next();
        *b = if boost != 0xFF && x < 64 { boost } else { x };
    }
    v[0] = header;
    for i in 0..len.saturating_sub(2) {
        if v[i] == 0 && v[i + 1] == 0 && v[i + 2] <= 3 {
            v[i + 2] = 0x55;
        }
    }
    let last = v.len() - 1;
    if v[last] == 0 {
        v[last] = 0x80;
    }
    v
}

/// ~50% isolated zeros: even offsets zero, odd offsets non-zero. No two
/// consecutive zeros exist, so no start-code triple can form — yet every
/// 8-byte word contains zeros, which is the SWAR worst case (the word skip
/// never fires) and a dense-fallback case for the stride-3 probe.
fn gen_nal_iso0(header: u8, len: usize, rng: &mut Xorshift) -> Vec<u8> {
    let mut v = vec![0u8; len];
    for (i, b) in v.iter_mut().enumerate() {
        if i % 2 == 1 {
            let x = rng.next();
            *b = if x == 0 { 0x55 } else { x };
        }
    }
    v[0] = header;
    let last = v.len() - 1;
    if v[last] == 0 {
        v[last] = 0x80;
    }
    v
}

/// All-zero payload run: header, zeros, non-zero stop byte. The scan walks
/// the full run without finding a triple; nothing is skippable for any
/// variant, making this the deepest zero-dense degradation face.
fn gen_nal_zerorun(header: u8, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    v[0] = header;
    v[len - 1] = 0x80;
    v
}

/// AU assembly: 4-byte start code before the first NAL, 3-byte separators
/// after (x264 emission shape).
fn make_au(nals: &[Vec<u8>]) -> Vec<u8> {
    let mut au = Vec::new();
    for (i, nal) in nals.iter().enumerate() {
        if i == 0 {
            au.extend_from_slice(&[0, 0, 0, 1]);
        } else {
            au.extend_from_slice(&[0, 0, 1]);
        }
        au.extend_from_slice(nal);
    }
    au
}

/// Separator-dense AU: `count` copies of one tiny NAL (0x41 header padded
/// with 0x80), so the scan spends nearly all its time at start-code
/// boundaries and a stride gets no reduction over the byte scan between
/// hits — the degradation face of stride variants (reviewer-supplied
/// family: thousands of one/two-byte NALs).
fn sepdense_au(nal_len: usize, count: usize) -> Vec<u8> {
    let mut nal = vec![0x80u8; nal_len];
    nal[0] = 0x41;
    make_au(&vec![nal; count])
}

/// Small AUs of 8-512 total bytes: per-walk overhead dominates on these,
/// checking a scan variant adds no fixed per-call cost.
fn small_au_batch(rng: &mut Xorshift) -> Vec<Vec<u8>> {
    let mut aus = Vec::new();
    for sz in [8usize, 16, 32, 64, 128, 256, 512] {
        aus.push(make_au(&[gen_nal(0x41, sz - 4, rng, 0xFF)]));
        if sz >= 32 {
            let half = (sz - 7) / 2;
            aus.push(make_au(&[
                gen_nal(0x06, half, rng, 0xFF),
                gen_nal(0x41, sz - 7 - half, rng, 0xFF),
            ]));
        }
    }
    aus
}

/// Exhaustive sweep: every buffer over the {0x00, 0x01, 0xAA} alphabet up
/// to length 10 (all values above 1 are equivalent to every scan variant),
/// with every scan origin in 0..=len+2 — including origins at and past the
/// end of the buffer. Because the shipping scan resolves its first eight
/// positions in a byte-wise prelude, short buffers alone never reach the
/// striding body: a second sweep prepends eight non-matching bytes so the
/// stride entry faces every alphabet pattern too, again over every origin
/// (origins inside the prefix shift the prelude/stride boundary through
/// the pattern).
#[test]
fn scan_variants_match_reference_exhaustively() {
    let alphabet = [0u8, 1, 0xAA];
    let mut buf = Vec::new();
    for prefix in [0usize, 8] {
        let max_len = if prefix == 0 { 10 } else { 8 };
        for len in 0usize..=max_len {
            let combos = 3usize.pow(len as u32);
            for combo in 0..combos {
                buf.clear();
                buf.resize(prefix, 0xAA);
                let mut c = combo;
                for _ in 0..len {
                    buf.push(alphabet[c % 3]);
                    c /= 3;
                }
                for from in 0..=buf.len() + 2 {
                    let want = find_startcode_reference(&buf, from);
                    assert_eq!(
                        find_startcode(&buf, from),
                        want,
                        "stride3 vs reference on {buf:02X?} from={from}"
                    );
                    assert_eq!(
                        find_startcode_swar(&buf, from),
                        want,
                        "swar vs reference on {buf:02X?} from={from}"
                    );
                }
            }
        }
    }
}

/// Constructed boundary fixtures: start codes at every offset (crossing
/// every 8-byte word phase), 3- vs 4-byte codes, zero tails, all-zero
/// buffers, isolated-zero and 0x01-dense fields — each swept over every
/// scan origin. A few absolute positions anchor the reference itself.
#[test]
fn scan_variants_match_reference_on_constructed_boundaries() {
    let mut fixtures: Vec<Vec<u8>> = Vec::new();
    // A start code at every offset within a 0xAA field, alone and followed
    // by a second code.
    for offset in 0..24 {
        for code in [&[0u8, 0, 1][..], &[0u8, 0, 0, 1][..]] {
            let mut v = vec![0xAAu8; 40];
            v[offset..offset + code.len()].copy_from_slice(code);
            fixtures.push(v.clone());
            let second = offset + code.len() + 5;
            if second + 3 <= v.len() {
                v[second..second + 3].copy_from_slice(&[0, 0, 1]);
                fixtures.push(v);
            }
        }
    }
    // Zero tails and codes hugging the end of the buffer.
    for tail in [
        &[0u8][..],
        &[0u8, 0][..],
        &[0u8, 0, 0][..],
        &[0u8, 1][..],
        &[0u8, 0, 1][..],
        &[0u8, 0, 0, 1][..],
    ] {
        let mut v = vec![0xAAu8; 21 - tail.len()];
        v.extend_from_slice(tail);
        fixtures.push(v);
    }
    // All-zero buffers: no triple exists at any origin.
    for len in 0..=48 {
        fixtures.push(vec![0u8; len]);
    }
    // Isolated zeros, all-ones, and 00 01 repeats.
    fixtures.push((0..64u8).map(|i| if i % 2 == 0 { 0 } else { 0xAB }).collect());
    fixtures.push(vec![1u8; 64]);
    fixtures.push([0u8, 1].repeat(32));
    // Triples straddling the first 8-byte word after a leading zero run.
    for zeros in 5..=9 {
        let mut v = vec![0u8; zeros];
        v.extend_from_slice(&[0, 0, 1, 0x41, 0x9A]);
        v.resize(24, 0xAA);
        fixtures.push(v);
    }

    for data in &fixtures {
        for from in 0..=data.len() + 2 {
            let want = find_startcode_reference(data, from);
            assert_eq!(
                find_startcode(data, from),
                want,
                "stride3 vs reference on {data:02X?} from={from}"
            );
            assert_eq!(
                find_startcode_swar(data, from),
                want,
                "swar vs reference on {data:02X?} from={from}"
            );
        }
    }

    // Anchor the reference with absolute expectations.
    assert_eq!(find_startcode_reference(&[0, 0, 1, 9], 0), Some(0));
    assert_eq!(find_startcode_reference(&[9, 0, 0, 0, 1], 0), Some(1));
    assert_eq!(find_startcode_reference(&[0, 0, 0, 1], 0), Some(0));
    assert_eq!(find_startcode_reference(&[0, 0, 0, 1], 1), Some(1));
    assert_eq!(find_startcode_reference(&[0, 0, 1], 1), None);
    assert_eq!(find_startcode_reference(&[0xAA, 0, 0, 1, 0, 0, 1], 0), Some(1));
}

/// Walker-level parity on seeded-random access units across density
/// families (realistic, zero-boosted, 0x01-boosted, isolated-zero, zero-run,
/// small, malformed): the shipping walker must produce the same result,
/// scan counters, NAL positions and normalized bytes as the injected
/// byte-by-byte reference; scan-origin sweeps run on a spread of the AUs.
#[test]
fn walker_parity_on_seeded_random_aus() {
    let mut rng = Xorshift(0xDEAD_BEEF);
    let mut aus: Vec<Vec<u8>> = Vec::new();
    for _ in 0..60 {
        let n1 = gen_nal(0x41, 17 + (rng.next() as usize * 7) % 2000, &mut rng, 0xFF);
        let n2 = gen_nal(0x06, 2 + (rng.next() as usize) % 60, &mut rng, 0xFF);
        let n3 = gen_nal(0x65, 100 + (rng.next() as usize * 31) % 4000, &mut rng, 0x00);
        let n4 = gen_nal(0x41, 64 + (rng.next() as usize) % 512, &mut rng, 0x01);
        aus.push(make_au(&[n2.clone(), n3.clone()]));
        aus.push(make_au(&[n1, n2, n3, n4]));
    }
    for len in [9usize, 17, 64, 509, 2048] {
        aus.push(make_au(&[gen_nal_iso0(0x41, len, &mut rng)]));
        aus.push(make_au(&[gen_nal_zerorun(0x41, len)]));
    }
    aus.extend(small_au_batch(&mut rng));
    // Separator-dense: hundreds of one/two-byte NALs per AU.
    aus.push(sepdense_au(1, 512));
    aus.push(sepdense_au(2, 512));
    // Malformed shapes must fail identically through both walkers.
    aus.push(vec![0x12, 0, 0, 1, 0x67]);
    aus.push(vec![0, 0, 1, 0, 0, 1, 0x41, 0x9A]);
    aus.push(vec![0, 0, 0, 1, 0, 0]);
    aus.push(vec![0, 0, 1]);
    aus.push(vec![0, 0, 0, 1, 0x65, 0xAA, 0, 0, 1]);

    let mut checked = 0usize;
    for au in &aus {
        let base = au.as_ptr() as usize;
        let mut got_spans = Vec::new();
        let mut want_spans = Vec::new();
        let mut got_out = Vec::new();
        let mut want_out = Vec::new();
        let got = walk_annexb(au, |nal| {
            got_spans.push((nal.as_ptr() as usize - base, nal.len()));
            push_length_prefixed(nal, &mut got_out);
        });
        let want = walk_annexb_with(au, find_startcode_reference, |nal| {
            want_spans.push((nal.as_ptr() as usize - base, nal.len()));
            push_length_prefixed(nal, &mut want_out);
        });
        assert_eq!(got, want, "walk result on {} bytes", au.len());
        assert_eq!(got_spans, want_spans, "NAL spans on {} bytes", au.len());
        assert_eq!(got_out, want_out, "normalized bytes on {} bytes", au.len());
        // The concrete reference walker (production-paired benchmark row)
        // must agree byte-for-byte as well.
        let mut prod_spans = Vec::new();
        let mut prod_out = Vec::new();
        let prod = walk_annexb_reference(au, |nal| {
            prod_spans.push((nal.as_ptr() as usize - base, nal.len()));
            push_length_prefixed(nal, &mut prod_out);
        });
        assert_eq!(prod, want, "reference walker result on {} bytes", au.len());
        assert_eq!(prod_spans, want_spans, "reference walker spans");
        assert_eq!(prod_out, want_out, "reference walker bytes");
        checked += 1;
    }
    for au in aus.iter().step_by(4) {
        for from in 0..au.len().min(64) {
            let want = find_startcode_reference(au, from);
            assert_eq!(find_startcode(au, from), want);
            assert_eq!(find_startcode_swar(au, from), want);
        }
    }
    assert!(checked > 130, "corpus unexpectedly small: {checked}");
}

/// Time-based sampling: run `f` for ~60 ms per sample after a ~20 ms
/// warmup, five samples, median ns per call.
fn sample_median<F: FnMut()>(mut f: F) -> f64 {
    let target = std::time::Duration::from_millis(60);
    let warmup = Instant::now();
    while warmup.elapsed() < std::time::Duration::from_millis(20) {
        f();
    }
    let mut samples: Vec<f64> = (0..5)
        .map(|_| {
            let start = Instant::now();
            let mut n = 0u64;
            loop {
                f();
                n += 1;
                if n.is_multiple_of(16) && start.elapsed() >= target {
                    break;
                }
            }
            start.elapsed().as_nanos() as f64 / n as f64
        })
        .collect();
    samples.sort_by(|a, b| a.partial_cmp(b).expect("finite sample"));
    samples[samples.len() / 2]
}

/// (census ns, census+rewrite ns) per pass over `aus` through the injected
/// finder, mirroring the two-walk Annex-B shape of `normalize_au`. All
/// benchmark comparison rows go through THIS one function so every variant
/// shares the same walker shape, call surface and inline opportunity —
/// each finder in the form it ships (or shipped) in.
fn time_pair<F>(aus: &[Vec<u8>], find: F) -> (f64, f64)
where
    F: Fn(&[u8], usize) -> Option<usize> + Copy,
{
    let census = sample_median(|| {
        for au in aus {
            let mut exact = 0usize;
            let scan = walk_annexb_with(black_box(au.as_slice()), find, |nal| {
                exact += NAL_LENGTH_SIZE + nal.len();
            })
            .expect("benchmark AU is valid");
            black_box((scan, exact));
        }
    });
    let mut scratch: Vec<u8> = Vec::new();
    let normalize = sample_median(|| {
        for au in aus {
            scratch.clear();
            let mut exact = 0usize;
            walk_annexb_with(black_box(au.as_slice()), find, |nal| {
                exact += NAL_LENGTH_SIZE + nal.len();
            })
            .expect("benchmark AU is valid");
            scratch.reserve(exact);
            let scan = walk_annexb_with(black_box(au.as_slice()), find, |nal| {
                push_length_prefixed(nal, &mut scratch)
            })
            .expect("benchmark AU is valid");
            black_box((scan, scratch.len()));
        }
    });
    (census, normalize)
}

/// Same two timings through the concrete reference walker — the parent's
/// production composition. One half of the paired production-grade
/// comparison.
fn time_pair_production_ref(aus: &[Vec<u8>]) -> (f64, f64) {
    let census = sample_median(|| {
        for au in aus {
            let mut exact = 0usize;
            let scan = walk_annexb_reference(black_box(au.as_slice()), |nal| {
                exact += NAL_LENGTH_SIZE + nal.len();
            })
            .expect("benchmark AU is valid");
            black_box((scan, exact));
        }
    });
    let mut scratch: Vec<u8> = Vec::new();
    let normalize = sample_median(|| {
        for au in aus {
            scratch.clear();
            let mut exact = 0usize;
            walk_annexb_reference(black_box(au.as_slice()), |nal| {
                exact += NAL_LENGTH_SIZE + nal.len();
            })
            .expect("benchmark AU is valid");
            scratch.reserve(exact);
            let scan = walk_annexb_reference(black_box(au.as_slice()), |nal| {
                push_length_prefixed(nal, &mut scratch)
            })
            .expect("benchmark AU is valid");
            black_box((scan, scratch.len()));
        }
    });
    (census, normalize)
}

/// Same two timings through the real `walk_annexb` — the exact shipping
/// composition, the other half of the paired production-grade comparison.
fn time_pair_production(aus: &[Vec<u8>]) -> (f64, f64) {
    let census = sample_median(|| {
        for au in aus {
            let mut exact = 0usize;
            let scan = walk_annexb(black_box(au.as_slice()), |nal| {
                exact += NAL_LENGTH_SIZE + nal.len();
            })
            .expect("benchmark AU is valid");
            black_box((scan, exact));
        }
    });
    let mut scratch: Vec<u8> = Vec::new();
    let normalize = sample_median(|| {
        for au in aus {
            scratch.clear();
            let mut exact = 0usize;
            walk_annexb(black_box(au.as_slice()), |nal| {
                exact += NAL_LENGTH_SIZE + nal.len();
            })
            .expect("benchmark AU is valid");
            scratch.reserve(exact);
            let scan = walk_annexb(black_box(au.as_slice()), |nal| {
                push_length_prefixed(nal, &mut scratch)
            })
            .expect("benchmark AU is valid");
            black_box((scan, scratch.len()));
        }
    });
    (census, normalize)
}

#[test]
#[ignore = "micro-benchmark; run in release with --ignored --nocapture"]
fn bench_nal_startcode_scan() {
    let mut rng = Xorshift(0xDEAD_BEEF);
    let corpora: Vec<(&str, Vec<Vec<u8>>)> = vec![
        (
            "realistic_3k",
            vec![make_au(&[
                gen_nal(0x06, 32, &mut rng, 0xFF),
                gen_nal(0x41, 3 * 1024, &mut rng, 0xFF),
            ])],
        ),
        (
            "realistic_16k",
            vec![make_au(&[gen_nal(0x41, 16 * 1024, &mut rng, 0xFF)])],
        ),
        (
            "realistic_96k",
            vec![make_au(&[
                gen_nal(0x06, 64, &mut rng, 0xFF),
                gen_nal(0x65, 96 * 1024, &mut rng, 0xFF),
            ])],
        ),
        (
            "realistic_1m",
            vec![make_au(&[gen_nal(0x65, 1024 * 1024, &mut rng, 0xFF)])],
        ),
        (
            "zeros25_96k",
            vec![make_au(&[gen_nal(0x65, 96 * 1024, &mut rng, 0x00)])],
        ),
        (
            "ones25_96k",
            vec![make_au(&[gen_nal(0x65, 96 * 1024, &mut rng, 0x01)])],
        ),
        (
            "iso0_50_96k",
            vec![make_au(&[gen_nal_iso0(0x65, 96 * 1024, &mut rng)])],
        ),
        (
            "zerorun_96k",
            vec![make_au(&[gen_nal_zerorun(0x65, 96 * 1024)])],
        ),
        ("sepdense_1b_4096", vec![sepdense_au(1, 4096)]),
        ("sepdense_2b_4096", vec![sepdense_au(2, 4096)]),
        ("smallau_batch", small_au_batch(&mut rng)),
    ];

    println!("corpus,bytes,variant,census_ns,census_gbps,normalize_ns,normalize_gbps");
    for (name, aus) in &corpora {
        let bytes: usize = aus.iter().map(|au| au.len()).sum();
        // Symmetric comparison rows: every variant runs through the same
        // injected walker copy (identical walker shape, call surface and
        // inline context), sampled in interleaved rounds with the per-cell
        // minimum kept and the within-round order rotated every round, so
        // neither implementation shape nor measurement order privileges
        // any variant.
        type Timer = fn(&[Vec<u8>]) -> (f64, f64);
        let timers: [Timer; 3] = [
            |aus| time_pair(aus, find_startcode_reference),
            |aus| time_pair(aus, find_startcode),
            |aus| time_pair(aus, find_startcode_swar),
        ];
        let mut best = [(f64::INFINITY, f64::INFINITY); 3];
        for round in 0..3 {
            for offset in 0..timers.len() {
                let variant = (round + offset) % timers.len();
                let sample = timers[variant](aus);
                let cell = &mut best[variant];
                cell.0 = cell.0.min(sample.0);
                cell.1 = cell.1.min(sample.1);
            }
        }
        let labels = ["reference_byte", "stride3_shipping", "swar_rejected"];
        for (variant, (census, normalize)) in labels.iter().zip(best) {
            let census_gbps = bytes as f64 / census;
            let normalize_gbps = bytes as f64 / normalize;
            println!(
                "{name},{bytes},{variant},{census:.0},{census_gbps:.2},{normalize:.0},{normalize_gbps:.2}"
            );
        }
        // Paired production-grade rows — the release gate. Both walkers
        // are concrete (closure-generic only, like the shipped code): the
        // parent's composition with the byte scan compiled in, and the
        // real `walk_annexb`. Same interleaved rotation, per-cell minima;
        // compare these two rows PAIRED per invocation of this benchmark.
        let prod_timers: [Timer; 2] = [
            |aus| time_pair_production_ref(aus),
            |aus| time_pair_production(aus),
        ];
        let mut prod_best = [(f64::INFINITY, f64::INFINITY); 2];
        for round in 0..3 {
            for offset in 0..prod_timers.len() {
                let variant = (round + offset) % prod_timers.len();
                let sample = prod_timers[variant](aus);
                let cell = &mut prod_best[variant];
                cell.0 = cell.0.min(sample.0);
                cell.1 = cell.1.min(sample.1);
            }
        }
        let prod_labels = ["reference_production", "stride3_production"];
        for (variant, (census, normalize)) in prod_labels.iter().zip(prod_best) {
            let census_gbps = bytes as f64 / census;
            let normalize_gbps = bytes as f64 / normalize;
            println!(
                "{name},{bytes},{variant},{census:.0},{census_gbps:.2},{normalize:.0},{normalize_gbps:.2}"
            );
        }
    }
}
