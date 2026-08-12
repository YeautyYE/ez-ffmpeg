#!/usr/bin/env python3
"""aarch64 second-stage parity gate for the NAL start-code scan (C2).

Parses the `bench_nal_startcode_scan` micro-benchmark output (same-round
paired rows: `reference_production` = parent byte scan, `stride3_production`
= shipped stride scan, both concrete walkers with their finder inlined) and
decides whether the shipped scan holds architectural parity with the byte
scan on this runner.

Gate design (deliberately lenient — CI runners are shared and unpinned, so
the exact x86 pointwise bar of 19/19 pairs >= 0.995 would be flaky):

* ratio = reference-byte-scan time / shipped-stride-scan time, so >= 1.0
  means the shipped scan is at least as fast as the byte scan;
* pairs are formed WITHIN a benchmark invocation at equal round (the log
  may concatenate several invocations, each opened by a `#` header line;
  round indices repeat per invocation, so blocks are kept separate);
* per corpus, pool the same-round paired ratios across all invocations and
  take the MEDIAN on each walk (a pooled median over many rounds rejects
  transient-neighbour outliers that a single unpinned run would show,
  which matters most for the near-parity tiny-NAL corpora);
* require every corpus median (census and normalize) to clear FLOOR.

FLOOR = 0.85 is a net-regression tripwire, not a reproduction of the x86
win. The x86 per-corpus medians run 1.01-3.3x; the separator-dense
one/two-byte-NAL corpora sit inherently near parity (a stride cannot help
when the NAL is a byte or two) and hover around 1.0 even under load, so a
per-corpus floor must sit below 1.0 or it flakes on those. 0.85 means "no
corpus is more than 15% slower than the byte scan": comfortably clear of
the near-parity corpora (~0.96+ even on a loaded runner) yet a genuine
signal when breached — the stride scan is a material per-corpus loss on
this architecture, so the aarch64 byte-scan fallback
(`#[cfg(target_arch = "aarch64")]` over the parent byte finder) should be
gated in before release tagging. Running the benchmark more than once (the
workflow does) pools rounds so the median is stable well inside that
margin.

Exit 0 = pass, 1 = fail (below floor) or unparseable (fail-closed).
"""
import statistics
import sys

FLOOR = 0.85


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: nal_scan_parity_gate.py <bench-output.log>")
        return 1

    # Pool same-round pairs across invocations. A `#` header opens each
    # invocation; within one, (corpus, round) identifies a pair. Reset the
    # in-block table at each header so repeating round indices from a later
    # invocation do not overwrite an earlier one — the resulting ratios all
    # accumulate into per_corpus.
    per_corpus: dict[str, list[tuple[float, float]]] = {}
    block: dict[tuple[str, str], dict[str, tuple[int, int]]] = {}

    def flush(pending: dict[tuple[str, str], dict[str, tuple[int, int]]]) -> None:
        for (corpus, _rnd), d in pending.items():
            if "reference_production" in d and "stride3_production" in d:
                ref, ship = d["reference_production"], d["stride3_production"]
                if ship[0] > 0 and ship[1] > 0:
                    per_corpus.setdefault(corpus, []).append(
                        (ref[0] / ship[0], ref[1] / ship[1])
                    )

    try:
        with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith("#"):
                    flush(block)
                    block = {}
                    continue
                parts = line.rstrip("\n").split(",")
                if len(parts) != 8:
                    continue
                corpus, _bytes, variant, rnd = parts[0], parts[1], parts[2], parts[3]
                if variant not in ("reference_production", "stride3_production"):
                    continue
                try:
                    census_ns, normalize_ns = int(parts[4]), int(parts[6])
                except ValueError:
                    continue
                block.setdefault((corpus, rnd), {})[variant] = (census_ns, normalize_ns)
    except OSError as err:
        print(f"FAIL: cannot read {sys.argv[1]}: {err}")
        return 1
    flush(block)

    if not per_corpus:
        print("FAIL: no same-round production pairs parsed from benchmark output")
        return 1

    print()
    print("=== aarch64 NAL start-code scan parity ===")
    print("ratio = parent byte-scan time / shipped stride-scan time (>= 1.0 = shipped is faster)")
    print(
        f"{'corpus':22s} {'census_med':>10s} {'norm_med':>10s} "
        f"{'min_pair':>9s} {'pairs':>6s}"
    )
    lowest = float("inf")
    failed: list[str] = []
    for corpus in sorted(per_corpus):
        census = [c for c, _ in per_corpus[corpus]]
        normalize = [n for _, n in per_corpus[corpus]]
        census_med = statistics.median(census)
        normalize_med = statistics.median(normalize)
        min_pair = min(min(census), min(normalize))
        lowest = min(lowest, census_med, normalize_med)
        below = census_med < FLOOR or normalize_med < FLOOR
        flag = "  <-- BELOW FLOOR" if below else ""
        if below:
            failed.append(corpus)
        print(
            f"{corpus:22s} {census_med:10.3f} {normalize_med:10.3f} "
            f"{min_pair:9.3f} {len(per_corpus[corpus]):6d}{flag}"
        )

    print()
    print(
        f"floor (per-corpus median, both walks): {FLOOR:.2f}    "
        f"lowest corpus median: {lowest:.3f}"
    )
    if failed:
        print(
            f"FAIL: {len(failed)} corpus median(s) below {FLOOR} on aarch64: {failed}"
        )
        print("The shipped stride scan is materially slower than the byte scan here.")
        print("Per the SHIP-TWO-STAGE disposition, gate an aarch64 byte-scan fallback")
        print('(#[cfg(target_arch = "aarch64")] over the parent byte finder) before')
        print("release tagging.")
        return 1

    print(
        f"PASS: all {len(per_corpus)} corpora clear the {FLOOR:.2f} "
        "architectural-health floor on aarch64."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
