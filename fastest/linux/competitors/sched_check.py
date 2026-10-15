#!/usr/bin/env python3
"""sched_check.py -- checks of bbload's open-loop arrivals and window against an independent model (lane
fastest-linux-comp; competitors review 2 f6c2dbb2f..12f79726b items 1 and 2).

The model is written from the macOS bbload's text at artie 4010ff3b06 (claim_op and main), never from this port:
  seed  x0 = (SEED * 0xA24BAED4963EE407 ^ 0x9FB21C651E98DF25) mod 2^64, 1 if that is 0;
  xs    x ^= x << 13; x ^= x >> 7; x ^= x << 17 (mod 2^64);   unif = ((xs >> 11) + 0.5) / 2^53;
  gap   (uint64_t)(-log(unif) * 1e9 / RATE), truncated;  arrival k at t0 + gap_1 + ... + gap_k (arrival 0 at t0).
Python's math.log is the platform libm's log, the same one the C build calls on the same runner.

  sched_check.py offsets SEED RATE N            the model's first N arrival offsets (ns since t0), one per line
  sched_check.py raw OUTDIR SEED RATE           exit 0 only when OUTDIR/raw.tsv's warm-up and measured rows, sorted
                                                by arrival, have intended_ns - t0_ns equal to the model's offsets
                                                (every arrival 0..A-1 claimed, A = summary schedule_arrivals), and
                                                summary.json's seed, schedule_arrivals and schedule_hash agree
  sched_check.py same OUTDIR_A OUTDIR_B         exit 0 only when the two runs measured the same arrivals: the
                                                sorted intended_ns - t0_ns of their measured rows are equal (the
                                                window is defined in intended time, so a slower system must not
                                                measure more or other arrivals)
  sched_check.py after-tm1 OUTDIR               exit 0 only when no measured row has intended_ns >= summary tm1_ns
  sched_check.py selftest
"""
import csv
import json
import math
import os
import sys
import tempfile

M64 = (1 << 64) - 1


def xs(x):
    x ^= (x << 13) & M64
    x ^= x >> 7
    x ^= (x << 17) & M64
    return x


def offsets(seed, rate, n):
    x = ((seed * 0xA24BAED4963EE407) & M64) ^ 0x9FB21C651E98DF25
    x = x or 1
    out, t = [], 0
    for _ in range(n):
        out.append(t)
        x = xs(x)
        u = ((x >> 11) + 0.5) / 9007199254740992.0
        t += int(-math.log(u) * 1e9 / rate)
    return out


def fnv(offs):
    """FNV-1a 64 over each offset's 8 little-endian bytes in arrival order (the macOS schedule_hash)."""
    h = 1469598103934665603
    for off in offs:
        for b in range(8):
            h ^= (off >> (8 * b)) & 0xff
            h = (h * 1099511628211) & M64
    return f"{h:016x}"


def load(d):
    with open(os.path.join(d, "summary.json")) as f:
        sm = json.load(f)
    with open(os.path.join(d, "raw.tsv")) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return sm, rows


def check_raw(d, seed, rate):
    sm, rows = load(d)
    why = []
    t0 = sm.get("t0_ns")
    if not isinstance(t0, int):
        return [f"no t0_ns in {d}/summary.json"]
    if sm.get("seed") != seed:
        why.append(f"summary seed {sm.get('seed')!r} is not {seed}")
    cl = sorted((int(r["arrival"]), int(r["intended_ns"])) for r in rows if r["phase"] in ("warmup", "measure"))
    a = sm.get("schedule_arrivals")
    if a != len(cl):
        why.append(f"schedule_arrivals {a!r} but {len(cl)} warm-up and measured rows")
    if [i for i, _ in cl] != list(range(len(cl))):
        why.append("the warm-up and measured rows are not arrivals 0..A-1, each once")
    got = [t - t0 for _, t in cl]
    want = offsets(seed, rate, len(got))
    if got != want:
        bad = next(i for i, (g, w) in enumerate(zip(got, want)) if g != w)
        why.append(f"arrival {bad}: intended - t0 = {got[bad]} ns, the model says {want[bad]} ns")
    if sm.get("schedule_hash") != fnv(got):
        why.append(f"schedule_hash {sm.get('schedule_hash')!r}, the model's over these offsets is {fnv(got)}")
    return why


def measured(d):
    sm, rows = load(d)
    t0 = sm.get("t0_ns")
    if not isinstance(t0, int):
        raise ValueError(f"no t0_ns in {d}/summary.json")
    return sorted(int(r["intended_ns"]) - t0 for r in rows if r["phase"] == "measure")


def check_same(a, b):
    ma, mb = measured(a), measured(b)
    if ma == mb:
        return []
    return [f"the runs measured different arrivals: {len(ma)} and {len(mb)} measured, first difference at "
            f"{next((i for i, (x, y) in enumerate(zip(ma, mb)) if x != y), min(len(ma), len(mb)))}"]


def check_after_tm1(d):
    sm, rows = load(d)
    tm1 = sm.get("tm1_ns")
    if not isinstance(tm1, int) or not tm1:
        return [f"no tm1_ns in {d}/summary.json"]
    late = [r for r in rows if r["phase"] == "measure" and int(r["intended_ns"]) >= tm1]
    return [f"{len(late)} measured row(s) intended at or after tm1 {tm1}"] if late else []


def selftest():
    bad = n = 0

    def ok(name, cond):
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), name)
        bad += not cond
        n += 1

    # The model's own arithmetic, checked piecewise against the text (no driver involved): the seed, one xorshift
    # step and the unif map, computed here by hand-written steps
    x0 = ((1 * 0xA24BAED4963EE407) & M64) ^ 0x9FB21C651E98DF25
    ok("seed 1: x0 = 0xA24BAED4963EE407 ^ 0x9FB21C651E98DF25", x0 == 0x3df9b2b188a63b22)
    a = x0 ^ ((x0 << 13) & M64)
    b = a ^ (a >> 7)
    c = b ^ ((b << 17) & M64)
    ok("one xorshift step equals xs()", xs(x0) == c)
    ok("unif lies in (0, 1)", 0 < ((c >> 11) + 0.5) / 9007199254740992.0 < 1)
    o = offsets(1, 100.0, 4)
    ok("arrival 0 is at t0, and offsets increase", o[0] == 0 and o[1] < o[2] < o[3])
    ok("the gap is -log(unif) * 1e9 / RATE, truncated", o[1] == int(-math.log(((c >> 11) + 0.5) / 2 ** 53) * 1e9 / 100))
    ok("the seed changes the stream", offsets(2, 100.0, 3) != o[:3])
    ok("RATE scales the gaps (R=200 gives about half)", abs(offsets(1, 200.0, 2)[1] - o[1] / 2) <= 1)
    ok("the hash covers order", fnv([0, 1, 2]) != fnv([0, 2, 1]) and len(fnv([0])) == 16)

    with tempfile.TemporaryDirectory() as d:
        def run(name, rows, sm):
            p = os.path.join(d, name)
            os.makedirs(p)
            with open(os.path.join(p, "summary.json"), "w") as f:
                json.dump(sm, f)
            with open(os.path.join(p, "raw.tsv"), "w") as f:
                f.write("client\tseq\tphase\tok\tarrival\tintended_ns\n")
                for r in rows:
                    f.write("\t".join(str(v) for v in r) + "\n")
            return p
        t0 = 10 ** 12
        offs = offsets(7, 50.0, 6)
        good_rows = [(i % 2, i // 2, "warmup" if i < 2 else "measure", 1, i, t0 + o) for i, o in enumerate(offs)]
        sm = {"t0_ns": t0, "seed": 7, "schedule_arrivals": 6, "schedule_hash": fnv(offs), "tm1_ns": t0 + offs[-1] + 1}
        g = run("good", good_rows, sm)
        ok("raw: the model's arrivals are accepted", check_raw(g, 7, 50.0) == [])
        ok("raw: another seed is refused", any("seed" in w for w in check_raw(g, 8, 50.0)))
        moved = [r if r[4] != 3 else r[:5] + (r[5] + 1,) for r in good_rows]
        ok("raw: one arrival 1 ns off is refused",
           any("arrival 3" in w for w in check_raw(run("moved", moved, sm), 7, 50.0)))
        ok("raw: a missing arrival is refused",
           any("schedule_arrivals" in w for w in check_raw(run("gap", good_rows[:3] + good_rows[4:], sm), 7, 50.0)))
        ok("raw: a wrong schedule_hash is refused",
           any("schedule_hash" in w for w in check_raw(run("hash", good_rows, dict(sm, schedule_hash="0" * 16)), 7, 50.0)))
        ok("raw: drain rows are not arrivals of the schedule",
           check_raw(run("drain", good_rows + [(0, 9, "drain", 1, 6, t0 + 10 ** 9)], sm), 7, 50.0) == [])
        ok("same: identical measured arrivals", check_same(g, run("g2", good_rows, sm)) == [])
        extra = good_rows + [(1, 9, "measure", 1, 6, t0 + 10 ** 10)]
        ok("same: one more measured arrival is refused", check_same(g, run("extra", extra, sm)) != [])
        ok("after-tm1: none at or after tm1", check_after_tm1(g) == [])
        ok("after-tm1: a measured row at tm1 is refused",
           check_after_tm1(run("late", good_rows, dict(sm, tm1_ns=t0 + offs[-1]))) != [])
        ok("after-tm1: no tm1_ns is refused", check_after_tm1(run("notm1", good_rows, {"t0_ns": t0})) != [])
    print(f"sched_check selftest: {n - bad}/{n}")
    return 1 if bad else 0


def main(a):
    if a == ["selftest"]:
        return selftest()
    try:
        if len(a) == 4 and a[0] == "offsets":
            print("\n".join(str(o) for o in offsets(int(a[1]), float(a[2]), int(a[3]))))
            return 0
        if len(a) == 4 and a[0] == "raw":
            why = check_raw(a[1], int(a[2]), float(a[3]))
        elif len(a) == 3 and a[0] == "same":
            why = check_same(a[1], a[2])
        elif len(a) == 2 and a[0] == "after-tm1":
            why = check_after_tm1(a[1])
        else:
            print(__doc__, file=sys.stderr)
            return 2
    except (OSError, ValueError, KeyError) as e:
        print(f"sched_check: REFUSED: {e}")
        return 2
    print("ok" if not why else "REFUSED: " + "; ".join(why))
    return 0 if not why else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
