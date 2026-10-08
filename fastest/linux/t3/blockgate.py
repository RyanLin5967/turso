#!/usr/bin/env python3
"""blockgate.py -- does a V3 batch let its block pass? (V3 review 2 item 5, rulings A10/A14, PREREG annex A14)

  blockgate.py batch OUTDIR RC BLOCK PLP     decide one batch from run.sh's OUTDIR and rc; print the decision JSON;
                                             exit 0 PASS or RECORDED, 1 FAIL
  blockgate.py plants OUTDIR RC PLP OUT      the A14 planted arms on copies of this real batch's record (below); write
                                             OUT (JSON); exit 0 iff every plant was decided as planted
  blockgate.py self-test                     the decision on synthetic records; exit 0 iff every case passes

BLOCK is loop, brd or device; PLP is yes or no (a real run's --plp declaration; dry runs say no).
Ruling A14 (lead, 2026-10-06T19:36Z): timing is never the evidence that a flush reached media.
  - brd (dry runs only): rc 0 passes; rc 3 whose ONLY void reason is the timing control is RECORDED (an expected
    VOID; brd feeds no rule); anything else fails.
  - (b) the write-cache state is recorded: the batch's leaf record carries the kernel's queue/write_cache ("write back"
    or "write through"). The probe itself refuses a kernel state that disagrees with the drive's own report (NVMe VWC,
    SCSI WCE, virtio cache_type), so a recorded state is one both agree on. Missing: FAIL.
  - Class "no volatile cache" when the kernel or the drive reports write through, or PLP is declared; else "volatile".
  - volatile: the timing control applies as registered: rc 0 passes, rc 3 fails.
  - no volatile cache: the timing control is NOT APPLICABLE, so rc 3 is recorded when the timing control is its only
    void reason; the block passes only if (a) the leaf's flush counter rose by at least the run's sync count
    (flush_gate: leaf_flushes_completed >= required_flushes = n x gated arms). Any other void reason fails.
    NOTE, measured: a queue whose write_cache reads "write through" receives no flush request at all (the block layer
    ends an empty flush bio before it is accounted; brd read 0.0 flushes per fsync, dry run 37519757687), so on such
    a drive (a) cannot hold and the block FAILS. Reported to the lead; this script implements the ruling as written.
  - rc 2 (refused), rc 1 (an op failed), any other rc, or a missing summary.json/raw.tsv: FAIL.
The plants (run on every block, on copies of the real record, so each gate is forced to fire on real numbers):
  a-short    PLP declared, leaf_flushes_completed = required - 1        must FAIL with "(a)"
  a-control  PLP declared, the real counts                              must PASS when the real counts meet (a)
  b-missing  the write-cache state removed                              must FAIL with "(b)"
"""
import copy
import json
import os
import sys


def load(outdir):
    sj = None
    p = os.path.join(outdir, "summary.json")
    if os.path.exists(p):
        try:
            sj = json.load(open(p))
        except ValueError:
            sj = None
    return sj, os.path.exists(os.path.join(outdir, "raw.tsv"))


def decide(sj, raw, rc, block, plp):
    d = {"rc": rc, "block": block, "plp": plp, "reasons": []}

    def fail(why):
        d["reasons"].append(why)
        d["decision"] = "FAIL"
        return d

    if rc not in (0, 3):
        return fail(f"rc {rc}: the batch was refused (2) or an op failed (1)")
    if sj is None or not raw:
        return fail("summary.json or raw.tsv missing")
    fc = str(sj.get("flush_control") or "")
    g = sj.get("flush_gate") or {}
    bk = (g.get("blkflush_leaf_gate") or {}).get("outcome")
    timing_void = fc.startswith("FAIL")
    counter_void = g.get("outcome") == "FAIL" or bk == "FAIL"
    d.update({"flush_control": fc, "flush_gate": g.get("outcome"), "blkflush_leaf_gate": bk})
    if block == "brd":
        if rc == 0:
            d["decision"] = "PASS"
        elif timing_void and not counter_void:
            d["decision"] = "RECORDED"
            d["reasons"].append("brd: an expected VOID (no drive behind brd; the timing control cannot discriminate)")
        else:
            return fail("brd: rc 3 for a reason other than the timing control")
        return d
    leaf = sj.get("leaf") or {}
    wc = leaf.get("write_cache") or sj.get("leaf_write_cache")
    drive = leaf.get("drive_reports")
    d.update({"write_cache": wc, "drive_reports": drive})
    if wc not in ("write back", "write through"):
        return fail(f"(b) the write-cache state is not recorded (kernel {wc!r}, drive {drive!r})")
    novol = wc == "write through" or drive == "write through" or plp == "yes"
    d["class"] = "no volatile cache" if novol else "volatile"
    if not novol:
        if rc == 0:
            d["decision"] = "PASS"
            return d
        return fail("volatile cache: the timing control applies as registered, and the batch is VOID"
                    + (" (timing)" if timing_void else "") + (" (flush counter)" if counter_void else ""))
    if rc == 3 and (counter_void or not timing_void):
        return fail("no volatile cache: rc 3 for a reason other than the timing control")
    if rc == 3:
        d["reasons"].append("timing control NOT APPLICABLE (no volatile cache or PLP): rc 3 recorded")
    need, got = g.get("required_flushes"), g.get("leaf_flushes_completed")
    d.update({"required_flushes": need, "leaf_flushes_completed": got})
    if not isinstance(need, int) or not isinstance(got, int) or need <= 0:
        return fail(f"(a) cannot be determined (required {need!r}, completed {got!r})")
    if got < need:
        why = f"(a) the leaf's flush counter rose {got}, fewer than the run's sync count {need}"
        if wc == "write through":
            why += " (a write-through queue receives no flush request: (a) cannot hold on this drive)"
        return fail(why)
    d["decision"] = "PASS"
    return d


def plants(sj, raw, rc, plp):
    """The A14 arms on copies of a real record; returns (results, all_fired)."""
    out = []
    g = (sj or {}).get("flush_gate") or {}
    need = g.get("required_flushes")
    s1 = copy.deepcopy(sj)
    if isinstance(need, int) and need > 0 and s1 is not None:
        s1.setdefault("flush_gate", {})["leaf_flushes_completed"] = need - 1
        r = decide(s1, raw, 0, "device", "yes")
        out.append({"plant": "a-short", "want": "FAIL (a)", "got": r,
                    "fired": r["decision"] == "FAIL" and any(x.startswith("(a)") for x in r["reasons"])})
    else:
        out.append({"plant": "a-short", "want": "FAIL (a)", "got": None, "fired": False,
                    "why": f"no usable required_flushes in the real record ({need!r})"})
    r = decide(copy.deepcopy(sj), raw, 0, "device", "yes")
    got = g.get("leaf_flushes_completed")
    meets = isinstance(got, int) and isinstance(need, int) and need > 0 and got >= need
    out.append({"plant": "a-control", "want": "PASS" if meets else "FAIL (a): the real counts do not meet (a)",
                "got": r, "fired": (r["decision"] == "PASS") == meets})
    s3 = copy.deepcopy(sj)
    if s3 is not None:
        s3.pop("leaf_write_cache", None)
        if isinstance(s3.get("leaf"), dict):
            s3["leaf"].pop("write_cache", None)
            s3["leaf"].pop("drive_reports", None)
    r = decide(s3, raw, 0, "device", plp)
    out.append({"plant": "b-missing", "want": "FAIL (b)", "got": r,
                "fired": r["decision"] == "FAIL" and any(x.startswith("(b)") for x in r["reasons"])})
    return out, all(p["fired"] for p in out)


def self_test():
    def rec(wc="write back", drive="write back", fc="pass", gate="pass", bk="pass", need=1400, got=2800):
        return {"flush_control": fc, "leaf": {"write_cache": wc, "drive_reports": drive, "kind": "drive"},
                "flush_gate": {"outcome": gate, "required_flushes": need, "leaf_flushes_completed": got,
                               "blkflush_leaf_gate": {"outcome": bk}}}

    def dec(sj, rc=0, block="device", plp="no", raw=True):
        return decide(sj, raw, rc, block, plp)["decision"]

    void_t = dict(fc="FAIL: run void (ow4k)")
    cases = [
        ("volatile rc 0 PASS", dec(rec()) == "PASS"),
        ("volatile timing VOID (rc 3) FAILs", dec(rec(**void_t), 3) == "FAIL"),
        ("volatile counter VOID (rc 3) FAILs", dec(rec(gate="FAIL"), 3) == "FAIL"),
        ("rc 2 FAILs", dec(rec(), 2) == "FAIL"),
        ("rc 1 FAILs", dec(rec(), 1) == "FAIL"),
        ("missing raw.tsv FAILs", dec(rec(), 0, raw=False) == "FAIL"),
        ("missing summary FAILs", dec(None, 0) == "FAIL"),
        ("(b) no write-cache state FAILs", dec(rec(wc=None), 0) == "FAIL"),
        ("PLP + timing VOID + counter met: rc 3 recorded, PASS", dec(rec(**void_t), 3, plp="yes") == "PASS"),
        ("PLP + counter one short FAILs (a)", dec(rec(need=1400, got=1399), 0, plp="yes") == "FAIL"),
        ("PLP + rc 3 from the counter gate FAILs", dec(rec(gate="FAIL"), 3, plp="yes") == "FAIL"),
        ("write-through drive, 0 flushes: FAILs (a) (cannot hold)",
         dec(rec(wc="write through", drive="write through", gate="not applicable: no volatile cache: no drive flush",
                 bk="not applicable", got=0, **void_t), 3) == "FAIL"),
        ("write-through drive with the counter met PASSes (timing not applicable)",
         dec(rec(wc="write through", drive="write through", gate="not applicable", bk="not applicable",
                 got=1400, **void_t), 3) == "PASS"),
        ("brd rc 0 PASS", dec(rec(wc="write through", drive=None), 0, "brd") == "PASS"),
        ("brd timing VOID RECORDED", dec(rec(wc="write through", gate="not applicable: brd", bk="not applicable",
                                             **void_t), 3, "brd") == "RECORDED"),
        ("brd rc 3 from a counter gate FAILs", dec(rec(gate="FAIL", **void_t), 3, "brd") == "FAIL"),
        ("brd rc 2 FAILs", dec(rec(), 2, "brd") == "FAIL"),
    ]
    p, ok = plants(rec(need=1400, got=2800), True, 0, "no")
    cases.append(("plants on a record meeting (a): all three fire", ok and [x["plant"] for x in p] == ["a-short", "a-control", "b-missing"]))
    p, ok = plants(rec(need=1400, got=10), True, 0, "no")
    cases.append(("plants on a record short of (a): the control says FAIL and still counts as fired", ok))
    bad = [n for n, good in cases if not good]
    for n, good in cases:
        print(f"BLOCKGATE self-test {'PASS' if good else 'FAIL'}: {n}")
    print(f"BLOCKGATE SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:] == ["self-test"]:
        return self_test()
    if len(a) == 6 and a[1] == "batch":
        sj, raw = load(a[2])
        d = decide(sj, raw, int(a[3]), a[4], a[5])
        print(json.dumps(d))
        return 0 if d["decision"] in ("PASS", "RECORDED") else 1
    if len(a) == 6 and a[1] == "plants":
        sj, raw = load(a[2])
        res, ok = plants(sj, raw, int(a[3]), a[4])
        json.dump({"plants": res, "all_fired": ok}, open(a[5], "w"), indent=1)
        print(json.dumps({"all_fired": ok, "fired": {p["plant"]: p["fired"] for p in res}}))
        return 0 if ok else 1
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
