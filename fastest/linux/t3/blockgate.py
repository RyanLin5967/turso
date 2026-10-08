#!/usr/bin/env python3
"""blockgate.py -- does a V3 batch let its block pass? (V3 review 2 item 5; rulings A14 and A16, PREREG annex)

  blockgate.py batch OUTDIR RC BLOCK PLP     decide one batch from run.sh's OUTDIR and rc; print the decision JSON;
                                             exit 0 PASS or RECORDED, 1 FAIL
  blockgate.py plants OUTDIR RC PLP OUT      the planted arms on copies of this real batch's record (below); write
                                             OUT (JSON); exit 0 iff the control passed and every plant was decided
                                             as planted
  blockgate.py self-test                     the decision on synthetic records; exit 0 iff every case passes

BLOCK is loop, brd or device; PLP is yes or no (a real run's --plp; dry runs say no).
A14: timing is never the evidence that a flush reached media. A16 (annex row, 2026-10-08) makes A14's counter gate
conditional on the cross-checked write-cache state:
  - (b) the state is recorded and cross-checked: the batch's leaf record carries the kernel's queue/write_cache and
    the drive's own report (NVMe VWC, SCSI WCE, virtio cache_type). Kernel missing, or kernel and drive disagreeing,
    REFUSES. (The probe already refuses a disagreement; this re-checks the record.)
  - write back (volatile cache): at least one device flush per sync: flush_gate leaf_flushes_completed >=
    required_flushes (n x gated arms), and neither the probe's flush_gate nor its blkflush leaf gate FAILed. The
    timing control applies as registered (rc 3 fails), unless PLP is declared: then the timing control is NOT
    APPLICABLE (rc 3 recorded when timing is its only void reason) and the counter rule still binds.
  - write through (kernel and drive both): the block layer sends the drive no flush, so the counter MUST read 0; a
    non-zero count contradicts the declared state and REFUSES. The timing control is NOT APPLICABLE (rc 3 recorded
    when timing is its only void reason; the field timing_control says so). A16 (3): the app issued at least one sync
    in every gated window, per the strace labelling run: the batch must be BOUND (binary.txt) to a fire-check verdict
    for this binary whose F1b:real-all and F1b:real-4k checks passed (the exact per-window syscall sequences, each
    gated window carrying its fsync). An unbound (smoke) batch or a failed F1b FAILS.
  - brd (dry runs only): rc 0 passes; rc 3 whose ONLY void reason is the timing control is RECORDED; else FAIL.
  - rc 2 (refused), rc 1 (an op failed), any other rc, or a missing summary.json/raw.tsv: FAIL.
The plants (every block, on copies of the real record; gate-6 reviews: each plant is ONE field changed on a record
the control shows passing, so a plant cannot fire without its defect):
  control         the real record, judged as a device block                           must PASS (else NOT-RUN)
  b-missing       the kernel write_cache removed                                      must FAIL (b)
  b-mismatch      the drive's report flipped against the kernel's                     must FAIL (b)
  wb-short        as write back, leaf_flushes_completed = required - 1                must FAIL (write back)
  wb-gatefail     as write back, rc 0 but the probe's flush_gate says FAIL            must FAIL (write back)
  wt-pass         as write through (kernel and drive), the counter reading 0          must PASS
  wt-nonzero      as write through (kernel and drive), the counter reading 1          must FAIL (write through)
  wt-nosync       as write through, the strace evidence of a sync per window absent   must FAIL (write through)
On an unbound (smoke, brd) batch the plant base assumes the app-sync evidence, so wt-nosync is still one field.
The branch the real drive is not on is reached by setting both kernel and drive to that state; the counts stay the
record's own except the one planted field.
"""
import copy
import hashlib
import json
import os
import sys


F1B_REAL = ("F1b:real-all", "F1b:real-4k")


def app_sync_evidence(outdir):
    """A16 (3): the batch's binding (run.sh's binary.txt) to a fire-check verdict whose real strace checks passed.
    Returns {"bound": bool, "verdict": path, "f1b_real": True/False/None, "why": ...}."""
    ev = {"bound": False, "verdict": None, "f1b_real": None}
    b = os.path.join(outdir, "binary.txt")
    if not os.path.exists(b):
        ev["why"] = "no binary.txt"
        return ev
    kv = dict(l.split("=", 1) for l in open(b).read().splitlines() if "=" in l)
    bound = kv.get("bound", "")
    if not bound.startswith("fire-checked: "):
        ev["why"] = f"not bound ({bound!r})"
        return ev
    ev["bound"], ev["verdict"] = True, bound[len("fire-checked: "):]
    try:
        raw = open(ev["verdict"], "rb").read()
        if hashlib.sha256(raw).hexdigest() != kv.get("verdict_sha256"):
            ev["f1b_real"], ev["why"] = False, "the verdict file is not the one the batch was bound to (sha256)"
            return ev
        v = json.loads(raw)
        got = {c["id"]: c.get("pass") for c in v.get("checks") or []}
        ev["f1b_real"] = all(got.get(k) is True for k in F1B_REAL) and v.get("v3floor_sha256") == kv.get("v3floor_sha256")
        if not ev["f1b_real"]:
            ev["why"] = f"F1b real checks {[got.get(k) for k in F1B_REAL]}, verdict sha matches {v.get('v3floor_sha256') == kv.get('v3floor_sha256')}"
    except (OSError, ValueError, KeyError, TypeError) as e:
        ev["f1b_real"], ev["why"] = False, f"verdict unreadable: {type(e).__name__}"
    return ev


def load(outdir):
    sj = None
    p = os.path.join(outdir, "summary.json")
    if os.path.exists(p):
        try:
            sj = json.load(open(p))
        except ValueError:
            sj = None
    if sj is not None:
        sj["_app_sync"] = app_sync_evidence(outdir)
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
        return fail(f"(b) the kernel's write-cache state is not recorded ({wc!r})")
    if drive in ("write back", "write through") and drive != wc:
        return fail(f"(b) the kernel says {wc!r} and the drive reports {drive!r}: a mismatch refuses (A16)")
    need, got = g.get("required_flushes"), g.get("leaf_flushes_completed")
    d.update({"required_flushes": need, "leaf_flushes_completed": got})
    if not isinstance(got, int):
        return fail(f"the leaf's flush counter is not recorded ({got!r})")
    if wc == "write through":
        d["class"] = "write through"
        d["timing_control"] = "NOT APPLICABLE (write-through drive)"
        if got != 0:
            return fail(f"(write through) the leaf's flush counter rose {got}: a write-through drive gets no flush "
                        "request, so a non-zero count contradicts the recorded state (A16 refuses)")
        if rc == 3 and (bk == "FAIL" or not timing_void):
            return fail("(write through) rc 3 for a reason other than the timing control")
        ev = sj.get("_app_sync") or {}
        d["app_sync"] = ev
        if not ev.get("f1b_real"):
            return fail("(write through) no strace evidence of a sync in every gated window (A16 (3)): "
                        + str(ev.get("why") or "no binding record"))
        if rc == 3:
            d["reasons"].append("timing control NOT APPLICABLE on a write-through drive: rc 3 recorded")
        d["decision"] = "PASS"
        return d
    d["class"] = "write back"
    d["timing_control"] = "NOT APPLICABLE (PLP declared)" if plp == "yes" else "applies"
    if not isinstance(need, int) or need <= 0:
        return fail(f"(write back) the run's sync count is not recorded ({need!r})")
    if got < need:
        return fail(f"(write back) the leaf's flush counter rose {got}, fewer than one per sync ({need})")
    if counter_void:
        return fail(f"(write back) the probe's own flush gate FAILed (flush_gate {g.get('outcome')!r}, "
                    f"blkflush {bk!r})")
    if rc == 3:
        if plp != "yes":
            return fail("(write back) the timing control applies as registered, and the batch is VOID")
        d["reasons"].append("timing control NOT APPLICABLE (PLP declared): rc 3 recorded")
    d["decision"] = "PASS"
    return d


def _set_state(sj, state):
    sj.setdefault("leaf", {})
    sj["leaf"]["write_cache"] = state
    sj["leaf"]["drive_reports"] = state
    sj["leaf_write_cache"] = state


def plants(sj, raw, rc, plp):
    """The planted arms on copies of a real record; returns (results, ok). ok needs the control to pass and every
    plant to be decided as planted; a control that does not pass makes every plant NOT-RUN (ok False)."""
    out = []
    if sj is not None and not (sj.get("_app_sync") or {}).get("f1b_real"):
        sj = copy.deepcopy(sj)  # an unbound (smoke) record: the base assumes the evidence, so wt-nosync is one field
        sj["_app_sync"] = {"bound": False, "f1b_real": True, "why": "ASSUMED for the plant base (unbound batch)"}
    ctl = decide(copy.deepcopy(sj), raw, 0 if rc == 3 else rc, "device", plp) if sj is not None else None
    out.append({"plant": "control", "want": "PASS", "got": ctl, "fired": bool(ctl) and ctl["decision"] == "PASS"})
    if not out[0]["fired"]:
        out.append({"plant": "all", "want": "-", "got": None, "fired": False,
                    "why": "NOT-RUN: the real record does not pass as a device block, so no plant can discriminate"})
        return out, False
    real = (sj.get("leaf") or {}).get("write_cache") or sj.get("leaf_write_cache")
    g = sj.get("flush_gate") or {}
    need = g.get("required_flushes")

    def arm(name, mutate, want_prefix):
        s = copy.deepcopy(sj)
        mutate(s)
        r = decide(s, raw, 0, "device", plp)
        fired = r["decision"] == "FAIL" and any(x.startswith(want_prefix) for x in r["reasons"])
        out.append({"plant": name, "want": f"FAIL {want_prefix}", "got": r, "fired": fired})

    def b_missing(s):
        s["leaf"].pop("write_cache", None)
        s.pop("leaf_write_cache", None)

    def b_mismatch(s):
        s["leaf"]["drive_reports"] = "write through" if real == "write back" else "write back"

    def wb(s, got=None, gate=None):
        _set_state(s, "write back")
        fg = s.setdefault("flush_gate", {})
        if not isinstance(fg.get("required_flushes"), int) or fg["required_flushes"] <= 0:
            fg["required_flushes"] = 1400  # a write-through record's own need, when the probe left it unset
        if got is not None:
            fg["leaf_flushes_completed"] = got(fg["required_flushes"])
        else:
            fg["leaf_flushes_completed"] = max(fg.get("leaf_flushes_completed") or 0, fg["required_flushes"])
        if gate:
            fg["outcome"] = gate

    def wt(s, flushes=0, sync=True):
        _set_state(s, "write through")
        s.setdefault("flush_gate", {})["leaf_flushes_completed"] = flushes
        if not sync:
            s["_app_sync"] = {"bound": False, "f1b_real": False, "why": "PLANTED: no sync evidence"}

    arm("b-missing", b_missing, "(b)")
    arm("b-mismatch", b_mismatch, "(b)")
    arm("wb-short", lambda s: wb(s, got=lambda n: n - 1), "(write back)")
    arm("wb-gatefail", lambda s: wb(s, gate="FAIL"), "(write back)")
    s = copy.deepcopy(sj)
    wt(s)
    r = decide(s, raw, 0, "device", plp)
    out.append({"plant": "wt-pass", "want": "PASS", "got": r, "fired": r["decision"] == "PASS"})
    arm("wt-nonzero", lambda s: wt(s, flushes=1), "(write through)")
    arm("wt-nosync", lambda s: wt(s, sync=False), "(write through)")
    return out, all(p["fired"] for p in out)


def self_test():
    def rec(wc="write back", drive="write back", fc="pass", gate="pass", bk="pass", need=1400, got=2800, kind="drive",
            sync=True):
        return {"flush_control": fc, "leaf": {"write_cache": wc, "drive_reports": drive, "kind": kind},
                "flush_gate": {"outcome": gate, "required_flushes": need, "leaf_flushes_completed": got,
                               "blkflush_leaf_gate": {"outcome": bk}},
                "_app_sync": {"bound": sync, "f1b_real": sync}}

    def wt(**k):
        base = dict(wc="write through", drive="write through", gate="not applicable: no volatile cache: no drive flush",
                    bk="not applicable", got=0)
        base.update(k)
        return rec(**base)

    def dec(sj, rc=0, block="device", plp="no", raw=True):
        return decide(sj, raw, rc, block, plp)["decision"]

    void_t = dict(fc="FAIL: run void (ow4k)")
    cases = [
        ("write back rc 0 with the counter met PASS", dec(rec()) == "PASS"),
        ("write back, counter one short FAILs", dec(rec(got=1399)) == "FAIL"),
        ("write back, the probe's gate FAIL with rc 0 FAILs (defensive)", dec(rec(gate="FAIL")) == "FAIL"),
        ("write back timing VOID (rc 3) FAILs", dec(rec(**void_t), 3) == "FAIL"),
        ("write back + PLP: timing VOID recorded, counter met: PASS", dec(rec(**void_t), 3, plp="yes") == "PASS"),
        ("write back + PLP: counter short FAILs", dec(rec(got=10, **void_t), 3, plp="yes") == "FAIL"),
        ("A16 write through, counter 0, rc 0: PASS (the sda runners of 37800269067)", dec(wt()) == "PASS"),
        ("A16 write through, timing VOID recorded: PASS", dec(wt(**void_t), 3) == "PASS"),
        ("A16 write through with a non-zero counter REFUSES", dec(wt(got=5)) == "FAIL"),
        ("A16 (3) write through with no strace sync evidence (unbound or F1b failed) FAILs", dec(wt(sync=False)) == "FAIL"),
        ("write back does not need the binding (its counter is the evidence)", dec(rec(sync=False)) == "PASS"),
        ("A16 kernel write through, drive write back: mismatch REFUSES", dec(wt(drive="write back")) == "FAIL"),
        ("A16 kernel write back, drive write through: mismatch REFUSES", dec(rec(drive="write through")) == "FAIL"),
        ("(b) no kernel state FAILs", dec(rec(wc=None)) == "FAIL"),
        ("no counter reading FAILs", dec(rec(got=None)) == "FAIL"),
        ("rc 2 FAILs", dec(rec(), 2) == "FAIL"),
        ("rc 1 FAILs", dec(rec(), 1) == "FAIL"),
        ("missing raw.tsv FAILs", dec(rec(), 0, raw=False) == "FAIL"),
        ("missing summary FAILs", dec(None, 0) == "FAIL"),
        ("brd rc 0 PASS", dec(wt(kind="brd"), 0, "brd") == "PASS"),
        ("brd timing VOID RECORDED", dec(wt(kind="brd", **void_t), 3, "brd") == "RECORDED"),
        ("brd rc 3 from a counter gate FAILs", dec(rec(gate="FAIL", **void_t), 3, "brd") == "FAIL"),
    ]
    p, ok = plants(rec(), True, 0, "no")
    cases.append(("plants on a passing write-back record: the control passes and all seven fire",
                  ok and [x["plant"] for x in p] == ["control", "b-missing", "b-mismatch", "wb-short", "wb-gatefail",
                                                     "wt-pass", "wt-nonzero", "wt-nosync"]))
    p, ok = plants(wt(), True, 0, "no")
    cases.append(("plants on a passing write-through record: all fire (the write-back arms on its own need)", ok))
    p, ok = plants(wt(kind="brd", sync=False, **void_t), True, 3, "no")
    cases.append(("plants on an unbound brd record with a timing VOID: the base assumes the evidence, all fire", ok))
    p, ok = plants(rec(got=10), True, 0, "no")
    cases.append(("plants on a record that does not pass: NOT-RUN, not fired (review MED 2)", not ok and len(p) == 2))
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
