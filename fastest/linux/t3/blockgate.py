#!/usr/bin/env python3
"""blockgate.py -- does a V3 batch let its block pass? (V3 review 2 item 5; rulings A14 and A16, PREREG annex)

  blockgate.py batch OUTDIR RC BLOCK PLP     decide one batch from run.sh's OUTDIR and rc; print the decision JSON;
                                             exit 0 PASS, 1 FAIL
  blockgate.py plants OUTDIR RC PLP OUT      the planted arms on copies of this real batch's record (below); write
                                             OUT (JSON); exit 0 iff the control passed and every plant was decided
                                             as planted
  blockgate.py self-test [TESTDATA]          the decision on synthetic records and on the real records in TESTDATA
                                             (default: testdata/ beside this file); exit 0 iff every case passes

BLOCK is loop, brd or device; PLP is yes or no (a real run's --plp; dry runs say no). A device block (a real run)
also needs the batch bound to a passing fire-check verdict and of the registered shape, N=10000, whatever its class
(fourth lane review LOW 17); every non-brd block needs the record's required_flushes to equal n x its flush-gated
arms, re-derived as batchgate.post defines it (LOW 19).
Records are judged in the shape the V3 probe writes from c225124ae on (timing_control, d0_control, plp, and
batchgate.post's flush_gate.voids). Any other shape FAILs: the fields are an allowlist, not optional.
Every class first:
  - rc must be 0 or 3, summary.json and raw.tsv present, the batch's own plp equal to this run's.
  - VOID fails the block on every class (third lane review, HIGH 1): the probe's timing control FAILed, its D0
    control FAILed (a foreign writer: nosync25 p50 >= 50 us), or batchgate.post recorded a void in
    flush_gate.voids (the flush gate, a write-back layer's window with no flush-carrying request, a gated window
    with no fsync by the probe). rc 3 with no void recorded FAILs; so does rc 0 with one. The D0 control must read
    "pass". Annex A20 (the lead's ruling): on T3 a D0 foreign-writer void FAILS the block, because a foreign writer
    on the leaf also inflates the flush counter A16 relies on. A11's "brd VOID recorded" is narrowed by the same
    change: the probe has not voided brd on timing since c225124ae, so every brd void is a real one (annex row for
    A11 owed by the lead; fourth lane review LOW 18).
  - the probe's timing control must read what A14 gives the class: "pass" (write back, no PLP), "not applicable:
    PLP" (write back, --plp yes), "not applicable: no volatile cache" (write through), "not applicable: brd".
A16 (annex row, 2026-10-08) makes A14's counter gate conditional on the cross-checked write-cache state:
  - (b) the state is recorded and cross-checked: the leaf record carries the kernel's queue/write_cache and the
    drive's own report (NVMe VWC, SCSI WCE, virtio cache_type), and they are equal. Kernel missing, drive report
    missing or the two disagreeing REFUSES. (The probe already refuses a disagreement; this re-checks the record.)
  - write back (volatile cache): at least one device flush per sync: flush_gate leaf_flushes_completed >=
    required_flushes (n x gated arms), and neither the probe's flush_gate nor its blkflush leaf gate FAILed.
  - write through (kernel and drive both): the block layer sends the drive no flush, so the counter MUST read 0; a
    non-zero count contradicts the declared state and REFUSES. The timing control is NOT APPLICABLE (the field
    timing_control says so). A16 (3): the app issued at least one sync in every gated window, per the strace
    labelling run: the batch must be BOUND (binary.txt) to a fire-check verdict for this binary whose F1b:real-all
    and F1b:real-4k checks passed (the exact per-window syscall sequences, each gated window carrying its fsync). An
    unbound (smoke) batch or a failed F1b FAILS.
  - brd (dry runs only): PASS when nothing above fails.
The plants (every block, on copies of the real record; gate-6 reviews: each plant is ONE field changed on a record
a positive arm shows passing, so a plant cannot fire without its defect):
  control         the real record, judged as a loop block (the class rules; not the
                  device-only binding and shape)                                      must PASS (else NOT-RUN)
  b-missing       the kernel write_cache removed                                      must FAIL (b)
  b-mismatch      the drive's report flipped against the kernel's                     must FAIL (b)
  b-nodrive       the drive's report removed                                          must FAIL (b)
  void-d0         the D0 control FAILed (a foreign writer), rc 3                      must FAIL VOID
  void-nosync     a gated window with no fsync in flush_gate.voids, rc 3              must FAIL VOID
  wb-pass         as write back (kernel and drive), the counter at least the need     must PASS
  wb-short        wb-pass with leaf_flushes_completed = required - 1                  must FAIL (write back)
  wb-gatefail     wb-pass with rc 0 but the probe's flush_gate saying FAIL            must FAIL (write back)
  plp-pass        wb-pass under --plp yes (timing control "not applicable: PLP")      must PASS
  plp-d0          plp-pass with the D0 control FAILed, rc 3                           must FAIL VOID
  plp-nosync      plp-pass with a gated window with no fsync, rc 3                    must FAIL VOID
  wt-pass         as write through (kernel and drive), the counter reading 0          must PASS
  wt-nonzero      wt-pass with the counter reading 1                                  must FAIL (write through)
  wt-nosync       wt-pass with the strace evidence of a sync per window absent        must FAIL (write through)
  wt-tampered     wt-pass with the evidence re-read from a copy of this batch's binary.txt whose verdict_sha256
                  is rewritten (third lane review MED 6)                               must FAIL (write through)
                  (an unbound batch has no binding to tamper: recorded "not applicable", not counted)
On an unbound (smoke, brd) batch the plant base assumes the app-sync evidence, so wt-nosync is still one field; on
a brd record the base is first made a drive (write through, drive agreeing, its timing control as for that class).
The branch the real drive is not on is reached by setting both kernel and drive to that state (and the timing
control to that class's); the counts stay the record's own except the one planted field.
PREREG citations as ':N' or 'line N' are lines of artie frontier/fastest/PREREG-v1-FINAL-CANDIDATE.md, the text the
rulings cite, until PREREG-v1.md is registered (fourth lane review LOW 26).
"""
import copy
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile


F1B_REAL = ("F1b:real-all", "F1b:real-4k")
DEVICE_N = 10000  # the registered V3 batch shape (PREREG section 4; run.sh refuses a bound batch of any other N)
# the timing control a passing batch of each class carries (v3floor.c at c225124ae; annex A14): an allowlist
TIMING = {"write back": "pass", "write back+plp": "not applicable: PLP",
          "write through": "not applicable: no volatile cache", "brd": "not applicable: brd"}
NOSYNC_VOID = "fsync: append25's windows without an fsync or fdatasync by the probe: 1 of 10000"


def app_sync_evidence(outdir):
    """A16 (3): the batch's binding (run.sh's binary.txt) to a fire-check verdict whose real strace checks passed.
    Returns {"bound": bool, "verdict": path, "f1b_real": True/False/None, "why": ...}. The verdict is read at the
    path binary.txt names, or, when that path does not exist (a package read off the runner), at
    <outdir>/../v3-firecheck/<its name>, where t3run puts it; the sha256 binary.txt recorded binds either."""
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
    path = ev["verdict"]
    if not os.path.exists(path) and os.path.basename(os.path.dirname(path)) == "v3-firecheck":
        path = os.path.join(os.path.dirname(os.path.abspath(outdir)), "v3-firecheck", os.path.basename(path))
        ev["verdict_read_at"] = path
    try:
        raw = open(path, "rb").read()
        if hashlib.sha256(raw).hexdigest() != kv.get("verdict_sha256"):
            ev["f1b_real"], ev["why"] = False, "the verdict file is not the one the batch was bound to (sha256)"
            return ev
        v = json.loads(raw)
        got = {c["id"]: c.get("pass") for c in v.get("checks") or []}
        # the binary's sha must BE a sha256 and the verdict's must equal it: two absent fields, or two equal non-sha
        # strings, are no binding (T3 runner review item 15)
        exe = kv.get("v3floor_sha256")
        exe_ok = isinstance(exe, str) and re.fullmatch(r"[0-9a-f]{64}", exe) is not None and v.get("v3floor_sha256") == exe
        ev["f1b_real"] = all(got.get(k) is True for k in F1B_REAL) and exe_ok
        if not ev["f1b_real"]:
            ev["why"] = f"F1b real checks {[got.get(k) for k in F1B_REAL]}, binary sha {exe!r} bound and matching: {exe_ok}"
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as e:
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


def voids(sj):
    """Every void the record carries (the probe's timing and D0 controls, batchgate.post's flush_gate.voids), or None
    when the batch gate's void list is not recorded."""
    tc, d0 = str(sj.get("timing_control") or ""), str(sj.get("d0_control") or "")
    gv = (sj.get("flush_gate") or {}).get("voids")
    if not isinstance(gv, list):
        return None
    return ([f"timing control: {tc}"] if tc.startswith("FAIL") else []) + \
           ([f"D0 control: {d0}"] if d0.startswith("FAIL") else []) + [f"batch gate: {x}" for x in gv]


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
    g = sj.get("flush_gate") or {}
    bk = (g.get("blkflush_leaf_gate") or {}).get("outcome")
    tc, d0 = str(sj.get("timing_control") or ""), str(sj.get("d0_control") or "")
    counter_void = g.get("outcome") == "FAIL" or bk == "FAIL"
    d.update({"probe_timing_control": tc, "d0_control": d0, "flush_gate": g.get("outcome"), "blkflush_leaf_gate": bk,
              "voids": g.get("voids")})
    if sj.get("plp") != plp:
        return fail(f"the batch declares plp {sj.get('plp')!r}, this run {plp!r}")
    vs = voids(sj)
    if vs is None:
        return fail(f"the batch gate's void list is not recorded (flush_gate.voids {g.get('voids')!r})")
    if vs:
        return fail("VOID: " + "; ".join(vs) + " (a void fails its block on every class)")
    if rc == 3:
        return fail("rc 3 with no void recorded in the summary")
    if not d0.startswith("pass"):
        return fail(f"the D0 control did not pass ({d0!r})")
    if block == "device":
        # a real run's block (fourth lane review LOW 17): a batch bound to its fire-check verdict, of the registered
        # shape, whatever the class (a smoke batch, or N=200, is a dry run's)
        ev = sj.get("_app_sync") or {}
        if not (ev.get("bound") and ev.get("f1b_real")):
            return fail(f"(device) the batch is not bound to a passing fire-check verdict ({ev.get('why')})")
        if sj.get("n") != DEVICE_N:
            return fail(f"(device) the batch ran N={sj.get('n')!r}, the registered shape is N={DEVICE_N}")
    if block == "brd":
        if not tc.startswith(TIMING["brd"]):
            return fail(f"brd: the probe's timing control reads {tc!r}, not {TIMING['brd']!r}")
        d["decision"] = "PASS"
        return d
    leaf = sj.get("leaf") or {}
    wc = leaf.get("write_cache") or sj.get("leaf_write_cache")
    drive = leaf.get("drive_reports")
    d.update({"write_cache": wc, "drive_reports": drive})
    if wc not in ("write back", "write through"):
        return fail(f"(b) the kernel's write-cache state is not recorded ({wc!r})")
    if drive != wc:
        return fail(f"(b) the kernel says {wc!r} and the drive reports {drive!r}: they must agree (A16)")
    need, got = g.get("required_flushes"), g.get("leaf_flushes_completed")
    d.update({"required_flushes": need, "leaf_flushes_completed": got})
    # the sync count is re-derived as batchgate.post defines it, n x the flush-gated arms, never trusted as a field
    # (fourth lane review LOW 19)
    n = sj.get("n")
    gated = [a for a, r in (sj.get("flush_control_arms") or {}).items() if isinstance(r, dict) and r.get("gated")]
    derived = n * len(gated) if isinstance(n, int) and gated else None
    d["required_flushes_derived"] = derived
    if derived is None or need != derived:
        return fail(f"(shape) the record's required_flushes {need!r} is not n x the flush-gated arms "
                    f"({n!r} x {len(gated)} = {derived!r})")
    if not isinstance(got, int):
        return fail(f"the leaf's flush counter is not recorded ({got!r})")
    if wc == "write through":
        d["class"] = "write through"
        d["timing_control"] = "NOT APPLICABLE (write-through drive)"
        if not tc.startswith(TIMING["write through"]):
            return fail(f"(write through) the probe's timing control reads {tc!r}, not {TIMING['write through']!r}")
        if got != 0:
            return fail(f"(write through) the leaf's flush counter rose {got}: a write-through drive gets no flush "
                        "request, so a non-zero count contradicts the recorded state (A16 refuses)")
        ev = sj.get("_app_sync") or {}
        d["app_sync"] = ev
        if not ev.get("f1b_real"):
            return fail("(write through) no strace evidence of a sync in every gated window (A16 (3)): "
                        + str(ev.get("why") or "no binding record"))
        d["decision"] = "PASS"
        return d
    d["class"] = "write back"
    want = TIMING["write back+plp" if plp == "yes" else "write back"]
    d["timing_control"] = "NOT APPLICABLE (PLP declared)" if plp == "yes" else "applies"
    if not (tc.startswith(want) if plp == "yes" else tc == want):
        return fail(f"(write back) the probe's timing control reads {tc!r}, not {want!r} (--plp {plp})")
    if not isinstance(need, int) or need <= 0:
        return fail(f"(write back) the run's sync count is not recorded ({need!r})")
    if got < need:
        return fail(f"(write back) the leaf's flush counter rose {got}, fewer than one per sync ({need})")
    if counter_void:
        return fail(f"(write back) the probe's own flush gate FAILed (flush_gate {g.get('outcome')!r}, "
                    f"blkflush {bk!r})")
    d["decision"] = "PASS"
    return d


def _set_state(sj, state, plp="no"):
    sj.setdefault("leaf", {})
    sj["leaf"]["write_cache"] = state
    sj["leaf"]["drive_reports"] = state
    sj["leaf_write_cache"] = state
    sj["timing_control"] = TIMING["write back+plp" if state == "write back" and plp == "yes" else state]


def plants(sj, raw, rc, plp, outdir=None):
    """The planted arms on copies of a real record; returns (results, ok). ok needs the control to pass and every
    counted plant to be decided as planted; a control that does not pass makes every plant NOT-RUN (ok False).
    outdir (the real batch's run.sh OUT) enables wt-tampered."""
    out = []
    bound = bool(sj) and bool((sj.get("_app_sync") or {}).get("bound"))
    # where the real batch's verdict was actually read (on the runner: the bound path; off it: beside the batch), so
    # the tampered copy differs from the real binding in verdict_sha256 ONLY (fourth lane review MED 7)
    read_at = ((sj or {}).get("_app_sync") or {}).get("verdict_read_at") or ((sj or {}).get("_app_sync") or {}).get("verdict")
    if sj is not None:
        sj = copy.deepcopy(sj)
        if (sj.get("leaf") or {}).get("kind") == "brd":
            sj["leaf"]["kind"] = "drive"  # a brd record is made a drive first: the counts stay its own
            _set_state(sj, "write through")
        if not (sj.get("_app_sync") or {}).get("f1b_real"):
            # an unbound (smoke) record: the base assumes the evidence, so wt-nosync is one field
            sj["_app_sync"] = {"bound": False, "f1b_real": True, "why": "ASSUMED for the plant base (unbound batch)"}
    ctl = decide(copy.deepcopy(sj), raw, rc, "loop", plp) if sj is not None else None
    out.append({"plant": "control", "want": "PASS", "got": ctl, "fired": bool(ctl) and ctl["decision"] == "PASS"})
    if not out[0]["fired"]:
        out.append({"plant": "all", "want": "-", "got": None, "fired": False,
                    "why": "NOT-RUN: the real record does not pass as a loop block, so no plant can discriminate"})
        return out, False
    real = (sj.get("leaf") or {}).get("write_cache") or sj.get("leaf_write_cache")

    def arm(name, base, mutate, want_prefix, rc_=0, plp_=plp):
        s = copy.deepcopy(sj)
        base(s)
        mutate(s)
        r = decide(s, raw, rc_, "loop", plp_)
        if want_prefix is None:
            out.append({"plant": name, "want": "PASS", "got": r, "fired": r["decision"] == "PASS"})
            return
        fired = r["decision"] == "FAIL" and any(x.startswith(want_prefix) for x in r["reasons"])
        out.append({"plant": name, "want": f"FAIL {want_prefix}", "got": r, "fired": fired})

    def same(s):
        pass

    def b_missing(s):
        s["leaf"].pop("write_cache", None)
        s.pop("leaf_write_cache", None)

    def b_mismatch(s):
        s["leaf"]["drive_reports"] = "write through" if real == "write back" else "write back"

    def d0_fail(s):
        s["d0_control"] = "FAIL: run void (nosync25 p50 61.0 us >= 50 us: a foreign writer on the device)"

    def nosync(s):
        s.setdefault("flush_gate", {}).setdefault("voids", []).append(NOSYNC_VOID)

    def wb(s, plp_="no"):
        _set_state(s, "write back", plp_)
        s["plp"] = plp_
        fg = s.setdefault("flush_gate", {})
        if not isinstance(fg.get("required_flushes"), int) or fg["required_flushes"] <= 0:
            fg["required_flushes"] = 1400  # a write-through record's own need, when the probe left it unset
        fg["leaf_flushes_completed"] = max(fg.get("leaf_flushes_completed") or 0, fg["required_flushes"])
        if str(fg.get("outcome", "")).startswith("not applicable"):
            fg["outcome"] = "pass"  # a write-through record's gate outcome, as a write-back drive's gate reads

    def wbp(s):
        wb(s, "yes")

    def wt(s):
        _set_state(s, "write through")
        s.setdefault("flush_gate", {})["leaf_flushes_completed"] = 0

    arm("b-missing", same, b_missing, "(b)")
    arm("b-mismatch", same, b_mismatch, "(b)")
    arm("b-nodrive", same, lambda s: s["leaf"].pop("drive_reports", None), "(b)")
    arm("void-d0", same, d0_fail, "VOID", 3)
    arm("void-nosync", same, nosync, "VOID", 3)
    arm("wb-pass", wb, same, None, plp_="no")
    arm("wb-short", wb, lambda s: s["flush_gate"].update(leaf_flushes_completed=s["flush_gate"]["required_flushes"] - 1),
        "(write back)", plp_="no")
    arm("wb-gatefail", wb, lambda s: s["flush_gate"].update(outcome="FAIL"), "(write back)", plp_="no")
    arm("plp-pass", wbp, same, None, plp_="yes")
    arm("plp-d0", wbp, d0_fail, "VOID", 3, plp_="yes")
    arm("plp-nosync", wbp, nosync, "VOID", 3, plp_="yes")
    arm("wt-pass", wt, same, None)
    arm("wt-nonzero", wt, lambda s: s["flush_gate"].update(leaf_flushes_completed=1), "(write through)")
    arm("wt-nosync", wt, lambda s: s.update(_app_sync={"bound": False, "f1b_real": False,
                                                        "why": "PLANTED: no sync evidence"}), "(write through)")
    if bound and outdir:
        tmp = tempfile.mkdtemp(prefix="blockgate-tamper-")
        try:
            sub = os.path.join(tmp, os.path.basename(os.path.normpath(outdir)))
            os.makedirs(sub)
            lines = open(os.path.join(outdir, "binary.txt")).read().splitlines()
            lines = [("verdict_sha256=" + "0" * 64) if l.startswith("verdict_sha256=")
                     else ("bound=fire-checked: " + os.path.abspath(read_at)) if l.startswith("bound=") and read_at
                     else l for l in lines]
            open(os.path.join(sub, "binary.txt"), "w").write("\n".join(lines) + "\n")
            ev = app_sync_evidence(sub)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        s_ = copy.deepcopy(sj)
        wt(s_)
        s_["_app_sync"] = ev
        r = decide(s_, raw, 0, "loop", plp)
        # fired only by the sha256 binding itself: a verdict the copy could not find is not the defect planted
        fired = r["decision"] == "FAIL" and any(x.startswith("(write through)") and "sha256" in x for x in r["reasons"])
        out.append({"plant": "wt-tampered", "want": "FAIL (write through) ... sha256", "got": r, "fired": fired})
    else:
        out.append({"plant": "wt-tampered", "want": "-", "got": None, "fired": None, "counted": False,
                    "why": "not applicable: " + ("an unbound batch has no binding to tamper" if not bound
                                                 else "no batch directory given")})
    return out, all(p["fired"] for p in out if p.get("counted", True))


def _write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def evidence_cases(tmp):
    """app_sync_evidence on temporary batch directories (third lane review MED 6): the function that parses
    binary.txt, checks the verdict sha256, the F1b ids and v3floor_sha256."""
    exe = "a" * 64

    def verdict(f1b=(True, True), exe_=exe):
        return json.dumps({"v3floor_sha256": exe_, "checks": [{"id": "F1b:real-all", "pass": f1b[0]},
                                                               {"id": "F1b:real-4k", "pass": f1b[1]},
                                                               {"id": "F3:complete", "pass": True}]})

    def batch(name, vtext, bound="fire-checked: {v}", sha=None, write_verdict=True, where="abs", exe_line=None):
        fs = os.path.join(tmp, name)
        out = os.path.join(fs, "v3-before")
        vpath = os.path.join(fs, "v3-firecheck", "verdict.json")
        if write_verdict:
            _write(vpath, vtext)
        named = vpath if where == "abs" else os.path.join("/nonexistent-runner-path", "fs-x", "v3-firecheck",
                                                         "verdict.json")
        s = sha if sha is not None else hashlib.sha256(vtext.encode()).hexdigest()
        xl = f"v3floor_sha256={exe}\n" if exe_line is None else exe_line
        _write(os.path.join(out, "binary.txt"), f"{xl}bound={bound.format(v=named)}\n"
                                                f"verdict_sha256={s}\nplp=no\n")
        return app_sync_evidence(out)

    good = verdict()
    e = {
        "good": batch("good", good),
        "relocated": batch("relocated", good, where="moved"),
        "tampered": batch("tampered", good, sha="0" * 64),
        "f1b-false": batch("f1b", verdict((True, False))),
        "f1b-absent": batch("f1b-absent", json.dumps({"v3floor_sha256": exe, "checks": []})),
        "other-binary": batch("exe", verdict(exe_="b" * 64)),
        "smoke": batch("smoke", good, bound="smoke: V3_SMOKE=1, not bound to a fire-check, never credited"),
        "missing-verdict": batch("missing", good, write_verdict=False),
        "garbage-verdict": batch("garbage", "[1, 2"),
        # T3 runner review item 15: two absent shas, or two equal non-sha strings, compared equal and passed
        "no-exe-sha": batch("noexe", json.dumps({"checks": json.loads(good)["checks"]}), exe_line=""),
        "non-hex-sha": batch("nonhex", verdict(exe_="x"), exe_line="v3floor_sha256=x\n"),
    }
    os.makedirs(os.path.join(tmp, "nobin", "v3-before"))
    e["no-binary"] = app_sync_evidence(os.path.join(tmp, "nobin", "v3-before"))
    return [
        ("app_sync_evidence: a bound batch whose verdict passed F1b for this binary is evidence", e["good"]["f1b_real"] is True),
        ("app_sync_evidence: a package read off the runner finds the verdict beside it, sha-bound",
         e["relocated"]["f1b_real"] is True and e["relocated"].get("verdict_read_at", "").startswith(tmp)),
        ("app_sync_evidence: a verdict file that is not the bound one (sha256) is not evidence", e["tampered"]["f1b_real"] is False),
        ("app_sync_evidence: F1b:real-4k false is not evidence", e["f1b-false"]["f1b_real"] is False),
        ("app_sync_evidence: no F1b checks at all is not evidence", e["f1b-absent"]["f1b_real"] is False),
        ("app_sync_evidence: a verdict for another binary is not evidence", e["other-binary"]["f1b_real"] is False),
        ("app_sync_evidence: a smoke binding is unbound, not evidence", e["smoke"]["bound"] is False and not e["smoke"]["f1b_real"]),
        ("app_sync_evidence: a missing verdict file is not evidence", e["missing-verdict"]["f1b_real"] is False),
        ("app_sync_evidence: an unparseable verdict is not evidence", e["garbage-verdict"]["f1b_real"] is False),
        ("app_sync_evidence: no binary.txt is unbound", e["no-binary"]["bound"] is False and not e["no-binary"]["f1b_real"]),
        ("item 15: a binary.txt and a verdict that both lack v3floor_sha256 are not evidence (None is not a sha)",
         e["no-exe-sha"]["f1b_real"] is False),
        ("item 15: a binary.txt and a verdict that both carry the same non-sha string are not evidence",
         e["non-hex-sha"]["f1b_real"] is False),
    ], os.path.join(tmp, "good", "v3-before")


def self_test(data):
    def rec(wc="write back", drive="write back", tc=None, d0="pass (nosync25 p50 3.0 us < 50 us)", gate="pass",
            bk="pass", need=1400, got=2800, kind="drive", sync=True, plp="no", voids=()):
        if tc is None:
            tc = TIMING.get("brd" if kind == "brd" else "write back+plp" if wc == "write back" and plp == "yes" else wc,
                            "pass")
        gated = ["append25", "append64", "ow4k", "ow64k", "ow1m", "cfr2b", "fdatasync4k"]  # 7 x n=200 = need 1400
        return {"timing_control": tc, "flush_control": tc, "d0_control": d0, "plp": plp, "traced": False, "n": 200,
                "flush_control_arms": {a: {"gated": True} for a in gated} | {"nosync25": {"gated": False}},
                "leaf": {"write_cache": wc, "drive_reports": drive, "kind": kind}, "leaf_write_cache": wc,
                "flush_gate": {"outcome": gate, "required_flushes": need, "leaf_flushes_completed": got,
                               "blkflush_leaf_gate": {"outcome": bk}, "voids": list(voids)},
                "_app_sync": {"bound": sync, "f1b_real": sync}}

    def wt(**k):
        base = dict(wc="write through", drive="write through", gate="not applicable: no volatile cache: no drive flush",
                    bk="not applicable", got=0)
        base.update(k)
        return rec(**base)

    def brd(**k):
        base = dict(drive="none (RAM)", kind="brd", sync=False)
        base.update(k)
        return wt(**base)

    # the class rules are judged as a loop (dry) block; the device-only rules (LOW 17) have their own cases
    def dec(sj, rc=0, block="loop", plp="no", raw=True):
        return decide(sj, raw, rc, block, plp)["decision"]

    foreign = "FAIL: run void (nosync25 p50 61.0 us >= 50 us: a foreign writer on the device)"
    timing_void = "FAIL: run void (append25 ratio 3.1 <= threshold 10.00)"
    cases = [
        ("write back rc 0 with the counter met PASS", dec(rec()) == "PASS"),
        ("write back, counter one short FAILs", dec(rec(got=1399)) == "FAIL"),
        ("write back, the probe's gate FAIL with rc 0 FAILs (defensive)", dec(rec(gate="FAIL")) == "FAIL"),
        ("write back timing VOID (rc 3) FAILs", dec(rec(tc=timing_void), 3) == "FAIL"),
        ("HIGH 1: write back + PLP, D0 foreign writer, rc 3 FAILs", dec(rec(plp="yes", d0=foreign), 3, plp="yes") == "FAIL"),
        ("HIGH 1: write back + PLP, a window without a sync, rc 3 FAILs",
         dec(rec(plp="yes", voids=[NOSYNC_VOID]), 3, plp="yes") == "FAIL"),
        ("write back + PLP rc 0, counter met, timing not applicable: PASS", dec(rec(plp="yes"), 0, plp="yes") == "PASS"),
        ("write back + PLP: counter short FAILs", dec(rec(plp="yes", got=10), 0, plp="yes") == "FAIL"),
        ("write back + PLP declared but the probe applied timing ('pass'): FAILs", dec(rec(plp="yes", tc="pass"), 0, plp="yes") == "FAIL"),
        ("write back, no PLP, but the probe says 'not applicable: PLP': FAILs", dec(rec(tc="not applicable: PLP")) == "FAIL"),
        ("the batch's plp differs from the run's: FAILs (write through, where nothing else reads plp)",
         dec(wt(plp="yes"), 0, plp="no") == "FAIL" and dec(wt(plp="yes"), 0, plp="yes") == "PASS"),
        ("rc 0 with a void recorded FAILs", dec(rec(voids=["flush gate: x"])) == "FAIL"),
        ("rc 3 with no void recorded FAILs", dec(rec(), 3) == "FAIL"),
        ("no void list (an older probe's shape) FAILs", dec({**rec(), "flush_gate": {**rec()["flush_gate"], "voids": None}}) == "FAIL"),
        ("the D0 control not run FAILs", dec(rec(d0="not run: nosync25 not selected")) == "FAIL"),
        ("A16 write through, counter 0, rc 0, bound: PASS", dec(wt()) == "PASS"),
        ("write through, D0 foreign writer, rc 3: FAILs (LOW 8: no timing VOID can pass)", dec(wt(d0=foreign), 3) == "FAIL"),
        ("write through, a window without a sync in the batch gate, rc 3: FAILs", dec(wt(voids=[NOSYNC_VOID]), 3) == "FAIL"),
        ("write through with the timing control applied ('pass'): FAILs", dec(wt(tc="pass")) == "FAIL"),
        ("A16 write through with a non-zero counter REFUSES", dec(wt(got=5)) == "FAIL"),
        ("A16 (3) write through with no strace sync evidence (unbound or F1b failed) FAILs", dec(wt(sync=False)) == "FAIL"),
        ("write back does not need the binding (its counter is the evidence)", dec(rec(sync=False)) == "PASS"),
        ("A16 kernel write through, drive write back: mismatch REFUSES", dec(wt(drive="write back")) == "FAIL"),
        ("A16 kernel write back, drive write through: mismatch REFUSES", dec(rec(drive="write through")) == "FAIL"),
        ("LOW 7: write through with no drive report REFUSES", dec(wt(drive=None)) == "FAIL"),
        ("LOW 7: write through with drive report 'unknown' REFUSES", dec(wt(drive="unknown")) == "FAIL"),
        ("(b) no kernel state FAILs", dec(rec(wc=None)) == "FAIL"),
        ("no counter reading FAILs", dec(rec(got=None)) == "FAIL"),
        ("rc 2 FAILs", dec(rec(), 2) == "FAIL"),
        ("rc 1 FAILs", dec(rec(), 1) == "FAIL"),
        ("missing raw.tsv FAILs", dec(rec(), 0, raw=False) == "FAIL"),
        ("missing summary FAILs", dec(None, 0) == "FAIL"),
        ("brd rc 0 PASS", dec(brd(), 0, "brd") == "PASS"),
        ("brd D0 foreign writer rc 3 FAILs (no RECORDED any more)", dec(brd(d0=foreign), 3, "brd") == "FAIL"),
        ("brd with an applied timing control FAILs", dec(brd(tc="pass"), 0, "brd") == "FAIL"),
        # fourth lane review LOW 16: red at 2d42982a0, whose brd branch RECORDED a timing-only void
        ("LOW 16: brd rc 3 whose only void is the timing control FAILs (no RECORDED)",
         dec(brd(tc="FAIL: run void (append25 ratio 3.1 <= threshold 10.00)"), 3, "brd") == "FAIL"),
        # fourth lane review LOW 19: the sync count is re-derived, n x the flush-gated arms, never trusted
        ("LOW 19: required_flushes that is not n x the gated arms FAILs",
         dec(dict(rec(), n=200, flush_control_arms={"append25": {"gated": True}, "ow4k": {"gated": True}})) == "FAIL"),
        ("LOW 19: required_flushes equal to n x the gated arms passes",
         dec(dict(rec(need=400), n=200, flush_control_arms={"append25": {"gated": True}, "ow4k": {"gated": True}})) == "PASS"),
        # fourth lane review LOW 17: a real run's device block needs a bound batch of the registered shape, any class
        ("LOW 17: an unbound (smoke) write-back batch FAILs a device block", dec(rec(sync=False), 0, "device") == "FAIL"),
        ("LOW 17: the same batch passes a loop (dry-run) block", dec(rec(sync=False), 0, "loop") == "PASS"),
        ("LOW 17: a bound batch of N=200 FAILs a device block", dec(dict(rec(), n=200), 0, "device") == "FAIL"),
        ("LOW 17: a bound batch of N=10000 passes a device block",
         dec(dict(rec(need=70000, got=140000), n=10000), 0, "device") == "PASS"),
    ]
    want_order = ["control", "b-missing", "b-mismatch", "b-nodrive", "void-d0", "void-nosync", "wb-pass", "wb-short",
                  "wb-gatefail", "plp-pass", "plp-d0", "plp-nosync", "wt-pass", "wt-nonzero", "wt-nosync", "wt-tampered"]
    p, ok = plants(rec(), True, 0, "no")
    cases.append(("plants on a passing write-back record: the control passes and every counted plant fires",
                  ok and [x["plant"] for x in p] == want_order))
    p, ok = plants(wt(), True, 0, "no")
    cases.append(("plants on a passing write-through record: all fire (the write-back arms on its own need)", ok))
    p, ok = plants(brd(), True, 0, "no")
    cases.append(("plants on an unbound brd record: the base is made a drive and assumes the evidence, all fire", ok))
    p, ok = plants(rec(got=10), True, 0, "no")
    cases.append(("plants on a record that does not pass: NOT-RUN, not fired (review MED 2)", not ok and len(p) == 2))
    p, ok = plants(rec(d0=foreign), True, 3, "no")
    cases.append(("plants on a VOID record: NOT-RUN, not fired", not ok and len(p) == 2))
    with tempfile.TemporaryDirectory(prefix="blockgate-selftest-") as tmp:
        ecases, good_dir = evidence_cases(tmp)
        cases += ecases
        sj = wt()
        sj["_app_sync"] = app_sync_evidence(good_dir)
        cases.append(("decide on the bound temp batch's real evidence: write through PASS", dec(sj) == "PASS"))
        p, ok = plants(sj, True, 0, "no", good_dir)
        t = [x for x in p if x["plant"] == "wt-tampered"]
        cases.append(("MED 6 plant: verdict_sha256 rewritten on a copy of the bound batch's binary.txt fires",
                      ok and len(t) == 1 and t[0]["fired"] is True
                      and "sha256" in str(t[0]["got"]["reasons"])))
        p, ok = plants(wt(sync=False), True, 0, "no", good_dir)
        t = [x for x in p if x["plant"] == "wt-tampered"]
        cases.append(("an unbound batch's wt-tampered is recorded not applicable and not counted",
                      ok and t[0]["fired"] is None and t[0]["counted"] is False))
        # fourth lane review MED 7: off the runner (the bound path absent, the verdict beside the batch) the plant must
        # still reach the sha256 check, not fire on a verdict it could not find
        moved = os.path.join(tmp, "relocated", "v3-before")
        sj = wt()
        sj["_app_sync"] = app_sync_evidence(moved)
        p, ok = plants(sj, True, 0, "no", moved)
        t = [x for x in p if x["plant"] == "wt-tampered"]
        cases.append(("MED 7: off the runner, wt-tampered fires on the sha256 check, not on a missing verdict",
                      ok and len(t) == 1 and t[0]["fired"] is True
                      and any("sha256" in r for r in t[0]["got"]["reasons"])
                      and not any("FileNotFoundError" in r for r in t[0]["got"]["reasons"])))
    # the real records (testdata/README.md): run 37812355435's batches in the c225124ae format
    for name, want in (("v3-37812355435-x86-ext4loop", "PASS"), ("v3-37812355435-arm-ext4loop", "FAIL")):
        d = os.path.join(data, name)
        sj, raw = load(d)
        rc = int(open(os.path.join(d, "rc")).read().split("rc=")[-1].split()[0])  # run.sh's own rc (LOW 19)
        r = decide(sj, raw, rc, "loop", "no") if sj is not None else {"decision": None, "reasons": ["no record"]}
        why = "write back, counter met" if want == "PASS" else "write through, unbound smoke: no A16 (3) evidence"
        cases.append((f"real {name}: {want} ({why})", r["decision"] == want
                      and (want == "PASS" or any(x.startswith("(write through) no strace") for x in r["reasons"]))))
        p, ok = plants(sj, raw, rc, "no", d)
        cases.append((f"real {name}: plants all fire on copies of it", ok and [x["plant"] for x in p] == want_order))
        if want == "PASS":
            cases.append((f"LOW 17: real {name} (smoke, N=200) FAILs as a device block",
                          decide(sj, raw, rc, "device", "no")["decision"] == "FAIL"))
    bad = [n for n, good in cases if not good]
    for n, good in cases:
        print(f"BLOCKGATE self-test {'PASS' if good else 'FAIL'}: {n}")
    print(f"BLOCKGATE SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:2] == ["self-test"] and len(a) <= 3:
        return self_test(a[2] if len(a) == 3 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
    if len(a) == 6 and a[1] == "batch":
        sj, raw = load(a[2])
        d = decide(sj, raw, int(a[3]), a[4], a[5])
        print(json.dumps(d))
        return 0 if d["decision"] == "PASS" else 1
    if len(a) == 6 and a[1] == "plants":
        sj, raw = load(a[2])
        res, ok = plants(sj, raw, int(a[3]), a[4], a[2])
        json.dump({"plants": res, "all_fired": ok}, open(a[5], "w"), indent=1)
        print(json.dumps({"all_fired": ok, "fired": {p["plant"]: p["fired"] for p in res}}))
        return 0 if ok else 1
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
