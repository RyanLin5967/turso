#!/usr/bin/env python3
"""summarize.py OUT SHA DRY MANIFEST -- t3run.sh's closing record: are the raws complete?
   summarize.py self-test            the completeness rules on synthetic packages; exit 0 iff every case passes

Prints summary.json to stdout: the sha, mode, manifest and its sha256, every stage's seconds, the total
wall time, per block (one filesystem) its V3 cell and V3/V3L records, and per planned run: its attempts,
adapter rc, void verdict and whether its result exists.
A planned run is COMPLETE when its last attempt is VALID and its result is on disk (ours:
result/summary.json; a competitor: result/functional.txt ending in a VERDICT line), or when the engine
refused the class (adapter rc 4, 'NOT AVAILABLE' -- a recorded absence, not a result). A complete
competitor run whose VERDICT is not PASS is listed in failed_checks (complete raws of a failed cell;
the dry-run workflow fails on any).
A block is OK when both its V3 batches (before, after) returned rc 0 and left summary.json and raw.tsv, and its
V3L before and after are both VALID (review 2 items 5 and 6; a VOID V3L voids the block, PREREG line 180). Each
batch's rc, flush_control, frame-arm p50, D0 ratio, flush_sent_to_device and floor_kind, the before-to-after
frame-arm drift, and V3L's pooled p50, ratio and drift are copied in (published, not gates).
Exit 1 if any planned run is not complete, any block is not OK or never ran (fslist.txt names every block), a
stage failed, or nothing was planned: a run that collected nothing has not passed.
"""
import glob
import hashlib
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v3l  # noqa: E402


def read_rcs(path):
    """'before rc=N' lines -> {'before': N}; the last line for a name wins."""
    rcs = {}
    if os.path.exists(path):
        for line in open(path).read().splitlines():
            p = line.split()
            if len(p) == 2 and p[1].startswith("rc="):
                rcs[p[0]] = int(p[1][3:])
    return rcs


def v3_batch(fsdir, when, rc, block=None):
    d = os.path.join(fsdir, f"v3-{when}")
    rec = {"rc": rc, "summary": os.path.exists(os.path.join(d, "summary.json")),
           "raw": os.path.exists(os.path.join(d, "raw.tsv"))}
    if rec["summary"]:
        try:
            s = json.load(open(os.path.join(d, "summary.json")))
            fa = s.get("frame_arm")
            rec.update({"flush_control": s.get("flush_control"), "frame_arm": fa,
                        "frame_arm_p50_us": (s.get("arms", {}).get(fa) or {}).get("p50_us") if fa else None,
                        "flush_d0_p50_ratio": s.get("flush_d0_p50_ratio"),
                        "flush_sent_to_device": s.get("flush_sent_to_device"), "floor_kind": s.get("floor_kind"),
                        "flush_gate": (s.get("flush_gate") or {}).get("outcome"),
                        "leaf": (s.get("leaf") or {}).get("disk"), "leaf_kind": (s.get("leaf") or {}).get("kind")})
        except (OSError, ValueError, AttributeError) as e:
            rec["summary_error"] = f"{type(e).__name__}: {e}"
    b = os.path.join(d, "binary.txt")
    if os.path.exists(b):
        rec["bound"] = next((l[6:] for l in open(b).read().splitlines() if l.startswith("bound=")), None)
    complete = rec["summary"] and rec["raw"] and "summary_error" not in rec
    # brd (dry runs only): a VOID batch with complete raws is recorded; brd has no drive, so the probe's timing
    # control cannot discriminate there (t3run.sh v3batch). Every other block keeps rc 0.
    rec["brd_void_recorded"] = rc == 3 and block == "brd" and bool(complete)
    rec["ok"] = bool(complete) and (rc == 0 or rec["brd_void_recorded"])
    return rec


def block_record(out, fs):
    fsdir = os.path.join(out, f"fs-{fs}")
    if not os.path.isdir(fsdir):
        return {"fs": fs, "ok": False, "why": ["the block never ran"]}
    meta = {}
    bt = os.path.join(fsdir, "block.txt")
    if os.path.exists(bt):
        meta = dict(l.split("=", 1) for l in open(bt).read().splitlines() if "=" in l)
    rcs = read_rcs(os.path.join(fsdir, "v3.rc"))
    rec = {"fs": fs, "cell": meta.get("cell"), "block": meta.get("block"), "v3": {}, "why": []}
    for when in ("before", "after"):
        if when not in rcs:
            rec["v3"][when] = {"rc": None, "ok": False}
            rec["why"].append(f"V3 {when}: no batch ran")
            continue
        r = v3_batch(fsdir, when, rcs[when], meta.get("block"))
        rec["v3"][when] = r
        if not r["ok"]:
            rec["why"].append(f"V3 {when}: rc {r['rc']}, summary.json {'present' if r['summary'] else 'MISSING'}, "
                              f"raw.tsv {'present' if r['raw'] else 'MISSING'}")
    b, a = rec["v3"]["before"].get("frame_arm_p50_us"), rec["v3"]["after"].get("frame_arm_p50_us")
    rec["v3_frame_arm_drift_us"] = round(a - b, 2) if a is not None and b is not None else None
    rec["v3l"] = v3l.block(os.path.join(fsdir, "v3l-before", "v3l.json"), os.path.join(fsdir, "v3l-after", "v3l.json"))
    if rec["v3l"]["verdict"] != "VALID":
        reasons = [x for k in ("before", "after") for x in (rec["v3l"][k].get("void_reasons") or
                                                            ([rec["v3l"][k]["why"]] if "why" in rec["v3l"][k] else []))]
        rec["why"].append(f"V3L {rec['v3l']['verdict']}: " + "; ".join(reasons))
    rec["ok"] = not rec["why"]
    return rec


def summarize(out, sha, dry, manifest):
    stages = []
    sp = os.path.join(out, "stages.tsv")
    if os.path.exists(sp):
        for line in open(sp).read().splitlines()[1:]:
            name, s, e, secs, rc = line.split("\t")
            stages.append({"stage": name, "start": s, "end": e, "seconds": int(secs), "rc": int(rc)})
    attempts = {}
    cp = os.path.join(out, "cells.tsv")
    if os.path.exists(cp):
        for line in open(cp).read().splitlines()[1:]:
            fs, cell, system, clients, attempt, rc, void = line.split("\t")
            attempts.setdefault((fs, cell), []).append({"attempt": int(attempt), "adapter_rc": int(rc), "void": void,
                                                        "system": system, "clients": int(clients)})
    planned = []
    for plan in sorted(glob.glob(os.path.join(out, "fs-*", "plan.tsv"))):
        fs = os.path.basename(os.path.dirname(plan))[3:]
        for line in open(plan).read().splitlines():
            cell, system = line.split("\t")[:2]
            planned.append((fs, cell, system))
    runs, incomplete, failed_checks = [], [], []
    for fs, cell, system in planned:
        a = attempts.get((fs, cell), [])
        last = a[-1] if a else None
        d = os.path.join(out, f"fs-{fs}", "cells", cell, f"a{last['attempt']}") if last else None
        result, why = False, "no attempt recorded"
        if last:
            adapter = open(os.path.join(d, "adapter.txt"), errors="replace").read() if os.path.exists(os.path.join(d, "adapter.txt")) else ""
            if last["adapter_rc"] == 4 and "NOT AVAILABLE" in adapter:
                result, why = True, "NOT AVAILABLE (class absent at this sha)"
            elif last["void"] != "VALID":
                why = "last attempt VOID"
            elif system == "ours":
                result = os.path.exists(os.path.join(d, "result", "summary.json")) and last["adapter_rc"] == 0
                why = "ok" if result else f"no result (adapter rc {last['adapter_rc']})"
            else:
                f = os.path.join(d, "result", "functional.txt")
                text = open(f).read() if os.path.exists(f) else ""
                result = "VERDICT" in text
                why = (text.strip().splitlines() or ["no functional.txt"])[-1] if result else \
                    f"no functional VERDICT (adapter rc {last['adapter_rc']})"
                if result and "VERDICT PASS" not in text:
                    failed_checks.append(f"{fs}/{cell}: " + "; ".join(
                        l for l in text.splitlines() if l.startswith("FAIL ")))
        runs.append({"fs": fs, "cell": cell, "system": system, "attempts": a, "complete": result, "note": why})
        if not result:
            incomplete.append(f"{fs}/{cell}: {why}")
    fl = os.path.join(out, "fslist.txt")
    fslist = open(fl).read().split() if os.path.exists(fl) else []
    blocks = [block_record(out, fs) for fs in fslist]
    failed_blocks = [f"{b['fs']}: " + " | ".join(b["why"]) for b in blocks if not b["ok"]]
    if not fslist:
        failed_blocks.append("fslist.txt missing or empty: no block was planned")
    mode = {}
    mt = os.path.join(out, "mode.txt")
    if os.path.exists(mt):
        mode = dict(l.split("=", 1) for l in open(mt).read().splitlines() if "=" in l)
    mp = os.path.join(out, "manifest.tsv")
    msha = hashlib.sha256(open(mp, "rb").read()).hexdigest() if os.path.exists(mp) else None
    total = next((s["seconds"] for s in stages if s["stage"] == "TOTAL"), None)
    failed = [s["stage"] for s in stages if s["rc"] != 0 and s["stage"] != "TOTAL"]
    summary = {"sha": sha, "dry_run": dry == "1", "block": mode.get("block"), "plant": mode.get("plant") or None,
               "manifest": manifest, "manifest_sha256": msha,
               "wall_seconds": total, "stages": stages, "planned_runs": len(planned),
               "complete_runs": sum(r["complete"] for r in runs), "incomplete": incomplete,
               "failed_checks": failed_checks, "failed_blocks": failed_blocks,
               "failed_stages": failed, "blocks": blocks, "runs": runs}
    ok = bool(planned) and not incomplete and not failed and not failed_blocks
    return summary, ok


def main(out, sha, dry, manifest):
    summary, ok = summarize(out, sha, dry, manifest)
    json.dump(summary, sys.stdout, indent=1)
    print()
    print(f"summarize: planned={summary['planned_runs']} complete={summary['complete_runs']} "
          f"failed_stages={summary['failed_stages']} failed_blocks={summary['failed_blocks']} "
          f"failed_checks={len(summary['failed_checks'])} wall_s={summary['wall_seconds']}", file=sys.stderr)
    return 0 if ok else 1


def self_test():
    """Synthetic packages: one good block, then each way a block or the package can be incomplete."""
    def w(path, text):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w").write(text)

    def v3lj(verdict="VALID"):
        return json.dumps({"verdict": verdict, "void_reasons": [] if verdict == "VALID" else ["planted"],
                           "floor_kind": "no volatile cache: no drive flush", "leaf": {"disk": "ram0"},
                           "published": {"fsync_p50_us": 10.0, "fsync_over_control_write_p50": 5.0},
                           "arms": {"fsync": {"timed": {"fsync_bins_ns": {"10000": 5}}}}})

    def make(root, before_rc=0, before_v3l="VALID", after_v3l="VALID", drop=None, fslist="xfs", block="loop"):
        out = os.path.join(root, "out")
        w(f"{out}/stages.tsv", "stage\tstart_utc\tend_utc\tseconds\trc\nfs-xfs\ta\tb\t5\t0\nTOTAL\ta\tb\t9\t0\n")
        w(f"{out}/cells.tsv", "fs\tcell\tsystem\tclients\tattempt\tadapter_rc\tvoid\nxfs\tours-full-c1\tours\t1\t1\t0\tVALID\n")
        w(f"{out}/fslist.txt", fslist + "\n")
        w(f"{out}/mode.txt", "dry=1\nblock=loop\nplant=\n")
        f = f"{out}/fs-xfs"
        w(f"{f}/plan.tsv", "ours-full-c1\tours\t1\t200\t1\tfull\n")
        w(f"{f}/cells/ours-full-c1/a1/result/summary.json", "{}")
        w(f"{f}/cells/ours-full-c1/a1/adapter.txt", "")
        w(f"{f}/block.txt", f"cell={'xfs' if block == 'brd' else 'xfsloop'}\nblock={block}\n")
        w(f"{f}/v3.rc", f"before rc={before_rc}\nafter rc=0\n")
        for when in ("before", "after"):
            if not (when == "before" and before_rc == 2):  # a refused batch leaves no out dir
                w(f"{f}/v3-{when}/summary.json", json.dumps({"frame_arm": "append64", "arms": {"append64": {"p50_us": 80.0}},
                                                             "flush_control": "pass", "floor_kind": "x"}))
                w(f"{f}/v3-{when}/raw.tsv", "arm\tus\n")
        w(f"{f}/v3l-before/v3l.json", v3lj(before_v3l))
        if after_v3l:
            w(f"{f}/v3l-after/v3l.json", v3lj(after_v3l))
        if drop:
            os.unlink(f"{f}/{drop}")
        return out

    cases = []
    for name, kw, want_ok in [
        ("a complete package with a good block passes", {}, True),
        ("item 5 red test: the BEFORE batch refused (rc 2, no out dir) fails the package", {"before_rc": 2}, False),
        ("an AFTER batch with rc 0 but no raw.tsv fails the package", {"drop": "v3-after/raw.tsv"}, False),
        ("item 6: a VOID V3L after fails the package", {"after_v3l": "VOID"}, False),
        ("item 6: a missing V3L after fails the package", {"after_v3l": None}, False),
        ("item 6: a VOID V3L before with no after (the run stopped) reads VOID, not MISSING",
         {"before_v3l": "VOID", "after_v3l": None}, False),
        ("a block named in fslist.txt that never ran fails the package", {"fslist": "xfs btrfs"}, False),
        ("an empty fslist.txt fails the package", {"fslist": ""}, False),
        ("a VOID (rc 3) V3 batch with complete raws fails a loop block", {"before_rc": 3}, False),
        ("a VOID (rc 3) V3 batch with complete raws is recorded on a brd block", {"before_rc": 3, "block": "brd"}, True),
        ("a refused (rc 2) V3 batch fails a brd block too", {"before_rc": 2, "block": "brd"}, False),
    ]:
        root = tempfile.mkdtemp(prefix="summarize-st-")
        try:
            s, ok = summarize(make(root, **kw), "sha", "1", "m")
            good = ok == want_ok
            if "reads VOID" in name:
                good = good and s["failed_blocks"][0].startswith("xfs: V3L VOID: planted")
            cases.append((name, good, s["failed_blocks"]))
        finally:
            shutil.rmtree(root)
    root = tempfile.mkdtemp(prefix="summarize-st-")
    try:
        s, _ = summarize(make(root), "sha", "1", "m")
        b = s["blocks"][0]
        cases.append(("the good block carries the batches' frame-arm p50, the drift (0.0) and the pooled V3L p50 (10.0 us)",
                      b["v3"]["before"]["frame_arm_p50_us"] == 80.0 and b["v3_frame_arm_drift_us"] == 0.0
                      and b["v3l"]["published"]["pooled_fsync_p50_us"] == 10.0, None))
    finally:
        shutil.rmtree(root)
    bad = [c for c in cases if not c[1]]
    for n, ok, fb in cases:
        print(f"SUMMARIZE self-test {'PASS' if ok else 'FAIL'}: {n}" + (f" (failed_blocks {fb})" if fb else ""))
    print(f"SUMMARIZE SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["self-test"]:
        sys.exit(self_test())
    sys.exit(main(*sys.argv[1:5]))
