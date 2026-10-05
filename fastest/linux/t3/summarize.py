#!/usr/bin/env python3
"""summarize.py OUT SHA DRY MANIFEST -- t3run.sh's closing record: are the raws complete?

Prints summary.json to stdout: the sha, mode, manifest and its sha256, every stage's seconds, the total
wall time, and per planned run: its attempts, adapter rc, void verdict and whether its result exists.
A planned run is COMPLETE when its last attempt is VALID and its result is on disk (ours:
result/summary.json; a competitor: result/functional.txt ending in a VERDICT line), or when the engine
refused the class (adapter rc 4, 'NOT AVAILABLE' -- a recorded absence, not a result). A complete
competitor run whose VERDICT is not PASS is listed in failed_checks (complete raws of a failed cell;
the dry-run workflow fails on any). Exit 1 if any
planned run is not complete, if a stage failed, or if nothing was planned: a run that collected nothing
has not passed.
"""
import glob
import hashlib
import json
import os
import sys


def main(out, sha, dry, manifest):
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
    mp = os.path.join(out, "manifest.tsv")
    msha = hashlib.sha256(open(mp, "rb").read()).hexdigest() if os.path.exists(mp) else None
    total = next((s["seconds"] for s in stages if s["stage"] == "TOTAL"), None)
    failed = [s["stage"] for s in stages if s["rc"] != 0 and s["stage"] != "TOTAL"]
    summary = {"sha": sha, "dry_run": dry == "1", "manifest": manifest, "manifest_sha256": msha,
               "wall_seconds": total, "stages": stages, "planned_runs": len(planned),
               "complete_runs": sum(r["complete"] for r in runs), "incomplete": incomplete,
               "failed_checks": failed_checks,
               "failed_stages": failed, "runs": runs}
    json.dump(summary, sys.stdout, indent=1)
    print()
    ok = planned and not incomplete and not failed
    print(f"summarize: planned={len(planned)} complete={summary['complete_runs']} failed_stages={failed} "
          f"failed_checks={len(failed_checks)} wall_s={total}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:5]))
