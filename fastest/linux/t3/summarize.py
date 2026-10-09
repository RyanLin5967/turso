#!/usr/bin/env python3
"""summarize.py OUT SHA DRY MANIFEST -- t3run.sh's closing record: are the raws complete?
   summarize.py self-test            the completeness rules on synthetic packages; exit 0 iff every case passes

Prints summary.json to stdout: the sha, mode, manifest and its sha256, every stage's seconds, the total
wall time, per block (one filesystem) its V3 cell and V3/V3L records, and per planned run: its attempts,
adapter rc, void verdict and whether its result exists.
A planned run is COMPLETE when its last attempt is VALID, its adapter exited 0 and its result is on disk (ours:
result/summary.json; a competitor: result/functional.txt ending in a VERDICT line; third lane review LOW 12: a run
killed after writing VERDICT is not complete), or when the engine refused the class (adapter rc 4, 'NOT AVAILABLE'
-- a recorded absence, not a result). A complete competitor run whose VERDICT is not PASS is listed in
failed_checks (complete raws of a failed cell; the dry-run workflow fails on any).
Parity (gate-6 review 3 and 12; third lane review MED 3), every measured run: its warm-up rule is the package's one
rule (warmup.txt, OPS:S:MAX_S) as the run itself recorded it (ours: result/summary.json warmup_rule; a competitor:
warmup_rule of every timed run, result/cells/*/timed/summary.json), its op total is its plan row's (ours:
ops_total == ops_total_asked == the plan's ops; a competitor: every timed run's timed.json, timedrun.py's verdict ok
and its N == the plan's ops; a run ended by the registered cap is complete with reduced n, in runs[].reduced_n,
fourth lane review MED 4). Totals are NOT
compared across systems: the PREREG sizes n_run per system (fourth lane review HIGH 2 overrides gate-6 item 12).
Any difference is listed in parity_refusals and fails.
A block is OK when both its V3 batches (before, after) pass blockgate.py (review 2 item 5, rulings A14 and A16; every
VOID fails), the A16 plants re-derived on its real BEFORE record all fire (blockgate-plants.json stored and
re-run), the before-to-after drift was taken (batchgate.py drift; REFUSED fails; a VOID is published, not a gate on
T3: PREREG :180 and departure 3), and its V3L pairs are VALID with the verdicts re-derived from their own arms
(items 6, review L5; a VOID V3L voids the block, PREREG line 180). Each batch's rc, timing and D0 controls, voids,
frame-arm p50, D0 ratio, flush_sent_to_device and floor_kind, append25's and the frame arm's drift, and V3L's
pooled p50, ratio and drift are copied in (published, not gates). Every measured run carries its own block's pooled
V3L p50 as its normaliser; a null one fails (third lane review LOW 11). A measured run with no settle.txt, or whose
settle (taken before it started) did not go quiet, fails (unquiet_runs; fourth lane review MED 5).
Exit 1 if any planned run is not complete, any block is not OK or never ran (fslist.txt names every block), a
stage failed, a parity check failed, a run is unsettled, or nothing was planned: a run that collected nothing has
not passed.
PREREG citations as ':N' or 'line N' are lines of artie frontier/fastest/PREREG-v1-FINAL-CANDIDATE.md, the text the
rulings cite, until PREREG-v1.md is registered (fourth lane review LOW 26).
"""
import glob
import hashlib
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import blockgate  # noqa: E402
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


def v3_batch(fsdir, when, rc, block=None, plp="no"):
    d = os.path.join(fsdir, f"v3-{when}")
    rec = {"rc": rc, "summary": os.path.exists(os.path.join(d, "summary.json")),
           "raw": os.path.exists(os.path.join(d, "raw.tsv"))}
    if rec["summary"]:
        try:
            s = json.load(open(os.path.join(d, "summary.json")))
            fa = s.get("frame_arm")
            rec.update({"flush_control": s.get("flush_control"), "timing_control": s.get("timing_control"),
                        "d0_control": s.get("d0_control"), "voids": (s.get("flush_gate") or {}).get("voids"),
                        "append25_p50_us": (s.get("arms", {}).get("append25") or {}).get("p50_us"), "frame_arm": fa,
                        "frame_arm_p50_us": (s.get("arms", {}).get(fa) or {}).get("p50_us") if fa else None,
                        "flush_d0_p50_ratio": s.get("flush_d0_p50_ratio"),
                        "flush_sent_to_device": s.get("flush_sent_to_device"), "floor_kind": s.get("floor_kind"),
                        "flush_gate": (s.get("flush_gate") or {}).get("outcome"),
                        "leaf": (s.get("leaf") or {}).get("disk"), "leaf_kind": (s.get("leaf") or {}).get("kind"),
                        "virtualization": s.get("virtualization")})
        except (OSError, ValueError, AttributeError) as e:
            rec["summary_error"] = f"{type(e).__name__}: {e}"
    b = os.path.join(d, "binary.txt")
    if os.path.exists(b):
        rec["bound"] = next((l[6:] for l in open(b).read().splitlines() if l.startswith("bound=")), None)
    t = os.path.join(fsdir, f"v3-{when}.txt")
    rec["run_sh_tail"] = open(t, errors="replace").read().strip().splitlines()[-1:] if os.path.exists(t) else []
    sj, raw = blockgate.load(d)
    g = blockgate.decide(sj, raw, rc, block or "loop", plp)
    rec["blockgate"] = g
    rec["ok"] = g["decision"] == "PASS" and "summary_error" not in rec
    return rec


def block_record(out, fs, plp="no"):
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
        r = v3_batch(fsdir, when, rcs[when], meta.get("block"), plp)
        rec["v3"][when] = r
        if not r["ok"]:
            rec["why"].append(f"V3 {when}: rc {r['rc']}, summary.json {'present' if r['summary'] else 'MISSING'}, "
                              f"raw.tsv {'present' if r['raw'] else 'MISSING'}; "
                              + "; ".join(r["blockgate"]["reasons"]) + "; run.sh: " + " ".join(r["run_sh_tail"]))
    # The A16 plants: the stored record must exist, and the plants are RE-RUN here on the real BEFORE record (lane
    # review LOW 11: a stored all_fired is not trusted); the two must agree.
    pl = os.path.join(fsdir, "blockgate-plants.json")
    try:
        rec["a16_plants"] = json.load(open(pl))
    except (OSError, ValueError) as e:
        rec["a16_plants"] = None
        if rec["v3"].get("before", {}).get("ok"):
            rec["why"].append(f"A16 plants: no record ({type(e).__name__})")
    if rec["a16_plants"] is not None:
        sj, raw = blockgate.load(os.path.join(fsdir, "v3-before"))
        again, ok_again = blockgate.plants(sj, raw, rcs.get("before", 0), plp, os.path.join(fsdir, "v3-before"))
        fired = {p["plant"]: p["fired"] for p in again}
        rec["a16_plants_rederived"] = fired
        if not ok_again or not rec["a16_plants"].get("all_fired"):
            rec["why"].append(f"A16 plants: not every plant was decided as planted (re-derived {fired})")
    # V3L at every block boundary (gate-6 review 9): b0 before block 1, bK after block K. The plan names the blocks.
    plan = os.path.join(fsdir, "plan.tsv")
    ks = sorted({int(l.split("\t")[6]) for l in open(plan).read().splitlines() if len(l.split("\t")) > 6}) \
        if os.path.exists(plan) else []
    nb = max(ks) if ks else 1  # no plan (the block failed before planning): the first block's pair is still expected
    bounds = [f"b{k}" for k in range(0, nb + 1)]
    # V3L's plants likewise, on every measurement that exists
    for when in bounds:
        j = os.path.join(fsdir, f"v3l-{when}", "v3l.json")
        if not os.path.exists(j):
            continue
        try:
            stored = json.load(open(os.path.join(fsdir, f"v3l-{when}-plants.json")))
        except (OSError, ValueError):
            stored = None
        # loaded once; an unreadable measurement fails the block with its reason (T3 runner review LOW 22)
        try:
            meas = json.load(open(j))
        except (OSError, ValueError) as e:
            rec["why"].append(f"V3L {when}: v3l-{when}/v3l.json unreadable ({type(e).__name__})")
            continue
        try:
            again, ok_again = v3l.plants(meas)
        except (ValueError, KeyError, TypeError, AttributeError):
            again, ok_again = [], False
        rec.setdefault("v3l_plants", {})[when] = {p["plant"]: p["fired"] for p in again}
        if stored is None or not stored.get("all_fired") or not ok_again:
            if not isinstance(meas, dict) or meas.get("verdict") == "VALID":  # a VOID measurement already fails it
                rec["why"].append(f"V3L {when} plants: stored {None if stored is None else stored.get('all_fired')}, "
                                  f"re-derived {rec['v3l_plants'][when]}")
    b, a = rec["v3"]["before"].get("frame_arm_p50_us"), rec["v3"]["after"].get("frame_arm_p50_us")
    rec["v3_frame_arm_drift_us"] = round(a - b, 2) if a is not None and b is not None else None
    b, a = rec["v3"]["before"].get("append25_p50_us"), rec["v3"]["after"].get("append25_p50_us")
    rec["v3_append25_drift_us"] = round(a - b, 2) if a is not None and b is not None else None
    # batchgate.py drift's own record (third lane review MED 2): taken whenever both batches passed; REFUSED fails
    # the block, VOID is published (T3's drift is descriptive: PREREG :180, departure 3 :553, V3 review 2 item 6)
    try:
        drc = int(open(os.path.join(fsdir, "v3-drift.rc")).read().strip())
        dj = json.load(open(os.path.join(fsdir, "v3-drift.json")))
    except (OSError, ValueError):
        drc, dj = None, None
    rec["v3_drift"] = {"rc": drc, "outcome": (dj or {}).get("outcome"), "arms": (dj or {}).get("arms"),
                       "why": (dj or {}).get("why"), "voids": (dj or {}).get("voids"), "gate": "published, not a gate on T3"}
    if rec["v3"]["before"].get("ok") and rec["v3"]["after"].get("ok"):
        mine = ((dj or {}).get("arms") or {}).get("append25", {}).get("drift_us")
        if dj is None or drc not in (0, 3):
            rec["why"].append(f"V3 drift: {'no record' if dj is None else 'REFUSED'} (rc {drc}): {(dj or {}).get('why')}")
        elif mine is None or rec["v3_append25_drift_us"] is None or abs(mine - rec["v3_append25_drift_us"]) > 0.1:
            rec["why"].append(f"V3 drift: append25 {mine} in v3-drift.json, {rec['v3_append25_drift_us']} from the "
                              "batches' own summaries")
    pairs = []
    for k in range(1, nb + 1):
        pr = v3l.block(os.path.join(fsdir, f"v3l-b{k - 1}", "v3l.json"), os.path.join(fsdir, f"v3l-b{k}", "v3l.json"))
        pr["k"] = k
        pairs.append(pr)
        if pr["verdict"] != "VALID":
            reasons = [x for side in ("before", "after") for x in (pr[side].get("void_reasons") or
                                                                    ([pr[side]["why"]] if "why" in pr[side] else []))]
            rec["why"].append(f"V3L {pr['verdict']} (block {k}): " + "; ".join(reasons))
    verdicts = [pr["verdict"] for pr in pairs]
    agg = "VOID" if "VOID" in verdicts else "MISSING" if ("MISSING" in verdicts or not pairs) else "VALID"
    rec["v3l"] = {"verdict": agg, "blocks": pairs, "before": pairs[0]["before"] if pairs else None,
                  "after": pairs[-1]["after"] if pairs else None,
                  "published": pairs[0].get("published") if pairs else None}
    # a run's normaliser comes from a VALID pair only: a VOID block's pooled p50 normalises nothing
    rec["v3l_pooled_by_block"] = {pr["k"]: (pr.get("published") or {}).get("pooled_fsync_p50_us")
                                  if pr["verdict"] == "VALID" else None for pr in pairs}
    rec["ok"] = not rec["why"]
    return rec


def run_parity(d, system):
    """What a measured run itself recorded of its warm-up rule and measured op total (ours: result/summary.json; a
    competitor: every timed run under result/cells/*/timed/summary.json, the only latency files)."""
    if system == "ours":
        try:
            j = json.load(open(os.path.join(d, "result", "summary.json")))
        except (OSError, ValueError):
            j = {}
        return {"warmup_rules": [j.get("warmup_rule")], "ops_measured": [j.get("ops_total")],
                "ops_total_asked": j.get("ops_total_asked")}
    # A competitor's timed run is judged by timedrun.py check (its timed.json: verdict ok, ops = the N it ran for),
    # the validated rule that also accepts a run ended by the registered cap with >= 1000 ok ops (PREREG :212,
    # "completed with reduced n"; fourth lane review MED 4); summarize compares the warm-up rule and N to the plan.
    rules, ops, verdicts, reduced, seen = [], [], [], {}, set()
    for t in sorted(glob.glob(os.path.join(d, "result", "cells", "*", "timed", "summary.json"))):
        cell = os.path.basename(os.path.dirname(os.path.dirname(t)))
        try:
            j = json.load(open(t))
        except (OSError, ValueError):
            j = {}
        try:
            tj = json.load(open(os.path.join(os.path.dirname(os.path.dirname(t)), "timed.json")))
        except (OSError, ValueError):
            tj = None
        rules.append(j.get("warmup_rule"))
        ops.append((tj or {}).get("ops"))
        seen.add(cell)
        verdicts.append((cell, "no timed.json" if tj is None else tj.get("verdict")))
        if j.get("capped") and isinstance(j.get("measured_ops"), int):
            reduced[cell] = j["measured_ops"]
    # every cell the run said it would produce has its timed run (fourth lane review LOW 24): run_system.sh writes
    # result/expected-cells.txt before any cell runs, so a cell that returned early cannot drop out silently
    try:
        expected = [x for x in open(os.path.join(d, "result", "expected-cells.txt")).read().split() if x]
        missing = [f"timed run {x}: missing (listed in expected-cells.txt)" for x in expected if x not in seen]
        if not expected:
            missing = ["expected-cells.txt lists no cell"]
    except OSError:
        missing = ["no expected-cells.txt: the cells this run had to produce are unknown"]
    return {"warmup_rules": rules, "ops_measured": ops, "timed_verdicts": verdicts, "reduced_n": reduced or None,
            "expected_missing": missing}


def fixture_check(d, system, plan, dry):
    """One parent fixture for every system (gate-6 t3run HIGH 4; comp's gen_seed parent aged by FT_AGE, with
    FT_PREBRANCH live branches) and the package's mode (fourth lane review LOW 15): what the run itself recorded must
    equal its plan row's age and live columns, and a competitor's dry= its package's. Returns the refusals."""
    bad = []
    if plan.get("age") is None or plan.get("live") is None:
        return ["the plan row has no age/live columns"]
    if system == "ours":
        try:
            fx = json.load(open(os.path.join(d, "result", "summary.json"))).get("fixture")
        except (OSError, ValueError):
            fx = None
        if not isinstance(fx, dict):
            return ["ours records no fixture record (fastest_profile does not yet build the plan's parent: gen_seed "
                    "rows, the age's single-row UPDATEs, the live branches)"]
        got = {"age": fx.get("age"), "live": fx.get("live")}
    else:
        try:
            line = open(os.path.join(d, "result", "run-info.txt")).readline()
        except OSError:
            return ["no run-info.txt (the competitor's fixture and mode are unknown)"]
        kv = dict(t.split("=", 1) for t in line.split() if "=" in t)
        if kv.get("dry") != str(dry):
            bad.append(f"run-info dry={kv.get('dry')!r}, the package's {dry!r}")
        got = {"age": int(kv["age"]) if kv.get("age", "").isdigit() else kv.get("age"),
               "live": int(kv["prebranch"]) if kv.get("prebranch", "").isdigit() else kv.get("prebranch")}
    for k in ("age", "live"):
        if got[k] != plan[k]:
            bad.append(f"fixture {k} {got[k]!r}, planned {plan[k]!r}")
    return bad


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
    planned, prow = [], {}
    for plan in sorted(glob.glob(os.path.join(out, "fs-*", "plan.tsv"))):
        fs = os.path.basename(os.path.dirname(plan))[3:]
        for line in open(plan).read().splitlines():
            f = line.split("\t")
            cell, system = f[:2]
            planned.append((fs, cell, system))
            num = lambda k: int(f[k]) if len(f) > k and f[k].isdigit() else None  # noqa: E731
            prow[(fs, cell)] = {"clients": f[2] if len(f) > 2 else None, "ops": num(3), "block_k": num(6),
                                "age": num(7), "live": num(8)}
    wt = os.path.join(out, "warmup.txt")
    wtext = open(wt).read().split() if os.path.exists(wt) else []
    rule = wtext[2] if len(wtext) > 2 and wtext[:2] == ["warm-up", "rule"] else None
    runs, incomplete, failed_checks, parity, fixture = [], [], [], [], []
    for fs, cell, system in planned:
        a = attempts.get((fs, cell), [])
        last = a[-1] if a else None
        d = os.path.join(out, f"fs-{fs}", "cells", cell, f"a{last['attempt']}") if last else None
        result, why, not_available = False, "no attempt recorded", False
        if last:
            adapter = open(os.path.join(d, "adapter.txt"), errors="replace").read() if os.path.exists(os.path.join(d, "adapter.txt")) else ""
            if last["adapter_rc"] == 4 and any(l.startswith("NOT AVAILABLE") for l in adapter.splitlines()):
                result, why, not_available = True, "NOT AVAILABLE (class absent at this sha)", True
            elif last["void"] != "VALID":
                why = "last attempt VOID"
            elif system == "ours":
                result = os.path.exists(os.path.join(d, "result", "summary.json")) and last["adapter_rc"] == 0
                why = "ok" if result else f"no result (adapter rc {last['adapter_rc']})"
            else:
                f = os.path.join(d, "result", "functional.txt")
                text = open(f).read() if os.path.exists(f) else ""
                # the verdict is the LAST 'VERDICT ' line (T3 runner review LOW 21). run_system.sh exits 0 on PASS and
                # 1 on FAIL, so a FAIL arrives at rc 1: complete raws of a failed check, its FAIL lines kept (fourth
                # lane review LOW 21); any other rc (a kill, a refusal) is an incomplete run (LOW 12)
                vl = [l.strip() for l in text.splitlines() if l.startswith("VERDICT ")]
                verdict = vl[-1] if vl else None
                rc_ = last["adapter_rc"]
                if verdict == "VERDICT PASS":
                    result = rc_ == 0
                elif verdict is not None:
                    result = rc_ in (0, 1)
                else:
                    result = False
                why = verdict if result else f"no functional VERDICT (adapter rc {rc_})" if verdict is None else \
                    f"functional {verdict} written but the adapter exited {rc_}"
                if result and verdict != "VERDICT PASS":
                    failed_checks.append(f"{fs}/{cell}: " + "; ".join(
                        l for l in text.splitlines() if l.startswith("FAIL ")))
        st = os.path.join(d, "settle.txt") if d else None
        settle = open(st).read().strip() if st and os.path.exists(st) else None
        measured = result and not not_available  # a flag, not the note's text (T3 runner review LOW 23)
        r = {"fs": fs, "cell": cell, "system": system, "attempts": a, "complete": result, "note": why,
             "measured": measured, "settle": settle, "plan": prow.get((fs, cell))}
        if measured:
            r.update(run_parity(d, system))
            want = (prow.get((fs, cell)) or {}).get("ops")
            # timedrun.py's own verdict first: it names why a competitor's timed run cannot stand
            for tcell, v in r.get("timed_verdicts") or []:
                if v != "ok":
                    what = v if v == "no timed.json" else f"timedrun verdict {v!r}"
                    parity.append(f"{fs}/{cell}: timed run {tcell}: {what}")
            for x in r["warmup_rules"] or [None]:
                if x is None or x != rule:
                    parity.append(f"{fs}/{cell}: warm-up rule {x!r}, the package's {rule!r}")
            for x in r["ops_measured"] or [None]:
                if x is None or x != want:
                    parity.append(f"{fs}/{cell}: measured {x!r} ops, planned {want!r}")
            if system == "ours" and r.get("ops_total_asked") != want:
                parity.append(f"{fs}/{cell}: ops_total_asked {r.get('ops_total_asked')!r}, planned {want!r}")
            for x in r.get("expected_missing") or []:
                parity.append(f"{fs}/{cell}: {x}")
            fixture += [f"{fs}/{cell}: {x}" for x in fixture_check(d, system, prow.get((fs, cell)) or {}, dry)]
        runs.append(r)
        if not result:
            incomplete.append(f"{fs}/{cell}: {why}")
    # No cross-system equal-total rule (fourth lane review HIGH 2; the PREREG sizes n_run per system from that
    # system's own t-hat, overriding gate-6 item 12): each run is held to its own plan row above, nothing more.
    fl = os.path.join(out, "fslist.txt")
    fslist = open(fl).read().split() if os.path.exists(fl) else []
    plp = "no"
    mt0 = os.path.join(out, "mode.txt")
    if os.path.exists(mt0):
        plp = dict(l.split("=", 1) for l in open(mt0).read().splitlines() if "=" in l).get("plp") or "no"
    blocks = [block_record(out, fs, plp) for fs in fslist]
    # each run is normalised by its own block's pooled V3L p50 (PREREG line 180; gate-6 review 9)
    pooled = {b["fs"]: b.get("v3l_pooled_by_block") or {} for b in blocks}
    normaliser = []
    for r in runs:
        r["block_k"] = (r.get("plan") or {}).get("block_k")
        r["v3l_pooled_fsync_p50_us"] = pooled.get(r["fs"], {}).get(r["block_k"])
        if r["measured"] and r["v3l_pooled_fsync_p50_us"] is None:  # third lane review LOW 11
            normaliser.append(f"{r['fs']}/{r['cell']}: no pooled V3L p50 for its block {r['block_k']}")
    # every measured run settled before it started, and went quiet (fourth lane review MED 5; T3 runner review item 9)
    unquiet = [f"{r['fs']}/{r['cell']}: " + (r["settle"] or "no settle.txt") for r in runs
               if r["measured"] and (not r["settle"] or "quiet=yes" not in r["settle"].split())]
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
               "failed_checks": failed_checks, "failed_blocks": failed_blocks, "warmup_rule": rule,
               "parity_refusals": parity, "fixture_refusals": fixture, "normaliser_missing": normaliser,
               "unquiet_runs": unquiet,
               "failed_stages": failed, "blocks": blocks, "runs": runs}
    # lane review LOW 7 and 10: a failed competitor check and a package that measured nothing both fail the run
    summary["measured_runs"] = sum(r["measured"] for r in runs)
    ok = bool(planned) and not incomplete and not failed and not failed_blocks and not failed_checks \
        and summary["measured_runs"] > 0 and not parity and not fixture and not normaliser and not unquiet
    summary["ok"] = ok  # the one verdict the workflow reads (fourth lane review LOW 23)
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
    """Synthetic packages, the V3 batches in the probe's c225124ae shape: one good block, then each way a block or the
    package can be incomplete."""
    def w(path, text):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w").write(text)

    N = v3l.N
    RULE = "1000:10:180"

    def v3lj(verdict="VALID", lie=False, bins=(10000, 5), block="loop"):
        """A full V3L record: VALID arms, or VOID arms (5,000 fsyncs, as the fsync-half plant); lie=True records VALID
        over VOID arms (review L5). A brd block's record is a ram disk's, as the real one is (ram0, write through, the
        probe's 'none (RAM)', no flush counted: fourth lane review HIGH 3's second half); any other is a write-back
        nvme0n1."""
        fs = N if verdict == "VALID" and not lie else N // 2
        brd = block == "brd"

        def arm(fsyncs, syncs):
            return {"v1l": {"data_writes": N, "data_fsyncs": fsyncs, "other_fsyncs": 0, "failed": 0, "io_uring_setup": 0,
                            "fdatasync": 0, "sync_file_range": 0, "syncfs": 0, "msync": 0},
                    "labelling_fio": {"writes": N, "syncs": syncs},
                    "timed": {"writes": N, "syncs": syncs, "fsync_p50_us": bins[0] / 1e3 if syncs else None,
                              "fsync_bins_ns": {str(bins[0]): bins[1]} if syncs else None,
                              "flush_ios_delta": 0 if brd else 2 * N, **({"lab_flush_ios_delta": 0} if brd else {}),
                              # what v3l.py measure() records since T3 item 10's remainder (data + metadata)
                              "sectors_written_delta": 2 * N * 8, "lab_sectors_written_delta": 2 * N * 8}}
        r = {"verdict": "VALID" if lie else verdict, "void_reasons": [] if verdict == "VALID" or lie else ["planted"],
             "floor_kind": "x", "leaf": {"disk": "ram0" if brd else "nvme0n1",
                                         "write_cache": "write through" if brd else "write back",
                                         "drive_reports": "none (RAM)" if brd else "write back", "layers": []},
             "published": {"fsync_p50_us": 10.0, "fsync_over_control_write_p50": 5.0},
             "arms": {"fsync": arm(fs, fs - 1), "control": arm(0, 0)}}
        return json.dumps(r)

    def v3sum(rc, block, plp="no", a25=300.0, void=None):
        """A V3 batch summary as the c225124ae probe and batchgate.post write it; void: 'd0' or 'nosync' (rc 3)."""
        brd = block == "brd"
        tc = "not applicable: brd (no drive)" if brd else "not applicable: PLP" if plp == "yes" else "pass"
        d0 = "pass (nosync25 p50 3.0 us < 50 us)"
        voids = []
        if void == "d0":
            d0 = "FAIL: run void (nosync25 p50 61.0 us >= 50 us: a foreign writer on the device)"
        elif void == "nosync":
            voids = ["fsync: append25's windows without an fsync or fdatasync by the probe: 3 of 10000"]
        return json.dumps({"frame_arm": "append64", "arms": {"append64": {"p50_us": 80.0}, "append25": {"p50_us": a25}},
                           "floor_kind": "x", "timing_control": tc, "flush_control": tc, "d0_control": d0, "plp": plp,
                           "traced": False, "n": 200,  # 200 x the 7 flush-gated arms = required_flushes 1400 (LOW 19)
                           "flush_control_arms": {x: {"gated": True} for x in ("append25", "append64", "ow4k", "ow64k",
                                                                                "ow1m", "cfr2b", "fdatasync4k")},
                           "leaf": {"kind": "brd" if brd else "drive", "disk": "ram0" if brd else "nvme0n1",
                                    "write_cache": "write through" if brd else "write back",
                                    "drive_reports": "none (RAM)" if brd else "write back"},
                           "leaf_write_cache": "write through" if brd else "write back",
                           "flush_gate": {"outcome": "not applicable: brd" if brd else "pass", "required_flushes": 1400,
                                          "leaf_flushes_completed": 0 if brd else 2800, "voids": voids,
                                          "blkflush_leaf_gate": {"outcome": "not applicable" if brd else "pass"}}})

    def make(root, before_rc=0, before_v3l="VALID", after_v3l="VALID", drop=None, fslist="xfs", block="loop",
             plants=True, plants_fired=True, plp="no", lie=False, cell="ok", void=None, drift="pass", a25_after=300.0,
             rule=RULE, ours_rule=RULE, ours_ops=(200, 200), comp_ops=200, adapter_rc=0, k=1, settle="quiet=yes",
             plan_ops=None, plan_cols=9, drift_json_us=None, bound=False, timed=None, age=200, live=20,
             ours_fixture="plan", comp_info="plan", expected=("c1-create", "c1-m1"), verdict_text=None,
             boundary_bins=None, garbage_v3l=None, v3l_plants_record=None):
        out = os.path.join(root, "out")
        w(f"{out}/stages.tsv", "stage\tstart_utc\tend_utc\tseconds\trc\nfs-xfs\ta\tb\t5\t0\nTOTAL\ta\tb\t9\t0\n")
        system = {"ok": "ours", "na": "ours", "compfail": "dolt", "comp": "dolt"}[cell]
        cells = [f"{system}-full-c1-r{i}" for i in range(1, k + 1)]
        rows = []
        for i, c in enumerate(cells, 1):
            rc_, v_ = ("4", "N/A") if cell == "na" else (str(adapter_rc), "VALID")
            rows.append(f"xfs\t{c}\t{system}\t1\t1\t{rc_}\t{v_}")
        w(f"{out}/cells.tsv", "fs\tcell\tsystem\tclients\tattempt\tadapter_rc\tvoid\n" + "\n".join(rows) + "\n")
        w(f"{out}/fslist.txt", fslist + "\n")
        w(f"{out}/mode.txt", f"dry=1\nblock={block}\nplant=\nplp={plp}\n")
        if rule is not None:
            w(f"{out}/warmup.txt", f"warm-up rule {rule} (cap 1800s)\n")
        f = f"{out}/fs-xfs"
        ops = plan_ops or [200] * k
        w(f"{f}/plan.tsv", "".join("\t".join([c, system, "1", str(ops[i]), "1", "full", str(i + 1), str(age),
                                             str(live)][:plan_cols]) + "\n" for i, c in enumerate(cells)))
        for c in cells:
            a1 = f"{f}/cells/{c}/a1"
            if settle is not None:
                w(f"{a1}/settle.txt", f"dev=loop0 settle_s=2.2 {settle} inflight=0 dirty_kb=10 writes/discards/flushes=1/0/1\n")
            if system == "ours":
                fx = {"age": age, "live": live} if ours_fixture == "plan" else ours_fixture
                w(f"{a1}/result/summary.json", json.dumps({"warmup_rule": ours_rule, "ops_total": ours_ops[0],
                                                           "ops_total_asked": ours_ops[1]}
                                                          | ({"fixture": fx} if fx is not None else {})))
                w(f"{a1}/adapter.txt", "NOT AVAILABLE: no async class\n" if cell == "na" else "")
            else:
                verdict = verdict_text or ("FAIL F1: something\nVERDICT FAIL\n" if cell == "compfail"
                                           else "PASS F1\nVERDICT PASS\n")
                w(f"{a1}/result/functional.txt", verdict)
                info = f"dry=1 age={age} prebranch={live}" if comp_info == "plan" else comp_info
                if info is not None:
                    w(f"{a1}/result/run-info.txt", f"system=dolt kind=dolt rows=10000 cap_s=1800 warmup={RULE} {info} "
                                                   "parent_sum=1\n")
                if expected is not None:
                    w(f"{a1}/result/expected-cells.txt", "".join(e + "\n" for e in expected))
                for cc in ("c1-create", "c1-m1"):
                    # timed: None (an uncapped run of comp_ops), or (measured_ops, capped, timedrun's verdict)
                    mo, cap, tv = timed if timed else (comp_ops, False, "ok")
                    w(f"{a1}/result/cells/{cc}/timed/summary.json",
                      json.dumps({"warmup_rule": RULE, "measured_ops": mo, "capped": cap, "verdict": "ok", "rc": 0}))
                    if tv is not None:
                        w(f"{a1}/result/cells/{cc}/timed.json", json.dumps({"verdict": tv, "ops": comp_ops}))
                w(f"{a1}/adapter.txt", "")
        w(f"{f}/block.txt", f"cell={'xfs' if block == 'brd' else 'xfsloop'}\nblock={block}\n")
        w(f"{f}/v3.rc", f"before rc={before_rc}\nafter rc=0\n")
        for when in ("before", "after"):
            if not (when == "before" and before_rc == 2):  # a refused batch leaves no out dir
                w(f"{f}/v3-{when}/summary.json", v3sum(before_rc if when == "before" else 0, block, plp,
                                                       300.0 if when == "before" else a25_after,
                                                       void if when == "before" else None))
                w(f"{f}/v3-{when}/raw.tsv", "arm\tus\n")
            w(f"{f}/v3-{when}.txt", "run.sh: REFUSED: planted refusal text\n" if before_rc == 2 and when == "before" else "ok\n")
            if bound and os.path.isdir(f"{f}/v3-{when}"):  # a fire-checked batch: binary.txt binds a passing verdict
                vt = json.dumps({"v3floor_sha256": "e" * 64, "checks": [{"id": "F1b:real-all", "pass": True},
                                                                        {"id": "F1b:real-4k", "pass": True}]})
                w(f"{f}/v3-firecheck/verdict.json", vt)
                w(f"{f}/v3-{when}/binary.txt", f"v3floor_sha256={'e' * 64}\nbound=fire-checked: /runner/fs-xfs/v3-firecheck/"
                                               f"verdict.json\nverdict_sha256={hashlib.sha256(vt.encode()).hexdigest()}\n")
        if drift is not None:
            d = round(a25_after - 300.0, 1) if drift_json_us is None else drift_json_us
            dj = {"outcome": drift, "arms": {"append25": {"start_p50_us": 300.0, "end_p50_us": a25_after, "drift_us": d}},
                  "voids": ["append25 drifted"] if drift == "VOID" else []}
            if drift == "REFUSED":
                dj["why"] = ["plp differs"]
            w(f"{f}/v3-drift.json", json.dumps(dj))
            w(f"{f}/v3-drift.rc", {"pass": "0", "VOID": "3", "REFUSED": "2"}[drift] + "\n")
        if plants:
            sj = json.loads(v3sum(0, block, plp))
            sj["_app_sync"] = blockgate.app_sync_evidence(f"{f}/v3-before")
            res, ok = blockgate.plants(sj, True, 0, plp, f"{f}/v3-before")
            w(f"{f}/blockgate-plants.json", json.dumps({"plants": res, "all_fired": ok and plants_fired}))
        bounds = [f"b{i}" for i in range(k + 1)]
        bb = boundary_bins or [(10000, 5)] * (k + 1)
        w(f"{f}/v3l-b0/v3l.json", v3lj(before_v3l, lie, bb[0], block))
        for i, b in enumerate(bounds[1:], 1):
            if i == 1 and after_v3l is None:
                continue
            w(f"{f}/v3l-{b}/v3l.json", v3lj(after_v3l if i == 1 else "VALID", bins=bb[i], block=block))
        for b in bounds:  # what t3run's v3l() writes after each measurement
            if os.path.exists(f"{f}/v3l-{b}/v3l.json"):
                res, ok = v3l.plants(json.load(open(f"{f}/v3l-{b}/v3l.json")))
                w(f"{f}/v3l-{b}-plants.json", json.dumps({"plants": res, "all_fired": ok}, default=str))
        if v3l_plants_record is not None:  # a stored plants record that passes whatever the measurement is
            w(f"{f}/v3l-b0-plants.json", json.dumps(v3l_plants_record))
        if drop:
            os.unlink(f"{f}/{drop}")
        if garbage_v3l:  # an unreadable measurement, written after its plants record
            w(f"{f}/{garbage_v3l}/v3l.json", "{not json")
        return out

    def first(s, key):
        return (s.get(key) or [""])[0]

    cases = []
    for name, kw, want_ok, extra in [
        ("a complete package with a good block passes", {}, True, None),
        ("item 5 red test: the BEFORE batch refused (rc 2, no out dir) fails the package", {"before_rc": 2}, False, None),
        ("an AFTER batch with rc 0 but no raw.tsv fails the package", {"drop": "v3-after/raw.tsv"}, False, None),
        ("item 6: a VOID V3L after fails the package", {"after_v3l": "VOID"}, False, None),
        ("item 6: a missing V3L after fails the package", {"after_v3l": None}, False, None),
        ("item 6: a VOID V3L before with no after (the run stopped) reads VOID, not MISSING",
         {"before_v3l": "VOID", "after_v3l": None}, False,
         lambda s: s["failed_blocks"][0].startswith("xfs: V3L VOID (block 1): planted")),
        ("a block named in fslist.txt that never ran fails the package", {"fslist": "xfs btrfs"}, False, None),
        ("an empty fslist.txt fails the package", {"fslist": ""}, False, None),
        ("a VOID (rc 3, D0 foreign writer) V3 batch fails a loop block", {"before_rc": 3, "void": "d0"}, False, None),
        ("third lane review HIGH 1 / LOW 8: a VOID (rc 3) V3 batch fails a brd block too (no RECORDED)",
         {"before_rc": 3, "void": "d0", "block": "brd"}, False, None),
        ("a brd block with rc 0 passes", {"block": "brd"}, True, None),
        ("fourth lane review HIGH 3: a brd block's V3L records are ram0's (write through, 'none (RAM)'), and every "
         "plant summarize re-derives on them fires", {"block": "brd"}, True,
         lambda s: s["blocks"][0]["v3l"]["before"]["leaf"]["disk"] == "ram0"
         and s["blocks"][0]["v3l_plants"] and all(all(v.values()) for v in s["blocks"][0]["v3l_plants"].values())),
        ("a refused (rc 2) V3 batch fails a brd block too", {"before_rc": 2, "block": "brd"}, False, None),
        ("A16: PLP declared, rc 0 with the flush counter met passes", {"plp": "yes"}, True, None),
        ("third lane review HIGH 1: PLP declared, a D0 foreign-writer VOID fails", {"plp": "yes", "before_rc": 3, "void": "d0"},
         False, None),
        ("third lane review HIGH 1: PLP declared, a window without a sync (rc 3) fails",
         {"plp": "yes", "before_rc": 3, "void": "nosync"}, False, None),
        ("A14: the plants record missing fails the block", {"plants": False}, False, None),
        ("A14: a plant that did not fire fails the block", {"plants_fired": False}, False, None),
        ("review L5: a VALID recorded over VOID arms fails the block", {"lie": True}, False, None),
        # T3 runner review item 6: with a stored plants record that passes, the block fails through block()'s own
        # re-derivation, on its text (the case above also fails through the plants path, so it could not see L5 go)
        ("item 6: a VALID recorded over VOID arms, stored plants passing, fails on block()'s re-derivation text",
         {"lie": True, "v3l_plants_record": {"plants": [], "all_fired": True}}, False,
         lambda s: "disagrees with the gates re-run" in s["failed_blocks"][0]),
        ("review M2: a refusal's run.sh line reaches the block's why", {"before_rc": 2}, False,
         lambda s: "planted refusal text" in s["failed_blocks"][0]),
        ("lane review LOW 10: a package whose only cell is NOT AVAILABLE measured nothing and fails", {"cell": "na"}, False, None),
        ("lane review LOW 7: a competitor VERDICT FAIL fails the package", {"cell": "compfail"}, False, None),
        ("a competitor run with VERDICT PASS, its timed runs at the rule and the planned ops, passes", {"cell": "comp"}, True,
         None),
        ("third lane review LOW 12: a competitor that wrote VERDICT PASS but exited 124 is not complete",
         {"cell": "comp", "adapter_rc": 124}, False, lambda s: "exited 124" in first(s, "incomplete")),
        ("third lane review MED 2: a drift VOID is published, not a gate (T3)", {"drift": "VOID", "a25_after": 400.0}, True,
         lambda s: s["blocks"][0]["v3_append25_drift_us"] == 100.0 and s["blocks"][0]["v3_drift"]["outcome"] == "VOID"),
        ("third lane review MED 2: a REFUSED drift fails the block", {"drift": "REFUSED"}, False,
         lambda s: "V3 drift: REFUSED" in s["failed_blocks"][0]),
        ("third lane review MED 2: no drift record fails the block", {"drift": None}, False,
         lambda s: "V3 drift: no record" in s["failed_blocks"][0]),
        ("MED 3: ours measured more ops than asked fails (parity)", {"ours_ops": (256, 200)}, False,
         lambda s: "measured 256 ops, planned 200" in first(s, "parity_refusals")),
        ("MED 3: ours asked other than the plan fails (parity)", {"ours_ops": (200, 199)}, False,
         lambda s: "ops_total_asked 199" in first(s, "parity_refusals")),
        ("MED 3: ours recorded another warm-up rule fails (parity)", {"ours_rule": "cycles_per_client:20"}, False,
         lambda s: "warm-up rule 'cycles_per_client:20'" in first(s, "parity_refusals")),
        ("MED 3: a competitor's timed run at other than the planned ops fails (parity)", {"cell": "comp", "comp_ops": 199},
         False, lambda s: "199" in first(s, "parity_refusals")),
        ("MED 4: a competitor run ended by the cap (150 of 200 ops) that timedrun ok'd is complete, with reduced n",
         {"cell": "comp", "timed": (150, True, "ok")}, True,
         lambda s: s["runs"][0].get("reduced_n") == {"c1-create": 150, "c1-m1": 150}),
        ("MED 4: a capped run timedrun refused (fewer than 1000 ok ops) fails",
         {"cell": "comp", "timed": (80, True, "REFUSED: capped with 80 ok ops (< 1000)")}, False,
         lambda s: "timedrun verdict" in first(s, "parity_refusals")),
        ("MED 4: an uncapped short run timedrun refused fails",
         {"cell": "comp", "timed": (150, False, "REFUSED: timed run measured 150 ops, not N=200")}, False,
         lambda s: "timedrun verdict" in first(s, "parity_refusals")),
        ("MED 4: a timed run with no timed.json fails", {"cell": "comp", "timed": (200, False, None)}, False,
         lambda s: "no timed.json" in first(s, "parity_refusals")),
        ("MED 3: no package warm-up rule (warmup.txt) fails (parity)", {"rule": None}, False,
         lambda s: "the package's None" in first(s, "parity_refusals")),
        ("MED 3: ours planned 150 ops, asked and measured 150 passes", {"plan_ops": [150], "ours_ops": (150, 150)}, True, None),
        # TEST EDIT, flagged (fourth lane review MED 5 / T3 runner review item 9, gating chosen by the lane): this case
        # said a run that did not settle is "listed, not gated" (ok True); it now fails the package, still listed
        ("MED 5: a run that did not settle fails the package (listed in unquiet_runs)", {"settle": "quiet=no"}, False,
         lambda s: len(s["unquiet_runs"]) == 1 and "quiet=no" in s["unquiet_runs"][0]),
        ("MED 5: a measured run with no settle.txt fails the package", {"settle": None}, False,
         lambda s: any("no settle.txt" in x for x in s["unquiet_runs"])),
        ("LOW 11: a plan row without its block column leaves the run without a normaliser: fails",
         {"plan_cols": 6}, False, lambda s: "no pooled V3L p50" in first(s, "normaliser_missing") and not s["failed_blocks"]),
        ("MED 2: a drift record that disagrees with the batches' own append25 p50s fails the block",
         {"a25_after": 400.0, "drift_json_us": 0.0}, False, lambda s: "V3 drift: append25 0.0" in s["failed_blocks"][0]),
        ("MED 6: a bound batch's plants are re-derived with its directory: wt-tampered fires",
         {"bound": True}, True, lambda s: s["blocks"][0]["a16_plants_rederived"].get("wt-tampered") is True),
        ("HIGH 4 (fixture parity): ours recording no fixture fails", {"ours_fixture": None}, False,
         lambda s: any("no fixture record" in x for x in s["fixture_refusals"])),
        ("HIGH 4: ours whose fixture differs from its plan row (live 10 vs 20) fails",
         {"ours_fixture": {"age": 200, "live": 10}}, False, lambda s: any("live" in x for x in s["fixture_refusals"])),
        ("HIGH 4: a competitor whose run-info age differs from its plan row fails",
         {"cell": "comp", "comp_info": "dry=1 age=0 prebranch=20"}, False,
         lambda s: any("age" in x for x in s["fixture_refusals"])),
        ("LOW 15: a competitor run-info saying dry=0 inside a dry package fails",
         {"cell": "comp", "comp_info": "dry=0 age=200 prebranch=20"}, False,
         lambda s: any("dry" in x for x in s["fixture_refusals"])),
        ("LOW 15: a competitor with no run-info.txt fails", {"cell": "comp", "comp_info": None}, False,
         lambda s: any("run-info" in x for x in s["fixture_refusals"])),
        ("LOW 24: a competitor cell listed in expected-cells.txt with no timed run fails",
         {"cell": "comp", "expected": ("c1-create", "c1-m1", "c4-create")}, False,
         lambda s: any("c4-create" in x for x in s["parity_refusals"])),
        ("LOW 24: a competitor run with no expected-cells.txt fails", {"cell": "comp", "expected": None}, False,
         lambda s: any("expected-cells.txt" in x for x in s["parity_refusals"])),
        # fourth lane review LOW 21: the real driver's VERDICT FAIL arrives with adapter rc 1; its FAIL lines are kept
        ("LOW 21: a competitor VERDICT FAIL at adapter rc 1 (the real shape) keeps its FAIL lines in failed_checks",
         {"cell": "compfail", "adapter_rc": 1}, False,
         lambda s: any("FAIL F1: something" in x for x in s["failed_checks"])),
        # T3 runner review LOW 21: the verdict is the LAST VERDICT line, not 'VERDICT PASS' anywhere in the file
        ("T3 runner LOW 21: a functional.txt whose last VERDICT line is FAIL fails, despite an earlier VERDICT PASS",
         {"cell": "comp", "verdict_text": "VERDICT PASS\nFAIL F2: later\nVERDICT FAIL\n"}, False,
         lambda s: any("FAIL F2: later" in x for x in s["failed_checks"])),
        # fourth lane review LOW 22: distinct pooled p50s per block (bins hand-derived with the documented rule: the
        # first bin where 2 x cumulative >= total): (10000x5, 15000x6) -> 15000; (15000x6, 25000x7) -> 25000;
        # (25000x7, 40000x8) -> 40000
        ("LOW 22: K=3 with distinct pooled p50s: each run carries exactly its own block's normaliser",
         {"k": 3, "boundary_bins": [(10000, 5), (15000, 6), (25000, 7), (40000, 8)]}, True,
         lambda s: [(r["block_k"], r["v3l_pooled_fsync_p50_us"]) for r in s["runs"]] == [(1, 15.0), (2, 25.0), (3, 40.0)]),
        # fourth lane review LOW 23: summarize states its own verdict in summary.json, so the workflow reads one field
        ("LOW 23: summary.json carries ok, equal to the exit verdict (good package)", {}, True, lambda s: s.get("ok") is True),
        ("LOW 23: summary.json carries ok false on a failing package", {"before_rc": 2}, False,
         lambda s: s.get("ok") is False),
        # T3 runner review LOW 22: an unreadable v3l.json fails its block with a reason, it does not crash summarize
        ("T3 runner LOW 22: an unreadable v3l.json fails the block instead of crashing", {"garbage_v3l": "v3l-b1"}, False,
         lambda s: any("v3l-b1" in x for x in s["failed_blocks"])),
        ("LOW 11: K=3 blocks, all VALID: every run carries its own block's normaliser", {"k": 3}, True,
         lambda s: [r["block_k"] for r in s["runs"]] == [1, 2, 3] and all(r["v3l_pooled_fsync_p50_us"] == 10.0 for r in s["runs"])),
        ("T3 runner item 20: K=3 blocks, all VALID: every run carries its block's V3L verdict", {"k": 3}, True,
         lambda s: [r.get("v3l_block_verdict") for r in s["runs"]] == ["VALID"] * 3),
        ("T3 runner item 20: K=3 with a VOID b1: runs 1 and 2 carry VOID, run 3 VALID", {"k": 3, "after_v3l": "VOID"},
         False, lambda s: [r.get("v3l_block_verdict") for r in s["runs"]] == ["VOID", "VOID", "VALID"]),
        ("LOW 11: K=3 with a VOID b1 fails blocks 1 and 2 and leaves their runs without a normaliser", {"k": 3, "after_v3l": "VOID"},
         False, lambda s: [r["v3l_pooled_fsync_p50_us"] for r in s["runs"]] == [None, None, 10.0]
         and len(s["normaliser_missing"]) == 2 and "V3L VOID (block 1)" in s["failed_blocks"][0]
         and "V3L VOID (block 2)" in s["failed_blocks"][0] and "block 3" not in s["failed_blocks"][0]),
    ]:
        root = tempfile.mkdtemp(prefix="summarize-st-")
        try:
            s, ok = summarize(make(root, **kw), "sha", "1", "m")
            good = ok == want_ok and (extra is None or bool(extra(s)))
            cases.append((name, good, s["failed_blocks"] + s["parity_refusals"] + s["incomplete"]))
        finally:
            shutil.rmtree(root)
    # the cross-system rule needs two systems in one block: ours and a competitor planned other totals
    root = tempfile.mkdtemp(prefix="summarize-st-")
    try:
        out = make(root)
        with open(f"{out}/fs-xfs/plan.tsv", "a") as fh:
            fh.write("dolt-full-c1-r1\tdolt\t1\t300\t1\tfull\t1\n")
        with open(f"{out}/cells.tsv", "a") as fh:
            fh.write("xfs\tdolt-full-c1-r1\tdolt\t1\t1\t0\tVALID\n")
        a1 = f"{out}/fs-xfs/cells/dolt-full-c1-r1/a1"
        w(f"{a1}/result/functional.txt", "VERDICT PASS\n")
        w(f"{a1}/result/cells/c1-create/timed/summary.json", json.dumps({"warmup_rule": RULE, "measured_ops": 300}))
        # the records a competitor run writes (comp b49fb656a+): timedrun's verdict and the cells it had to produce
        w(f"{a1}/result/cells/c1-create/timed.json", json.dumps({"verdict": "ok", "ops": 300}))
        w(f"{a1}/result/expected-cells.txt", "c1-create\n")
        s, ok = summarize(out, "sha", "1", "m")
        # TEST EDIT, flagged (fourth lane review HIGH 2): this case pinned the withdrawn cross-system rule (it
        # required a refusal); per-system n_run means ours at 200 beside a competitor at 300 is NOT a refusal
        cases.append(("HIGH 2: ours (200 ops) beside a competitor (300 ops) in one block at C=1 is no parity refusal",
                      s["parity_refusals"] == [], s["parity_refusals"]))
    finally:
        shutil.rmtree(root)
    # fourth lane review HIGH 2: the repo's own smoke manifest, planned by cells.py, gives no parity refusal (the
    # PREREG sizes n_run per system, so ours at 200 and a competitor at 30 in one block is the registered shape)
    import cells  # noqa: E402
    root = tempfile.mkdtemp(prefix="summarize-st-")
    try:
        out = os.path.join(root, "out")
        man = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cells-smoke.tsv")
        fsl = sorted({r["fs"] for r in cells.load(man)})
        w(f"{out}/fslist.txt", " ".join(fsl) + "\n")
        w(f"{out}/warmup.txt", f"warm-up rule {RULE} (cap 1800s)\n")
        rows = ["fs\tcell\tsystem\tclients\tattempt\tadapter_rc\tvoid"]
        for fs in fsl:
            plan = cells.plan(man, fs, 20261005)
            w(f"{out}/fs-{fs}/plan.tsv", "".join("\t".join(r) + "\n" for r in plan))
            for cell, system, clients, ops in (r[:4] for r in plan):
                rows.append(f"{fs}\t{cell}\t{system}\t{clients}\t1\t0\tVALID")
                a1 = f"{out}/fs-{fs}/cells/{cell}/a1"
                if system == "ours":
                    w(f"{a1}/result/summary.json", json.dumps({"warmup_rule": RULE, "ops_total": int(ops),
                                                               "ops_total_asked": int(ops)}))
                else:
                    w(f"{a1}/result/functional.txt", "VERDICT PASS\n")
                    w(f"{a1}/result/cells/c1/timed/summary.json", json.dumps({"warmup_rule": RULE,
                                                                              "measured_ops": int(ops)}))
                    w(f"{a1}/result/cells/c1/timed.json", json.dumps({"verdict": "ok", "ops": int(ops)}))
                    w(f"{a1}/result/expected-cells.txt", "c1\n")
        w(f"{out}/cells.tsv", "\n".join(rows) + "\n")
        s, _ = summarize(out, "sha", "1", "m")
        cases.append(("HIGH 2: cells-smoke.tsv planned by cells.py gives no parity refusal (n_run is per system)",
                      s["parity_refusals"] == [] and s["planned_runs"] > 0, s["parity_refusals"]))
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
        print(f"SUMMARIZE self-test {'PASS' if ok else 'FAIL'}: {n}" + (f" (why {fb})" if fb and not ok else ""))
    print(f"SUMMARIZE SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["self-test"]:
        sys.exit(self_test())
    sys.exit(main(*sys.argv[1:5]))
