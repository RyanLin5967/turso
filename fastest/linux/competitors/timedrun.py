#!/usr/bin/env python3
"""timedrun.py -- the untraced timed run of a competitor cell (lane fastest-linux-comp; gate-6 review, t3run item 2).

PREREG :173: timed T3 runs carry no tracer. Every cell therefore has TWO runs of the identical bbload/clonebench
command: the traced LABELLING run (CELLDIR/bb/, strace attached: the flush counts, never a latency) and the untraced
TIMED run (CELLDIR/timed/: the only latency file a summary may use). The driver (trace.sh timed_run) sweeps the
TracerPid of every task of the command's process tree and the server's at the start, every 0.05 s and at the end,
appending to CELLDIR/timed.tracer.tsv (tracer_sweep's format), and writes the run's exit status to CELLDIR/timed.rc;
the load generator records its own TracerPid at the window's start and end (summary.json tracerpid_tm0/tm1).

  timedrun.py check CELLDIR N RULE LIVE CAP
                                CELLDIR/timed.json; exit 0 only when the timed run exists, exited 0, measured
                                exactly N ops like the labelling run, the tracer record shows it untraced throughout
                                (tracer_problems: no TracerPid, no gap over GAP_S, sweeps bracketing the window, both
                                roles swept; MED 4), both runs recorded the warm-up RULE AND did it (run_problems,
                                MED 6: the effective warm-up meets OPS and S or ran to MAX_S, the window bound is CAP,
                                capped only with a window at its bound), and the live-branch count held at LIVE: CELLDIR/live.tsv's four
                                counts (label_before, label_after, timed_before, timed_after) all equal LIVE (lead
                                review 62430d8bf..b49fb656a HIGH 1: every create is followed by an untimed delete,
                                so N is the same before and after each run and the same for every cell)
  timedrun.py tracer-check TRACER.TSV SUMMARY.JSON
                                tracer_problems alone (firecheck_strace.sh F15); exit 0 only when it finds none
  timedrun.py ops C N1 N4 [TOTAL]
                                the run's ops total: TOTAL (FT_OPS_TOTAL) for every C when given, else N1 at C=1 and
                                N4 otherwise (gate-6 review, t3run item 12). A run capped by the registered window
                                is judged by tier() alone (MED 5): >= 1000 ok ops complete with reduced n, 100-999
                                complete with p50 only (timed.json p50_only), fewer failed with cause 'cap'
  timedrun.py tier SUMMARY.JSON the tier of one run summary (complete | p50_only | failed)
  timedrun.py rule CAP_S        PREREG :210's warm-up for a run capped at CAP_S seconds, as bbload/clonebench
                                --warmup OPS:S:MAX_S: min(max(1000 ops, 10 s), 10% of the cap)
  timedrun.py real CAP_S WARMUP exit 0 only when a REAL run (run_system.sh FT_DRY=0, the T3 runner) may use this cap
                                and warm-up: the registered cap (REGISTERED_CAP_S) and PREREG :210's rule at it. The
                                CI smoke warm-up cap (1000:10:2) is accepted for smoke runs only (lead ruling, artie
                                DECISIONS 6b0bef481b); anything else prints why and exits 2
  timedrun.py selftest          known-answer fixtures for check; exit 0 only if every verdict is as expected
"""
import json
import os
import sys
import tempfile


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


GAP_S = 0.25  # the longest stretch of the timed run allowed without a tracer sweep (trace.sh sweeps every 0.05 s)


def tracer_problems(path, summary):
    """Why trace.sh's tracer record (timed_run's OUT.tracer.tsv) and the timed run's own summary cannot show the timed
    run untraced ([] = they can; lead review 62430d8bf..b49fb656a MED 4). Refused: no record or no roles header; trees
    that could not be walked (tree=none); any TRACED line or a sweep with a nonzero TracerPid; no start sweep of the
    command's tasks; for a server, no start or no end sweep of its tasks; a gap between sweeps over GAP_S; no sweep at
    or before the measured window's start (tm0) or at or after its end (tm1), so every stretch of the window longer
    than GAP_S holds a sweep and a shorter window is bracketed; and the load generator's own TracerPid at tm0 or tm1
    not recorded as 0."""
    try:
        lines = open(path).read().splitlines()
    except OSError:
        return ["tracer record timed.tracer.tsv missing or unreadable"]
    roles = next((ln.split() for ln in lines if ln.startswith("roles ")), None)
    if roles is None:
        return ["tracer record has no roles header"]
    why = []
    if "tree=children" not in roles:
        why.append(f"the process trees could not be walked ({' '.join(roles[1:])})")
    srv = "srv=none" not in roles
    sweeps, traced = [], []
    for ln in lines:
        p = ln.split()
        if p and p[0] == "SWEEP":
            try:
                cn, cm = (int(x) for x in p[3].split("=", 1)[1].split(":"))
                sn, sm = (int(x) for x in p[4].split("=", 1)[1].split(":"))
                sweeps.append((p[1], float(p[2]), cn, cm, sn, sm))
            except (IndexError, ValueError):
                return why + [f"unreadable sweep line {ln!r}"]
        elif p and p[0] == "TRACED":
            traced.append(ln)
    if not sweeps:
        return why + ["no tracer sweeps"]
    if traced:
        why.append(f"traced during the timed run: {traced[:5]}")
    hot = [s for s in sweeps if s[3] or s[5]]
    if hot:
        why.append(f"a sweep saw a TracerPid: {hot[:3]}")
    st = [s for s in sweeps if s[0] == "start"]
    en = [s for s in sweeps if s[0] == "end"]
    if not st or st[0][2] == 0:
        why.append("no start sweep of the command's tasks")
    if srv and (not st or st[0][4] == 0):
        why.append("no start sweep of the server's tasks")
    if srv and (not en or en[-1][4] == 0):
        why.append("no end sweep of the server's tasks")
    ts = sorted(s[1] for s in sweeps)
    gap = max((b - a for a, b in zip(ts, ts[1:])), default=0.0)
    if gap > GAP_S:
        why.append(f"a {gap:.3f} s stretch with no sweep (over {GAP_S} s): a tracer could have come and gone unseen")
    sm = summary or {}
    t0, t1 = sm.get("tm0_realtime_s"), sm.get("tm1_realtime_s")
    if not isinstance(t0, (int, float)) or not isinstance(t1, (int, float)):
        why.append("the timed run's summary has no measured window (tm0_realtime_s, tm1_realtime_s)")
    else:
        if ts[0] > t0:
            why.append(f"no sweep before the window opened: first {ts[0]:.6f}, tm0 {t0:.6f}")
        if ts[-1] < t1:
            why.append(f"no sweep after the window closed: last {ts[-1]:.6f}, tm1 {t1:.6f}")
    for k in ("tracerpid_tm0", "tracerpid_tm1"):
        if sm.get(k) != 0:
            why.append(f"the load generator's own {k} is {sm.get(k)!r}, not 0")
    return why


def rule(cap_s):
    """PREREG :210: warm-up = min(max(1000 ops, 10 s), 10% of the cap), as "OPS:S:MAX_S" for --warmup."""
    m = float(cap_s) / 10
    # %g, as bbload and clonebench format their recorded warmup_rule from the effective values (MED 7)
    return "1000:10:%s" % (("%d" % m) if m == int(m) else ("%g" % m))


def ops(c, n1, n4, total=""):
    """The run's ops TOTAL (all clients together, as bbload/clonebench --max-ops count them): FT_OPS_TOTAL when set
    (one number for every C and every system), else N1 at C=1 and N4 otherwise; None for a non-count."""
    if total not in ("", None):
        return int(total) if str(total).isdigit() and int(total) > 0 else None
    return int(n1) if int(c) == 1 else int(n4)


# PREREG FINAL-CANDIDATE :214, a run the registered window capped (lead review 62430d8bf..b49fb656a MED 5): the ONE
# owner of these boundaries (bbload and clonebench only report capped, their counts, rc 0 and verdict "capped").
CAP_COMPLETE = 1000  # >= this many ok ops: complete, reduced n
CAP_P50 = 100        # >= this many: complete with p50 only; fewer: failed, cause 'cap'
REGISTERED_CAP_S = 1800  # PREREG's per-run cap (30 min; t3run.sh RUN_CAP_S)


def tier(sm):
    """'complete', 'p50_only' or None (failed: cause 'cap') for a run summary; an uncapped run is 'complete'."""
    if (sm or {}).get("capped") is not True:
        return "complete"
    ok = (sm or {}).get("measured_ok")
    if not isinstance(ok, int):
        return None
    return "complete" if ok >= CAP_COMPLETE else "p50_only" if ok >= CAP_P50 else None


def real_problem(cap_s, warmup):
    """Why a REAL run (run_system.sh FT_DRY=0; the T3 runner) may not use CAP_S and WARMUP; None when it may. The lead's
    ruling (artie DECISIONS 6b0bef481b) accepts the CI smoke warm-up cap for smoke runs only: a real run takes the
    registered cap and PREREG :210's rule at it. A cap equal to its own rule is not enough (rule(20) = 1000:10:2).
    Both compare as EXACT strings: every consumer downstream (run_system.sh's integer OUTER_S, the warmup_rule each run
    records, timedrun.py check) reads the text, so "1.8e3" or "1000:10:180.0" must not pass as equal."""
    why = []
    if cap_s != str(REGISTERED_CAP_S):
        why.append(f"cap {cap_s!r} s is not the registered {str(REGISTERED_CAP_S)!r} s")
    want = rule(REGISTERED_CAP_S)
    if warmup != want:
        why.append(f"warm-up {warmup!r} is not PREREG :210's rule at the registered cap, {want!r}")
    return "; ".join(why) or None


LIVE_KEYS = ("label_before", "label_after", "timed_before", "timed_after")
WARM_SLACK_S = 0.05  # a warm-up may overrun MAX_S by the load generator's 1 ms tick plus scheduling slack


def run_problems(nm, sm, warm_rule, cap):
    """MED 6: what the run DID against what it was told -- its effective warm-up (warmup_ops, warmup_s) must satisfy
    the rule OPS:S:MAX_S ((ops >= OPS and s >= S) or s >= MAX_S, and s <= MAX_S + slack), its window bound must be
    the cap, and a run that says capped must have a window at least its bound."""
    why = []
    try:
        o_r, s_r, m_r = warm_rule.split(":")
        o_r, s_r, m_r = int(o_r), float(s_r), float(m_r)
    except (AttributeError, ValueError):
        return [f"{nm} run: warm-up rule {warm_rule!r} unreadable"]
    wo, ws = sm.get("warmup_ops"), sm.get("warmup_s")
    if not isinstance(wo, int) or isinstance(wo, bool) or not isinstance(ws, (int, float)) or isinstance(ws, bool):
        why.append(f"{nm} run has no effective warm-up record (warmup_ops {wo!r}, warmup_s {ws!r})")
    else:
        if not ((wo >= o_r and ws >= s_r) or (m_r > 0 and ws >= m_r)):
            why.append(f"{nm} run left its warm-up early: {wo} ops in {ws:.3f} s against {warm_rule}")
        if m_r > 0 and ws > m_r + WARM_SLACK_S:
            why.append(f"{nm} run's warm-up overran MAX_S: {ws:.3f} s against {m_r:g} s")
    if cap is not None:
        mw, w = sm.get("max_window_s"), sm.get("window_s")
        try:
            capf = float(cap)
        except (TypeError, ValueError):
            capf = None
        if not isinstance(mw, (int, float)) or capf is None or abs(mw - capf) > 1e-6:
            why.append(f"{nm} run's window bound {mw!r} is not the cap {cap!r}")
        elif sm.get("capped") is True and (not isinstance(w, (int, float)) or w < mw):
            why.append(f"{nm} run says capped but its window {w!r} s is shorter than its bound {mw!r} s")
    return why


def check(celldir, n, warm_rule=None, live=None, cap=None):
    """The reasons CELLDIR's timed run cannot supply a latency ([] = it can)."""
    why = []
    lab = load(os.path.join(celldir, "bb", "summary.json"))

    def short(sm):  # measured fewer than N: complete only when the registered cap ended it, in a tier (MED 5)
        got = (sm or {}).get("measured_ops")
        if (sm or {}).get("capped") is True:
            return None if tier(sm) else (f"capped with {(sm or {}).get('measured_ok')} ok ops (fewer than "
                                          f"{CAP_P50}): failed, cause 'cap'")
        if got == n:
            return None
        return f"measured {got} ops, not N={n}"
    if not lab:
        why.append("no labelling run summary")
    elif short(lab):
        why.append("labelling run " + short(lab))
    t = load(os.path.join(celldir, "timed", "summary.json"))
    if t is None or not os.path.exists(os.path.join(celldir, "timed", "raw.tsv")):
        why.append("no timed run (timed/summary.json and timed/raw.tsv): only the traced labelling run's latency")
    else:
        # "capped" only with capped: true (MED 5: the binaries' contract for a run the window ended)
        want_v = "capped" if t.get("capped") is True else "ok"
        if t.get("verdict") != want_v or t.get("rc") != 0:
            why.append(f"timed run verdict {t.get('verdict')} rc {t.get('rc')} (want {want_v}, 0)")
        if short(t):
            why.append("timed run " + short(t))
    try:
        rc = open(os.path.join(celldir, "timed.rc")).read().strip()
    except OSError:
        rc = "missing"
    if rc != "0":
        why.append(f"timed run exit status {rc}")
    if warm_rule is not None:  # gate-6 review, t3run item 3: the registered rule, the same for both runs
        for nm, sm in (("labelling", lab), ("timed", t)):
            got = (sm or {}).get("warmup_rule")
            if got != warm_rule:
                why.append(f"{nm} run warm-up rule {got!r}, not the registered {warm_rule!r}")
            if sm:  # MED 6: and what each run DID against that rule and the cap
                why += run_problems(nm, sm, warm_rule, cap)
    why += tracer_problems(os.path.join(celldir, "timed.tracer.tsv"), t)  # MED 4
    if live is not None:  # lead review 62430d8bf..b49fb656a HIGH 1: N held at LIVE around both runs
        got = {}
        try:
            for ln in open(os.path.join(celldir, "live.tsv")):
                p = ln.split()
                if len(p) == 2:
                    got[p[0]] = p[1]
        except OSError:
            got = None
        if got is None:
            why.append("no live-branch record live.tsv: N around the runs is unknown")
        else:
            bad = {k: got.get(k) for k in LIVE_KEYS if got.get(k) != str(live)}
            if bad:
                why.append(f"live-branch count not held at {live}: {bad}")
    return why


def final_verdict(why, warm_rule=None, cap=None):
    """MED 7: 'ok' only for a clean cell whose rule is the REGISTERED one at its cap, rule(CAP), compared as an exact
    string; a clean cell warmed up by any other rule (the CI smoke cap) is 'ok-smoke-warmup', never 'ok'."""
    if why:
        return "REFUSED: " + "; ".join(why)
    try:
        registered = rule(cap) if cap is not None else None
    except (TypeError, ValueError):
        registered = None
    return "ok" if registered is not None and warm_rule == registered else "ok-smoke-warmup"


def write_verdict(celldir, n, why, warm_rule=None, cap=None):
    lab = load(os.path.join(celldir, "bb", "summary.json")) or {}
    t = load(os.path.join(celldir, "timed", "summary.json")) or {}
    out = {"verdict": final_verdict(why, warm_rule, cap), "latency_file": "timed/raw.tsv", "warmup_rule": warm_rule,
           "registered_rule": rule(cap) if cap is not None else None,
           "labelling_dir": "bb", "ops_total": n,
           "labelling_measured_ops": lab.get("measured_ops"), "timed_measured_ops": t.get("measured_ops"),
           "capped": {"labelling": lab.get("capped", False), "timed": t.get("capped", False)},
           # MED 5: a capped timed run with 100-999 ok ops is complete with p50 only (PREREG FINAL-CANDIDATE :214)
           "tier": tier(t) if t else None, "p50_only": tier(t) == "p50_only" if t else None}
    try:  # the live-branch counts around both runs, as run_system.sh read them (HIGH 1)
        out["live_branches"] = dict(ln.split() for ln in open(os.path.join(celldir, "live.tsv")) if len(ln.split()) == 2)
    except OSError:
        out["live_branches"] = None
    with open(os.path.join(celldir, "timed.json"), "w") as f:
        json.dump(out, f)
    return out


# ---------------------------------------------------------------- selftest
RULE = "1000:10:180"  # the registered warm-up at a 1800 s cap


LIVE = 21  # PREBRANCH 20 live branches + Dolt's main


def trace_rec(sweeps=None, traced=(), roles="cmd=500 srv=100"):
    """A timed.tracer.tsv in trace.sh's tracer_sweep format (MED 4): one "SWEEP phase t cmd=N:MAX srv=N:MAX" line per
    sweep, a "TRACED phase role pid tid tracerpid" line per traced task. Default: start 100.00, a sweep every 0.05 s
    to 100.40, end 100.45, every task untraced, around a window [100.1, 100.3]."""
    if sweeps is None:
        sweeps = ([("start", 100.0, 3, 0, 12, 0)] + [("mid", round(100.05 + 0.05 * k, 2), 3, 0, 12, 0) for k in range(8)]
                  + [("end", 100.45, 0, 0, 12, 0)])
    out = [f"roles {roles}\n"]
    for ph, t, cn, cm, sn, sm in sweeps:
        out.append(f"SWEEP {ph} {t:.6f} cmd={cn}:{cm} srv={sn}:{sm}\n")
    for tr in traced:
        out.append("TRACED " + " ".join(str(x) for x in tr) + "\n")
    return "".join(out)


CAP = 1800  # the registered per-run cap the fixtures' runs were bounded by
# MED 6: what the binaries record about the warm-up and the window they actually ran (a warm-up that met OPS 1000 and
# S 10 of the rule 1000:10:180; an uncapped 5 s window under the 1800 s cap)
RUN = {"warmup_ops": 1500, "warmup_s": 10.2, "max_window_s": 1800.0, "window_s": 5.0}


def fixture(root, name, n=200, timed=True, rc=0, timed_ops=None, tracer=None, lab_rule=RULE, timed_rule=RULE,
            lab_ops=None, capped=False, live="held", self_tp=(0, 0), window=(100.1, 100.3), lab=True, raw=True,
            rc_file=None, lab_extra=None, timed_extra=None):
    """One cell; lab_extra / timed_extra override single summary fields (MED 6: one defect per case)."""
    d = os.path.join(root, name)
    os.makedirs(os.path.join(d, "bb"))
    if live == "held":
        live = {k: LIVE for k in LIVE_KEYS}
    if live is not None:  # None: no live.tsv at all
        with open(os.path.join(d, "live.tsv"), "w") as f:
            f.write("".join(f"{k}\t{v}\n" for k, v in live.items()))
    lo = n if lab_ops is None else lab_ops
    run = dict(RUN, window_s=1800.0 if capped else RUN["window_s"])
    if lab:
        ls = dict(run, verdict="capped" if capped else "ok", rc=0, measured_ops=lo, measured_ok=lo,
                  warmup_rule=lab_rule, capped=capped)
        ls.update(lab_extra or {})
        with open(os.path.join(d, "bb", "summary.json"), "w") as f:
            json.dump(ls, f)
    if timed:
        os.makedirs(os.path.join(d, "timed"))
        to = n if timed_ops is None else timed_ops
        # MED 5: a run the registered window capped exits 0 with verdict "capped"; timedrun.py alone judges its tier
        sm = dict(run, verdict=("capped" if capped else "ok") if rc == 0 else "fail", rc=rc, measured_ops=to,
                  measured_ok=to, warmup_rule=timed_rule, capped=capped, tm0_realtime_s=window[0],
                  tm1_realtime_s=window[1])
        if self_tp is not None:  # the load generator's own TracerPid at tm0 and tm1 (MED 4)
            sm["tracerpid_tm0"], sm["tracerpid_tm1"] = self_tp
        sm.update(timed_extra or {})
        with open(os.path.join(d, "timed", "summary.json"), "w") as f:
            json.dump(sm, f)
        if raw:
            with open(os.path.join(d, "timed", "raw.tsv"), "w") as f:
                f.write("client\tseq\tphase\tok\tlat_ns\n0\t0\tmeasure\t1\t1000\n")
        with open(os.path.join(d, "timed.rc"), "w") as f:
            f.write(f"{rc if rc_file is None else rc_file}\n")
    if tracer is not None:
        with open(os.path.join(d, "timed.tracer.tsv"), "w") as f:
            f.write(tracer)
    return d


def selftest():
    # MED 4: the tracer record is trace.sh's tracer_sweep format (append-only sweeps every 0.05 s, whole process trees)
    clean = trace_rec()
    sw = [("start", 100.0, 3, 0, 12, 0)] + [("mid", round(100.05 + 0.05 * k, 2), 3, 0, 12, 0) for k in range(8)]
    cases = [  # (name, fixture kwargs, expect ok)
        ("untraced timed run", dict(tracer=clean), True),
        ("a server task traced at the end of the timed run",
         dict(tracer=trace_rec(sw + [("end", 100.45, 0, 0, 12, 4242)], traced=[("end", "srv", 101, 101, 4242)])), False),
        ("a server task traced at its start",
         dict(tracer=trace_rec([("start", 100.0, 3, 0, 12, 77)] + sw[1:] + [("end", 100.45, 0, 0, 12, 0)],
                               traced=[("start", "srv", 100, 100, 77)])), False),
        ("no timed run: only the traced labelling run's latency file", dict(timed=False, tracer=clean), False),
        ("no tracer samples", dict(tracer=""), False),
        ("no sample at the end", dict(tracer=trace_rec(sw)), False),
        ("tracer record missing", dict(tracer=None), False),
        ("a tracer attached mid-run and detached before the end (seen by a mid sweep)",
         dict(tracer=trace_rec(sw[:4] + [("mid", 100.2, 3, 0, 12, 31337)] + sw[5:] + [("end", 100.45, 0, 0, 12, 0)],
                               traced=[("mid", "srv", 104, 107, 31337)])), False),
        ("a TRACED line whose sweep line says 0 (the line alone refuses)",
         dict(tracer=trace_rec(traced=[("mid", "cmd", 500, 501, 9)])), False),
        ("a blind second mid-run (a gap longer than the 0.25 s allowed)",
         dict(tracer=trace_rec(sw[:3] + [("mid", 101.2, 3, 0, 12, 0)] + [("end", 101.25, 0, 0, 12, 0)]),
              window=(100.1, 101.1)), False),
        ("no sweep before the window opened",
         dict(tracer=trace_rec([("start", 100.15, 3, 0, 12, 0)] + sw[4:] + [("end", 100.45, 0, 0, 12, 0)])), False),
        ("no sweep after the window closed",
         dict(tracer=trace_rec(sw[:6] + [("end", 100.28, 0, 0, 12, 0)])), False),
        ("B1: no server role; the command's tree sampled throughout",
         dict(tracer=trace_rec([(p, t, n, m, 0, 0) for p, t, n, m, _, _ in sw] + [("end", 100.45, 0, 0, 0, 0)],
                               roles="cmd=500 srv=none")), True),
        ("a server role whose start sweep found no task",
         dict(tracer=trace_rec([("start", 100.0, 3, 0, 0, 0)] + sw[1:] + [("end", 100.45, 0, 0, 12, 0)])), False),
        ("no roles header", dict(tracer="".join(ln for ln in clean.splitlines(True) if not ln.startswith("roles"))),
         False),
        ("the load generator itself traced at tm0 (its own record)", dict(tracer=clean, self_tp=(77, 0)), False),
        ("no own tracer record in the timed summary", dict(tracer=clean, self_tp=None), False),
        ("timed run failed", dict(rc=3, tracer=clean), False),
        ("timed run measured another N", dict(timed_ops=150, tracer=clean), False),
        # gate-6 review, t3run item 3: one warm-up rule, PREREG :210's, for both runs
        ("timed run warmed up by another rule", dict(tracer=clean, timed_rule="20:0:0"), False),
        ("labelling run warmed up by another rule", dict(tracer=clean, lab_rule="1000:10:60"), False),
        ("no warm-up rule recorded", dict(tracer=clean, lab_rule=None, timed_rule=None), False),
        # gate-6 review, t3run item 16: a run that hit the registered cap with >= 1000 ops is complete with reduced n
        # MED 5: PREREG FINAL-CANDIDATE :214's three tiers for a run the registered window capped, applied here alone
        # (the binaries report capped, rc 0, verdict "capped"): >= 1000 ok ops complete; 100-999 complete with p50
        # only; < 100 failed with cause 'cap'. (The case "a capped run with < 1000 ops" refused 800 ops; under the
        # registered tiers 800 is p50-only, so it is now an accepted case below.)
        # (check_n: the N the cell asked for; LOW 27: it used to be chosen by a substring of the case's name)
        ("both runs capped with >= 1000 ops: complete, reduced n",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=1500, check_n=5000), True),
        ("capped at 1000 ok ops: complete",
         dict(tracer=clean, capped=True, lab_ops=1000, timed_ops=1000, check_n=5000), True),
        ("capped at 999 ok ops: complete, p50 only",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=999, check_n=5000), True),
        ("capped at 800 ok ops: complete, p50 only",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=800, check_n=5000), True),
        ("capped at 100 ok ops: complete, p50 only",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=100, check_n=5000), True),
        ("capped at 99 ok ops: failed, cause cap",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=99, check_n=5000), False),
        ("the labelling run capped at 99 ok ops",
         dict(tracer=clean, capped=True, lab_ops=99, timed_ops=1200, check_n=5000), False),
        ("an uncapped run short of N", dict(tracer=clean, lab_ops=1200, timed_ops=1500, check_n=5000), False),
        # MED 6: one defect per guard, each alone (a mutant that deletes or loosens one guard must turn one case red)
        ("no labelling run summary", dict(tracer=clean, lab=False), False),
        ("the labelling run measured another N", dict(tracer=clean, lab_ops=150), False),
        ("the timed run's raw.tsv missing (summary present)", dict(tracer=clean, raw=False), False),
        ("the timed summary says fail with rc 0", dict(tracer=clean, timed_extra={"verdict": "fail"}), False),
        ("the timed summary says ok with rc 3", dict(tracer=clean, timed_extra={"rc": 3}), False),
        ("the timed run's exit status file says 1", dict(tracer=clean, rc_file=1), False),
        ("capped with 1200 ops but 50 ok (the tier counts ok ops)",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=1200, timed_extra={"measured_ok": 50},
              check_n=5000), False),
        ("a warm-up that left before S and before MAX_S (1000 ops, 1.0 s of 10 s)",
         dict(tracer=clean, timed_extra={"warmup_s": 1.0}), False),
        ("a warm-up that left before OPS and before MAX_S (500 ops, 12 s)",
         dict(tracer=clean, timed_extra={"warmup_ops": 500, "warmup_s": 12.0}), False),
        ("a warm-up ended by MAX_S (500 ops, 180.0 s)",
         dict(tracer=clean, timed_extra={"warmup_ops": 500, "warmup_s": 180.0}), True),
        ("a warm-up past MAX_S plus the slack (181 s)",
         dict(tracer=clean, timed_extra={"warmup_ops": 50000, "warmup_s": 181.0}), False),
        ("the labelling run's warm-up left early", dict(tracer=clean, lab_extra={"warmup_s": 1.0}), False),
        ("a window bound that is not the registered cap", dict(tracer=clean, timed_extra={"max_window_s": 20.0}),
         False),
        ("capped although the window was shorter than its bound",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=1200, timed_extra={"window_s": 300.0},
              check_n=5000), False),
        ("no warm-up record", dict(tracer=clean, timed_extra={"warmup_ops": None, "warmup_s": None}), False),
        # lead review 62430d8bf..b49fb656a HIGH 1: the live-branch count N is held fixed and recorded around both runs
        ("N held at 21 around both runs", dict(tracer=clean), True),
        ("the labelling run at N=20, the timed run at N=1020",
         dict(tracer=clean, live={"label_before": 20, "label_after": 20, "timed_before": 1020, "timed_after": 1020}),
         False),
        ("N grew by the run's creates inside the timed run (no delete)",
         dict(tracer=clean, live={"label_before": LIVE, "label_after": LIVE, "timed_before": LIVE,
                                  "timed_after": LIVE + 200}), False),
        ("no live-branch record", dict(tracer=clean, live=None), False),
        ("a live-branch count missing", dict(tracer=clean, live={"label_before": LIVE, "label_after": LIVE,
                                                                 "timed_before": LIVE}), False),
        ("a live-branch count that is not a count", dict(tracer=clean, live={"label_before": LIVE, "label_after": "x",
                                                                             "timed_before": LIVE, "timed_after": LIVE}),
         False),
    ]
    bad = 0
    with tempfile.TemporaryDirectory() as root:
        for i, (name, kw, want) in enumerate(cases):
            kw = dict(kw)
            n_check = kw.pop("check_n", 200)
            d = fixture(root, f"c{i}", **kw)
            why = check(d, n_check, RULE, LIVE, CAP)
            got = not why
            print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
            bad += got != want
    # gate-6 review, t3run item 12: ops is ONE total per run for every system; FT_OPS_TOTAL overrides N1/N4 for all C
    for args, want in (((1, 200, 300, ""), 200), ((4, 200, 300, ""), 300), ((4, 200, 300, "5000"), 5000),
                       ((1, 200, 300, "5000"), 5000), ((1, 200, 300, "x"), None), ((1, 200, 300, "0"), None)):
        got = ops(*args)
        print(("PASS" if got == want else "FAIL"), f"ops{args} = {got!r}, want {want!r}")
        bad += got != want
        cases.append(None)
    # MED 7: the verdict is computed against the registered rule at the cap, rule(CAP), never the run's own: a clean
    # cell that warmed up by any other rule (the CI smoke 1000:10:2) is 'ok-smoke-warmup', never 'ok'
    for why_in, rule_in, cap_in, want in (([], "1000:10:180", "1800", "ok"), ([], "1000:10:2", "1800", "ok-smoke-warmup"),
                                          ([], "1000:10:6", "60", "ok"), (["x"], "1000:10:180", "1800", "REFUSED: x"),
                                          ([], "1000:10:180.0", "1800", "ok-smoke-warmup")):
        got = final_verdict(why_in, rule_in, cap_in)
        print(("PASS" if got == want else "FAIL"), f"final_verdict({why_in}, {rule_in}, cap {cap_in}) = {got!r}, "
              f"want {want!r}")
        bad += got != want
        cases.append(None)
    # MED 5: the tier of a capped run, at the registered boundaries (one owner: tier())
    for ok_ops, capped, want in ((1000, True, "complete"), (999, True, "p50_only"), (100, True, "p50_only"),
                                 (99, True, None), (0, True, None), (200, False, "complete")):
        got = tier({"capped": capped, "measured_ok": ok_ops, "measured_ops": ok_ops})
        print(("PASS" if got == want else "FAIL"), f"tier(capped={capped}, ok={ok_ops}) = {got!r}, want {want!r}")
        bad += got != want
        cases.append(None)
    for cap, want in ((1800, "1000:10:180"), (60, "1000:10:6"), (3600, "1000:10:360")):
        got = rule(cap)
        print(("PASS" if got == want else "FAIL"), f"rule({cap}) = {got!r}, want {want!r}")
        bad += got != want
        cases.append(None)
    # Lead ruling (artie DECISIONS 6b0bef481b): the CI smoke warm-up cap is for smoke runs only; a REAL run (FT_DRY=0)
    # takes the registered cap and PREREG :210's rule at it, and refuses anything else.
    # Both values are compared as EXACT strings (review of e11a3c993, finding 5: "1.8e3" passed a numeric check and then
    # made run_system.sh's integer OUTER_S 601 s; "1000:10:180.0" was recorded verbatim): so "1800.0" and
    # "1000:10:180.0", accepted at d8fef669b, are refused from here on.
    for cap, warm, want in (("1800", "1000:10:180", True), ("1800", "1000:10:180.0", False),
                            ("1800.0", "1000:10:180", False), ("1.8e3", "1000:10:180", False),
                            ("18e2", "1000:10:180", False), ("1_800", "1000:10:180", False),
                            (" 1800", "1000:10:180", False), ("+1800", "1000:10:180", False),
                            ("1800", "01000:010:0180", False), ("1800", " 1000:10:180", False),
                            # the cap ALONE (finding 2): the registered warm-up at a non-registered cap
                            ("3600", "1000:10:180", False), ("20", "1000:10:180", False),
                            ("1800", "1000:10:2", False),     # the CI smoke cap on a real run: the ruling's plant
                            ("20", "1000:10:2", False),       # = rule(20), but 20 s is not the registered cap
                            ("3600", "1000:10:360", False),   # = rule(3600), but not the registered cap
                            ("1800", "20:0:0", False), ("1800", "1000:5:180", False), ("1800", "", False),
                            ("1800", "1000:10", False), ("1800", "1000:10:180:1", False), ("1800", "x:10:180", False),
                            ("", "1000:10:180", False), ("nan", "1000:10:180", False), ("inf", "1000:10:180", False)):
        why = real_problem(cap, warm)
        got = why is None
        print(("PASS" if got == want else "FAIL"), f"real run cap={cap!r} warmup={warm!r} ->",
              "accepted" if got else f"refused: {why}")
        bad += got != want
        cases.append(None)
    print(f"timedrun selftest: {len(cases) - bad}/{len(cases)}")
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) == 7 and sys.argv[1] == "check":
        n = int(sys.argv[3])
        why = check(sys.argv[2], n, sys.argv[4], sys.argv[5], sys.argv[6])
        print(json.dumps(write_verdict(sys.argv[2], n, why, sys.argv[4], sys.argv[6])))
        sys.exit(0 if not why else 1)  # ok and ok-smoke-warmup both exit 0; reduce.py tells them apart
    if len(sys.argv) == 3 and sys.argv[1] == "tier":  # SUMMARY.JSON: run_system.sh's cap plants (MED 5)
        print(tier(load(sys.argv[2])) or "failed")
        sys.exit(0)
    if len(sys.argv) == 4 and sys.argv[1] == "tracer-check":  # TRACER.TSV SUMMARY.JSON: the fire-check's F15
        why = tracer_problems(sys.argv[2], load(sys.argv[3]))
        print("ok" if not why else "REFUSED: " + "; ".join(why))
        sys.exit(0 if not why else 1)
    if len(sys.argv) in (5, 6) and sys.argv[1] == "ops":
        v = ops(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5] if len(sys.argv) == 6 else "")
        if v is None:
            sys.exit("timedrun.py ops: FT_OPS_TOTAL is not a positive count")
        print(v)
        sys.exit(0)
    if len(sys.argv) == 3 and sys.argv[1] == "rule":
        print(rule(sys.argv[2]))
        sys.exit(0)
    if len(sys.argv) == 4 and sys.argv[1] == "real":
        why = real_problem(sys.argv[2], sys.argv[3])
        print("accepted: the registered cap and warm-up" if why is None else "REFUSED: " + why)
        sys.exit(0 if why is None else 2)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
