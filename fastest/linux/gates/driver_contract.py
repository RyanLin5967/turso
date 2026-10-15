#!/usr/bin/env python3
"""driver_contract.py -- what fastest_profile's summary.json must say about its own run (fourth lane review MED 6).

  driver_contract.py check SUMMARY.json T C RULE [BY_CLIENT]   exit 0 iff the record keeps its contract:
        ops_total == ops_total_asked == T, ops_by_client sums to T and equals the exact split (T / C, plus one for the
        clients whose id is below T % C) or BY_CLIENT (a comma list) when given, ops_per_client null unless the split
        is even, warmup_per_client null, warmup_rule == RULE, and the warm-up record keeps RULE (OPS:S:MAX_S):
          - stop_secs >= min(S, MAX_S) (the stop is decided at a claim, by bbload's rule; no early stop),
          - stop_secs < MAX_S + SLACK (never past MAX_S by more than the cycles in flight and a scheduling delay),
          - stop_secs < MAX_S implies ops_at_stop >= OPS (it stopped early only because both minimums held),
          - capped is a bool (bbload's warmup_cap_applied); ops_at_stop < OPS implies capped, and capped implies
            stop_secs >= MAX_S (fourth lane review LOW 14: bbload's OPS:S:MAX_S rule is the definition); not capped
            implies stop_secs >= S, and OPS and S both met implies not capped (review 5 LOW 15),
          - drain_secs >= 0, secs == stop_secs + drain_secs (to print precision), ops == ops_at_stop (A23 counts
            claims, so nothing is counted after the claim that ended the warm-up; review 5 LOW 15).
  driver_contract.py tie SUMMARY.json WARMUP-LIVE.json          the live warm-up record is that run's own warm-up
                                                                (tie()); exit 0 kept, 1 broken, 2 unreadable
  driver_contract.py self-test                                  the checker on synthetic records; exit 0 iff all pass

The fastest-gates release job runs the built driver with rules whose outcome is forced (1000000:0:2 must run to
MAX_S; 10:0:30 must stop on its ops long before MAX_S) and checks each record here, so a driver that ignores OPS,
swaps S and MAX_S, stops at once, or splits T into T+1 ops fails the job, not only a later T3 package.
"""
import json
import sys

# Database::branch_exception_counters(), in its order (the lead's routing ruling, DECISIONS 2026-10-15T05:16:51Z)
EXC = ("hardcap_upgrades", "header_raise_exclusive", "inline_compaction_syncs", "compaction_waits")
SLACK = 0.5  # seconds past MAX_S a stop may land: the next claim after a cycle in flight, plus a loaded runner's delay


def split(t, c):
    return [t // c + (1 if i < t % c else 0) for i in range(c)]


def check(j, t, c, rule, by_client=None):
    bad = []
    want = by_client if by_client is not None else split(t, c)
    if j.get("ops_total") != t or j.get("ops_total_asked") != t:
        bad.append(f"ops_total {j.get('ops_total')!r}, asked {j.get('ops_total_asked')!r}, want {t}")
    if j.get("ops_by_client") != want or sum(j.get("ops_by_client") or []) != t:
        bad.append(f"ops_by_client {j.get('ops_by_client')!r}, want {want}")
    even = len(set(want)) == 1
    if j.get("ops_per_client") != (want[0] if even else None):
        bad.append(f"ops_per_client {j.get('ops_per_client')!r}, want {want[0] if even else None}")
    if j.get("warmup_per_client") is not None:
        bad.append(f"warmup_per_client {j.get('warmup_per_client')!r} under a rule, want null")
    if j.get("warmup_rule") != rule:
        bad.append(f"warmup_rule {j.get('warmup_rule')!r}, want {rule!r}")
    w = j.get("warmup") or {}
    try:
        ops, s, m = rule.split(":")
        ops, s, m = int(ops), float(s), float(m)
    except ValueError:
        return bad + [f"the rule {rule!r} is not OPS:S:MAX_S"]
    if w.get("rule") != rule:
        bad.append(f"warmup.rule {w.get('rule')!r}, want {rule!r}")
    st, dr, se, oa, ot = (w.get(k) for k in ("stop_secs", "drain_secs", "secs", "ops_at_stop", "ops"))
    if not all(isinstance(x, (int, float)) for x in (st, dr, se, oa, ot)):
        return bad + [f"the warm-up record is incomplete: {w!r}"]
    if st < min(s, m) - 1e-3:
        bad.append(f"stopped at {st} s, before min(S, MAX_S) = {min(s, m)}")
    if st >= m + SLACK:
        bad.append(f"stopped at {st} s, past MAX_S {m} by more than {SLACK} s")
    if st < m and oa < ops:
        bad.append(f"stopped at {st} s, before MAX_S {m}, with {oa} ops < OPS {ops}")
    if dr < 0 or abs(se - (st + dr)) > 2e-3:
        bad.append(f"drain {dr}, secs {se} vs stop + drain {st + dr}")
    # review 5 LOW 15: under A23 the warm-up ops are the claims before the claim that ended it, read after every client
    # finished, so a count that differs from ops_at_stop counted a claim after the end (or lost one)
    if ot != oa:
        bad.append(f"ops {ot} vs ops_at_stop {oa}: the warm-up count moved after the claim that ended it")
    cap = w.get("capped")
    if not isinstance(cap, bool):
        bad.append(f"warmup.capped {cap!r}, want true or false (bbload's warmup_cap_applied)")
    else:
        if oa < ops and not cap:
            bad.append(f"stopped with {oa} ops < OPS {ops} and not capped: only MAX_S can end it there")
        if cap and st < m - 1e-3:
            bad.append(f"capped at {st} s, before MAX_S {m}")
        # both ways (LOW 15): ending before S takes MAX_S, and done (OPS and S both met, clear of print precision) is
        # never capped, as warm_ends' capped = !done
        if not cap and st < s - 1e-3:
            bad.append(f"not capped, stopped at {st} s before S {s}: only MAX_S ends a warm-up before S")
        if cap and oa >= ops and st >= s + 1e-3:
            bad.append(f"capped, but OPS {ops} and S {s} were both met ({oa} ops at {st} s): done is not capped")
    # review 5 MED 8 (the driver half): the warm-up's clock starts after every client is ready (bbload's g_t0)
    rd = w.get("t0_after_ready_ns")
    if type(rd) is not int or rd < 0:
        bad.append(f"warmup.t0_after_ready_ns {rd!r}: the warm-up's start must follow the last client's ready (an int "
                   ">= 0 ns), so no connect or setup counts as warm-up")
    return bad + stamp_problems(j)


def tie(j, live):
    """The live warm-up record (warmup-live.json) is the run's own warm-up (summary.json `warmup`): the replay in
    warmup_conformance.py live shares warm_ends with the live loop, so only this ties the record to the run (fresh read
    of 7a8c3a817..0802dd739, finding 2). One claim per warm-up op plus the stop's, its stop at ops_at_stop == ops, the
    stop's claim at stop_secs (printed to ms), the same capped and rule."""
    if not isinstance(live, dict):
        return ["no live warm-up record (warmup-live.json)"]
    w = j.get("warmup") or {}
    claims, st, wo, cap = live.get("claims_ns"), live.get("stop_at"), live.get("warm_ops"), live.get("capped")
    bad = []
    if not isinstance(claims, list) or not claims or any(type(c) is not int for c in claims):
        return ["the live record's claims_ns is not a non-empty list of ints"]
    if type(st) is not int or len(claims) != st + 1:
        bad.append(f"{len(claims)} live claims for stop_at {st!r}: want one per warm-up op plus the stop's")
    if not (st == wo == w.get("ops_at_stop") == w.get("ops")):
        bad.append(f"live stop_at {st!r}, warm_ops {wo!r}; summary ops_at_stop {w.get('ops_at_stop')!r}, ops "
                   f"{w.get('ops')!r}: not one warm-up")
    ss = w.get("stop_secs")
    if not isinstance(ss, (int, float)) or abs(claims[-1] / 1e9 - ss) > 0.0005 + 1e-9:
        bad.append(f"the live stop's claim at {claims[-1] / 1e9:.6f} s, summary stop_secs {ss!r}")
    if cap not in (0, 1) or bool(cap) != w.get("capped"):
        bad.append(f"live capped {cap!r}, summary capped {w.get('capped')!r}")
    if live.get("rule") != w.get("rule"):
        bad.append(f"live rule {live.get('rule')!r}, summary rule {w.get('rule')!r}")
    return bad


STAMP_SLACK_S = 0.05  # a window's realtime span vs its monotonic secs: two clocks read a few calls apart


def stamp_problems(j):
    """The MED 12 ruling's window stamps: every window's realtime bounds (CLOCK_REALTIME, as the shared sweeper's
    EPOCHREALTIME), opening at or after the previous one closed, each span within STAMP_SLACK_S of its secs, and the
    run's measured span tm0/tm1_realtime_s (the competitors' names) from the first window's open to the last's close."""
    wins = j.get("windows")
    if not isinstance(wins, list) or not wins:
        return ["no windows: the measured span has no realtime bounds"]
    num = lambda x: isinstance(x, (int, float)) and not isinstance(x, bool)  # noqa: E731
    bad, prev = [], None
    for i, w in enumerate(wins):
        a, b, secs = (w.get(k) if isinstance(w, dict) else None for k in ("t0_realtime_s", "t1_realtime_s", "secs"))
        name = w.get("window") if isinstance(w, dict) else f"#{i}"
        if not (num(a) and num(b) and num(secs)):
            bad.append(f"window {name}: realtime bounds {a!r}, {b!r} and secs {secs!r} are not all numbers")
            prev = None
        else:
            if b < a:
                bad.append(f"window {name}: closes at {b} before it opens at {a}")
            elif abs((b - a) - secs) > STAMP_SLACK_S:
                bad.append(f"window {name}: realtime span {b - a:.6f} s vs secs {secs} (over {STAMP_SLACK_S} s apart)")
            if prev is not None and a < prev:
                bad.append(f"window {name}: opens at {a} before the previous window closed at {prev}")
            prev = b
        # the engine's four branch exception counters at the window's open and close (routing ruling 05:16:51Z)
        ec = w.get("exception_counters") if isinstance(w, dict) else None
        if not isinstance(ec, dict) or not isinstance(ec.get("before"), dict) or not isinstance(ec.get("after"), dict):
            bad.append(f"window {name}: no exception_counters {{before, after}} (Database::branch_exception_counters())")
        else:
            for k in EXC:
                x, y = ec["before"].get(k), ec["after"].get(k)
                if type(x) is not int or type(y) is not int or x < 0 or y < x:
                    bad.append(f"window {name}: exception counter {k} before {x!r} after {y!r}: not two non-negative "
                               "ints, never going back")
    for k in ("tracerpid_tm0", "tracerpid_tm1"):
        if k not in j or not (j[k] is None or (type(j[k]) is int and j[k] >= 0)):
            bad.append(f"{k} {j.get(k, '(absent)')!r}: the driver's own TracerPid at the measured span's bound, an int "
                       ">= 0, or null where there is no /proc")
    first, last = wins[0], wins[-1]
    for k, want in (("tm0_realtime_s", first.get("t0_realtime_s") if isinstance(first, dict) else None),
                    ("tm1_realtime_s", last.get("t1_realtime_s") if isinstance(last, dict) else None)):
        if not num(j.get(k)) or j.get(k) != want:
            bad.append(f"{k} {j.get(k)!r}, want {want!r} (the measured span's bound)")
    return bad


def self_test():
    # TEST EDIT, flagged (review 5 LOW 15): ot defaults to oa. It was 5010 beside oa 5000, and two cases gave 40 beside
    # 12: a completion-counting driver's numbers, which A23's claim count cannot produce. No expectation changes.
    def rec(t=33, c=4, rule="1000000:0:2", st=2.02, dr=0.3, oa=5000, ot=None, by=None, per="auto", wpc=None,
            capped=True, ready=1500, wins="auto", tm=("auto", "auto")):
        ot = oa if ot is None else ot
        by = by if by is not None else split(t, c)
        # TEST EDIT, flagged (review 5 MED 8's driver half; the MED 12 ruling's window stamps): the record gains the
        # warm-up's start after the last client was ready (t0_after_ready_ns) and each window's realtime bounds with
        # the run's measured span (tm0/tm1_realtime_s), new required fields at passing values; no expectation changes
        if wins == "auto":
            # TEST EDIT, flagged (the lead's routing ruling 05:16:51Z): each window also carries the engine's four
            # branch exception counters read at its open and close, at passing values
            ec = {"before": {k: 7 for k in EXC}, "after": {k: 7 for k in EXC}}
            wins = [{"window": "create", "secs": 0.5, "t0_realtime_s": 1000.0, "t1_realtime_s": 1000.5,
                     "exception_counters": ec},
                    {"window": "connect", "secs": 0.25, "t0_realtime_s": 1000.6, "t1_realtime_s": 1000.85,
                     "exception_counters": ec}]
        r = {"ops_total": t, "ops_total_asked": t, "ops_by_client": by,
             "ops_per_client": (by[0] if len(set(by)) == 1 else None) if per == "auto" else per,
             "warmup_per_client": wpc, "warmup_rule": rule,
             "warmup": {"rule": rule, "ops_at_stop": oa, "ops": ot, "stop_secs": st, "drain_secs": dr,
                        "secs": round(st + dr, 3), "capped": capped}, "windows": wins}
        if ready is not None:
            r["warmup"]["t0_after_ready_ns"] = ready
        # TEST EDIT, flagged: the driver's own TracerPid at the measured span's open and close (0: untraced)
        r["tracerpid_tm0"] = r["tracerpid_tm1"] = 0
        for k, v, x in (("tm0_realtime_s", tm[0], 0), ("tm1_realtime_s", tm[1], -1)):
            if v == "auto":
                v = wins[x]["t0_realtime_s" if x == 0 else "t1_realtime_s"] if wins else None
            if v is not None:
                r[k] = v
        return r

    EC = {"before": {k: 7 for k in EXC}, "after": {k: 7 for k in EXC}}
    # rec()'s warm-up as its live record: 5000 warm-up claims, then the stop's claim at 2.02 s, capped
    LIVE = {"rule": "1000000:0:2", "claims_ns": [i * 400_000 for i in range(5000)] + [2_020_000_000], "stop_at": 5000,
            "warm_ops": 5000, "capped": 1}
    W0 = {"window": "create", "secs": 0.5, "t0_realtime_s": 1000.0, "t1_realtime_s": 1000.5, "exception_counters": EC}

    cases = [
        ("a run to MAX_S with T=33 over 4 clients keeps the contract", check(rec(), 33, 4, "1000000:0:2") == []),
        ("the split is [9, 8, 8, 8]", split(33, 4) == [9, 8, 8, 8]),
        ("T=20 over 16 clients is four at 2", split(20, 16) == [2] * 4 + [1] * 12),
        ("an early stop on ops (10:0:30) keeps the contract",
         check(rec(rule="10:0:30", st=0.04, oa=12, capped=False), 33, 4, "10:0:30") == []),
        ("mutant: OPS ignored (stops at S=0 with 0 of 10^6 ops)",
         check(rec(st=0.02, oa=3), 33, 4, "1000000:0:2") != []),
        ("mutant: S and MAX_S swapped (stops at 0 with MAX_S 2)", check(rec(st=0.0, oa=0), 33, 4, "1000000:0:2") != []),
        ("mutant: stops at once", check(rec(rule="10:0:30", st=0.0, oa=0, ot=0), 33, 4, "10:0:30") != []),
        ("mutant: runs past MAX_S", check(rec(st=4.0), 33, 4, "1000000:0:2") != []),
        ("mutant: the T+1 split (id <= T % C)",
         check(rec(by=[9, 9, 8, 8]) | {"ops_total": 34}, 33, 4, "1000000:0:2") != []),
        ("ops_per_client must be null on an uneven split", check(rec(per=9), 33, 4, "1000000:0:2") != []),
        ("warmup_per_client must be null under a rule", check(rec(wpc=20), 33, 4, "1000000:0:2") != []),
        ("another rule string fails", check(rec(), 33, 4, "1000:10:180") != []),
        ("secs must be stop + drain", check(rec() | {"warmup": {**rec()["warmup"], "secs": 9.9}}, 33, 4,
                                             "1000000:0:2") != []),
        ("an incomplete warm-up record fails", check(rec() | {"warmup": {"rule": "1000000:0:2"}}, 33, 4,
                                                     "1000000:0:2") != []),
        # fourth lane review LOW 14: the record says whether MAX_S ended it, as bbload's warmup_cap_applied does
        ("LOW 14: a warm-up record with no capped field fails",
         check(rec() | {"warmup": {k: v for k, v in rec()["warmup"].items() if k != "capped"}}, 33, 4,
               "1000000:0:2") != []),
        ("LOW 14: capped as a string fails", check(rec(capped="true"), 33, 4, "1000000:0:2") != []),
        ("LOW 14: a stop below OPS that says not capped fails", check(rec(capped=False), 33, 4, "1000000:0:2") != []),
        ("LOW 14: capped before MAX_S fails",
         check(rec(rule="10:0:30", st=0.04, oa=12, capped=True), 33, 4, "10:0:30") != []),
        # review 5 LOW 15: under A23 the warm-up ops are the claims before the ending claim, read after every client
        # finished, so ops == ops_at_stop; capped both ways (not capped needs S reached; done wins over capped)
        ("LOW 15: ops above ops_at_stop fails (a claim counted after the end)",
         check(rec(rule="10:0:30", st=0.04, oa=12, ot=13, capped=False), 33, 4, "10:0:30") != []),
        ("LOW 15: not capped before S fails (only MAX_S can end it before S)",
         check(rec(rule="10:1:30", st=0.5, oa=12, ot=12, capped=False), 33, 4, "10:1:30") != []),
        ("LOW 15: done (OPS and S met) but recorded capped fails (done wins)",
         check(rec(rule="10:1:2", st=2.0, oa=12, ot=12, capped=True), 33, 4, "10:1:2") != []),
        ("LOW 15: a clean early stop with S > 0 keeps the contract",
         check(rec(rule="10:1:30", st=1.01, oa=12, ot=12, capped=False), 33, 4, "10:1:30") == []),
        # the capped=(claimed < OPS) mutant differs from the rule only when MAX_S < S: at MAX_S with OPS met and S not
        # reached, the rule says capped and the mutant says not (parse_rule accepts S > MAX_S)
        ("LOW 15: MAX_S below S, OPS met, recorded not capped fails (the capped=(claimed<OPS) mutant)",
         check(rec(rule="10:5:2", st=2.0, oa=12, capped=False), 33, 4, "10:5:2") != []),
        ("LOW 15: MAX_S below S, OPS met, capped keeps the contract (done needs S too)",
         check(rec(rule="10:5:2", st=2.0, oa=12, capped=True), 33, 4, "10:5:2") == []),
        # review 5 MED 8 (the driver half): the warm-up's clock starts only after every client is ready, as bbload's
        # g_t0 does, so no client's connect or setup counts as warm-up: t0 minus the last client's ready, in ns, >= 0
        ("MED 8: a warm-up record with no t0_after_ready_ns fails", check(rec(ready=None), 33, 4, "1000000:0:2") != []),
        ("MED 8: t0 before the last client was ready (-5 ns) fails", check(rec(ready=-5), 33, 4, "1000000:0:2") != []),
        ("MED 8: t0_after_ready_ns as a bool fails", check(rec(ready=True), 33, 4, "1000000:0:2") != []),
        ("MED 8: t0 exactly at the last ready (0 ns) keeps the contract", check(rec(ready=0), 33, 4, "1000000:0:2") == []),
        # the MED 12 ruling's window stamps: each window's realtime bounds and the run's measured span, so ours' timed
        # run is judged over its measured windows as the competitors' is (tm0/tm1_realtime_s, their names)
        ("stamps: no windows fails", check(rec(wins=[]), 33, 4, "1000000:0:2") != []),
        ("stamps: a window without t0_realtime_s fails",
         check(rec(wins=[{k: v for k, v in W0.items() if k != "t0_realtime_s"}], tm=(1000.0, 1000.5)), 33, 4,
               "1000000:0:2") != []),
        ("stamps: a window closing before it opens fails",
         check(rec(wins=[W0 | {"t1_realtime_s": 999.5, "secs": -0.5}], tm=(1000.0, 999.5)), 33, 4, "1000000:0:2") != []),
        ("stamps: a window opening before the previous one closed fails",
         check(rec(wins=[W0, W0 | {"window": "connect", "t0_realtime_s": 1000.4, "t1_realtime_s": 1000.9}]), 33, 4,
               "1000000:0:2") != []),
        ("stamps: tm0_realtime_s other than the first window's open fails",
         check(rec(tm=(999.0, "auto")), 33, 4, "1000000:0:2") != []),
        ("stamps: tm1_realtime_s other than the last window's close fails",
         check(rec(tm=("auto", 1001.0)), 33, 4, "1000000:0:2") != []),
        ("stamps: no tm1_realtime_s fails", check(rec(tm=("auto", None)), 33, 4, "1000000:0:2") != []),
        ("stamps: a window whose realtime span (0.5 s) disagrees with its secs (0.2) by over 0.05 s fails",
         check(rec(wins=[W0 | {"secs": 0.2}]), 33, 4, "1000000:0:2") != []),
        # the lead's routing ruling (DECISIONS 2026-10-15T05:16:51Z): every run records the engine's four branch exception
        # counters (Database::branch_exception_counters()) at each window's open and close, as non-negative ints; the
        # record is the driver's contract, a nonzero delta is summarize's to label (here a moved counter still keeps it)
        ("counters: a window without exception_counters fails",
         check(rec(wins=[{k: v for k, v in W0.items() if k != "exception_counters"}]), 33, 4, "1000000:0:2") != []),
        ("counters: a window whose after lacks compaction_waits fails",
         check(rec(wins=[W0 | {"exception_counters": {"before": EC["before"],
                                                      "after": {k: 7 for k in EXC[:3]}}}]), 33, 4, "1000000:0:2") != []),
        ("counters: a counter that is a string fails",
         check(rec(wins=[W0 | {"exception_counters": {"before": EC["before"],
                                                      "after": EC["after"] | {"hardcap_upgrades": "7"}}}]), 33, 4,
               "1000000:0:2") != []),
        ("counters: a counter that went backwards (7 -> 6) fails",
         check(rec(wins=[W0 | {"exception_counters": {"before": EC["before"],
                                                      "after": EC["after"] | {"compaction_waits": 6}}}]), 33, 4,
               "1000000:0:2") != []),
        ("counters: a counter that moved (7 -> 9) keeps the contract (summarize labels the cell)",
         check(rec(wins=[W0 | {"exception_counters": {"before": EC["before"],
                                                      "after": EC["after"] | {"compaction_waits": 9}}}]), 33, 4,
               "1000000:0:2") == []),
        # the driver records its own TracerPid at the measured span's open and close (/proc/self/status, as bbload's
        # tracerpid_tm0/tm1): an int, or null where the platform has no /proc (summarize, for T3, requires 0)
        ("tracerpid: no tracerpid_tm0 fails", check({k: v for k, v in rec().items() if k != "tracerpid_tm0"}, 33, 4,
                                                    "1000000:0:2") != []),
        ("tracerpid: tracerpid_tm1 as a string fails", check(rec() | {"tracerpid_tm1": "0"}, 33, 4, "1000000:0:2") != []),
        ("tracerpid: null on a platform without /proc keeps the contract",
         check(rec() | {"tracerpid_tm0": None, "tracerpid_tm1": None}, 33, 4, "1000000:0:2") == []),
        # second fresh read (0802dd739..79c58561f) LOW 2 and 4: on Linux the driver can read its TracerPid, so null is
        # refused there; tie refuses a summary or warm-up that is not an object instead of crashing
        ("tracerpid: null on Linux fails (the driver could read /proc)",
         tracerpid_problems(rec() | {"tracerpid_tm0": None, "tracerpid_tm1": None}, linux=True) != []),
        ("tracerpid: 0 on Linux keeps the contract", tracerpid_problems(rec(), linux=True) == []),
        ("tie: a summary that is not an object is refused, not a crash", tie([], LIVE) != []),
        ("tie: a summary whose warmup is not an object is refused", tie(rec() | {"warmup": 5}, LIVE) != []),
        # fresh read of 7a8c3a817..0802dd739, finding 2: the live record shares warm_ends with the replay, so replaying it
        # alone can only catch a claim-handling split; it must also be the run's own warm-up (summary.json warmup):
        # one claim per warm-up op plus the stop's, its stop at ops_at_stop == ops, its last claim at stop_secs, capped
        ("tie: a live record that is the summary's warm-up keeps the contract", tie(rec(), LIVE) == []),
        ("tie: a live record missing a claim (5000 claims for stop_at 5000) fails",
         tie(rec(), LIVE | {"claims_ns": LIVE["claims_ns"][1:]}) != []),
        ("tie: a live stop_at other than ops_at_stop fails", tie(rec(), LIVE | {"stop_at": 4999, "warm_ops": 4999}) != []),
        ("tie: a live last claim at 2.5 s beside stop_secs 2.02 fails",
         tie(rec(), LIVE | {"claims_ns": LIVE["claims_ns"][:-1] + [2_500_000_000]}) != []),
        ("tie: a live capped 0 beside the summary's capped true fails", tie(rec(), LIVE | {"capped": 0}) != []),
        ("tie: a live rule other than the summary's fails", tie(rec(), LIVE | {"rule": "10:0:30"}) != []),
        ("tie: no live record (None) fails", tie(rec(), None) != []),
    ]
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"DRIVER-CONTRACT self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"DRIVER-CONTRACT SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:] == ["self-test"]:
        return self_test()
    if a[1:2] == ["tie"] and len(a) == 4:
        try:
            j, live = json.load(open(a[2])), json.load(open(a[3]))
        except (OSError, ValueError) as e:
            print(f"driver contract tie: REFUSED: {type(e).__name__}: {e}")
            return 2
        bad = tie(j, live)
        for b in bad:
            print(f"driver contract tie: {a[3]}: {b}")
        print(f"driver contract tie: {a[3]}: {'ok' if not bad else 'BROKEN'}")
        return 0 if not bad else 1
    if a[1:2] == ["check"] and len(a) in (6, 7):
        j = json.load(open(a[2]))
        by = [int(x) for x in a[6].split(",")] if len(a) == 7 else None
        bad = check(j, int(a[3]), int(a[4]), a[5], by)
        for b in bad:
            print(f"driver contract: {a[2]}: {b}")
        print(f"driver contract: {a[2]}: {'ok' if not bad else 'BROKEN'}")
        return 0 if not bad else 1
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
