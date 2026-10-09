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
            stop_secs >= MAX_S (fourth lane review LOW 14: bbload's OPS:S:MAX_S rule is the definition),
          - drain_secs >= 0, secs == stop_secs + drain_secs (to print precision), ops >= ops_at_stop.
  driver_contract.py self-test                                  the checker on synthetic records; exit 0 iff all pass

The fastest-gates release job runs the built driver with rules whose outcome is forced (1000000:0:2 must run to
MAX_S; 10:0:30 must stop on its ops long before MAX_S) and checks each record here, so a driver that ignores OPS,
swaps S and MAX_S, stops at once, or splits T into T+1 ops fails the job, not only a later T3 package.
"""
import json
import sys

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
    if dr < 0 or abs(se - (st + dr)) > 2e-3 or ot < oa:
        bad.append(f"drain {dr}, secs {se} vs stop + drain {st + dr}, ops {ot} vs ops_at_stop {oa}")
    cap = w.get("capped")
    if not isinstance(cap, bool):
        bad.append(f"warmup.capped {cap!r}, want true or false (bbload's warmup_cap_applied)")
    else:
        if oa < ops and not cap:
            bad.append(f"stopped with {oa} ops < OPS {ops} and not capped: only MAX_S can end it there")
        if cap and st < m - 1e-3:
            bad.append(f"capped at {st} s, before MAX_S {m}")
    return bad


def self_test():
    # TEST EDIT, flagged (review 5 LOW 15): ot defaults to oa. It was 5010 beside oa 5000, and two cases gave 40 beside
    # 12: a completion-counting driver's numbers, which A23's claim count cannot produce. No expectation changes.
    def rec(t=33, c=4, rule="1000000:0:2", st=2.02, dr=0.3, oa=5000, ot=None, by=None, per="auto", wpc=None,
            capped=True):
        ot = oa if ot is None else ot
        by = by if by is not None else split(t, c)
        return {"ops_total": t, "ops_total_asked": t, "ops_by_client": by,
                "ops_per_client": (by[0] if len(set(by)) == 1 else None) if per == "auto" else per,
                "warmup_per_client": wpc, "warmup_rule": rule,
                "warmup": {"rule": rule, "ops_at_stop": oa, "ops": ot, "stop_secs": st, "drain_secs": dr,
                           "secs": round(st + dr, 3), "capped": capped}}

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
    ]
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"DRIVER-CONTRACT self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"DRIVER-CONTRACT SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:] == ["self-test"]:
        return self_test()
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
