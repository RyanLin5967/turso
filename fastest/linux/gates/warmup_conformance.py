#!/usr/bin/env python3
"""warmup_conformance.py -- one warm-up rule for every load driver (fourth lane review LOW 14, the lead's ruling):
bbload's OPS:S:MAX_S implementation is the definition, and clonebench and fastest_profile must make the same stop
decision and count the same warm-up ops on the same input. Each driver exposes the decision its live loop uses as

    DRIVER --warmup-replay OPS:S:MAX_S   < TRACE

TRACE is one line per claim of an op (a client about to start one), the claim's time in integer nanoseconds since
the warm-up began, non-decreasing. The driver prints exactly one line:

    stop_at=I warm_ops=N capped=0|1        the warm-up ends at claim I (0-based), which is not a warm-up op
    stop_at=none warm_ops=N capped=none    the trace ended first

The rule (PREREG annex A23; bbload.c claim_op at artie ec9bba5552, factored into warm_ends()): at each claim, with
`claimed` warm-up ops claimed before it and `el` ns since the start, the warm-up ends at that claim when
claimed >= OPS and el >= S x 1e9 (done), or when el >= MAX_S x 1e9 (capped); capped is reported as not done, so a
claim that meets both reads capped=0. Seconds become nanoseconds by truncation, as (uint64_t)(S * 1e9) does. Every
claim before the stop is a warm-up op, so warm_ops == stop_at.

  warmup_conformance.py run [--bbload BIN] [--clonebench BIN] [--fastest-profile BIN] [--record FILE]
        exit 0: all three given and every one matches every case; 3: every given one matches, but not all three
        were given (PARTIAL, never a conformance verdict); 1: a mismatch; 2: refused (none given, a driver missing,
        two roles naming one file or one sha256, timed out, or printing anything but one result line). Each
        driver is reported by realpath and sha256; --record writes the verdict as JSON (rc, verdict, cases, and per
        role path, realpath, sha256), which summarize binds to the binaries a package ran (review 5 MED 6, 7)
  warmup_conformance.py self-test
        the harness on fake replayers: a correct one passes, and four wrong ones (> for >=, capped winning over
        done, the stop claim counted, rounding instead of truncating) each fail on a named case

The expected values below are derived by hand from the rule's text, never from running a driver.
"""
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

S = 10**9  # ns per second
MS = 10**6

# (name, rule, claim times in ns, expected (stop_at, warm_ops, capped)); None = the trace ended first
CASES = [
    ("both minimums: 1000 ops by 1 s, 10 s at claim 10000", "1000:10:180",
     [i * MS for i in range(12001)], (10000, 10000, 0)),
    ("ops bound: 5 ops at one claim a second, S 2 s", "5:2:180", [i * S for i in range(21)], (5, 5, 0)),
    ("capped: 1000 ops never reached, MAX_S 30 s at claim 30", "1000:10:30", [i * S for i in range(41)],
     (30, 30, 1)),
    ("edge: exactly OPS claimed and exactly S elapsed ends it (>= on both)", "3:5:100",
     [0, 1 * S, 2 * S, 5 * S, 6 * S], (3, 3, 0)),
    ("edge: 1 ns before S does not end it", "3:5:100", [0, 1 * S, 2 * S, 5 * S - 1, 5 * S], (4, 4, 0)),
    ("done and capped at one claim reads capped=0", "3:5:5", [0, 1 * S, 2 * S, 5 * S], (3, 3, 0)),
    ("capped before OPS reads capped=1", "3:5:5", [0, 1 * S, 5 * S], (2, 2, 1)),
    ("the trace ends first", "10:1:100", [i * S for i in range(5)], (None, 5, None)),
    ("a zero rule ends at the first claim", "0:0:180", [0], (0, 0, 0)),
    ("fractional S: 0.5 s is 500000000 ns", "2:0.5:100", [0, S // 2 - 1, S // 2], (2, 2, 0)),
    ("claims at one instant count one by one", "2:0:100", [0, 0, 0, 0], (2, 2, 0)),
    ("truncation: S 1.0000000009 s is 1000000000 ns, not 1000000001", "1:1.0000000009:100",
     [0, S, S + 1], (1, 1, 0)),
]
LINE = re.compile(r"^stop_at=(\d+|none) warm_ops=(\d+) capped=(0|1|none)$")


class Refused(Exception):
    pass


def replay(binary, rule, trace):
    try:
        r = subprocess.run([binary, "--warmup-replay", rule], input="".join(f"{t}\n" for t in trace),
                           capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise Refused(f"{binary} --warmup-replay {rule}: {type(e).__name__}: {e}")
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    m = LINE.match(lines[0].strip()) if r.returncode == 0 and len(lines) == 1 else None
    if not m:
        raise Refused(f"{binary} --warmup-replay {rule}: rc {r.returncode}, stdout {r.stdout[-300:]!r}, "
                      f"stderr {r.stderr[-300:]!r}: not one result line")
    stop, warm, capped = m.groups()
    return (None if stop == "none" else int(stop), int(warm), None if capped == "none" else int(capped))


def identity(binary):
    """(realpath, sha256) of a driver binary; OSError when it cannot be read"""
    rp = os.path.realpath(binary)
    with open(rp, "rb") as f:
        return rp, hashlib.sha256(f.read()).hexdigest()


VERDICTS = {0: "PASS", 1: "FAIL", 2: "REFUSED", 3: "PARTIAL"}


def run(drivers, record=None):
    """drivers: {role: binary}; returns (rc, report lines). Each role's binary is named by realpath and sha256, and
    two roles resolving to one file or to one sha256 are REFUSED (review 5 MED 7: the same binary under all three roles
    gave rc 0). RECORD, when given, receives the verdict as JSON: rc, verdict, the case count, and per role its path,
    realpath and sha256, so a package can bind the verdict to the binaries it ran (review 5 MED 6)."""
    out, bad, ids = [], 0, {}

    def done(rc, lines):
        if record:
            with open(record, "w") as f:
                json.dump({"rc": rc, "verdict": VERDICTS[rc], "cases": len(CASES),
                           "drivers": {r: {"path": drivers[r], "realpath": ids[r][0], "sha256": ids[r][1]}
                                       for r in ids}, "report": lines[-1] if lines else ""}, f, indent=1)
        return rc, lines

    if not drivers:
        return done(2, ["warmup conformance: REFUSED: no driver given"])
    try:
        for role, binary in drivers.items():
            ids[role] = identity(binary)
    except OSError as e:
        return done(2, [f"warmup conformance: REFUSED: a driver cannot be read: {e}"])
    roles = sorted(ids)
    for i, a in enumerate(roles):
        for b in roles[i + 1:]:
            if ids[a][0] == ids[b][0] or ids[a][1] == ids[b][1]:
                return done(2, [f"warmup conformance: REFUSED: {a} and {b} are the same driver ({ids[a][0]}, sha256 "
                                f"{ids[a][1][:12]}...): one verdict cannot stand for two drivers"])
    for role in roles:
        out.append(f"warmup conformance: driver {role}: {ids[role][0]} sha256 {ids[role][1]}")
    got = {}
    try:
        for name, binary in drivers.items():
            for case, rule, trace, want in CASES:
                g = replay(binary, rule, trace)
                got[(name, case)] = g
                ok = g == want
                bad += not ok
                out.append(f"warmup conformance {'PASS' if ok else 'FAIL'}: {name}: {case} ({rule}): got {g}, want {want}")
    except Refused as e:
        return done(2, out + [f"warmup conformance: REFUSED: {e}"])
    names = sorted(drivers)
    for case, _, _, _ in CASES:  # pairwise, so a report names who disagrees with whom even when both are wrong
        vals = {n: got[(n, case)] for n in names}
        if len(set(vals.values())) > 1:
            out.append(f"warmup conformance: DISAGREE on {case!r}: {vals}")
    missing = [n for n in ("bbload", "clonebench", "fastest_profile") if n not in drivers]
    if bad:
        out.append(f"warmup conformance: FAIL ({bad} mismatches over {len(drivers)} drivers x {len(CASES)} cases)")
        return done(1, out)
    if missing:
        out.append(f"warmup conformance: PARTIAL: {len(drivers)} x {len(CASES)} match; not given: {missing}")
        return done(3, out)
    out.append(f"warmup conformance: PASS: 3 drivers x {len(CASES)} cases match the rule")
    return done(0, out)


FAKE = r'''#!/usr/bin/env python3
# fake replayer {name}
import sys
ops, s, m = sys.argv[2].split(":")
ops, s_ns, m_ns = int(ops), {conv}(float(s) * 1e9), {conv}(float(m) * 1e9)
claimed = 0
for line in sys.stdin:
    el = int(line)
    done = claimed >= ops and el {cmp} s_ns
    capped = el >= m_ns
    if done or capped:
        print(f"stop_at={{claimed}} warm_ops={{claimed + {extra}}} capped={{{capped_expr}}}")
        sys.exit(0)
    claimed += 1
print(f"stop_at=none warm_ops={{claimed}} capped=none")
'''


def _safe(f):
    try:
        return bool(f())
    except Exception:  # noqa: BLE001
        return False


def fake(d, name, conv="int", cmp=">=", extra=0, capped_expr="0 if done else 1"):
    p = os.path.join(d, name)
    with open(p, "w") as f:
        f.write(FAKE.format(conv=conv, cmp=cmp, extra=extra, capped_expr=capped_expr, name=name))
    os.chmod(p, 0o755)
    return p


def self_test():
    d = tempfile.mkdtemp(prefix="warmup-conformance-")
    try:
        return _self_test(d)
    finally:
        shutil.rmtree(d, ignore_errors=True)  # review 5 LOW 13: the directory leaked


def _self_test(d):
    # TEST EDIT, flagged (review 5 MED 7): the cases below gave ONE file under several roles, which is the hole MED 7
    # closes (the same binary under all three roles gave rc 0); each role now gets its own correct fake (the files
    # differ by their name line), and every expectation is unchanged
    good = fake(d, "good")
    good_b, good_c = fake(d, "good-bbload"), fake(d, "good-clonebench")
    wrong = {
        "strict time (> for >=)": (fake(d, "gt", cmp=">"), "edge: exactly OPS claimed and exactly S elapsed"),
        "capped wins over done": (fake(d, "capwins", capped_expr="1 if capped else 0"),
                                  "done and capped at one claim reads capped=0"),
        "the stop claim counted as warm-up": (fake(d, "plus1", extra=1), "both minimums"),
        "rounding, not truncation": (fake(d, "round", conv="round"), "truncation"),
    }
    cases = []
    rc, rep = run({"bbload": good_b, "clonebench": good_c, "fastest_profile": good})
    cases.append(("three correct fakes, one per role, pass (rc 0)", rc == 0, rep[-1]))
    # review 5 MED 7: a verdict is about three binaries, so one file (or one byte-identical copy) under two roles refuses
    rc, rep = run({"bbload": good, "clonebench": good, "fastest_profile": good})
    cases.append(("MED 7: one file under all three roles is REFUSED (rc 2)", rc == 2 and "same" in rep[-1], rep[-1]))
    twin = os.path.join(d, "twin")
    shutil.copyfile(good, twin)
    os.chmod(twin, 0o755)
    rc, rep = run({"bbload": good_b, "clonebench": twin, "fastest_profile": good})
    cases.append(("MED 7: a byte-identical copy under a second role is REFUSED (rc 2)", rc == 2 and "same" in rep[-1],
                  rep[-1]))
    rec = os.path.join(d, "record.json")

    def rec_ok():
        run({"bbload": good_b, "clonebench": good_c, "fastest_profile": good}, record=rec)
        r = json.load(open(rec))
        return (r["rc"] == 0 and r["verdict"] == "PASS" and r["cases"] == len(CASES)
                and all(r["drivers"][role]["sha256"] == hashlib.sha256(open(p, "rb").read()).hexdigest()
                        and r["drivers"][role]["realpath"] == os.path.realpath(p)
                        for role, p in (("bbload", good_b), ("clonebench", good_c), ("fastest_profile", good))))
    cases.append(("MED 7: --record writes the rc, the verdict and each role's realpath and sha256", _safe(rec_ok), ""))
    rc, rep = run({"fastest_profile": good})
    cases.append(("one correct driver alone is PARTIAL (rc 3), never a pass", rc == 3, rep[-1]))
    for what, (p, case) in wrong.items():
        rc, rep = run({"bbload": good_b, "clonebench": good_c, "fastest_profile": p})
        named = any(ln.startswith("warmup conformance FAIL: fastest_profile: " + case) for ln in rep)
        cases.append((f"a wrong fake ({what}) fails (rc 1) on its case {case!r}", rc == 1 and named, rep[-1]))
    rc, rep = run({"fastest_profile": os.path.join(d, "absent")})
    cases.append(("a missing driver is REFUSED (rc 2)", rc == 2, rep[-1]))
    noisy = os.path.join(d, "noisy")
    with open(noisy, "w") as f:
        f.write("#!/bin/sh\necho hello\necho stop_at=0 warm_ops=0 capped=0\n")
    os.chmod(noisy, 0o755)
    rc, rep = run({"fastest_profile": noisy})
    cases.append(("a driver printing more than its result line is REFUSED (rc 2)", rc == 2, rep[-1]))
    cases.append(("no driver at all is REFUSED (rc 2)", run({})[0] == 2, ""))
    bad = [n for n, ok, _ in cases if not ok]
    for n, ok, last in cases:
        print(f"WARMUP-CONFORMANCE self-test {'PASS' if ok else 'FAIL'}: {n}" + ("" if ok else f" ({last})"))
    print(f"WARMUP-CONFORMANCE SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:] == ["self-test"]:
        return self_test()
    if a[1:2] == ["run"]:
        flags = {"--bbload": "bbload", "--clonebench": "clonebench", "--fastest-profile": "fastest_profile"}
        rest, drivers, record = a[2:], {}, None
        if len(rest) % 2 or any(rest[i] not in flags and rest[i] != "--record" for i in range(0, len(rest), 2)):
            print(__doc__, file=sys.stderr)
            return 2
        for i in range(0, len(rest), 2):
            if rest[i] == "--record":
                record = rest[i + 1]
            else:
                drivers[flags[rest[i]]] = rest[i + 1]
        rc, rep = run(drivers, record)
        print("\n".join(rep))
        return rc
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
