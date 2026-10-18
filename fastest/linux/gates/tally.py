#!/usr/bin/env python3
"""tally.py -- the one outcome record every self-test row prints (linux-rev-uw2 MED 1, the lead's ruling 2026-10-18).

A skip spelled any other way than the old `N/A-on-<cause>` (the reviewer's 'SKIPPED (no root)', exit 0) passed the
gate, because the gate read wording. Now every self-test prints exactly ONE line

    TALLY ran=R pass=P na=N fail=F         (R = P + F; N = the cases this host could not run)

and its rc must agree with it: rc 0 iff N = 0, F = 0 and P >= 1; rc 3 iff N >= 1 and F = 0; rc 1 iff F >= 1. A row with
no tally, two tallies, a malformed one, or a tally that disagrees with its rc is REFUSED (outcome "fail"). That is an
allowlist of outcomes: nothing a test prints reads as a pass unless its own tally says so and its rc agrees.

  tally.py check OUTFILE RC [--require-all]   exit 0 iff the record is an honest pass (with --require-all, also no
                                              N/A: t3run's selftests stage on the T3 box); else 1, printing why
  tally.py self-test                          the rules on planted records
Blind spot, stated: a test that skips a case and counts it as passed in its own tally is not caught here; the tally
is the test's own declaration, and each test's N/A cases are reviewed where they are written.
"""
import re
import sys

LINE_RE = re.compile(r"^TALLY ran=(\d+) pass=(\d+) na=(\d+) fail=(\d+)$")


def line(passed, failed, na=0):
    """the tally line for P passed, F failed and N not run"""
    return f"TALLY ran={passed + failed} pass={passed} na={na} fail={failed}"


def judge(text, rc):
    """-> (outcome, tally or None, problems). outcome: 'pass', 'na' or 'fail'; problems refuse the row."""
    found = [LINE_RE.match(ln.strip()) for ln in text.splitlines() if ln.strip().startswith("TALLY")]
    if not found:
        return "fail", None, ["no TALLY line (every self-test row prints one: TALLY ran= pass= na= fail=)"]
    if len(found) > 1:
        return "fail", None, [f"{len(found)} TALLY lines, not one"]
    if found[0] is None:
        return "fail", None, ["a malformed TALLY line"]
    t = dict(zip(("ran", "pass", "na", "fail"), map(int, found[0].groups())))
    probs = []
    if t["ran"] != t["pass"] + t["fail"]:
        probs.append(f"TALLY ran={t['ran']} is not pass + fail ({t['pass']} + {t['fail']})")
    if rc == 0:
        if t["na"] or t["fail"] or not t["pass"]:
            probs.append(f"rc 0 and the tally (pass={t['pass']} na={t['na']} fail={t['fail']}) disagree: rc 0 means "
                         "every case ran and passed")
        return ("fail" if probs else "pass"), t, probs
    if rc == 3:
        if t["fail"] or not t["na"]:
            probs.append(f"rc 3 and the tally (na={t['na']} fail={t['fail']}) disagree: rc 3 means some case was N/A and "
                         "none failed")
        return ("fail" if probs else "na"), t, probs
    if rc == 1:
        if not t["fail"]:
            probs.append(f"rc 1 and the tally (fail={t['fail']}) disagree: rc 1 means a case failed")
        return "fail", t, probs
    return "fail", t, probs + [f"rc {rc}"]


def self_test():
    cases = [
        ("a pass: rc 0, ran=2 pass=2", judge("x\nTALLY ran=2 pass=2 na=0 fail=0\n", 0)[0] == "pass"),
        ("an N/A: rc 3, na=1", judge("TALLY ran=1 pass=1 na=1 fail=0", 3)[0] == "na"),
        ("a failure: rc 1, fail=1", judge("TALLY ran=1 pass=0 na=0 fail=1", 1)[0] == "fail"),
        ("no tally refuses", judge("PASS everything\n", 0)[2] != []),
        ("two tallies refuse", judge("TALLY ran=1 pass=1 na=0 fail=0\nTALLY ran=1 pass=1 na=0 fail=0", 0)[2] != []),
        ("a malformed tally refuses", judge("TALLY ran=1 pass=1", 0)[2] != []),
        ("rc 0 with na=1 refuses", judge("TALLY ran=1 pass=1 na=1 fail=0", 0)[2] != []),
        ("rc 0 with nothing run refuses", judge("TALLY ran=0 pass=0 na=0 fail=0", 0)[2] != []),
        ("rc 3 with fail=1 refuses (a failure is never N/A)", judge("TALLY ran=1 pass=0 na=1 fail=1", 3)[2] != []),
        ("rc 3 with na=0 refuses", judge("TALLY ran=1 pass=1 na=0 fail=0", 3)[2] != []),
        ("rc 1 with fail=0 refuses", judge("TALLY ran=1 pass=1 na=0 fail=0", 1)[2] != []),
        ("ran other than pass + fail refuses", judge("TALLY ran=3 pass=1 na=0 fail=0", 0)[2] != []),
        ("line() writes what judge reads", judge(line(4, 0, 2), 3)[0] == "na"),
    ]
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"TALLY self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(line(len(cases) - len(bad), len(bad)))
    print(f"TALLY SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:] == ["self-test"]:
        return self_test()
    if len(a) in (4, 5) and a[1] == "check" and (len(a) == 4 or a[4] == "--require-all"):
        try:
            text, rc = open(a[2], errors="replace").read(), int(a[3])
        except (OSError, ValueError) as e:
            print(f"tally: REFUSED: {e}")
            return 1
        outcome, t, probs = judge(text, rc)
        for p in probs:
            print(f"tally: {a[2]}: {p}")
        if outcome == "na" and len(a) == 5:
            print(f"tally: {a[2]}: {t['na']} case(s) N/A, and N/A is never a pass here (--require-all)")
            return 1
        return 0 if outcome == "pass" else 1
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
