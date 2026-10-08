#!/usr/bin/env python3
"""fixture.py -- one parent fixture for every system (lane fastest-linux-comp; gate-6 review, t3run item 4).

Every system times its creates against the same parent: gen_seed.py's table t at ROWS rows, aged by AGE committed
random single-row UPDATEs (0 = fresh, PREREG §7 / amendment 52: 1e5), with PREBRANCH live branches made before the
cells (each with one private write: the system's own M1 op, untimed). Each job writes RAW/fixture.json:

  {"system", "rows", "age_updates", "prebranch", "gen_seed_sha256" (of the exact SQL stream loaded, aging included),
   "du_bytes" (du of the parent's files), "engine_bytes" (as the engine reports it; null only with "engine_bytes_why"),
   "extents" (data file -> extent count), "maintenance" (the post-load steps run)}

  fixture.py compare JSON...   exit 0 only when every fixture.json names the same rows, age_updates, prebranch and
                               gen_seed_sha256, and each has du_bytes and either engine_bytes or a reason
  fixture.py selftest          known-answer fixtures for compare
"""
import json
import sys


KEYS = ("rows", "age_updates", "prebranch", "gen_seed_sha256")


def compare(fixtures):
    """STUB (red): accepts everything."""
    return []


def selftest():
    base = {"system": "pg18-d2", "rows": 10000, "age_updates": 0, "prebranch": 0, "gen_seed_sha256": "ab" * 32,
            "du_bytes": 9000000, "engine_bytes": 8900000, "extents": {"t": 3}, "maintenance": ["VACUUM", "CHECKPOINT"]}

    def v(**kw):
        d = dict(base)
        d.update(kw)
        return {k: x for k, x in d.items() if x is not KeyError}

    cases = [
        ("same parent on every system", [v(), v(system="dolt"), v(system="b1")], True),
        ("a system loaded another row count", [v(), v(system="b1", rows=1000)], False),
        ("a system aged its parent, another did not", [v(), v(system="dolt", age_updates=100000)], False),
        ("a system made other live branches", [v(), v(system="doltgres", prebranch=10000)], False),
        ("a system loaded other SQL (its own generator)", [v(), v(system="b1", gen_seed_sha256="cd" * 32)], False),
        ("no generator digest", [v(), v(system="b1", gen_seed_sha256=KeyError)], False),
        ("no du size", [v(), v(system="dolt", du_bytes=KeyError)], False),
        ("engine size missing with no reason", [v(), v(system="dolt", engine_bytes=None)], False),
        ("engine size missing with a reason", [v(), v(system="dolt", engine_bytes=None,
                                                         engine_bytes_why="Dolt reports no database size")], True),
        ("no fixture at all", [], False),
    ]
    bad = 0
    for name, fx, want in cases:
        why = compare(fx)
        got = not why
        print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
        bad += got != want
    print(f"fixture selftest: {len(cases) - bad}/{len(cases)}")
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "compare":
        fx = []
        for p in sys.argv[2:]:
            try:
                fx.append(json.load(open(p)))
            except (OSError, ValueError) as e:
                print(f"REFUSED: {p}: {e}")
                sys.exit(1)
        why = compare(fx)
        print("ok" if not why else "REFUSED: " + "; ".join(why))
        sys.exit(0 if not why else 1)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
