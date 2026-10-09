#!/usr/bin/env python3
"""fixture.py -- one parent fixture for every system (lane fastest-linux-comp; gate-6 review, t3run item 4).

Every system times its creates against the same parent: gen_seed.py's table t at ROWS rows, aged by AGE committed
random single-row UPDATEs (0 = fresh, PREREG §7 / amendment 52: 1e5), with PREBRANCH live branches made before the
cells (each with one private write: the system's own M1 op, untimed). Each job writes RAW/fixture.json:

  {"system", "rows", "age_updates", "prebranch", "live_branches" (MEASURED after prebranch: the live-branch count N
   every cell runs at, main excluded; lead review HIGH 1), "gen_seed_sha256" (gen_seed.py's digest of the SQL stream
   for these rows and aging), "du_bytes" (du of the parent's files), "engine_bytes" (as the engine reports it; null
   only with "engine_bytes_why"), "extents" (data file -> extent count), "maintenance" (the post-load steps run)}

  fixture.py compare JSON...   exit 0 only when every fixture.json names the same rows, age_updates, prebranch,
                               live_branches and gen_seed_sha256, each one's live_branches equals its prebranch, and
                               each has du_bytes and either engine_bytes or a reason
  fixture.py write OUT --system S --rows R --age K --prebranch N --live L --du PATH... [--engine-bytes B |
                   --engine-why W] [--extents FILE...] [--maintenance TEXT]
                               OUT = fixture.json: du_bytes = `du -sB1` over the PATHs (allocated bytes), extents =
                               filefrag's count per FILE, gen_seed_sha256 = gen_seed.py's digest for (R, K, seed 1)
  fixture.py sqlite FILE --rows R --age K --sqlite3 BIN [--digest-dir DIR]
                               the parent as an SQLite file, the way B1 (and ours, which opens SQLite files) get it:
                               journal_mode=WAL, gen_seed.py's SQL, its K aging UPDATEs (each autocommitted), then
                               the documented maintenance, a TRUNCATE checkpoint; prints the engine's size
                               (page_count x page_size) and the fed streams' digests (also DIR/seed-{sql,age}.sha256)
  fixture.py write ... --streams SQL.sha256 AGE.sha256 --readback 'COUNT|SUM|ROW_HASH'
                               (MED 3) the write path's own stream digests and the engine's read-back of t (gen_seed.py
                               readback-sql / readback-sqlite), recorded beside the generator's expected values;
                               compare refuses a job where either differs. The size tolerance against ours is not
                               implemented here (competitor half only; ours is fastest-linux's)
  fixture.py selftest          known-answer fixtures for compare
"""
import hashlib
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gen_seed  # noqa: E402


KEYS = ("rows", "age_updates", "prebranch", "live_branches", "gen_seed_sha256", "stream_sha256", "readback")


def compare(fixtures):
    """The reasons these fixtures are not one parent ([] = they are)."""
    if not fixtures:
        return ["no fixture.json at all"]
    why = []
    for k in KEYS:
        vals = {}
        for f in fixtures:
            vals.setdefault(json.dumps(f.get(k)), []).append(f.get("system", "?"))
        if any(json.loads(v) is None for v in vals):
            why.append(f"{k} missing for {[s for v, ss in vals.items() if json.loads(v) is None for s in ss]}")
        if len(vals) > 1:
            why.append(f"{k} differs: " + "; ".join(f"{json.loads(v)} on {ss}" for v, ss in vals.items()))
    for f in fixtures:
        s = f.get("system", "?")
        # HIGH 1: the live-branch count every cell runs at is MEASURED (count_branches after prebranch, main
        # excluded) and must be the requested one
        if f.get("live_branches") is not None and f.get("live_branches") != f.get("prebranch"):
            why.append(f"{s}: {f.get('live_branches')} live branches measured, {f.get('prebranch')} requested")
        # MED 3: what the write path piped, and what the engine read back, each against the generator
        if f.get("stream_sha256") is not None and f.get("stream_sha256") != f.get("expected_streams"):
            why.append(f"{s}: piped stream digests {f.get('stream_sha256')} are not the generator's "
                       f"{f.get('expected_streams')}")
        if f.get("readback") is not None and f.get("readback") != f.get("expected_readback"):
            why.append(f"{s}: the engine read back {f.get('readback')}, the generator wrote {f.get('expected_readback')}")
        if not isinstance(f.get("du_bytes"), int) or f["du_bytes"] <= 0:
            why.append(f"{s}: no du_bytes")
        eb = f.get("engine_bytes")
        if not (isinstance(eb, int) and eb > 0) and not f.get("engine_bytes_why"):
            why.append(f"{s}: no engine_bytes and no reason")
    return why


def opts(av, multi=("--du", "--extents", "--streams")):
    o, i, key = {}, 0, None
    while i < len(av):
        a = av[i]
        if a.startswith("--"):
            key = a
            if a in multi:
                o.setdefault(a, [])
            elif i + 1 < len(av):
                o[a] = av[i + 1]
                i += 1
                key = None
        elif key in multi:
            o[key].append(a)
        else:
            sys.exit(f"fixture.py: stray argument {a}")
        i += 1
    return o


def du_bytes(paths):
    r = subprocess.run(["du", "-sB1", "-c", *paths], capture_output=True, text=True, timeout=600)
    m = re.search(r"^(\d+)\s+total$", r.stdout, re.M)
    return int(m.group(1)) if r.returncode == 0 and m else None


def extent_count(path):
    r = subprocess.run(["filefrag", path], capture_output=True, text=True, timeout=120)
    m = re.search(r"(\d+) extents? found", r.stdout)
    return int(m.group(1)) if m else None


def write(av):
    out, o = av[0], opts(av[1:])
    rows, age = int(o["--rows"]), int(o["--age"])
    lv = o.get("--live", "")
    fx = {"system": o["--system"], "rows": rows, "age_updates": age, "prebranch": int(o["--prebranch"]),
          "live_branches": int(lv) if lv.isdigit() else None,
          "gen_seed_sha256": gen_seed.digest(rows, age, 1), "du_paths": o.get("--du", []),
          "du_bytes": du_bytes(o.get("--du", [])) if o.get("--du") else None,
          "engine_bytes": int(o["--engine-bytes"]) if o.get("--engine-bytes", "").isdigit() else None,
          "extents": {p: extent_count(p) for p in o.get("--extents", [])},
          "maintenance": o.get("--maintenance", "")}
    # MED 3: the write path's own digest files (gen_seed.py --digest-out, or fixture.py sqlite's) and the engine's
    # read-back triple, recorded beside what the generator says they must be
    sd = {}
    for k, p in zip(("sql", "age"), o.get("--streams", [])):
        try:
            sd[k] = open(p).read().strip() or None
        except OSError:
            sd[k] = None
    fx["stream_sha256"] = sd if len(sd) == 2 and all(sd.values()) else None
    fx["expected_streams"] = gen_seed.stream_digests(rows, age, 1)
    m = re.fullmatch(r"(\d+)\|(\d+)\|(\d+)", o.get("--readback", "").strip())
    fx["readback"] = dict(zip(("count", "sum", "row_hash"), map(int, m.groups()))) if m else None
    fx["expected_readback"] = dict(zip(("count", "sum", "row_hash"), map(int, gen_seed.expect(rows, age, 1).split("|"))))
    if fx["engine_bytes"] is None:
        fx["engine_bytes_why"] = o.get("--engine-why") or f"engine size not read ({o.get('--engine-bytes')!r})"
        if not o.get("--engine-why"):
            fx["engine_bytes_why"] = None  # an unread size is not a reason: compare refuses it
    with open(out, "w") as f:
        json.dump(fx, f, indent=1)
    print(json.dumps(fx))


def sqlite(av):
    path, o = av[0], opts(av[1:])
    rows, age, sq3 = int(o["--rows"]), int(o["--age"]), o["--sqlite3"]
    if os.path.exists(path):
        sys.exit(f"fixture.py sqlite: {path} exists")
    sql, aging = list(gen_seed.sql_lines(rows)), list(gen_seed.age_lines(rows, age, 1))
    # MED 3: the digest of each stream as this write path feeds it (the same framing as gen_seed.py --digest-out)
    dg = {k: hashlib.sha256("".join(ln + "\n" for ln in ls).encode()).hexdigest() for k, ls in (("sql", sql), ("age", aging))}
    if o.get("--digest-dir"):
        for k, h in dg.items():
            with open(os.path.join(o["--digest-dir"], f"seed-{k}.sha256"), "w") as f:
                f.write(h + "\n")
    feed = ["PRAGMA journal_mode=WAL;"] + sql + aging + ["PRAGMA wal_checkpoint(TRUNCATE);"]
    r = subprocess.run([sq3, path], input="\n".join(feed) + "\n", capture_output=True, text=True, timeout=3600)
    if r.returncode != 0:
        sys.exit(f"fixture.py sqlite: {sq3} rc {r.returncode}: {r.stderr[-400:]}")
    q = subprocess.run([sq3, path, "PRAGMA page_count; PRAGMA page_size; SELECT count(*), sum(v) FROM t;"],
                       capture_output=True, text=True, timeout=600)
    lines = q.stdout.split()
    eng = int(lines[0]) * int(lines[1]) if q.returncode == 0 and len(lines) >= 3 else None
    print(json.dumps({"file": path, "rows": rows, "age_updates": age, "engine_bytes": eng,
                      "count_sum": lines[2] if len(lines) >= 3 else None, "stream_sha256": dg,
                      "maintenance": "journal_mode=WAL; load; aged %d; wal_checkpoint(TRUNCATE)" % age}))


def selftest():
    streams = {"sql": "aa" * 32, "age": "bb" * 32}
    rb = {"count": 10000, "sum": 0, "row_hash": 83937210371}
    base = {"system": "pg18-d2", "rows": 10000, "age_updates": 0, "prebranch": 0, "live_branches": 0,
            "gen_seed_sha256": "ab" * 32, "du_bytes": 9000000, "engine_bytes": 8900000, "extents": {"t": 3},
            "maintenance": ["VACUUM", "CHECKPOINT"], "stream_sha256": dict(streams), "expected_streams": dict(streams),
            "readback": dict(rb), "expected_readback": dict(rb)}

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
        # lead review 62430d8bf..b49fb656a HIGH 1: the live-branch count every cell runs at, MEASURED after prebranch
        # (count_branches, main excluded), is the same on every system and is the requested PREBRANCH
        ("a system's measured live branches differ", [v(), v(system="dolt", live_branches=24)], False),
        ("measured live branches are not the requested prebranch",
         [v(prebranch=20, live_branches=24), v(system="dolt", prebranch=20, live_branches=24)], False),
        ("no measured live branches", [v(live_branches=KeyError), v(system="dolt", live_branches=KeyError)], False),
        ("20 requested, 20 measured on every system",
         [v(prebranch=20, live_branches=20), v(system="dolt", prebranch=20, live_branches=20)], True),
        # MED 3: what was piped (the digest of the bytes the write path sent) and what the engine READ BACK (count,
        # sum(v), the order-independent row hash), each against the generator
        ("the engine read back another table than the generator wrote",
         [v(), v(system="dolt", readback=dict(rb, row_hash=rb["row_hash"] + 1))], False),
        ("the engine read back another sum", [v(), v(system="b1", readback=dict(rb, sum=1))], False),
        ("the piped parent stream is not the generator's",
         [v(), v(system="doltgres", stream_sha256=dict(streams, sql="cc" * 32))], False),
        ("no read-back", [v(), v(system="dolt", readback=KeyError)], False),
        ("no stream digest", [v(), v(system="dolt", stream_sha256=KeyError)], False),
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
    if len(sys.argv) >= 3 and sys.argv[1] == "write":
        write(sys.argv[2:])
        sys.exit(0)
    if len(sys.argv) >= 3 and sys.argv[1] == "sqlite":
        sqlite(sys.argv[2:])
        sys.exit(0)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
