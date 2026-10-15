#!/usr/bin/env python3
"""fixture.py -- one parent fixture for every system (lane fastest-linux-comp; gate-6 review, t3run item 4).

Every system times its creates against the same parent: gen_seed.py's table t at ROWS rows, aged by AGE committed
random single-row UPDATEs (0 = fresh, PREREG §7 / amendment 52: 1e5), with PREBRANCH live branches made before the
cells (each with one private write: the system's own M1 op, untimed). Each job writes RAW/fixture.json:

  {"system", "rows", "age_updates", "prebranch", "live_branches" (MEASURED after prebranch: the live-branch count N
   every cell runs at, main excluded; lead review HIGH 1), "gen_seed_sha256" (gen_seed.py's digest of the SQL stream
   for these rows and aging), "du_bytes" (du of the --du paths when the record is written, AFTER the prebranch: the
   parent's files for PG and B1, but the WHOLE store, branches included, for Dolt and Doltgres -- not comparable
   across systems), "parent_du_bytes" (comp review 1 LOW 26: the same paths' du BEFORE the prebranch, the parent
   alone on every system: the comparable size), "store_du_at_n_bytes" and "store_du_paths" (the whole store at N,
   after the prebranch), "engine_bytes" (as the engine reports it; null only with "engine_bytes_why"), "extents" (data
   file -> extent count), "maintenance" (the post-load steps run)}

  fixture.py compare JSON...   exit 0 only when every fixture.json names the same rows, age_updates, prebranch,
                               live_branches and gen_seed_sha256, each one's live_branches equals its prebranch, and
                               each has du_bytes and either engine_bytes or a reason
  fixture.py write OUT --system S --rows R --age K --prebranch N --live L --du PATH... [--engine-bytes B |
                   --engine-why W] [--extents FILE...] [--maintenance TEXT] --parent-du-bytes B --store-du PATH...
                               OUT = fixture.json: du_bytes = `du -sB1` over the PATHs (allocated bytes), extents =
                               filefrag's count per FILE, gen_seed_sha256 = gen_seed.py's digest for (R, K, seed 1);
                               parent_du_bytes = B, run_system.sh's du of the same paths before the prebranch, and
                               store_du_at_n_bytes = `du -sB1` over the --store-du PATHs now (LOW 26)
  fixture.py sqlite FILE --rows R --age K --sqlite3 BIN [--digest-dir DIR]
                               the parent as an SQLite file, the way B1 (and ours, which opens SQLite files) get it:
                               journal_mode=WAL, gen_seed.py's SQL, its K aging UPDATEs (each autocommitted), then
                               the documented maintenance, a TRUNCATE checkpoint; prints the engine's size
                               (page_count x page_size) and the fed streams' digests (also DIR/seed-{sql,age}.sha256)
  fixture.py write ... --evidence 'k=v ...'
                               (comp review 1 LOW 24) the maintenance's outcomes as the seed read them back (PG
                               last_vacuum, last_analyze, redo_after_vacuum; Dolt/Doltgres gc_verdict; B1 journal_mode,
                               header_18_19, checkpoints), checked by compare with the steps before the aging
                               (REGISTERED_PRE_AGE, REGISTERED_OUTCOMES)
  fixture.py write ... --streams SQL.sha256 AGE.sha256 --readback 'COUNT|SUM|ROW_HASH'
                               (MED 3) the write path's own stream digests and the engine's read-back of t (gen_seed.py
                               readback-sql / readback-sqlite), recorded beside the generator's expected values
                               (gen_seed.facts, the job's one generator pass beyond the seed's stream); write exits
                               non-zero when either is missing or differs (comp review 1 MED 10), and compare refuses
                               such a job too. The size tolerance against ours is not implemented here (competitor
                               half only; ours is fastest-linux's)
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


KEYS = ("rows", "age_updates", "prebranch", "live_branches", "gen_seed_sha256", "schema_sha256", "stream_sha256",
        "readback")


# The registered post-load maintenance after the aging step (PREREG §7: PG VACUUM, CHECKPOINT; Dolt/Doltgres
# dolt_commit then dolt_gc; the SQLite B1 parent a TRUNCATE checkpoint), as each seed writes it -- run whether or not
# the parent was aged (lead review 62430d8bf..b49fb656a MED 10: Dolt/Doltgres ran their GC only when aged).
REGISTERED_MAINT = {"pg18-d2": ["VACUUM ANALYZE", "CHECKPOINT"], "dolt": ["DOLT_COMMIT age", "DOLT_GC"],
                    "doltgres": ["dolt_commit age", "dolt_gc"], "b1": ["wal_checkpoint(TRUNCATE)"]}


# Comp review 1 LOW 24: the steps before the aging (PREREG §7: "loaded, checkpointed, then aged") and what each
# system's maintenance must have DONE, read back by its seed (maintenance_evidence; the text alone was trusted):
# PG's VACUUM and ANALYZE stamped t (pg_stat_user_tables) and the last checkpoint's redo point is after the VACUUM;
# Dolt's and Doltgres's GC verdict (fthelp.py gcverdict) is ok; B1's file is in WAL mode (header bytes 18-19 = 2, 2)
# and both TRUNCATE checkpoints replied 0|0|0 (not blocked, no frame left in the log).
REGISTERED_PRE_AGE = {"pg18-d2": ["CHECKPOINT"], "dolt": ["DOLT_COMMIT seed"], "doltgres": ["dolt_commit seed"],
                      "b1": ["journal_mode=WAL", "load", "wal_checkpoint(TRUNCATE)"]}
REGISTERED_OUTCOMES = {"pg18-d2": {"last_vacuum": "true", "last_analyze": "true", "redo_after_vacuum": "true"},
                       "dolt": {"gc_verdict": "ok"}, "doltgres": {"gc_verdict": "ok"},
                       "b1": {"journal_mode": "wal", "header_18_19": "2,2", "checkpoints": "0|0|0;0|0|0"}}


def maint_problem(f):
    """Why a fixture's maintenance record is not its system's registered one (None = it is): the steps after the aging
    (MED 10), the steps before it, and the recorded outcomes (LOW 24)."""
    s = f.get("system")
    want = REGISTERED_MAINT.get(s)
    if want is None:
        return f"{s}: no registered post-load maintenance"
    m = f.get("maintenance")
    items = [x.strip() for x in m.split(";")] if isinstance(m, str) else None
    if not items:
        return f"{s}: no maintenance record"
    aged = next((i for i, x in enumerate(items) if x.startswith("aged ")), None)
    if aged is None:
        return f"{s}: maintenance {m!r} has no aging step"
    if items[aged + 1:] != want:
        return f"{s}: post-load maintenance {items[aged + 1:]} is not the registered {want}"
    if items[:aged] != REGISTERED_PRE_AGE[s]:
        return f"{s}: the steps before the aging {items[:aged]} are not the registered {REGISTERED_PRE_AGE[s]}"
    ev, wo = f.get("maintenance_evidence"), REGISTERED_OUTCOMES[s]
    if not isinstance(ev, dict):
        return f"{s}: no maintenance evidence (what the maintenance did, read back by the seed)"
    bad = {k: ev.get(k) for k in wo if ev.get(k) != wo[k]}
    if bad:
        return f"{s}: maintenance outcomes {bad} are not the registered {({k: wo[k] for k in bad})}"
    return None


def evidence(text):
    """'k=v k=v ...' (a seed's 'evidence:' line) -> {k: v}; None for an empty or unparsable line."""
    pairs = (text or "").split()
    if not pairs or not all(re.fullmatch(r"[a-z0-9_]+=\S+", p) for p in pairs):
        return None
    return dict(p.split("=", 1) for p in pairs)


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
        mp = maint_problem(f)  # MED 10
        if mp:
            why.append(mp)
        if not isinstance(f.get("du_bytes"), int) or f["du_bytes"] <= 0:
            why.append(f"{s}: no du_bytes")
        for k in ("parent_du_bytes", "store_du_at_n_bytes"):  # LOW 26: the comparable sizes
            if not isinstance(f.get(k), int) or isinstance(f.get(k), bool) or f[k] <= 0:
                why.append(f"{s}: no {k} ({f.get(k)!r})")
        eb = f.get("engine_bytes")
        if not (isinstance(eb, int) and eb > 0) and not f.get("engine_bytes_why"):
            why.append(f"{s}: no engine_bytes and no reason")
    return why


def opts(av, multi=("--du", "--extents", "--streams", "--store-du")):
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
    gf = gen_seed.facts(rows, age, 1)  # MED 12: every generator value below from ONE pass over the stream
    fx = {"system": o["--system"], "rows": rows, "age_updates": age, "prebranch": int(o["--prebranch"]),
          "live_branches": int(lv) if lv.isdigit() else None,
          "gen_seed_sha256": gf["digest"], "du_paths": o.get("--du", []),
          # MED 11: the table's schema line, by itself (a schema change no longer hides inside the stream digest)
          "schema_sha256": hashlib.sha256((gen_seed.SCHEMA + "\n").encode()).hexdigest(),
          "du_bytes": du_bytes(o.get("--du", [])) if o.get("--du") else None,
          # LOW 26: the parent's du measured BEFORE the prebranch (run_system.sh), comparable across systems, and the
          # whole store's du now, at N (Dolt's and Doltgres's du_bytes above covers their whole store already)
          "parent_du_bytes": int(o["--parent-du-bytes"]) if o.get("--parent-du-bytes", "").isdigit() else None,
          "store_du_paths": o.get("--store-du", []),
          "store_du_at_n_bytes": du_bytes(o.get("--store-du", [])) if o.get("--store-du") else None,
          "engine_bytes": int(o["--engine-bytes"]) if o.get("--engine-bytes", "").isdigit() else None,
          "extents": {p: extent_count(p) for p in o.get("--extents", [])},
          "maintenance": o.get("--maintenance", ""),
          # LOW 24: what the maintenance did (the seed's 'evidence:' line), checked by compare
          "maintenance_evidence": evidence(o.get("--evidence", ""))}
    # MED 3: the write path's own digest files (gen_seed.py --digest-out, or fixture.py sqlite's) and the engine's
    # read-back triple, recorded beside what the generator says they must be
    sd = {}
    for k, p in zip(("sql", "age"), o.get("--streams", [])):
        try:
            sd[k] = open(p).read().strip() or None
        except OSError:
            sd[k] = None
    fx["stream_sha256"] = sd if len(sd) == 2 and all(sd.values()) else None
    fx["expected_streams"] = gf["streams"]
    m = re.fullmatch(r"(\d+)\|(\d+)\|(\d+)", o.get("--readback", "").strip())
    fx["readback"] = dict(zip(("count", "sum", "row_hash"), map(int, m.groups()))) if m else None
    fx["expected_readback"] = {k: gf[k] for k in ("count", "sum", "row_hash")}
    if fx["engine_bytes"] is None:
        # only a caller's stated reason excuses a missing size: an unread size is not a reason, so compare refuses it
        # (LOW 27: this set a default message and then overwrote it with None)
        fx["engine_bytes_why"] = o.get("--engine-why") or None
    with open(out, "w") as f:
        json.dump(fx, f, indent=1)
    print(json.dumps(fx))
    # comp review 1 MED 10: the job's own check of its parent, against facts() (the drivers' `gen_seed.py expect`, a
    # further pass over the parent, is gone): refused -- non-zero, after fixture.json is written for the record -- when
    # the engine's read-back is missing or differs, or the piped streams' digests are missing or differ
    why = []
    if fx["readback"] is None:
        why.append(f"no read-back of t as three counts (--readback [{o.get('--readback', '')}])")
    elif fx["readback"] != fx["expected_readback"]:
        why.append(f"the engine read back {fx['readback']}, the generator wrote {fx['expected_readback']}")
    if fx["stream_sha256"] != fx["expected_streams"]:
        why.append(f"piped stream digests {fx['stream_sha256']} are not the generator's {fx['expected_streams']}")
    if why:
        sys.exit("fixture.py write: REFUSED: " + "; ".join(why))


def sqlite(av):
    path, o = av[0], opts(av[1:])
    rows, age, sq3 = int(o["--rows"]), int(o["--age"]), o["--sqlite3"]
    if os.path.exists(path):
        sys.exit(f"fixture.py sqlite: {path} exists")
    # MED 12: the stream goes into sqlite3's stdin line by line as it is generated (the old feed held the whole SQL
    # about three times over: ~28 GB at the 10 GiB tier), and each stream's digest (MED 3, the same framing as gen_seed.py
    # --digest-out) is computed in that same pass. sqlite3's stdout and stderr go to temporary files, so neither pipe can
    # fill and stall the writer.
    import tempfile
    hs = {"sql": hashlib.sha256(), "age": hashlib.sha256()}
    with tempfile.TemporaryFile("w+") as so, tempfile.TemporaryFile("w+") as se:
        p = subprocess.Popen([sq3, path], stdin=subprocess.PIPE, stdout=so, stderr=se, text=True)
        try:
            p.stdin.write("PRAGMA journal_mode=WAL;\n")
            for k, lines in (("sql", gen_seed.sql_lines(rows)), ("age", gen_seed.age_lines(rows, age, 1))):
                for ln in lines:
                    b = ln + "\n"
                    p.stdin.write(b)
                    hs[k].update(b.encode())
                if k == "sql":  # LOW 24: loaded, checkpointed, then aged (PREREG §7)
                    p.stdin.write("PRAGMA wal_checkpoint(TRUNCATE);\n")
            p.stdin.write("PRAGMA wal_checkpoint(TRUNCATE);\n")
            p.stdin.close()
        except BrokenPipeError:
            pass  # sqlite3 died: its rc and stderr say why, below
        try:
            rc = p.wait(timeout=3600)
        except subprocess.TimeoutExpired:
            p.kill()
            sys.exit(f"fixture.py sqlite: {sq3} did not finish in 3600 s")
        se.seek(0)
        err = se.read()
        so.seek(0)
        replies = so.read().split()  # LOW 24: sqlite3's replies, read: "wal", then one row per checkpoint
    if rc != 0:
        sys.exit(f"fixture.py sqlite: {sq3} rc {rc}: {err[-400:]}")
    with open(path, "rb") as f:
        hdr = f.read(100)
    ev = {"journal_mode": replies[0] if replies else "none",
          "header_18_19": f"{hdr[18]},{hdr[19]}" if len(hdr) >= 20 else "short",
          "checkpoints": ";".join(replies[1:]) or "none"}
    dg = {k: h.hexdigest() for k, h in hs.items()}
    if o.get("--digest-dir"):  # written only after sqlite3 consumed the whole stream and exited 0
        for k, h in dg.items():
            with open(os.path.join(o["--digest-dir"], f"seed-{k}.sha256"), "w") as f:
                f.write(h + "\n")
    # the engine's own size; the parent's contents are judged by the read-back triple (gen_seed.py readback-sqlite,
    # MED 3), so no count|sum is recorded here (LOW 27: count_sum had no reader)
    q = subprocess.run([sq3, path, "PRAGMA page_count; PRAGMA page_size;"], capture_output=True, text=True, timeout=600)
    lines = q.stdout.split()
    ok = q.returncode == 0 and len(lines) == 2 and all(x.isdigit() for x in lines)
    eng = int(lines[0]) * int(lines[1]) if ok else None
    print(json.dumps({"file": path, "rows": rows, "age_updates": age, "engine_bytes": eng, "stream_sha256": dg,
                      "maintenance": "journal_mode=WAL; load; wal_checkpoint(TRUNCATE); aged %d; wal_checkpoint(TRUNCATE)"
                      % age, "evidence": " ".join(f"{k}={v}" for k, v in ev.items())}))


def selftest():
    streams = {"sql": "aa" * 32, "age": "bb" * 32}
    rb = {"count": 10000, "sum": 0, "row_hash": 83937210371}
    base = {"system": "pg18-d2", "rows": 10000, "age_updates": 0, "prebranch": 0, "live_branches": 0,
            "gen_seed_sha256": "ab" * 32, "schema_sha256": "5c" * 32, "du_bytes": 9000000, "engine_bytes": 8900000,
            "parent_du_bytes": 9000000, "store_du_at_n_bytes": 41000000,
            "extents": {"t": 3},
            "maintenance": ["VACUUM", "CHECKPOINT"], "stream_sha256": dict(streams), "expected_streams": dict(streams),
            "readback": dict(rb), "expected_readback": dict(rb)}

    # MED 10: each system's maintenance line as its seed writes it (seed.txt "maintenance: ..."), aged or not. Comp
    # review 1 LOW 24: B1 checkpoints before the aging too (PREREG §7: "loaded, checkpointed, then aged"), and each
    # system records its maintenance's OUTCOMES (maintenance_evidence), which compare checks, not the text alone
    maint_of = {"pg18-d2": "CHECKPOINT; aged 0; VACUUM ANALYZE; CHECKPOINT",
                "dolt": "DOLT_COMMIT seed; aged 0; DOLT_COMMIT age; DOLT_GC",
                "doltgres": "dolt_commit seed; aged 0; dolt_commit age; dolt_gc",
                "b1": "journal_mode=WAL; load; wal_checkpoint(TRUNCATE); aged 0; wal_checkpoint(TRUNCATE)"}
    ev_of = {"pg18-d2": {"last_vacuum": "true", "last_analyze": "true", "redo_after_vacuum": "true"},
             "dolt": {"gc_verdict": "ok"}, "doltgres": {"gc_verdict": "ok"},
             "b1": {"journal_mode": "wal", "header_18_19": "2,2", "checkpoints": "0|0|0;0|0|0", "autoindex": "0"}}

    def v(**kw):
        d = dict(base)
        d["maintenance"] = maint_of.get(kw.get("system", base["system"]), base["maintenance"])
        d["maintenance_evidence"] = dict(ev_of.get(kw.get("system", base["system"]), {}))
        d.update(kw)
        return {k: x for k, x in d.items() if x is not KeyError}
    ev = ev_of  # (the cases below change one outcome each)

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
        # MED 10: every system runs its registered post-load maintenance whether or not it aged the parent
        ("an unaged Dolt parent whose maintenance lacks DOLT_GC",
         [v(), v(system="dolt", maintenance="DOLT_COMMIT seed")], False),
        ("an unaged Doltgres parent without dolt_gc",
         [v(), v(system="doltgres", maintenance="dolt_commit seed; aged 0; dolt_commit age")], False),
        ("PG without its final CHECKPOINT", [v(maintenance="CHECKPOINT; aged 0; VACUUM ANALYZE")], False),
        # LOW 24: the pre-aging step of every system, and each maintenance's outcome
        ("PG without its pre-aging CHECKPOINT", [v(maintenance="aged 0; VACUUM ANALYZE; CHECKPOINT")], False),
        ("B1 without its pre-aging checkpoint (the old feed)",
         [v(), v(system="b1", maintenance="journal_mode=WAL; load; aged 0; wal_checkpoint(TRUNCATE)")], False),
        ("Dolt without its seed commit before the aging",
         [v(), v(system="dolt", maintenance="aged 0; DOLT_COMMIT age; DOLT_GC")], False),
        ("a Dolt GC whose recorded verdict is not ok",
         [v(), v(system="dolt", maintenance_evidence={"gc_verdict": "REFUSED: client rc 1"})], False),
        ("a Doltgres parent with no maintenance evidence", [v(), v(system="doltgres", maintenance_evidence=KeyError)],
         False),
        ("PG whose VACUUM left no last_vacuum", [v(maintenance_evidence=dict(ev["pg18-d2"], last_vacuum="false"))],
         False),
        ("PG whose ANALYZE left no last_analyze", [v(maintenance_evidence=dict(ev["pg18-d2"], last_analyze="false"))],
         False),
        ("PG whose last checkpoint's redo point is before the VACUUM",
         [v(maintenance_evidence=dict(ev["pg18-d2"], redo_after_vacuum="false"))], False),
        ("a B1 parent whose header is not WAL (bytes 18-19 = 1,1)",
         [v(), v(system="b1", maintenance_evidence=dict(ev["b1"], header_18_19="1,1"))], False),
        ("a B1 checkpoint that left frames (reply 0|5|5)",
         [v(), v(system="b1", maintenance_evidence=dict(ev["b1"], checkpoints="0|0|0;0|5|5"))], False),
        # comp review 1 LOW 26: the parent's du BEFORE the prebranch (comparable across systems: Dolt's and
        # Doltgres's du after it covers the whole store, branches included) and the whole store's at N after it
        ("no parent du before the prebranch", [v(), v(system="dolt", parent_du_bytes=KeyError)], False),
        ("no store du at N", [v(), v(system="doltgres", store_du_at_n_bytes=KeyError)], False),
        ("a store du at N that is not a count", [v(), v(system="b1", store_du_at_n_bytes="x")], False),
        ("a B1 parent with one checkpoint reply of two",
         [v(), v(system="b1", maintenance_evidence=dict(ev["b1"], checkpoints="0|0|0"))], False),
        ("a system with no registered maintenance", [v(), v(system="mystery", maintenance="none")], False),
        # MED 11: the schema line is compared by itself
        # comp review 1 LOW 30: schema_sha256 hashed the generator's own CREATE line, so it could never fire; what
        # MED 11 protects is B1's table as SQLite READS it -- id the rowid, no sqlite_autoindex_t_1 (fixture.py sqlite's
        # evidence) -- and the generator's line is already inside gen_seed_sha256
        ("a B1 parent whose table carries an autoindex (INT PRIMARY KEY)",
         [v(), v(system="b1", maintenance_evidence=dict(ev["b1"], autoindex="1"))], False),
        ("a B1 parent with no autoindex record",
         [v(), v(system="b1", maintenance_evidence={k: x for k, x in ev["b1"].items() if k != "autoindex"})], False),
        ("every system's registered maintenance, unaged",
         [v(), v(system="dolt"), v(system="doltgres"), v(system="b1")], True),
    ]
    bad = 0
    for name, fx, want in cases:
        why = compare(fx)
        got = not why
        print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
        bad += got != want

    # comp review 1 MED 10: write() itself refuses -- exits non-zero, after recording fixture.json -- a parent the
    # engine read back otherwise than the generator wrote, a piped stream that is not the generator's, and a job with
    # no read-back (the drivers' separate `gen_seed.py expect` pass is gone); and it makes ONE pass over the parent
    # (facts), so a job generates the parent twice: the seed's stream, then this
    import contextlib
    import io
    import tempfile
    rb_ok = "2500|131727070|21429546430"  # what Dolt, Doltgres and PG 18 read back after this stream (gen_seed selftest)
    sql_d = hashlib.sha256("".join(ln + "\n" for ln in gen_seed.sql_lines(2500)).encode()).hexdigest()
    age_d = hashlib.sha256("".join(ln + "\n" for ln in gen_seed.age_lines(2500, 300, 1)).encode()).hexdigest()

    def wr(readback, sqld=sql_d):
        """(exit status 0 or 1, gen_seed.sql_lines calls) of one write() of a 2500-row parent aged 300"""
        with tempfile.TemporaryDirectory() as td:
            sp, ap = os.path.join(td, "seed-sql.sha256"), os.path.join(td, "seed-age.sha256")
            for p, h in ((sp, sqld), (ap, age_d)):
                with open(p, "w") as f:
                    f.write(h + "\n")
            av = [os.path.join(td, "fixture.json"), "--system", "pg18-d2", "--rows", "2500", "--age", "300",
                  "--prebranch", "0", "--live", "0", "--engine-why", "selftest", "--maintenance", "selftest",
                  "--streams", sp, ap] + ([] if readback is None else ["--readback", readback])
            real, calls = gen_seed.sql_lines, []

            def counted(*a, **k):
                calls.append(a)
                return real(*a, **k)
            gen_seed.sql_lines = counted
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    write(av)
                code = 0
            except SystemExit as e:
                code = 0 if e.code in (None, 0) else 1
            finally:
                gen_seed.sql_lines = real
            return code, len(calls)
    wcases = [
        ("write: the engines' read-back and the generator's streams exit 0, after ONE pass over the parent",
         wr(rb_ok), (0, 1)),
        ("write: the engine read back another row hash", wr("2500|131727070|21429546431")[0], 1),
        ("write: the engine read back another count", wr("2499|131727070|21429546430")[0], 1),
        ("write: the piped parent stream is not the generator's", wr(rb_ok, sqld="cc" * 32)[0], 1),
        ("write: no read-back", wr(None)[0], 1),
        ("write: a read-back that is not three counts", wr("2500|x|21429546430")[0], 1),
    ]
    for name, got, want in wcases:
        print(("PASS" if got == want else "FAIL"), name, "->", got)
        bad += got != want
    n = len(cases) + len(wcases)
    print(f"fixture selftest: {n - bad}/{n}")
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
