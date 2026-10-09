#!/usr/bin/env python3
"""gen_seed.py -- the ONE parent fixture generator for every system (PG, Doltgres, Dolt, the SQLite B1 baseline, and
ours: gate-6 review, t3run item 4). The table: t(id INTEGER PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL), ids
1..ROWS, v = 0, a deterministic 100-char pad. Plain SQL accepted by PostgreSQL, Doltgres, Dolt (MySQL dialect) and
SQLite; INSERTs in batches of 1000 rows.

  gen_seed.py ROWS                        the parent SQL (unchanged form, used by pg18.sh, dolt.sh, doltgres.sh)
  gen_seed.py sql --rows R [--digest-out FILE]
                                          the same; --digest-out: the sha256 of the bytes written, into FILE (MED 3)
  gen_seed.py age --rows R --updates K [--seed S] [--digest-out FILE]
                                          K random single-row UPDATEs, one statement each (autocommit: each committed),
                                          the same ids and values for every system (PREREG §7 / amendment 52 ages with
                                          1e5); deterministic for (R, K, S); K = 0 writes nothing
  gen_seed.py expect --rows R [--updates K] [--seed S]
                                          'count|sum|row_hash' the engine must read back from t
  gen_seed.py readback-sql --dialect pg|mysql
                                          the engine's own read-back query of that triple (pg: PostgreSQL, Doltgres)
  gen_seed.py readback-sqlite FILE        the same triple read from an SQLite file
  gen_seed.py digest --rows R [--updates K] [--seed S]
                                          sha256 of the exact SQL stream (parent, then aging): fixture.json's
                                          gen_seed_sha256, the same for every system that loaded it
  gen_seed.py sum --rows R [--updates K] [--seed S]
                                          sum(v) over t after the aging (0 fresh): what isolation checks expect
  gen_seed.py rows-for --bytes B          the row count for a parent of about B bytes (128 logical bytes a row:
                                          id 4 + v 4 + pad 120), so every system is sized by one rule
  gen_seed.py selftest
"""
import hashlib
import sys

ALPHA = "abcdefghijklmnopqrstuvwxyz0123456789"


def xorshift32(x):
    x ^= (x << 13) & 0xFFFFFFFF
    x ^= x >> 17
    x ^= (x << 5) & 0xFFFFFFFF
    return x


def sql_lines(rows):
    """The parent: byte for byte what gen_seed.py ROWS has always printed (one pad stream across batches)."""
    # INTEGER PRIMARY KEY (lead review 62430d8bf..b49fb656a MED 11): in SQLite only this spelling makes id the rowid, so
    # B1's parent has no separate sqlite_autoindex_t_1 that every first write searches; PostgreSQL, Dolt and Doltgres
    # read INTEGER as INT, so one stream still serves every system. (cd88722e0..MED 11 loaded INT PRIMARY KEY.)
    yield "CREATE TABLE t (id INTEGER PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL);"
    x = 2463534242
    for start in range(1, rows + 1, 1000):
        vals = []
        for i in range(start, min(start + 1000, rows + 1)):
            chars = []
            for _ in range(100):
                x = xorshift32(x)
                chars.append(ALPHA[x % 36])
            vals.append("(%d,0,'%s')" % (i, "".join(chars)))
        yield "INSERT INTO t (id, v, pad) VALUES " + ",".join(vals) + ";"


def age_lines(rows, updates, seed=1):
    """K random single-row UPDATEs, each its own statement (autocommit commits each): a uniform id in 1..rows and a
    value from the same stream, so every system applies the same writes in the same order."""
    x = (0x9E3779B9 ^ (seed * 2654435761)) & 0xFFFFFFFF or 1
    for _ in range(updates):
        x = xorshift32(x)
        i = 1 + x % rows
        x = xorshift32(x)
        yield "UPDATE t SET v = %d WHERE id = %d;" % (x % 1000000, i)


def aged_sum(rows, updates=0, seed=1):
    """sum(v) over t after the aging: the isolation checks' expected parent value (0 when fresh)."""
    last = {}
    for ln in age_lines(rows, updates, seed):
        v, i = ln[len("UPDATE t SET v = "):-1].split(" WHERE id = ")
        last[int(i)] = int(v)
    return sum(last.values())


def digest(rows, updates=0, seed=1):
    h = hashlib.sha256()
    for ln in sql_lines(rows):
        h.update(ln.encode() + b"\n")
    for ln in age_lines(rows, updates, seed):
        h.update(ln.encode() + b"\n")
    return h.hexdigest()


def rows_for(nbytes):
    return max(1, -(-int(nbytes) // 128))


# ---- what the engine must READ BACK (lead review 62430d8bf..b49fb656a MED 3: the fixture guard compared its own
# inputs; now each job records the digest of the bytes it actually piped and the engine's own read-back of t)
def stream_digests(rows, updates=0, seed=1):
    """sha256 of each stream exactly as `gen_seed.py sql` and `gen_seed.py age` write it (every line + "\\n")."""
    out = {}
    for k, lines in (("sql", sql_lines(rows)), ("age", age_lines(rows, updates, seed))):
        h = hashlib.sha256()
        for ln in lines:
            h.update(ln.encode() + b"\n")
        out[k] = h.hexdigest()
    return out


def hash_row(i, v, pad):
    return int(hashlib.md5(f"{i}:{v}:{pad}".encode()).hexdigest()[:6], 16)


def row_hash(rows, updates=0, seed=1):
    """The order-independent row hash: sum over t of the first 6 hex digits of md5("id:v:pad") (exact in every engine:
    a 24-bit term, so 5e8 rows stay below 2^53, where Dolt's SUM is a double). Streams the parent once, holding only
    the aged rows' final values (O(K) memory, not O(rows))."""
    last = {}
    for ln in age_lines(rows, updates, seed):
        v, i = ln[len("UPDATE t SET v = "):-1].split(" WHERE id = ")
        last[int(i)] = int(v)
    total = 0
    for ln in sql_lines(rows):
        if ln.startswith("INSERT"):
            for tup in ln[ln.index("VALUES ") + 7:-1].split("),("):
                i, v, pad = tup.strip("()").split(",", 2)
                i = int(i)
                total += hash_row(i, last.get(i, int(v)), pad.strip("'"))
    return total


def readback_sql(dialect):
    """The engine's own read-back of t as 'count|sum|row_hash', in its dialect (pg: PostgreSQL and Doltgres, which has
    no bit casts or get_byte, so the hex digits are decoded with ascii(); mysql: Dolt, whose SUM over integers is a
    double, so both sums are CAST to DECIMAL(20,0) to print as integers)."""
    if dialect == "mysql":
        return ("SELECT count(*), CAST(SUM(v) AS DECIMAL(20,0)), "
                "CAST(SUM(CONV(SUBSTR(MD5(CONCAT(id, ':', v, ':', pad)), 1, 6), 16, 10)) AS DECIMAL(20,0)) FROM t")
    if dialect == "pg":
        h = "md5(id::text || ':' || v::text || ':' || pad)"

        def dig(k):
            c = f"ascii(substr({h}, {k}, 1))"
            return f"(CASE WHEN {c} > 57 THEN {c} - 87 ELSE {c} - 48 END)"
        expr = " + ".join(f"{dig(k)} * {16 ** (6 - k)}" for k in range(1, 7))
        return f"SELECT count(*), sum(v), sum({expr}) FROM t"
    raise ValueError(f"unknown dialect {dialect}")


def expect(rows, updates=0, seed=1):
    """'count|sum|row_hash' the engine must read back from t after loading these streams."""
    return f"{rows}|{aged_sum(rows, updates, seed)}|{row_hash(rows, updates, seed)}"


def selftest():
    bad = n = 0

    def ok(name, cond):  # counts every case it is given (LOW 27: the total used to be a hard-coded number)
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), name)
        bad += not cond
        n += 1

    a = list(sql_lines(2500))
    ok("parent SQL: CREATE then 3 INSERT batches for 2500 rows", len(a) == 4 and a[0].startswith("CREATE TABLE t ("))
    ok("parent SQL is deterministic", a == list(sql_lines(2500)))
    ok("rows 1..2500, each once", sum(x.count("),(") + 1 for x in a[1:]) == 2500 if len(a) == 4 else False)
    ok("the first 1000 rows do not depend on ROWS", list(sql_lines(1000))[1:] == a[1:2])
    g = list(age_lines(2500, 300, 7))
    ok("aging: 300 single-row UPDATEs", len(g) == 300 and all(x.startswith("UPDATE t SET v = ") and " WHERE id = " in x
                                                               for x in g))
    ok("aging is deterministic for (rows, updates, seed)", g == list(age_lines(2500, 300, 7)))
    ok("aging ids stay in 1..rows", all(1 <= int(x.rsplit("= ", 1)[1].rstrip(";")) <= 2500 for x in g) if g else False)
    ok("another seed ages differently", g != list(age_lines(2500, 300, 8)))
    d0, d1 = digest(2500), digest(2500, 300, 7)
    ok("digest: 64 hex", len(d0) == 64 and all(c in "0123456789abcdef" for c in d0))
    ok("digest covers the aging", d0 != d1 and d1 == digest(2500, 300, 7))
    ok("digest covers the row count", d0 != digest(2501))
    ok("rows-for: 1 MB, 100 MB, 1 GB", (rows_for(1 << 20), rows_for(100 << 20), rows_for(1 << 30)) ==
       (8192, 819200, 8388608))
    # The value SQLite itself reported after loading this stream (fixture.py sqlite, 2500 rows, 300 updates, seed 1,
    # SQLite 3.53 on the Mac, 2026-10-08): count|sum = 2500|131727070. Not computed by the subject.
    ok("aged sum(v) = what SQLite read back after the same stream", aged_sum(2500, 300, 1) == 131727070)
    ok("a fresh parent sums to 0", aged_sum(2500, 0, 1) == 0)
    # MED 3: the order-independent row hash, sum over t of the first 6 hex digits of md5("id:v:pad"), as Dolt 2.4.1,
    # Doltgres 1.4.0 and PostgreSQL 18 READ IT BACK after loading this stream (2500 rows, aged 300, seed 1; local probe
    # 2026-10-09, all three: 2500|131727070|21429546430). Not computed by the subject.
    ok("row hash = what three engines read back after the same stream", row_hash(2500, 300, 1) == 21429546430)
    ok("expect() = count|sum|row_hash", expect(2500, 300, 1) == "2500|131727070|21429546430")
    sd = stream_digests(2500, 300, 1)
    # MED 11: the table's key is INTEGER PRIMARY KEY, a rowid alias in SQLite (INT PRIMARY KEY gave B1's parent an
    # extra sqlite_autoindex_t_1 that every first write searched); every other byte of the parent stream is still the
    # generator's of 62430d8bf, pinned (LOW 17) by putting the old CREATE line back: 351af3d4... at 10000 rows
    OLD_CREATE = "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL);"
    s10 = list(sql_lines(10000))
    ok("the parent's CREATE: id INTEGER PRIMARY KEY",
       s10[0] == "CREATE TABLE t (id INTEGER PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL);")
    ok("the rest of the parent stream is 62430d8bf's (351af3d4... with the old CREATE line)",
       hashlib.sha256("".join(ln + "\n" for ln in [OLD_CREATE] + s10[1:]).encode()).hexdigest() ==
       "351af3d4e0eca98b1ccdda69b548d7337357b0cba56fc4851fb928cf85e1f26a")
    ok("stream digests: an empty aging stream hashes as empty",
       stream_digests(2500, 0, 1)["age"] == hashlib.sha256(b"").hexdigest())
    import sqlite3
    db = sqlite3.connect(":memory:")
    db.executescript("\n".join(list(sql_lines(2500)) + list(age_lines(2500, 300, 1))))
    ok("SQLite: no sqlite_autoindex_t_1 (id is the rowid)",
       not db.execute("SELECT name FROM sqlite_schema WHERE name LIKE 'sqlite_autoindex_t%'").fetchall())
    plan = " ".join(str(r[-1]) for r in db.execute("EXPLAIN QUERY PLAN UPDATE t SET v = v + 1 WHERE id = 5"))
    ok(f"SQLite: the M1 statement searches t by its INTEGER PRIMARY KEY ({plan})", "INTEGER PRIMARY KEY" in plan)
    cnt, sv = db.execute("SELECT count(*), sum(v) FROM t").fetchone()
    rh = sum(hash_row(i, v, p) for i, v, p in db.execute("SELECT id, v, pad FROM t"))
    ok("SQLite read back the generator's triple", f"{cnt}|{sv}|{rh}" == expect(2500, 300, 1))
    db.close()
    ok("stream digests differ between aged and fresh", sd["age"] != stream_digests(2500, 0, 1)["age"])
    # The CI default fixture (ROWS 10000, AGE 200, seed 1): what PG 18.6, Doltgres 1.4.0 and SQLite read back in run
    # 37841577896 (every pg18, doltgres and b1 job's functional.txt: parent count|sum(v) = 10000|89963830; Dolt 2.4.1
    # printed the same value as 8.996383e+07, review of 62430d8bf..b49fb656a, HIGH 2). Not computed by the subject.
    ok("aged sum(v) at the CI default = what three servers read back", aged_sum(10000, 200, 1) == 89963830)
    print(f"gen_seed selftest: {n - bad}/{n}")
    return 1 if bad else 0


def arg(argv, name, default=None, conv=int):
    if name in argv:
        i = argv.index(name)
        return conv(argv[i + 1])
    if default is None:
        sys.exit(f"gen_seed.py: {name} is required")
    return default


if __name__ == "__main__":
    av = sys.argv[1:]
    out = sys.stdout
    if len(av) == 1 and av[0].isdigit():
        av = ["sql", "--rows", av[0]]
    if not av:
        sys.exit(__doc__)
    cmd = av[0]
    if cmd == "selftest":
        sys.exit(selftest())
    if cmd in ("sql", "age"):
        r = arg(av, "--rows")
        if r < 1:
            sys.exit("gen_seed.py: ROWS must be >= 1")
        lines = sql_lines(r) if cmd == "sql" else age_lines(r, arg(av, "--updates"), arg(av, "--seed", 1))
        # --digest-out FILE: the sha256 of the bytes this process WROTE (MED 3: the write path's own record of what
        # it piped), written only after the last byte went out
        h = hashlib.sha256()
        for ln in lines:
            b = ln + "\n"
            out.write(b)
            h.update(b.encode())
        out.flush()
        if "--digest-out" in av:
            with open(av[av.index("--digest-out") + 1], "w") as f:
                f.write(h.hexdigest() + "\n")
    elif cmd == "expect":
        print(expect(arg(av, "--rows"), arg(av, "--updates", 0), arg(av, "--seed", 1)))
    elif cmd == "readback-sql":
        print(readback_sql(arg(av, "--dialect", conv=str)))
    elif cmd == "readback-sqlite" and len(av) == 2:
        import sqlite3
        # a plain connection: a read-only open of a WAL database can fail for want of its -shm; only SELECTs run here
        con = sqlite3.connect(av[1])
        n = s = hsum = 0
        for i, v, pad in con.execute("SELECT id, v, pad FROM t"):
            n, s, hsum = n + 1, s + v, hsum + hash_row(i, v, pad)
        con.close()
        print(f"{n}|{s}|{hsum}")
    elif cmd == "digest":
        print(digest(arg(av, "--rows"), arg(av, "--updates", 0), arg(av, "--seed", 1)))
    elif cmd == "rows-for":
        print(rows_for(arg(av, "--bytes")))
    elif cmd == "sum":
        print(aged_sum(arg(av, "--rows"), arg(av, "--updates", 0), arg(av, "--seed", 1)))
    else:
        sys.exit(__doc__)
