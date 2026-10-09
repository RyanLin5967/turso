#!/usr/bin/env python3
"""gen_seed.py -- the ONE parent fixture generator for every system (PG, Doltgres, Dolt, the SQLite B1 baseline, and
ours: gate-6 review, t3run item 4). The table: t(id INT PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL), ids
1..ROWS, v = 0, a deterministic 100-char pad. Plain SQL accepted by PostgreSQL, Doltgres, Dolt (MySQL dialect) and
SQLite; INSERTs in batches of 1000 rows.

  gen_seed.py ROWS                        the parent SQL (unchanged form, used by pg18.sh, dolt.sh, doltgres.sh)
  gen_seed.py sql --rows R                the same
  gen_seed.py age --rows R --updates K [--seed S]
                                          K random single-row UPDATEs, one statement each (autocommit: each committed),
                                          the same ids and values for every system (PREREG §7 / amendment 52 ages with
                                          1e5); deterministic for (R, K, S)
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
    yield "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL);"
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


def selftest():
    bad = 0

    def ok(name, cond):
        nonlocal bad
        print(("PASS" if cond else "FAIL"), name)
        bad += not cond

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
    # The CI default fixture (ROWS 10000, AGE 200, seed 1): what PG 18.6, Doltgres 1.4.0 and SQLite read back in run
    # 37841577896 (every pg18, doltgres and b1 job's functional.txt: parent count|sum(v) = 10000|89963830; Dolt 2.4.1
    # printed the same value as 8.996383e+07, review of 62430d8bf..b49fb656a, HIGH 2). Not computed by the subject.
    ok("aged sum(v) at the CI default = what three servers read back", aged_sum(10000, 200, 1) == 89963830)
    n = 15
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
    if cmd == "sql":
        r = arg(av, "--rows")
        if r < 1:
            sys.exit("gen_seed.py: ROWS must be >= 1")
        for ln in sql_lines(r):
            out.write(ln + "\n")
    elif cmd == "age":
        for ln in age_lines(arg(av, "--rows"), arg(av, "--updates"), arg(av, "--seed", 1)):
            out.write(ln + "\n")
    elif cmd == "digest":
        print(digest(arg(av, "--rows"), arg(av, "--updates", 0), arg(av, "--seed", 1)))
    elif cmd == "rows-for":
        print(rows_for(arg(av, "--bytes")))
    elif cmd == "sum":
        print(aged_sum(arg(av, "--rows"), arg(av, "--updates", 0), arg(av, "--seed", 1)))
    else:
        sys.exit(__doc__)
