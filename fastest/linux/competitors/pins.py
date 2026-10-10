#!/usr/bin/env python3
"""pins.py -- the registered versions and PG's shared_buffers (lane fastest-linux-comp; gate-6 review, t3run items 13
and 15). Every value comes from versions.tsv (one table) or /proc/meminfo, and every check refuses a mismatch.

  pins.py get SYSTEM ARCH KIND          the table's value (exit 1 with a message if absent): e.g. dolt amd64 tarball_sha256
  pins.py version SYSTEM                the registered version
  pins.py check-version SYSTEM FILE     exit 0 only when FILE (a version command's output) has, on its FIRST line and
                                        in that command's own form (FIRST_LINE), exactly the registered version
  pins.py check-binary SYSTEM ARCH FILE exit 0 only when FILE's sha256 is the table's binary_sha256 for SYSTEM, ARCH
  pins.py shared-buffers MEMINFO        PG's shared_buffers, 25% of MemTotal, as "<N>MB" (N rounded down)
  pins.py check-pg TSV MEMINFO          exit 0 only when pg18.sh settings' dump (name, setting, unit, source TSV) has
                                        shared_buffers in 8kB pages equal to that 25%
  pins.py selftest
"""
import hashlib
import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
TABLE = os.path.join(HERE, "versions.tsv")


def table(path=TABLE):
    rows = []
    for ln in open(path):
        if ln.startswith("#") or not ln.strip():
            continue
        f = ln.rstrip("\n").split("\t")
        if len(f) != 5:
            raise ValueError(f"versions.tsv: bad row {ln!r}")
        rows.append(dict(zip(("system", "version", "arch", "kind", "value"), f)))
    return rows


def get(system, arch, kind, path=TABLE):
    for r in table(path):
        if r["system"] == system and r["kind"] == kind and r["arch"] in (arch, "any"):
            return r["value"]
    return None


def version(system, path=TABLE):
    vs = {r["version"] for r in table(path) if r["system"] == system}
    return vs.pop() if len(vs) == 1 else None  # two versions of one system in the table is no registration


# Each version command's FIRST line, with the version as its own field (LOW 19: a whole-word search anywhere accepted
# 2.4.1-rc1, +dirty, 18.6devel and a later line that only mentioned the version)
FIRST_LINE = {"dolt": r"dolt version (\S+)", "doltgres": r"Doltgres version (\S+)",
              "postgresql": r"postgres \(PostgreSQL\) (\S+)(?: \(.*\))?"}


def check_version(system, text, path=TABLE):
    v = version(system, path)
    pat = FIRST_LINE.get(system)
    first = text.splitlines()[0].strip() if text.strip() else ""
    m = re.fullmatch(pat, first) if pat else None
    return bool(v) and m is not None and m.group(1) == v


def check_binary(system, arch, path, table_path=TABLE):
    """True only when the binary at PATH has versions.tsv's binary_sha256 for (SYSTEM, ARCH) (LOW 19: the tarball was
    checked at fetch time and the binary never again)."""
    want = get(system, arch, "binary_sha256", table_path)
    if not want:
        return False
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
    except OSError:
        return False
    return h.hexdigest() == want


def mem_total_kb(meminfo_text):
    m = re.search(r"^MemTotal:\s+(\d+)\s+kB", meminfo_text, re.M)
    return int(m.group(1)) if m else None


def shared_buffers_mb(meminfo_text):
    kb = mem_total_kb(meminfo_text)
    if kb is None:
        raise ValueError("no MemTotal in meminfo")
    return kb // 4 // 1024


def check_pg(settings_tsv_text, meminfo_text):
    why = []
    if mem_total_kb(meminfo_text) is None:
        return ["no MemTotal in meminfo: 25% cannot be computed"]
    want_mb = shared_buffers_mb(meminfo_text)
    row = next((ln.split("\t") for ln in settings_tsv_text.splitlines() if ln.split("\t")[0] == "shared_buffers"), None)
    if row is None or len(row) < 2 or not row[1].isdigit():
        return ["no shared_buffers row in pg_settings"]
    # pg18.sh settings dumps (name, setting, unit, source) (LOW 21: the old dump had no unit column, and a dict keyed
    # by the source column fell back to 8 kB for every value); shared_buffers is reported in 8kB pages, and any other
    # or missing unit is refused rather than guessed
    if len(row) < 3 or row[2] != "8kB":
        return [f"shared_buffers unit {row[2] if len(row) > 2 else None!r} is not 8kB"]
    got = int(row[1]) * 8 * 1024
    if got != want_mb << 20:
        why.append(f"shared_buffers {got >> 20} MB, not 25% of MemTotal = {want_mb} MB")
    return why


def selftest():
    bad = 0
    n = 0

    def ok(name, cond):  # counts its own cases (the total was hard-coded)
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), name)
        bad += not cond
        n += 1

    with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as f:
        f.write("# c\ndolt\t2.4.1\tamd64\ttarball_sha256\taa\ndolt\t2.4.1\tarm64\ttarball_sha256\tbb\n"
                "postgresql\t18.6\tany\tpgdg_package\t18.6-1.pgdg24.04+2\n"
                "doltgres\t1.4.0\tamd64\tbinary_sha256\t" + hashlib.sha256(b"doltgres-binary").hexdigest() + "\n")
        p = f.name
    with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as f2:
        f2.write("dolt\t2.4.1\tamd64\ttarball_sha256\taa\ndolt\t2.5.0\tarm64\ttarball_sha256\tbb\n")
        p2 = f2.name
    ok("get: the amd64 digest", get("dolt", "amd64", "tarball_sha256", p) == "aa")
    ok("get: the arm64 digest (LOW 20)", get("dolt", "arm64", "tarball_sha256", p) == "bb")
    ok("get: an 'any' row matches every arch", get("postgresql", "arm64", "pgdg_package", p) == "18.6-1.pgdg24.04+2")
    ok("get: no row -> None", get("doltgres", "amd64", "tarball_sha256", p) is None)
    ok("version", version("dolt", p) == "2.4.1")
    ok("version: a table with two versions of one system registers none (LOW 20)",
       version("dolt", p2) is None and not check_version("dolt", "dolt version 2.4.1\n", p2))
    ok("check-version: the registered version", check_version("dolt", "dolt version 2.4.1\n", p))
    ok("check-version: another version refused", not check_version("dolt", "dolt version 2.3.5\n", p))
    ok("check-version: a version that only starts with it refused", not check_version("dolt", "dolt version 2.4.10", p))
    ok("check-version: PG 18.6 from postgres --version",
       check_version("postgresql", "postgres (PostgreSQL) 18.6 (Ubuntu 18.6-1.pgdg24.04+2)", p))
    ok("check-version: PG 18.7 refused", not check_version("postgresql", "postgres (PostgreSQL) 18.7 (Ubuntu)", p))
    # LOW 19: the version is the first line's own field, equal to the registered one: no suffix, no other line
    for text in ("dolt version 2.4.1-rc1\n", "dolt version 2.4.1+dirty\n", "dolt version 12.4.1\n",
                 "dolt version 2.4.1 (built from source)\n", "warning: something\ndolt version 2.4.1\n",
                 "dolt version 2.5.0\nupgrade from 2.4.1\n"):
        ok(f"check-version: {text.strip()!r} refused", not check_version("dolt", text, p))
    ok("check-version: postgres 18.6devel refused", not check_version("postgresql", "postgres (PostgreSQL) 18.6devel\n", p))
    ok("check-version: Doltgres's own first line", check_version("doltgres", "Doltgres version 1.4.0\nmore\n", p))
    # LOW 19: the binary itself is the registered one (versions.tsv binary_sha256)
    with tempfile.NamedTemporaryFile("wb", delete=False) as b:
        b.write(b"doltgres-binary")
        bp = b.name
    with tempfile.NamedTemporaryFile("wb", delete=False) as b2:
        b2.write(b"doltgres-binary, patched")
        bp2 = b2.name
    ok("check-binary: the registered binary", check_binary("doltgres", "amd64", bp, p))
    ok("check-binary: another binary refused", not check_binary("doltgres", "amd64", bp2, p))
    ok("check-binary: no binary_sha256 row refused", not check_binary("doltgres", "arm64", bp, p))
    ok("check-binary: a missing file refused", not check_binary("doltgres", "amd64", bp + ".gone", p))
    for x in (p, p2, bp, bp2):
        os.unlink(x)
    mem = "MemTotal:       16374968 kB\nMemFree:  100 kB\n"
    ok("shared_buffers = 25% of MemTotal, MB rounded down (16374968 kB -> 3997MB)", shared_buffers_mb(mem) == 3997)
    # pg18.sh settings' format (LOW 21): name, setting, unit, source -- shared_buffers' setting is in its unit, 8kB
    good = "fsync\ton\t\tdefault\nshared_buffers\t511616\t8kB\tconfiguration file\n"  # 3997 MB = 511616 x 8 kB
    ok("check-pg: 25% accepted", check_pg(good, mem) == [])
    ok("check-pg: initdb's 128 MB refused (run 37809124979's value)",
       check_pg("shared_buffers\t16384\t8kB\tconfiguration file\n", mem) != [])
    ok("check-pg: one page more refused (LOW 20: a '<' instead of '!=' mutant)",
       check_pg("shared_buffers\t511617\t8kB\tconfiguration file\n", mem) != [])
    ok("check-pg: one page less refused", check_pg("shared_buffers\t511615\t8kB\tconfiguration file\n", mem) != [])
    ok("check-pg: 12 GB refused", check_pg("shared_buffers\t1572864\t8kB\tconfiguration file\n", mem) != [])
    ok("check-pg: a setting in another unit is refused, not guessed (LOW 21)",
       check_pg("shared_buffers\t511616\tkB\tconfiguration file\n", mem) != [])
    ok("check-pg: no unit column refused", check_pg("shared_buffers\t511616\n", mem) != [])
    ok("check-pg: no shared_buffers row refused", check_pg("fsync\ton\t\tdefault\n", mem) != [])
    ok("check-pg: no MemTotal refused", check_pg(good, "MemFree: 1 kB\n") != [])
    print(f"pins selftest: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    a = sys.argv[1:]
    if a == ["selftest"]:
        sys.exit(selftest())
    if len(a) == 4 and a[0] == "get":
        v = get(a[1], a[2], a[3])
        if v is None:
            sys.exit(f"pins.py: no {a[3]} for {a[1]} on {a[2]} in versions.tsv")
        print(v)
        sys.exit(0)
    if len(a) == 2 and a[0] == "version":
        v = version(a[1])
        if v is None:
            sys.exit(f"pins.py: no version for {a[1]}")
        print(v)
        sys.exit(0)
    if len(a) == 3 and a[0] == "check-version":
        t = open(a[2], errors="replace").read()
        if check_version(a[1], t):
            print(f"ok: {a[1]} {version(a[1])}")
            sys.exit(0)
        print(f"REFUSED: {a[1]} is not the registered {version(a[1])}: {t.strip()[:200]}")
        sys.exit(1)
    if len(a) == 4 and a[0] == "check-binary":
        if check_binary(a[1], a[2], a[3]):
            print(f"ok: {a[1]} {a[2]} binary {a[3]} is the registered one")
            sys.exit(0)
        print(f"REFUSED: {a[3]} is not the registered {a[1]} {a[2]} binary (versions.tsv binary_sha256 "
              f"{get(a[1], a[2], 'binary_sha256')})")
        sys.exit(1)
    if len(a) == 2 and a[0] == "shared-buffers":
        print(f"{shared_buffers_mb(open(a[1]).read())}MB")
        sys.exit(0)
    if len(a) == 3 and a[0] == "check-pg":
        why = check_pg(open(a[1]).read(), open(a[2]).read())
        print("ok" if not why else "REFUSED: " + "; ".join(why))
        sys.exit(0 if not why else 1)
    sys.exit(__doc__)
