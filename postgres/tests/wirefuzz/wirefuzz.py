#!/usr/bin/env python3
"""wirefuzz.py -- a WIRE-LEVEL DIFFERENTIAL FUZZER: random SQL sequences run against `tursopg --server` and against
PostgreSQL 18.6 (the oracle), over the simple and the extended protocol, compared statement by statement.

  wirefuzz.py run --tursopg BIN --pg-bin DIR --seeds A-B --out DIR [--protocol simple|extended|both]
                  [--statements N] [--control pg] [--plant] [--no-shrink]
  wirefuzz.py replay --tursopg BIN --pg-bin DIR --seed S --protocol P [--statements N]   (one seed, verbose)

SEEDS. Every sequence is a pure function of (seed, --statements): gen(seed) uses random.Random(seed) and nothing
else, so a finding is reproduced by its seed alone. A shard is one seed range.

COMPARED, per statement, on both sides: the SQLSTATE (exact; an error on one side only is a divergence), the
CommandComplete tags (exact), the RowDescription type OIDs (exact), the ReadyForQuery status, and the rows (as a
sequence when the statement orders every row by a key, else as a multiset; float columns by value, every other
column by its text). The branch functions, which PostgreSQL has no equivalent of, run on tursopg only and are
never compared.

ALLOWLIST (allowlist.json): the known divergences, each an EXACT tuple (aspect, the PostgreSQL value, ours, and
the template id when the entry names one), never a pattern. A divergence that matches an entry is counted under
that entry and, unless the entry says "stop", the sequence goes on (a type or tag difference leaves both
databases in the same state); one that matches none is a FINDING, and the sequence stops there (what follows may
only cascade from it).

SHRINKER. For each finding, delta debugging over the statements before it: a chunk is dropped when the shorter
sequence, run fresh on both sides, still ends in the same divergence (aspect, template, both values). The
shrunk sequence is written beside the finding.

CONTROLS. --control pg runs both sides against PostgreSQL (two fresh databases): every compared statement must
agree, so any divergence there is the fuzzer's own defect and the run exits 3 (the negative control). --plant
makes the second side's first T_CONST statement answer another constant ('planted divergence'); with --control
pg every finding must be that plant and there must be one (the positive control: exit 0 when reported, 3 when
not).

REFUSES (exit 2) a run that compared no statement. Exit 1 when there is a finding, 0 when there is none.
Stdlib only; correctness only (no timing is read or reported).
"""
import argparse
import decimal
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import proto  # noqa: E402

# ---------------------------------------------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------------------------------------------
TYPES = ["int", "bigint", "smallint", "text", "varchar(5)", "numeric(6,2)", "boolean", "double precision"]
TEXTS = ["", "a", "ab", "O'Brien", "x y", "abcdef", "Zz", "123", "été", "tab\there"]


def value(rnd, ty):
    if rnd.random() < 0.08:
        return None
    if ty in ("int", "bigint", "smallint"):
        edge = {"int": [2147483647, -2147483648], "bigint": [9223372036854775807], "smallint": [32767, -32768]}[ty]
        return rnd.choice([0, 1, -1, 7, 42, 100, rnd.randint(-1000, 1000)] + edge)
    if ty in ("text", "varchar(5)"):
        return rnd.choice(TEXTS)
    if ty == "numeric(6,2)":
        return rnd.choice(["0", "1.5", "-2.25", "9999.99", "1234.567", "0.005", "10000"])
    if ty == "boolean":
        return rnd.choice(["true", "false"])
    return rnd.choice(["0", "1.5", "-2.25", "0.1", "1e10", "3.0"])


def lit(v, ty):
    if v is None:
        return "NULL"
    if ty in ("text", "varchar(5)"):
        return "'" + v.replace("'", "''") + "'"
    if ty == "boolean":
        return v
    if ty in ("numeric(6,2)", "double precision"):
        return v if not v.startswith("-") else "(%s)" % v
    return str(v) if v >= 0 else "(%d)" % v


def gen(seed, n):
    """The sequence for `seed`: a list of statements {tid, lit, sql, params, copy, turso_only, ordered}."""
    rnd = random.Random(seed)
    tables, seq, ids, state = {}, [], {}, {"tx": False, "sp": []}
    tcount = [0]

    def add(tid, lit_sql, sql=None, params=None, copy=None, turso_only=False, ordered=False):
        seq.append({"tid": tid, "lit": lit_sql, "sql": sql or lit_sql, "params": params or [],
                    "copy": copy, "turso_only": turso_only, "ordered": ordered})

    def new_table():
        name = "t%d" % tcount[0]
        tcount[0] += 1
        cols = [("c%d" % i, rnd.choice(TYPES)) for i in range(rnd.randint(1, 3))]
        tables[name], ids[name] = cols, 0
        decl = ", ".join("%s %s" % c for c in cols)
        add("T_CREATE", "CREATE TABLE %s (id int PRIMARY KEY, %s)" % (name, decl))

    new_table()
    while len(seq) < n:
        if not tables:
            new_table()
            continue
        t = rnd.choice(sorted(tables))
        cols = tables[t]
        r = rnd.random()
        if r < 0.05:
            new_table()
        elif r < 0.22:
            ids[t] += 1
            vals = [value(rnd, ty) for _, ty in cols]
            i = ids[t] if rnd.random() > 0.1 else max(1, ids[t] - 1)
            names = "id, " + ", ".join(c for c, _ in cols)
            ret = " RETURNING id, %s" % cols[0][0] if rnd.random() < 0.3 else ""
            add("T_INSERT" if not ret else "T_INSERT_RET",
                "INSERT INTO %s (%s) VALUES (%d, %s)%s" % (t, names, i, ", ".join(lit(v, ty) for v, (_, ty) in zip(vals, cols)), ret),
                "INSERT INTO %s (%s) VALUES ($1, %s)%s" % (t, names, ", ".join("$%d" % (k + 2) for k in range(len(cols))), ret),
                [i] + vals)
        elif r < 0.27:
            rows = []
            for _ in range(rnd.randint(1, 3)):
                ids[t] += 1
                rows.append([str(ids[t])] + ["\\N" if v is None else str(v).replace("\t", "\\t")
                                              for v in (value(rnd, ty) for _, ty in cols)])
            data = "".join("\t".join(x) + "\n" for x in rows)
            add("T_COPY", "COPY %s (id, %s) FROM STDIN" % (t, ", ".join(c for c, _ in cols)), copy=data)
        elif r < 0.40:
            add("T_SELECT_ALL", "SELECT * FROM %s ORDER BY id" % t, ordered=True)
        elif r < 0.50:
            c, ty = rnd.choice(cols)
            v = value(rnd, ty)
            op = rnd.choice(["=", "<>", "<", ">="]) if ty != "boolean" else rnd.choice(["=", "<>"])
            add("T_SELECT_WHERE", "SELECT id, %s FROM %s WHERE %s %s %s ORDER BY id" % (c, t, c, op, lit(v, ty)),
                "SELECT id, %s FROM %s WHERE %s %s $1 ORDER BY id" % (c, t, c, op), [v], ordered=True)
        elif r < 0.56:
            nums = [c for c, ty in cols if ty in ("int", "bigint", "smallint", "numeric(6,2)", "double precision")]
            parts = ["count(*)"] + ["sum(%s)" % c for c in nums[:1]] + ["min(%s)" % c for c, _ in cols[:1]] \
                + ["max(%s)" % c for c, _ in cols[-1:]]
            add("T_SELECT_AGG", "SELECT %s FROM %s" % (", ".join(parts), t))
        elif r < 0.62:
            c, ty = rnd.choice(cols)
            expr = {"int": "%s + 1", "bigint": "%s * 2", "smallint": "%s - 1", "text": "%s || 'x'",
                    "varchar(5)": "length(%s)", "numeric(6,2)": "%s / 4", "boolean": "NOT %s",
                    "double precision": "%s * 0.5"}[ty] % c
            add("T_SELECT_EXPR", "SELECT id, %s FROM %s ORDER BY id" % (expr, t), ordered=True)
        elif r < 0.65:
            ty = rnd.choice(TYPES)
            add("T_CONST", "SELECT %s AS c" % lit(value(rnd, ty), ty))
        elif r < 0.73:
            c, ty = rnd.choice(cols)
            v, i = value(rnd, ty), rnd.randint(1, max(1, ids[t]))
            add("T_UPDATE", "UPDATE %s SET %s = %s WHERE id = %d" % (t, c, lit(v, ty), i),
                "UPDATE %s SET %s = $1 WHERE id = $2" % (t, c), [v, i])
        elif r < 0.78:
            i = rnd.randint(1, max(1, ids[t]))
            add("T_DELETE", "DELETE FROM %s WHERE id = %d" % (t, i), "DELETE FROM %s WHERE id = $1" % t, [i])
        elif r < 0.80:
            ty = rnd.choice(TYPES)
            c = "c%d" % len(cols)
            cols.append((c, ty))
            add("T_ALTER_ADD", "ALTER TABLE %s ADD COLUMN %s %s" % (t, c, ty))
        elif r < 0.81:
            c, _ = rnd.choice(cols)
            add("T_INDEX", "CREATE INDEX ON %s (%s)" % (t, c))
        elif r < 0.82:
            add("T_TRUNCATE", "TRUNCATE %s" % t)
        elif r < 0.83:
            del tables[t]
            add("T_DROP", "DROP TABLE %s" % t)
        elif r < 0.88:
            if not state["tx"]:
                state["tx"], state["sp"] = True, []
                add("T_BEGIN", "BEGIN")
            else:
                k = rnd.random()
                if k < 0.35:
                    state["tx"] = False
                    add("T_COMMIT", "COMMIT")
                elif k < 0.55:
                    state["tx"] = False
                    add("T_ROLLBACK", "ROLLBACK")
                elif k < 0.75:
                    sp = "s%d" % len(state["sp"])
                    state["sp"].append(sp)
                    add("T_SAVEPOINT", "SAVEPOINT %s" % sp)
                elif state["sp"]:
                    sp = rnd.choice(state["sp"])
                    if k < 0.9:
                        add("T_ROLLBACK_TO", "ROLLBACK TO SAVEPOINT %s" % sp)
                    else:
                        state["sp"] = state["sp"][:state["sp"].index(sp)]
                        add("T_RELEASE", "RELEASE SAVEPOINT %s" % sp)
        elif r < 0.90:
            add("T_BRANCH", "SELECT turso_branch_create('fz_%d_%d')" % (seed, len(seq)), turso_only=True)
        elif r < 0.93:
            add("T_ERR_MISSING", "SELECT * FROM nosuch_%d" % rnd.randint(0, 3))
        elif r < 0.96:
            add("T_ERR_NOTNULL", "INSERT INTO %s (id) VALUES (NULL)" % t)
        else:
            c, ty = rnd.choice(cols)
            add("T_SELECT_COUNT_WHERE", "SELECT count(*) FROM %s WHERE %s IS NULL" % (t, c))
    if state["tx"]:
        add("T_COMMIT", "COMMIT")
    return seq[:n] if len(seq) > n else seq


# ---------------------------------------------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------------------------------------------
def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def wait_port(port, proc, seconds=60):
    end = time.time() + seconds
    while time.time() < end:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError("server exited rc %s" % proc.returncode)
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not listen on %d" % port)


class Pg:
    """One PostgreSQL 18.6 cluster for the shard; every run gets a fresh database."""

    def __init__(self, bindir, work):
        self.bindir, self.dir, self.port = bindir, os.path.join(work, "pg"), free_port()
        subprocess.run([os.path.join(bindir, "initdb"), "-D", self.dir, "-U", "postgres", "--auth", "trust",
                        "--no-locale", "-E", "UTF8"], check=True, capture_output=True)
        subprocess.run([os.path.join(bindir, "pg_ctl"), "-D", self.dir, "-l", os.path.join(work, "pg.log"), "-w",
                        "-o", "-p %d -k %s -c listen_addresses=127.0.0.1 -c lc_messages=C" % (self.port, work),
                        "start"], check=True, capture_output=True)
        self.n = 0

    def fresh(self):
        self.n += 1
        name = "fz%d" % self.n
        c = proto.Conn(self.port, "postgres")
        r = c.simple("CREATE DATABASE %s" % name)
        c.close()
        if r.error:
            raise RuntimeError("CREATE DATABASE: %s" % (r.error,))
        return proto.Conn(self.port, name), None

    def stop(self):
        subprocess.run([os.path.join(self.bindir, "pg_ctl"), "-D", self.dir, "-m", "fast", "stop"],
                       capture_output=True)


class Turso:
    """A fresh `tursopg --server` on a fresh database file for every run."""

    def __init__(self, binary, work):
        self.binary, self.work, self.n = binary, work, 0

    def fresh(self):
        self.n += 1
        d = os.path.join(self.work, "tu%d" % self.n)
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
        port = free_port()
        log = open(os.path.join(d, "server.log"), "w")
        p = subprocess.Popen([self.binary, os.path.join(d, "w.db"), "--server", "127.0.0.1:%d" % port],
                             stdout=log, stderr=log)
        wait_port(port, p)
        return proto.Conn(port, "e5/main"), (p, d)

    @staticmethod
    def done(handle):
        if handle:
            p, d = handle
            p.terminate()
            try:
                p.wait(10)
            except subprocess.TimeoutExpired:
                p.kill()
            shutil.rmtree(d, ignore_errors=True)


def execute(conn, st, extended, sql=None):
    sql = sql or (st["sql"] if extended and st["params"] else st["lit"])
    try:
        if st["copy"] is not None:
            return conn.simple(st["lit"], copy_data=st["copy"]).as_dict()
        if extended:
            return conn.extended(sql, st["params"] if sql == st["sql"] else ()).as_dict()
        return conn.simple(sql).as_dict()
    except (EOFError, OSError, proto.Refused) as x:
        return {"rows": [], "oids": [], "tags": [], "error": ["CLIENT", "%s: %s" % (type(x).__name__, x)],
                "status": None}


# ---------------------------------------------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------------------------------------------
FLOAT = {700, 701}


def same_cell(a, b, oid):
    if a is None or b is None or a == b:
        return a == b
    if oid in FLOAT:
        try:
            x, y = float(a), float(b)
            return x == y or abs(x - y) <= 1e-12 * max(abs(x), abs(y))
        except ValueError:
            return False
    return False


def diffs(st, pg, tu):
    """Every way the two answers differ, as (aspect, pg value, ours value)."""
    out = []
    pe, te = (pg["error"] or [None])[0], (tu["error"] or [None])[0]
    if pe != te:
        out.append(("sqlstate", pe, te))
        return out
    if pg["status"] != tu["status"]:
        out.append(("status", pg["status"], tu["status"]))
    if pe is not None:
        return out
    if pg["tags"] != tu["tags"]:
        out.append(("tag", " | ".join(pg["tags"]), " | ".join(tu["tags"])))
    if len(pg["oids"]) != len(tu["oids"]):
        out.append(("columns", len(pg["oids"]), len(tu["oids"])))
        return out
    for a, b in zip(pg["oids"], tu["oids"]):
        if a != b:
            out.append(("oid", a, b))
    oids = pg["oids"]
    key = lambda r: json.dumps(r)
    pr, tr = pg["rows"], tu["rows"]
    if not st["ordered"]:
        pr, tr = sorted(pr, key=key), sorted(tr, key=key)
    if len(pr) != len(tr) or any(len(x) != len(y) or not all(same_cell(a, b, o) for a, b, o in zip(x, y, oids))
                                 for x, y in zip(pr, tr)):
        out.append(("rows", json.dumps(pg["rows"])[:400], json.dumps(tu["rows"])[:400]))
    return out


def allowed(allow, st, d):
    aspect, pv, tv = d
    for e in allow:
        if e["aspect"] == aspect and e["pg"] == pv and e["ours"] == tv and e.get("tid", st["tid"]) == st["tid"]:
            return e
    return None


def run_seq(seq, extended, pg, other, allow, plant=False):
    """Run `seq` fresh on both sides. Returns (compared, allowlisted ids, the first finding or None)."""
    a, ah = pg.fresh()
    b, bh = other.fresh()
    compared, hits, finding, planted = 0, [], None, False
    try:
        for i, st in enumerate(seq):
            if st["turso_only"]:
                if isinstance(other, Turso):
                    execute(b, st, extended)
                continue
            ra = execute(a, st, extended)
            sql_b = None
            if plant and not planted and st["tid"] == "T_CONST":
                sql_b, planted = "SELECT 'planted divergence' AS c", True
            rb = execute(b, st, extended, sql_b)
            compared += 1
            stop = False
            for d in diffs(st, ra, rb):
                e = allowed(allow, st, d)
                if e:
                    hits.append(e["id"])
                    stop = stop or e.get("stop", False)
                else:
                    finding = {"index": i, "tid": st["tid"], "aspect": d[0], "pg": d[1], "ours": d[2],
                               "sql": st["lit"], "params": st["params"], "pg_answer": ra, "ours_answer": rb}
                    break
            if finding or stop:
                break
    finally:
        a.close()
        b.close()
        if isinstance(other, Turso):
            Turso.done(bh)
    return compared, hits, finding


def signature(f):
    return (f["aspect"], f["tid"], json.dumps(f["pg"]), json.dumps(f["ours"]))


def shrink(seq, finding, extended, pg, other, allow, budget=120):
    """Delta debugging over the statements before the finding's; returns the shrunk sequence."""
    target, sig = seq[finding["index"]], signature(finding)
    pre, tries = seq[:finding["index"]], 0
    chunk = max(1, len(pre) // 2)
    while chunk >= 1 and tries < budget:
        i, changed = 0, False
        while i < len(pre) and tries < budget:
            cand = pre[:i] + pre[i + chunk:]
            tries += 1
            _, _, f = run_seq(cand + [target], extended, pg, other, allow)
            if f and f["index"] == len(cand) and signature(f) == sig:
                pre, changed = cand, True
            else:
                i += chunk
        if not changed:
            chunk //= 2
    return pre + [target], tries


# ---------------------------------------------------------------------------------------------------------------
def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["run", "replay"])
    ap.add_argument("--tursopg")
    ap.add_argument("--pg-bin", required=True)
    ap.add_argument("--seeds", default="")
    ap.add_argument("--seed", type=int)
    ap.add_argument("--out", default=".")
    ap.add_argument("--protocol", default="both", choices=["simple", "extended", "both"])
    ap.add_argument("--statements", type=int, default=60)
    ap.add_argument("--control", choices=["pg"])
    ap.add_argument("--plant", action="store_true")
    ap.add_argument("--no-shrink", action="store_true")
    ap.add_argument("--allowlist", default=os.path.join(HERE, "allowlist.json"))
    a = ap.parse_args(argv)
    allow = json.load(open(a.allowlist))
    ids = [e["id"] for e in allow]
    if len(set(ids)) != len(ids):
        sys.exit("REFUSED: duplicate allowlist ids")
    if a.mode == "replay":
        a.seeds = "%d-%d" % (a.seed, a.seed)
    lo, hi = (int(x) for x in a.seeds.split("-"))
    protos = ["simple", "extended"] if a.protocol == "both" else [a.protocol]
    os.makedirs(a.out, exist_ok=True)
    work = tempfile.mkdtemp(prefix="wirefuzz-", dir=os.environ.get("TMPDIR"))
    pg = Pg(a.pg_bin, work)
    other = Pg(a.pg_bin, os.path.join(work, "b")) if a.control == "pg" else None
    if a.control != "pg":
        if not a.tursopg:
            sys.exit("REFUSED: --tursopg is required unless --control pg")
        other = Turso(a.tursopg, work)
    findings = open(os.path.join(a.out, "findings.jsonl"), "a")
    summary = {"seeds": [lo, hi], "statements_per_seed": a.statements, "protocols": protos, "control": a.control,
               "plant": a.plant, "compared": 0, "runs": 0, "findings": 0, "allowlisted": {}}
    try:
        for seed in range(lo, hi + 1):
            seq = gen(seed, a.statements)
            for p in protos:
                ext = p == "extended"
                compared, hits, f = run_seq(seq, ext, pg, other, allow, a.plant)
                summary["runs"] += 1
                summary["compared"] += compared
                for h in hits:
                    summary["allowlisted"][h] = summary["allowlisted"].get(h, 0) + 1
                if f:
                    f.update({"seed": seed, "protocol": p})
                    if not a.no_shrink:
                        small, tries = shrink(seq, f, ext, pg, other, allow)
                        f["shrunk"] = [{"tid": s["tid"], "sql": s["lit"] if not ext or not s["params"] else s["sql"],
                                        "params": s["params"] if ext else [], "copy": s["copy"],
                                        "turso_only": s["turso_only"]} for s in small]
                        f["shrink_runs"] = tries
                    findings.write(json.dumps(f) + "\n")
                    findings.flush()
                    summary["findings"] += 1
                    if a.mode == "replay":
                        print(json.dumps(f, indent=1))
    finally:
        pg.stop()
        if a.control == "pg":
            other.stop()
        shutil.rmtree(work, ignore_errors=True)
    json.dump(summary, open(os.path.join(a.out, "summary-%d-%d.json" % (lo, hi)), "w"), indent=1)
    print(json.dumps(summary))
    if summary["compared"] == 0:
        print("REFUSED: no statement was compared", file=sys.stderr)
        return 2
    if a.plant:
        # The positive control: every finding must be the plant, and at least one must be found.
        planted = [json.loads(l) for l in open(os.path.join(a.out, "findings.jsonl"))]
        ok = [f for f in planted if "planted divergence" in json.dumps(f.get("ours_answer"))]
        if not ok or (a.control == "pg" and len(ok) != len(planted)):
            print("PLANT NOT REPORTED (or another divergence in the control): %d of %d findings are the plant"
                  % (len(ok), len(planted)), file=sys.stderr)
            return 3
        print("PLANT REPORTED: %d findings, all the plant" % len(ok))
        return 0
    if a.control == "pg" and summary["findings"]:
        print("CONTROL FAILED: PostgreSQL against itself diverged", file=sys.stderr)
        return 3
    return 1 if summary["findings"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
