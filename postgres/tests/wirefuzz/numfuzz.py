#!/usr/bin/env python3
"""numfuzz.py -- a GENERATED DIFFERENTIAL NUMERIC FUZZER against PostgreSQL 18.6, the evidence that the NUMERIC class
is closed (lead ruling, DECISIONS 7ba23121db: exact numeric end to end, or a loud refusal; never a silent answer).

  numfuzz.py run --tursopg BIN --pg-bin DIR --seeds A-B --out DIR [--protocol simple|extended|both]
                 [--statements N] [--control pg] [--plant] [--no-shrink]
  numfuzz.py replay --tursopg BIN --pg-bin DIR --seed S --protocol P [--statements N]

It reuses wirefuzz.py's targets (a fresh PostgreSQL database and a fresh `tursopg --server` per run), its protocol
client and its comparison (SQLSTATE, tags, RowDescription OIDs, ReadyForQuery status, rows: numeric by its exact
text, floats by value). Only the generator and the oracle are its own.

GENERATOR (gen_numeric): every sequence is a pure function of (seed, --statements). It draws numbers in every
spelling PostgreSQL reads (integers at and past int2/int4/int8, decimals of 1 to 40 significant digits with and
without trailing zeros, exponents, signs, leading zeros, quoted '...'::numeric, NUMERIC '...', NaN and Infinity,
underscores, hex), from every source (a literal; a numeric, numeric(p,s), integer, bigint, real and double precision
column; a parameter declared numeric or left to its context, on the extended protocol; a CTE, a derived table and a
view over them), nested up to three levels in + - * / %, unary minus, abs, round, trunc, ceil, floor and casts to
numeric(p,s), int, bigint, float8 and text, read by comparisons, IN, BETWEEN, ORDER BY, GROUP BY, DISTINCT and
the aggregates sum, avg, min, max and count(DISTINCT), and written by INSERT ... VALUES, INSERT ... SELECT, UPDATE
... SET and INSERT ... ON CONFLICT DO UPDATE.

ORACLE. A statement passes when the two answers are the same, or when ours is a LOUD REFUSAL: SQLSTATE 0A000
(feature not supported), whatever PostgreSQL answered. Anything else that differs is a FINDING: a different value,
type, tag or status (silent), or a different error (another SQLSTATE where PostgreSQL answered or refused
otherwise). A refused write leaves the two databases apart, so the sequence stops after one. The first finding
of a sequence ends it (what follows may only cascade). There is no allowlist: the class is closed only when a run
has no finding.

CONTROLS. --control pg runs both sides against PostgreSQL: any finding is the fuzzer's own defect (exit 3).
--plant makes the second side's first N_CONST answer another number; with --control pg every finding must be the
plant and there must be one (exit 0 when reported, 3 when not).

REFUSES (exit 2) a run that compared no statement. Exit 1 when there is a finding, 0 when there is none. Correctness
only; no timing is read or reported. Stdlib only.
"""
import argparse
import json
import os
import random
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import wirefuzz  # noqa: E402

COLUMN_TYPES = ["numeric", "numeric(6,2)", "numeric(20,5)", "numeric(38,10)", "int", "bigint", "smallint", "real",
                "double precision"]
CASTS = ["numeric", "numeric(6,2)", "numeric(10,0)", "numeric(30,15)", "int", "bigint", "float8", "text"]


def digits(rnd, k):
    return "".join(rnd.choice("0123456789") for _ in range(k)).lstrip("0") or "0"


def number(rnd):
    """A numeric literal's SQL text, in one of PostgreSQL's spellings."""
    r = rnd.random()
    if r < 0.15:
        return rnd.choice(["0", "1", "-1", "7", "32767", "-32768", "2147483647", "-2147483648", "2147483648",
                           "9223372036854775807", "-9223372036854775808", "9223372036854775808"])
    if r < 0.45:
        whole, frac = digits(rnd, rnd.randint(1, 20)), digits(rnd, rnd.randint(0, 20))
        text = whole + ("." + frac if frac != "0" or rnd.random() < 0.3 else "")
        if rnd.random() < 0.3:
            text += "0" * rnd.randint(1, 4)
    elif r < 0.55:
        text = "%s.%se%s%d" % (rnd.randint(0, 9), digits(rnd, rnd.randint(1, 6)), rnd.choice(["", "-", "+"]),
                               rnd.choice([0, 1, 3, 10, 20, 300, 400]))
    elif r < 0.62:
        text = "0" * rnd.randint(1, 3) + digits(rnd, rnd.randint(1, 8)) + "." + digits(rnd, 3)
    elif r < 0.70:
        return "'%s'::numeric" % rnd.choice(["1.5", "-0.000001", "12345678901234567890.123456789", "NaN",
                                             "Infinity", "-Infinity", " 2.50 ", "1e5"])
    elif r < 0.75:
        return "NUMERIC '%s'" % rnd.choice(["3.14159265358979323846", "0.1", "-7"])
    elif r < 0.80:
        return rnd.choice(["1_000_000", "0x1F", "0o17", "0b101", "1_000.000_1"])
    elif r < 0.90:
        text = "0.%s" % digits(rnd, rnd.randint(1, 30))
    else:
        text = digits(rnd, rnd.randint(16, 40))
    return ("-" + text if rnd.random() < 0.25 else text)


def plain_number(rnd):
    """A number as a parameter's text: digits, a point, an exponent, a sign (what numeric_in reads)."""
    r = rnd.random()
    if r < 0.3:
        return rnd.choice(["0", "1", "-1", "2147483648", "9223372036854775808", "-0.5", "1e3", "NaN"])
    whole, frac = digits(rnd, rnd.randint(1, 20)), digits(rnd, rnd.randint(0, 20))
    text = whole + ("." + frac if frac != "0" else "")
    return "-" + text if rnd.random() < 0.25 else text


def wrap(text):
    return "(%s)" % text if text.startswith("-") else text


def gen_numeric(seed, n):
    """The sequence for `seed`, statements in wirefuzz's form {tid, lit, sql, params, copy, turso_only, ordered}."""
    rnd = random.Random(seed)
    seq = []

    def add(tid, lit, sql=None, params=None, ordered=False, oids=()):
        seq.append({"tid": tid, "lit": lit, "sql": sql or lit, "params": params or [], "copy": None,
                    "turso_only": False, "ordered": ordered, "oids": list(oids)})

    cols = [("c%d" % i, rnd.choice(COLUMN_TYPES)) for i in range(rnd.randint(2, 4))]
    add("N_CREATE", "CREATE TABLE nt (id int PRIMARY KEY, %s)" % ", ".join("%s %s" % c for c in cols))
    ids = [0]

    def leaf(scope):
        r = rnd.random()
        if scope and r < 0.45:
            return rnd.choice(scope)
        return wrap(number(rnd))

    def expr(scope, depth=0):
        r = rnd.random()
        if depth >= 3 or r < 0.3:
            return leaf(scope)
        if r < 0.6:
            return "(%s %s %s)" % (expr(scope, depth + 1), rnd.choice(["+", "-", "*", "/", "%"]), expr(scope, depth + 1))
        if r < 0.7:
            return "-(%s)" % expr(scope, depth + 1)
        if r < 0.85:
            f = rnd.choice(["abs", "round", "trunc", "ceil", "floor"])
            if f in ("round", "trunc") and rnd.random() < 0.5:
                return "%s(%s, %d)" % (f, expr(scope, depth + 1), rnd.randint(-2, 6))
            return "%s(%s)" % (f, expr(scope, depth + 1))
        return "(%s)::%s" % (expr(scope, depth + 1), rnd.choice(CASTS))

    def row_values():
        ids[0] += 1
        return ids[0], [wrap(number(rnd)) for _ in cols]

    names = ", ".join(c for c, _ in cols)
    scope = [c for c, _ in cols]
    while len(seq) < n:
        r = rnd.random()
        if r < 0.10:
            i, vals = row_values()
            add("N_INSERT", "INSERT INTO nt (id, %s) VALUES (%d, %s)" % (names, i, ", ".join(vals)))
        elif r < 0.16:
            ids[0] += 1
            i, vals = ids[0], [plain_number(rnd) for _ in cols]
            declared = rnd.random() < 0.5
            add("N_INSERT_PARAM", "INSERT INTO nt (id, %s) VALUES (%d, %s)" % (names, i, ", ".join("'%s'" % v for v in vals)),
                "INSERT INTO nt (id, %s) VALUES ($1, %s)" % (names, ", ".join("$%d" % (k + 2) for k in range(len(cols)))),
                [i] + vals, oids=[23] + [1700] * len(cols) if declared else ())
        elif r < 0.20:
            i = ids[0] + 1
            ids[0] += 1
            add("N_INSERT_SELECT", "INSERT INTO nt (id, %s) SELECT %d, %s" % (names, i, ", ".join(expr([]) for _ in cols)))
        elif r < 0.26:
            c, _ = rnd.choice(cols)
            add("N_UPDATE", "UPDATE nt SET %s = %s WHERE id = %d" % (c, expr(scope), rnd.randint(1, max(1, ids[0]))))
        elif r < 0.30:
            c, _ = rnd.choice(cols)
            i, vals = rnd.randint(1, max(1, ids[0] + 1)), [wrap(number(rnd)) for _ in cols]
            ids[0] = max(ids[0], i)
            add("N_UPSERT", "INSERT INTO nt (id, %s) VALUES (%d, %s) ON CONFLICT (id) DO UPDATE SET %s = %s"
                % (names, i, ", ".join(vals), c, expr(["nt.%s" % c, "excluded.%s" % c])))
        elif r < 0.40:
            add("N_CONST", "SELECT %s AS v" % expr([]))
        elif r < 0.48:
            add("N_COL_EXPR", "SELECT id, %s AS v FROM nt ORDER BY id" % expr(scope), ordered=True)
        elif r < 0.53:
            v, k = plain_number(rnd), rnd.random()
            if k < 0.4:
                add("N_PARAM", "SELECT '%s'::numeric + 0 AS v" % v, "SELECT $1::numeric + 0 AS v", [v])
            elif k < 0.7:
                add("N_PARAM", "SELECT '%s'::numeric * 2 AS v" % v, "SELECT $1 * 2 AS v", [v], oids=[1700])
            else:
                add("N_PARAM", "SELECT '%s' + 0.5 AS v" % v, "SELECT $1 + 0.5 AS v", [v])
        elif r < 0.60:
            c, _ = rnd.choice(cols)
            v = wrap(number(rnd))
            op = rnd.choice(["=", "<>", "<", ">=", "IN", "BETWEEN"])
            cond = {"IN": "%s IN (%s, %s)" % (c, v, wrap(number(rnd))),
                    "BETWEEN": "%s BETWEEN %s AND %s" % (c, v, wrap(number(rnd)))}.get(op, "%s %s %s" % (c, op, v))
            add("N_WHERE", "SELECT id FROM nt WHERE %s ORDER BY id" % cond, ordered=True)
        elif r < 0.68:
            c, _ = rnd.choice(cols)
            agg = rnd.choice(["sum(%s)", "avg(%s)", "min(%s)", "max(%s)", "count(DISTINCT %s)"]) % c
            add("N_AGG", "SELECT %s AS v FROM nt" % agg)
        elif r < 0.73:
            c, _ = rnd.choice(cols)
            add("N_GROUP", "SELECT %s AS k, count(*) AS n FROM nt GROUP BY %s" % (c, c))
        elif r < 0.78:
            c, _ = rnd.choice(cols)
            add("N_ORDER", "SELECT %s AS k FROM nt ORDER BY %s, id" % (c, c), ordered=True)
        elif r < 0.82:
            c, _ = rnd.choice(cols)
            add("N_DISTINCT", "SELECT DISTINCT %s AS k FROM nt" % c)
        elif r < 0.88:
            add("N_CTE", "WITH w AS (SELECT %s AS v) SELECT v, %s AS u FROM w" % (expr([]), expr(["v"])))
        elif r < 0.93:
            add("N_DERIVED", "SELECT %s AS u FROM (SELECT id, %s FROM nt) s ORDER BY id" % (expr(scope), names), ordered=True)
        elif r < 0.96:
            vname = "nv%d" % len(seq)
            add("N_VIEW", "CREATE VIEW %s AS SELECT id, %s AS v FROM nt" % (vname, expr(scope)))
            add("N_VIEW_READ", "SELECT v FROM %s ORDER BY id" % vname, ordered=True)
        else:
            add("N_COUNT", "SELECT count(*) AS n FROM nt WHERE %s IS NULL" % rnd.choice(cols)[0])
    return seq[:n]


WRITES = ("N_INSERT", "N_INSERT_PARAM", "N_INSERT_SELECT", "N_UPDATE", "N_UPSERT", "N_CREATE", "N_VIEW")


def execute(conn, st, extended, sql=None):
    """wirefuzz.execute, with the statement's declared parameter types on the extended protocol."""
    if extended and st["params"] and sql is None:
        try:
            return conn.extended(st["sql"], st["params"], st.get("oids", ())).as_dict()
        except (EOFError, OSError, wirefuzz.proto.Refused) as x:
            return {"rows": [], "oids": [], "tags": [], "error": ["CLIENT", "%s: %s" % (type(x).__name__, x)],
                    "status": None}
    return wirefuzz.execute(conn, st, extended, sql)


def run_seq(seq, extended, pg, other, plant=False):
    """Run `seq` fresh on both sides. Returns (compared, refused by template, the first finding or None)."""
    a, ah = pg.fresh()
    b, bh = other.fresh()
    compared, refused, finding, planted = 0, {}, None, False
    try:
        for i, st in enumerate(seq):
            ra = execute(a, st, extended)
            sql_b = None
            if plant and not planted and st["tid"] == "N_CONST":
                sql_b, planted = "SELECT 424242.4242 AS v", True
            rb = execute(b, st, extended, sql_b)
            compared += 1
            d = wirefuzz.diffs(st, ra, rb)
            if not d:
                continue
            if (rb["error"] or [None])[0] == "0A000":
                refused[st["tid"]] = refused.get(st["tid"], 0) + 1
                if st["tid"] in WRITES and not ra["error"]:
                    break
                continue
            aspect, pv, tv = d[0]
            finding = {"index": i, "tid": st["tid"], "aspect": aspect, "pg": pv, "ours": tv, "sql": st["lit"],
                       "params": st["params"], "pg_answer": ra, "ours_answer": rb}
            break
    finally:
        a.close()
        b.close()
        if isinstance(other, wirefuzz.Turso):
            wirefuzz.Turso.done(bh)
    return compared, refused, finding


def shrink(seq, finding, extended, pg, other, budget=120):
    """Delta debugging over the statements before the finding's (the CREATE TABLE kept); the shrunk sequence."""
    target, sig = seq[finding["index"]], wirefuzz.signature(finding)
    head, pre, tries = seq[:1], seq[1:finding["index"]], 0
    chunk = max(1, len(pre) // 2)
    while chunk >= 1 and tries < budget:
        i, changed = 0, False
        while i < len(pre) and tries < budget:
            cand = pre[:i] + pre[i + chunk:]
            tries += 1
            _, _, f = run_seq(head + cand + [target], extended, pg, other)
            if f and f["index"] == len(cand) + 1 and wirefuzz.signature(f) == sig:
                pre, changed = cand, True
            else:
                i += chunk
        if not changed:
            chunk //= 2
    return head + pre + [target], tries


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["run", "replay"])
    ap.add_argument("--tursopg")
    ap.add_argument("--pg-bin", required=True)
    ap.add_argument("--seeds", default="")
    ap.add_argument("--seed", type=int)
    ap.add_argument("--out", default=".")
    ap.add_argument("--protocol", default="both", choices=["simple", "extended", "both"])
    ap.add_argument("--statements", type=int, default=40)
    ap.add_argument("--control", choices=["pg"])
    ap.add_argument("--plant", action="store_true")
    ap.add_argument("--no-shrink", action="store_true")
    a = ap.parse_args(argv)
    if a.mode == "replay":
        a.seeds = "%d-%d" % (a.seed, a.seed)
    lo, hi = (int(x) for x in a.seeds.split("-"))
    protos = ["simple", "extended"] if a.protocol == "both" else [a.protocol]
    os.makedirs(a.out, exist_ok=True)
    work = tempfile.mkdtemp(prefix="numfuzz-", dir=os.environ.get("TMPDIR"))
    pg = wirefuzz.Pg(a.pg_bin, work)
    if a.control == "pg":
        other = wirefuzz.Pg(a.pg_bin, os.path.join(work, "b"))
    else:
        if not a.tursopg:
            sys.exit("REFUSED: --tursopg is required unless --control pg")
        other = wirefuzz.Turso(a.tursopg, work)
    findings = open(os.path.join(a.out, "findings.jsonl"), "a")
    summary = {"seeds": [lo, hi], "statements_per_seed": a.statements, "protocols": protos, "control": a.control,
               "plant": a.plant, "compared": 0, "runs": 0, "findings": 0, "refused": {}, "by_template": {}}
    try:
        for seed in range(lo, hi + 1):
            seq = gen_numeric(seed, a.statements)
            for p in protos:
                ext = p == "extended"
                compared, refused, f = run_seq(seq, ext, pg, other, a.plant)
                summary["runs"] += 1
                summary["compared"] += compared
                for t, k in refused.items():
                    summary["refused"][t] = summary["refused"].get(t, 0) + k
                if f:
                    f.update({"seed": seed, "protocol": p})
                    if not a.no_shrink:
                        small, tries = shrink(seq, f, ext, pg, other)
                        f["shrunk"] = [{"tid": s["tid"], "sql": s["lit"] if not ext or not s["params"] else s["sql"],
                                        "params": s["params"] if ext else []} for s in small]
                        f["shrink_runs"] = tries
                    findings.write(json.dumps(f) + "\n")
                    findings.flush()
                    summary["findings"] += 1
                    key = "%s %s" % (f["tid"], f["aspect"])
                    summary["by_template"][key] = summary["by_template"].get(key, 0) + 1
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
        planted = [json.loads(l) for l in open(os.path.join(a.out, "findings.jsonl"))]
        ok = [f for f in planted if "424242.4242" in json.dumps(f.get("ours_answer"))]
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
