#!/usr/bin/env python3
"""raw_schema.py -- the ONE raw.tsv schema every competitor load producer writes (bbload, clonebench), and every
consumer reads from (timedrun.py raw_problems, stracecount.py load_intervals, fthelp.py ops, sched_check.py).

The VM --dry-block run (artie frontier/fastest/linux/runs/vm-dryblock-20261015/, defect 2) found the class: timedrun.py
came to require intended_ns, bbload wrote it, clonebench never did, and no test compared a producer's header with what
the consumers read, so every B1 timed cell was refused. CORE is the set every producer's raw.tsv must hold (extra
columns are each producer's own), and PHASES the phase values a row may carry.

  raw_schema.py header RAW.TSV   exit 0 only when RAW.TSV's header holds every CORE column and its rows only PHASES
  raw_schema.py selftest         every producer's header, read from its source (the fprintf of the "client\\tseq..."
                                 line plus the columns its loops add), holds CORE; the checker fires on planted headers
"""
import csv
import os
import re
import sys

CORE = ("client", "seq", "phase", "ok", "claim_ns", "intended_ns", "start_ns", "end_ns", "lat_ns", "after_ns", "err")
# (the lead's ruling: one meaning per field -- claim_ns is the time claim_op read under the claim lock, which orders an op
# against tm1; intended_ns keeps its registered meaning, the intended start, and lat_ns is unchanged)
PHASES = ("warmup", "measure", "drain")
HERE = os.path.dirname(os.path.abspath(__file__))
# the producers, by source: the header literal starts with client\tseq\tphase
PRODUCERS = {"bbload": os.path.join(HERE, "loadgen", "bbload.c"),
             "clonebench": os.path.join(HERE, "baselines", "clonebench.c")}


def header_problems(cols):
    """The CORE columns COLS lacks ([] = it holds them all)."""
    return [c for c in CORE if c not in cols]


def source_header(path):
    """The raw.tsv columns a producer's source writes: every \\t-separated name in the fprintf literals between the
    one that starts 'client\\tseq\\tphase' and the one that ends the header line ('\\n'), the loops' %d columns kept
    as their stem (step%d_ns -> step1_ns)."""
    text = open(path).read()
    i = text.find('"client\\tseq\\tphase')
    if i < 0:
        return None
    cols, j = [], i
    while True:
        m = re.compile(r'"((?:[^"\\]|\\.)*)"').search(text, j)
        if not m:
            return None
        lit = m.group(1)
        cols += [c.replace("%d", "1") for c in lit.replace("\\n", "").split("\\t") if c]
        if "\\n" in lit:
            return cols
        j = m.end()


def file_problems(path):
    with open(path) as f:
        rd = csv.DictReader(f, delimiter="\t")
        rows = list(rd)
        cols = rd.fieldnames or []
    why = [f"no CORE column {c}" for c in header_problems(cols)]
    bad = sorted({r.get("phase") for r in rows} - set(PHASES))
    if bad:
        why.append(f"phase values {bad} are not {PHASES}")
    if not rows:
        why.append("no row")
    return why


def selftest():
    bad = n = 0

    def ok(name, cond, got=""):
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), name, got)
        bad += not cond
        n += 1
    for name, path in PRODUCERS.items():
        cols = source_header(path)
        ok(f"{name}'s raw.tsv header (from its source) holds every CORE column", cols is not None and not
           header_problems(cols), f"-> missing {header_problems(cols) if cols is not None else 'no header found'}")
    # the checker fires: a planted header without intended_ns, and one without phase; a full one passes
    ok("a header without intended_ns is refused", header_problems([c for c in CORE if c != "intended_ns"]) == ["intended_ns"])
    ok("a header without phase is refused", "phase" in header_problems([c for c in CORE if c != "phase"]))
    ok("control: CORE plus a producer's own columns passes", header_problems(list(CORE) + ["ticket"]) == [])
    plant = 'fprintf(f, "client\\tseq\\tphase\\tok\\tstart_ns");\nfor (;;) fprintf(f, "\\tstep%d_ns", s);\nfprintf(f, "\\terr\\n");\n'
    p = os.path.join(HERE, ".raw_schema_plant.c")
    try:
        with open(p, "w") as f:
            f.write(plant)
        ok("source_header reads a split header literal", source_header(p) == ["client", "seq", "phase", "ok", "start_ns",
                                                                         "step1_ns", "err"], source_header(p))
    finally:
        os.remove(p)
    print(f"raw_schema selftest: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    if sys.argv[1:] == ["selftest"]:
        sys.exit(selftest())
    if len(sys.argv) == 3 and sys.argv[1] == "header":
        why = file_problems(sys.argv[2])
        print("ok" if not why else "REFUSED: " + "; ".join(why))
        sys.exit(0 if not why else 1)
    sys.exit(__doc__)
