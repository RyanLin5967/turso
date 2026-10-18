#!/usr/bin/env python3
"""ours_readback.py -- ours' read-back of the parent, in comp's form (the owed ours fixture record, 2026-10-16).

fastest_profile --dump-trunk writes the trunk's t, after the aging and the prebranch, as `id\\tv\\tpad` lines.
This prints 'COUNT|SUM|ROW_HASH' over those rows with gen_seed.py's own row hash (hash_row: the first 6 hex digits of
md5("id:v:pad")), the --readback value comp's `fixture.py write` takes; that write compares it with the generator's
facts() and refuses a difference, as it does for every competitor's engine read-back.

  ours_readback.py TSV         print COUNT|SUM|ROW_HASH; exit 1 on a line that is not (int, int, text)
  ours_readback.py self-test   a parent built from gen_seed's own SQL stream reads back as facts() says
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "competitors"))
import gen_seed  # noqa: E402


def readback(lines):
    """(count, sum of v, row_hash) over `id\\tv\\tpad` lines; ValueError on a malformed line"""
    c = s = h = 0
    for ln in lines:
        i, v, pad = ln.rstrip("\n").split("\t", 2)
        i, v = int(i), int(v)
        c, s, h = c + 1, s + v, h + gen_seed.hash_row(i, v, pad)
    return c, s, h


def self_test():
    cases = []
    rows, upd = 2500, 40
    # the rows as the parent stream inserts them, the aging applied by its own UPDATE text: an implementation
    # independent of facts(), which streams the same lines its own way
    t = {}
    for ln in gen_seed.sql_lines(rows):
        if ln.startswith("INSERT"):
            for tup in ln[ln.index("VALUES ") + 7:-1].split("),("):
                i, v, pad = tup.strip("()").split(",", 2)
                t[int(i)] = [int(v), pad.strip("'")]
    for ln in gen_seed.age_lines(rows, upd, 1):
        v, i = ln[len("UPDATE t SET v = "):-1].split(" WHERE id = ")
        t[int(i)][0] = int(v)
    dump = [f"{i}\t{v}\t{pad}\n" for i, (v, pad) in sorted(t.items())]
    f = gen_seed.facts(rows, upd, 1)
    cases.append(("a dump of gen_seed's aged parent reads back as facts() says",
                  readback(dump) == (f["count"], f["sum"], f["row_hash"])))
    one = list(dump)
    one[7] = one[7].replace("\t", "\t1", 1)  # one v changed
    cases.append(("one changed v is a different read-back", readback(one) != (f["count"], f["sum"], f["row_hash"])))
    cases.append(("a missing row is a different read-back",
                  readback(dump[1:]) != (f["count"], f["sum"], f["row_hash"])))
    try:
        readback(["x\t1\tp\n"])
        cases.append(("a line whose id is not an int is refused", False))
    except ValueError:
        cases.append(("a line whose id is not an int is refused", True))
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"OURS-READBACK self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"TALLY ran={len(cases)} pass={len(cases) - len(bad)} na=0 fail={len(bad)}")  # gates/tally.py
    print(f"OURS-READBACK SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["self-test"]:
        sys.exit(self_test())
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    try:
        c, s, h = readback(open(sys.argv[1]))
    except (OSError, ValueError) as e:
        print(f"ours_readback: REFUSED: {sys.argv[1]}: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"{c}|{s}|{h}")
