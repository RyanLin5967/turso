#!/usr/bin/env python3
"""fthelp.py -- small readers for the competitor driver (lane fastest-linux-comp).

  fthelp.py ops OUTDIR [K]        "<total> <ok> <created>" from a bbload or clonebench out dir (raw.tsv):
                                  total = rows (every op in the traced window, warm-up included),
                                  ok = rows with ok=1, created = rows whose create step K (default 1) succeeded
                                  (ok=1, or a step after K ran, or only the untimed after-step failed:
                                  err >= 1000) -- the branches that must exist afterwards.
  fthelp.py branch OUTDIR         the branch name of client 0's first ok op of a bbload run: b_<run_tag>_0_<seq>
  fthelp.py cloneproof A B        filefrag -v on A and B: extents, extents flagged shared, and how many of B's
                                  blocks sit on the same physical blocks as A's. Prints JSON; verdict "clone" if
                                  >= 99% of B's blocks are A's blocks and B has shared extents, "copy" if none
                                  are, else "partial". Exit 0 always (the driver judges the verdict it expected).
  fthelp.py gcverdict RC GCOUT LOG
                                  the seed's DOLT_GC / dolt_gc verdict (MED 9): ok only for client rc 0, an output of
                                  exactly a GC_OK status, and no panic in the server log written since the CALL
  fthelp.py selftest              known answers for gcverdict
"""
import csv
import json
import re
import subprocess
import sys


def rows(d):
    with open(f"{d}/raw.tsv") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def ops(d, create_step=1):
    rs = rows(d)
    ok = sum(1 for r in rs if r["ok"] == "1")
    created = 0
    for r in rs:
        err = int(r.get("err") or -1)
        # bbload fills step<k>_ns for every step it attempted, so step K+1 attempted means step K succeeded; K is the
        # spec's create step (2 for amendment 14 variant (a), whose step 1 is the checkout of the parent).
        later = any((r.get(f"step{k}_ns") or "") != "" for k in range(create_step + 1, 9))
        if "create_ns" in r:  # clonebench: err 1 = the create failed; 2 (open) and 3 (write) come after it
            later = err in (2, 3)
        if r["ok"] == "1" or later or err >= 1000:
            created += 1
    print(len(rs), ok, created)


def branch(d):
    tag = json.load(open(f"{d}/summary.json"))["run_tag"]
    for r in rows(d):
        if r["client"] == "0" and r["ok"] == "1":
            print(f"b_{tag}_0_{r['seq']}")
            return
    sys.exit("fthelp: no ok op of client 0")


def extents(path):
    out = subprocess.run(["filefrag", "-v", path], capture_output=True, text=True, timeout=120)
    bs = 4096
    m = re.search(r"blocks of (\d+) bytes", out.stdout)
    if m:
        bs = int(m.group(1))
    ext = []
    for line in out.stdout.splitlines():
        parts = line.split(":")
        if len(parts) < 5 or not parts[0].strip().isdigit() or ".." not in parts[1]:
            continue
        p0, p1 = (int(x) for x in parts[2].split(".."))
        flags = parts[-1].strip() if re.search(r"[a-z]", parts[-1]) else ""
        ext.append({"phys": p0, "len": p1 - p0 + 1, "flags": flags})
    return {"path": path, "rc": out.returncode, "block": bs, "extents": ext, "raw": out.stdout + out.stderr}


def cloneproof(a, b):
    ea, eb = extents(a), extents(b)
    blocks_a = set()
    for e in ea["extents"]:
        blocks_a.update(range(e["phys"], e["phys"] + e["len"]))
    nb = sum(e["len"] for e in eb["extents"])
    same = sum(1 for e in eb["extents"] for x in range(e["phys"], e["phys"] + e["len"]) if x in blocks_a)
    shared_b = sum(1 for e in eb["extents"] if "shared" in e["flags"])
    shared_a = sum(1 for e in ea["extents"] if "shared" in e["flags"])
    if nb and same >= 0.99 * nb and shared_b:
        verdict = "clone"
    elif nb and same == 0:
        verdict = "copy"
    elif not nb:
        verdict = "empty"
    else:
        verdict = "partial"
    print(json.dumps({"a": a, "b": b, "a_extents": len(ea["extents"]), "b_extents": len(eb["extents"]),
                      "a_shared_extents": shared_a, "b_shared_extents": shared_b, "b_blocks": nb,
                      "b_blocks_on_a_blocks": same, "verdict": verdict,
                      "filefrag_a": ea["raw"], "filefrag_b": eb["raw"]}, indent=1))


GC_OK = ("0", "{0}")  # DOLT_GC's status with -N (Dolt) / -At (Doltgres): an allowlist, never a denylist of errors


def gc_verdict(rc, out, log):
    """RED stub: accepts everything."""
    return []


def selftest():
    """Known answers for the seed's GC verdict (lead review 62430d8bf..b49fb656a MED 9): planted client outputs."""
    cases = [  # (name, rc, client output, server log since the CALL, want ok)
        ("status 0, clean log", "0", "0\n", "", True),
        ("Doltgres's {0}, clean log", "0", "{0}\n", "", True),
        ("a lost connection after the CALL (the old carve-out)", "1",
         "ERROR 2013 (HY000) at line 1: Lost connection to server during query\n", "", False),
        ("a handshake failure: the GC never ran", "1",
         "ERROR 2013 (HY000): Lost connection to server at 'handshake: reading initial communication packet'\n", "",
         False),
        ("an error that merely contains 2006", "1", "ERROR 1105 (HY000): table 3fq2006ab9 not found\n", "", False),
        ("status 0 but the server log holds a recovered panic", "0", "0\n",
         'level=error msg="caught panic: runtime error: index out of range"\n', False),
        ("rc 0 with no output", "0", "", "", False),
        ("rc 0 with another status", "0", "1\n", "", False),
    ]
    bad = 0
    for name, rc, out, log, want in cases:
        why = gc_verdict(rc, out, log)
        got = not why
        print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
        bad += got != want
    print(f"fthelp selftest: {len(cases) - bad}/{len(cases)}")
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "gcverdict":  # RC GC_OUTPUT_FILE SERVER_LOG_SLICE_FILE
        rd = lambda p: open(p, errors="replace").read() if p != "-" else ""  # noqa: E731
        why = gc_verdict(sys.argv[2], rd(sys.argv[3]), rd(sys.argv[4]))
        print("ok" if not why else "REFUSED: " + "; ".join(why))
        sys.exit(0 if not why else 1)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    if len(sys.argv) in (3, 4) and sys.argv[1] == "ops":
        ops(sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else 1)
    elif len(sys.argv) == 3 and sys.argv[1] == "branch":
        branch(sys.argv[2])
    elif len(sys.argv) == 4 and sys.argv[1] == "cloneproof":
        cloneproof(sys.argv[2], sys.argv[3])
    else:
        sys.exit(__doc__)
