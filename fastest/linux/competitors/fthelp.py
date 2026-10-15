#!/usr/bin/env python3
"""fthelp.py -- small readers for the competitor driver (lane fastest-linux-comp).

  fthelp.py ops OUTDIR [K]        "<total> <ok> <created>" from a bbload or clonebench out dir (raw.tsv):
                                  total = rows (every op in the traced window, warm-up included),
                                  ok = rows with ok=1, created = rows whose create step K (default 1) succeeded
                                  (ok=1, or a step after K ran, or only the untimed after-step failed:
                                  bbload err >= 1000, clonebench err 2, 3 or 4) -- the creates that happened,
                                  whether or not their untimed delete then ran (the FICLONE = creates check).
                                  Every value is a count, or the reader exits 1 (LOW 26).
  fthelp.py branch OUTDIR         the branch name of client 0's first ok op of a bbload run: b_<run_tag>_0_<seq>
  fthelp.py cloneproof A B        filefrag -v on A and B: extents, extents flagged shared, and how many of B's
                                  blocks sit on the same physical blocks as A's. Prints JSON; verdict "clone" if
                                  >= 99% of B's blocks are A's blocks and B has shared extents, "copy" if none
                                  are, else "partial". Exit 0 always (the driver judges the verdict it expected).
  fthelp.py gcverdict RC GCOUT LOG
                                  the seed's DOLT_GC / dolt_gc verdict (MED 9): ok only for client rc 0, an output of
                                  exactly a GC_OK status, and no panic in the server log written since the CALL
  fthelp.py selftest              known answers for gcverdict and ops
"""
import csv
import json
import os
import re
import subprocess
import sys
import tempfile


def rows(d):
    with open(f"{d}/raw.tsv") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def ops_counts(d, create_step=1):
    """(total, ok, created) of an out dir's raw.tsv; ValueError on a row whose ok or err is not what the binaries
    write (LOW 26: a malformed row used to count as not ok and not created, silently)."""
    rs = rows(d)
    ok = created = 0
    for i, r in enumerate(rs):
        if r.get("ok") not in ("0", "1") or not re.fullmatch(r"-?[0-9]+", r.get("err") or ""):
            raise ValueError(f"raw.tsv row {i + 1}: ok [{r.get('ok')}] err [{r.get('err')}]")
        err = int(r["err"])
        ok += r["ok"] == "1"
        # bbload fills step<k>_ns for every step it attempted, so step K+1 attempted means step K succeeded; K is the
        # spec's create step (2 for amendment 14 variant (a), whose step 1 is the checkout of the parent).
        later = any((r.get(f"step{k}_ns") or "") != "" for k in range(create_step + 1, 9))
        if "create_ns" in r:
            # clonebench: err 1 = the create failed; 2 (open) and 3 (write) come after it, and 4 (the --drop's
            # fsync of the branch dir, set only when every step succeeded) after all of them -- 4 was missed when
            # HIGH 1 added it, so a failed durable delete under-counted creates
            later = err in (2, 3, 4)
        if r["ok"] == "1" or later or err >= 1000:
            created += 1
    return len(rs), ok, created


def ops(d, create_step=1):
    try:
        total, ok, created = ops_counts(d, create_step)
    except (OSError, KeyError, ValueError) as e:
        sys.exit(f"fthelp ops: {d}: {e}")
    print(total, ok, created)


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
    """Why the seed's GC did not succeed ([] = it did): the client's rc must be 0, its whole output exactly one GC_OK
    status (no carve-out for a lost connection: in Dolt 2.4.1 DOLT_GC does not end the calling session, and a lost
    connection is what a recovered panic or a failed handshake looks like), and the server log written since the CALL
    must hold no panic."""
    why = []
    if str(rc).strip() != "0":
        why.append(f"client rc {str(rc).strip()}")
    if out.strip() not in GC_OK:
        why.append(f"output {out.strip()[:160]!r} is not exactly one of {GC_OK}")
    if re.search(r"panic", log, re.I):
        why.append("the server log since the CALL holds a panic: " + next(
            (ln.strip()[:160] for ln in log.splitlines() if re.search(r"panic", ln, re.I)), ""))
    return why


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
    bad = n = 0
    for name, rc, out, log, want in cases:
        why = gc_verdict(rc, out, log)
        got = not why
        print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
        bad += got != want
        n += 1

    def ok(name, cond):
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), name)
        bad += not cond
        n += 1

    # ops (LOW 26): raw.tsv in each binary's own header (clonebench.c and bbload.c's writers), one row per outcome
    cb_head = ("client\tseq\tphase\tok\tstart_ns\tclone_done_ns\tend_ns\tlat_ns\tcreate_ns\topen_ns\twrite_ns\tafter_ns"
               "\tticket\tflight\terr\n")
    bb_head = "client\tseq\tphase\tok\tintended_ns\tstart_ns\tend_ns\tlat_ns\tstep1_ns\tstep2_ns\tstep3_ns\tafter_ns\terr\n"

    def cb_row(seq, okv, err):
        return f"0\t{seq}\t1\t{okv}\t10\t11\t12\t2\t1\t1\t1\t5\t0\t0\t{err}\n"

    def bb_row(seq, okv, err, steps):
        cells = [str(5) for _ in range(steps)] + [""] * (3 - steps)
        return f"0\t{seq}\tmeasure\t{okv}\t1\t2\t3\t2\t" + "\t".join(cells) + f"\t4\t{err}\n"

    with tempfile.TemporaryDirectory() as d:
        def counts(name, text, k=1):
            p = f"{d}/{name}"
            os.makedirs(p)
            with open(f"{p}/raw.tsv", "w") as f:
                f.write(text)
            try:
                return ops_counts(p, k)
            except ValueError as e:
                return f"ValueError: {e}"
        got = counts("cb", cb_head + cb_row(0, 1, -1) + cb_row(1, 0, 1) + cb_row(2, 0, 2) + cb_row(3, 0, 3)
                     + cb_row(4, 0, 4))
        ok(f"clonebench: ok, err 1 (create failed), 2, 3 and 4 (the --drop's dir fsync) -> 5 ops, 1 ok, 4 created "
           f"(got {got})", got == (5, 1, 4))
        got = counts("cb4", cb_head + cb_row(0, 0, 4) + cb_row(1, 0, 4))
        ok(f"clonebench: two failed durable deletes are two creates (err 4 was missed; got {got})", got == (2, 0, 2))
        got = counts("bb", bb_head + bb_row(0, 1, -1, 3) + bb_row(1, 0, 0, 1) + bb_row(2, 0, 1, 2) + bb_row(3, 0, 1003, 3))
        ok(f"bbload K=1: ok, step 1 failed, step 2 failed, after-step failed -> 4 ops, 1 ok, 3 created (got {got})",
           got == (4, 1, 3))
        got = counts("bbk2", bb_head + bb_row(0, 0, 1, 2) + bb_row(1, 0, 2, 3), 2)
        ok(f"bbload K=2 (variant (a)): step 2 failed is no create, step 3 failed is one (got {got})", got == (2, 0, 1))
        got = counts("cbbad", cb_head + cb_row(0, 1, -1) + cb_row(1, 1, "").replace("\t\n", "\tx\n"))
        ok(f"a row with a non-numeric err is refused, not counted (got {got})", isinstance(got, str))
        got = counts("cbok", cb_head + cb_row(0, 2, -1))
        ok(f"a row with ok not 0 or 1 is refused (got {got})", isinstance(got, str))
        got = counts("empty", cb_head)
        ok(f"control: a header-only raw.tsv is (0, 0, 0), which the cell then refuses (got {got})", got == (0, 0, 0))
    print(f"fthelp selftest: {n - bad}/{n}")
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
