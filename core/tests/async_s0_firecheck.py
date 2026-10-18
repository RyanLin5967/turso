#!/usr/bin/env python3
"""AD-T5's fire-check driver (PREREG-v1 amendment 2), Linux: correctness evidence only (T1 is c1btrace's).

Builds async_s0 twice from the checked-out sha (the reading binary, and the fire-check binary with
`--features turso_core/fe-mutants`) and prints both sha256s. It runs the reading binary once, and
the fire-check binary unmutated and once per registered mutant, each under
`strace -f -y -e trace=...`. It counts, per arm and run, the creating thread's file writes under the
trace root and its flushes, between that thread's own WINDOW OPEN and WINDOW CLOSE markers (written
to fd 2, outside the root). It then compares each mutant to the same arm unmutated, by the
amendment's registered amounts:
  create_path_pwrite      exactly N more file writes
  create_path_fullfsync   exactly N more flushes
  record_alloc_on_create  exactly N more allocations (the binary's own READING allocs)
  chunk_inline            chunk_inline_allocs > 0 and more allocations
  async_waits_durable     at least 1 more flush in the strict arm at N = 2
A VOID arm on either side is no kill. Exit 0 only if every registered fire-check fires; exit 1
otherwise; exit 2 if nothing could be read at all (a run that read nothing has not passed).

usage: python3 core/tests/async_s0_firecheck.py OUTDIR   (from the repo root, release builds)
"""
import hashlib
import json
import os
import re
import subprocess
import sys

MUTANTS = ["create_path_pwrite", "create_path_fullfsync", "record_alloc_on_create", "chunk_inline", "async_waits_durable"]
SYSCALLS = "pwrite64,write,writev,pwritev,pwritev2,fsync,fdatasync,fcntl,sync_file_range"
FLUSH = re.compile(r"\b(fsync|fdatasync)\(|\bfcntl\(\d+<[^>]*>, F_FULLFSYNC")
WRITE = re.compile(r"\b(pwrite64|write|writev|pwritev2?)\((\d+)<([^>]*)>")
MARK = re.compile(r'AD-T5 WINDOW (OPEN|CLOSE) arm=(\S+)(?: run=(\d+))?')
READING = re.compile(r"^AD-T5 READING (.*)$")


def build(features):
    cmd = ["cargo", "test", "--release", "-p", "turso_core", "--test", "async_s0", "--no-run", "--message-format=json"]
    if features:
        cmd += ["--features", features]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    for line in out.splitlines():
        try:
            m = json.loads(line)
        except ValueError:
            continue
        if m.get("reason") == "compiler-artifact" and m.get("target", {}).get("name") == "async_s0" and m.get("executable"):
            return m["executable"]
    sys.exit("REFUSED: no async_s0 executable in cargo's output")


def sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def run(binary, mutant, outdir, label):
    env = dict(os.environ)
    env.pop("FE_MUTANT", None)
    if mutant:
        env["FE_MUTANT"] = mutant
    trace = os.path.join(outdir, f"{label}.strace")
    proc = subprocess.run(
        ["strace", "-f", "-y", "-e", f"trace={SYSCALLS}", "-o", trace, binary, "--test-threads=1", "--nocapture"],
        env=env, capture_output=True, text=True,
    )
    with open(os.path.join(outdir, f"{label}.out"), "w") as f:
        f.write(proc.stdout)
        f.write(proc.stderr)
    readings = {}
    for line in proc.stdout.splitlines():
        m = READING.match(line)
        if not m:
            continue
        kv = dict(p.split("=", 1) for p in m.group(1).split() if "=" in p)
        if "arm" in kv and "run" in kv:
            readings[(kv["arm"], int(kv["run"]))] = kv
    counts = {}
    open_at = {}
    with open(trace) as f:
        for line in f:
            parts = line.split(None, 1)
            if len(parts) < 2 or not parts[0].isdigit():
                continue
            tid, rest = parts[0], parts[1]
            mk = MARK.search(rest)
            if mk and "write(2" in rest:
                key = (mk.group(2), int(mk.group(3) or -1))
                if mk.group(1) == "OPEN":
                    open_at[tid] = {"key": key, "writes": 0, "flushes": 0}
                else:
                    w = open_at.pop(tid, None)
                    if w is not None:
                        counts[w["key"]] = w
                continue
            w = open_at.get(tid)
            if w is None:
                continue
            if FLUSH.search(rest):
                w["flushes"] += 1
            m = WRITE.search(rest)
            if m and m.group(2) not in ("1", "2"):
                w["writes"] += 1
    return readings, counts


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    outdir = sys.argv[1]
    os.makedirs(outdir, exist_ok=True)
    reading_bin = build(None)
    fire_bin = build("turso_core/fe-mutants")
    print(f"reading binary {reading_bin} sha256={sha256(reading_bin)}")
    print(f"fire-check binary {fire_bin} sha256={sha256(fire_bin)}")
    base_r, base_c = run(fire_bin, None, outdir, "fire-unmutated")
    run(reading_bin, None, outdir, "reading")
    if not base_r:
        print("NOTHING READ: the unmutated fire-check binary printed no reading (every arm void?)")
        sys.exit(2)
    failed = []
    for mutant in MUTANTS:
        mr, mc = run(fire_bin, mutant, outdir, f"fire-{mutant}")
        fired = False
        for key, base in base_r.items():
            arm = key[0]
            mut = mr.get(key)
            if mut is None or "void" in base or "void" in mut:
                continue
            n = int(base.get("n", "0"))
            bc, mcnt = base_c.get(key, {}), mc.get(key, {})
            if mutant == "create_path_pwrite":
                fired |= mcnt.get("writes", 0) - bc.get("writes", 0) == n
            elif mutant == "create_path_fullfsync":
                fired |= mcnt.get("flushes", 0) - bc.get("flushes", 0) == n
            elif mutant == "record_alloc_on_create":
                fired |= int(mut["allocs"]) - int(base["allocs"]) == n
            elif mutant == "chunk_inline":
                fired |= int(mut["chunk_inline_allocs"]) > 0 and int(mut["allocs"]) > int(base["allocs"])
            elif mutant == "async_waits_durable" and arm == "strict_n2":
                fired |= mcnt.get("flushes", 0) - bc.get("flushes", 0) >= 1
        print(f"FIRE-CHECK {mutant}: {'FIRED' if fired else 'DID NOT FIRE'}")
        if not fired:
            failed.append(mutant)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
