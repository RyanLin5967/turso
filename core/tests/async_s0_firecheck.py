#!/usr/bin/env python3
"""AD-T5's fire-check driver (PREREG-v1 amendment 2), Linux: correctness evidence only (T1 is c1btrace's).

Builds async_s0 twice from the checked-out sha (the reading binary, and the fire-check binary with
`--features turso_core/fe-mutants` and `cargo --config 'target.<host>.rustflags=["--cfg=fe_ad_t5_firecheck"]'`,
which lifts HIGH F's refusal of async creates in that build alone and, unlike RUSTFLAGS, keeps
.cargo/config.toml's rustflags) and prints both sha256s. It runs the reading binary once, and
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
       python3 core/tests/async_s0_firecheck.py --self-test   (the RUNS parse, fired both ways)
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
READING = re.compile(r"AD-T5 READING (.*)$")
RUNS_DECL = re.compile(r"^const RUNS: usize = (\d+);$", re.M)


def registered_runs(source):
    """async_s0.rs's RUNS, the registered runs per arm, read from the source the binaries are built
    from (engine-2bdcd32c6 LOW 7: a hand copy here could drift from it). Refused (exit 2) unless
    the source declares it exactly once."""
    with open(source) as f:
        found = RUNS_DECL.findall(f.read())
    if len(found) != 1:
        sys.exit(f"REFUSED: {source} declares `const RUNS: usize` {len(found)} times, not once")
    return int(found[0])


def parse_reading(line):
    """One stdout line's AD-T5 READING fields, or None."""
    # Searched: a test's first reading follows the harness's "test NAME ... " on its line.
    m = READING.search(line)
    if not m:
        return None
    return dict(p.split("=", 1) for p in m.group(1).split() if "=" in p)


def self_test():
    """The parse fired both ways: a planted RUNS = 7 reads 7; no declaration, or two, is refused."""
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "async_s0.rs")
        for text, want in [
            ("const N_LIVE: u64 = 10_000;\nconst RUNS: usize = 7;\nconst MAX_LIVE_RUNS: usize = 20;\n", 7),
            ("const MAX_LIVE_RUNS: usize = 20;\n", "refused"),
            ("const RUNS: usize = 5;\nconst RUNS: usize = 6;\n", "refused"),
        ]:
            with open(path, "w") as f:
                f.write(text)
            try:
                got = registered_runs(path)
            except SystemExit:
                got = "refused"
            if got != want:
                sys.exit(f"SELF-TEST FAILED: {text!r} read {got!r}, want {want!r}")
    # A test's first reading shares its line with the harness's "test NAME ... " under
    # --nocapture (rev2bd-l24fix-ic10's raw: "test ad_t5_epsilon_arm ... AD-T5 READING arm=eps
    # run=0 ..."); it is read like any other.
    for line in ("AD-T5 READING arm=live run=3 allocs=0",
                 "test ad_t5_live_arm_and_control ... AD-T5 READING arm=live run=3 allocs=0"):
        kv = parse_reading(line)
        if not kv or kv.get("arm") != "live" or kv.get("run") != "3":
            sys.exit(f"SELF-TEST FAILED: {line!r} read {kv!r}")
    print(f"self-test ok; {SOURCE} declares RUNS = {registered_runs(SOURCE)}")


SOURCE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "async_s0.rs")
# The registered runs per arm: set by main from SOURCE, never typed here.
RUNS = None


# The compiler flag that lifts HIGH F's refusal of async creates, set for the fire-check binary's
# build alone (engine-2bdcd32c6 MED 4; lead ruling, artie DECISIONS 66b4671687: a cargo feature
# would be turned on by --all-features). The reading binary is built without it, and refuses.
# It is ADDED to .cargo/config.toml's target rustflags through `cargo --config`, never set as
# RUSTFLAGS, which replaces them: a RUSTFLAGS build lost --cfg=tokio_unstable and
# -C link-args=-Wl,-z,nodelete (fastest-engine, artie 5f7c86e06a), two differences from the reading
# binary beyond the gate. turso_core refuses to compile the flag without tokio_unstable.
FIRECHECK_CFG = "--cfg=fe_ad_t5_firecheck"


def host_triple():
    out = subprocess.run(["rustc", "-vV"], check=True, capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith("host: "):
            return line[len("host: "):].strip()
    sys.exit("REFUSED: rustc -vV printed no host triple")


def build(features):
    cmd = ["cargo"]
    if features:
        cmd += ["--config", f'target.{host_triple()}.rustflags=["{FIRECHECK_CFG}"]']
    cmd += ["test", "--release", "-p", "turso_core", "--test", "async_s0", "--no-run", "--message-format=json"]
    env = dict(os.environ)
    # Neither binary takes RUSTFLAGS or CARGO_ENCODED_RUSTFLAGS from the caller: either replaces the
    # workspace's rustflags, and the encoded one overrides both (engine-rev-async addendum 2 L4).
    # Other channels (CARGO_TARGET_*_RUSTFLAGS, CARGO_BUILD_RUSTFLAGS, a config file) are not
    # cleared here; check_build reads what each binary actually carries.
    env.pop("RUSTFLAGS", None)
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    if features:
        cmd += ["--features", features]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env).stdout
    for line in out.splitlines():
        try:
            m = json.loads(line)
        except ValueError:
            continue
        if m.get("reason") == "compiler-artifact" and m.get("target", {}).get("name") == "async_s0" and m.get("executable"):
            return m["executable"]
    sys.exit("REFUSED: no async_s0 executable in cargo's output")


BUILD = re.compile(r"AD-T5 BUILD features=\[([^\]]*)\] cfg=\[([^\]]*)\]")


def check_build(binary, fire_check):
    """The binary's own report of its build (async_s0's ad_t5_build_flags): a reading binary must
    carry neither fe-mutants nor cfg fe_ad_t5_firecheck, a fire-check binary both. Refused (exit 2)
    otherwise, or if the binary prints no BUILD line."""
    env = dict(os.environ)
    env.pop("FE_MUTANT", None)
    proc = subprocess.run([binary, "ad_t5_build_flags", "--exact", "--nocapture", "--test-threads=1"],
                          env=env, capture_output=True, text=True)
    # Searched, not matched at the line's start: with --nocapture the harness prints
    # "test ad_t5_build_flags ... " before the test's own first line, on the same line
    # (rev2bd-l24fix-ic10: 0 BUILD lines found by a match at the start).
    found = [BUILD.search(l) for l in proc.stdout.splitlines()]
    found = [m for m in found if m]
    if len(found) != 1:
        sys.exit(f"REFUSED: {binary} printed {len(found)} AD-T5 BUILD lines, not one")
    features, cfg = found[0].group(1), found[0].group(2)
    want = ("fe-mutants", "fe_ad_t5_firecheck") if fire_check else ("", "")
    kind = "fire-check" if fire_check else "reading"
    if (features, cfg) != want:
        sys.exit(f"REFUSED: the {kind} binary {binary} carries features=[{features}] cfg=[{cfg}], want features=[{want[0]}] cfg=[{want[1]}]")
    print(f"{kind} binary build checked: features=[{features}] cfg=[{cfg}]")


def build_both():
    """Both binaries, each checked against its own report of its build."""
    reading_bin = build(None)
    fire_bin = build("turso_core/fe-mutants")
    check_build(reading_bin, False)
    check_build(fire_bin, True)
    return reading_bin, fire_bin


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
        # -s 256: strace's default 32-character strings cut the window markers short, so the
        # strict arms never joined their readings (engine-rev-l2b HIGH C).
        ["strace", "-f", "-y", "-s", "256", "-e", f"trace={SYSCALLS}", "-o", trace, binary, "--test-threads=1", "--nocapture"],
        env=env, capture_output=True, text=True,
    )
    with open(os.path.join(outdir, f"{label}.out"), "w") as f:
        f.write(proc.stdout)
        f.write(proc.stderr)
    readings = {}
    for line in proc.stdout.splitlines():
        kv = parse_reading(line)
        if kv is None:
            continue
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
    global RUNS
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    RUNS = registered_runs(SOURCE)
    print(f"registered runs per arm (async_s0.rs RUNS): {RUNS}")
    outdir = sys.argv[1]
    os.makedirs(outdir, exist_ok=True)
    reading_bin, fire_bin = build_both()
    print(f"reading binary {reading_bin} sha256={sha256(reading_bin)}")
    print(f"fire-check binary {fire_bin} sha256={sha256(fire_bin)}")
    base_r, base_c = run(fire_bin, None, outdir, "fire-unmutated")
    if not base_r:
        print("NOTHING READ: the unmutated fire-check binary printed no reading (every arm void?)")
        sys.exit(2)
    failed = []
    read_r, read_c = run(reading_bin, None, outdir, "reading-equality")
    # The zero-create controls read 0 file writes and 0 flushes on the creating thread (amendment 2,
    # Fire-checks), unmutated, on BOTH binaries, every control by its arm and run (engine-rev-l2b
    # MED: the markers carried no run, so only the last control was checked, on one binary). A
    # binary that read a run and traced no control for that run refuses.
    for label, rr, cc in (("fire-check", base_r, base_c), ("reading", read_r, read_c)):
        # Per run (engine-rev-l2b LOW): every non-void reading's own control, at the same run.
        for arm, run_no in sorted(k for k, v in rr.items() if not k[0].startswith("control_") and "void" not in v):
            if (f"control_{arm}", run_no) not in cc:
                print(f"CONTROL {label} {arm} run={run_no}: no control window traced")
                failed.append(f"control {label} {arm} run={run_no} untraced")
        for key, c in cc.items():
            if key[0].startswith("control_") and (c.get("writes", 0) or c.get("flushes", 0)):
                print(f"CONTROL {label} {key}: {c.get('writes', 0)} writes, {c.get('flushes', 0)} flushes (must be 0)")
                failed.append(f"control {label} {key}")
    # The equality between the binaries: per arm (live; strict at N = 1000), each count's maximum
    # over the registered unmutated runs (0..RUNS; the live arm's extra parity runs differ in number
    # between binaries, engine-rev-l2b MED) must be the same on the reading and the fire-check binary.
    for arm in ("live", "strict_n1000"):
        # An arm neither trace windowed cannot be equal by default (engine-rev-qaf-fix2 LOW 9: two
        # empty traces compared equal, None == None).
        if not any(k[0] == arm and k[1] < RUNS for k in base_c) or not any(k[0] == arm and k[1] < RUNS for k in read_c):
            print(f"EQUALITY {arm}: no window traced on one binary (refusing)")
            failed.append(f"equality {arm} untraced")
            continue
        for count in ("writes", "flushes"):
            a = max((v.get(count, 0) for k, v in base_c.items() if k[0] == arm and k[1] < RUNS), default=None)
            b = max((v.get(count, 0) for k, v in read_c.items() if k[0] == arm and k[1] < RUNS), default=None)
            if a != b:
                print(f"EQUALITY {arm} {count}: fire-check {a}, reading {b} (the fire-checks are void)")
                failed.append(f"equality {arm} {count}")
        for count in ("allocs", "chunk_inline_allocs"):
            a = max((int(v[count]) for k, v in base_r.items() if k[0] == arm and k[1] < RUNS and count in v), default=None)
            b = max((int(v[count]) for k, v in read_r.items() if k[0] == arm and k[1] < RUNS and count in v), default=None)
            if a != b:
                print(f"EQUALITY {arm} {count}: fire-check {a}, reading {b} (the fire-checks are void)")
                failed.append(f"equality {arm} {count}")
    for mutant in MUTANTS:
        mr, mc = run(fire_bin, mutant, outdir, f"fire-{mutant}")
        # Each registered fire-check must fire in the live AND the strict arm (amendment 2,
        # "Fire-checks, each in the live and the strict arm"); async_waits_durable's registered
        # amount is the strict arm at N = 2 only.
        arms = ("strict_n2",) if mutant == "async_waits_durable" else ("live", "strict_n1000")
        fired_in = {arm: False for arm in arms}
        for key, base in base_r.items():
            arm = key[0]
            if arm not in fired_in:
                continue
            mut = mr.get(key)
            if mut is None or "void" in base or "void" in mut:
                continue
            n = int(base.get("n", "0"))
            bc, mcnt = base_c.get(key, {}), mc.get(key, {})
            if mutant == "create_path_pwrite":
                hit = mcnt.get("writes", 0) - bc.get("writes", 0) == n
            elif mutant == "create_path_fullfsync":
                hit = mcnt.get("flushes", 0) - bc.get("flushes", 0) == n
            elif mutant == "record_alloc_on_create":
                hit = int(mut["allocs"]) - int(base["allocs"]) == n
            elif mutant == "chunk_inline":
                hit = int(mut["chunk_inline_allocs"]) > 0 and int(mut["allocs"]) > int(base["allocs"])
            else:
                hit = mcnt.get("flushes", 0) - bc.get("flushes", 0) >= 1
            fired_in[arm] |= hit
        fired = all(fired_in.values())
        print(f"FIRE-CHECK {mutant}: {'FIRED' if fired else 'DID NOT FIRE'} {fired_in}")
        if not fired:
            failed.append(mutant)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
