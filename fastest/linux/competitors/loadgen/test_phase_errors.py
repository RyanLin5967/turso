#!/usr/bin/env python3
"""A failed op, or a lost connection, is a failure in EVERY phase, never one the run hides (comp-rev-dolt250 HIGH-1;
comp-rev-failcount MED-1..3).

sql-serial narrows Dolt's concurrent-delete race and does not close it, so a serialized Dolt cell can still lose
an op (a recovered panic: "Lost connection") in any phase. Each case below makes one failure deterministic:
  warm-up   a `step write` (must affect >= 1 row) on a table missing id 1: the client's op 1, a warm-up op at
            --warmup 3:0:60, C=1, fails (HIGH-1)
  measured  the same with id 5 missing: a measured op fails
  drain     C=2, client 1 sleeps 1 s per op, client 0 does not, on a table holding ids 0..2 only: client 0's ops
            after the 4 measured claims are drain ops, and from its op 3 on they fail (MED-3; the reviewer's T1)
  teardown  a teardown statement on a missing table fails after the last op (MED-1; T2)
  reconnect the home connection is KILLed by another client mid-run; the run must count a reconnect (MED-2; T3)
Needs a running MySQL-protocol server (a Dolt sql-server) with database DB; no server is started here. The
reconnect case also needs a mysql/mariadb client binary (--mysql-client) to KILL the connection; without one it is
N/A.
  test_phase_errors.py --binary BBLOAD --port PORT [--db bench] [--mysql-client MARIADB]
Prints TALLY ran= pass= na= fail= (gates/tally.py); exit 0 when every case passed, 3 when some were N/A and none
failed, 1 when one failed.
"""
import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path

FAIL_KEYS = ("measured_err", "warmup_err", "drain_err", "other_err", "failed_ops", "reconnects")


def head(port, db):
    return f"protocol mysql\nconnect host=127.0.0.1 port={port} user=root db={db}\n"


def bb(binary, root, spec_text, clients=1, max_ops=10, warmup="3:0:60", extra=(), popen=False):
    spec = root / "t.spec"
    spec.write_text(spec_text)
    out = root / "out"
    cmd = [binary, "--spec", str(spec), "--out", str(out), "--clients", str(clients), "--warmup", warmup,
           "--max-ops", str(max_ops), *extra]
    if popen:
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True), out
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    return p.returncode, summary(out), p.stdout + p.stderr


def summary(out):
    f = out / "summary.json"
    return json.loads(f.read_text()) if f.exists() else {}


def errors_txt(out):
    f = out / "errors.txt"
    return f.read_text() if f.exists() else ""


def prep(binary, port, db, root, sqls):
    """run SQLS once, untimed, on one connection (a 1-op C=1 run whose setup lines are SQLS)"""
    rc, sm, txt = bb(binary, root, head(port, db) + "".join(f"setup {s}\n" for s in sqls) + "step sql SELECT 1\n",
                     max_ops=1, warmup="0:0:0")
    if rc != 0:
        raise SystemExit(f"prep failed rc={rc}: {txt[-400:]}")


def table(name, ids):
    return [f"CREATE TABLE IF NOT EXISTS {name} (id INT PRIMARY KEY, v INT)", f"DELETE FROM {name}",
            f"INSERT INTO {name} VALUES " + ",".join(f"({i},0)" for i in ids)]


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--binary", required=True)
    a.add_argument("--port", required=True)
    a.add_argument("--db", default="bench")
    a.add_argument("--mysql-client")
    args = a.parse_args()
    res = []  # (name, "pass" | "fail" | "na")

    def case(name, ok, got, na=False):
        res.append("na" if na else ("pass" if ok else "fail"))
        print(("N/A " if na else "PASS" if ok else "FAIL"), name, "->", got)

    def fresh():
        return tempfile.TemporaryDirectory(prefix="pe-")

    H = head(args.port, args.db)
    upd = "step write UPDATE {t} SET v = v + 1 WHERE id = {{i}}\n"
    with fresh() as t:
        prep(args.binary, args.port, args.db, Path(t), table("pe0", range(200)))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe0"))
        # LOW-2 (comp-rev-failcount): the control judges behaviour; a counter it does not print is 0, not a failure
        case("control: no op fails -> rc 0, verdict ok, every failure counter 0 or absent",
             rc == 0 and sm.get("verdict") == "ok" and all(sm.get(k, 0) == 0 for k in FAIL_KEYS),
             (rc, sm.get("verdict"), {k: sm.get(k) for k in FAIL_KEYS}))
    with fresh() as t:
        prep(args.binary, args.port, args.db, Path(t), table("pe1", [i for i in range(200) if i != 1]))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe1"))
        case("a failed warm-up op refuses the run (rc 3) and is counted (warmup_err 1)",
             rc == 3 and sm.get("warmup_err") == 1 and "warm-up" in str(sm.get("verdict", "")),
             (rc, sm.get("warmup_err"), sm.get("verdict")))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe1"), extra=("--allow-errors",))
        # the failure must be REPORTED: before HIGH-1 no field and no line said that a warm-up op failed
        case("--allow-errors accepts it but still reports it (warmup_err 1, failed_ops 1)",
             rc == 0 and sm.get("warmup_err") == 1 and sm.get("failed_ops") == 1,
             (rc, sm.get("warmup_err"), sm.get("failed_ops"), sm.get("verdict")))
    with fresh() as t:
        prep(args.binary, args.port, args.db, Path(t), table("pe5", [i for i in range(200) if i != 5]))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe5"))
        case("a failed measured op refuses (rc 3) and is counted (measured_err 1)",
             rc == 3 and sm.get("measured_err") == 1, (rc, sm.get("measured_err"), sm.get("verdict")))
    with fresh() as t:
        prep(args.binary, args.port, args.db, Path(t), table("dr", range(3)))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + "step sql SELECT SLEEP({c})\n" + upd.format(t="dr"), clients=2,
                         max_ops=4, warmup="0:0:0")
        case("C=2: failed drain ops refuse the run (rc 3) and are counted (drain_err >= 1, measured_err 0)",
             rc == 3 and (sm.get("drain_err") or 0) >= 1 and sm.get("measured_err") == 0,
             (rc, sm.get("drain_err"), sm.get("measured_err"), sm.get("verdict")))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + "step sql SELECT 1\nteardown SELECT * FROM no_such_table_t2\n",
                         max_ops=5, warmup="0:0:0")
        case("a failed teardown refuses the run (rc 3), is counted (other_err >= 1) and listed in errors.txt",
             rc == 3 and (sm.get("other_err") or 0) >= 1 and "no_such_table_t2" in errors_txt(Path(t) / "out"),
             (rc, sm.get("other_err"), sm.get("verdict"), errors_txt(Path(t) / "out")[:120]))
    if not args.mysql_client:
        case("a KILLed home connection counts as a reconnect (reconnects >= 1) and refuses (rc 3)", False,
             "no --mysql-client to KILL with", na=True)
    else:
        with fresh() as t:
            p, out = bb(args.binary, Path(t), H + "step sql SELECT SLEEP(0.05)\n", max_ops=60, warmup="0:0:0",
                        popen=True)
            time.sleep(1.5)
            my = [args.mysql_client, "--protocol=TCP", "-h", "127.0.0.1", "-P", str(args.port), "-u", "root",
                  "--skip-ssl", "-N", "-B"]
            pl = subprocess.run(my + ["-e", "SHOW PROCESSLIST"], capture_output=True, text=True, timeout=30).stdout
            ids = [ln.split("\t")[0] for ln in pl.splitlines() if "SLEEP(0.05)" in ln]
            k = subprocess.run(my + ["-e", f"KILL {ids[0]}"], capture_output=True, text=True,
                               timeout=30) if ids else None
            log, _ = p.communicate(timeout=180)
            sm = summary(out)
            case("a KILLed home connection counts as a reconnect (reconnects >= 1) and refuses (rc 3)",
                 bool(ids) and k.returncode == 0 and p.returncode == 3 and (sm.get("reconnects") or 0) >= 1,
                 (ids[:1], k.returncode if k else None, p.returncode, sm.get("reconnects"), sm.get("measured_err"),
                  sm.get("verdict")))
    n = len(res)
    np_, nn, nf = res.count("pass"), res.count("na"), res.count("fail")
    print(f"TALLY ran={np_ + nf} pass={np_} na={nn} fail={nf}")
    print(f"test_phase_errors: {np_}/{n} ({nn} N/A)")
    return 1 if nf else 3 if nn else 0


if __name__ == "__main__":
    raise SystemExit(main())
