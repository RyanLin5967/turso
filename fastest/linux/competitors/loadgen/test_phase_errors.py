#!/usr/bin/env python3
"""A failed op, or a lost connection, is a failure in EVERY phase, never one the run hides (comp-rev-dolt250 HIGH-1;
comp-rev-failcount MED-1..3; comp-rev-b15 LOW-1, LOW-2).

sql-serial narrows Dolt's concurrent-delete race and does not close it, so a serialized Dolt cell can still lose
an op (a recovered panic: "Lost connection") in any phase. Each case makes one failure deterministic.

MySQL protocol (a Dolt sql-server, --port):
  warm-up      a `step write` (it must affect >= 1 row) on a table missing id 1: op 1, a warm-up op at
               --warmup 3:0:60, C=1, fails (HIGH-1)
  measured     the same with id 5 missing: a measured op fails
  drain        C=2; client 1 sleeps 1 s per op, client 0 does not, on a table holding ids 0..2 only. Client 0's ops
               after the 4 measured claims are drain ops, and from its op 3 on they fail (MED-3, the reviewer's T1)
  server-kill  (--dolt) the test's OWN Dolt server is SIGKILLed mid-run: rc 3, failed ops, and failed reconnects in
               other_err (LOW-2)
Both protocols (MySQL; PG with --pg-port and --psql):
  teardown       a teardown statement on a missing table fails after the last op (MED-1, T2)
  kill-running   the home connection is KILLed mid-query; the run must count a reconnect (MED-2, T3)
  kill-idle      ops run on branch connections only (step connect ... / after close); the home connection is KILLed
                 while idle; the run must notice it (comp-rev-b15 LOW-1)
  setup-rerun    a setup statement that succeeds once (CREATE TABLE) fails when the KILLed home reconnects and
                 re-runs it; the run must count it in other_err (LOW-2)
PG only:
  hung-idle      the idle home backend is SIGSTOPped (the test must run on the PG host as its user): the end-of-run
                 check is bounded and blames the home connection (rc 3), never an ops stall (rc 4) (comp-rev-67b2 LOW-B)
Needs running servers; no MySQL/PG server is started here, except for server-kill. KILLs go through a client binary:
--mysql-client (mariadb) and --psql. A case with no tool or server given is N/A.
  test_phase_errors.py --binary BBLOAD --port PORT [--db bench] [--mysql-client MARIADB] [--dolt DOLT]
                       [--pg-port PORT --psql PSQL [--pg-db postgres] [--pg-user postgres]]
Prints TALLY ran= pass= na= fail= (gates/tally.py); exit 0 when every case passed, 3 when some were N/A and none
failed, 1 when one failed.
"""
import argparse
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

FAIL_KEYS = ("measured_err", "warmup_err", "drain_err", "other_err", "failed_ops", "reconnects")


def bb(binary, root, spec_text, clients=1, max_ops=10, warmup="3:0:60", extra=(), popen=False):
    spec = root / "t.spec"
    spec.write_text(spec_text)
    out = root / "out"
    cmd = [binary, "--spec", str(spec), "--out", str(out), "--clients", str(clients), "--warmup", warmup,
           "--max-ops", str(max_ops), "--stall-s", "60", *extra]
    if popen:
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True), out
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=240)
    return p.returncode, summary(out), p.stdout + p.stderr


def summary(out):
    f = out / "summary.json"
    return json.loads(f.read_text()) if f.exists() else {}


def errors_txt(out):
    f = out / "errors.txt"
    return f.read_text() if f.exists() else ""


class MySQL:
    name = "mysql"

    def __init__(self, port, db, client):
        self.port, self.db, self.client = port, db, client

    def head(self, port=None):
        return f"protocol mysql\nconnect host=127.0.0.1 port={port or self.port} user=root db={self.db}\n"

    def sleep(self, s):
        return f"SELECT SLEEP({s})"

    branch = property(lambda self: f"db={self.db}")

    def sql(self, q):
        return subprocess.run([self.client, "--protocol=TCP", "-h", "127.0.0.1", "-P", str(self.port), "-u", "root",
                               "--skip-ssl", "-N", "-B", self.db, "-e", q], capture_output=True, text=True, timeout=30)

    def kill_running(self, marker):
        ids = [ln.split("\t")[0] for ln in self.sql("SHOW PROCESSLIST").stdout.splitlines() if marker in ln]
        return bool(ids) and self.sql(f"KILL {ids[0]}").returncode == 0, ids[:1]

    def kill_idle(self):
        # the home connection: bbload's FIRST connection (lowest id) among those not running the sleep; branch
        # connections run the sleep or are newer; our own row runs SHOW PROCESSLIST
        lines = self.sql("SHOW PROCESSLIST").stdout.splitlines()
        idle = sorted(int(ln.split("\t")[0]) for ln in lines
                      if ln.split("\t")[0].isdigit() and "SLEEP" not in ln and "PROCESSLIST" not in ln.upper())
        return bool(idle) and self.sql(f"KILL {idle[0]}").returncode == 0, idle[:1]


class PG:
    name = "pg"

    def __init__(self, port, db, user, psql):
        self.port, self.db, self.user, self.psql = port, db, user, psql

    def head(self, port=None):
        return f"protocol pg\nconnect host=127.0.0.1 port={port or self.port} user={self.user} dbname={self.db}\n"

    def sleep(self, s):
        return f"SELECT pg_sleep({s})"

    branch = property(lambda self: f"dbname={self.db}")

    def sql(self, q):
        return subprocess.run([self.psql, "-X", "-h", "127.0.0.1", "-p", str(self.port), "-U", self.user, "-d", self.db,
                               "-At", "-c", q], capture_output=True, text=True, timeout=30)

    def kill_running(self, marker):
        r = self.sql("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND "
                     f"query LIKE '{marker}%' AND state = 'active' LIMIT 1").stdout.split()
        return bool(r) and r[0].endswith("|t"), r[:1]

    def kill_idle(self):
        r = self.sql("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND "
                     f"datname = '{self.db}' AND backend_type = 'client backend' AND state = 'idle' "
                     "ORDER BY backend_start LIMIT 1").stdout.split()
        return bool(r) and r[0].endswith("|t"), [int(r[0].split("|")[0])] if r else []

    def idle_home_pid(self):
        r = self.sql("SELECT pid FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND "
                     f"datname = '{self.db}' AND backend_type = 'client backend' AND state = 'idle' "
                     "ORDER BY backend_start LIMIT 1").stdout.split()
        return int(r[0]) if r else None


def home_ids(out):
    """the home connections' server-side ids bbload recorded (backends.tsv rows of kind home)"""
    f = out / "backends.tsv"
    if not f.exists():
        return []
    return [int(ln.split("\t")[3]) for ln in f.read_text().splitlines()[1:] if ln.split("\t")[2:3] == ["home"]]


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--binary", required=True)
    a.add_argument("--port", required=True)
    a.add_argument("--db", default="bench")
    a.add_argument("--mysql-client")
    a.add_argument("--dolt")
    a.add_argument("--pg-port")
    a.add_argument("--psql")
    a.add_argument("--pg-db", default="postgres")
    a.add_argument("--pg-user", default="postgres")
    args = a.parse_args()
    res = []

    def case(name, ok, got, na=False):
        res.append("na" if na else ("pass" if ok else "fail"))
        print(("N/A " if na else "PASS" if ok else "FAIL"), name, "->", got)

    def fresh():
        return tempfile.TemporaryDirectory(prefix="pe-")

    my = MySQL(args.port, args.db, args.mysql_client)
    H = my.head()

    def prep(sqls):
        """run SQLS once on one MySQL connection (a 1-op C=1 run whose setup lines are SQLS)"""
        with fresh() as t:
            rc, sm, txt = bb(args.binary, Path(t), H + "".join(f"setup {s}\n" for s in sqls) + "step sql SELECT 1\n",
                             max_ops=1, warmup="0:0:0")
            if rc != 0:
                raise SystemExit(f"prep failed rc={rc}: {txt[-400:]}")

    def table(name, ids):
        return [f"CREATE TABLE IF NOT EXISTS {name} (id INT PRIMARY KEY, v INT)", f"DELETE FROM {name}",
                f"INSERT INTO {name} VALUES " + ",".join(f"({i},0)" for i in ids)]

    upd = "step write UPDATE {t} SET v = v + 1 WHERE id = {{i}}\n"
    # ---- MySQL only: the op phases ----
    prep(table("pe0", range(200)))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe0"))
        # LOW-2 (comp-rev-failcount): the control judges behaviour; a counter it does not print is 0, not a failure
        case("mysql control: no op fails -> rc 0, verdict ok, every failure counter 0 or absent",
             rc == 0 and sm.get("verdict") == "ok" and all(sm.get(k, 0) == 0 for k in FAIL_KEYS),
             (rc, sm.get("verdict"), {k: sm.get(k) for k in FAIL_KEYS}))
    prep(table("pe1", [i for i in range(200) if i != 1]))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe1"))
        case("mysql: a failed warm-up op refuses the run (rc 3) and is counted (warmup_err 1)",
             rc == 3 and sm.get("warmup_err") == 1 and "warm-up" in str(sm.get("verdict", "")),
             (rc, sm.get("warmup_err"), sm.get("verdict")))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe1"), extra=("--allow-errors",))
        # the failure must be REPORTED: before HIGH-1 no field and no line said that a warm-up op failed
        case("mysql: --allow-errors accepts it but still reports it (warmup_err 1, failed_ops 1)",
             rc == 0 and sm.get("warmup_err") == 1 and sm.get("failed_ops") == 1,
             (rc, sm.get("warmup_err"), sm.get("failed_ops"), sm.get("verdict")))
    prep(table("pe5", [i for i in range(200) if i != 5]))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + upd.format(t="pe5"))
        case("mysql: a failed measured op refuses (rc 3) and is counted (measured_err 1)",
             rc == 3 and sm.get("measured_err") == 1, (rc, sm.get("measured_err"), sm.get("verdict")))
    prep(table("dr", range(3)))
    with fresh() as t:
        rc, sm, txt = bb(args.binary, Path(t), H + "step sql SELECT SLEEP({c})\n" + upd.format(t="dr"), clients=2,
                         max_ops=4, warmup="0:0:0")
        case("mysql C=2: failed drain ops refuse the run (rc 3) and are counted (drain_err >= 1, measured_err 0)",
             rc == 3 and (sm.get("drain_err") or 0) >= 1 and sm.get("measured_err") == 0,
             (rc, sm.get("drain_err"), sm.get("measured_err"), sm.get("verdict")))

    # ---- both protocols: failures outside an op, and lost home connections ----
    backends = [(my, bool(args.mysql_client), "no --mysql-client to KILL with")]
    pg = PG(args.pg_port, args.pg_db, args.pg_user, args.psql) if args.pg_port and args.psql else None
    backends.append((pg, pg is not None, "no --pg-port/--psql"))
    for be, have_kill, why_na in backends:
        nm = be.name if be else "pg"
        if be is None:
            for c in ("teardown", "kill-running", "kill-idle", "setup-rerun"):
                case(f"{nm} {c}", False, why_na, na=True)
            continue
        with fresh() as t:
            rc, sm, txt = bb(args.binary, Path(t), be.head() + "step sql SELECT 1\nteardown SELECT * FROM no_such_table_t2\n",
                             max_ops=5, warmup="0:0:0")
            case(f"{nm} teardown: a failed teardown refuses (rc 3), is counted (other_err >= 1), listed in errors.txt",
                 rc == 3 and (sm.get("other_err") or 0) >= 1 and "no_such_table_t2" in errors_txt(Path(t) / "out"),
                 (rc, sm.get("other_err"), sm.get("verdict")))
        if not have_kill:
            for c in ("kill-running", "kill-idle", "setup-rerun"):
                case(f"{nm} {c}", False, why_na, na=True)
            continue
        with fresh() as t:
            p, out = bb(args.binary, Path(t), be.head() + f"step sql {be.sleep(0.05)}\n", max_ops=60, warmup="0:0:0",
                        popen=True)
            time.sleep(1.5)
            killed, ids = be.kill_running(be.sleep(0.05))
            p.communicate(timeout=240)
            sm = summary(out)
            case(f"{nm} kill-running: a KILLed home connection counts as a reconnect (>= 1) and refuses (rc 3)",
                 killed and p.returncode == 3 and (sm.get("reconnects") or 0) >= 1,
                 (ids, killed, p.returncode, sm.get("reconnects"), sm.get("measured_err"), sm.get("verdict")))
        with fresh() as t:
            spec = be.head() + f"step connect {be.branch}\nstep sql {be.sleep(0.051)}\nafter close\n"
            p, out = bb(args.binary, Path(t), spec, max_ops=60, warmup="0:0:0", popen=True)
            time.sleep(1.5)
            killed, ids = be.kill_idle()
            p.communicate(timeout=240)
            sm = summary(out)
            # comp-rev-67b2 LOW-A: the case asserts its own reason -- the KILLed id is bbload's home connection
            # (backends.tsv), and errors.txt names the end-of-run check as what found it
            et = errors_txt(out)
            case(f"{nm} kill-idle: a home connection KILLed while idle is noticed (rc 3, other_err >= 1, named "
                 "'found after the last op', and the KILLed id is the home row of backends.tsv)",
                 killed and p.returncode == 3 and (sm.get("other_err") or 0) >= 1 and "found after the last op" in et
                 and bool(ids) and int(ids[0]) in home_ids(out),
                 (ids, home_ids(out), killed, p.returncode, sm.get("other_err"), sm.get("reconnects"), et[:120]))
        if be.name == "pg":
            # comp-rev-67b2 LOW-B: a home backend that HANGS (SIGSTOPped) while idle. The end-of-run check must be
            # bounded and blame the home connection (rc 3, other_err >= 1), not report a stall of the ops (rc 4)
            # after --stall-s (60 s here); the test runs on the PG host as the PG user, so it can stop the backend
            with fresh() as t:
                spec = be.head() + f"step connect {be.branch}\nstep sql {be.sleep(0.051)}\nafter close\n"
                p, out = bb(args.binary, Path(t), spec, max_ops=60, warmup="0:0:0", popen=True)
                time.sleep(1.5)
                pid = be.idle_home_pid()
                t0 = time.monotonic()
                if pid:
                    os.kill(pid, signal.SIGSTOP)
                try:
                    p.communicate(timeout=240)
                finally:
                    if pid:
                        os.kill(pid, signal.SIGCONT)
                secs = time.monotonic() - t0
                sm = summary(out)
                et = errors_txt(out)
                case("pg hung-idle: a SIGSTOPped idle home backend is reported as the home connection (rc 3, "
                     "other_err >= 1, named in errors.txt) within 30 s, not as an ops stall (rc 4)",
                     bool(pid) and pid in home_ids(out) and p.returncode == 3 and (sm.get("other_err") or 0) >= 1
                     and "home connection" in et and secs < 30,
                     (pid, home_ids(out), p.returncode, round(secs, 1), sm.get("other_err"), et[:160]))
        be.sql("DROP TABLE IF EXISTS once_t")
        with fresh() as t:
            spec = be.head() + "setup CREATE TABLE once_t (id INT)\n" + f"step sql {be.sleep(0.05)}\n"
            p, out = bb(args.binary, Path(t), spec, max_ops=60, warmup="0:0:0", popen=True)
            time.sleep(1.5)
            killed, ids = be.kill_running(be.sleep(0.05))
            p.communicate(timeout=240)
            sm = summary(out)
            et = errors_txt(out)
            case(f"{nm} setup-rerun: a setup statement failing on the reconnected home refuses (rc 3), other_err >= 1",
                 killed and p.returncode == 3 and (sm.get("other_err") or 0) >= 1 and "setup on a reconnected" in et,
                 (ids, killed, p.returncode, sm.get("other_err"), sm.get("reconnects"), et[:160]))
        be.sql("DROP TABLE IF EXISTS once_t")

    # ---- MySQL only: the server itself dies mid-run (the test's own Dolt server) ----
    if not args.dolt:
        case("mysql server-kill", False, "no --dolt to start a server to kill", na=True)
    else:
        with fresh() as t:
            root = Path(t)
            (root / "root/.dolt").mkdir(parents=True)
            (root / "root/.dolt/config_global.json").write_text(
                '{"metrics.disabled":"true","versioncheck.disabled":"true","user.name":"t","user.email":"t@x.invalid"}\n')
            env = dict(os.environ, DOLT_ROOT_PATH=str(root / "root"), DOLT_DISABLE_EVENT_FLUSH="1")
            (root / "dbs" / args.db).mkdir(parents=True)
            subprocess.run([args.dolt, "init"], cwd=root / "dbs" / args.db, env=env, capture_output=True, timeout=60)
            port = free_port()
            (root / "config.yaml").write_text(f"log_level: warning\nlistener:\n  host: 127.0.0.1\n  port: {port}\n"
                                              f"data_dir: {root / 'dbs'}\ncfg_dir: {root / 'cfg'}\n")
            srv = subprocess.Popen([args.dolt, "sql-server", "--config", str(root / "config.yaml")], cwd=root, env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(120):
                try:
                    socket.create_connection(("127.0.0.1", port), 1).close()
                    break
                except OSError:
                    time.sleep(0.5)
            (root / "w").mkdir()
            p, out = bb(args.binary, root / "w", my.head(port) + "step sql SELECT SLEEP(0.01)\n", max_ops=300,
                        warmup="0:0:0", popen=True)
            time.sleep(1.0)
            srv.send_signal(signal.SIGKILL)
            srv.wait(timeout=30)
            p.communicate(timeout=240)
            sm = summary(out)
            case("mysql server-kill: a server killed mid-run refuses (rc 3), counts failed ops and failed reconnects",
                 p.returncode == 3 and (sm.get("failed_ops") or 0) >= 1 and (sm.get("other_err") or 0) >= 1,
                 (p.returncode, sm.get("failed_ops"), sm.get("other_err"), sm.get("reconnects"), sm.get("verdict")))
    n = len(res)
    np_, nn, nf = res.count("pass"), res.count("na"), res.count("fail")
    print(f"TALLY ran={np_ + nf} pass={np_} na={nn} fail={nf}")
    print(f"test_phase_errors: {np_}/{n} ({nn} N/A)")
    return 1 if nf else 3 if nn else 0


if __name__ == "__main__":
    raise SystemExit(main())
