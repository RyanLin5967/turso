#!/usr/bin/env bash
# run_system.sh SYSTEM MNT RAW -- one competitor on one filesystem (lane fastest-linux-comp). SMOKE ONLY: no
# latency here is credited or quoted before the lead registers the PREREG.
#
#   SYSTEM  pg18-d2 | dolt | doltgres | b1   (pg18-defaults is refused: dropped on Linux, lead ruling artie 6b0bef481b)
#   MNT     the filesystem under test (fs/mkloop.sh made it); everything this run writes lives in MNT/SYSTEM.noindex
#   RAW     the output tree (uploaded as the job's artifact)
#
# For each spec and each C in FT_CLIENTS (default "1 4"), one CELL (RAW/cells/<spec>-c<C>/):
#   conncheck : C+16 connections opened at once and a SELECT 1 on each (amendment 27 (1)); untraced
#   idle      : strace -f -C attached to every server process for FT_IDLE_S seconds with no client (the control)
#   load      : the same attach around one bbload run of exactly N ops (FT_N1 at C=1, FT_N4 otherwise): the traced
#               LABELLING run (bb/), whose latencies are never used
#   timed     : the identical bbload command again with no tracer anywhere (timed/), its TracerPid samples
#               (timed.tracer.tsv) and exit status (timed.rc), judged by timedrun.py into timed.json: the cell's only
#               latency file is timed/raw.tsv (gate-6 review, t3run item 2; PREREG :173)
#   deferred  : PG only -- one CHECKPOINT after the ops, in the LOAD window's own attach after a tsplit stamp
#               (counted as the trace's "post" part): the flushes the ops left for later, reported apart from the
#               window and never added to it. Every cycle DROPs its branch, and dropdb discards the branch's dirty
#               pages before its own checkpoint, so this reads about 0 at C=1 (comp review 1 MED 8): what a KEPT
#               branch pays later is the kept arm's (C=1 cells, FT_KEPT_K kept creates and one CHECKPOINT in one
#               attach, CELLDIR/kept.json; the kept branches are dropped afterwards, untraced)
#   cell.json : stracecount.py cell: flushes per CYCLE = (load - idle x load_s/idle_s) / ops, the raw count, and the
#               split by process role (foreground: main, the load generator's backends, PG checkpointer/walwriter/
#               bgwriter/io workers; background: everything else); background_free when the idle control and the
#               load window's background processes flushed nothing; at C=1 also the split by op phase (create /
#               delete / between, by the load generator's op times), whose create share is the only flushes-per-create
# Every op is a CYCLE (lead review 62430d8bf..b49fb656a HIGH 1; PREREG §7 "N is held fixed"): the spec's timed steps,
# then its untimed after-steps -- close the branch connection, delete the branch (Dolt/Doltgres one at a time) --
# or clonebench --drop, so the live-branch count N stays LIVE0 (read back after prebranch) through every labelling and
# timed run; CELLDIR/live.tsv records it around both, and timedrun.py check refuses a cell where it moved.
# B1 is embedded (no server): its load window runs clonebench under strace from exec, there is no idle control
# (no process exists outside the op loop), and the per-path classes in cell.json split branch from parent flushes.
#
# Then the functional checks of tools/competitors/SMOKE.md: the branch count is LIVE0 after the cells; isolation
# (parent/main sum(v) = the aged parent's PSUM, gen_seed.py sum; a DESIGNATED branch, one M1 op made after the cells
# and kept, reads PSUM + 1 over ROWS rows), and the count then rose by exactly the designated branches, plus, where the system
# clones, the clone proof (filefrag: the branch file's blocks ARE the parent's blocks, flagged shared; strace: the
# copy_file_range / FICLONE calls). RAW/functional.txt ends in a VERDICT line; exit 1 if any check failed.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SYSTEM=${1:?usage: run_system.sh SYSTEM MNT RAW}
MNT=${2:?usage: run_system.sh SYSTEM MNT RAW}
RAW=${3:?usage: run_system.sh SYSTEM MNT RAW}
source "$HERE/common.sh"
source "$HERE/trace.sh"
BB=${FT_BBLOAD:?FT_BBLOAD}
CB=${FT_CLONEBENCH:?FT_CLONEBENCH}
SQ3=${FT_SQLITE3:?FT_SQLITE3}
N1=${FT_N1:-200}
N4=${FT_N4:-200}
ROWS=${FT_ROWS:-10000}
IDLE_S=${FT_IDLE_S:-30}
CLIENTS=${FT_CLIENTS:-1 4}
# comp review 1 MED 8: the PG kept arm's creates per C=1 cell (kept_arm), a count or nothing runs
KEPT_K=${FT_KEPT_K:-10}
[[ $KEPT_K =~ ^[1-9][0-9]*$ ]] || { echo "REFUSED: FT_KEPT_K [$KEPT_K] is not a positive count" >&2; exit 2; }
# Comp review 1 LOW 21: the prebranch runs at FT_PREBRANCH_C clients (C=1 cannot reach N = 1e5 or 1e6 within CAP_S),
# bounded by its own FT_PREBRANCH_S (default: the cap; unregistered: the PREREG names no prebranch bound, a lead ruling
# owed before a real run at 1e5 or more)
PREBRANCH_C=${FT_PREBRANCH_C:-4}
[[ $PREBRANCH_C =~ ^[1-9][0-9]*$ ]] || { echo "REFUSED: FT_PREBRANCH_C [$PREBRANCH_C] is not a positive count" >&2; exit 2; }
# The warm-up of every timed and labelling run (gate-6 review, t3run item 3): PREREG :210's one rule,
# min(max(1000 ops, 10 s), 10% of the cap), as bbload/clonebench --warmup OPS:S:MAX_S; FT_WARMUP overrides it only
# with the same OPS:S:MAX_S form (the T3 driver passes the value it gives fastest_profile). FT_CAP_S is the
# registered per-run cap (default 1800 s). Recorded in run-info.txt and in every run's summary.json warmup_rule.
CAP_S=${FT_CAP_S:-1800}
# A number of seconds on every run: OUTER_S below is integer shell arithmetic on it (review of e11a3c993, finding 5:
# 1.8e3 gave a 601 s outer timeout, 1_800 an arithmetic error).
[[ $CAP_S =~ ^[0-9]+(\.[0-9]+)?$ ]] || { echo "REFUSED: FT_CAP_S [$CAP_S] is not a number of seconds" >&2; exit 2; }
WARMUP=${FT_WARMUP:-$(python3 "$HERE/timedrun.py" rule "$CAP_S")}
# One parent fixture for every system (gate-6 review, t3run item 4): gen_seed.py's t at ROWS rows, aged by AGE
# committed single-row UPDATEs (0 = fresh; PREREG §7 ages with 1e5) and the system's documented maintenance, with
# PREBRANCH live branches made before the cells, each with one private write (the system's own M1 op, untimed and
# untraced). RAW/fixture.json records it (fixture.py write); reduce.py refuses a run whose systems' fixtures differ.
AGE=${FT_AGE:-0}
PREBRANCH=${FT_PREBRANCH:-0}
[[ $AGE =~ ^[0-9]+$ && $PREBRANCH =~ ^[0-9]+$ ]] || { echo "REFUSED: FT_AGE [$AGE] / FT_PREBRANCH [$PREBRANCH] not counts" >&2; exit 2; }
PSUM=$(python3 "$HERE/gen_seed.py" sum --rows "$ROWS" --updates "$AGE")  # the parent's sum(v) after the aging
# LOW 26: a count, or nothing runs (a failed gen_seed left PSUM empty, so every isolation check compared against
# "ROWS|" and "ROWS|1" and failed for the wrong reason, or, in shell arithmetic, read as 0)
[[ $PSUM =~ ^[0-9]+$ ]] || { echo "REFUSED: the parent's sum(v) from gen_seed.py [$PSUM] is not a count" >&2; exit 2; }
[[ $WARMUP =~ ^[0-9]+:[0-9]+(\.[0-9]+)?:[0-9]+(\.[0-9]+)?$ ]] || { echo "REFUSED: warm-up [$WARMUP] is not OPS:S:MAX_S" >&2; exit 2; }
# Real or smoke (lead ruling, artie DECISIONS 6b0bef481b): the CI smoke warm-up cap (FT_WARMUP=1000:10:2) is accepted
# for SMOKE runs only, which say so with FT_DRY=1 and are never credited. Every other run is REAL (FT_DRY=0, also the
# default when FT_DRY is unset) and refuses, before anything runs:
#   - any cap but the registered "1800" and any warm-up but PREREG :210's rule at it, "1000:10:180", compared as exact
#     strings (timedrun.py real);
#   - a fixture or ops total left to this script's smoke defaults: FT_AGE, FT_PREBRANCH and either FT_OPS_TOTAL or both
#     FT_N1 and FT_N4 must be given (review of e11a3c993, finding 4: AGE=0, PREBRANCH=0 and N=200 passed as real).
#     Their VALUES come from the caller's manifest (PREREG §7, amendment 52); this checks they were chosen, not what.
# The T3 runner must pass FT_DRY=1 on --dry-run and these variables on every run. At fork/fastest-linux-t3 37d91390a it
# passes none of FT_DRY, FT_AGE and FT_PREBRANCH (fastest-linux told 2026-10-08T21:10Z), so its runs are refused here.
DRY=${FT_DRY:-0}
case $DRY in
  1) ;;
  0) rwhy=()
     tr=$(python3 -B "$HERE/timedrun.py" real "$CAP_S" "$WARMUP") || rwhy+=("${tr#REFUSED: }")  # empty if it crashed
     [ -n "${FT_AGE:-}" ] || rwhy+=("FT_AGE unset (the smoke default is 0)")
     [ -n "${FT_PREBRANCH:-}" ] || rwhy+=("FT_PREBRANCH unset (the smoke default is 0)")
     [ -n "${FT_OPS_TOTAL:-}" ] || { [ -n "${FT_N1:-}" ] && [ -n "${FT_N4:-}" ]; } ||
       rwhy+=("no ops total: FT_OPS_TOTAL, or both FT_N1 and FT_N4, unset (the smoke default is 200)")
     if [ ${#rwhy[@]} -gt 0 ]; then
       echo "REFUSED: a real run (FT_DRY=0) takes only the registered cap and warm-up and an explicit fixture and ops total: $(printf '[%s] ' "${rwhy[@]}")" >&2
       exit 2
     fi ;;
  *) echo "REFUSED: FT_DRY [$DRY] is neither 0 (a real run) nor 1 (smoke, uncredited)" >&2; exit 2 ;;
esac
SC="$HERE/stracecount.py"
FH="$HERE/fthelp.py"
SPECS="$HERE/loadgen/specs"
ROOT="$MNT/$SYSTEM.noindex"
DATA="$ROOT/data"
mkdir -p "$RAW/cells" "$ROOT"
FUN="$RAW/functional.txt"
: >"$FUN"
nfail=0
fun() { echo "$*" | tee -a "$FUN"; }
pass() { fun "PASS $*"; }
fail() { fun "FAIL $*"; nfail=$((nfail + 1)); }
expect() { # expect NAME GOT WANT
  if [ "$2" = "$3" ]; then pass "$1: $2"; else fail "$1: got [$2] want [$3]"; fi
}
# nops C -> the run's ops TOTAL (all clients together): FT_OPS_TOTAL for every C and system when set, else N1 at C=1
# and N4 otherwise (gate-6 review, t3run item 12); timedrun.py check refuses a run that measured another total.
nops() { python3 "$HERE/timedrun.py" ops "$1" "$N1" "$N4" "${FT_OPS_TOTAL:-}"; }
# LOW 13: every C's ops total is a positive count, checked once before anything runs (a non-count used to fail inside
# each cell, after its conncheck and idle window), and recorded in run-info.txt
OPS_TOTALS=""
for c in $CLIENTS; do
  t=$(nops "$c" 2>&1) && [[ $t =~ ^[0-9]+$ ]] ||
    { echo "REFUSED: the ops total for C=$c is not a positive count (FT_N1 [$N1] FT_N4 [$N4] FT_OPS_TOTAL [${FT_OPS_TOTAL:-}]; FT_CLIENTS [$CLIENTS]): $t" >&2; exit 2; }
  OPS_TOTALS+="${OPS_TOTALS:+,}c$c=$t"
done
# The registered per-run cap bounds every measured window (bbload/clonebench --max-window-s CAP_S: a run it ends with
# >= 1000 ok ops is complete with reduced n), and one outer timeout above it, the same for every system, bounds each
# invocation (gate-6 review, t3run item 16).
OUTER_S=$((${CAP_S%.*} + 600))
# LOW 21's prebranch bound, whole seconds (prebrun_bound's shell arithmetic)
PREBRANCH_S=${FT_PREBRANCH_S:-${CAP_S%.*}}
[[ $PREBRANCH_S =~ ^[1-9][0-9]*$ ]] || { echo "REFUSED: FT_PREBRANCH_S [$PREBRANCH_S] is not a whole number of seconds" >&2; exit 2; }
# LOW 14: a time budget for the whole job (FT_BUDGET_S, seconds; 0 or unset = none). Comp review 1 LOW 32: a cell
# is STARTED only while the budget still holds the longest cell this job has run so far (CELL_MAX_SEEN, measured over
# whole cells: conncheck, idle window, both runs, the CHECKPOINTs, the counts, the kept arm); a cell it does not
# start is recorded "NOT RUN: budget" (cell.json and timed.json; reduce.py counts it, never a silent MISSING). Every
# step of a cell is bounded: the load generators by OUTER_S, every SQL statement (the CHECKPOINTs included) by
# SQL_TIMEOUT_S, every stracecount count by COUNT_TIMEOUT_S. What this does NOT promise (the old comment's "can
# never cut a cell" was false: a cell has three OUTER_S-bounded runs and the CHECKPOINTs and counts had no bound): a
# cell longer than every earlier one can still overrun the budget, and the work after the cells (the plants since
# LOW 25, the designated branches, the functional checks) is outside it; the workflow's step timeout leaves room for
# that, and if it fires the job fails and reduce.py reports the cells it cut as MISSING.
SQL_TIMEOUT_S=600
COUNT_TIMEOUT_S=1800
BUDGET_S=${FT_BUDGET_S:-0}
[[ $BUDGET_S =~ ^[0-9]+$ ]] || { echo "REFUSED: FT_BUDGET_S [$BUDGET_S] is not a number of seconds" >&2; exit 2; }
CELL_MAX_SEEN=0
cell_done() { # cell_done START_SECONDS -- a cell begun at START has ended: the longest so far is the next reserve
  local dt=$((SECONDS - $1))
  [ "$dt" -gt "$CELL_MAX_SEEN" ] && CELL_MAX_SEEN=$dt
  return 0
}
over_budget() { # over_budget CELLDIR NAME -> 0 (and the cell recorded NOT RUN) when the budget cannot hold another cell
  [ "$BUDGET_S" -gt 0 ] && [ $((SECONDS + CELL_MAX_SEEN)) -ge "$BUDGET_S" ] || return 1
  local v="NOT RUN: budget (${SECONDS} s elapsed + ${CELL_MAX_SEEN} s for the longest cell so far >= FT_BUDGET_S ${BUDGET_S} s)"
  mkdir -p "$1"
  printf '{"name": "%s", "verdict": "%s"}\n' "$SYSTEM/$2" "$v" >"$1/cell.json"
  printf '{"verdict": "%s"}\n' "$v" >"$1/timed.json"
  fail "$2: $v"
  return 0
}
fsused() { sync -f "$MNT"; df -B1 --output=used "$MNT" | tail -1 | tr -d ' '; }

# Amendment 14's registered variants. PG18: STRATEGY=FILE_COPY with file_copy_method=clone and STRATEGY=WAL_LOG at D2
# (pg18-d2), in the forms M1c-create (CREATE alone), M1c-connect (pg18-m1c*: CREATE, a new connection, SELECT 1) and
# M1 (CREATE, connect, first write), plus the clone proof's negative control pg18-create-copy (FILE_COPY with this
# session's file_copy_method = copy). The at-defaults system pg18-defaults (pg18.sh MODE d1clone) is DROPPED on Linux
# by the lead's ruling (artie DECISIONS 6b0bef481b): on Linux it configures the same server as pg18-d2 (pg18.sh's
# header), and PREREG-CORE v2's OUT list excludes PG18 at its defaults; it is refused below, not run.
# Dolt sql-server and Doltgres: variants (a) checkout(parent) + checkout('-b', name),
# (b) dolt_branch(name, parent) + checkout(name), (c) dolt_branch(name, parent) + a new connection to db/name +
# SELECT 1, each also as M1 (+ first write), and dolt_branch alone (M1c-create of (b) and (c)).
PG_SPECS="pg18-select1 pg18-create pg18-m1c pg18-m1 pg18-create-wal pg18-m1c-wal pg18-m1-wal"
DOLT_V="create a-m1c a-m1 b-m1c b-m1 c-m1c c-m1"
case $SYSTEM in
  pg18-d2) KIND=pg MODE=d2 PORT=55432 SPECLIST="$PG_SPECS pg18-create-copy" ;;
  pg18-defaults) echo "REFUSED: pg18-defaults is dropped on Linux (lead ruling, artie DECISIONS 6b0bef481b); run pg18-d2, which carries pg18-create-copy" >&2; exit 2 ;;
  dolt) KIND=dolt PORT=53306 SPECLIST="dolt-select1$(for v in $DOLT_V; do printf ' dolt-%s' "$v"; done)" ;;
  doltgres) KIND=doltgres PORT=55433 SPECLIST="doltgres-select1$(for v in $DOLT_V; do printf ' doltgres-%s' "$v"; done)" ;;
  b1) KIND=b1 SPECLIST="m1c-d2 m1-d2 m1c-d0 m1-d0" ;;
  *) echo "unknown system $SYSTEM" >&2; exit 2 ;;
esac
# The isolation checks' count and sum(v), the sum as an exact integer. Dolt's SUM over an INT column is a DOUBLE,
# which the mariadb client prints as 8.996383e+07 once the aged parent's sum is large (run 37841577896 at b49fb656a:
# all four dolt jobs failed isolation with got [10000|8.996383e+07] want [10000|89963830]); CAST(... AS DECIMAL(20,0))
# prints it as an integer (measured by the lead's review on Dolt 2.4.1: 10000|89963830). PG's sum(int) is a bigint,
# and Doltgres printed exact integers in that run (all 16 PG and Doltgres jobs: 10000|89963830, branches 89963831).
case $KIND in
  dolt) COUNTSUM="SELECT count(*), CAST(sum(v) AS DECIMAL(20,0)) FROM t" ;;
  *) COUNTSUM="SELECT count(*), sum(v) FROM t" ;;
esac
for spec in $SPECLIST; do  # a missing spec file is a harness defect, found before anything runs
  [ "$KIND" = b1 ] || [ -f "$SPECS/$spec.spec" ] || { echo "REFUSED: no spec $SPECS/$spec.spec" >&2; exit 2; }
done
# FDSYNC_SCAN_HOOK is the fire-check's fault injector (F6d/F6e make the pre-attach fd scan lose its task): a run
# with it set would count windows whose scans were sabotaged (third re-review, finding 4).
[ -z "${FDSYNC_SCAN_HOOK:-}" ] || { echo "REFUSED: FDSYNC_SCAN_HOOK is set ($FDSYNC_SCAN_HOOK); it is for firecheck_strace.sh only" >&2; exit 2; }
# The flush counter must have passed its fire-check on this runner and filesystem first (review finding 8).
FC=${FT_FIRECHECK:?FT_FIRECHECK: the fire-check verdict file (firecheck_strace.sh OUT/firecheck.txt)}
# ALL of this tree's fire-check passed, not a verdict that merely starts with PASS (fifth review, finding 4): the last
# line is "VERDICT PASS n/n" with n = firecheck_strace.sh's own NCHECK, n PASS lines with n distinct names, no FAIL.
FC_N=$(sed -n 's/^NCHECK=\([0-9][0-9]*\)$/\1/p' "$HERE/firecheck_strace.sh")
FC_LAST=$(tail -1 "$FC" 2>/dev/null) || FC_LAST=""
FC_PASS=$(grep '^PASS ' "$FC" 2>/dev/null | cut -d: -f1 | sort | uniq | awk 'END {print NR}')
FC_PASSL=$(grep '^PASS ' "$FC" 2>/dev/null | awk 'END {print NR}')
FC_FAIL=$(grep '^FAIL' "$FC" 2>/dev/null | awk 'END {print NR}')
if [ -z "$FC_N" ] || [ "$FC_LAST" != "VERDICT PASS $FC_N/$FC_N" ] || [ "$FC_PASS" != "$FC_N" ] ||
  [ "$FC_PASSL" != "$FC_N" ] || [ "$FC_FAIL" != 0 ]; then
  echo "REFUSED: the flush counter's fire-check did not pass all $FC_N checks: $FC ends [$FC_LAST], $FC_PASSL PASS line(s), $FC_PASS distinct, $FC_FAIL FAIL line(s)" | tee "$FUN"
  exit 1
fi
# The drive class under MNT (lead ruling, artie DECISIONS 6b0bef481b; SMOKE erratum E3: GitHub jobs land on a
# write-through virtual disk or a write-back NVMe with FUA): drive.py follows MNT's device through the loop to its
# backing file's disk and records RAW/drive.json; a class it cannot determine refuses the run.
DRIVE=$(python3 -B "$HERE/drive.py" record "$MNT" "$RAW/drive.json") ||
  { echo "REFUSED: the drive class under $MNT: [$DRIVE] (drive.py)" | tee "$FUN"; exit 2; }
# comp review 1 LOW 35: a real run refuses a volatile RAM disk and any loop device in the chain
if [ "$DRY" = 0 ]; then
  RC_REAL=$(python3 -B "$HERE/drive.py" real-check "$RAW/drive.json") || { echo "$RC_REAL" | tee "$FUN"; exit 2; }
fi
# Every cell this run must produce, written BEFORE any runs: reduce.py reports a listed cell without a cell.json as
# MISSING, so a cell that returns early cannot simply disappear (review finding 11).
for spec in $SPECLIST; do
  for c in $CLIENTS; do
    if [ "$KIND" = b1 ]; then echo "b1-$spec-c$c"; else echo "$spec-c$c"; fi
  done
done >"$RAW/expected-cells.txt"
{ echo "system=$SYSTEM kind=$KIND mnt=$MNT fstype=$(findmnt -n -o FSTYPE -T "$MNT") rows=$ROWS n1=$N1 n4=$N4 idle_s=$IDLE_S clients=[$CLIENTS] cap_s=$CAP_S warmup=$WARMUP dry=$DRY ops_total=$OPS_TOTALS age=$AGE prebranch=$PREBRANCH prebranch_c=$PREBRANCH_C prebranch_s=$PREBRANCH_S parent_sum=$PSUM";
  [ "$KIND" = pg ] && echo "pg_systems=pg18-d2 (with pg18-create-copy) pg18-defaults=DROPPED on Linux (lead ruling artie 6b0bef481b)"
  echo "$DRIVE"
  echo "strace=$(strace -V | sed -n 1p) kernel=$(uname -r) arch=$(uname -m)"
  echo "## df (the loop backing file lives on / or /mnt)"; df -B1 / /mnt "$MNT" 2>&1; } | tee "$RAW/run-info.txt"

# ---------------------------------------------------------------- server systems
server_pid() { # the server's recorded main pid; strace_attach adds every live descendant (PG: the postmaster's
  # checkpointer, walwriter, bgwriter, io workers, launchers and backends, forked before the attach, which -f alone
  # would not reach) and proves none escaped. Dolt/Doltgres are one process.
  cat "$DATA.ftpid"
}
srv() { # srv CMD ARGS... -- the system's own script
  case $KIND in
    pg) bash "$HERE/pg18.sh" "$@" ;;
    dolt) bash "$HERE/dolt.sh" "$@" ;;
    doltgres) bash "$HERE/doltgres.sh" "$@" ;;
  esac
}
sqlq() { # sqlq SQL -- one statement on the home database (PG: postgres, NEVER the template: a session on p makes
         # a concurrent CREATE DATABASE wait in CountOtherDBBackends); columns joined by '|'
  case $KIND in  # LOW 32: every statement bounded (a CHECKPOINT had no bound)
    pg) timeout "$SQL_TIMEOUT_S" bash "$HERE/pg18.sh" sql "$DATA" postgres "$1" ;;
    dolt) timeout "$SQL_TIMEOUT_S" bash "$HERE/dolt.sh" sql "$DATA" "$1" | tr '\t' '|' ;;
    doltgres) timeout "$SQL_TIMEOUT_S" bash "$HERE/doltgres.sh" sql "$DATA" "$1" | tr '\t' '|' ;;
  esac
}
sqlp() { # sqlp SQL -- on the parent: PG's template p (only after every cell), Dolt/Doltgres main
  case $KIND in
    pg) timeout "$SQL_TIMEOUT_S" bash "$HERE/pg18.sh" sql "$DATA" p "$1" ;;
    *) sqlq "$1" ;;
  esac
}
template_idle() { # PG: wait until pg_stat_activity shows no backend on the template (amendment 14), max 30 s
  local i n
  for ((i = 0; i < 120; i++)); do
    n=$(sqlq "SELECT count(*) FROM pg_stat_activity WHERE datname = 'p'")
    [ "$n" = 0 ] && return 0
    sleep 0.25
  done
  return 1
}
on_branch() { # on_branch BRANCH SQL -- SQL on a branch; prints the last output line
  case $KIND in
    pg) srv sql "$DATA" "$1" "$2" ;;
    dolt) "$FT_MARIADB" --protocol=TCP -h 127.0.0.1 -P "$PORT" -u root --skip-ssl -N bench \
            -e "CALL DOLT_CHECKOUT('$1'); $2" | tail -1 | tr '\t' '|' ;;
    doltgres) PGPASSWORD=password "$FT_PG18/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres \
            -d postgres -At -c "SELECT dolt_checkout('$1')" -c "$2" | tail -1 ;;
  esac
}
count_branches() {
  case $KIND in
    pg) sqlq "SELECT count(*) FROM pg_database WHERE datname LIKE 'b\_%'" ;;
    dolt|doltgres) sqlq "SELECT count(*) FROM dolt_branches" ;;
    b1) find "$ROOT/branches" -name 'b_*.db' | awk 'END {print NR}' ;;
  esac
}
# The live-branch count N (lead review 62430d8bf..b49fb656a HIGH 1). Every create spec deletes its branch in an
# untimed after-step (bbload) or with --drop (clonebench), so N stays at LIVE0 -- the count right after prebranch, main
# included for Dolt/Doltgres -- for every cell. live_mark CELLDIR KEY appends "KEY N" to CELLDIR/live.tsv (read
# outside every traced window); timedrun.py check refuses the cell unless all four keys read LIVE0.
LIVE0=""
live_mark() { echo "$2 $(count_branches)" >>"$1/live.tsv"; }

bb_args() { # bb_args SPEC C N OUT [nowarm] -> BBA: the one bbload command line, so the labelling and timed runs are
  # identical; both warm up by WARMUP (gate-6 review, t3run item 3). nowarm: the untimed conncheck.
  BBA=("$BB" --spec "$SPECS/$1.spec" --out "$4" --clients "$2" --max-ops "$3" --set port="$PORT" --set rows="$ROWS"
    --stall-s 600 --max-window-s "$CAP_S")
  BBA=(timeout "$OUTER_S" "${BBA[@]}")
  [ "${5:-}" = nowarm ] || BBA+=(--warmup "$WARMUP")
}
cb_args() { # cb_args OP SYNC BDIR C N OUT -> CBA: the one clonebench command line of a B1 cell, so its labelling and
  # timed runs are identical, as bb_args makes the servers' (LOW 27: the two were hand-copied)
  CBA=(timeout "$OUTER_S" "$CB" run --mode b1 --op "$1" --sync "$2" --parent "$ROOT/parent.db" --dir "$3"
    --clients "$4" --max-ops "$5" --rows "$ROWS" --warmup "$WARMUP" --max-window-s "$CAP_S" --drop --out "$6")
}
# kept_arm SPEC CELLDIR -- comp review 1 MED 8 (PG, at C=1): KEPT_K creates of SPEC whose branches are KEPT (bbload
# --skip-after: no close, no DROP), then one CHECKPOINT inside the same attach after a tsplit stamp. That "post" part is
# the deferred writes a kept branch pays, which the cells' per-cycle DROP throws away (dropdb's DropDatabaseBuffers and
# ForgetDatabaseSyncRequests run before its forced checkpoint, so the cells' deferred window reads about 0 at C=1).
# CELLDIR/kept.json: K, the deferred flushes, per create, by class, and the count's verdict. Then the K branches are
# dropped and checkpointed, untraced, and the live-branch count must be back at LIVE0.
kept_arm() {
  local spec=$1 d=$2 k=$KEPT_K kd="$2/kept" tag i rc=0
  template_idle || { fail "$spec kept arm: a backend stayed on template p for 30 s"; return; }
  strace_attach "$kd" "$(server_pid)" || { fail "$spec kept arm attach"; return; }
  bb_args "$spec" 1 "$k" "$d/kept-bb" nowarm
  "${BBA[@]}" --skip-after >"$d/kept-bb.txt" 2>&1 || rc=$?
  strace_mark "$kd" tsplit
  sqlq "CHECKPOINT" >"$d/kept.checkpoint.txt" 2>&1 || fail "$spec kept arm CHECKPOINT rc=$?"
  strace_detach "$kd"
  [ $rc -eq 0 ] || fail "$spec kept arm bbload rc=$rc ($(tail -1 "$d/kept-bb.txt"))"
  count "$kd" "$d/kept-bb/backends.tsv" post "$d/kept-deferred.json"
  python3 -B -c 'import json, sys
c = json.load(open(sys.argv[1])); k = int(sys.argv[2]); ok = c.get("verdict") == "ok" and isinstance(c.get("flushes"), int)
json.dump({"spec": sys.argv[3], "k": k, "verdict": c.get("verdict"), "deferred_flushes": c.get("flushes"),
           "per_create": c["flushes"] / k if ok else None, "by_class": c.get("by_class")}, open(sys.argv[4], "w"))' \
    "$d/kept-deferred.json" "$k" "$spec" "$d/kept.json" || fail "$spec kept arm: kept.json"
  tag=$(jf "$d/kept-bb/summary.json" run_tag)
  [[ $tag =~ ^[a-z0-9]+$ ]] || { fail "$spec kept arm: run tag [$tag]"; return; }
  for ((i = 0; i < k; i++)); do
    sqlq "DROP DATABASE b_${tag}_0_${i}" >>"$d/kept.drops.txt" 2>&1 || fail "$spec kept arm: DROP DATABASE b_${tag}_0_${i}"
  done
  sqlq "CHECKPOINT" >/dev/null 2>&1 || fail "$spec kept arm: CHECKPOINT after the drops"
  expect "kept arm $spec: branches back at LIVE0 after the drops" "$(count_branches)" "$LIVE0"
}
# Every spec's create is its step 1 (comp review 1 MED 6: variant (a)'s timed no-op checkout of main is gone, so its
# create_step 2 case went with it), so fthelp.py ops reads the create at step 1 everywhere.
bbload() { # bbload SPEC C N OUT [nowarm] -> bbload's rc
  local rc=0
  bb_args "$@"
  "${BBA[@]}" >"$4.txt" 2>&1 || rc=$?
  cat "$4.txt"
  return $rc
}
# timed_run OUT SERVERPID -- CMD...: trace.sh's (lead review 62430d8bf..b49fb656a MED 4: append-only whole-tree sweeps
# every 0.05 s; the old sampler kept only the last sample, every 0.5 s, and forked ps per sample).

# count OUT -- stracecount over one window. A refused or crashed count FAILS the job (review finding 1: it used to
# end in `|| true`, so a REFUSED window still left the job green); the cell carries the verdict too.
count() { # count OUT [CLIENTS [PART JSON]] -- PART pre|post counts one side of OUT's tsplit stamp into JSON
  # CPHASES=RAW.tsv (set by the caller for a C=1 load window only): split its flushes into create / delete / between by
  # the load generator's op times (lead ruling on HIGH 1's flush attribution: at C>1 ops overlap, so no split).
  local rc=0 cl=() pt=() ph=() js="$1.json"
  [ -n "${2:-}" ] && cl=(--clients "$2")
  [ -n "${3:-}" ] && { pt=(--part "$3"); js=$4; }
  [ -n "${CPHASES:-}" ] && ph=(--phases "$CPHASES")
  timeout "$COUNT_TIMEOUT_S" python3 "$SC" count "$1.strace" --extra "$1.strace.err" --root "$DATA" --window "$1.window" \
    ${cl[@]+"${cl[@]}"} ${pt[@]+"${pt[@]}"} ${ph[@]+"${ph[@]}"} >"$js" || rc=$?  # LOW 32: bounded
  [ $rc -eq 0 ] || fail "count $js: stracecount rc=$rc ($(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['verdict'])" "$js" 2>&1 | tail -1))"
}
# ops_of BBOUT CELLDIR -- "<total> <ok> <created>" into CELLDIR/ops.txt. A reader failure FAILS the job and writes
# zero ops, which the cell then refuses (it used to turn silently into "0 0 0"). Called in the driver's own shell,
# never inside $(...) or <(...), where fail()'s count would be lost.
ops_of() {
  if ! python3 "$FH" ops "$1" 1 >"$2/ops.txt"; then fail "ops reader on $1"; echo "0 0 0" >"$2/ops.txt"; fi
  # LOW 26: the reader's line itself must be three counts, or the job FAILS (a short or empty line read as empty
  # totals and reached stracecount's --ops)
  ops_line_ok "$2/ops.txt" || { fail "ops reader on $1: [$(head -c 200 "$2/ops.txt")] is not three counts"; echo "0 0 0" >"$2/ops.txt"; }
}
# ops_line_ok FILE -- FILE is exactly one line of three counts, "<total> <ok> <created>"
ops_line_ok() {
  [ "$(wc -l <"$1" | tr -d ' ')" = 1 ] && grep -Eq '^[0-9]+ [0-9]+ [0-9]+$' "$1"
}
# timedrun_check CELLDIR N LABEL -- the timed run must stand (timedrun.py check: untraced at both ends, rc 0, exactly
# N ops); its created branches go to CELLDIR/timed.ops.txt for the branch-count check. Anything else FAILS the job.
# no_plant_before_cells -- comp review 1 LOW 25: no test plant (the stream, cap, warm-up, claim-time or delete plants)
# has run on MNT before the measured cells, so none perturbs them (a real run's cells must meet the store as the seed
# and the prebranch left it); their output dirs are the evidence
no_plant_before_cells() {
  local ran=none
  { [ -e "$RAW/plants" ] || [ -e "$RAW/plant-stream-1e6.txt" ]; } && ran="ran: $(ls -d "$RAW"/plants "$RAW"/plant-* 2>/dev/null | tr '\n' ' ')"
  expect "no test plant ran on MNT before the measured cells (LOW 25)" "$ran" none
}
timedrun_check() {
  # the lead's LOW 21 ruling: the cell's prebranch (N, clients, bound) goes into timed.json; beyond 1,000 it is
  # exploratory, never credited
  echo "prebranch=$PREBRANCH prebranch_c=$PREBRANCH_C prebranch_s=$PREBRANCH_S" >"$1/prebranch.txt"
  python3 "$HERE/timedrun.py" check "$1" "$2" "$WARMUP" "${LIVE0:-unknown}" "$CAP_S" "$DRY" >"$1/timed.check.txt" 2>&1 || fail "$3 timed run: $(tail -c 400 "$1/timed.check.txt")"
  if [ -d "$1/timed" ] && python3 "$FH" ops "$1/timed" 1 >"$1/timed.ops.txt" 2>"$1/timed.ops.err" &&
    ops_line_ok "$1/timed.ops.txt"; then :; else  # LOW 26: three counts, or the job FAILS
    fail "$3 timed run: ops reader ($(tail -c 200 "$1/timed.ops.err" 2>/dev/null))"; echo "0 0 0" >"$1/timed.ops.txt"
  fi
}
# judge_cell CELLDIR -- the cell's verdict must be "ok"; anything else (REFUSED, INCOMPLETE, NOT CLEAN, a missing or
# unreadable cell.json) FAILS the job.
judge_cell() {
  local v
  v=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['verdict'])" "$1/cell.json" 2>&1 | tail -1)
  [ "$v" = ok ] || fail "cell $(basename "$1"): $v"
}

run_server_cell() { # run_server_cell SPEC C
  local spec=$1 c=$2 n d rc used0 used1 total ok created
  n=$(nops "$c")
  d="$RAW/cells/$spec-c$c"
  over_budget "$d" "$spec-c$c" && return  # LOW 14
  mkdir -p "$d"
  echo "=== $SYSTEM $spec C=$c N=$n"
  bbload "${KIND/pg/pg18}-select1" $((c + 16)) $((c + 16)) "$d/conncheck" nowarm >/dev/null ||
    fail "$spec-c$c conncheck: C+16=$((c + 16)) connections (rc $?, $(tail -1 "$d/conncheck.txt"))"
  strace_attach "$d/idle" "$(server_pid)" || { fail "$spec-c$c idle attach"; return; }
  sleep "$IDLE_S"
  strace_detach "$d/idle"
  if [ "$KIND" = pg ]; then template_idle || fail "$spec-c$c: a backend stayed on template p for 30 s"; fi
  used0=$(fsused)
  local log0 log1
  log0=$(stat -c %s "$DATA.log")
  live_mark "$d" label_before
  strace_attach "$d/load" "$(server_pid)" || { fail "$spec-c$c load attach"; return; }
  rc=0
  bbload "$spec" "$c" "$n" "$d/bb" || rc=$?
  # Nothing but a stat between the ops and the window's end (tsplit for PG, the detach otherwise): fsused's
  # `sync -f` and df ran inside the window and stretched load_s (third review, finding 5); it now runs after it. The
  # end stamp itself is a pipe round-trip to trace.sh's stamper, not an interpreter start (clock_pair).
  log1=$(stat -c %s "$DATA.log")
  local defer=() tw=()
  if [ "$KIND" = pg ]; then
    # The deferred window is part of a PG cell (for WAL_LOG it holds most of the cost). It runs INSIDE the load
    # window's attach, after a tsplit stamp, and is counted as that trace's "post" part (second review, finding 2:
    # every strace state-mismatch message came from a separate second attach to a server it had just loaded). A failed
    # CHECKPOINT FAILS the job, and the cell refuses without the deferred count (review finding 6).
    defer=(--deferred "$d/deferred.json")
    strace_mark "$d/load" tsplit
    sqlq "CHECKPOINT" >"$d/deferred.checkpoint.txt" 2>&1 || fail "$spec-c$c deferred CHECKPOINT rc=$? ($(tail -1 "$d/deferred.checkpoint.txt"))"
  fi
  strace_detach "$d/load"
  live_mark "$d" label_after
  used1=$(fsused)  # PG: after the CHECKPOINT
  echo "fs_used_before=$used0 fs_used_after=$used1 delta=$((used1 - used0))" >"$d/space.txt"
  [ $rc -eq 0 ] || fail "$spec-c$c bbload rc=$rc ($(tail -1 "$d/bb.txt"))"
  # The server log written during the load window. PG (amendment 14 section 6): a CREATE DATABASE that found a backend
  # on the template waited in CountOtherDBBackends, which terminates autovacuum workers there (logged FATAL
  # "terminating autovacuum process due to administrator command") and errors after 5 s if another backend stays
  # ("source database ... is being accessed by other users"). Those creates are FLAGGED here, not dropped.
  tail -c +$((log0 + 1)) "$DATA.log" | head -c $((log1 - log0)) >"$d/server_log_load.txt"
  # Comp review 1 LOW 19: the per-cycle DROP runs CountOtherDBBackends too, so its terminations and busy errors are
  # counted apart (fthelp.py pgwaits: delete_waits.txt) and only the template's flag the cell
  if [ "$KIND" = pg ]; then
    local av busy
    python3 -B "$HERE/fthelp.py" pgwaits "$d/server_log_load.txt" "$d/template_waits.txt" "$d/delete_waits.txt" \
      >"$d/pgwaits.txt" 2>&1 || fail "$spec-c$c: pgwaits ($(tail -1 "$d/pgwaits.txt"))"
    av=$(sed -n 's/.*autovacuum_terminated_in_window=\([0-9]*\).*/\1/p' "$d/template_waits.txt")
    busy=$(sed -n 's/.*template_busy_errors=\([0-9]*\).*/\1/p' "$d/template_waits.txt")
    tw=(--template-waits "$d/template_waits.txt")
    [ "$av" = 0 ] && [ "$busy" = 0 ] || fun "FLAG $spec-c$c: creates waited on the template (CountOtherDBBackends): autovacuum workers terminated $av (unattributed included), busy-template errors $busy"
  fi
  # The TIMED run: the identical bbload command with no tracer anywhere (gate-6 review, t3run item 2; PREREG :173).
  # The labelling run above gives the flush counts, never a latency; this one gives the only latency file
  # (timed/raw.tsv). PG: the template must be idle again first, and a CHECKPOINT after it (untraced) keeps the timed
  # run's deferred work out of the next cell's idle control and labelling window.
  if [ "$KIND" = pg ]; then template_idle || fail "$spec-c$c: a backend stayed on template p for 30 s (before the timed run)"; fi
  bb_args "$spec" "$c" "$n" "$d/timed"
  live_mark "$d" timed_before
  local tlog0 tlog1
  tlog0=$(stat -c %s "$DATA.log")
  timed_run "$d/timed" "$(server_pid)" -- "${BBA[@]}" >/dev/null || true
  tlog1=$(stat -c %s "$DATA.log")
  live_mark "$d" timed_after
  # LOW 15: the timed run's own server log and, for PG, its template waits (amendment 14 section 6's flag covered only
  # the labelling run): recorded in timed.template_waits.txt, read into timed.json, and FLAGGED like the labelling run's
  tail -c +$((tlog0 + 1)) "$DATA.log" | head -c $((tlog1 - tlog0)) >"$d/timed.server_log.txt"
  if [ "$KIND" = pg ]; then
    local tav tbusy
    python3 -B "$HERE/fthelp.py" pgwaits "$d/timed.server_log.txt" "$d/timed.template_waits.txt" \
      "$d/timed.delete_waits.txt" >"$d/timed.pgwaits.txt" 2>&1 || fail "$spec-c$c: timed pgwaits ($(tail -1 "$d/timed.pgwaits.txt"))"
    tav=$(sed -n 's/.*autovacuum_terminated_in_window=\([0-9]*\).*/\1/p' "$d/timed.template_waits.txt")
    tbusy=$(sed -n 's/.*template_busy_errors=\([0-9]*\).*/\1/p' "$d/timed.template_waits.txt")
    [ "$tav" = 0 ] && [ "$tbusy" = 0 ] ||
      fun "FLAG $spec-c$c timed run: creates waited on the template (CountOtherDBBackends): autovacuum workers terminated $tav (unattributed included), busy-template errors $tbusy"
  fi
  if [ "$KIND" = pg ]; then sqlq "CHECKPOINT" >"$d/timed.checkpoint.txt" 2>&1 || fail "$spec-c$c post-timed CHECKPOINT rc=$?"; fi
  # Comp review 1 MED 8: the per-cycle DROP throws a branch's dirty pages away before any checkpoint writes them, so
  # the deferred window above reads about 0 for WAL_LOG; the KEPT arm counts what a kept branch pays (kept_arm, C=1).
  if [ "$KIND" = pg ] && [ "$c" = 1 ] && [ "$spec" != pg18-select1 ]; then
    kept_arm "$spec" "$d"
    if [ "$spec" = pg18-create-wal ]; then
      expect "kept arm $spec-c1: deferred flushes per kept create > 0 (the writes the per-cycle DROP erases)" \
        "$(python3 -B -c 'import json, sys; k = json.load(open(sys.argv[1])); print(k.get("verdict") == "ok" and (k.get("per_create") or 0) > 0)' "$d/kept.json" 2>&1 | tail -1)" \
        True
    fi
  fi
  timedrun_check "$d" "$n" "$spec-c$c"
  count "$d/idle"
  # Every op is a cycle now (create [+ switch][+ first write] + the untimed delete): at C=1 the load window's flushes
  # are also split by op phase, the create share being the only per-create flush figure (lead ruling on HIGH 1).
  local cph=""
  [ "$c" = 1 ] && cph="$d/bb/raw.tsv"
  if [ "$KIND" = pg ]; then
    CPHASES=$cph count "$d/load" "$d/bb/backends.tsv" pre "$d/load.json"
    count "$d/load" "$d/bb/backends.tsv" post "$d/deferred.json"
  else
    CPHASES=$cph count "$d/load" "$d/bb/backends.tsv"
  fi
  ops_of "$d/bb" "$d"
  read -r total ok created <"$d/ops.txt"
  # comp review 1 LOW 22: every server here is D2, so a spec's untimed delete must flush (checked at C=1); select1
  # deletes nothing
  local ddur=d2
  case $spec in *-select1) ddur=none ;; esac
  python3 "$SC" cell --name "$SYSTEM/$spec-c$c" --load "$d/load.json" --idle "$d/idle.json" \
    --load-s "$(window_s "$d/load")" --idle-s "$(window_s "$d/idle")" --ops "$total" --ops-ok "$ok" \
    --delete-durable "$ddur" ${defer[@]+"${defer[@]}"} ${tw[@]+"${tw[@]}"} >"$d/cell.json"
  judge_cell "$d"
  python3 -c "import json,sys; c=json.load(open(sys.argv[1])); p=c.get('per_op',{}); print('cell', c['name'], 'ops', c['ops'], 'flushes/op', p.get('flushes'), 'raw/op', p.get('flushes_raw'), 'foreground/op', p.get('foreground'), 'background/op', p.get('background'), 'background_free', c.get('background_free'), 'deferred/op', c.get('deferred',{}).get('per_op'), c['verdict'])" "$d/cell.json" | tee -a "$RAW/cells.txt"
}

# du_total PATH... -> the allocated bytes of PATHs together (du -sB1 -c), empty when du fails
du_total() { du -sB1 -c "$@" 2>/dev/null | awk '$2 == "total" {print $1}'; }
# parent_du_paths / store_du_paths -- comp review 1 LOW 26: the parent's own files (what server_fixture's --du names:
# PG the template p's directory, Dolt and Doltgres their whole store, which BEFORE the prebranch is the parent alone)
# and the whole store (PG the cluster's data directory), each into the array DUP
parent_du_paths() {
  local oid
  case $KIND in
    pg) oid=$(sqlq "SELECT oid FROM pg_database WHERE datname = 'p'"); DUP=("$DATA/base/$oid") ;;
    dolt) DUP=("$DATA/dbs/bench") ;;
    doltgres) DUP=("$DATA/databases") ;;
    b1) DUP=("$ROOT/parent.db") ;;
  esac
}
store_du_paths() {
  case $KIND in
    pg) DUP=("$DATA") ;;
    dolt) DUP=("$DATA/dbs/bench") ;;
    doltgres) DUP=("$DATA/databases") ;;
    b1) DUP=("$ROOT/parent.db" "$ROOT/branches") ;;
  esac
}
# parent_du_before_prebranch -> PARENT_DU0 (LOW 26), or the job fails
parent_du_before_prebranch() {
  local -a DUP
  parent_du_paths
  PARENT_DU0=$(du_total "${DUP[@]}")
  [[ $PARENT_DU0 =~ ^[1-9][0-9]*$ ]] || fail "parent du before the prebranch [$PARENT_DU0] of ${DUP[*]} is not a count"
}
# server_fixture -- RAW/fixture.json for the parent this server just seeded (gate-6 review, t3run item 4): du of the
# parent's own files, the size as the engine reports it (or why it cannot), the extent count of its data files.
server_fixture() {
  local du=() ext=() eb="" why="" maint mev
  maint=$(sed -n 's/^maintenance: //p' "$RAW/seed.txt" | tail -1)
  mev=$(sed -n 's/^evidence: //p' "$RAW/seed.txt" | tail -1)  # comp review 1 LOW 24: the maintenance's outcomes
  # LOW 18: the extent record covers every data file: PG every fork (_fsm, _vm, _init) and segment (.1, .2, ...) of t
  # and t_pkey (it had segment 0 of t's main fork only); Dolt and Doltgres every regular file under noms, recursively
  # (it missed oldgen/*.darc after the GC). LOW 26: an empty or non-numeric oid or file path fails the job (it made du
  # cover all of base/).
  local oid rel rp f
  case $KIND in
    pg)
      oid=$(sqlq "SELECT oid FROM pg_database WHERE datname = 'p'")
      [[ $oid =~ ^[0-9]+$ ]] || { fail "fixture: template p's oid [$oid] is not a number"; return; }
      du=("$DATA/base/$oid")
      eb=$(sqlq "SELECT pg_database_size('p')")
      for rel in t t_pkey; do
        rp=$(srv sql "$DATA" p "SELECT pg_relation_filepath('$rel')")
        [[ $rp =~ ^base/[0-9]+/[0-9]+$ ]] || { fail "fixture: $rel's file path [$rp] is not base/<oid>/<file>"; return; }
        for f in "$DATA/$rp" "$DATA/$rp".[0-9]* "$DATA/$rp"_*; do [ -f "$f" ] && ext+=("$f"); done
      done ;;
    dolt)
      du=("$DATA/dbs/bench")
      why="Dolt has no SQL function for a database's size; du only"
      mapfile -t ext < <(find "$DATA/dbs/bench/.dolt/noms" -type f | sort) ;;
    doltgres)
      du=("$DATA/databases")
      why="Doltgres has no working pg_database_size; du only"
      mapfile -t ext < <(find "$DATA/databases" -path '*/.dolt/noms/*' -type f | sort) ;;
  esac
  [ ${#ext[@]} -gt 0 ] || { fail "fixture: no data file found for the extent record"; return; }
  # MED 3: what the engine READS BACK from the parent (count, exact sum, order-independent row hash: gen_seed.py
  # readback-sql in its dialect), against what the generator wrote; and the digests the seed's own write path recorded.
  # Comp review 1 MED 10: fixture.py write compares both with gen_seed.facts -- the job's one generator pass beyond
  # the seed's stream -- and exits non-zero on a difference (the separate `gen_seed.py expect` pass is gone)
  local rb dial=pg
  [ "$KIND" = dolt ] && dial=mysql
  rb=$(sqlp "$(python3 -B "$HERE/gen_seed.py" readback-sql --dialect "$dial")" | tr '\t' '|' | tail -1)
  echo "$rb" >"$RAW/readback.txt"
  cp "$DATA.seed-sql.sha256" "$DATA.seed-age.sha256" "$RAW/" 2>/dev/null || fail "seed stream digests missing"
  local -a DUP
  store_du_paths  # LOW 26: the whole store at N
  if python3 "$HERE/fixture.py" write "$RAW/fixture.json" --system "$SYSTEM" --rows "$ROWS" --age "$AGE" \
    --prebranch "$PREBRANCH" --live "$(live_excl_main)" --du "${du[@]}" ${eb:+--engine-bytes "$eb"} \
    ${why:+--engine-why "$why"} --extents "${ext[@]}" --maintenance "$maint" --evidence "$mev" \
    --parent-du-bytes "${PARENT_DU0:-}" --store-du "${DUP[@]}" \
    --streams "$DATA.seed-sql.sha256" "$DATA.seed-age.sha256" --readback "$rb" >/dev/null 2>"$RAW/fixture-write.err"; then
    pass "parent read back by the engine (count|sum|row hash) and its piped streams are the generator's: $rb"
  else
    fail "fixture.json: $(tail -1 "$RAW/fixture-write.err")"
  fi
}
# jf FILE KEY -> one key of a JSON file (empty when unreadable)
jf() { python3 -B -c 'import json, sys; print(json.load(open(sys.argv[1])).get(sys.argv[2], ""))' "$1" "$2" 2>/dev/null; }
# cap_plants -- PG only, after the cells since comp review 1 LOW 25 (lead review 62430d8bf..b49fb656a MED 5): the registered cap's code path on
# the real binary, which CI otherwise never reaches. Three closed-loop runs ended by --max-window-s 1 (ops of about 0,
# 5 and 20 ms) must each exit 0 with capped: true and verdict "capped", and timedrun.py must put them in the tiers
# complete (>= 1000 ok), p50_only (100-999) and failed (< 100). Then the warm-up rule of PREREG annex A23 (the macOS
# bbload's claim_op, decided at each claim by warm_ends) on the real binary: --warmup 1000:1:60 must leave its warm-up
# done, not capped, only when BOTH 1000 ops and 1 s are reached (the OPS-and-S clause); and, since A23-AM1 gives MAX_S 0
# no "no limit" meaning, --warmup 1000:1:0 must end it at the first claim, capped, with 0 warm-up ops (this plant used
# 1000:1:0 for the OPS-and-S clause, reading MAX_S 0 as no limit).
cap_plants() {
  local sw spec want d rc
  mkdir -p "$RAW/plants"
  for sw in pg18-select1:complete pg18-sleep5:p50_only pg18-sleep20:failed; do
    spec=${sw%%:*} want=${sw#*:} d="$RAW/plants/cap-$spec" rc=0
    timeout 120 "$BB" --spec "$SPECS/$spec.spec" --out "$d" --clients 1 --max-ops 100000000 --set port="$PORT" \
      --set rows="$ROWS" --stall-s 60 --max-window-s 1 >"$d.txt" 2>&1 || rc=$?
    expect "cap plant $spec (closed loop ended by --max-window-s 1): rc|capped|verdict" \
      "$rc|$(jf "$d/summary.json" capped)|$(jf "$d/summary.json" verdict)" "0|True|capped"
    expect "cap plant $spec: tier at $(jf "$d/summary.json" measured_ok) ok ops" \
      "$(python3 -B "$HERE/timedrun.py" tier "$d/summary.json")" "$want"
  done
  d="$RAW/plants/warmup-ops-and-s" rc=0
  timeout 120 "$BB" --spec "$SPECS/pg18-select1.spec" --out "$d" --clients 1 --max-ops 100 --set port="$PORT" \
    --set rows="$ROWS" --stall-s 60 --warmup 1000:1:60 >"$d.txt" 2>&1 || rc=$?
  expect "warm-up plant --warmup 1000:1:60: rc|warmup_ops >= 1000|warmup_s >= 1|warmup_capped" \
    "$rc|$(python3 -B -c 'import json, sys; s = json.load(open(sys.argv[1])); print(s.get("warmup_ops", -1) >= 1000, s.get("warmup_s", -1) >= 1.0, s.get("warmup_capped"))' "$d/summary.json" 2>/dev/null | tr ' ' '|')" \
    "0|True|True|False"
  d="$RAW/plants/warmup-max0" rc=0
  timeout 120 "$BB" --spec "$SPECS/pg18-select1.spec" --out "$d" --clients 1 --max-ops 10 --set port="$PORT" \
    --set rows="$ROWS" --stall-s 60 --warmup 1000:1:0 >"$d.txt" 2>&1 || rc=$?
  expect "warm-up plant --warmup 1000:1:0 (A23: MAX_S 0 ends at the first claim): rc|warmup_ops|warmup_capped" \
    "$rc|$(jf "$d/summary.json" warmup_ops)|$(jf "$d/summary.json" warmup_capped)" "0|0|True"
  # A23 open loop (the macOS claim_op's one run-wide arrival stream, drawn under the claim lock): four clients at
  # 100 ops/s judge their claims in arrival order, so the record says loop open and 0 backsteps. A regression check
  # on the live binary, not a deterministic red (per-client schedules stepped back only through wake-up jitter).
  # Lateness is not this plant's question, so its VOID bound is lifted (--max-send-late-us); every other check stands.
  local open_args=(--mode open --set port="$PORT" --set rows="$ROWS" --stall-s 60 --max-send-late-us 1000000)
  d="$RAW/plants/warmup-open" rc=0
  timeout 120 "$BB" --spec "$SPECS/pg18-select1.spec" --out "$d" --clients 4 --rate 100 --duration-s 2 \
    --warmup 20:0.2:60 "${open_args[@]}" >"$d.txt" 2>&1 || rc=$?
  expect "warm-up plant open loop --warmup 20:0.2:60: rc|loop|warmup_backsteps|warmup_ops >= 20" \
    "$rc|$(jf "$d/summary.json" loop)|$(jf "$d/summary.json" warmup_backsteps)|$(python3 -B -c 'import json, sys; print(json.load(open(sys.argv[1])).get("warmup_ops", -1) >= 20)' "$d/summary.json" 2>/dev/null)" \
    "0|open|0|True"
  # Competitors review 2 item 2: the arrivals themselves, against sched_check.py's model written from the macOS text
  # (seed 1, the default, at 100/s): every warm-up and measured arrival, the recorded seed, count and schedule_hash
  expect "open-loop plant: the arrivals are the macOS claim_op's stream (sched_check.py raw)" \
    "$(python3 -B "$HERE/sched_check.py" raw "$d" 1 100 2>&1 | tail -1)" ok
  # ... and the warm-up depends on the arrivals alone: the same seed and rate at C=1 decide it identically
  local d1="$RAW/plants/warmup-open-c1"
  rc=0
  timeout 120 "$BB" --spec "$SPECS/pg18-select1.spec" --out "$d1" --clients 1 --rate 100 --duration-s 2 \
    --warmup 20:0.2:60 "${open_args[@]}" >"$d1.txt" 2>&1 || rc=$?
  expect "open-loop plant: C=1 and C=4 at one seed decide the warm-up identically (warmup_ops|end_ns|last_claim_ns)" \
    "$rc|$(jf "$d1/summary.json" warmup_ops)|$(jf "$d1/summary.json" warmup_end_ns)|$(jf "$d1/summary.json" warmup_last_claim_ns)" \
    "0|$(jf "$d/summary.json" warmup_ops)|$(jf "$d/summary.json" warmup_end_ns)|$(jf "$d/summary.json" warmup_last_claim_ns)"
  # Competitors review 2 item 1: the window is defined in intended time, decided at the claims (claim_op). A slow
  # system (5 ms ops at C=1 against 300 arrivals/s: a growing backlog) must measure exactly the arrivals a fast one
  # does, and no measured row may be intended at or after tm1. Both run 200 measured arrivals (--min-ops 200,
  # --duration-s 0) after the same warm-up.
  local wf="$RAW/plants/window-fast" ws="$RAW/plants/window-slow" rcf=0 rcs=0
  timeout 120 "$BB" --spec "$SPECS/pg18-select1.spec" --out "$wf" --clients 1 --rate 300 --seed 3 --min-ops 200 \
    --duration-s 0 --warmup 20:0.2:60 "${open_args[@]}" >"$wf.txt" 2>&1 || rcf=$?
  timeout 120 "$BB" --spec "$SPECS/pg18-sleep5.spec" --out "$ws" --clients 1 --rate 300 --seed 3 --min-ops 200 \
    --duration-s 0 --warmup 20:0.2:60 "${open_args[@]}" >"$ws.txt" 2>&1 || rcs=$?
  expect "open-loop window plant: rc fast|rc slow|the same measured arrivals (sched_check.py same)" \
    "$rcf|$rcs|$(python3 -B "$HERE/sched_check.py" same "$wf" "$ws" 2>&1 | tail -1)" "0|0|ok"
  expect "open-loop window plant: no measured row intended at or after tm1 (slow|fast)" \
    "$(python3 -B "$HERE/sched_check.py" after-tm1 "$ws" 2>&1 | tail -1)|$(python3 -B "$HERE/sched_check.py" after-tm1 "$wf" 2>&1 | tail -1)" \
    "ok|ok"
  # Comp review 1 LOW 14: a live claim path where OPS binds (C=4 clients, 5 ms ops: 300 warm-up ops take longer than
  # S 0.2 s), so a mutant dropping OPS from the live decision cannot pass; the record must pass timedrun.py's A23 check
  d="$RAW/plants/warmup-ops-binding" rc=0
  timeout 120 "$BB" --spec "$SPECS/pg18-sleep5.spec" --out "$d" --clients 4 --max-ops 50 --set port="$PORT" \
    --set rows="$ROWS" --stall-s 60 --warmup 300:0.2:60 >"$d.txt" 2>&1 || rc=$?
  expect "warm-up plant OPS-binding (C=4, 300:0.2:60): rc|warmup_ops|loop|warmup_backsteps|timedrun warm-check" \
    "$rc|$(jf "$d/summary.json" warmup_ops)|$(jf "$d/summary.json" loop)|$(jf "$d/summary.json" warmup_backsteps)|$(python3 -B "$HERE/timedrun.py" warm-check "$d" 300:0.2:60 2>&1 | tail -1)" \
    "0|300|closed|0|ok"
  # Competitors review 2 item 4: the backstep counter made to fire. bbload-plant-claim-early (build.sh, the same source
  # with -DPLANT_CLAIM_TIME_EARLY) reads the claim's time BEFORE the claim lock and sleeps 20-200 us on odd clients, the
  # defect fixed at 0138d2128; at C=8 its record must count backsteps and timedrun.py must refuse it, while the real
  # binary, same arguments, records 0 and passes.
  local bbp v
  bbp="$(dirname "$BB")/bbload-plant-claim-early"
  for v in real plant; do
    local bin=$BB
    [ $v = plant ] && bin=$bbp
    d="$RAW/plants/claim-time-$v" rc=0
    if [ ! -x "$bin" ]; then fail "claim-time plant: no $bin"; continue; fi
    timeout 120 "$bin" --spec "$SPECS/pg18-sleep5.spec" --out "$d" --clients 8 --max-ops 50 --set port="$PORT" \
      --set rows="$ROWS" --stall-s 60 --warmup 200:0.5:60 >"$d.txt" 2>&1 || rc=$?
    local bs chk
    bs=$(jf "$d/summary.json" warmup_backsteps)
    chk=$(python3 -B "$HERE/timedrun.py" warm-check "$d" 200:0.5:60 2>&1 | tail -1)
    if [ $v = real ]; then
      expect "claim-time plant, the real binary at C=8: rc|warmup_backsteps|timedrun warm-check" "$rc|$bs|$chk" "0|0|ok"
    elif [ "$rc" = 0 ] && [[ $bs =~ ^[1-9][0-9]*$ ]] && [[ $chk == REFUSED* ]]; then
      pass "claim-time plant, the early-clock mutant at C=8: $bs backsteps, timedrun refused it"
    else
      fail "claim-time plant, the early-clock mutant at C=8: rc $rc, warmup_backsteps [$bs], warm-check [$chk] (want >0 and REFUSED)"
    fi
  done
  # MED 7: --warmup with a legacy flag is refused (rc 2), and the recorded rule is the EFFECTIVE one
  d="$RAW/plants/warmup-mixed" rc=0
  timeout 120 "$BB" --spec "$SPECS/pg18-select1.spec" --out "$d" --clients 1 --max-ops 10 --set port="$PORT" \
    --warmup 1000:10:180 --warmup-ops 20 >"$d.txt" 2>&1 || rc=$?
  expect "warm-up plant --warmup 1000:10:180 --warmup-ops 20: refused (rc)" "$rc" 2
  d="$RAW/plants/warmup-legacy" rc=0
  timeout 120 "$BB" --spec "$SPECS/pg18-select1.spec" --out "$d" --clients 1 --max-ops 10 --set port="$PORT" \
    --warmup-ops 20 --warmup-s 0 >"$d.txt" 2>&1 || rc=$?
  # the legacy flags take MAX_S as 0.1 x the run's window bound, as the macOS bbload takes 0.1 x its run cap (A23-AM1:
  # MAX_S is finite); with --max-window-s at its default 3600 s that is 360 s
  expect "warm-up plant --warmup-ops 20 alone: rc|recorded warmup_rule" "$rc|$(jf "$d/summary.json" warmup_rule)" "0|20:0:360"
}
# designate SPEC -- after the cells: ONE op of SPEC (C=1, no warm-up) with its after-steps skipped, so its branch
# stays for the functional checks to read (the isolation read, the clone proof); prints the branch name. Untraced,
# untimed, outside every cell.
designate() {
  mkdir -p "$RAW/designated"
  bb_args "$1" 1 1 "$RAW/designated/$1" nowarm
  "${BBA[@]}" --skip-after >"$RAW/designated/$1.txt" 2>&1 || { tail -1 "$RAW/designated/$1.txt"; return 1; }
  python3 "$FH" branch "$RAW/designated/$1"
}
# live_excl_main -- LIVE0 without Dolt/Doltgres's main: the live branches every cell runs at, comparable across systems
live_excl_main() {
  case $LIVE0 in ''|*[!0-9]*) echo unknown; return ;; esac
  if [ "$KIND" = dolt ] || [ "$KIND" = doltgres ]; then echo $((LIVE0 - 1)); else echo "$LIVE0"; fi
}
# prebranch_server -- PREBRANCH live branches before the cells, each with one private write: the system's own M1 op
# (bbload --skip-after, so the branches stay; no warm-up; untraced, untimed). Then LIVE0 is READ (count_branches), and
# the job fails unless it is exactly PREBRANCH (+ main for Dolt/Doltgres): every cell runs at that N (lead review
# 62430d8bf..b49fb656a HIGH 1, MED 3: the requested count used to be recorded, and a C=4 run with a warm tick
# overshot it by about C). Comp review 1 LOW 21: it runs at PREBRANCH_C clients (C=1 cannot reach N = 1e5 or 1e6 within
# CAP_S) with --no-drain, so exactly --max-ops ops run (claim_op's drain ops would each keep one more), bounded by its
# own PREBRANCH_S, and its duration is recorded.
prebrun_bound() { # BBA or CBA's timeout and --max-window-s -> PREBRANCH_S (the arrays name the array to edit)
  local -n arr=$1
  local i
  arr[1]=$((PREBRANCH_S + 300))
  for i in "${!arr[@]}"; do [ "${arr[$i]}" = --max-window-s ] && arr[$((i + 1))]=$PREBRANCH_S; done
}
prebranch_server() {
  local sp want
  want=$PREBRANCH
  [ "$KIND" = pg ] || want=$((want + 1))  # Dolt/Doltgres: main is a branch too
  if [ "$PREBRANCH" -gt 0 ]; then
    case $KIND in pg) sp=pg18-m1 ;; dolt) sp=dolt-b-m1 ;; doltgres) sp=doltgres-b-m1 ;; esac
    bb_args "$sp" "$PREBRANCH_C" "$PREBRANCH" "$RAW/prebranch" nowarm  # LOW 21: at C>1, exactly PREBRANCH
    prebrun_bound BBA
    "${BBA[@]}" --skip-after --no-drain >"$RAW/prebranch.txt" 2>&1 || fail "prebranch: $sp x $PREBRANCH ($(tail -1 "$RAW/prebranch.txt"))"
    if [ "$KIND" = pg ]; then sqlq "CHECKPOINT" >/dev/null 2>&1 || fail "prebranch CHECKPOINT"; fi
    fun "prebranch: $PREBRANCH branches at C=$PREBRANCH_C in $(jf "$RAW/prebranch/summary.json" window_s) s (bound ${PREBRANCH_S} s, capped $(jf "$RAW/prebranch/summary.json" capped))"
  fi
  LIVE0=$(count_branches)
  expect "live branches before the cells (LIVE0, read back)" "$LIVE0" "$want"
}

server_main() {
  local spec c rc=0
  # comp review 1 LOW 33: a failed init stops the job as an init failure (pg18.sh's shared_buffers refusal, LOW 22,
  # used to surface as a misleading start failure)
  case $KIND in
    pg) srv init "$DATA" "$MODE" "$PORT" >"$RAW/server-init.txt" 2>&1 || rc=$? ;;
    *) srv init "$DATA" "$PORT" >"$RAW/server-init.txt" 2>&1 || rc=$? ;;
  esac
  cat "$RAW/server-init.txt"
  [ "$rc" = 0 ] || { fail "server init (rc $rc): $(tail -1 "$RAW/server-init.txt")"; return; }
  if [ "$KIND" = pg ]; then
    # What Linux lacks: wal_sync_method=fsync_writethrough (macOS's F_FULLFSYNC; also Windows). Measured, not assumed:
    # this postgres binary is asked to accept it and must refuse (on Linux fsync/fdatasync themselves send the device
    # cache flush, so D2 here is wal_sync_method=fdatasync, which is also PG's Linux default).
    srv writethrough "$DATA" >"$RAW/fsync_writethrough-probe.txt" 2>&1
    if grep -q '^rc=0$' "$RAW/fsync_writethrough-probe.txt" ||
      ! grep -q 'invalid value for parameter "wal_sync_method": "fsync_writethrough"' "$RAW/fsync_writethrough-probe.txt"; then
      fail "Linux PG refuses wal_sync_method=fsync_writethrough: $(tr '\n' ' ' <"$RAW/fsync_writethrough-probe.txt" | cut -c1-300)"
    else
      pass "Linux PG refuses wal_sync_method=fsync_writethrough ($(grep -m1 'invalid value' "$RAW/fsync_writethrough-probe.txt" | cut -c1-160); $(tail -1 "$RAW/fsync_writethrough-probe.txt"))"
    fi
  fi
  # LOW 19: the server binary is the registered one (versions.tsv binary_sha256 for this arch), checked before it starts
  local bin pin_sys pin_arch
  case $KIND in pg) bin="$FT_PG18/postgres" pin_sys=postgresql ;; dolt) bin=$FT_DOLT pin_sys=dolt ;;
    doltgres) bin=$FT_DOLTGRES pin_sys=doltgres ;; esac
  case $(uname -m) in x86_64) pin_arch=amd64 ;; aarch64) pin_arch=arm64 ;; *) pin_arch=$(uname -m) ;; esac
  python3 -B "$HERE/pins.py" check-binary "$pin_sys" "$pin_arch" "$bin" >"$RAW/binary-check.txt" 2>&1 ||
    { fail "server binary: $(cat "$RAW/binary-check.txt")"; return; }
  pass "server binary: $(cat "$RAW/binary-check.txt")"
  srv start "$DATA" 2>&1 | tee "$RAW/server-start.txt" || { fail "server start: $(tail -1 "$RAW/server-start.txt")"; return; }
  srv seed "$DATA" "$ROWS" "$AGE" | tee "$RAW/seed.txt" || { cp "$DATA".gc.* "$RAW/" 2>/dev/null; fail "seed"; return; }
  # the GC's own record, banked with the job (MED 9: gc.txt was never banked): client output, log slice, verdict
  for f in "$DATA".gc.txt "$DATA".gc.log "$DATA".gc.verdict "$DATA".gc-settings.txt "$DATA".gc-settings.verdict; do
    [ -e "$f" ] && cp "$f" "$RAW/seed-$(basename "$f")"
  done
  # comp review 1 LOW 28: Dolt's and Doltgres's GC settings record must say what GC the server ran with
  if [ "$KIND" = dolt ] || [ "$KIND" = doltgres ]; then
    expect "seed GC settings record (fthelp.py gcsettings)" "$(head -1 "$DATA.gc-settings.verdict" 2>/dev/null)" ok
  fi
  parent_du_before_prebranch  # LOW 26: the parent's du, comparable across systems, before any branch exists
  prebranch_server
  server_fixture  # after prebranch: it records the live-branch count read back there (HIGH 1, MED 3)
  if [ "$KIND" = pg ]; then
    srv settings "$DATA" >"$RAW/pg_settings.tsv" || fail "pg_settings dump"
    expect "server wal_sync_method" "$(awk -F'\t' '$1 == "wal_sync_method" {print $2}' "$RAW/pg_settings.tsv")" fdatasync
    expect "server file_copy_method" "$(awk -F'\t' '$1 == "file_copy_method" {print $2}' "$RAW/pg_settings.tsv")" clone
    # comp review 1 LOW 36: all five D2 settings (fsync, synchronous_commit and full_page_writes too), read back
    python3 -B "$HERE/pins.py" check-d2 "$RAW/pg_settings.tsv" >"$RAW/d2-check.txt" 2>&1 &&
      pass "server D2 settings: $(cat "$RAW/d2-check.txt")" || fail "server D2 settings: $(cat "$RAW/d2-check.txt")"
    # shared_buffers = 25% of MemTotal (gate-6 review, t3run item 15), recorded in pg_settings.tsv and meminfo.txt --
    # the meminfo pg18.sh init computed it from (LOW 22: a second read of /proc/meminfo could differ), banked
    cp "$DATA.meminfo" "$RAW/meminfo.txt" 2>/dev/null || fail "server shared_buffers: init's meminfo copy is missing"
    if python3 -B "$HERE/pins.py" check-pg "$RAW/pg_settings.tsv" "$RAW/meminfo.txt" >"$RAW/shared_buffers-check.txt" 2>&1; then
      pass "server shared_buffers = 25% of MemTotal ($(awk -F'\t' '$1 == "shared_buffers" {print $2}' "$RAW/pg_settings.tsv") x 8 kB)"
    else
      fail "server shared_buffers: $(cat "$RAW/shared_buffers-check.txt")"
    fi
  fi
  if [ "$KIND" = pg ]; then  # the seed's own checkpoint; a failure FAILS the job (second review, finding 9)
    sqlq "CHECKPOINT" >"$RAW/seed-checkpoint.txt" 2>&1 || fail "pre-cell CHECKPOINT rc=$? ($(tail -1 "$RAW/seed-checkpoint.txt"))"
  fi
  [ "$KIND" = pg ] || dolt_quiet_root "$ROOT/vroot"  # the version commands below run with metrics/version check off too
  case $KIND in
    pg) { "$FT_PG18/postgres" --version; sha256sum "$FT_PG18/postgres"; dpkg-query -W 'postgresql-18*' 'libpq5' 2>/dev/null; } >"$RAW/version.txt" ;;
    dolt) { "$FT_DOLT" version; sha256sum "$FT_DOLT"; } >"$RAW/version.txt" 2>&1 ;;
    doltgres) { (cd "$ROOT" && "$FT_DOLTGRES" -version); sha256sum "$FT_DOLTGRES"; } >"$RAW/version.txt" 2>&1 ;;
  esac
  # The server is the registered version of versions.tsv, or the job fails (gate-6 review, t3run item 13); PG's
  # installed package must also be the registered one.
  local vsys
  case $KIND in pg) vsys=postgresql ;; *) vsys=$KIND ;; esac
  head -1 "$RAW/version.txt" >"$RAW/version-line.txt"
  python3 -B "$HERE/pins.py" check-version "$vsys" "$RAW/version-line.txt" >"$RAW/version-check.txt" 2>&1 &&
    pass "server version: $(cat "$RAW/version-check.txt")" || fail "server version: $(cat "$RAW/version-check.txt")"
  if [ "$KIND" = pg ]; then
    expect "PGDG postgresql-18 package" "$(awk '$1 == "postgresql-18" {print $2}' "$RAW/version.txt")" \
      "$(python3 -B "$HERE/pins.py" get postgresql any pgdg_package)"
  fi
  no_plant_before_cells
  local t0c
  for spec in $SPECLIST; do
    for c in $CLIENTS; do
      t0c=$SECONDS
      run_server_cell "$spec" "$c"
      cell_done "$t0c"  # LOW 32: the measured reserve
    done
  done
  # comp review 1 LOW 25: PG's cap, warm-up and claim-time plants run AFTER the measured cells, so none perturbs them
  if [ "$KIND" = pg ]; then cap_plants; fi

  fun "## functional checks ($SYSTEM on $(findmnt -n -o FSTYPE -T "$MNT"))"
  # Every cell deleted what it created (HIGH 1), so the count is still LIVE0 here; the designated branches below are
  # each ONE create with no delete (bbload --skip-after), and the count must rise by exactly their number: the counter
  # that holds N fixed is shown to see a create that was not deleted.
  expect "branch count after every cell (every create deleted: LIVE0)" "$(count_branches)" "$LIVE0"
  local br spec
  NDES=0  # designated branches made (each one create, no delete)
  # The clone proof first: a later read of the template could dirty a page whose write-back un-shares its extent.
  if [ "$KIND" = pg ]; then pg_clone_proof; fi
  expect "isolation: parent/main count|sum(v)" "$(sqlp "$COUNTSUM")" "$ROWS|$PSUM"
  local nm1=0
  for spec in $SPECLIST; do  # every M1 variant: one designated branch, one UPDATE, sum PSUM + 1
    case $spec in *-m1|*-m1-wal) ;; *) continue ;; esac
    nm1=$((nm1 + 1))
    br=$(designate "$spec") || { fail "isolation $spec: no designated branch ($br)"; continue; }
    NDES=$((NDES + 1))
    expect "isolation: $spec designated branch $br count|sum(v) after one UPDATE" \
      "$(on_branch "$br" "$COUNTSUM" | tr '\t' '|')" "$ROWS|$((PSUM + 1))"
  done
  [ $nm1 -gt 0 ] || fail "isolation: no M1 spec to make a designated branch from"
  expect "branch count after $NDES designated create(s) with no delete (the N counter sees a kept create)" \
    "$(count_branches)" "$((LIVE0 + NDES))"
  srv stop "$DATA" | tee -a "$RAW/server-stop.txt" || fail "server stop by recorded pid"
  cp "$DATA.log" "$RAW/server_log.txt" 2>/dev/null
}

pg_clone_proof() {
  # The template's table file vs. a FILE_COPY branch's: same physical blocks (and flagged shared) under
  # file_copy_method=clone; disjoint under copy. The strace half: copy_file_range calls in the create window. The
  # server runs clone (pg18-d2); pg18-create-copy is the negative control, its session
  # SET to copy: the same two instruments must read "copy" and 0 there, or the proof could not tell them apart.
  # Every cell's branches are deleted (HIGH 1), so the filefrag half reads a DESIGNATED branch of the same spec, made
  # after the cells (one create, kept); the strace half still reads the cell's own C=1 window.
  local cell want br tfile bfile cfr ncell=0
  tfile="$DATA/$(srv sql "$DATA" p "SELECT pg_relation_filepath('t')")"
  for cell in pg18-create-c1 pg18-create-copy-c1; do
    [ -d "$RAW/cells/$cell/bb" ] || continue
    ncell=$((ncell + 1))
    case $cell in *copy*) want=copy ;; *) want=clone ;; esac
    br=$(designate "${cell%-c1}") || { fail "clone proof $cell: no designated branch ($br)"; continue; }
    NDES=$((NDES + 1))
    bfile="$DATA/$(srv sql "$DATA" "$br" "SELECT pg_relation_filepath('t')")"
    sync -f "$MNT"
    python3 "$FH" cloneproof "$tfile" "$bfile" >"$RAW/cloneproof-$cell.json"
    expect "clone proof $cell (filefrag, template t vs $br t): verdict" \
      "$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['verdict'])" "$RAW/cloneproof-$cell.json")" "$want"
    cfr=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['copy_file_range_calls'])" "$RAW/cells/$cell/load.json")
    if [ "$want" = clone ]; then
      [ "$cfr" -gt 0 ] && pass "clone proof $cell (strace): copy_file_range calls in the C=1 window = $cfr" ||
        fail "clone proof $cell (strace): no copy_file_range in the C=1 window"
    else
      expect "copy control $cell (strace): copy_file_range calls in the C=1 window" "$cfr" 0
    fi
    [ "$(findmnt -n -o FSTYPE -T "$MNT")" = btrfs ] && btrfs filesystem du -s "$tfile" "$bfile" >"$RAW/cloneproof-$cell-btrfs-du.txt" 2>&1
  done
  [ $ncell -gt 0 ] || fail "clone proof: no FILE_COPY create cell ran"
  echo "template_bytes=$(srv sql "$DATA" p "SELECT pg_database_size('p')")" >"$RAW/template-size.txt"
}

# ---------------------------------------------------------------- B1 (embedded)
# designate_b1 SPEC -- after the cells: ONE clonebench op of SPEC (C=1, no warm-up, no --drop) into its own directory,
# kept for the functional checks; prints the branch file's path.
designate_b1() {
  local op=${1%-*} sync=${1#*-} dir="$ROOT/branches/designated-$1"
  mkdir -p "$dir"
  timeout "$OUTER_S" "$CB" run --mode b1 --op "$op" --sync "$sync" --parent "$ROOT/parent.db" --dir "$dir" \
    --clients 1 --max-ops 1 --rows "$ROWS" --out "$RAW/designated-$1" >"$RAW/designated-$1.txt" 2>&1 ||
    { tail -1 "$RAW/designated-$1.txt"; return 1; }
  local f
  f=$(find "$dir" -maxdepth 1 -name 'b_*.db')
  [ "$(printf '%s\n' "$f" | awk 'NF {n++} END {print n + 0}')" = 1 ] || { echo "not one branch file in $dir: [$f]"; return 1; }
  echo "$f"
}
# b1_plants -- B1's test plants on the real binaries (comp review 1 LOW 25: run after the cells): the stream
# plant and its held-copy control (MED 12, comp review 1 MED 11), clonebench's capped path (MED 5), the A23
# warm-up and claim-time plants, and the delete-durability plants (LOW 22)
b1_plants() {
  # MED 12 plant: a 1e6-row parent (about 114 MB of SQL) built under a virtual-memory limit must succeed, since the
  # stream is piped and never held (the old feed held it about three times over). Comp review 1 MED 11: the limit is
  # 120000 KB, about the SQL itself, so it refuses even ONE held copy (the control below); at 300000 KB it passed one
  # held copy and probably two. The streaming feed's own need is the interpreter and one 1000-row batch line (about
  # 114 KB), and sqlite3's page cache in its own process -- each process gets the limit (RLIMIT_AS) by itself
  local mp="$ROOT/plant-1e6.noindex.db" mrc=0 mem_kb=120000
  ( ulimit -v "$mem_kb"; exec python3 -B "$HERE/fixture.py" sqlite "$mp" --rows 1000000 --age 0 --sqlite3 "$SQ3" ) \
    >"$RAW/plant-stream-1e6.txt" 2>&1 || mrc=$?
  expect "stream plant: a 1e6-row SQLite parent under ulimit -v $mem_kb (rc)" "$mrc" 0
  rm -f "$mp" "$mp-wal" "$mp-shm"
  # Comp review 1 MED 11: the limit must refuse a feed that holds the SQL just ONCE, or the plant above cannot tell a
  # streamed feed from a held one. The mutant builds the same 1e6-row stream into one bytearray (one copy, no list of
  # lines beside it) before writing it out; under the same limit it must die of MemoryError (the reason is checked,
  # so a feed that fails for another cause is not counted as the limit firing)
  local hrc=0
  ( ulimit -v "$mem_kb"; exec python3 -B -c 'import sys
sys.path.insert(0, sys.argv[1])
import gen_seed
held = bytearray()
for ln in gen_seed.sql_lines(int(sys.argv[2])):
    held += (ln + "\n").encode()
sys.stdout.buffer.write(held)' "$HERE" 1000000 ) >/dev/null 2>"$RAW/plant-held-1e6.err" || hrc=$?
  if [ "$hrc" != 0 ] && grep -q MemoryError "$RAW/plant-held-1e6.err"; then
    pass "stream plant control: one held copy of the 1e6-row SQL under ulimit -v $mem_kb dies of MemoryError (rc $hrc)"
  else
    fail "stream plant control: one held copy of the 1e6-row SQL under ulimit -v $mem_kb: rc $hrc, $(tail -1 "$RAW/plant-held-1e6.err") (want MemoryError: the limit cannot tell a held feed from a streamed one)"
  fi
  # MED 5: clonebench's capped path on the real binary (a d0 closed loop ended by --max-window-s 1, every branch
  # dropped): rc 0, capped: true, verdict "capped"; the tiers themselves are timedrun.py's (selftest; PG cap plants)
  local cpd="$RAW/plants/cap-b1" rcp=0
  mkdir -p "$RAW/plants" "$ROOT/branches/cap-plant"
  timeout 120 "$CB" run --mode b1 --op m1c --sync d0 --parent "$ROOT/parent.db" --dir "$ROOT/branches/cap-plant" \
    --clients 1 --max-ops 100000000 --rows "$ROWS" --max-window-s 1 --drop --out "$cpd" >"$cpd.txt" 2>&1 || rcp=$?
  expect "cap plant b1 (clonebench d0 ended by --max-window-s 1): rc|capped|verdict" \
    "$rcp|$(jf "$cpd/summary.json" capped)|$(jf "$cpd/summary.json" verdict)" "0|True|capped"
  expect "cap plant b1: its branches dropped (LIVE0 unchanged)" "$(count_branches)" "$LIVE0"
  # PREREG annex A23 on clonebench's live claim path (d0, every branch dropped): --warmup 50:0.2:60 ends done, not
  # capped, with >= 50 ops and >= 0.2 s; --warmup 1000:1:0 ends at the first claim, capped, with 0 warm-up ops
  local wpd="$RAW/plants/warmup-b1" wpz="$RAW/plants/warmup-b1-max0" rcw=0 rcz=0
  mkdir -p "$ROOT/branches/warmup-plant"
  timeout 120 "$CB" run --mode b1 --op m1c --sync d0 --parent "$ROOT/parent.db" --dir "$ROOT/branches/warmup-plant" \
    --clients 1 --max-ops 5 --rows "$ROWS" --warmup 50:0.2:60 --drop --out "$wpd" >"$wpd.txt" 2>&1 || rcw=$?
  expect "warm-up plant b1 --warmup 50:0.2:60: rc|warmup_ops >= 50|warmup_s >= 0.2|warmup_capped" \
    "$rcw|$(python3 -B -c 'import json, sys; s = json.load(open(sys.argv[1])); print(s.get("warmup_ops", -1) >= 50, s.get("warmup_s", -1) >= 0.2, s.get("warmup_capped"))' "$wpd/summary.json" 2>/dev/null | tr ' ' '|')" \
    "0|True|True|False"
  timeout 120 "$CB" run --mode b1 --op m1c --sync d0 --parent "$ROOT/parent.db" --dir "$ROOT/branches/warmup-plant" \
    --clients 1 --max-ops 5 --rows "$ROWS" --warmup 1000:1:0 --drop --out "$wpz" >"$wpz.txt" 2>&1 || rcz=$?
  expect "warm-up plant b1 --warmup 1000:1:0 (A23: MAX_S 0 ends at the first claim): rc|warmup_ops|warmup_capped" \
    "$rcz|$(jf "$wpz/summary.json" warmup_ops)|$(jf "$wpz/summary.json" warmup_capped)" "0|0|True"
  # Comp review 1 LOW 14: clonebench's live claim path where OPS binds (C=4, S 0: only OPS can end it done), and the
  # record through timedrun.py's A23 check; review 2 item 4's twin for clonebench: C=4 records loop closed, 0 backsteps
  local wpo="$RAW/plants/warmup-b1-ops" rco=0
  timeout 120 "$CB" run --mode b1 --op m1c --sync d0 --parent "$ROOT/parent.db" --dir "$ROOT/branches/warmup-plant" \
    --clients 4 --max-ops 20 --rows "$ROWS" --warmup 300:0:60 --drop --out "$wpo" >"$wpo.txt" 2>&1 || rco=$?
  expect "warm-up plant b1 OPS-binding (C=4, 300:0:60): rc|warmup_ops|loop|warmup_backsteps|timedrun warm-check" \
    "$rco|$(jf "$wpo/summary.json" warmup_ops)|$(jf "$wpo/summary.json" loop)|$(jf "$wpo/summary.json" warmup_backsteps)|$(python3 -B "$HERE/timedrun.py" warm-check "$wpo" 300:0:60 2>&1 | tail -1)" \
    "0|300|closed|0|ok"
  # Competitors review 2 item 4, clonebench's half: clonebench-plant-claim-early (build.sh; the time read before the
  # claim lock, 20-200 us sleep on odd clients) at C=8 must count backsteps and be refused by warm-check, while the
  # real binary, same arguments (d0 clones of about 100 us), records 0 and passes
  local cbp vv wpc rcc bsc chkc
  cbp="$(dirname "$CB")/clonebench-plant-claim-early"
  for vv in real plant; do
    local bin=$CB
    [ $vv = plant ] && bin=$cbp
    wpc="$RAW/plants/claim-time-b1-$vv" rcc=0
    if [ ! -x "$bin" ]; then fail "claim-time plant b1: no $bin"; continue; fi
    timeout 120 "$bin" run --mode b1 --op m1c --sync d0 --parent "$ROOT/parent.db" --dir "$ROOT/branches/warmup-plant" \
      --clients 8 --max-ops 20 --rows "$ROWS" --warmup 200:0:60 --drop --out "$wpc" >"$wpc.txt" 2>&1 || rcc=$?
    bsc=$(jf "$wpc/summary.json" warmup_backsteps)
    chkc=$(python3 -B "$HERE/timedrun.py" warm-check "$wpc" 200:0:60 2>&1 | tail -1)
    if [ $vv = real ]; then
      expect "claim-time plant b1, the real binary at C=8: rc|warmup_backsteps|timedrun warm-check" "$rcc|$bsc|$chkc" "0|0|ok"
    elif [ "$rcc" = 0 ] && [[ $bsc =~ ^[1-9][0-9]*$ ]] && [[ $chkc == REFUSED* ]]; then
      pass "claim-time plant b1, the early-clock mutant at C=8: $bsc backsteps, timedrun refused it"
    else
      fail "claim-time plant b1, the early-clock mutant at C=8: rc $rcc, warmup_backsteps [$bsc], warm-check [$chkc] (want >0 and REFUSED)"
    fi
  done
  expect "warm-up plants b1: their branches dropped (LIVE0 unchanged)" "$(count_branches)" "$LIVE0"
  # Comp review 1 LOW 22: the delete's durability is checked, at C=1 where the flushes split by op phase -- a D2
  # delete (clonebench --drop under --sync d2: the unlinks and an fsync of the branch directory) flushes at least once
  # per cycle, a D0 one never. Each sync's traced run is judged as itself (ok) and as the other (refused).
  local dsync dpd dv
  for dsync in d2 d0; do
    dpd="$RAW/plants/delete-$dsync"
    mkdir -p "$dpd"
    strace_run "$dpd/load" timeout 120 "$CB" run --mode b1 --op m1c --sync "$dsync" --parent "$ROOT/parent.db" \
      --dir "$ROOT/branches/prebranch" --clients 1 --max-ops 20 --rows "$ROWS" --warmup 0:0:0 --max-window-s 60 \
      --drop --out "$dpd/bb" >"$dpd/bb.txt" 2>&1 || fail "delete plant $dsync: clonebench rc ($(tail -1 "$dpd/bb.txt"))"
    CPHASES="$dpd/bb/raw.tsv" count "$dpd/load"
    for dv in d2 d0; do
      python3 "$SC" cell --name "plant/delete-$dsync-as-$dv" --load "$dpd/load.json" --idle none --load-s 1 --idle-s 0 \
        --ops 20 --ops-ok 20 --delete-durable "$dv" >"$dpd/cell-as-$dv.json" 2>&1
      if [ "$dsync" = "$dv" ]; then
        expect "delete plant: a $dsync delete judged $dv (verdict)" "$(jf "$dpd/cell-as-$dv.json" verdict)" ok
      else
        case $(jf "$dpd/cell-as-$dv.json" verdict) in
          "NOT CLEAN"*delete*) pass "delete plant: a $dsync delete judged $dv is refused" ;;
          *) fail "delete plant: a $dsync delete judged $dv: verdict [$(jf "$dpd/cell-as-$dv.json" verdict)] (want NOT CLEAN on its delete)" ;;
        esac
      fi
    done
  done
  expect "delete plants b1: their branches dropped (LIVE0 unchanged)" "$(count_branches)" "$LIVE0"
}
b1_main() {
  local cell spec op sync c n d rc total ok created bdir
  DATA="$ROOT"  # stracecount classes are relative to ROOT: parent.db, branches/prebranch/... (the cells', LOW 20)
  mkdir -p "$ROOT/branches"
  # The parent from gen_seed.py like every other system (gate-6 review, t3run item 4: clonebench mkparent wrote its own
  # table and pad), through the pinned sqlite3: WAL, the SQL, the aging, a TRUNCATE checkpoint.
  python3 "$HERE/fixture.py" sqlite "$ROOT/parent.db" --rows "$ROWS" --age "$AGE" --sqlite3 "$SQ3" --digest-dir "$RAW" |
    tee "$RAW/mkparent.json" || { fail "parent (fixture.py sqlite)"; return; }
  # MED 3: the parent as SQLite reads it back (count, sum, row hash), which fixture.py write below compares with what
  # the generator wrote (gen_seed.facts; comp review 1 MED 10: no separate `gen_seed.py expect` pass)
  local rb
  rb=$(python3 -B "$HERE/gen_seed.py" readback-sqlite "$ROOT/parent.db")
  echo "$rb" >"$RAW/readback.txt"
  # PREBRANCH live branches first, each with one private write (untraced, untimed; C=1 and no warm-up, so exactly
  # PREBRANCH ops; bounded by the outer timeout and the cap like every run), kept: no --drop. LIVE0 is then READ (the
  # branch files under branches/) and must be PREBRANCH (HIGH 1, MED 3).
  parent_du_before_prebranch  # LOW 26
  if [ "$PREBRANCH" -gt 0 ]; then
    mkdir -p "$ROOT/branches/prebranch"
    # LOW 21: at PREBRANCH_C clients with --no-drain (exactly PREBRANCH ops), bounded by PREBRANCH_S, duration recorded
    timeout "$((PREBRANCH_S + 300))" "$CB" run --mode b1 --op m1 --sync d2 --parent "$ROOT/parent.db" \
      --dir "$ROOT/branches/prebranch" --clients "$PREBRANCH_C" --max-ops "$PREBRANCH" --rows "$ROWS" \
      --max-window-s "$PREBRANCH_S" --no-drain --out "$RAW/prebranch" \
      >"$RAW/prebranch.txt" 2>&1 || fail "prebranch ($(tail -1 "$RAW/prebranch.txt"))"
    fun "prebranch: $PREBRANCH branches at C=$PREBRANCH_C in $(jf "$RAW/prebranch/summary.json" window_s) s (bound ${PREBRANCH_S} s, capped $(jf "$RAW/prebranch/summary.json" capped))"
  fi
  LIVE0=$(count_branches)
  expect "live branches before the cells (LIVE0, branch files read back)" "$LIVE0" "$PREBRANCH"
  local -a DUP
  store_du_paths  # LOW 26: the whole store at N (the parent and the branch files)
  if python3 "$HERE/fixture.py" write "$RAW/fixture.json" --system "$SYSTEM" --rows "$ROWS" --age "$AGE" \
    --prebranch "$PREBRANCH" --live "$(live_excl_main)" --du "$ROOT/parent.db" \
    --parent-du-bytes "${PARENT_DU0:-}" --store-du "${DUP[@]}" \
    --engine-bytes "$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['engine_bytes'])" "$RAW/mkparent.json")" \
    --extents "$ROOT/parent.db" --maintenance "$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['maintenance'])" "$RAW/mkparent.json")" \
    --evidence "$(jf "$RAW/mkparent.json" evidence)" \
    --streams "$RAW/seed-sql.sha256" "$RAW/seed-age.sha256" --readback "$rb" >/dev/null 2>"$RAW/fixture-write.err"; then
    pass "parent read back from the SQLite file (count|sum|row hash) and its fed streams are the generator's: $rb"
  else
    fail "fixture.json: $(tail -1 "$RAW/fixture-write.err")"
  fi
  { "$SQ3" --version; sha256sum "$CB" "$SQ3"; } >"$RAW/version.txt"
  no_plant_before_cells
  for spec in $SPECLIST; do
    op=${spec%-*} sync=${spec#*-}
    for c in $CLIENTS; do
      n=$(nops "$c")
      d="$RAW/cells/b1-$spec-c$c"
      over_budget "$d" "b1-$spec-c$c" && continue  # LOW 14
      local t0c=$SECONDS  # LOW 32: the measured reserve
      # LOW 20: both runs create in the prebranch directory, among its PREBRANCH live branches (clonebench names each
      # branch b_<run tag>_<client>_<seq>.db with a per-run tag, so a cell's names never meet the prebranch's)
      bdir="$ROOT/branches/prebranch"
      mkdir -p "$d" "$bdir"
      echo "=== b1 $spec C=$c N=$n"
      rc=0
      # --drop: every op's branch is deleted, durably and untimed (HIGH 1), so N stays LIVE0; the labelling run is
      # split by op phase at C=1 like the servers' (lead ruling on HIGH 1's flush attribution)
      # comp review 1 LOW 20: B1's N is a directory's population as well as a count, so its creates must meet the
      # PREBRANCH live branches the record says they run at (a fresh directory measured the directory at N=0)
      expect "b1-$spec-c$c creates where the $PREBRANCH live branches are (branch files in its directory)" \
        "$(find "$bdir" -maxdepth 1 -name 'b_*.db' | wc -l | tr -d ' ')" "$PREBRANCH"
      live_mark "$d" label_before
      cb_args "$op" "$sync" "$bdir" "$c" "$n" "$d/bb"
      strace_run "$d/load" "${CBA[@]}" >"$d/bb.txt" 2>&1 || rc=$?
      live_mark "$d" label_after
      cat "$d/bb.txt"
      [ $rc -eq 0 ] || fail "b1-$spec-c$c clonebench rc=$rc ($(tail -1 "$d/bb.txt"); stderr: $(tail -1 "$d/load.cmd.err" 2>/dev/null))"
      local cph=""
      [ "$c" = 1 ] && cph="$d/bb/raw.tsv"
      CPHASES=$cph count "$d/load"
      ops_of "$d/bb" "$d"
      read -r total ok created <"$d/ops.txt"
      python3 "$SC" cell --name "$SYSTEM/b1-$spec-c$c" --load "$d/load.json" --idle none \
        --load-s "$(window_s "$d/load")" --idle-s 0 --ops "$total" --ops-ok "$ok" --delete-durable "$sync" \
        >"$d/cell.json"  # LOW 22: --drop's delete is durable under d2 and takes no flush under d0
      judge_cell "$d"
      python3 -c "import json,sys; c=json.load(open(sys.argv[1])); p=c.get('per_op',{}); print('cell', c['name'], 'ops', c['ops'], 'flushes/cycle', p.get('flushes'), 'create/op(C=1)', p.get('create'), 'by_class', c.get('load_by_class'), c['verdict'])" "$d/cell.json" | tee -a "$RAW/cells.txt"
      expect "b1-$spec-c$c branch files after the labelling run (every created branch deleted: the $PREBRANCH remain)" \
        "$(find "$bdir" -maxdepth 1 -name 'b_*.db' | wc -l | tr -d ' ')" "$PREBRANCH"
      # The TIMED run: the identical clonebench command, untraced (gate-6 review, t3run item 2), in the same directory
      # (LOW 20): the only latency file of the cell is timed/raw.tsv.
      expect "b1-$spec-c$c timed run creates where the $PREBRANCH live branches are (LOW 20)" \
        "$(find "$bdir" -maxdepth 1 -name 'b_*.db' | wc -l | tr -d ' ')" "$PREBRANCH"
      live_mark "$d" timed_before
      cb_args "$op" "$sync" "$bdir" "$c" "$n" "$d/timed"
      timed_run "$d/timed" "" -- "${CBA[@]}" >/dev/null || true
      live_mark "$d" timed_after
      timedrun_check "$d" "$n" "b1-$spec-c$c"
      expect "b1-$spec-c$c branch files after the timed run (every created branch deleted: the $PREBRANCH remain)" \
        "$(find "$bdir" -maxdepth 1 -name 'b_*.db' | wc -l | tr -d ' ')" "$PREBRANCH"
      cell_done "$t0c"
    done
  done
  # comp review 1 LOW 25: the test plants run AFTER the measured cells, so none perturbs them
  b1_plants
  fun "## functional checks (b1 on $(findmnt -n -o FSTYPE -T "$MNT"))"
  expect "branch files after every cell (every create deleted: LIVE0)" "$(count_branches)" "$LIVE0"
  expect "isolation: parent count|sum(v)" "$("$SQ3" "$ROOT/parent.db" "SELECT count(*), sum(v) FROM t")" "$ROWS|$PSUM"
  local f
  NDES=0
  # Every cell deleted its branches (HIGH 1): the checks read DESIGNATED branches, one create each with no --drop,
  # made after the cells; the branch-file count must rise by exactly their number.
  for spec in m1-d2 m1-d0; do
    f=$(designate_b1 "$spec") || { fail "isolation $spec: no designated branch ($f)"; continue; }
    NDES=$((NDES + 1))
    expect "isolation: $spec designated branch $(basename "$f") integrity|count|sum(v)" \
      "$("$SQ3" "$f" "PRAGMA integrity_check; SELECT count(*), sum(v) FROM t;" | tr '\n' '|' | sed 's/|$//')" "ok|$ROWS|$((PSUM + 1))"
  done
  if f=$(designate_b1 m1c-d2); then
    NDES=$((NDES + 1))
    expect "isolation: m1c-d2 branch $(basename "$f") count|sum(v)" "$("$SQ3" "$f" "SELECT count(*), sum(v) FROM t")" "$ROWS|$PSUM"
    sync -f "$MNT"
    python3 "$FH" cloneproof "$ROOT/parent.db" "$f" >"$RAW/cloneproof-m1c.json"
    expect "clone proof (filefrag, parent vs $(basename "$f")): verdict" \
      "$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['verdict'])" "$RAW/cloneproof-m1c.json")" clone
    "$CB" extents "$f" >"$RAW/extents-m1c-branch.json" 2>&1
    "$CB" extents "$ROOT/parent.db" >"$RAW/extents-parent.json" 2>&1
  else
    fail "clone proof: no designated m1c-d2 branch ($f)"
  fi
  expect "branch files after $NDES designated create(s) with no delete (the N counter sees a kept create)" \
    "$(count_branches)" "$((LIVE0 + NDES))"
  local fic created1
  fic=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['ficlone'])" "$RAW/cells/b1-m1c-d2-c1/load.json" 2>/dev/null)
  created1=$(cut -d' ' -f3 "$RAW/cells/b1-m1c-d2-c1/ops.txt" 2>/dev/null)
  # Both sides must exist and be positive: two empty strings used to compare equal and pass (second review, finding
  # 8, e.g. FT_CLIENTS without 1). FICLONE counts only calls that returned 0.
  if [ -n "$fic" ] && [ -n "$created1" ] && [ "$created1" -gt 0 ] 2>/dev/null; then
    expect "clone proof (strace): successful FICLONE calls in the m1c-d2 C=1 window = creates" "$fic" "$created1"
  else
    fail "clone proof (strace): no m1c-d2 C=1 window to read (ficlone=[$fic], created=[$created1])"
  fi
}

if [ "$KIND" = b1 ]; then b1_main; else server_main; fi
python3 "$SC" table "$RAW/cells" >"$RAW/flushes.tsv" || true
cat "$RAW/flushes.tsv"
if [ $nfail -eq 0 ]; then fun "VERDICT PASS ($SYSTEM)"; exit 0; fi
fun "VERDICT FAIL ($SYSTEM): $nfail check(s) failed"
exit 1
