#!/usr/bin/env bash
# run_system.sh SYSTEM MNT RAW -- one competitor on one filesystem (lane fastest-linux-comp). SMOKE ONLY: no
# latency here is credited or quoted before the lead registers the PREREG.
#
#   SYSTEM  pg18-d2 | pg18-default | dolt | doltgres | b1
#   MNT     the filesystem under test (fs/mkloop.sh made it); everything this run writes lives in MNT/SYSTEM.noindex
#   RAW     the output tree (uploaded as the job's artifact)
#
# For each spec and each C in FT_CLIENTS (default "1 4"), one CELL (RAW/cells/<spec>-c<C>/):
#   conncheck : C+16 connections opened at once and a SELECT 1 on each (amendment 27 (1)); untraced
#   idle      : strace -f -C attached to every server process for FT_IDLE_S seconds with no client (the control)
#   load      : the same attach around one bbload run of exactly N ops (FT_N1 at C=1, FT_N4 otherwise)
#   deferred  : PG only -- the same attach around one CHECKPOINT: the flushes the ops left for later (WAL_LOG's
#               data files, every op's dirty pages), reported apart from the window and never added to it
#   cell.json : stracecount.py cell: flushes per op = (load - idle x load_s/idle_s) / ops; "exact" when idle = 0
# B1 is embedded (no server): its load window runs clonebench under strace from exec, there is no idle control
# (no process exists outside the op loop), and the per-path classes in cell.json split branch from parent flushes.
#
# Then the functional checks of tools/competitors/SMOKE.md: isolation (parent/main sum(v)=0; a branch written by
# one M1 op reads sum(v)=1 over ROWS rows) and branch counts (every created branch exists), plus, where the system
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
N1=${FT_N1:-20}
N4=${FT_N4:-40}
ROWS=${FT_ROWS:-10000}
IDLE_S=${FT_IDLE_S:-10}
CLIENTS=${FT_CLIENTS:-1 4}
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
nops() { [ "$1" = 1 ] && echo "$N1" || echo "$N4"; }
fsused() { sync -f "$MNT"; df -B1 --output=used "$MNT" | tail -1 | tr -d ' '; }

case $SYSTEM in
  pg18-d2) KIND=pg MODE=d2 PORT=55432 SPECLIST="pg18-select1 pg18-m1c pg18-m1 pg18-m1c-wal pg18-m1-wal" ;;
  pg18-default) KIND=pg MODE=default PORT=55432 SPECLIST="pg18-select1 pg18-m1c pg18-m1 pg18-m1c-wal pg18-m1-wal" ;;
  dolt) KIND=dolt PORT=53306 SPECLIST="dolt-select1 dolt-m1c dolt-m1c-branch dolt-m1" ;;
  doltgres) KIND=doltgres PORT=55433 SPECLIST="doltgres-select1 doltgres-m1c doltgres-m1c-branch doltgres-m1" ;;
  b1) KIND=b1 SPECLIST="m1c-d2 m1-d2 m1c-d0 m1-d0" ;;
  *) echo "unknown system $SYSTEM" >&2; exit 2 ;;
esac
# The flush counter must have passed its fire-check on this runner and filesystem first (review finding 8).
FC=${FT_FIRECHECK:?FT_FIRECHECK: the fire-check verdict file (firecheck_strace.sh OUT/firecheck.txt)}
case "$(tail -1 "$FC" 2>/dev/null)" in
  "VERDICT PASS"*) ;;
  *) echo "REFUSED: the flush counter's fire-check did not pass: $FC ends [$(tail -1 "$FC" 2>/dev/null)]" | tee "$FUN"; exit 1 ;;
esac
{ echo "system=$SYSTEM kind=$KIND mnt=$MNT fstype=$(findmnt -n -o FSTYPE -T "$MNT") rows=$ROWS n1=$N1 n4=$N4 idle_s=$IDLE_S clients=[$CLIENTS]";
  echo "strace=$(strace -V | head -1) kernel=$(uname -r) arch=$(uname -m)"; } | tee "$RAW/run-info.txt"

# ---------------------------------------------------------------- server systems
server_pids() {
  local pid
  pid=$(cat "$DATA.ftpid")
  echo "$pid"
  # PG: every running child of the postmaster (checkpointer, walwriter, bgwriter, io workers, launchers): they
  # were forked before the attach, so -f alone would not reach them. Dolt/Doltgres are one process.
  [ "$KIND" = pg ] && ps -o pid= --ppid "$pid" | tr -d ' '
  return 0
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
  case $KIND in
    pg) srv sql "$DATA" postgres "$1" ;;
    dolt|doltgres) srv sql "$DATA" "$1" | tr '\t' '|' ;;
  esac
}
sqlp() { # sqlp SQL -- on the parent: PG's template p (only after every cell), Dolt/Doltgres main
  case $KIND in
    pg) srv sql "$DATA" p "$1" ;;
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
  esac
}

bbload() { # bbload SPEC C N OUT -> bbload's rc
  local rc=0
  "$BB" --spec "$SPECS/$1.spec" --out "$4" --clients "$2" --max-ops "$3" --set port="$PORT" --set rows="$ROWS" \
    --stall-s 600 --max-window-s 3600 >"$4.txt" 2>&1 || rc=$?
  cat "$4.txt"
  return $rc
}

# count OUT -- stracecount over one window. A refused or crashed count FAILS the job (review finding 1: it used to
# end in `|| true`, so a REFUSED window still left the job green); the cell carries the verdict too.
count() {
  local rc=0
  python3 "$SC" count "$1.strace" --extra "$1.strace.err" --root "$DATA" --window "$1.window" >"$1.json" || rc=$?
  [ $rc -eq 0 ] || fail "count $1: stracecount rc=$rc ($(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['verdict'])" "$1.json" 2>&1 | tail -1))"
}
# ops_of BBOUT CELLDIR -- "<total> <ok> <created>" into CELLDIR/ops.txt. A reader failure FAILS the job and writes
# zero ops, which the cell then refuses (it used to turn silently into "0 0 0"). Called in the driver's own shell,
# never inside $(...) or <(...), where fail()'s count would be lost.
ops_of() {
  if ! python3 "$FH" ops "$1" >"$2/ops.txt"; then fail "ops reader on $1"; echo "0 0 0" >"$2/ops.txt"; fi
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
  mkdir -p "$d"
  echo "=== $SYSTEM $spec C=$c N=$n"
  bbload "${KIND/pg/pg18}-select1" $((c + 16)) $((c + 16)) "$d/conncheck" >/dev/null ||
    fail "$spec-c$c conncheck: C+16=$((c + 16)) connections (rc $?, $(tail -1 "$d/conncheck.txt"))"
  strace_attach "$d/idle" $(server_pids) || { fail "$spec-c$c idle attach"; return; }
  sleep "$IDLE_S"
  strace_detach "$d/idle"
  if [ "$KIND" = pg ]; then template_idle || fail "$spec-c$c: a backend stayed on template p for 30 s"; fi
  used0=$(fsused)
  strace_attach "$d/load" $(server_pids) || { fail "$spec-c$c load attach"; return; }
  rc=0
  bbload "$spec" "$c" "$n" "$d/bb" || rc=$?
  strace_detach "$d/load"
  used1=$(fsused)
  echo "fs_used_before=$used0 fs_used_after=$used1 delta=$((used1 - used0))" >"$d/space.txt"
  [ $rc -eq 0 ] || fail "$spec-c$c bbload rc=$rc ($(tail -1 "$d/bb.txt"))"
  local defer=()
  if [ "$KIND" = pg ]; then
    # The deferred window is part of a PG cell (for WAL_LOG it holds most of the cost): a failed attach or a failed
    # CHECKPOINT FAILS the job, and the cell refuses without it (review finding 6: both used to drop it silently).
    defer=(--deferred "$d/deferred.json")
    if strace_attach "$d/deferred" $(server_pids); then
      sqlq "CHECKPOINT" >"$d/deferred.checkpoint.txt" 2>&1 || fail "$spec-c$c deferred CHECKPOINT rc=$? ($(tail -1 "$d/deferred.checkpoint.txt"))"
      strace_detach "$d/deferred"
      count "$d/deferred"
    else
      fail "$spec-c$c deferred attach"
    fi
  fi
  count "$d/idle"
  count "$d/load"
  ops_of "$d/bb" "$d"
  read -r total ok created <"$d/ops.txt"
  python3 "$SC" cell --name "$SYSTEM/$spec-c$c" --load "$d/load.json" --idle "$d/idle.json" \
    --load-s "$(window_s "$d/load")" --idle-s "$(window_s "$d/idle")" --ops "$total" --ops-ok "$ok" \
    ${defer[@]+"${defer[@]}"} >"$d/cell.json"
  judge_cell "$d"
  python3 -c "import json,sys; c=json.load(open(sys.argv[1])); p=c.get('per_op',{}); print('cell', c['name'], 'ops', c['ops'], 'flushes/op', p.get('flushes'), 'raw/op', p.get('flushes_raw'), 'exact', c.get('exact'), 'deferred/op', c.get('deferred',{}).get('per_op'), c['verdict'])" "$d/cell.json" | tee -a "$RAW/cells.txt"
}

server_main() {
  local spec c rc
  case $KIND in
    pg) srv init "$DATA" "$MODE" "$PORT" ;;
    *) srv init "$DATA" "$PORT" ;;
  esac
  srv start "$DATA" | tee "$RAW/server-start.txt" || { fail "server start"; return; }
  srv seed "$DATA" "$ROWS" | tee "$RAW/seed.txt" || { fail "seed"; return; }
  [ "$KIND" = pg ] && sqlq "CHECKPOINT"
  [ "$KIND" = pg ] || dolt_quiet_root "$ROOT/vroot"  # the version commands below run with metrics/version check off too
  case $KIND in
    pg) { "$FT_PG18/postgres" --version; sha256sum "$FT_PG18/postgres"; dpkg-query -W 'postgresql-18*' 'libpq5' 2>/dev/null; } >"$RAW/version.txt" ;;
    dolt) { "$FT_DOLT" version; sha256sum "$FT_DOLT"; } >"$RAW/version.txt" 2>&1 ;;
    doltgres) { (cd "$ROOT" && "$FT_DOLTGRES" -version); sha256sum "$FT_DOLTGRES"; } >"$RAW/version.txt" 2>&1 ;;
  esac
  for spec in $SPECLIST; do
    for c in $CLIENTS; do
      run_server_cell "$spec" "$c"
    done
  done

  fun "## functional checks ($SYSTEM on $(findmnt -n -o FSTYPE -T "$MNT"))"
  # The clone proof first: a later read of the template could dirty a page whose write-back un-shares its extent.
  if [ "$KIND" = pg ]; then pg_clone_proof; fi
  expect "isolation: parent/main count|sum(v)" "$(sqlp "SELECT count(*), sum(v) FROM t")" "$ROWS|0"
  local want=0 m1 br
  for d in "$RAW"/cells/*; do
    case $(basename "$d") in *select1*) continue ;; esac
    [ -f "$d/ops.txt" ] && want=$((want + $(cut -d' ' -f3 "$d/ops.txt")))
  done
  [ "$KIND" = pg ] || want=$((want + 1))  # Dolt/Doltgres: main is a branch too
  expect "branch count (every created branch exists; created from raw.tsv)" "$(count_branches)" "$want"
  for m1 in "${KIND/pg/pg18}-m1" pg18-m1-wal; do
    [ -d "$RAW/cells/$m1-c1/bb" ] || continue
    br=$(python3 "$FH" branch "$RAW/cells/$m1-c1/bb") || { fail "isolation $m1: no ok op to read back"; continue; }
    expect "isolation: $m1 branch $br count|sum(v) after one UPDATE" \
      "$(on_branch "$br" "SELECT count(*), sum(v) FROM t" | tr '\t' '|')" "$ROWS|1"
  done
  srv stop "$DATA" | tee -a "$RAW/server-stop.txt" || fail "server stop by recorded pid"
  cp "$DATA.log" "$RAW/server_log.txt" 2>/dev/null
}

pg_clone_proof() {
  # The template's table file vs. the m1c branch's: same physical blocks (and flagged shared) under
  # file_copy_method=clone; disjoint under copy. The strace half: copy_file_range calls in the create window.
  local br tfile bfile want cfr
  br=$(python3 "$FH" branch "$RAW/cells/pg18-m1c-c1/bb") || { fail "clone proof: no m1c branch"; return; }
  tfile="$DATA/$(srv sql "$DATA" p "SELECT pg_relation_filepath('t')")"
  bfile="$DATA/$(srv sql "$DATA" "$br" "SELECT pg_relation_filepath('t')")"
  sync -f "$MNT"
  python3 "$FH" cloneproof "$tfile" "$bfile" >"$RAW/cloneproof-m1c.json"
  case $MODE in d2|d1clone) want=clone ;; *) want=copy ;; esac
  expect "clone proof (filefrag, template t vs $br t): verdict" \
    "$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['verdict'])" "$RAW/cloneproof-m1c.json")" "$want"
  cfr=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['copy_file_range_calls'])" "$RAW/cells/pg18-m1c-c1/load.json")
  if [ "$want" = clone ]; then
    [ "$cfr" -gt 0 ] && pass "clone proof (strace): copy_file_range calls in the m1c C=1 window = $cfr" ||
      fail "clone proof (strace): no copy_file_range in the m1c C=1 window"
  else
    expect "copy control (strace): copy_file_range calls in the m1c C=1 window" "$cfr" 0
  fi
  [ "$(findmnt -n -o FSTYPE -T "$MNT")" = btrfs ] && btrfs filesystem du -s "$tfile" "$bfile" >"$RAW/cloneproof-btrfs-du.txt" 2>&1
  echo "template_bytes=$(srv sql "$DATA" p "SELECT pg_database_size('p')")" >>"$RAW/cells/pg18-m1c-c1/space.txt"
}

# ---------------------------------------------------------------- B1 (embedded)
b1_main() {
  local cell spec op sync c n d rc total ok created bdir
  DATA="$ROOT"  # stracecount classes are relative to ROOT: parent.db, branches/<cell>/...
  mkdir -p "$ROOT/branches"
  "$CB" mkparent --db "$ROOT/parent.db" --rows "$ROWS" | tee "$RAW/mkparent.json" || { fail "mkparent"; return; }
  { "$SQ3" --version; sha256sum "$CB" "$SQ3"; } >"$RAW/version.txt"
  for spec in $SPECLIST; do
    op=${spec%-*} sync=${spec#*-}
    for c in $CLIENTS; do
      n=$(nops "$c")
      d="$RAW/cells/b1-$spec-c$c"
      bdir="$ROOT/branches/$spec-c$c"
      mkdir -p "$d" "$bdir"
      echo "=== b1 $spec C=$c N=$n"
      rc=0
      strace_run "$d/load" "$CB" run --mode b1 --op "$op" --sync "$sync" --parent "$ROOT/parent.db" --dir "$bdir" \
        --clients "$c" --max-ops "$n" --rows "$ROWS" --out "$d/bb" >"$d/bb.txt" 2>&1 || rc=$?
      cat "$d/bb.txt"
      [ $rc -eq 0 ] || fail "b1-$spec-c$c clonebench rc=$rc ($(tail -1 "$d/bb.txt"))"
      count "$d/load"
      ops_of "$d/bb" "$d"
      read -r total ok created <"$d/ops.txt"
      python3 "$SC" cell --name "$SYSTEM/b1-$spec-c$c" --load "$d/load.json" --idle none \
        --load-s "$(window_s "$d/load")" --idle-s 0 --ops "$total" --ops-ok "$ok" >"$d/cell.json"
      judge_cell "$d"
      python3 -c "import json,sys; c=json.load(open(sys.argv[1])); p=c.get('per_op',{}); print('cell', c['name'], 'ops', c['ops'], 'flushes/op', p.get('flushes'), 'by_class', c.get('load_by_class'), c['verdict'])" "$d/cell.json" | tee -a "$RAW/cells.txt"
      expect "b1-$spec-c$c branch files (every created branch exists)" \
        "$(find "$bdir" -maxdepth 1 -name 'b_*.db' | wc -l | tr -d ' ')" "$created"
    done
  done
  fun "## functional checks (b1 on $(findmnt -n -o FSTYPE -T "$MNT"))"
  expect "isolation: parent count|sum(v)" "$("$SQ3" "$ROOT/parent.db" "SELECT count(*), sum(v) FROM t")" "$ROWS|0"
  local f
  for spec in m1-d2 m1-d0; do
    f=$(find "$ROOT/branches/$spec-c1" -maxdepth 1 -name 'b_*_0_0.db' | head -1)
    [ -n "$f" ] || { fail "isolation $spec: no branch b_*_0_0.db"; continue; }
    expect "isolation: $spec branch $(basename "$f") integrity|count|sum(v)" \
      "$("$SQ3" "$f" "PRAGMA integrity_check; SELECT count(*), sum(v) FROM t;" | tr '\n' '|' | sed 's/|$//')" "ok|$ROWS|1"
  done
  f=$(find "$ROOT/branches/m1c-d2-c1" -maxdepth 1 -name 'b_*_0_0.db' | head -1)
  if [ -n "$f" ]; then
    expect "isolation: m1c-d2 branch $(basename "$f") count|sum(v)" "$("$SQ3" "$f" "SELECT count(*), sum(v) FROM t")" "$ROWS|0"
    sync -f "$MNT"
    python3 "$FH" cloneproof "$ROOT/parent.db" "$f" >"$RAW/cloneproof-m1c.json"
    expect "clone proof (filefrag, parent vs $(basename "$f")): verdict" \
      "$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['verdict'])" "$RAW/cloneproof-m1c.json")" clone
    "$CB" extents "$f" >"$RAW/extents-m1c-branch.json" 2>&1
    "$CB" extents "$ROOT/parent.db" >"$RAW/extents-parent.json" 2>&1
  else
    fail "clone proof: no m1c-d2 C=1 branch"
  fi
  local fic
  fic=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['ficlone'])" "$RAW/cells/b1-m1c-d2-c1/load.json" 2>/dev/null)
  expect "clone proof (strace): FICLONE calls in the m1c-d2 C=1 window = creates" "$fic" \
    "$(cut -d' ' -f3 "$RAW/cells/b1-m1c-d2-c1/ops.txt" 2>/dev/null)"
}

if [ "$KIND" = b1 ]; then b1_main; else server_main; fi
python3 "$SC" table "$RAW/cells" >"$RAW/flushes.tsv" || true
cat "$RAW/flushes.tsv"
if [ $nfail -eq 0 ]; then fun "VERDICT PASS ($SYSTEM)"; exit 0; fi
fun "VERDICT FAIL ($SYSTEM): $nfail check(s) failed"
exit 1
