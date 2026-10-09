#!/usr/bin/env bash
# run.sh V3FLOOR_BIN DIR OUT N [--arms A] [--seed S] -- one V3 device-floor batch with Linux stamps and the device
# flush record (port of frontier/fastest/tools/v3/run.sh). Writes OUT/stamp_start.json, the probe's OUT/raw.tsv and
# OUT/summary.probe.json, OUT/summary.json (the probe's plus device_flushes, device_flushes_per_op,
# layer_device_flushes_per_op and flush_gate), OUT/blkflush/ (blkflush.py's record and report.json),
# OUT/stamp_end.json, OUT/gate.json, OUT/binary.txt (what binds it), OUT/rc.
#
# Arguments (review 2 item 7): only --arms and --seed pass through, each at most once; anything else refuses
# (--mutant-nosync, --trace-clock, --crash-op, --crash-aim, --dir, --out, --n ...). V3FLOOR_FIRECHECK in the
# environment refuses (the probe's fire-check flags never pass through run.sh), and the probe runs without it.
# Cell (review 2 item 4): V3_CELL=<ext4|xfs|btrfs|ext4loop|xfsloop|btrfsloop> is required; the batch's layout must
# match it (batchgate.py post), and a bound batch's verdict must be for it.
# Binding (review 2 item 8). Exactly one of:
#   V3_FIRECHECK_VERDICT=<verdict.json>  check.py's whole verdict shape (batchgate.py verdict): every planned check
#                                        passing, for THIS binary (sha256), arch, DIR's fstype and V3_CELL, no
#                                        "planted" key, a leaf that is not brd, and the fire-check's own binding
#                                        record next to it (<verdict>.bind.json) passed for this verdict (a pending
#                                        record binds only firecheck.sh's own bind step, which names the verdict's
#                                        sha256 in V3_BIND_PENDING_SHA); V3FLOOR_BRD in the env refuses. binary.txt
#                                        records the verdict's sha256 and run id; the gate re-hashes the verdict after
#                                        the run
#   V3_SMOKE=1                           an explicitly unbound smoke batch, recorded as such, never credited
# Neither, or both: refused (rc 2) before anything runs.
# V3_REQUIRE_T3=1: also refuse before anything runs unless the registered T3 preconditions hold (every CPU on the
# "performance" governor, clocksource tsc or arch_sys_counter: review 2 items 16-17; batchgate.py t3pre), and after
# the run unless the registered values it used exist (A17: the cell class's d0 threshold, the frame arm).
# V3_PLP=yes|no is required: the operator's power-loss-protection declaration for the leaf drive (annex A14; a
# hosted runner or a dry run says no). The probe gets --plp and --registered REGISTERED.tsv (this directory's).
# A bound batch has the registered V3 shape (PREREG section 4; gate-6 review MED 6): N = 10000 and arms exactly
# append25, fdatasync4k (A18's ow4k + fdatasync), nosync25 and the registered frame arm when there is one; anything
# else refuses ("bound shape:"). A smoke batch may run any shape and records it.
# LD_PRELOAD, LD_AUDIT or LD_LIBRARY_PATH in the environment refuses (an interposer would not change the sha256).
# After the probe (batchgate.py post): refused (rc 2) if the summary carries mutant_nosync or trace_clock != 0, names
# another binary (exe_sha256), another layout, has no drive or brd leaf record, no gated arm ran, or (bound) a brd
# leaf, another leaf class, layer stack, leaf driver or drive model, or virtualization than the verdict's, or the
# verdict changed during the run; also refused when the gate itself fails (any rc but 0/2/3, or no gate.json), never
# read as a pass. VOID (rc 3) if the leaf reports write-back and its flush count is below n x gated arms, or a gated
# op's window holds no flush-carrying request to it. A killed run.sh stops its tracefs instance (EXIT trap).
# Exit: 2 refused or a record could not be taken; else the probe's rc (0 ok, 1 op failed, 3 void), or 3 from the gate.
# Not a credited measurement unless the PREREG is registered with its T3 rules.
set -uo pipefail
refuse() { echo "run.sh: REFUSED: $*" >&2; exit 2; }
[ $# -ge 4 ] || refuse "usage: run.sh V3FLOOR_BIN DIR OUT N [--arms A] [--seed S] (V3_CELL=..., V3_SMOKE=1 or V3_FIRECHECK_VERDICT=...)"
BIN=$1 DIR=$2 OUT=$3 N=$4
shift 4
args=()
seen_arms=0 seen_seed=0
while [ $# -gt 0 ]; do
  case $1 in
    --arms) { [ $seen_arms = 0 ] && [ $# -ge 2 ]; } || refuse "--arms takes one value, once"; args+=(--arms "$2"); seen_arms=1; shift 2 ;;
    --seed) { [ $seen_seed = 0 ] && [ $# -ge 2 ]; } || refuse "--seed takes one value, once"; args+=(--seed "$2"); seen_seed=1; shift 2 ;;
    *) refuse "argument '$1' is not allowed through run.sh (only --arms A and --seed S; the probe's fire-check flags never pass)" ;;
  esac
done
case $N in ''|*[!0-9]*) refuse "N '$N' is not a whole number" ;; esac
HERE="$(cd "$(dirname "$0")" && pwd)"
STAMP="$HERE/stamp.py" GATE="$HERE/batchgate.py" BLK="$HERE/blkflush.py"
CELL=${V3_CELL:-}
python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import v3cell; sys.exit(0 if sys.argv[2] in v3cell.CELLS else 1)' "$HERE" "$CELL" \
  || refuse "V3_CELL='$CELL' is not a cell (ext4, xfs, btrfs on a block device; ext4loop, xfsloop, btrfsloop on a loop)"
[ -z "${V3FLOOR_FIRECHECK:-}" ] || refuse "V3FLOOR_FIRECHECK is set: the probe's fire-check flags never pass through run.sh"
case ${V3_PLP:-} in yes|no) ;; *) refuse "V3_PLP='${V3_PLP:-}' is not yes or no (the leaf drive's power-loss protection, the operator's declaration: annex A14)" ;; esac
REG="$HERE/REGISTERED.tsv"
[ -f "$REG" ] || refuse "no registered file $REG"
for v in LD_PRELOAD LD_AUDIT LD_LIBRARY_PATH; do
  [ -z "${!v:-}" ] || refuse "$v is set: an interposed library could change what the probe does under an unchanged sha256"
done
sha=$(sha256sum "$BIN" | cut -d' ' -f1) && [ -n "$sha" ] || refuse "cannot hash $BIN"
fstype=$(findmnt -n -o FSTYPE -T "$DIR") || refuse "cannot find the filesystem of $DIR"
# the cell's layout before any op (ninth review L13): a loop cell's D is on a loop device, a block cell's is not; the
# whole layout is post's (v3cell.layout_problems), this only stops a mislabelled batch before it runs 10000 ops
msrc=$(findmnt -n -o SOURCE -T "$DIR") || refuse "cannot find the mount source of $DIR"
python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import v3cell; sys.exit(0 if v3cell.is_loop(sys.argv[2]) == sys.argv[3].startswith("/dev/loop") else 1)' "$HERE" "$CELL" "$msrc" \
  || refuse "cell layout: V3_CELL=$CELL is a $(python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import v3cell; print("loop" if v3cell.is_loop(sys.argv[2]) else "block")' "$HERE" "$CELL") cell, but $DIR is on $msrc"
arch=$(uname -m)
vsha="" vrun="" vleaf="" vbasis=""
if [ -n "${V3_FIRECHECK_VERDICT:-}" ] && [ -n "${V3_SMOKE:-}" ]; then
  refuse "set V3_FIRECHECK_VERDICT or V3_SMOKE=1, not both"
elif [ -n "${V3_FIRECHECK_VERDICT:-}" ]; then
  [ -z "${V3FLOOR_BRD:-}" ] || refuse "V3FLOOR_BRD is set: brd is fire-check only, never a bound batch"
  vj=$(python3 -B "$GATE" verdict "$V3_FIRECHECK_VERDICT" "$CELL" "$sha" "$arch" "$fstype")
  vrc=$?
  [ "$vrc" -eq 0 ] || refuse "$V3_FIRECHECK_VERDICT does not bind this batch (binary $sha, fs $fstype, arch $arch, cell $CELL): $vj"
  { read -r vsha; read -r vrun; read -r vleaf; read -r vbasis; } < <(python3 -B -c 'import json, sys; v = json.loads(sys.argv[1]); print("\n".join(str(v[k]) for k in ("verdict_sha256", "run_id", "leaf_class", "bind_basis")))' "$vj")
  { [ -n "$vsha" ] && [ -n "$vrun" ] && [ -n "$vleaf" ] && [ -n "$vbasis" ] && [ "$vbasis" != None ]; } || refuse "the binding fields could not be read back from: $vj"
  mode=bound
  bound="fire-checked: $V3_FIRECHECK_VERDICT"
elif [ "${V3_SMOKE:-}" = 1 ]; then
  mode=smoke
  bound="smoke: V3_SMOKE=1, not bound to a fire-check, never credited"
else
  refuse "set V3_FIRECHECK_VERDICT=<a passing fire-check verdict.json for this binary and cell> or V3_SMOKE=1"
fi
if [ "$mode" = bound ]; then
  frame=$(awk -F '\t' '$1 == "frame_arm" { v = $2 } END { print v }' "$REG")
  want="append25,fdatasync4k,nosync25${frame:+,$frame}"
  got=""
  for ((k = 0; k < ${#args[@]}; k++)); do [ "${args[$k]}" = --arms ] && got=${args[$((k + 1))]}; done
  norm() { tr ',' '\n' <<< "$1" | sort | paste -sd, -; }
  { [ "$N" = 10000 ] && [ -n "$got" ] && [ "$(norm "$got")" = "$(norm "$want")" ]; } \
    || refuse "bound shape: a bound batch runs N=10000 with arms {$want} (the registered V3 shape; frame arm ${frame:-unregistered}); this one is N=$N arms '${got:-the probe default}'"
  shape="bound V3: N=10000, $want"
else
  shape="smoke: N=$N arms ${args[*]:-default}"
fi
rental=no
if [ "${V3_REQUIRE_T3:-}" = 1 ]; then
  rental=yes
  tj=$(python3 -B "$GATE" t3pre) || refuse "V3_REQUIRE_T3=1 and the registered T3 preconditions do not hold: $tj"
  # a real run is on a drive (A16; eighth review M6): no loop cell; the probe itself refuses an unregistered frame arm
  # or threshold before any op (--require-registered, eighth review L7), and post re-checks both after
  python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import v3cell; sys.exit(1 if v3cell.is_loop(sys.argv[2]) else 0)' "$HERE" "$CELL" \
    || refuse "rental: V3_CELL=$CELL is a loop cell; a real run is on a drive (A16: loop devices are dry-run only)"
  args+=(--require-registered)
fi
TMP="$OUT.stamp_start.json" BLKD="$OUT.blkflush"
{ [ ! -e "$OUT" ] && [ ! -e "$TMP" ] && [ ! -e "$BLKD" ]; } || refuse "$OUT, $TMP or $BLKD exists"
python3 -B "$STAMP" start "$TMP" --dir "$DIR" > /dev/null || { rm -f "$TMP"; refuse "the start stamp failed"; }
python3 -B "$BLK" start "$BLKD" > /dev/null || { rm -f "$TMP"; rm -rf "$BLKD"; refuse "blkflush.py could not start (tracefs via sudo -n)"; }
# a killed run.sh must not leave the tracefs instance tracing (fresh review B-L6)
stopped=0
trap '[ "$stopped" = 1 ] || python3 -B "$BLK" stop "$BLKD" > /dev/null 2>&1' EXIT
env -u V3FLOOR_FIRECHECK "$BIN" --dir "$DIR" --out "$OUT" --n "$N" "${args[@]}" --plp "$V3_PLP" --registered "$REG"
prc=$?
python3 -B "$BLK" stop "$BLKD" > /dev/null
brc=$?
stopped=1
src=2 rrc=2 grc=2
if [ -d "$OUT" ]; then
  mv "$TMP" "$OUT/stamp_start.json"
  mv "$BLKD" "$OUT/blkflush"
  # the flush path's devices, from the probe's own summary (ninth review L12); none read is stamp end's own problem
  devs=$(python3 -B -c 'import json, sys
s = json.load(open(sys.argv[1]))
d = [l.get("disk") for l in s.get("flush_path") or []]  # not the multipath path disks (tenth review LOW 5)
print(",".join(x for x in d if x))' "$OUT/summary.json" 2>/dev/null)
  python3 -B "$STAMP" end "$OUT/stamp_start.json" "$OUT/stamp_end.json" --devices "${devs:-}"
  src=$?
  if [ -f "$OUT/raw.tsv" ]; then
    ppid=$(python3 -B -c 'import json, sys; print(int(json.load(open(sys.argv[1]))["pid"]))' "$OUT/summary.json" 2>/dev/null)
    if [ -n "$ppid" ]; then
      # the probe's own sync_fds key the per-window sync attribution (tenth review HIGH 1)
      python3 -B "$BLK" report "$OUT/blkflush" --windows "$OUT/raw.tsv" --pid "$ppid" --summary "$OUT/summary.json" > "$OUT/blkflush/report.json"
    else
      python3 -B "$BLK" report "$OUT/blkflush" --windows "$OUT/raw.tsv" > "$OUT/blkflush/report.json"
    fi
    rrc=$?
  fi
  printf 'v3floor_sha256=%s\nfstype=%s\narch=%s\ncell=%s\nbound=%s\nverdict_sha256=%s\nverdict_run_id=%s\nverdict_leaf_class=%s\nbind_basis=%s\nplp=%s\nshape=%s\nrental=%s\n' \
    "$sha" "$fstype" "$arch" "$CELL" "$bound" "$vsha" "$vrun" "$vleaf" "$vbasis" "$V3_PLP" "$shape" "$rental" > "$OUT/binary.txt"
  if [ -f "$OUT/summary.json" ]; then
    if [ "$mode" = bound ]; then
      python3 -B "$GATE" post "$OUT" "$CELL" "$sha" "$mode" "$V3_FIRECHECK_VERDICT"
    else
      python3 -B "$GATE" post "$OUT" "$CELL" "$sha" "$mode"
    fi
    grc=$?
    case $grc in
      0|2|3) ;;
      *) echo "run.sh: REFUSED after the run: the batch gate failed (rc $grc), so nothing it would have refused is known" >&2; grc=2 ;;
    esac
    [ -f "$OUT/gate.json" ] || { echo "run.sh: REFUSED after the run: the batch gate failed: no gate.json" >&2; grc=2; }
  fi
else
  rm -f "$TMP"
  rm -rf "$BLKD"
fi
rc=$prc
if [ "$prc" -eq 0 ] || [ "$prc" -eq 3 ]; then
  [ "$grc" -eq 2 ] && rc=2
  [ "$prc" -eq 0 ] && [ "$grc" -eq 3 ] && rc=3
  if [ "$rc" -ne 2 ] && { [ "$src" -ne 0 ] || [ "$brc" -ne 0 ] || [ "$rrc" -ne 0 ]; }; then rc=2; fi
fi
[ -d "$OUT" ] && echo "probe_rc=$prc stamp_rc=$src blkflush_stop_rc=$brc blkflush_report_rc=$rrc gate_rc=$grc rc=$rc" > "$OUT/rc"
exit "$rc"
