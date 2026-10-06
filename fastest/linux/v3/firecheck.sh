#!/usr/bin/env bash
# firecheck.sh V3FLOOR CELL WORKDIR OUT -- fire-check the Linux V3 device-floor probe on one cell.
# NOT a credited measurement. Port of frontier/fastest/tools/v3/firecheck.py, whose F1/F2 ran under the Mac's V1
# shim; here strace is the flush-syscall instrument and blkflush.py the device-flush one.
#   CELL     explicit (v3cell.py): ext4|xfs|btrfs on a block device, ext4loop|xfsloop|btrfsloop on a loop; WORKDIR
#            must be on it. On a brd cell (the work mount's source is /dev/ram*), V3FLOOR_BRD=1 is exported for the
#            probe runs (brd is fire-check only) and unset for every run.sh bound-mode plant.
#   OUT      raw output; must not exist.
#   V3_FX    (env) the base dir mkfixtures.sh made. Unset or a fixture missing: its plants FAIL in check.py.
#   V3_BASE  (env) a dir holding the base (df4b39e53) v3floor binary and run.sh/stamp.py/check.py: the red column
#            (OUT/red/, check.py writes red.json; never part of the verdict). V3_BASE_SHA names it.
#   V3_SHIM  (env) statfs_shim.so, for the R_statfs_shim plant.
# Stages (verdicts by check.py, which reads only OUT; every expectation there comes from the arm definitions):
#   F1  each arm set under strace -f -c at n = 1, 2, 3, 40: per-op syscalls equal the definitions, flush totals equal
#       setup + definition x n + teardown (ASLR off: setarch -R).
#   F1b the real probe under strace -f -y with --trace-clock: inside every timed window exactly the arm's syscalls on
#       its own files, sizes and offsets; nothing between windows; the only other trace line is the exit (item 14).
#       n = 300 for every arm and 4100 for the 4 KiB arms; check.py plants breaches into a copy and must reject each.
#   F2  the mutant (--mutant-nosync) under -c; F2c both specs reject each other's counts; F2d the mutant's sequence;
#       F2b the mutant unwatched at n = 200 fails the flush control (rc 3), and F3 does not.
#   F3  a real n = 200 run of every arm through run.sh (V3_SMOKE=1): stamps, blkflush.py's device flush record, the
#       batch gate.
#   C   (loop cells) crash.sh: one copy op, then a crash; controls survive, the no-flush mutant is lost (item 3).
#   B   blkflush.py's own fire-check: its self-test, 50 fsyncs of a raw loop device each issue exactly one flush
#       request, buffered writes and empty windows issue none, an overrun buffer refuses, misuse refuses.
#   F4  refusals: rc 2, the reason in the message, no out dir; the plants of review 2 items 1, 7-16; run.sh's
#       argument allowlist, environment refusals and verdict binding.
#   R   (V3_BASE set) the red column: the same plants against the base probe and scripts.
#   bind after check.py: run.sh bound to the cell's own verdict (P_runsh_ok), or refused on a brd cell
#       (P_runsh_brd); check.py --bind records it in OUT/bind.json.
# The fire-check-only flags need V3FLOOR_FIRECHECK=1, which this script sets only on those runs (F1b, F2, F2b, F2d,
# R_mutnod0, crash.sh) and run.sh refuses.
# Exit: 0 when check.py's verdict and the binding both pass; 1 a check failed; 2 usage.
set -uo pipefail
[ $# -eq 4 ] || { echo "usage: firecheck.sh V3FLOOR CELL WORKDIR OUT" >&2; exit 2; }
V3=$(readlink -f "$1") CELL=$2 W=$3 OUT=$4
HERE="$(cd "$(dirname "$0")" && pwd)"
KIND=$(python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import v3cell; print(v3cell.kind(sys.argv[2]))' "$HERE" "$CELL" 2>/dev/null) \
  || { echo "firecheck: '$CELL' is not a cell (v3cell.py)" >&2; exit 2; }
LOOPCELL=$(python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import v3cell; print(int(v3cell.is_loop(sys.argv[2])))' "$HERE" "$CELL")
[ -x "$V3" ] || { echo "firecheck: $V3 is not executable" >&2; exit 2; }
[ -e "$OUT" ] && { echo "firecheck: $OUT exists" >&2; exit 2; }
mkdir -p "$OUT/F1" "$OUT/F1b" "$OUT/F2" "$OUT/F4" "$OUT/B" "$W" || exit 2
W=$(readlink -f "$W")
ALL=append25,append64,ow4k,ow64k,ow1m,clone1b,clone2b,cfr2b,clean,fdatasync4k,nosync25
FLUSHED="append25 append64 ow4k ow64k ow1m fdatasync4k clone1b clone2b cfr2b"
NS="1 2 3 40"
ARCH=$(uname -m)
SHA=$(sha256sum "$V3" | cut -d' ' -f1)
WFS=$(findmnt -n -o FSTYPE -T "$W")
WSRC=$(findmnt -n -o SOURCE -T "$W")
ME=$(id -un)
FX=${V3_FX:-/nonexistent}
RS="$HERE/run.sh" GATE="$HERE/batchgate.py" BLK="$HERE/blkflush.py"
export V3_CELL=$CELL
BRD=0
case $WSRC in /dev/ram*) BRD=1; export V3FLOOR_BRD=1 ;; esac
CS=/sys/devices/system/clocksource/clocksource0
ROOTSRC=$(findmnt -n -o SOURCE /)
ROOTDISK=$(lsblk -no PKNAME "$ROOTSRC" 2>/dev/null | head -1)
[ -n "$ROOTDISK" ] || ROOTDISK=$(basename "$ROOTSRC")

# The probe reads LOOP_GET_STATUS64 on every loop of a flush path and NVMe Identify on the leaf's controller, as
# the calling user: grant read access (an ACL) to every loop device and NVMe controller node.
grant() {
  local d
  for d in /dev/loop[0-9]* /dev/nvme[0-9]; do
    [ -e "$d" ] || continue
    sudo setfacl -m "u:$ME:r" "$d" 2>/dev/null || sudo chmod o+r "$d"
  done
}
grant

{
  echo "utc=$(date -u +%FT%TZ)"
  echo "run_id=${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
  echo "cell=$CELL"
  echo "kind=$KIND"
  echo "brd_cell=$BRD"
  echo "work=$W"
  echo "work_fstype=$WFS"
  echo "work_mount=$(findmnt -n -o SOURCE,TARGET,OPTIONS -T "$W")"
  echo "root_mount=$(findmnt -n -o SOURCE,FSTYPE,OPTIONS /)"
  echo "root_disk=$ROOTDISK write_cache=[$(cat "/sys/block/$ROOTDISK/queue/write_cache" 2>/dev/null)]"
  echo "mnt_mount=$(findmnt -n -o SOURCE,FSTYPE,OPTIONS -T /mnt)"
  losetup -l -n -O NAME,BACK-FILE,DIO 2>/dev/null | sed 's/^/loop /'
  echo "v3floor_sha256=$SHA"
  echo "arch=$ARCH"
  echo "uname=$(uname -srm)"
  echo "strace=$(strace -V | head -1)"
  echo "clocksource=$(cat $CS/current_clocksource) available=[$(cat $CS/available_clocksource)]"
  echo "base_sha=${V3_BASE_SHA:-}"
  [ -n "${V3_BASE:-}" ] && echo "base_v3floor_sha256=$(sha256sum "$V3_BASE/v3floor" | cut -d' ' -f1)"
  # setarch -R: the traced runs have no ASLR (run 37243578945: arm64 munmap varied +-1 between runs without it).
  echo "personality_under_setarch_R=$(setarch "$ARCH" -R cat /proc/self/personality)"
  for d in /sys/block/*; do echo "block ${d##*/} write_cache=[$(cat "$d/queue/write_cache" 2>/dev/null)] fua=[$(cat "$d/queue/fua" 2>/dev/null)]"; done
  for d in /dev/loop[0-9]* /dev/nvme[0-9]; do [ -e "$d" ] && echo "acl $d $(getfacl -cp "$d" 2>/dev/null | grep "^user:$ME" | xargs)"; done
} > "$OUT/info.txt" 2>&1
cat "$OUT/info.txt"

tagof() { if [ "$1" = "$ALL" ]; then echo all; else echo "${1//,/+}"; fi; }
FC() { V3FLOOR_FIRECHECK=1 "$@"; }

# one probe run under strace -f -c: stage set n [extra args]
counted() {
  local stage=$1 set=$2 n=$3
  shift 3
  local base pre=()
  base="$OUT/$stage/$(tagof "$set").n$n"
  [ $# -gt 0 ] && pre=(env V3FLOOR_FIRECHECK=1)
  timeout 600 "${pre[@]}" setarch "$ARCH" -R strace -f -c -o "$base.strace" "$V3" --dir "$W" --out "$base.out" --n "$n" \
    --arms "$set" --seed 7 "$@" > "$base.txt" 2>&1
  echo $? > "$base.rc"
}
# one probe run under strace -f -y with --trace-clock: tag set n [extra args]; the trace is gzipped
sequenced() {
  local tag=$1 set=$2 n=$3
  shift 3
  local base="$OUT/F1b/$tag"
  timeout 900 env V3FLOOR_FIRECHECK=1 setarch "$ARCH" -R strace -f -y -s 1 -o "$base.trace" "$V3" --dir "$W" --out "$base.out" \
    --n "$n" --arms "$set" --seed 5 --trace-clock "$@" > "$base.txt" 2>&1
  echo $? > "$base.rc"
  gzip -9 "$base.trace"
}

echo "== F1: strace -f -c, real probe"
for set in nosync25 clean $(for a in $FLUSHED; do echo "$a,nosync25"; done) "$ALL"; do
  for n in $NS; do counted F1 "$set" "$n"; done
done
LEAF=$(python3 -B "$GATE" leafclass "$OUT/F1/nosync25.n1.out/summary.json" 2>/dev/null || echo unknown)
echo "leaf_class=$LEAF" >> "$OUT/info.txt"
echo "== F1b: strace -f -y --trace-clock, real probe"
sequenced real-all "$ALL" 300
sequenced real-4k ow4k,fdatasync4k,nosync25 4100
echo "== F2: strace -f -c, --mutant-nosync"
for set in clean $(for a in $FLUSHED; do echo "$a,nosync25"; done) "$ALL"; do
  for n in $NS; do counted F2 "$set" "$n" --mutant-nosync; done
done
echo "== F2d: strace -f -y --trace-clock, --mutant-nosync"
sequenced mutant-all "$ALL" 40 --mutant-nosync
echo "== F2b: the mutant unwatched, n=200"
FC timeout 900 "$V3" --dir "$W" --out "$OUT/F2b.out" --n 200 --arms "$ALL" --seed 11 --mutant-nosync > "$OUT/F2b.txt" 2>&1
echo $? > "$OUT/F2b.rc"
echo "== F3: the real probe, n=200, through run.sh"
V3_SMOKE=1 timeout 900 bash "$RS" "$V3" "$W" "$OUT/F3" 200 --arms "$ALL" --seed 11 > "$OUT/F3.txt" 2>&1
echo $? > "$OUT/F3.rc"

if [ "$LOOPCELL" = 1 ]; then
  echo "== C: crash arms ($KIND)"
  timeout 900 bash "$HERE/crash.sh" "$V3" "$KIND" "$OUT/crash" > "$OUT/crash.txt" 2>&1
  echo $? > "$OUT/crash.rc"
  grant
fi

echo "== B: blkflush.py's own fire-check"
B=$OUT/B
python3 -B "$BLK" self-test > "$B/selftest.txt" 2>&1
img=/var/tmp/v3blk-$$.img
sudo rm -f "$img"; sudo truncate -s 64M "$img"
bdev=$(sudo losetup --find --show "$img")
echo "$bdev" > "$B/loopdev.txt"
sudo setfacl -m "u:$ME:rw" "$bdev" 2>/dev/null || sudo chmod o+rw "$bdev"
python3 -B "$BLK" start "$B/trace" > "$B/start.txt" 2>&1
python3 -B "$BLK" gen "$bdev" "$B/windows.tsv" 50 > "$B/gen.txt" 2>&1
python3 -B "$BLK" stop "$B/trace" > "$B/stop.txt" 2>&1
python3 -B "$BLK" report "$B/trace" --windows "$B/windows.tsv" > "$B/report.json" 2> "$B/report.err"
python3 -B "$BLK" start "$B/ovf" --buffer-kb 4 > "$B/ovf-start.txt" 2>&1
python3 -B "$BLK" gen "$bdev" "$B/ovf.tsv" 3000 > "$B/ovf-gen.txt" 2>&1
python3 -B "$BLK" stop "$B/ovf" > "$B/ovf-stop.txt" 2>&1
python3 -B "$BLK" report "$B/ovf" > "$B/overflow.json" 2> "$B/overflow.err"
echo $? > "$B/overflow.rc"
python3 -B "$BLK" stop "$B/never-started" > "$B/stop_unstarted.txt" 2>&1
echo $? > "$B/stop_unstarted.rc"
python3 -B "$BLK" start "$B/trace" > "$B/start_twice.txt" 2>&1
echo $? > "$B/start_twice.rc"
sudo losetup -d "$bdev"
sudo rm -f "$img"

echo "== F4: refusals"
refuse() { # tag cmd...
  local tag=$1
  shift
  timeout 300 "$@" > "$OUT/F4/$tag.txt" 2>&1
  echo $? > "$OUT/F4/$tag.rc"
}
o() { echo "$OUT/F4/$1.out"; }
# a plant on a fixture: tag fixture-name [env...] -- runs the probe on $FX/<fixture>/w (or the .dir path)
fx() {
  local tag=$1 name=$2 dir
  shift 2
  dir=$FX/$name/w
  [ -f "$FX/$name.dir" ] && dir=$(cat "$FX/$name.dir")
  if [ ! -f "$FX/$name.ok" ]; then
    echo "fixture $name missing (no $FX/$name.ok)" > "$OUT/F4/$tag.txt"; echo missing > "$OUT/F4/$tag.rc"; return
  fi
  refuse "$tag" "$@" "$V3" --dir "$dir" --out "$(o "$tag")" --n 5 --arms append25,nosync25
}
shm=/dev/shm/v3fc.$$
mkdir -p "$shm"
echo "shm_fstype=$(findmnt -n -o FSTYPE -T "$shm")" >> "$OUT/info.txt"
refuse R_tmpfs "$V3" --dir "$shm" --out "$(o R_tmpfs)" --n 5 --arms nosync25
rmdir "$shm"
fx R_nobarrier nb
fx R_nobarrier_below nbx
mkdir -p "$(o R_outexists)" && echo sentinel > "$(o R_outexists)/sentinel"
refuse R_outexists "$V3" --dir "$W" --out "$(o R_outexists)" --n 5 --arms nosync25
refuse R_n0 "$V3" --dir "$W" --out "$(o R_n0)" --n 0 --arms nosync25
refuse R_nalpha "$V3" --dir "$W" --out "$(o R_nalpha)" --n abc --arms nosync25
refuse R_ntrail "$V3" --dir "$W" --out "$(o R_ntrail)" --n 5x --arms nosync25
refuse R_unknown "$V3" --dir "$W" --out "$(o R_unknown)" --n 5 --arms append25,bogus,nosync25
refuse R_twice "$V3" --dir "$W" --out "$(o R_twice)" --n 5 --arms nosync25,nosync25
refuse R_nod0 "$V3" --dir "$W" --out "$(o R_nod0)" --n 5 --arms append25,ow4k
refuse R_mutnod0 env V3FLOOR_FIRECHECK=1 "$V3" --dir "$W" --out "$(o R_mutnod0)" --n 5 --arms append25,ow4k --mutant-nosync
refuse R_nice nice -n 5 "$V3" --dir "$W" --out "$(o R_nice)" --n 5 --arms nosync25
refuse R_ionice ionice -c 3 "$V3" --dir "$W" --out "$(o R_ionice)" --n 5 --arms nosync25
refuse R_schedidle chrt -i 0 "$V3" --dir "$W" --out "$(o R_schedidle)" --n 5 --arms nosync25
refuse R_schedbatch chrt -b 0 "$V3" --dir "$W" --out "$(o R_schedbatch)" --n 5 --arms nosync25
refuse R_badarg "$V3" --dir "$W" --out "$(o R_badarg)" --n 5 --bogus
refuse R_noout "$V3" --dir "$W" --n 5 --arms nosync25
refuse R_pathlong "$V3" --dir "$W/$(printf 'a%.0s' $(seq 4000))" --out "$(o R_pathlong)" --n 5 --arms nosync25
refuse R_mutant_noenv env -u V3FLOOR_FIRECHECK "$V3" --dir "$W" --out "$(o R_mutant_noenv)" --n 5 --arms append25,nosync25 --mutant-nosync
refuse R_traceclock_noenv env -u V3FLOOR_FIRECHECK "$V3" --dir "$W" --out "$(o R_traceclock_noenv)" --n 5 --arms append25,nosync25 --trace-clock
refuse R_crashop_noenv env -u V3FLOOR_FIRECHECK "$V3" --dir "$W" --out "$(o R_crashop_noenv)" --crash-op cfr2b
# item 13: a planted symlink where an arm file goes, and a leftover regular file
tgt=/dev/shm/v3fc-symlink-target.$$
rm -f "$tgt"
ln -s "$tgt" "$W/ow1m"
refuse R_symlink "$V3" --dir "$W" --out "$(o R_symlink)" --n 5 --arms ow1m,nosync25
if [ -e "$tgt" ]; then echo present > "$OUT/F4/R_symlink.target"; else echo absent > "$OUT/F4/R_symlink.target"; fi
rm -f "$W/ow1m" "$tgt"
echo leftover > "$W/append25"
refuse R_leftover_file "$V3" --dir "$W" --out "$(o R_leftover_file)" --n 5 --arms append25,nosync25
rm -f "$W/append25"
# items 9, 10, 12, 15, 1(a): fixtures
fx R_hidden_tmpfs ht
fx R_hidden_nobarrier hn
fx R_lazy lz
fx R_deleted del
fx R_nest4 n4
fx R_dirsync ds
fx R_logdev ld
fx R_extjournal ej
fx R_multidev md
fx R_loop_wt wt
fx R_brd brd env -u V3FLOOR_BRD
fx R_driver dm
fx P_nest3 n3
if [ -n "${V3_SHIM:-}" ] && [ -f "$V3_SHIM" ]; then
  refuse R_statfs_shim env LD_PRELOAD="$(readlink -f "$V3_SHIM")" "$V3" --dir "$W" --out "$(o R_statfs_shim)" --n 5 --arms append25,nosync25
else
  echo "V3_SHIM missing" > "$OUT/F4/R_statfs_shim.txt"; echo missing > "$OUT/F4/R_statfs_shim.rc"
fi
if [ "$KIND" = ext4 ]; then
  # item 12(c): strace makes the trial FICLONE succeed; it is the K-th ioctl of an identical run
  timeout 300 setarch "$ARCH" -R strace -f -e trace=ioctl -o "$OUT/F4/ficlone_count.strace" "$V3" --dir "$W" \
    --out "$OUT/F4/ficlone_count.out" --n 1 --arms clone1b,append25,nosync25 > "$OUT/F4/ficlone_count.txt" 2>&1
  k=$(grep -n FICLONE "$OUT/F4/ficlone_count.strace" | head -1 | cut -d: -f1)
  rm -rf "$OUT/F4/ficlone_count.out"
  echo "ficlone_ioctl_index=${k:-none}" >> "$OUT/info.txt"
  if [ -n "$k" ]; then
    refuse R_ficlone_accept setarch "$ARCH" -R strace -f -e trace=ioctl -e "inject=ioctl:retval=0:when=$k" \
      -o "$OUT/F4/R_ficlone_accept.strace" "$V3" --dir "$W" --out "$(o R_ficlone_accept)" --n 1 --arms clone1b,append25,nosync25
  else
    echo "no FICLONE ioctl in the counting run" > "$OUT/F4/R_ficlone_accept.txt"; echo missing > "$OUT/F4/R_ficlone_accept.rc"
  fi
  ls -A "$W" > "$OUT/F4/R_ficlone_accept.left"
else
  refuse X_allclones "$V3" --dir "$W" --out "$(o X_allclones)" --n 5 --arms clone1b,clone2b,cfr2b,nosync25
fi
# a clones dir left by a dead run: xfs/btrfs refuse it; ext4 refuses the clone arm before looking
mkdir "$W/clone1b.clones"
refuse R_leftover "$V3" --dir "$W" --out "$(o R_leftover)" --n 5 --arms clone1b,nosync25
rmdir "$W/clone1b.clones"
ROOTW=$(cat "$FX/root.dir" 2>/dev/null || echo /var/tmp/v3fx-root/w)
# item 1(b): the kernel's write_cache overridden against the drive, on the root disk (plantable only on write-back)
flip() { # tag cmd... -- run cmd with the root disk's queue/write_cache flipped to write through, then restore
  local tag=$1 wc="/sys/block/$ROOTDISK/queue/write_cache" before after
  shift
  before=$(cat "$wc" 2>/dev/null)
  if [ "$before" != "write back" ]; then
    echo "not planted: the root disk $ROOTDISK reads '$before' (kernel 6.17's queue/write_cache can only disable a cache)" > "$tag.na"
    return
  fi
  echo "write through" | sudo tee "$wc" > /dev/null
  "$@"
  echo "write back" | sudo tee "$wc" > /dev/null
  after=$(cat "$wc")
  echo "$ROOTDISK write_cache: before '$before', during 'write through', after '$after'" >> "$tag.flip"
}
flip "$OUT/F4/R_leaf_flip" refuse R_leaf_flip "$V3" --dir "$ROOTW" --out "$(o R_leaf_flip)" --n 5 --arms append25,nosync25
# item 16: a clocksource other than tsc/arch_sys_counter, then restored
csalt() { # tag cmd...
  local tag=$1 cur alt
  shift
  cur=$(cat $CS/current_clocksource)
  alt=$(tr ' ' '\n' < $CS/available_clocksource | grep -v -x -e "$cur" -e tsc -e arch_sys_counter -e '' | head -1)
  if [ -z "$alt" ]; then echo "not planted: no clocksource other than $cur available" > "$tag.na"; return; fi
  echo "$alt" | sudo tee $CS/current_clocksource > /dev/null
  "$@"
  echo "$cur" | sudo tee $CS/current_clocksource > /dev/null
  echo "clocksource: $cur -> $alt -> $(cat $CS/current_clocksource)" >> "$tag.cs"
}
csalt "$OUT/F4/R_clocksource" refuse R_clocksource "$V3" --dir "$W" --out "$(o R_clocksource)" --n 5 --arms append25,nosync25

echo "== F4: run.sh: the argument allowlist, the environment, the verdict binding"
fixture() { # name mod -> a planted full-shape verdict (batchgate.py fixture), path on stdout
  python3 -B "$GATE" fixture "$OUT/F4/verdict-$1.json" "$CELL" "${4:-$ARCH}" "$LEAF" "${3:-$SHA}" "${5:-$WFS}" ${2:+"$2"}
  echo "$OUT/F4/verdict-$1.json"
}
NB=(env -u V3FLOOR_BRD -u V3_SMOKE)
refuse R_runsh_none env -u V3_SMOKE -u V3_FIRECHECK_VERDICT -u V3FLOOR_BRD bash "$RS" "$V3" "$W" "$(o R_runsh_none)" 5 --arms append25,nosync25
refuse R_runsh_both env V3_SMOKE=1 V3_FIRECHECK_VERDICT="$(fixture both "")" bash "$RS" "$V3" "$W" "$(o R_runsh_both)" 5 --arms append25,nosync25
refuse R_runsh_sha "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture sha "" 0000)" bash "$RS" "$V3" "$W" "$(o R_runsh_sha)" 5 --arms append25,nosync25
refuse R_runsh_fail "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture fail allfail)" bash "$RS" "$V3" "$W" "$(o R_runsh_fail)" 5 --arms append25,nosync25
refuse R_runsh_fs "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture fs "" "$SHA" "$ARCH" tmpfs)" bash "$RS" "$V3" "$W" "$(o R_runsh_fs)" 5 --arms append25,nosync25
refuse R_runsh_arch "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture arch "" "$SHA" sparc)" bash "$RS" "$V3" "$W" "$(o R_runsh_arch)" 5 --arms append25,nosync25
refuse R_runsh_planted "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture planted "")" bash "$RS" "$V3" "$W" "$(o R_runsh_planted)" 5 --arms append25,nosync25
refuse R_runsh_shape "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture shape fail-one)" bash "$RS" "$V3" "$W" "$(o R_runsh_shape)" 5 --arms append25,nosync25
refuse R_runsh_count "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture count drop-one)" bash "$RS" "$V3" "$W" "$(o R_runsh_count)" 5 --arms append25,nosync25
other=xfsloop; [ "$CELL" = xfsloop ] && other=xfs
refuse R_runsh_cellv "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture cellv "cell=$other")" bash "$RS" "$V3" "$W" "$(o R_runsh_cellv)" 5 --arms append25,nosync25
refuse R_runsh_brdenv env -u V3_SMOKE V3FLOOR_BRD=1 V3_FIRECHECK_VERDICT="$(fixture brdenv "")" bash "$RS" "$V3" "$W" "$(o R_runsh_brdenv)" 5 --arms append25,nosync25
refuse R_runsh_mutant env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_mutant)" 200 --arms ow1m,nosync25 --mutant-nosync
refuse R_runsh_traceclock env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_traceclock)" 5 --arms append25,nosync25 --trace-clock
refuse R_runsh_crashop env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_crashop)" 5 --crash-op cfr2b
refuse R_runsh_crashaim env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_crashaim)" 5 --arms append25,nosync25 --crash-aim
refuse R_runsh_dir env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_dir)" 5 --arms append25,nosync25 --dir "$ROOTW"
refuse R_runsh_out env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_out)" 5 --arms append25,nosync25 --out "$OUT/F4/elsewhere"
refuse R_runsh_n env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_n)" 5 --arms append25,nosync25 --n 7
refuse R_runsh_fcenv env V3_SMOKE=1 V3FLOOR_FIRECHECK=1 bash "$RS" "$V3" "$W" "$(o R_runsh_fcenv)" 5 --arms append25,nosync25
refuse R_runsh_nocell env -u V3_CELL V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_nocell)" 5 --arms append25,nosync25
refuse R_runsh_badcell env V3_CELL=bogus V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_badcell)" 5 --arms append25,nosync25
refuse R_runsh_t3 env V3_SMOKE=1 V3_REQUIRE_T3=1 bash "$RS" "$V3" "$W" "$(o R_runsh_t3)" 5 --arms append25,nosync25
# after the run: a wrapper "binary" that runs the real probe with the mutant (summary names another binary)
printf '#!/bin/sh\nV3FLOOR_FIRECHECK=1 exec "%s" "$@" --mutant-nosync\n' "$V3" > "$OUT/F4/wrapper.sh"
chmod +x "$OUT/F4/wrapper.sh"
refuse R_runsh_post env V3_SMOKE=1 bash "$RS" "$OUT/F4/wrapper.sh" "$W" "$(o R_runsh_post)" 5 --arms append25,nosync25

if [ -n "${V3_BASE:-}" ] && [ -x "${V3_BASE}/v3floor" ]; then
  echo "== R: the red column, against the base probe and scripts (${V3_BASE_SHA:-?})"
  R=$OUT/red BB=$V3_BASE/v3floor BR=$V3_BASE/run.sh
  mkdir -p "$R"
  WR="$(dirname "$W")/v3red-work"
  mkdir -p "$WR"
  BSHA=$(sha256sum "$BB" | cut -d' ' -f1)
  printf '{"all_pass": true, "v3floor_sha256": "%s", "fstype": "%s", "arch": "%s"}\n' "$BSHA" "$WFS" "$ARCH" > "$R/base-vfx.json"
  red() { local tag=$1; shift; timeout 600 "$@" > "$R/$tag.txt" 2>&1; echo $? > "$R/$tag.rc"; }
  rfx() { # tag fixture: the base probe on a fixture
    local tag=$1 name=$2 dir=$FX/$2/w
    [ -f "$FX/$name.ok" ] || { echo "fixture $name missing" > "$R/$tag.na"; return; }
    red "$tag" "$BB" --dir "$dir" --out "$R/$tag.out" --n 5 --arms append25,nosync25
  }
  rfx red_1a_brd brd
  rfx red_1a_driver dm
  flip "$R/red_1b_leafflip" red red_1b_leafflip "$BB" --dir "$ROOTW" --out "$R/red_1b_leafflip.out" --n 5 --arms append25,nosync25
  red red_2_devflush env -u V3FLOOR_BRD V3_SMOKE=1 bash "$BR" "$BB" "$WR" "$R/red_2_devflush.out" 20
  if [ "$KIND" = ext4 ]; then
    echo "ext4: clone1b is refused at base too" > "$R/red_3_clone1b_gated.na"
  else
    red red_3_clone1b_gated env V3_SMOKE=1 bash "$BR" "$BB" "$WR" "$R/red_3_clone1b_gated.out" 5 --arms clone1b,nosync25
  fi
  red red_7_mutant env -u V3_SMOKE V3_FIRECHECK_VERDICT="$R/base-vfx.json" bash "$BR" "$BB" "$WR" "$R/red_7_mutant.out" 200 --arms ow1m,nosync25 --mutant-nosync
  red red_7_traceclock env -u V3_SMOKE V3_FIRECHECK_VERDICT="$R/base-vfx.json" bash "$BR" "$BB" "$WR" "$R/red_7_traceclock.out" 5 --arms append25,nosync25 --trace-clock
  red red_7_dir env -u V3_SMOKE V3_FIRECHECK_VERDICT="$R/base-vfx.json" bash "$BR" "$BB" "$WR" "$R/red_7_dir.out" 5 --arms append25,nosync25 --dir "$ROOTW"
  red red_8_planted env -u V3_SMOKE V3_FIRECHECK_VERDICT="$R/base-vfx.json" bash "$BR" "$BB" "$WR" "$R/red_8_planted.out" 5 --arms append25,nosync25
  rfx red_9_loopwt wt
  rfx red_10a_hidden_tmpfs ht
  rfx red_10b_hidden_nobarrier hn
  rfx red_10c_lazy lz
  red red_11_append64 "$BB" --dir "$WR" --out "$R/red_11_append64.out" --n 5 --arms append64,nosync25
  rfx red_12a_deleted del
  rfx red_12b_nest4 n4
  if [ -n "${V3_SHIM:-}" ] && [ -f "$V3_SHIM" ]; then
    red red_12d_shim env LD_PRELOAD="$(readlink -f "$V3_SHIM")" "$BB" --dir "$WR" --out "$R/red_12d_shim.out" --n 5 --arms append25,nosync25
  else
    echo "V3_SHIM missing" > "$R/red_12d_shim.na"
  fi
  rtgt=/dev/shm/v3red-symlink-target.$$
  rm -f "$rtgt"; ln -s "$rtgt" "$WR/ow1m"
  red red_13_symlink "$BB" --dir "$WR" --out "$R/red_13_symlink.out" --n 5 --arms ow1m,nosync25
  if [ -e "$rtgt" ]; then echo present > "$R/red_13_symlink.target"; else echo absent > "$R/red_13_symlink.target"; fi
  rm -f "$WR/ow1m" "$rtgt"
  rfx red_15_dirsync ds
  rfx red_15_logdev ld
  rfx red_15_extjournal ej
  rfx red_15_multidev md
  if [ "$KIND" != ext4 ] && [ "$LOOPCELL" = 0 ]; then
    echo "no ext4 layer on this cell's flush path" > "$R/red_15_fields.na"
  elif [ -d "$R/red_2_devflush.out" ]; then
    cp -r "$R/red_2_devflush.out" "$R/red_15_fields.out"; cp "$R/red_2_devflush.rc" "$R/red_15_fields.rc"
  fi
  if [ "$ARCH" = x86_64 ]; then
    csalt "$R/red_16_clocksource" red red_16_clocksource "$BB" --dir "$WR" --out "$R/red_16_clocksource.out" --n 5 --arms append25,nosync25
  else
    echo "arm64: no clocksource other than arch_sys_counter" > "$R/red_16_clocksource.na"
  fi
  red red_17_t3 env V3_SMOKE=1 V3_REQUIRE_T3=1 bash "$BR" "$BB" "$WR" "$R/red_17_t3.out" 5 --arms append25,nosync25
  # the base binary's own trace (item 14's input for red.py)
  timeout 900 setarch "$ARCH" -R strace -f -y -s 1 -o "$R/base-trace" "$BB" --dir "$WR" --out "$R/base-trace.out" --n 40 \
    --arms append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,fdatasync4k,nosync25 --seed 5 --trace-clock > "$R/base-trace.txt" 2>&1
  echo $? > "$R/base-trace.rc"
  gzip -9 "$R/base-trace"
  ls -A "$WR" > "$R/work-leftover.txt"
  python3 -B "$HERE/red.py" offline "$V3_BASE/check.py" "$R/base-trace.gz" "$WR" "$CELL" "$R/offline.json" > "$R/offline.txt" 2>&1
fi

ls -A "$W" > "$OUT/work-leftover.txt"
python3 -B "$HERE/check.py" "$OUT" "$CELL"
crc=$?
echo "== bind: run.sh bound to this cell's own verdict"
if [ "$BRD" = 1 ]; then
  refuse P_runsh_brd env -u V3FLOOR_BRD -u V3_SMOKE V3_FIRECHECK_VERDICT="$OUT/verdict.json" bash "$RS" "$V3" "$W" "$(o P_runsh_brd)" 5 --arms append25,nosync25
else
  refuse P_runsh_ok env -u V3FLOOR_BRD -u V3_SMOKE V3_FIRECHECK_VERDICT="$OUT/verdict.json" bash "$RS" "$V3" "$W" "$(o P_runsh_ok)" 5 --arms append25,nosync25
fi
python3 -B "$HERE/check.py" --bind "$OUT" "$CELL"
brc=$?
[ "$crc" -eq 0 ] && [ "$brc" -eq 0 ] && exit 0
exit 1
