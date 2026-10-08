#!/usr/bin/env bash
# firecheck.sh V3FLOOR CELL WORKDIR OUT -- fire-check the Linux V3 device-floor probe on one cell.
# NOT a credited measurement. Port of frontier/fastest/tools/v3/firecheck.py, whose F1/F2 ran under the Mac's V1
# shim; here strace is the flush-syscall instrument and blkflush.py the device-flush one.
#   CELL     explicit (v3cell.py): ext4|xfs|btrfs on a block device, ext4loop|xfsloop|btrfsloop on a loop; WORKDIR
#            must be on it. On a brd cell (the work mount's source is /dev/ram*), V3FLOOR_BRD=1 is exported for the
#            probe runs (brd is fire-check only) and unset for every run.sh bound-mode plant.
#   OUT      raw output; must not exist.
# The inputs, all in one place. Build them with `build.sh DIST` (the one build of the probe: fastest-v3.yml and t3run
# both call it). REQUIRED, or firecheck.sh refuses before anything runs (exit 2):
#   V3FLOOR       (arg 1) DIST/v3floor, the static build
#   V3_DYN        DIST/v3floor.dyn, a dynamic build of the same v3floor.c (fourth review M4): no preload reaches the
#                 static build, so the statfs-shim plant and the /etc/ld.so.preload refusal run this one
#   V3_SHIM       DIST/statfs_shim.so, for the R_statfs_shim and R_ldpreload plants
#   V3_NOOP       DIST/noop_shim.so, the library the /etc/ld.so.preload plants name
#   V3_FX         the base dir mkfixtures.sh made (a fixture it could not make: that fixture's plants FAIL in check.py)
#   V3_PLP        yes|no: the leaf drive's power-loss protection, the operator's declaration (annex A14; a hosted
#                 runner and a dry run say no). It reaches the probe (F2b), run.sh (F3, the plants) and check.py (the
#                 plan: F2b:discriminates only for a write-back leaf without PLP).
# OPTIONAL (informational columns, never part of the verdict):
#   V3_BASE, V3_BASE_SHA   the base (df4b39e53) v3floor and run.sh/stamp.py/check.py: the red column (OUT/red/,
#                          red.json)
#   V3_PREV, V3_PREV_SHA   the previous tip's (40a3c9502) v3floor, batchgate.py, check.py, v3cell.py: the second red
#                          column (OUT/prev/, prev.json)
# Also needed on the box: sudo -n, strace, xfsprogs, btrfs-progs, acl, util-linux (setpriv, unshare), systemd-detect-
# virt, and the brd, dm-flakey and scsi_debug modules (mkfixtures.sh installs linux-modules-extra for scsi_debug).
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
#   R2  (V3_PREV set) the fourth review's plants against the previous tip's probe and gate.
#   bind after check.py: run.sh bound to the cell's own verdict (P_runsh_ok), or refused on a brd cell
#       (P_runsh_brd); check.py --bind records it in OUT/verdict.bind.json, the binding record run.sh requires
#       next to a verdict (check.py leaves OUT/verdict.bind.pending until then).
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
case ${V3_PLP:-} in yes|no) export V3_PLP ;; *) echo "firecheck: V3_PLP='${V3_PLP:-}' is not yes or no (see the header)" >&2; exit 2 ;; esac
for need in V3_DYN V3_SHIM V3_NOOP V3_FX; do
  [ -n "${!need:-}" ] && [ -e "${!need}" ] || { echo "firecheck: $need is unset or missing (see the header; build.sh DIST makes the binaries)" >&2; exit 2; }
done
[ -x "$V3_DYN" ] || { echo "firecheck: V3_DYN $V3_DYN is not executable" >&2; exit 2; }
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
V3DYN=$(readlink -f "${V3_DYN:-/nonexistent}") NOOP=$(readlink -f "${V3_NOOP:-/nonexistent}")
export V3_CELL=$CELL
BRD=0
case $WSRC in /dev/ram*) BRD=1; export V3FLOOR_BRD=1 ;; esac
CS=/sys/devices/system/clocksource/clocksource0
# the whole disk under a block device MAJ:MIN, through partitions and the first dm or md member (sixth review H1)
disk_of() {
  local p s
  p=$(readlink -f "/sys/dev/block/$1" 2>/dev/null) || return 1
  [ -d "$p" ] || return 1
  [ -f "$p/partition" ] && p=$(dirname "$p")
  s=$(ls "$p/slaves" 2>/dev/null | head -1)
  if [ -n "$s" ]; then disk_of "$(cat "/sys/class/block/$s/dev")"; return; fi
  basename "$p"
}
ROOTSRC=$(findmnt -n --nofsroot -o SOURCE /)
ROOTDISK=$(disk_of "$(findmnt -n -o MAJ:MIN /)")
[ -n "$ROOTDISK" ] || ROOTDISK=$(lsblk -no PKNAME "$ROOTSRC" 2>/dev/null | head -1)
[ -n "$ROOTDISK" ] || ROOTDISK=$(basename "$ROOTSRC")

# The probe reads LOOP_GET_STATUS64 on every loop of a flush path, NVMe Identify on the leaf's controller and SCSI
# MODE SENSE on an sd leaf, as the calling user: grant read access (an ACL) to those nodes.
grant() {
  local d
  for d in /dev/loop[0-9]* /dev/nvme[0-9] /dev/sd[a-z] /dev/vd[a-z]; do
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
  for d in /dev/loop[0-9]* /dev/nvme[0-9] /dev/sd[a-z]; do [ -e "$d" ] && echo "acl $d $(getfacl -cp "$d" 2>/dev/null | grep "^user:$ME" | xargs)"; done
  # an instrument outside the probe for its virtualization verdict (fourth review M1)
  echo "detect_virt=$(systemd-detect-virt 2>/dev/null)"
  echo "plp=$V3_PLP"
  echo "scsi_hosts=$(for h in /sys/class/scsi_host/host*; do printf '%s:%s ' "${h##*/}" "$(cat "$h/proc_name" 2>/dev/null)"; done)"
  echo "v3dyn_sha256=$(sha256sum "$V3DYN" 2>/dev/null | cut -d' ' -f1) noop_sha256=$(sha256sum "$NOOP" 2>/dev/null | cut -d' ' -f1)"
  echo "prev_sha=${V3_PREV_SHA:-}"
  [ -n "${V3_PREV:-}" ] && echo "prev_v3floor_sha256=$(sha256sum "$V3_PREV/v3floor" | cut -d' ' -f1)"
  echo "ldso_preload_before=$(cat /etc/ld.so.preload 2>/dev/null | xargs)"
} > "$OUT/info.txt" 2>&1
# the harness as it is now; check.py compares it with the harness at check time (fourth review L9)
python3 -B -c 'import json, sys; sys.path.insert(0, sys.argv[1]); import check; print(json.dumps(check.harness_sha256(sys.argv[1]), indent=1, sort_keys=True))' "$HERE" > "$OUT/harness_start.json"
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
# The leaf plants run on the cell's own leaf disk, in a directory beside W on the cell's filesystem (LEAFW); on a brd
# cell, whose leaf is RAM, on the root disk (ROOTW, a dir on the root filesystem). Sixth review H1: on a T3 box the
# cell's leaf is the data drive, and the root may be md or LVM, which the probe refuses.
if [ "$BRD" = 1 ]; then
  LEAFDISK=$ROOTDISK
else
  LEAFDISK=$(python3 -B -c 'import json, sys; print(json.load(open(sys.argv[1]))["flush_path"][-1]["disk"])' "$OUT/F1/nosync25.n1.out/summary.json" 2>/dev/null)
fi
echo "leafdisk=${LEAFDISK:-?} write_cache=[$(cat "/sys/block/$LEAFDISK/queue/write_cache" 2>/dev/null)] scsi=$([ -d "/sys/block/$LEAFDISK/device/scsi_disk" ] && echo 1 || echo 0)" >> "$OUT/info.txt"
BOX=$(python3 -B "$HERE/check.py" --box "$OUT")
BOXSPEC=$(python3 -B -c 'import json, sys; b = json.loads(sys.argv[1]); print("virt=%s,flip=%s,plp=%s" % (b["virt"], b["flip"], b["plp"]))' "$BOX")
echo "box=$BOXSPEC" >> "$OUT/info.txt"
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
FC timeout 900 "$V3" --dir "$W" --out "$OUT/F2b.out" --n 200 --arms "$ALL" --seed 11 --mutant-nosync --plp "$V3_PLP" \
  --registered "$HERE/REGISTERED.tsv" > "$OUT/F2b.txt" 2>&1
echo $? > "$OUT/F2b.rc"
echo "== F3: the real probe, n=200, through run.sh"
V3_SMOKE=1 timeout 900 bash "$RS" "$V3" "$W" "$OUT/F3" 200 --arms "$ALL" --seed 11 > "$OUT/F3.txt" 2>&1
echo $? > "$OUT/F3.rc"
# gate-6 review LOW 10: what the device-flush tracing costs. The same arms, n and seed with nothing traced (the probe
# directly, no run.sh, no tracefs), recorded beside F3's traced p50s in the verdict; descriptive, never a gate
timeout 900 "$V3" --dir "$W" --out "$OUT/T.out" --n 200 --arms "$ALL" --seed 11 --plp "$V3_PLP" \
  --registered "$HERE/REGISTERED.tsv" > "$OUT/T.txt" 2>&1
echo $? > "$OUT/T.rc"

if [ "$LOOPCELL" = 1 ]; then
  echo "== C: crash arms ($KIND)"
  timeout 900 bash "$HERE/crash.sh" "$V3" "$KIND" "$OUT/crash" > "$OUT/crash.txt" 2>&1
  echo $? > "$OUT/crash.rc"
  grant
fi

echo "== B: blkflush.py's own fire-check"
B=$OUT/B
python3 -B "$BLK" self-test > "$B/selftest.txt" 2>&1
echo $? > "$B/selftest.rc"
python3 -B "$GATE" self-test "$HERE/testdata" > "$B/batchgate-selftest.txt" 2>&1
echo $? > "$B/batchgate-selftest.rc"
python3 -B "$HERE/check.py" --self-test > "$B/check-selftest.txt" 2>&1
echo $? > "$B/check-selftest.rc"
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
# ninth review L13, M6, M7, L9: the probe's own registration and rental refusals, each on a planted --registered file
printf '# planted: a threshold, no frame arm\nd0_threshold/planted/wb/vm\t9.5\tplanted\n' > "$OUT/F4/reg_noframe.tsv"
printf 'frame_arm\tow64k\tplanted\n' > "$OUT/F4/reg_ow64k.tsv"
printf 'frame_arm\tappend25\tplanted\n' > "$OUT/F4/reg_frame25.tsv"
printf 'frame_arm\tow4k\tDECISIONS \xe2\x80\xa6 (PREREG \xc2\xa74)\n' > "$OUT/F4/reg_nonascii.tsv"
{ printf '#%.0s' $(seq 600); printf '\nframe_arm\tow4k\tplanted\n'; } > "$OUT/F4/reg_longline.tsv"
refuse R_rental_noreg "$V3" --dir "$W" --out "$(o R_rental_noreg)" --n 5 --arms append25,nosync25 --plp no --require-registered
for t in noframe:reg_noframe novariant:reg_ow64k; do
  refuse "R_rental_${t%%:*}" "$V3" --dir "$W" --out "$(o "R_rental_${t%%:*}")" --n 5 --arms append25,nosync25 --plp no \
    --registered "$OUT/F4/${t#*:}.tsv" --require-registered
done
for t in frame25 nonascii longline; do
  refuse "R_reg_$t" "$V3" --dir "$W" --out "$(o "R_reg_$t")" --n 5 --arms append25,nosync25 --plp no --registered "$OUT/F4/reg_$t.tsv"
done
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
fx R_datajournal dj
# fourth review M2: a write-back SCSI drive (scsi_debug, WCE=1): accepted only under the fire-check flag, and the
# kernel's copy flipped to "temporary write through" against the drive's WCE=1 refuses
fx P_sdbg_wb sdbg env V3FLOOR_FIRECHECK=1
fx R_sdbg_noenv sdbg env -u V3FLOOR_FIRECHECK
SDBG=$(cat "$FX/sdbg.disk" 2>/dev/null)
if [ -f "$FX/sdbg.ok" ] && [ -n "$SDBG" ]; then
  sct=$(ls /sys/block/"$SDBG"/device/scsi_disk/*/cache_type 2>/dev/null | head -1)
  swc=/sys/block/$SDBG/queue/write_cache
  sbefore=$(cat "$swc")
  echo "temporary write through" | sudo tee "$sct" > /dev/null
  sduring=$(cat "$swc")
  [ "$sduring" != "$sbefore" ] && echo "changed=1 $SDBG write_cache '$sbefore' -> '$sduring' (cache_type $(cat "$sct"))" > "$OUT/F4/R_sdbg_flip.state"
  refuse R_sdbg_flip env V3FLOOR_FIRECHECK=1 "$V3" --dir "$FX/sdbg/w" --out "$(o R_sdbg_flip)" --n 5 --arms append25,nosync25
  echo "temporary write back" | sudo tee "$sct" > /dev/null
  echo "$SDBG write_cache: before '$sbefore', during '$sduring', after '$(cat "$swc")'" > "$OUT/F4/R_sdbg_flip.flip"
else
  echo "fixture sdbg missing" > "$OUT/F4/R_sdbg_flip.txt"; echo missing > "$OUT/F4/R_sdbg_flip.rc"
fi
if [ -n "${V3_SHIM:-}" ] && [ -f "$V3_SHIM" ]; then
  # the shim is a fire-check tool, so the probe's own LD_PRELOAD refusal is lifted for it (V3FLOOR_FIRECHECK=1) ...
  # (on the dynamic build of the same source: the static V3FLOOR loads no preload at all)
  refuse R_statfs_shim env V3FLOOR_FIRECHECK=1 LD_PRELOAD="$(readlink -f "$V3_SHIM")" "$V3DYN" --dir "$W" --out "$(o R_statfs_shim)" --n 5 --arms append25,nosync25
  # ... and without it the preload itself refuses (fresh review: an interposer under an unchanged exe_sha256)
  refuse R_ldpreload env -u V3FLOOR_FIRECHECK LD_PRELOAD="$(readlink -f "$V3_SHIM")" "$V3" --dir "$W" --out "$(o R_ldpreload)" --n 5 --arms append25,nosync25
else
  for t in R_statfs_shim R_ldpreload; do echo "V3_SHIM missing" > "$OUT/F4/$t.txt"; echo missing > "$OUT/F4/$t.rc"; done
fi
# fresh review M4: a per-file sync attribute on D (chattr +S) changes what every fsync does, invisibly to mount options
CD="$(dirname "$W")/v3chattr-work"
mkdir -p "$CD" && sudo chattr +S "$CD"
echo "chattr_dir=$CD $(lsattr -d "$CD" 2>&1)" >> "$OUT/info.txt"
refuse R_chattr "$V3" --dir "$CD" --out "$(o R_chattr)" --n 5 --arms append25,nosync25
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
LEAFW=$ROOTW
if [ "$BRD" = 0 ]; then
  LEAFW="$(dirname "$W")/v3leaf-work"
  mkdir -p "$LEAFW"
  # the leaf plants' dir must be on the cell's own filesystem (W's), or they test another disk (seventh review L2)
  [ "$(stat -c %d "$LEAFW")" = "$(stat -c %d "$W")" ] || { echo "firecheck: $LEAFW is not on W's filesystem" >&2; exit 2; }
fi
echo "leafw=$LEAFW" >> "$OUT/info.txt"
# item 1(b): the kernel's view of the root disk's cache made to disagree with the drive, then restored. A write-back
# disk: queue/write_cache set to write through. A write-through sd disk (the hosted runners' sda): sd's
# "temporary write back", which moves sd's cache_type and queue/write_cache together and sends the drive nothing --
# the override the old cache_type comparison could not see (fresh review H2); MODE SENSE still reads the drive.
flip() { # tag cmd...
  local tag=$1 wc="/sys/block/$LEAFDISK/queue/write_cache" before during after ct=""
  shift
  before=$(cat "$wc" 2>/dev/null)
  ct=$(ls /sys/block/"$LEAFDISK"/device/scsi_disk/*/cache_type 2>/dev/null | head -1)
  if [ "$before" = "write back" ]; then
    echo "write through" | sudo tee "$wc" > /dev/null
  elif [ -n "$ct" ]; then
    echo "temporary write back" | sudo tee "$ct" > /dev/null
  else
    echo "not planted: the leaf disk $LEAFDISK reads '$before' and is not sd" > "$tag.na"
    return
  fi
  during=$(cat "$wc")
  [ "$during" != "$before" ] && echo "changed=1 $LEAFDISK write_cache '$before' -> '$during'" > "$tag.state"
  "$@"
  if [ "$before" = "write back" ]; then echo "write back" | sudo tee "$wc" > /dev/null
  else echo "temporary write through" | sudo tee "$ct" > /dev/null; fi
  after=$(cat "$wc")
  echo "$LEAFDISK write_cache: before '$before', during '$during', after '$after'" >> "$tag.flip"
}
flip "$OUT/F4/R_leaf_flip" refuse R_leaf_flip "$V3" --dir "$LEAFW" --out "$(o R_leaf_flip)" --n 5 --arms append25,nosync25
# fourth review M1, fifth review M1/M2/L6: what the probe reads from /proc and /sys planted by bind mounts in a
# private mount namespace (nsfake.sh: the premise read back inside it before the probe starts; the probe runs as this
# user). HIDE hides cpuinfo's hypervisor flag and DMI's names.
FAKE=$OUT/F4/fake
mkdir -p "$FAKE"
sed -E 's/ hypervisor( |$)/\1/' /proc/cpuinfo > "$FAKE/cpuinfo"
echo "Dell Inc." > "$FAKE/sys_vendor"
echo "PowerEdge R650" > "$FAKE/product_name"
echo tcm_loopback > "$FAKE/proc_name"
echo tcp > "$FAKE/transport"
# planted: a hypervisor flag in cpuinfo (x86 flags line, arm64 Features line) and a VM in DMI
sed -E '/^(flags|Features)[[:space:]]*:/ s/$/ hypervisor/' /proc/cpuinfo > "$FAKE/cpuinfo-hv"
echo QEMU > "$FAKE/sys_vendor-vm"
echo "Virtual Machine" > "$FAKE/product_name-vm"
HIDE=("$FAKE/cpuinfo:/proc/cpuinfo" "$FAKE/sys_vendor:/sys/class/dmi/id/sys_vendor" "$FAKE/product_name:/sys/class/dmi/id/product_name")
nsrun() { # prefix SRC:DST... -- cmd...: prefix.premise, prefix.txt, prefix.rc
  local pre=$1
  shift
  timeout 300 sudo unshare -m --propagation private bash "$HERE/nsfake.sh" "$pre.premise" "$(id -u)" "$(id -g)" "$@" > "$pre.txt" 2>&1
  echo $? > "$pre.rc"
}
# the leaf alone (driver, model, host path) must still show the VM (a VM box only: check.py plans it from the box)
case $BOXSPEC in
  virt=vm,*) nsrun "$OUT/F4/R_virt_hidden" "${HIDE[@]}" -- "$V3" --dir "$LEAFW" --out "$(o R_virt_hidden)" --n 5 --arms append25,nosync25 ;;
esac
# a leaf with no VM evidence of its own (scsi_debug), cpuinfo and DMI hidden: bare metal on x86_64 (the CPUID bit is
# the positive evidence), not ruled out on arm64 -- the only CI run of the plain and the null labels (fifth review L6)
# then the same leaf with WCE cleared by MODE SELECT (sd's non-temporary cache_type write; scsi_debug's caching page
# is changeable) and read back, restored after: the write-through labels (sixth review M2)
# and, on any box, a hypervisor flag planted in cpuinfo and a VM in DMI on that leaf: each detector fires on its own
# (sixth review H1: the only VM plant a bare-metal box can run)
SDBG=$(cat "$FX/sdbg.disk" 2>/dev/null)
if [ -f "$FX/sdbg.ok" ] && [ -n "$SDBG" ]; then
  nsrun "$OUT/F4/P_virt_bare" "${HIDE[@]}" -- env V3FLOOR_FIRECHECK=1 "$V3" --dir "$FX/sdbg/w" --out "$(o P_virt_bare)" --n 5 --arms append25,nosync25
  nsrun "$OUT/F4/R_virt_planted" "$FAKE/cpuinfo-hv:/proc/cpuinfo" "$FAKE/sys_vendor-vm:/sys/class/dmi/id/sys_vendor" \
    "$FAKE/product_name-vm:/sys/class/dmi/id/product_name" -- env V3FLOOR_FIRECHECK=1 "$V3" --dir "$FX/sdbg/w" --out "$(o R_virt_planted)" --n 5 --arms append25,nosync25
  # gate-6 review v3 #1 (A14): the no-fsync mutant (the fastest "fsync" there is) on the write-back scsi_debug leaf:
  # without PLP the timing control voids it, declared PLP it is not applicable
  tcrun() { # tag plp
    timeout 300 env V3FLOOR_FIRECHECK=1 "$V3" --dir "$FX/sdbg/w" --out "$(o "$1")" --n 100 --arms append25,nosync25 \
      --mutant-nosync --plp "$2" --registered "$HERE/REGISTERED.tsv" > "$OUT/F4/$1.txt" 2>&1
    echo $? > "$OUT/F4/$1.rc"
  }
  tcrun R_tc_wb no
  tcrun P_tc_plp yes
  sct=$(ls /sys/block/"$SDBG"/device/scsi_disk/*/cache_type 2>/dev/null | head -1)
  echo "write through" | sudo tee "$sct" > /dev/null
  sudo udevadm settle 2>/dev/null
  [ "$(cat "$sct")" = "write through" ] && [ "$(cat "/sys/block/$SDBG/queue/write_cache")" = "write through" ] \
    && echo "changed=1 $SDBG cache_type and write_cache 'write through' (MODE SELECT WCE=0)" > "$OUT/F4/P_virt_bare_wt.state"
  nsrun "$OUT/F4/P_virt_bare_wt" "${HIDE[@]}" -- env V3FLOOR_FIRECHECK=1 "$V3" --dir "$FX/sdbg/w" --out "$(o P_virt_bare_wt)" --n 5 --arms append25,nosync25
  # ... and on the write-through leaf the timing control is not applicable (A14), the state read back again
  [ "$(cat "$sct")" = "write through" ] && echo "changed=1 $SDBG cache_type 'write through'" > "$OUT/F4/P_tc_wt.state"
  tcrun P_tc_wt no
  echo "write back" | sudo tee "$sct" > /dev/null
  echo "$SDBG cache_type after restore: $(cat "$sct")" > "$OUT/F4/P_virt_bare_wt.restore"
else
  for t in P_virt_bare R_virt_planted P_virt_bare_wt R_tc_wb P_tc_plp P_tc_wt; do echo "fixture sdbg missing" > "$OUT/F4/$t.txt"; echo missing > "$OUT/F4/$t.rc"; done
fi
# the leaf disk made remote: an sd leaf's SCSI host named tcm_loopback, or an NVMe controller's transport tcp
# (fifth review M1, M2: both outside their allowlists). An NVMe multipath head's device link is its subsystem: its
# first controller is faked (the probe reads every controller's transport).
RD=/sys/block/$LEAFDISK
rctrl=$(basename "$(readlink -f "$RD/device")")
case $rctrl in nvme-subsys*) rctrl=$(ls "$(readlink -f "$RD/device")" | grep -E '^nvme[0-9]+$' | head -1) ;; esac
if [ -d "$RD/device/scsi_disk" ]; then
  rhost=$(readlink -f "$RD/device" | grep -o '/host[0-9]*/' | head -1 | tr -d /)
  echo "faked=sd_host $rhost" > "$OUT/F4/R_leaf_remote.what"
  nsrun "$OUT/F4/R_leaf_remote" "$FAKE/proc_name:/sys/class/scsi_host/$rhost/proc_name" -- "$V3" --dir "$LEAFW" --out "$(o R_leaf_remote)" --n 5 --arms append25,nosync25
elif [ -n "$rctrl" ] && [ -f "/sys/class/nvme/$rctrl/transport" ]; then
  echo "faked=nvme_transport $rctrl" > "$OUT/F4/R_leaf_remote.what"
  nsrun "$OUT/F4/R_leaf_remote" "$FAKE/transport:/sys/class/nvme/$rctrl/transport" -- "$V3" --dir "$LEAFW" --out "$(o R_leaf_remote)" --n 5 --arms append25,nosync25
else
  echo "the leaf disk $LEAFDISK is neither sd nor nvme" > "$OUT/F4/R_leaf_remote.txt"; echo missing > "$OUT/F4/R_leaf_remote.rc"
fi
ls -A "$LEAFW" > "$OUT/leafw-leftover.txt" 2>&1
# fourth review M4: /etc/ld.so.preload names the noop library for one run, then is restored
ldso() { # tag binary dir
  local tag=$1 bin=$2 dir=$3
  : > "$dir/$tag.mark"
  [ -e /etc/ld.so.preload ] && sudo cp /etc/ld.so.preload "$dir/$tag.ldso-before"
  if [ -f "$NOOP" ]; then
    echo "$NOOP" | sudo tee /etc/ld.so.preload > /dev/null
    [ "$(cat /etc/ld.so.preload)" = "$NOOP" ] && echo "changed=1 /etc/ld.so.preload names $NOOP" > "$dir/$tag.state"
  fi
  timeout 300 env V3_NOOP_MARK="$dir/$tag.mark" "$bin" --dir "$W" --out "$dir/$tag.out" --n 5 --arms append25,nosync25 > "$dir/$tag.txt" 2>&1
  echo $? > "$dir/$tag.rc"
  if [ -f "$dir/$tag.ldso-before" ]; then sudo cp "$dir/$tag.ldso-before" /etc/ld.so.preload; else sudo rm -f /etc/ld.so.preload; fi
}
ldso R_ldso_preload "$V3DYN" "$OUT/F4"
ldso P_ldso_static "$V3" "$OUT/F4"
echo "ldso_preload_after=$(cat /etc/ld.so.preload 2>/dev/null | xargs)" >> "$OUT/info.txt"
# item 16: a clocksource other than tsc/arch_sys_counter, then restored
csalt() { # tag cmd...
  local tag=$1 cur alt
  shift
  cur=$(cat $CS/current_clocksource)
  alt=$(tr ' ' '\n' < $CS/available_clocksource | grep -v -x -e "$cur" -e tsc -e arch_sys_counter -e '' | head -1)
  if [ -z "$alt" ]; then echo "not planted: no clocksource other than $cur available" > "$tag.na"; return; fi
  echo "$alt" | sudo tee $CS/current_clocksource > /dev/null
  [ "$(cat $CS/current_clocksource)" = "$alt" ] && echo "changed=1 clocksource $cur -> $alt" > "$tag.state"
  "$@"
  echo "$cur" | sudo tee $CS/current_clocksource > /dev/null
  echo "clocksource: $cur -> $alt -> $(cat $CS/current_clocksource)" >> "$tag.cs"
}
csalt "$OUT/F4/R_clocksource" refuse R_clocksource "$V3" --dir "$W" --out "$(o R_clocksource)" --n 5 --arms append25,nosync25

echo "== F4: run.sh: the argument allowlist, the environment, the verdict binding"
# The binding plants use a drive-leaf fixture on a brd cell too: a brd verdict is refused by its own rule (shown by
# P_runsh_brd and batchgate's self-test), which would otherwise mask the plan rule (run 37515067945: R_runsh_count).
FIXLEAF=$LEAF
[ "$LEAF" = brd ] && FIXLEAF=wt
fixture() { # name mod -> a planted full-shape verdict (batchgate.py fixture), path on stdout
  python3 -B "$GATE" fixture "$OUT/F4/verdict-$1.json" "$CELL" "${4:-$ARCH}" "$FIXLEAF" "${3:-$SHA}" "${5:-$WFS}" "$BOXSPEC" ${2:+"$2"}
  echo "$OUT/F4/verdict-$1.json"
}
NB=(env -u V3FLOOR_BRD -u V3_SMOKE -u V3_BIND_PENDING_SHA)
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
refuse R_runsh_noplp env -u V3_PLP V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_noplp)" 5 --arms append25,nosync25
# ninth review L13: this cell's own kind under the other layout's name (block <-> loop) refuses before the probe runs
if [ "${CELL%loop}" != "$CELL" ]; then otherlayout=${CELL%loop}; else otherlayout=${CELL}loop; fi
refuse R_runsh_celllayout env V3_CELL="$otherlayout" V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_celllayout)" 5 --arms append25,nosync25
# item 17: V3_REQUIRE_T3=1 on a box where the T3 rule is false. Some x86 runners expose cpufreq with every CPU on
# "performance" (run 37475543956), where the rule holds: there it is shown accepting (P_runsh_t3, recorded) and then
# made false for the plant by moving cpu0 to another governor, restored after.
GOV0=/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
T3HOLDS=0
python3 -B "$GATE" t3pre > "$OUT/F4/t3pre.json" 2>&1 && T3HOLDS=1
echo "t3_rule_holds=$T3HOLDS" >> "$OUT/info.txt"
t3false() { # tag cmd... -- run cmd while the T3 rule is false (moving cpu0 off "performance" when it holds)
  local tag=$1 alt
  shift
  if [ "$T3HOLDS" = 0 ]; then "$@"; return; fi
  alt=$(tr ' ' '\n' < "${GOV0%/*}/scaling_available_governors" | grep -v -x -e performance -e '' | head -1)
  if [ -z "$alt" ]; then echo "only 'performance' is available: the T3 rule cannot be made false here" > "$tag.na"; return; fi
  echo "$alt" | sudo tee "$GOV0" > /dev/null
  "$@"
  echo performance | sudo tee "$GOV0" > /dev/null
  echo "cpu0 governor: performance -> $alt -> $(cat "$GOV0")" > "$tag.gov"
}
[ "$T3HOLDS" = 1 ] && refuse P_runsh_t3 env V3_SMOKE=1 V3_REQUIRE_T3=1 bash "$RS" "$V3" "$W" "$(o P_runsh_t3)" 5 --arms append25,nosync25
t3false "$OUT/F4/R_runsh_t3" refuse R_runsh_t3 env V3_SMOKE=1 V3_REQUIRE_T3=1 bash "$RS" "$V3" "$W" "$(o R_runsh_t3)" 5 --arms append25,nosync25
[ -f "$OUT/F4/R_runsh_t3.na" ] && { cp "$OUT/F4/R_runsh_t3.na" "$OUT/F4/R_runsh_t3.txt"; echo missing > "$OUT/F4/R_runsh_t3.rc"; }
# fresh review I-H2 and I-M2: a stale harness, a library preload, no gated arm
refuse R_runsh_harness "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture harness harness)" bash "$RS" "$V3" "$W" "$(o R_runsh_harness)" 5 --arms append25,nosync25
# fourth review L3: the fire-check's own binding record failed for this verdict, or is absent
refuse R_runsh_bindfail "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture bindfail bindfail)" bash "$RS" "$V3" "$W" "$(o R_runsh_bindfail)" 5 --arms append25,nosync25
refuse R_runsh_nobind "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture nobind "")" bash "$RS" "$V3" "$W" "$(o R_runsh_nobind)" 5 --arms append25,nosync25
# fifth review L1: a pending record outside the bind step (V3_BIND_PENDING_SHA unset) binds nothing
refuse R_runsh_pending "${NB[@]}" V3_FIRECHECK_VERDICT="$(fixture pending pending)" bash "$RS" "$V3" "$W" "$(o R_runsh_pending)" 5 --arms append25,nosync25
if [ -n "${V3_SHIM:-}" ] && [ -f "$V3_SHIM" ]; then
  refuse R_runsh_ldpreload env V3_SMOKE=1 LD_PRELOAD="$(readlink -f "$V3_SHIM")" bash "$RS" "$V3" "$W" "$(o R_runsh_ldpreload)" 5 --arms append25,nosync25
else
  echo "V3_SHIM missing" > "$OUT/F4/R_runsh_ldpreload.txt"; echo missing > "$OUT/F4/R_runsh_ldpreload.rc"
fi
refuse R_runsh_nogated env V3_SMOKE=1 bash "$RS" "$V3" "$W" "$(o R_runsh_nogated)" 5 --arms clean,nosync25
# fresh review I-H1: a batch gate that fails (here a stub batchgate.py in a copy of the harness exiting 1) is a
# refusal, never the probe's rc 0
GH="$OUT/F4/gatecrash-harness"
cp -r "$HERE" "$GH" && printf 'import sys\nsys.exit(1 if sys.argv[1:2] == ["post"] else 2)\n' > "$GH/batchgate.py"
refuse R_runsh_gatecrash env V3_SMOKE=1 bash "$GH/run.sh" "$V3" "$W" "$(o R_runsh_gatecrash)" 5 --arms append25,nosync25
# fresh review I-M5, fourth review M3/M5/L9: batchgate.py post on copies of the F3 batch, each with one field
# planted (bound mode), and the control with nothing planted
for p in traceclock cell leaf brd stack driver virt verdictswap verdictbad nostamp noblk blkrefused leafkind model \
         nofsync wtflush wtmismatch plp unregistered; do
  refuse "R_post_$p" python3 -B "$HERE/postplant.py" "$OUT/F3" "$OUT/F4/R_post_$p" "$CELL" "$SHA" "$p"
done
# MED 3: a write-back layer 0 (a loop cell's loop, or a write-back leaf) that lost one window's flush-carrying request
if [ "$LOOPCELL" = 1 ] || [ "$LEAF" = wb ]; then
  refuse R_post_layerflush python3 -B "$HERE/postplant.py" "$OUT/F3" "$OUT/F4/R_post_layerflush" "$CELL" "$SHA" layerflush
fi
refuse P_post_none python3 -B "$HERE/postplant.py" "$OUT/F3" "$OUT/F4/P_post_none" "$CELL" "$SHA" none
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
    [ -f "$FX/$name.dir" ] && dir=$(cat "$FX/$name.dir")
    [ -f "$FX/$name.ok" ] || { echo "fixture $name missing" > "$R/$tag.na"; return; }
    red "$tag" "$BB" --dir "$dir" --out "$R/$tag.out" --n 5 --arms append25,nosync25
  }
  rfx red_1a_brd brd
  rfx red_1a_driver dm
  RLW="$(dirname "$W")/v3red-leaf"
  [ "$BRD" = 1 ] && RLW="$(dirname "$ROOTW")/v3red-leaf"
  mkdir -p "$RLW"
  flip "$R/red_1b_leafflip" red red_1b_leafflip "$BB" --dir "$RLW" --out "$R/red_1b_leafflip.out" --n 5 --arms append25,nosync25
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
  cp "$FX/lz.decoy" "$R/red_10c_lazy.decoy" 2>/dev/null
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
  red red_15_chattr "$BB" --dir "$CD" --out "$R/red_15_chattr.out" --n 5 --arms append25,nosync25
  t3false "$R/red_17_t3" red red_17_t3 env V3_SMOKE=1 V3_REQUIRE_T3=1 bash "$BR" "$BB" "$WR" "$R/red_17_t3.out" 5 --arms append25,nosync25
  # the base binary's own trace (item 14's input for red.py)
  timeout 900 setarch "$ARCH" -R strace -f -y -s 1 -o "$R/base-trace" "$BB" --dir "$WR" --out "$R/base-trace.out" --n 40 \
    --arms append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,fdatasync4k,nosync25 --seed 5 --trace-clock > "$R/base-trace.txt" 2>&1
  echo $? > "$R/base-trace.rc"
  gzip -9 "$R/base-trace"
  ls -A "$WR" > "$R/work-leftover.txt"
  python3 -B "$HERE/red.py" offline "$V3_BASE/check.py" "$R/base-trace.gz" "$WR" "$CELL" "$R/offline.json" > "$R/offline.txt" 2>&1
fi

if [ -n "${V3_PREV:-}" ] && [ -x "${V3_PREV}/v3floor" ]; then
  echo "== R2: the fourth review's plants against the previous tip (${V3_PREV_SHA:-?})"
  R2=$OUT/prev PB=$V3_PREV/v3floor
  mkdir -p "$R2"
  pv() { local tag=$1; shift; timeout 300 "$@" > "$R2/$tag.txt" 2>&1; echo $? > "$R2/$tag.rc"; }
  PLW="$(dirname "$W")/v3prev-leaf"
  [ "$BRD" = 1 ] && PLW="$(dirname "$ROOTW")/v3prev-leaf"
  mkdir -p "$PLW"
  nsrun "$R2/prev_M1_virt" "${HIDE[@]}" -- "$PB" --dir "$PLW" --out "$R2/prev_M1_virt.out" --n 5 --arms append25,nosync25
  if [ -f "$FX/sdbg.ok" ]; then
    pv prev_M2_sdbg env -u V3FLOOR_FIRECHECK "$PB" --dir "$FX/sdbg/w" --out "$R2/prev_M2_sdbg.out" --n 5 --arms append25,nosync25
  else
    echo "fixture sdbg missing" > "$R2/prev_M2_sdbg.na"
  fi
  pv prev_M3_driver python3 -B "$HERE/postplant.py" "$OUT/F3" "$R2/prev_M3_driver" "$CELL" "$SHA" driver "$V3_PREV/batchgate.py"
  ldso prev_M4_preload "$PB" "$R2"
  pv prev_L1_claim python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import batchgate as b; print(b.claim_from_counts({"leaf_write_cache": "write back", "leaf": {"kind": "drive"}, "fstype": "ext4"}, {"append25": 1.0, "nosync25": 0.0}))' "$V3_PREV"
  if [ -f "$FX/dj.ok" ]; then
    pv prev_L6_datajournal "$PB" --dir "$FX/dj/w" --out "$R2/prev_L6_datajournal.out" --n 5 --arms append25,nosync25
  else
    echo "fixture dj missing" > "$R2/prev_L6_datajournal.na"
  fi
  pv prev_L9_verdictswap python3 -B "$HERE/postplant.py" "$OUT/F3" "$R2/prev_L9_verdictswap" "$CELL" "$SHA" verdictswap "$V3_PREV/batchgate.py"
fi

sudo chattr -S "$CD" 2>/dev/null; rmdir "$CD" 2>/dev/null
[ "$LEAFW" != "$ROOTW" ] && rm -rf "$LEAFW"
ls -A "$W" > "$OUT/work-leftover.txt"
python3 -B "$HERE/check.py" "$OUT" "$CELL"
crc=$?
echo "== bind: run.sh bound to this cell's own verdict (the bind step names it: V3_BIND_PENDING_SHA)"
VSHA=$(sha256sum "$OUT/verdict.json" 2>/dev/null | cut -d' ' -f1)
if [ "$BRD" = 1 ]; then
  refuse P_runsh_brd env -u V3FLOOR_BRD -u V3_SMOKE V3_BIND_PENDING_SHA="$VSHA" V3_FIRECHECK_VERDICT="$OUT/verdict.json" bash "$RS" "$V3" "$W" "$(o P_runsh_brd)" 5 --arms append25,nosync25
else
  BSHAPE=append25,fdatasync4k,nosync25
  bframe=$(awk -F '\t' '$1 == "frame_arm" { v = $2 } END { print v }' "$HERE/REGISTERED.tsv")
  [ -n "$bframe" ] && BSHAPE="$BSHAPE,$bframe"
  timeout 1800 env -u V3FLOOR_BRD -u V3_SMOKE V3_BIND_PENDING_SHA="$VSHA" V3_FIRECHECK_VERDICT="$OUT/verdict.json" bash "$RS" "$V3" "$W" "$(o P_runsh_ok)" 10000 --arms "$BSHAPE" > "$OUT/F4/P_runsh_ok.txt" 2>&1
  echo $? > "$OUT/F4/P_runsh_ok.rc"
  refuse R_runsh_boundshape env -u V3FLOOR_BRD -u V3_SMOKE V3_BIND_PENDING_SHA="$VSHA" V3_FIRECHECK_VERDICT="$OUT/verdict.json" bash "$RS" "$V3" "$W" "$(o R_runsh_boundshape)" 5 --arms "$BSHAPE"
  # ... and the other half of the shape: N=10000 with an arm missing (eighth review L4)
  refuse R_runsh_boundarms env -u V3FLOOR_BRD -u V3_SMOKE V3_BIND_PENDING_SHA="$VSHA" V3_FIRECHECK_VERDICT="$OUT/verdict.json" bash "$RS" "$V3" "$W" "$(o R_runsh_boundarms)" 10000 --arms append25,nosync25
fi
python3 -B "$HERE/check.py" --bind "$OUT" "$CELL"
brc=$?
[ "$crc" -eq 0 ] && [ "$brc" -eq 0 ] && exit 0
exit 1
