#!/usr/bin/env bash
# firecheck.sh V3FLOOR FS WORKDIR OUT -- fire-check the Linux V3 device-floor probe on one filesystem.
# NOT a credited measurement. Port of frontier/fastest/tools/v3/firecheck.py, whose F1/F2 ran under the Mac's V1
# shim; here strace is the instrument.
#   FS       the cell, ext4|ext4loop|xfs|btrfs (the workflow matrix); WORKDIR must be on it (findmnt).
#   OUT      raw output; must not exist.
#   V3_FX    (env) the base dir mkfixtures.sh made: the nobarrier refusal fixtures. Unset: those checks FAIL.
# Arms (verdicts by check.py, which reads only OUT; every expectation there comes from the arm definitions):
#   F1  each arm set under strace -f -c at n = 1, 2, 3, 40: every syscall's count grows per op by exactly the
#       arms' definitions, and the flush syscalls' totals equal setup + definition x n + teardown. A slope over four
#       n cancels the setup and output syscalls, which strace -c cannot separate from the timed ops. ASLR is off
#       (setarch -R) so startup is identical run to run.
#   F1b the real probe under strace -f -y with --trace-clock (the clock read becomes a visible syscall): inside every
#       timed window, exactly the arm's syscalls on the arm's own files, sizes and offsets; nothing between windows;
#       every arm once per round. n = 300 for every arm (past ow1m's and ow64k's wrap) and n = 4100 for the 4 KiB
#       arms (past their wrap). check.py first plants 8 breaches into a copy of the trace and must reject each.
#   F2  the -c sets with --mutant-nosync (no flush in the flushed arms; clean keeps its fsync); F2c: the F1 check
#       rejects the mutant's counts and the mutant check rejects F1's; F2d: the mutant's sequence (n = 40);
#       F2b: the mutant unwatched at n = 200 fails the flush control (rc 3), and the same arms and seed without
#       the mutant (F3) do not.
#   F3  a real n = 200 run of every arm through run.sh (V3_SMOKE=1, stamps around it); the control is recorded.
#   F4  refusals: rc 2, the reason in the message, and no out dir; run.sh's binding to a fire-check verdict.
# Exit: check.py's (0 all pass, 1 a check failed, 2 usage).
set -uo pipefail
[ $# -eq 4 ] || { echo "usage: firecheck.sh V3FLOOR ext4|ext4loop|xfs|btrfs WORKDIR OUT" >&2; exit 2; }
V3=$(readlink -f "$1") FS=$2 W=$3 OUT=$4
HERE="$(cd "$(dirname "$0")" && pwd)"
case $FS in ext4|ext4loop|xfs|btrfs) ;; *) echo "firecheck: FS must be ext4, ext4loop, xfs or btrfs" >&2; exit 2 ;; esac
[ -x "$V3" ] || { echo "firecheck: $V3 is not executable" >&2; exit 2; }
[ -e "$OUT" ] && { echo "firecheck: $OUT exists" >&2; exit 2; }
mkdir -p "$OUT/F1" "$OUT/F1b" "$OUT/F2" "$OUT/F4" "$W" || exit 2
W=$(readlink -f "$W")
ALL=append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,fdatasync4k,nosync25
FLUSHED="append25 ow4k ow64k ow1m fdatasync4k clone1b clone2b"
NS="1 2 3 40"
ARCH=$(uname -m)
SHA=$(sha256sum "$V3" | cut -d' ' -f1)
WFS=$(findmnt -n -o FSTYPE -T "$W")

{
  echo "utc=$(date -u +%FT%TZ)"
  echo "cell=$FS"
  echo "work=$W"
  echo "work_fstype=$WFS"
  echo "work_mount=$(findmnt -n -o SOURCE,TARGET,OPTIONS -T "$W")"
  echo "root_mount=$(findmnt -n -o SOURCE,FSTYPE,OPTIONS /)"
  echo "mnt_mount=$(findmnt -n -o SOURCE,FSTYPE,OPTIONS -T /mnt)"
  losetup -l -n -O NAME,BACK-FILE,DIO 2>/dev/null | sed 's/^/loop /'
  echo "v3floor_sha256=$SHA"
  echo "arch=$ARCH"
  echo "uname=$(uname -srm)"
  echo "strace=$(strace -V | head -1)"
  # setarch -R: the traced runs have no ASLR. Run 37243578945 (arm64 only) saw munmap vary by +-1 between runs,
  # independent of n; run 37243798049 under -R was exact on every cell, consistent with address-dependent
  # alignment trimming at startup (the mechanism itself is not checked). Flag 0x0040000 = ADDR_NO_RANDOMIZE.
  echo "personality_under_setarch_R=$(setarch "$ARCH" -R cat /proc/self/personality)"
  for d in /sys/block/*; do echo "block ${d##*/} write_cache=[$(cat "$d/queue/write_cache" 2>/dev/null)] fua=[$(cat "$d/queue/fua" 2>/dev/null)]"; done
} > "$OUT/info.txt" 2>&1
cat "$OUT/info.txt"

tagof() { if [ "$1" = "$ALL" ]; then echo all; else echo "${1//,/+}"; fi; }

# one probe run under strace -f -c: stage set n [extra args]
counted() {
  local stage=$1 set=$2 n=$3
  shift 3
  local base
  base="$OUT/$stage/$(tagof "$set").n$n"
  timeout 600 setarch "$ARCH" -R strace -f -c -o "$base.strace" "$V3" --dir "$W" --out "$base.out" --n "$n" \
    --arms "$set" --seed 7 "$@" > "$base.txt" 2>&1
  echo $? > "$base.rc"
}
# one probe run under strace -f -y with --trace-clock: tag set n [extra args]; the trace is gzipped
sequenced() {
  local tag=$1 set=$2 n=$3
  shift 3
  local base="$OUT/F1b/$tag"
  timeout 900 setarch "$ARCH" -R strace -f -y -s 1 -o "$base.trace" "$V3" --dir "$W" --out "$base.out" --n "$n" \
    --arms "$set" --seed 5 --trace-clock "$@" > "$base.txt" 2>&1
  echo $? > "$base.rc"
  gzip -9 "$base.trace"
}

echo "== F1: strace -f -c, real probe"
for set in nosync25 clean $(for a in $FLUSHED; do echo "$a,nosync25"; done) "$ALL"; do
  for n in $NS; do counted F1 "$set" "$n"; done
done
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
timeout 900 "$V3" --dir "$W" --out "$OUT/F2b.out" --n 200 --arms "$ALL" --seed 11 --mutant-nosync > "$OUT/F2b.txt" 2>&1
echo $? > "$OUT/F2b.rc"
echo "== F3: the real probe, n=200, through run.sh"
V3_SMOKE=1 timeout 900 bash "$HERE/run.sh" "$V3" "$W" "$OUT/F3" 200 --arms "$ALL" --seed 11 > "$OUT/F3.txt" 2>&1
echo $? > "$OUT/F3.rc"

echo "== F4: refusals"
refuse() { # tag cmd...
  local tag=$1
  shift
  timeout 120 "$@" > "$OUT/F4/$tag.txt" 2>&1
  echo $? > "$OUT/F4/$tag.rc"
}
o() { echo "$OUT/F4/$1.out"; }
shm=/dev/shm/v3fc.$$
mkdir -p "$shm"
echo "shm_fstype=$(findmnt -n -o FSTYPE -T "$shm")" >> "$OUT/info.txt"
refuse R_tmpfs "$V3" --dir "$shm" --out "$(o R_tmpfs)" --n 5 --arms nosync25
rmdir "$shm"
if [ -n "${V3_FX:-}" ] && [ -d "$V3_FX/nb/w" ] && [ -d "$V3_FX/nbx/w" ]; then
  echo "fx_nb=$(findmnt -n -o SOURCE,FSTYPE,OPTIONS -T "$V3_FX/nb/w")" >> "$OUT/info.txt"
  echo "fx_nbx=$(findmnt -n -o SOURCE,FSTYPE,OPTIONS -T "$V3_FX/nbx/w")" >> "$OUT/info.txt"
  refuse R_nobarrier "$V3" --dir "$V3_FX/nb/w" --out "$(o R_nobarrier)" --n 5 --arms append25,nosync25
  refuse R_nobarrier_below "$V3" --dir "$V3_FX/nbx/w" --out "$(o R_nobarrier_below)" --n 5 --arms append25,nosync25
else
  echo "V3_FX fixtures missing" > "$OUT/F4/R_nobarrier.txt"; echo missing > "$OUT/F4/R_nobarrier.rc"
  echo "V3_FX fixtures missing" > "$OUT/F4/R_nobarrier_below.txt"; echo missing > "$OUT/F4/R_nobarrier_below.rc"
fi
mkdir -p "$(o R_outexists)" && echo sentinel > "$(o R_outexists)/sentinel"
refuse R_outexists "$V3" --dir "$W" --out "$(o R_outexists)" --n 5 --arms nosync25
refuse R_n0 "$V3" --dir "$W" --out "$(o R_n0)" --n 0 --arms nosync25
refuse R_nalpha "$V3" --dir "$W" --out "$(o R_nalpha)" --n abc --arms nosync25
refuse R_ntrail "$V3" --dir "$W" --out "$(o R_ntrail)" --n 5x --arms nosync25
refuse R_unknown "$V3" --dir "$W" --out "$(o R_unknown)" --n 5 --arms append25,bogus,nosync25
refuse R_twice "$V3" --dir "$W" --out "$(o R_twice)" --n 5 --arms nosync25,nosync25
refuse R_nod0 "$V3" --dir "$W" --out "$(o R_nod0)" --n 5 --arms append25,ow4k
refuse R_mutnod0 "$V3" --dir "$W" --out "$(o R_mutnod0)" --n 5 --arms append25,ow4k --mutant-nosync
refuse R_nice nice -n 5 "$V3" --dir "$W" --out "$(o R_nice)" --n 5 --arms nosync25
refuse R_ionice ionice -c 3 "$V3" --dir "$W" --out "$(o R_ionice)" --n 5 --arms nosync25
refuse R_schedidle chrt -i 0 "$V3" --dir "$W" --out "$(o R_schedidle)" --n 5 --arms nosync25
refuse R_schedbatch chrt -b 0 "$V3" --dir "$W" --out "$(o R_schedbatch)" --n 5 --arms nosync25
refuse R_badarg "$V3" --dir "$W" --out "$(o R_badarg)" --n 5 --bogus
refuse R_noout "$V3" --dir "$W" --n 5 --arms nosync25
refuse R_pathlong "$V3" --dir "$W/$(printf 'a%.0s' $(seq 4000))" --out "$(o R_pathlong)" --n 5 --arms nosync25
# ext4: every flushed arm refused -> rc 2; xfs/btrfs: the same arms run (the positive control)
refuse X_allclones "$V3" --dir "$W" --out "$(o X_allclones)" --n 5 --arms clone1b,clone2b,nosync25
# a clones dir left by a dead run: xfs/btrfs refuse it; ext4 refuses the clone arm before looking
mkdir "$W/clone1b.clones"
refuse R_leftover "$V3" --dir "$W" --out "$(o R_leftover)" --n 5 --arms clone1b,nosync25
rmdir "$W/clone1b.clones"

echo "== F4: run.sh binds a batch to a passing fire-check of this binary"
vfx() { # name all_pass sha fstype arch -> a planted verdict file
  printf '{"all_pass": %s, "v3floor_sha256": "%s", "fstype": "%s", "arch": "%s"}\n' "$2" "$3" "$4" "$5" > "$OUT/F4/verdict-$1.json"
  echo "$OUT/F4/verdict-$1.json"
}
RS="$HERE/run.sh"
refuse R_runsh_none env -u V3_SMOKE -u V3_FIRECHECK_VERDICT bash "$RS" "$V3" "$W" "$(o R_runsh_none)" 5 --arms append25,nosync25
refuse R_runsh_both env V3_SMOKE=1 V3_FIRECHECK_VERDICT="$(vfx ok true "$SHA" "$WFS" "$ARCH")" bash "$RS" "$V3" "$W" "$(o R_runsh_both)" 5 --arms append25,nosync25
refuse R_runsh_sha env -u V3_SMOKE V3_FIRECHECK_VERDICT="$(vfx sha true 0000 "$WFS" "$ARCH")" bash "$RS" "$V3" "$W" "$(o R_runsh_sha)" 5 --arms append25,nosync25
refuse R_runsh_fail env -u V3_SMOKE V3_FIRECHECK_VERDICT="$(vfx fail false "$SHA" "$WFS" "$ARCH")" bash "$RS" "$V3" "$W" "$(o R_runsh_fail)" 5 --arms append25,nosync25
refuse R_runsh_fs env -u V3_SMOKE V3_FIRECHECK_VERDICT="$(vfx fs true "$SHA" tmpfs "$ARCH")" bash "$RS" "$V3" "$W" "$(o R_runsh_fs)" 5 --arms append25,nosync25
refuse R_runsh_arch env -u V3_SMOKE V3_FIRECHECK_VERDICT="$(vfx arch true "$SHA" "$WFS" sparc)" bash "$RS" "$V3" "$W" "$(o R_runsh_arch)" 5 --arms append25,nosync25
refuse P_runsh_ok env -u V3_SMOKE V3_FIRECHECK_VERDICT="$(vfx ok true "$SHA" "$WFS" "$ARCH")" bash "$RS" "$V3" "$W" "$(o P_runsh_ok)" 5 --arms append25,nosync25

ls -A "$W" > "$OUT/work-leftover.txt"
python3 -B "$HERE/check.py" "$OUT" "$FS"
