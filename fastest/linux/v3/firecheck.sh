#!/usr/bin/env bash
# firecheck.sh V3FLOOR FS WORKDIR OUT -- fire-check the Linux V3 device-floor probe on one filesystem.
# NOT a credited measurement. Port of frontier/fastest/tools/v3/firecheck.py (whose F1/F2 ran under the Mac's V1
# shim; here strace -f -c is the instrument).
#   FS       the cell's filesystem, ext4|xfs|btrfs (the workflow matrix); WORKDIR must be on it (findmnt).
#   OUT      raw output; must not exist.
# Arms (verdicts by check.py, which reads only OUT; every expectation there comes from the arm definitions):
#   F1  each arm set under strace -f -c at n = 1, 2, 3, 40: every syscall's count grows per op by exactly the
#       arms' definitions, and the flush syscalls' totals equal setup + definition x n. A slope over four n
#       cancels the setup and output syscalls, which strace -c cannot separate from the timed ops.
#  F2   the same with --mutant-nosync (no flush in the flushed arms; clean keeps its fsync); F2c: the F1 check
#       rejects the mutant's counts and the mutant check rejects F1's (the detector can fire); F2b: the mutant
#       unwatched at n=200 fails the flush control (rc 3).
#   F3  a real n=200 run of every arm through run.sh (stamps around it); the flush control's verdict is RECORDED.
#   F4  refusals: rc 2, the reason in the message, and no out dir.
# Exit: check.py's (0 all pass, 1 a check failed, 2 usage).
set -uo pipefail
[ $# -eq 4 ] || { echo "usage: firecheck.sh V3FLOOR ext4|xfs|btrfs WORKDIR OUT" >&2; exit 2; }
V3=$(readlink -f "$1") FS=$2 W=$3 OUT=$4
HERE="$(cd "$(dirname "$0")" && pwd)"
case $FS in ext4|xfs|btrfs) ;; *) echo "firecheck: FS must be ext4, xfs or btrfs" >&2; exit 2 ;; esac
[ -x "$V3" ] || { echo "firecheck: $V3 is not executable" >&2; exit 2; }
[ -e "$OUT" ] && { echo "firecheck: $OUT exists" >&2; exit 2; }
mkdir -p "$OUT/F1" "$OUT/F2" "$OUT/F4" "$W" || exit 2
W=$(readlink -f "$W")
ALL=append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,fdatasync4k,nosync25
FLUSHED="append25 ow4k ow64k ow1m fdatasync4k clone1b clone2b"
NS="1 2 3 40"

{
  echo "utc=$(date -u +%FT%TZ)"
  echo "work=$W"
  echo "work_fstype=$(findmnt -n -o FSTYPE -T "$W")"
  echo "work_mount=$(findmnt -n -o SOURCE,TARGET,OPTIONS -T "$W")"
  echo "v3floor_sha256=$(sha256sum "$V3" | cut -d' ' -f1)"
  echo "uname=$(uname -srm)"
  echo "strace=$(strace -V | head -1)"
  for d in /sys/block/*; do echo "block ${d##*/} write_cache=[$(cat "$d/queue/write_cache" 2>/dev/null)] fua=[$(cat "$d/queue/fua" 2>/dev/null)]"; done
} > "$OUT/info.txt" 2>&1
cat "$OUT/info.txt"

tagof() { if [ "$1" = "$ALL" ]; then echo all; else echo "${1//,/+}"; fi; }

# one probe run under strace -f -c: stage set n [extra args]
traced() {
  local stage=$1 set=$2 n=$3
  shift 3
  local base
  base="$OUT/$stage/$(tagof "$set").n$n"
  timeout 600 strace -f -c -o "$base.strace" "$V3" --dir "$W" --out "$base.out" --n "$n" --arms "$set" --seed 7 "$@" \
    > "$base.txt" 2>&1
  echo $? > "$base.rc"
}

echo "== F1: strace -f -c, real probe"
for set in nosync25 clean $(for a in $FLUSHED; do echo "$a,nosync25"; done) "$ALL"; do
  for n in $NS; do traced F1 "$set" "$n"; done
done
echo "== F2: strace -f -c, --mutant-nosync"
for set in clean $(for a in $FLUSHED; do echo "$a,nosync25"; done) "$ALL"; do
  for n in $NS; do traced F2 "$set" "$n" --mutant-nosync; done
done
echo "== F2b: the mutant unwatched, n=200"
timeout 900 "$V3" --dir "$W" --out "$OUT/F2b.out" --n 200 --arms "$ALL" --seed 11 --mutant-nosync > "$OUT/F2b.txt" 2>&1
echo $? > "$OUT/F2b.rc"
echo "== F3: the real probe, n=200, through run.sh"
timeout 900 bash "$HERE/run.sh" "$V3" "$W" "$OUT/F3" 200 --arms "$ALL" --seed 11 > "$OUT/F3.txt" 2>&1
echo $? > "$OUT/F3.rc"

echo "== F4: refusals"
shm=/dev/shm/v3fc.$$
mkdir -p "$shm"
echo "shm_fstype=$(findmnt -n -o FSTYPE -T "$shm")" >> "$OUT/info.txt"
refuse() { # tag cmd...
  local tag=$1
  shift
  timeout 60 "$@" > "$OUT/F4/$tag.txt" 2>&1
  echo $? > "$OUT/F4/$tag.rc"
}
o() { echo "$OUT/F4/$1.out"; }
refuse R_tmpfs "$V3" --dir "$shm" --out "$(o R_tmpfs)" --n 5 --arms nosync25
refuse R_outexists "$V3" --dir "$W" --out "$W" --n 5 --arms nosync25
refuse R_n0 "$V3" --dir "$W" --out "$(o R_n0)" --n 0 --arms nosync25
refuse R_nalpha "$V3" --dir "$W" --out "$(o R_nalpha)" --n abc --arms nosync25
refuse R_ntrail "$V3" --dir "$W" --out "$(o R_ntrail)" --n 5x --arms nosync25
refuse R_unknown "$V3" --dir "$W" --out "$(o R_unknown)" --n 5 --arms append25,bogus,nosync25
refuse R_twice "$V3" --dir "$W" --out "$(o R_twice)" --n 5 --arms nosync25,nosync25
refuse R_nod0 "$V3" --dir "$W" --out "$(o R_nod0)" --n 5 --arms append25,ow4k
refuse R_mutnod0 "$V3" --dir "$W" --out "$(o R_mutnod0)" --n 5 --arms append25,ow4k --mutant-nosync
refuse R_nice nice -n 5 "$V3" --dir "$W" --out "$(o R_nice)" --n 5 --arms nosync25
refuse R_ionice ionice -c 3 "$V3" --dir "$W" --out "$(o R_ionice)" --n 5 --arms nosync25
refuse R_badarg "$V3" --dir "$W" --out "$(o R_badarg)" --n 5 --bogus
refuse R_noout "$V3" --dir "$W" --n 5 --arms nosync25
rmdir "$shm"

ls -A "$W" > "$OUT/work-leftover.txt"
python3 -B "$HERE/check.py" "$OUT" "$FS"
