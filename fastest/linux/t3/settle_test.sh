#!/bin/bash
# settle_test.sh -- settle.sh on fake sysfs/proc files (fourth lane review MED 5 and LOW 12/25: a test settle can fail).
# Linux only (date +%s%N); t3run's selftests stage runs it. Exit 0 iff every case holds.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/sys/class/block/loop9" "$T/proc" "$T/out"
export SETTLE_SYS=$T/sys SETTLE_PROC=$T/proc SETTLE_SYNC=true SETTLE_CAP_S=6
export SETTLE_FSTRIM="$T/fstrim" SETTLE_FINDMNT="$T/findmnt"
printf '#!/bin/sh\necho "$1" >> %s/fstrim.calls\necho "$1: 0 B (0 bytes) trimmed"\n' "$T" > "$T/fstrim"
printf '#!/bin/sh\necho /dev/loop9\n' > "$T/findmnt"
chmod +x "$T/fstrim" "$T/findmnt"
source "$HERE/settle.sh"
st() { echo "1 0 0 0 $1 0 0 0 $2 0 0 $3 0 0 0 $4 0" > "$T/sys/class/block/loop9/stat"; }  # writes inflight discards flushes
dirty() { echo "Dirty: $1 kB" > "$T/proc/meminfo"; }
field() { sed -n "s/.* $1=\([^ ]*\).*/\1/p" "$T/out/settle.txt"; }
pass=0 fail=0
check() { if eval "$2"; then pass=$((pass + 1)); echo "SETTLE self-test PASS: $1"; else fail=$((fail + 1)); echo "SETTLE self-test FAIL: $1 ($(cat "$T/out/settle.txt" 2>/dev/null))"; fi; }

dirty 12; st 100 0 5 7
check "settle_dev resolves the loop of MNT" '[ "$(settle_dev /mnt/x)" = loop9 ]'
echo "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15" > "$T/sys/class/block/loop9/stat"
check "settle_dev refuses a stat with fewer than 17 fields" '! settle_dev /mnt/x 2>/dev/null'

st 100 0 5 7; : > "$T/fstrim.calls"
settle /mnt/x loop9 "$T/out"
check "constant counters: quiet within about 2 s" '[ "$(field quiet)" = yes ] && awk -v s="$(field settle_s)" "BEGIN { exit !(s >= 2 && s < 4) }"'
check "fstrim ran once, on MNT, and is recorded" '[ "$(cat "$T/fstrim.calls")" = /mnt/x ] && grep -q "fstrim=\"/mnt/x: 0 B" "$T/out/settle.txt"'

st 100 0 5 7
( for i in 1 2 3 4 5; do sleep 0.3; st $((100 + i)) 0 5 7; done ) &
settle /mnt/x loop9 "$T/out"; wait
check "writes moving for 1.5 s, then still: quiet only 2 s after the last move" '[ "$(field quiet)" = yes ] && awk -v s="$(field settle_s)" "BEGIN { exit !(s >= 3.4) }"'

st 100 0 5 7
( for i in $(seq 1 20); do sleep 0.5; st 100 0 $((5 + i)) 7; done ) & p=$!
settle /mnt/x loop9 "$T/out"; kill $p 2>/dev/null; wait $p 2>/dev/null
check "a discard every 0.5 s (btrfs async discard): not quiet by the cap" '[ "$(field quiet)" = no ]'

st 100 0 5 7
( for i in $(seq 1 20); do sleep 0.5; st 100 0 5 $((7 + i)); done ) & p=$!
settle /mnt/x loop9 "$T/out"; kill $p 2>/dev/null; wait $p 2>/dev/null
check "a flush every 0.5 s: not quiet by the cap" '[ "$(field quiet)" = no ]'

st 100 1 5 7
settle /mnt/x loop9 "$T/out"
check "a request always in flight: not quiet, and bounded by the cap" '[ "$(field quiet)" = no ] && awk -v s="$(field settle_s)" "BEGIN { exit !(s < 8) }"'

st 100 0 5 7; dirty 4096
settle /mnt/x loop9 "$T/out"
check "4 MiB dirty: not quiet" '[ "$(field quiet)" = no ]'

echo "SETTLE SELF-TEST $pass/$((pass + fail)) $([ $fail = 0 ] && echo PASS || echo FAIL)"
[ $fail = 0 ]
