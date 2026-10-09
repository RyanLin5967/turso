#!/bin/bash
# t3lib_test.sh -- t3lib.sh's checks on fake sysfs trees and registries (fourth lane review LOW 8, 10, 11, 12, 16): the
# real-run preflight checks and the drift gate get tests that can fail, though only a real run reaches them.
# t3run's selftests stage runs it. Exit 0 iff every case holds.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
source "$HERE/t3lib.sh"
pass=0 fail=0
check() { if eval "$2" > "$T/out.txt" 2>&1; then pass=$((pass + 1)); echo "T3LIB self-test PASS: $1"
          else fail=$((fail + 1)); echo "T3LIB self-test FAIL: $1 ($(tr '\n' ' ' < "$T/out.txt"))"; fi; }
disk() { # disk NAME WRITE_CACHE MODEL FIRMWARE_FILE FIRMWARE
  mkdir -p "$T/sys/block/$1/queue" "$T/sys/block/$1/device"
  echo "$2" > "$T/sys/block/$1/queue/write_cache"; echo "$3" > "$T/sys/block/$1/device/model"
  [ -n "$4" ] && echo "$5" > "$T/sys/block/$1/device/$4"; return 0; }
reg() { printf '# comment\n' > "$T/reg.tsv"; for l in "$@"; do printf '%s\n' "$l" >> "$T/reg.tsv"; done; }
TAB=$'\t'

# LOW 8: the drift gate is an allowlist (0 pass, 3 a published void), never a denylist of 2
check "drift rc 0 passes" 'drift_ok 0'
check "drift rc 3 (a void, descriptive on T3) passes" 'drift_ok 3'
for rc in 1 2 124 127 137; do check "drift rc $rc fails the block" "! drift_ok $rc"; done

# LOW 11: an empty or failed block list refuses
check "a non-empty block list passes" 'fslist_ok "xfs btrfs"'
check "an empty block list refuses" '! fslist_ok ""'
check "a blank block list refuses" '! fslist_ok "   "'

# LOW 12: the registration preflight, over a sysfs root and a registry path
disk nvme0n1 "write back" "Samsung PM9A3" firmware_rev "GDC5602Q"
disk sda "write through" "INTEL SSDSC2KG96" rev "0100"
disk sdb "write back" "X" "" ""
reg "frame_arm${TAB}ow4k${TAB}DECISIONS x" "d0_threshold/xfs/wb/bare${TAB}9.5${TAB}DECISIONS y"
check "write back, no PLP, frame arm and the xfs threshold registered: passes" \
  'registered_ok "$T/sys" "$T/reg.tsv" nvme0n1 no xfs'
check "write back, no PLP, btrfs threshold missing: refuses naming it" \
  '! registered_ok "$T/sys" "$T/reg.tsv" nvme0n1 no "xfs btrfs" && grep -q "d0_threshold/btrfs/wb/bare" "$T/out.txt"'
check "write back with PLP needs no threshold" 'registered_ok "$T/sys" "$T/reg.tsv" nvme0n1 yes "xfs btrfs"'
check "write through needs no threshold" 'registered_ok "$T/sys" "$T/reg.tsv" sda no "xfs btrfs"'
reg "d0_threshold/xfs/wb/bare${TAB}9.5${TAB}DECISIONS y"
check "no frame arm refuses" '! registered_ok "$T/sys" "$T/reg.tsv" sda no xfs && grep -q frame_arm "$T/out.txt"'
reg "frame_arm${TAB}${TAB}DECISIONS x"
check "an empty frame arm value refuses" '! registered_ok "$T/sys" "$T/reg.tsv" sda no xfs'
reg "frame_arm${TAB}ow4k${TAB}DECISIONS x"
check "an unreadable write_cache refuses" '! registered_ok "$T/sys" "$T/reg.tsv" nvme9n9 no xfs'
check "an empty block list refuses here too" '! registered_ok "$T/sys" "$T/reg.tsv" sda no ""'

# LOW 9 / 16: the PLP drive identity, per driver, never empty
check "NVMe: model and firmware_rev" '[ "$(plp_drive_id "$T/sys" nvme0n1)" = "Samsung PM9A3${TAB}GDC5602Q" ]'
check "SCSI/SATA: model and rev" '[ "$(plp_drive_id "$T/sys" sda)" = "INTEL SSDSC2KG96${TAB}0100" ]'
check "no firmware field refuses" '! plp_drive_id "$T/sys" sdb'
check "no device refuses" '! plp_drive_id "$T/sys" nvme9n9'
printf 'Samsung PM9A3\tGDC5602Q\n' > "$T/plp.tsv"
check "a listed drive passes" 'plp_listed "$T/sys" nvme0n1 "$T/plp.tsv"'
check "an unlisted drive refuses" '! plp_listed "$T/sys" sda "$T/plp.tsv"'
printf 'Samsung PM9A3\n' > "$T/plp.tsv"
check "a model-only line does not list a drive" '! plp_listed "$T/sys" nvme0n1 "$T/plp.tsv"'

# devguard round-2 attack LOW 4: every mkfs and wipefs reaches the device preflight checked, by its resolved path.
# A refusal is rc 2 exactly, so a missing function (127) or a crash cannot pass as one.
refuses2() { "$@"; [ $? = 2 ]; }
mkdir -p "$T/dev"; : > "$T/dev/nvme1n1"; : > "$T/dev/nvme2n1"
REAL1=$(readlink -f "$T/dev/nvme1n1")
ln -sfn "$T/dev/nvme1n1" "$T/dev/by-id-x"
check "a device path that still resolves to the preflight device passes" 'dev_unmoved "$T/dev/by-id-x" "$REAL1"'
check "an empty preflight path refuses (rc 2)" 'refuses2 dev_unmoved "$T/dev/by-id-x" ""'
ln -sfn "$T/dev/nvme2n1" "$T/dev/by-id-x"
check "a device path that now resolves elsewhere refuses, naming where" \
  'refuses2 dev_unmoved "$T/dev/by-id-x" "$REAL1" && grep -q nvme2n1 "$T/out.txt"'
# the wipe in block_cleanup: only the filesystem this block made, on the unmoved device
printf '#!/bin/sh\necho xfs\n' > "$T/blkid-xfs"; printf '#!/bin/sh\nexit 2\n' > "$T/blkid-none"
chmod +x "$T/blkid-xfs" "$T/blkid-none"
ln -sfn "$T/dev/nvme1n1" "$T/dev/by-id-x"
check "the block's own filesystem on the unmoved device may be wiped" \
  'T3_BLKID="$T/blkid-xfs" fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs'
check "another filesystem refuses the wipe" 'T3_BLKID="$T/blkid-xfs" refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" btrfs'
check "no signature at all refuses the wipe" 'T3_BLKID="$T/blkid-none" refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs'
ln -sfn "$T/dev/nvme2n1" "$T/dev/by-id-x"
check "a moved device refuses the wipe" 'T3_BLKID="$T/blkid-xfs" refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs'

echo "T3LIB SELF-TEST $pass/$((pass + fail)) $([ $fail = 0 ] && echo PASS || echo FAIL)"
[ $fail = 0 ]
