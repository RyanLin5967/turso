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
# the wipe in block_cleanup: only the filesystem this block made (its type AND the UUID mkfs gave it, review 5 MED 3),
# on the unmoved, unchanged drive. blkid is reached through t3lib.sh's t3_blkid function, redefined here; production
# reads no override (review 5 LOW 22). Test edit, flagged: the four earlier cases used the T3_BLKID override, which
# LOW 22 removes, and gain the UUID argument; their expectations are unchanged.
t3_blkid() { case $1 in TYPE) echo "$FAKE_TYPE" ;; UUID) echo "$FAKE_UUID" ;; esac; }
FAKE_TYPE=xfs FAKE_UUID=u-1
ln -sfn "$T/dev/nvme1n1" "$T/dev/by-id-x"
check "the block's own filesystem (type and UUID) on the unmoved device may be wiped" \
  'fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs u-1'
check "another filesystem refuses the wipe" 'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" btrfs u-1'
check "MED 3: the same type with another UUID (another drive under the name, re-made) refuses, naming the UUID" \
  'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs u-2 && grep -q UUID "$T/out.txt"'
check "MED 3: an empty recorded UUID refuses" 'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs "" && grep -q UUID "$T/out.txt"'
FAKE_TYPE=""
check "no signature at all refuses the wipe" 'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs u-1'
FAKE_TYPE=xfs
ln -sfn "$T/dev/nvme2n1" "$T/dev/by-id-x"
check "a moved device refuses the wipe" 'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs u-1'

# review 5 MED 3: dev_unmoved compares path text, a tautology for a plain /dev/nvmeXnY, so the drive itself is what
# preflight records (device-id.txt) and every mkfs, mount and wipefs re-reads: wwid, MAJ:MIN, model, serial, firmware
ident() { # ident NAME WWID DEV MODEL SERIAL FW: a fake NVMe namespace (wwid on the block, the rest on its controller)
  mkdir -p "$T/sys/block/$1/device"
  echo "$2" > "$T/sys/block/$1/wwid"; echo "$3" > "$T/sys/block/$1/dev"; echo "$4" > "$T/sys/block/$1/device/model"
  echo "$5" > "$T/sys/block/$1/device/serial"; echo "$6" > "$T/sys/block/$1/device/firmware_rev"; }
NL=$'\n'
ident nvme5n1 eui.0001 259:5 PM9A3 S1 GDC5
check "MED 3: an NVMe namespace's identity is its wwid, MAJ:MIN, model, serial and firmware" \
  '[ "$(dev_identity "$T/sys" nvme5n1)" = "wwid=eui.0001${NL}dev=259:5${NL}model=PM9A3${NL}serial=S1${NL}firmware=GDC5" ]'
mkdir -p "$T/sys/block/sdc/device"; echo 8:32 > "$T/sys/block/sdc/dev"; echo "naa.5000c500" > "$T/sys/block/sdc/device/wwid"
echo "ST4000NM" > "$T/sys/block/sdc/device/model"; echo "SN04" > "$T/sys/block/sdc/device/rev"
printf '\000\200\000\010ZC10XYZ9' > "$T/sys/block/sdc/device/vpd_pg80"
check "MED 3: a SCSI disk's identity takes the device wwid, the printable vpd_pg80 serial and rev" \
  '[ "$(dev_identity "$T/sys" sdc)" = "wwid=naa.5000c500${NL}dev=8:32${NL}model=ST4000NM${NL}serial=ZC10XYZ9${NL}firmware=SN04" ]'
ident nvme6n1 "" 259:6 PM9A3 S2 GDC5
check "MED 3: an empty wwid refuses (rc 2)" 'refuses2 dev_identity "$T/sys" nvme6n1 && grep -q wwid "$T/out.txt"'
ident nvme7n1 eui.0007 259:7 PM9A3 "" GDC5
check "MED 3: an empty serial refuses (rc 2)" 'refuses2 dev_identity "$T/sys" nvme7n1 && grep -q serial "$T/out.txt"'
dev_identity "$T/sys" nvme5n1 > "$T/id5.txt"
: > "$T/dev/nvme5n1"; REAL5=$(readlink -f "$T/dev/nvme5n1")
check "MED 3: the unmoved path to the unchanged drive passes" \
  'dev_unchanged "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt"'
ident nvme5n1 eui.0099 259:5 PM9A3 S9 GDC5
check "MED 3: a plain node whose drive changed behind the same name (wwid, serial) refuses, naming wwid" \
  'refuses2 dev_unchanged "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt" && grep -q wwid "$T/out.txt"'
ident nvme5n1 eui.0001 259:9 PM9A3 S1 GDC5
check "MED 3: a new MAJ:MIN behind the same name refuses" \
  'refuses2 dev_unchanged "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt" && grep -q "dev=" "$T/out.txt"'
ident nvme5n1 eui.0001 259:5 PM9A3 S1 GDC5
: > "$T/id-empty.txt"
check "MED 3: an empty identity record refuses" \
  'refuses2 dev_unchanged "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id-empty.txt"'
check "MED 3: a moved path refuses before the identity is read" \
  'refuses2 dev_unchanged "$T/dev/by-id-x" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt"'

echo "T3LIB SELF-TEST $pass/$((pass + fail)) $([ $fail = 0 ] && echo PASS || echo FAIL)"
[ $fail = 0 ]
