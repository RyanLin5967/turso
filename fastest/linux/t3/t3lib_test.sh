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
# TEST EDIT, flagged (review 6 LOW 7): the probe's UUID is empty too, so only the empty-UUID guard can refuse; with u-1
# set, the UUID-mismatch guard refused instead and its text also says UUID, so deleting the guard stayed green
FAKE_UUID=""
check "MED 3: an empty recorded UUID refuses (the probe's empty as well: only the guard refuses)" \
  'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs "" && grep -q "no UUID recorded" "$T/out.txt"'
FAKE_UUID=u-1
FAKE_TYPE=""
check "no signature at all refuses the wipe" 'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs u-1'
FAKE_TYPE=xfs
ln -sfn "$T/dev/nvme2n1" "$T/dev/by-id-x"
check "a moved device refuses the wipe" 'refuses2 fs_is_ours "$T/dev/by-id-x" "$REAL1" xfs u-1'

# review 6 LOW 5: dev_unchanged also compares the node's own MAJ:MIN (st_rdev, t3lib.sh t3_rdev) with the recorded
# dev=, so a node elsewhere carrying another disk's numbers is not taken for the drive. The fake node files here are
# regular files, so t3_rdev is redefined: it answers FAKE_RDEV, nvme5n1's 259:5 unless a case sets another. TEST EDIT,
# flagged: a new fixture line the MED 3 cases below now depend on; their expectations are unchanged
t3_rdev() { echo "${FAKE_RDEV:-259:5}"; }
FAKE_RDEV=""
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
FAKE_RDEV=259:9
check "LOW 5: the node's own MAJ:MIN (259:9) is not the recorded dev=259:5: refuses (rc 2), naming dev=" \
  'refuses2 dev_unchanged "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt" && grep -q "dev=259:5" "$T/out.txt"'
FAKE_RDEV=""
check "LOW 5: production t3_rdev reads a real node's MAJ:MIN in decimal (/dev/null is 1:3)" \
  '( source "$HERE/t3lib.sh"; [ "$(t3_rdev /dev/null)" = 1:3 ] )'
# review 6 LOW 21: vpd_pg80's 4-byte header (its length byte is printable for a 32-126 byte serial) is not the serial
mkdir -p "$T/sys/block/sdd/device"; echo 8:48 > "$T/sys/block/sdd/dev"; echo "naa.5000c501" > "$T/sys/block/sdd/device/wwid"
echo "ST8000NM" > "$T/sys/block/sdd/device/model"; echo "SN05" > "$T/sys/block/sdd/device/rev"
printf '\000\200\000\050ZC20ABC1%32s' "" > "$T/sys/block/sdd/device/vpd_pg80"
check "LOW 21: a vpd_pg80 with a printable length byte (0x28, 40 bytes) gives the serial without it" \
  '[ "$(dev_identity "$T/sys" sdd | grep "^serial=")" = "serial=ZC20ABC1" ]'

# review 6 MED 1: the device branch is one t3lib function each for make and wipe, taking every input as a parameter,
# with the root commands as hooks (t3_pin, t3_devguard, t3_mkfs, t3_mount, t3_mounted, t3_wipe; t3_blkid above) that
# log here, so its order and every refusal fire on test input before a paid box. A refusal is rc 2 with no later
# step run, a failed command rc 1, and the pin is always released. The order (review 6 LOW 4): pin, devguard, the
# drive re-read, mkfs with the UUID it is given (LOW 6), the probe, the re-read, mount -t, the mount checked, unpin.
HL=$T/hooks.log
hl() { echo "$*" >> "$HL"; }
t3_pin() { hl pin "$1"; }
t3_unpin() { hl unpin; }
t3_devguard() { hl devguard "$1"; ${FAKE_DEVGUARD:-true}; }
t3_mkfs() { hl mkfs "$@"; ${FAKE_MKFS:-true} || return 1; FAKE_TYPE=$1 FAKE_UUID=${FAKE_MKFS_UUID:-$3}; }
t3_mount() { hl mount "$@"; FAKE_MOUNTED="${FAKE_MOUNT_SRC:-$2} $1"; }
t3_mounted() { echo "$FAKE_MOUNTED"; }
t3_wipe() { hl wipe "$1"; FAKE_TYPE="" FAKE_UUID=""; }
FAKE_DEVGUARD="" FAKE_MKFS="" FAKE_MKFS_UUID="" FAKE_MOUNT_SRC="" FAKE_MOUNTED=""
mkdir -p "$T/sys/block/nvme5n1/queue"; echo "write through" > "$T/sys/block/nvme5n1/queue/write_cache"
reg "frame_arm${TAB}ow4k${TAB}DECISIONS x"
dm() { : > "$HL"; rm -f "$T/made.txt"; FAKE_TYPE="" FAKE_UUID="" FAKE_MOUNTED=""
       device_make "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt" no "$T/reg.tsv" xfs U-1 /mnt/t3-xfs "$T/made.txt"; }
dw() { : > "$HL"; device_wipe "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt" xfs U-1; }
steps() { tr '\n' ',' < "$HL"; }
P="pin $REAL5," G="devguard $REAL5," M="mkfs xfs $REAL5 U-1," U="unpin,"
check "MED 1: device_make on the recorded drive: pin, devguard, mkfs with the UUID, mount -t, unpin, in that order" \
  'dm && [ "$(steps)" = "$P$G${M}mount xfs $REAL5 /mnt/t3-xfs,$U" ] && [ "$(cat "$T/made.txt")" = "xfs U-1" ]'
ident nvme5n1 eui.0099 259:5 PM9A3 S1 GDC5
check "MED 1 (review 5's scenario): a plain node whose wwid changed refuses (rc 2): no mkfs, nothing made, pin released" \
  'refuses2 dm && grep -q wwid "$T/out.txt" && [ "$(steps)" = "$P$G$U" ] && [ ! -e "$T/made.txt" ]'
ident nvme5n1 eui.0001 259:5 PM9A3 S1 GDC5
FAKE_DEVGUARD="ident nvme5n1 eui.0099 259:5 PM9A3 S9 GDC5"
check "LOW 4: the drive changes while devguard runs: the re-read between devguard and mkfs refuses (rc 2), no mkfs" \
  'refuses2 dm && [ "$(steps)" = "$P$G$U" ]'
FAKE_DEVGUARD=""; ident nvme5n1 eui.0001 259:5 PM9A3 S1 GDC5
FAKE_DEVGUARD=false
check "MED 1: devguard refusing stops before mkfs (rc 2), pin released" 'refuses2 dm && [ "$(steps)" = "$P$G$U" ]'
FAKE_DEVGUARD=""
echo "write back" > "$T/sys/block/nvme5n1/queue/write_cache"
check "MED 1: write_cache flipped to write back since preflight, no threshold registered: refuses before mkfs" \
  'refuses2 dm && grep -q "d0_threshold/xfs/wb/bare" "$T/out.txt" && [ "$(steps)" = "$P$G$U" ]'
echo "write through" > "$T/sys/block/nvme5n1/queue/write_cache"
FAKE_MKFS_UUID=u-other
check "LOW 6: a mkfs that did not take the given UUID refuses before mount, and the made record names what was asked" \
  'refuses2 dm && grep -q UUID "$T/out.txt" && [ "$(steps)" = "$P$G$M$U" ] && [ "$(cat "$T/made.txt")" = "xfs U-1" ]'
FAKE_MKFS_UUID=""
FAKE_MKFS="ident nvme5n1 eui.0099 259:5 PM9A3 S9 GDC5"
check "MED 1: the drive changes between mkfs and mount: refuses (rc 2), no mount" 'refuses2 dm && [ "$(steps)" = "$P$G$M$U" ]'
FAKE_MKFS=""; ident nvme5n1 eui.0001 259:5 PM9A3 S1 GDC5
FAKE_MKFS=false
check "MED 1: a failed mkfs is rc 1 (a failure, not a refusal): no mount, pin released" \
  'dm; [ $? = 1 ] && [ "$(steps)" = "$P$G$M$U" ]'
FAKE_MKFS=""
FAKE_MOUNT_SRC=/dev/sdz
check "LOW 4: findmnt naming another source after the mount refuses (rc 2)" \
  'refuses2 dm && grep -q /dev/sdz "$T/out.txt" && [ "$(steps)" = "$P$G${M}mount xfs $REAL5 /mnt/t3-xfs,$U" ]'
FAKE_MOUNT_SRC=""
FAKE_TYPE=xfs FAKE_UUID=U-1
check "MED 1: device_wipe on the recorded drive carrying the block's filesystem: pin, wipe, unpin" \
  'dw && [ "$(steps)" = "${P}wipe $REAL5,$U" ]'
FAKE_TYPE=xfs FAKE_UUID=U-1; ident nvme5n1 eui.0099 259:5 PM9A3 S1 GDC5
check "MED 1: device_wipe when the drive changed refuses (rc 2): no wipe, pin released" \
  'refuses2 dw && grep -q wwid "$T/out.txt" && [ "$(steps)" = "$P$U" ]'
ident nvme5n1 eui.0001 259:5 PM9A3 S1 GDC5
FAKE_TYPE=xfs FAKE_UUID=U-2
check "MED 1: device_wipe when the probe finds another UUID refuses (rc 2): no wipe" 'refuses2 dw && [ "$(steps)" = "$P$U" ]'
FAKE_TYPE=xfs FAKE_UUID=u-1

# the production hook bodies (review 6 MED 1 and LOW 6): t3lib.sh re-sourced in a subshell, sudo a stub recording argv
check "LOW 6: production t3_blkid runs sudo -n blkid -p -o value -s TAG DEV for each TAG, and ignores T3_BLKID" \
  '( source "$HERE/t3lib.sh"; sudo() { printf "%s\n" "$*" >> "$T/argv1.txt"; echo got; }; : > "$T/argv1.txt"
     export T3_BLKID=false; [ "$(t3_blkid TYPE /dev/x)" = got ] && [ "$(t3_blkid UUID /dev/x)" = got ] &&
     [ "$(cat "$T/argv1.txt")" = "-n blkid -p -o value -s TYPE /dev/x${NL}-n blkid -p -o value -s UUID /dev/x" ] )'
check "LOW 6: production t3_mkfs gives each filesystem the UUID it is passed (xfs -m uuid=, btrfs -U, ext4 -U)" \
  '( source "$HERE/t3lib.sh"; sudo() { printf "%s\n" "$*" >> "$T/argv2.txt"; }; : > "$T/argv2.txt"
     t3_mkfs xfs /dev/x U && t3_mkfs btrfs /dev/x U && t3_mkfs ext4 /dev/x U && ! t3_mkfs zfs /dev/x U &&
     [ "$(cat "$T/argv2.txt")" = "mkfs.xfs -f -m reflink=1,uuid=U /dev/x${NL}mkfs.btrfs -f -U U /dev/x${NL}mkfs.ext4 -F -U U -E lazy_itable_init=0,lazy_journal_init=0 /dev/x" ] )'
check "LOW 4: production t3_mount names the filesystem type (-t), and btrfs mounts nodiscard" \
  '( source "$HERE/t3lib.sh"; sudo() { printf "%s\n" "$*" >> "$T/argv3.txt"; }; : > "$T/argv3.txt"
     t3_mount xfs /dev/x /mnt/a && t3_mount btrfs /dev/x /mnt/b &&
     [ "$(cat "$T/argv3.txt")" = "mkdir -p /mnt/a${NL}mount -t xfs /dev/x /mnt/a${NL}mkdir -p /mnt/b${NL}mount -t btrfs -o nodiscard /dev/x /mnt/b" ] )'
check "LOW 4: production t3_pin holds the node open until t3_unpin (sudo stubbed to run as this user)" \
  '( source "$HERE/t3lib.sh"; sudo() { shift; "$@"; }; : > "$T/pinme"
     t3_pin "$T/pinme" && p=$T3PIN_PID && kill -0 "$p" && t3_unpin && ! kill -0 "$p" 2>/dev/null )'
check "LOW 4: production t3_pin on a node it cannot open refuses (rc 2) and leaves no helper" \
  '( source "$HERE/t3lib.sh"; sudo() { shift; "$@"; }; t3_pin "$T/no-such-node"; [ $? = 2 ] && [ -z "${T3PIN_PID:-}" ] )'
# the real probe on a real filesystem (review 6 LOW 6: the test replaced t3_blkid outright, so its body never ran)
U6=0b9e2d6c-3f1a-4c7e-9a55-0d3c6a1f2e47
check "LOW 6: production t3_blkid reads TYPE and UUID from a real ext4 image (needs mkfs.ext4 and sudo -n)" \
  '( source "$HERE/t3lib.sh"; command -v mkfs.ext4 > /dev/null || { echo "mkfs.ext4 missing"; exit 1; }
     truncate -s 8M "$T/img" && mkfs.ext4 -q -F -U "$U6" "$T/img" &&
     [ "$(t3_blkid TYPE "$T/img")" = ext4 ] && [ "$(t3_blkid UUID "$T/img")" = "$U6" ] )'

# The lead's MED 12 ruling (DECISIONS 2026-10-15T04:21:48Z, SAME INSTRUMENT FOR EVERY SYSTEM): ours' timed run runs
# under the competitors' shared sweeper (gates/sweeper.sh) and its timed.json records sweeper_cpu_s, read by the one
# reader (gates/sweeper_record.py). These need Linux /proc and bash 5 (EPOCHREALTIME), as the T3 box has.
SW=$HERE/../gates/sweeper.sh SR=$HERE/../gates/sweeper_record.py
check "MED 12: the shared sweeper (gates/sweeper.sh) and its one reader (gates/sweeper_record.py) are in the tree" \
  '[ -f "$SW" ] && [ -f "$SR" ]'
# swrun N CMD...: ours_timed_run into $T/swN with the real sweeper sourced; its rc is the function's
swrun() { local d=$T/sw$1; shift; mkdir -p "$d"; ( source "$SW" && ours_timed_run "$d" "$@" ); }
# the window as numbers, and the first and last sweep's times, from the files the run wrote
swnums() { awk '{for (i = 1; i <= NF; i++) { split($i, kv, "="); if (kv[1] == "t0") a = kv[2]; if (kv[1] == "t1") b = kv[2] }}
                END { print a, b }' "$1/timed.window"; awk '$1 == "SWEEP" { if (f == "") f = $3; l = $3 } END { print f, l }' \
                "$1/timed.tracer.tsv"; }
check "MED 12: ours_timed_run returns the command's rc (3) and records its window, t0 < t1, both inside the sweeps" \
  'swrun 1 bash -c "sleep 0.5; exit 3"; [ $? = 3 ] && grep -q "^sweeper pid=[0-9]* nice=19 " "$T/sw1/timed.tracer.tsv" &&
   grep -q " rc=3$" "$T/sw1/timed.window" &&
   swnums "$T/sw1" | tr "\n" " " | awk "{ exit !(\$1 < \$2 && \$3 <= \$1 && \$2 <= \$4) }"'
check "MED 12: the one reader accepts that record over that window (sweeper_record.py record, rc 0)" \
  'python3 -B "$SR" record "$T/sw1/timed.tracer.tsv" $(swnums "$T/sw1" | head -1)'
check "MED 12: ours_timed_record writes timed.json: sweeper_cpu_s a number, the window process, the reader's own value" \
  '( source "$HERE/t3lib.sh"; ours_timed_record "$T/sw1" "$SR" ) && python3 -B -c "
import json, sys; sys.path.insert(0, \"$HERE/../gates\"); import sweeper_record as s
j = json.load(open(\"$T/sw1/timed.json\"))
r, why = s.sweeper_record(open(\"$T/sw1/timed.tracer.tsv\").read().splitlines(), j[\"t0\"], j[\"t1\"])
sys.exit(not (isinstance(j[\"sweeper_cpu_s\"], float) and j[\"sweeper_window\"] == \"process\" and j[\"rc\"] == 3
              and r is not None and j[\"sweeper_cpu_s\"] == r[\"sweeper_cpu_s\"] and j[\"sweeper_refused\"] is None))"'
check "MED 12: ours_timed_run without the sweeper sourced refuses (rc 2) and never runs the command" \
  '( mkdir -p "$T/sw2"; unset -f sweeper_start sweeper_wait; ours_timed_run "$T/sw2" touch "$T/sw2/ran" ); [ $? = 2 ] &&
   [ ! -e "$T/sw2/ran" ]'
check "MED 12: a sweeper whose start sweep never comes: refused (rc 2) within its bound, the command never ran" \
  '( mkdir -p "$T/sw3"; source "$SW"; sweeper_start() { SWEEPER_PID=""; }; sweeper_wait() { :; }
     ours_timed_run "$T/sw3" touch "$T/sw3/ran" ); [ $? = 2 ] && [ ! -e "$T/sw3/ran" ] &&
   grep -q "no start sweep" "$T/sw3/timed.window"'
check "MED 12: a record the reader refuses (the sweeper at nice 0) gives timed.json sweeper_cpu_s null and the reason" \
  'mkdir -p "$T/sw4" && sed "s/ nice=19 / nice=0 /" "$T/sw1/timed.tracer.tsv" > "$T/sw4/timed.tracer.tsv" &&
   cp "$T/sw1/timed.window" "$T/sw4/" && ! ( source "$HERE/t3lib.sh"; ours_timed_record "$T/sw4" "$SR" ) &&
   python3 -B -c "import json, sys; j = json.load(open(\"$T/sw4/timed.json\"))
sys.exit(not (j[\"sweeper_cpu_s\"] is None and \"nice 0\" in (j[\"sweeper_refused\"] or \"\")))"'
check "MED 12: no window (the run never stamped one) gives timed.json sweeper_cpu_s null, naming the window" \
  'mkdir -p "$T/sw5" && cp "$T/sw1/timed.tracer.tsv" "$T/sw5/" && ! ( source "$HERE/t3lib.sh"; ours_timed_record "$T/sw5" "$SR" ) &&
   python3 -B -c "import json, sys; j = json.load(open(\"$T/sw5/timed.json\"))
sys.exit(not (j[\"sweeper_cpu_s\"] is None and \"window\" in (j[\"sweeper_refused\"] or \"\")))"'

echo "T3LIB SELF-TEST $pass/$((pass + fail)) $([ $fail = 0 ] && echo PASS || echo FAIL)"
[ $fail = 0 ]
