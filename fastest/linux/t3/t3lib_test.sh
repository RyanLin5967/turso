#!/bin/bash
# t3lib_test.sh -- t3lib.sh's checks on fake sysfs trees and registries (fourth lane review LOW 8, 10, 11, 12, 16): the
# real-run preflight checks and the drift gate get tests that can fail, though only a real run reaches them.
# t3run's selftests stage runs it. Exit 0 iff every case holds.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# TEST EDIT, flagged (the first --dry-block run, 2026-10-15: t3run exports T3_IDENTITY_MODE=virtio and its selftests
# stage ran this file under it, so the default-refusal case saw the virtio identity): every case starts from no mode,
# and the cases that need it set it themselves
unset T3_IDENTITY_MODE
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
# a case that needs Linux (/proc, Linux device numbers, mkfs.ext4 with sudo) says so: off Linux it is N/A-on-<platform>,
# printed and never counted as a pass (gates/platforms.tsv, the platform-inheritance class, 2026-10-15)
na=0
linux_check() { if [ "$(uname -s)" = Linux ]; then check "$@"; else na=$((na + 1)); echo "T3LIB self-test N/A-on-$(uname -s | tr A-Z a-z): $1"; fi; }
# the host-property class (2026-10-18, a real RHEL 8.10 host with no root): a case that needs root states it, and on a
# host without passwordless sudo it is N/A-on-noroot, printed and never counted as a pass, as linux_check is off Linux
# sudo_state (linux-rev-uw1 LOW 6): only a sudo that asks for a password is N/A-on-noroot; a missing or broken one runs
# the case, which then fails, so it is never mistaken for a no-root host
root_check() { if [ "$(uname -s)" != Linux ]; then linux_check "$@"; return; fi
               case $(sudo_state) in
                 password|denied) na=$((na + 1)); echo "T3LIB self-test N/A-on-noroot: $1" ;;
                 *) check "$@" ;;
               esac; }
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
# --dry-block on a VM (its first probe, 2026-10-15, lima vz): a virtio-blk disk has no wwid, model, serial or firmware
# in sysfs (block/serial is empty), so dev_identity refuses it. Under T3_IDENTITY_MODE=virtio, which t3run sets only
# for a DRY-VIRT run, a virtio-blk disk (device/modalias virtio:d00000002...) is identified by its diskseq, which the
# kernel gives each disk it attaches and never reuses while it runs, plus its MAJ:MIN and the virtio vendor
mkdir -p "$T/sys/block/vdb/device"; echo 253:16 > "$T/sys/block/vdb/dev"; echo 10 > "$T/sys/block/vdb/diskseq"
: > "$T/sys/block/vdb/serial"; echo 0x1af4 > "$T/sys/block/vdb/device/vendor"
echo "virtio:d00000002v00001AF4" > "$T/sys/block/vdb/device/modalias"
check "dry-block: a virtio-blk disk with no wwid still refuses by default (rc 2, naming wwid): a real run is unchanged" \
  'refuses2 dev_identity "$T/sys" vdb && grep -q wwid "$T/out.txt"'
check "dry-block: under T3_IDENTITY_MODE=virtio a virtio-blk disk is its diskseq, MAJ:MIN and vendor" \
  '[ "$(T3_IDENTITY_MODE=virtio dev_identity "$T/sys" vdb)" = "wwid=virtio-diskseq:10${NL}dev=253:16${NL}model=virtio-blk 0x1af4${NL}serial=diskseq-10${NL}firmware=virtio" ]'
mkdir -p "$T/sys/block/sdz/device"; echo 8:96 > "$T/sys/block/sdz/dev"; echo 11 > "$T/sys/block/sdz/diskseq"
check "dry-block: under T3_IDENTITY_MODE=virtio a disk that is not virtio-blk still refuses (rc 2)" \
  'refuses2 env T3_IDENTITY_MODE=virtio bash -c "source \"$HERE/t3lib.sh\"; dev_identity \"$T/sys\" sdz"'
check "dry-block: under T3_IDENTITY_MODE=virtio a virtio-blk disk with no diskseq refuses (rc 2)" \
  'mv "$T/sys/block/vdb/diskseq" "$T/vdb-diskseq"; refuses2 env T3_IDENTITY_MODE=virtio bash -c "source \"$HERE/t3lib.sh\"; dev_identity \"$T/sys\" vdb"; r=$?; mv "$T/vdb-diskseq" "$T/sys/block/vdb/diskseq"; [ $r = 0 ]'
check "dry-block: a re-attached virtio disk (diskseq 10 -> 12) is not the recorded one: dev_unchanged's identity check refuses" \
  'T3_IDENTITY_MODE=virtio dev_identity "$T/sys" vdb > "$T/idv.txt" && echo 12 > "$T/sys/block/vdb/diskseq" &&
   now=$(T3_IDENTITY_MODE=virtio dev_identity "$T/sys" vdb) && [ "$now" != "$(cat "$T/idv.txt")" ]; r=$?; echo 10 > "$T/sys/block/vdb/diskseq"; [ $r = 0 ]'
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
linux_check "LOW 5: production t3_rdev reads a real node's MAJ:MIN in decimal (/dev/null is 1:3)" \
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
# the lead's --dry-block ruling (DECISIONS, 2026-10-15): a dry run on a VM's virtual disk passes REG = DRY-VIRT, which
# skips the A17 registration (a dry run is never credited; t3run passes it only under --dry-block, and the stamp is
# refused for credit by summarize); every other step and refusal of device_make is unchanged
echo "write back" > "$T/sys/block/nvme5n1/queue/write_cache"
dmv() { : > "$HL"; rm -f "$T/made.txt"; FAKE_TYPE="" FAKE_UUID="" FAKE_MOUNTED=""
        device_make "$T/dev/nvme5n1" "$REAL5" "$T/sys" nvme5n1 "$T/id5.txt" no DRY-VIRT xfs U-1 /mnt/t3-xfs "$T/made.txt"; }
check "dry-block: device_make with REG DRY-VIRT on a write-back drive with nothing registered makes and mounts it, in order" \
  'dmv && [ "$(steps)" = "$P$G${M}mount xfs $REAL5 /mnt/t3-xfs,$U" ] && grep -q "DRY-VIRT" "$T/out.txt"'
ident nvme5n1 eui.0099 259:5 PM9A3 S1 GDC5
check "dry-block: DRY-VIRT keeps the identity refusal (a changed wwid refuses, rc 2, no mkfs)" \
  'refuses2 dmv && grep -q wwid "$T/out.txt" && [ "$(steps)" = "$P$G$U" ]'
ident nvme5n1 eui.0001 259:5 PM9A3 S1 GDC5
echo "write through" > "$T/sys/block/nvme5n1/queue/write_cache"
# virt_ok MODE VIRT: a real run refuses any virtualization (gate-6 review 8, unchanged); --dry-block allows it
check "dry-block: virt_ok real none passes" 'virt_ok real none'
for v in apple kvm qemu microsoft vmware ""; do
  check "dry-block: virt_ok real '$v' refuses (rc 2), naming bare metal" "refuses2 virt_ok real '$v' && grep -q 'bare metal' \"\$T/out.txt\""
done
check "dry-block: virt_ok dryblock apple passes (the VM is the point)" 'virt_ok dryblock apple'
check "dry-block: virt_ok with an unknown mode refuses (rc 2)" 'refuses2 virt_ok frobnicate none'
# host_room_ok FREE_KB [MIN_GB]: the Mac host must have at least 35 GiB free before a VM dry run builds
check "dry-block: host_room_ok 35 GiB free passes" 'host_room_ok $((35 * 1024 * 1024))'
check "dry-block: host_room_ok 1 KiB under 35 GiB refuses (rc 2), naming 35" 'refuses2 host_room_ok $((35 * 1024 * 1024 - 1)) && grep -q 35 "$T/out.txt"'
check "dry-block: host_room_ok on a reading that is no number refuses (rc 2)" 'refuses2 host_room_ok "n/a"'
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
root_check "LOW 6: production t3_blkid reads TYPE and UUID from a real ext4 image (needs mkfs.ext4 and sudo -n)" \
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
linux_check "MED 12: ours_timed_run returns the command's rc (3) and records its window, t0 < t1, both inside the sweeps" \
  'swrun 1 bash -c "sleep 0.5; exit 3"; [ $? = 3 ] && grep -q "^sweeper pid=[0-9]* nice=19 " "$T/sw1/timed.tracer.tsv" &&
   grep -q " rc=3$" "$T/sw1/timed.window" &&
   swnums "$T/sw1" | tr "\n" " " | awk "{ exit !(\$1 < \$2 && \$3 <= \$1 && \$2 <= \$4) }"'
linux_check "MED 12: the one reader accepts that record over that window (sweeper_record.py record, rc 0)" \
  'python3 -B "$SR" record "$T/sw1/timed.tracer.tsv" $(swnums "$T/sw1" | head -1)'
# TEST EDIT, flagged as a SPEC CHANGE (bb96279f3: fastest_profile stamps its measured span): the record is read over
# result/summary.json's tm0/tm1_realtime_s, here the wrapper's own bounds (inside the sweeps), not the process window
swsum() { mkdir -p "$1/result"; awk '{for (i = 1; i <= NF; i++) { split($i, kv, "="); if (kv[1] == "t0") a = kv[2]
          if (kv[1] == "t1") b = kv[2] }} END { printf "{\"tm0_realtime_s\": %s, \"tm1_realtime_s\": %s}\n", a, b }' \
          "$1/timed.window" > "$1/result/summary.json"; }
swsum "$T/sw1"
linux_check "MED 12: ours_timed_record writes timed.json: sweeper_cpu_s a number over the MEASURED span, the reader's own value" \
  '( source "$HERE/t3lib.sh"; ours_timed_record "$T/sw1" "$SR" ) && python3 -B -c "
import json, sys; sys.path.insert(0, \"$HERE/../gates\"); import sweeper_record as s
j = json.load(open(\"$T/sw1/timed.json\")); m = json.load(open(\"$T/sw1/result/summary.json\"))
r, why = s.sweeper_record(open(\"$T/sw1/timed.tracer.tsv\").read().splitlines(), j[\"t0\"], j[\"t1\"])
sys.exit(not (isinstance(j[\"sweeper_cpu_s\"], float) and j[\"sweeper_window\"] == \"measured\" and j[\"rc\"] == 3
              and (j[\"t0\"], j[\"t1\"]) == (m[\"tm0_realtime_s\"], m[\"tm1_realtime_s\"])
              and r is not None and j[\"sweeper_cpu_s\"] == r[\"sweeper_cpu_s\"] and j[\"sweeper_refused\"] is None))"'
check "MED 12: ours_timed_run without the sweeper sourced refuses (rc 2) and never runs the command" \
  '( mkdir -p "$T/sw2"; unset -f sweeper_start sweeper_wait; ours_timed_run "$T/sw2" touch "$T/sw2/ran" ); [ $? = 2 ] &&
   [ ! -e "$T/sw2/ran" ]'
check "MED 12: a sweeper whose start sweep never comes: refused (rc 2) within its bound, the command never ran" \
  '( mkdir -p "$T/sw3"; source "$SW"; sweeper_start() { SWEEPER_PID=""; }; sweeper_wait() { :; }
     ours_timed_run "$T/sw3" touch "$T/sw3/ran" ); [ $? = 2 ] && [ ! -e "$T/sw3/ran" ] &&
   grep -q "no start sweep" "$T/sw3/timed.window"'
linux_check "MED 12: a record the reader refuses (the sweeper at nice 0) gives timed.json sweeper_cpu_s null and the reason" \
  'mkdir -p "$T/sw4" && sed "s/ nice=19 / nice=0 /" "$T/sw1/timed.tracer.tsv" > "$T/sw4/timed.tracer.tsv" &&
   cp "$T/sw1/timed.window" "$T/sw4/" && swsum "$T/sw4" && ! ( source "$HERE/t3lib.sh"; ours_timed_record "$T/sw4" "$SR" ) &&
   python3 -B -c "import json, sys; j = json.load(open(\"$T/sw4/timed.json\"))
sys.exit(not (j[\"sweeper_cpu_s\"] is None and \"nice 0\" in (j[\"sweeper_refused\"] or \"\")))"'
# foreign_cpu.py classes a process as the run's own (sut) by FASTEST_CELL in /proc/PID/environ, which is the environment
# at exec: a sweeper forked straight from t3run's shell would read as foreign, while the competitors' (inside the exec'd
# run_system.sh) reads as sut. ours_timed_exec runs ours_timed_run in an exec'd bash, so ours' sweeper is classed as theirs
# the command, run once the start sweep is written: exit 0 iff the sweeper's own environ names the cell
mkdir -p "$T/sw6"
cat > "$T/sw6-env.sh" << 'EOF'
p=$(awk '$1 == "sweeper" { sub(/^pid=/, "", $2); print $2 }' "$1/timed.tracer.tsv")
[ -n "$p" ] && tr '\0' '\n' < "/proc/$p/environ" | grep -qx "FASTEST_CELL=$2"
EOF
linux_check "MED 12: ours_timed_exec's sweeper carries the exported FASTEST_CELL in its exec-time environ (sut, as theirs)" \
  '( export FASTEST_CELL=sw6-cell; ours_timed_exec "$T/sw6" bash "$T/sw6-env.sh" "$T/sw6" sw6-cell ) &&
   grep -q " rc=0$" "$T/sw6/timed.window"'
check "MED 12: ours_timed_exec with no gates/sweeper.sh beside t3lib.sh refuses (rc 2), the command never ran" \
  'mkdir -p "$T/lib7/t3" "$T/sw7" && cp "$HERE/t3lib.sh" "$T/lib7/t3/" &&
   ( source "$T/lib7/t3/t3lib.sh"; ours_timed_exec "$T/sw7" touch "$T/sw7/ran" ); [ $? = 2 ] && [ ! -e "$T/sw7/ran" ]'
check "MED 12 fix 7: ours_timed_exec whose gates/sweeper.sh fails to source refuses (rc 2, not 1), the command never ran" \
  'mkdir -p "$T/lib8/t3" "$T/lib8/gates" "$T/sw8" && cp "$HERE/t3lib.sh" "$T/lib8/t3/" &&
   echo "return 1" > "$T/lib8/gates/sweeper.sh" &&
   ( source "$T/lib8/t3/t3lib.sh"; ours_timed_exec "$T/sw8" touch "$T/sw8/ran" ); [ $? = 2 ] && [ ! -e "$T/sw8/ran" ]'
linux_check "MED 12: no window (the run never stamped one) gives timed.json sweeper_cpu_s null, naming the window" \
  'mkdir -p "$T/sw5" && cp "$T/sw1/timed.tracer.tsv" "$T/sw5/" && ! ( source "$HERE/t3lib.sh"; ours_timed_record "$T/sw5" "$SR" ) &&
   python3 -B -c "import json, sys; j = json.load(open(\"$T/sw5/timed.json\"))
sys.exit(not (j[\"sweeper_cpu_s\"] is None and \"window\" in (j[\"sweeper_refused\"] or \"\")))"'

# ours_timed_record on a synthetic record (no /proc needed; the summarize self-test's hand-computed one: sweeps every
# 0.05 s from 100.00, cpu = index, clk_tck 100; measured span [100.1, 100.3] gives 4 ticks = 0.04 s), in the process
# window [100.05, 100.4]
synth() { mkdir -p "$1/result"; { printf 'roles cmd=500 srv=none tree=children\nsweeper pid=600 nice=19 clk_tck=100\n'
          for i in 0 1 2 3 4 5 6 7 8 9; do printf 'SWEEP mid %s cmd=3:0 srv=0:0 cpu=%s\n' "$(echo "100 + 0.05 * $i" | bc)" "$i"; done
          } > "$1/timed.tracer.tsv"; echo "t0=100.05 t1=$2 rc=0" > "$1/timed.window"
          [ -n "$3" ] && echo "$3" > "$1/result/summary.json"; return 0; }
synth "$T/rec1" 100.4 '{"tm0_realtime_s": 100.1, "tm1_realtime_s": 100.3}'
check "measured span: ours_timed_record over [100.1, 100.3] gives 0.04 s, window measured, process bounds kept" \
  '( source "$HERE/t3lib.sh"; ours_timed_record "$T/rec1" "$SR" ) && python3 -B -c "import json, sys
j = json.load(open(\"$T/rec1/timed.json\"))
sys.exit(not (j[\"sweeper_cpu_s\"] == 0.04 and j[\"sweeper_window\"] == \"measured\" and (j[\"t0\"], j[\"t1\"]) == (100.1, 100.3)
              and (j[\"process_t0\"], j[\"process_t1\"]) == (100.05, 100.4) and j[\"rc\"] == 0))"'
synth "$T/rec2" 100.4 ""
check "measured span: no result/summary.json tm0/tm1 gives sweeper_cpu_s null, naming tm0_realtime_s, rc 1" \
  '! ( source "$HERE/t3lib.sh"; ours_timed_record "$T/rec2" "$SR" ) && python3 -B -c "import json, sys
j = json.load(open(\"$T/rec2/timed.json\"))
sys.exit(not (j[\"sweeper_cpu_s\"] is None and \"tm0_realtime_s\" in (j[\"sweeper_refused\"] or \"\")))"'
synth "$T/rec3" 100.25 '{"tm0_realtime_s": 100.1, "tm1_realtime_s": 100.3}'
check "measured span: a span ending after the process window gives sweeper_cpu_s null, naming outside, rc 1" \
  '! ( source "$HERE/t3lib.sh"; ours_timed_record "$T/rec3" "$SR" ) && python3 -B -c "import json, sys
j = json.load(open(\"$T/rec3/timed.json\"))
sys.exit(not (j[\"sweeper_cpu_s\"] is None and \"outside\" in (j[\"sweeper_refused\"] or \"\")))"'

# CLASS: the host acts during a measured window (the second full --dry-block run, 2026-10-15, btrfs pg18: an
# unattended-upgrade, started by apt-daily.timer, voided attempt 1 as foreign CPU; lima-guestagent's SyncTime stepped
# CLOCK_REALTIME every 10-20 s, "drift was 117.942399ms" in its journal, and stracecount refused the windows). The one
# design change: host_quiet stops every active systemd timer and every known clock adjuster before the first cell,
# then MEASURES the outcome (CLOCK_REALTIME minus CLOCK_MONOTONIC must hold still over a watch) and refuses
# otherwise; host_unquiet restarts what it stopped. Faked here: systemctl (T3_SYSTEMCTL) and the clock (T3_CLOCKDELTA)
mkdir -p "$T/sc/active" "$T/sc/sticky"
cat > "$T/systemctl" <<'SC'
#!/bin/bash
D=$(dirname "$0")/sc
echo "$*" >> "$D/calls.txt"
case $1 in
  list-timers) [ -f "$D/timers.broken" ] && exit 1; cat "$D/timers.txt" ;;
  is-active) if [ -e "$D/active/$2" ]; then echo active; else echo inactive; exit 3; fi ;;
  stop) [ -e "$D/sticky/$2" ] || rm -f "$D/active/$2" ;;
  start) touch "$D/active/$2" ;;
  *) exit 2 ;;
esac
SC
chmod +x "$T/systemctl"
cat > "$T/clockdelta" <<'CD'
#!/bin/bash
f=$(dirname "$0")/clock.n; n=$(cat "$f" 2>/dev/null || echo 0); echo $((n + 1)) > "$f"
sed -n "$((n + 1))p" "$(dirname "$0")/clock.txt"
CD
chmod +x "$T/clockdelta"
quiet_setup() { # quiet_setup "ACTIVE UNITS" "STICKY UNITS" "CLOCK VALUES"
  rm -rf "$T/sc/active" "$T/sc/sticky" "$T/sc/calls.txt" "$T/sc/timers.broken" "$T/clock.n" "$T/quiet.tsv"
  mkdir -p "$T/sc/active" "$T/sc/sticky"
  local u; for u in $1; do touch "$T/sc/active/$u"; done; for u in $2; do touch "$T/sc/sticky/$u"; done
  printf '%s\n' $3 > "$T/clock.txt"
  printf '%s\n' \
    "Fri 2026-10-16 06:14:21 EDT 10h Thu 2026-10-15 09:29:18 EDT - apt-daily-upgrade.timer apt-daily-upgrade.service" \
    "Fri 2026-10-16 16:06:44 EDT 20h Thu 2026-10-15 18:55:34 EDT 21min ago apt-daily.timer apt-daily.service" \
    "- - - - snapd.snap-repair.timer snapd.snap-repair.service" > "$T/sc/timers.txt"
}
QENV='T3_SYSTEMCTL=$T/systemctl T3_CLOCKDELTA=$T/clockdelta T3_QUIET_WATCH_S=0'
quiet_setup "apt-daily.timer apt-daily.service apt-daily-upgrade.timer systemd-timesyncd lima-guestagent" "" "0.5 0.5"
check "host_quiet stops each active timer, its running service and each active clock adjuster, records them, rc 0" \
  "env $QENV \"$BASH\" -c 'source \"\$0\"; host_quiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\" &&
   [ -z \"\$(ls \"$T/sc/active\")\" ] && grep -qx \"timer${TAB}apt-daily.timer\" \"$T/quiet.tsv\" &&
   grep -qx \"service${TAB}apt-daily.service\" \"$T/quiet.tsv\" && grep -qx \"clock${TAB}lima-guestagent\" \"$T/quiet.tsv\" &&
   grep -qx \"clock${TAB}systemd-timesyncd\" \"$T/quiet.tsv\" && ! grep -q snap-repair \"$T/quiet.tsv\""
# TEST EDIT, flagged (the host-property class, 2026-10-18): the listing is sorted in C collation, as the expected text
# is; RHEL 8.10's en_US ls order put apt-daily.timer first, a locale's order and not a different set
check "host_unquiet restarts exactly the timers and clock adjusters it stopped (a timer starts its service itself)" \
  "env $QENV \"$BASH\" -c 'source \"\$0\"; host_unquiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\" &&
   [ \"\$(ls \"$T/sc/active\" | LC_ALL=C sort | tr '\n' ' ')\" = 'apt-daily-upgrade.timer apt-daily.timer lima-guestagent systemd-timesyncd ' ]"
quiet_setup "apt-daily.timer" "apt-daily.timer" "0.5 0.5"
check "a timer still active after its stop refuses, naming it" \
  "! env $QENV \"$BASH\" -c 'source \"\$0\"; host_quiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\" && grep -q 'apt-daily.timer' \"$T/out.txt\""
quiet_setup "chrony" "chrony" "0.5 0.5"
check "a clock adjuster still active after its stop refuses, naming it" \
  "! env $QENV \"$BASH\" -c 'source \"\$0\"; host_quiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\" && grep -q chrony \"$T/out.txt\""
quiet_setup "" "" "0.5 0.62"
check "the outcome: CLOCK_REALTIME moving 0.12 s against MONOTONIC over the watch, every adjuster stopped, refuses" \
  "! env $QENV \"$BASH\" -c 'source \"\$0\"; host_quiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\" && grep -q CLOCK_REALTIME \"$T/out.txt\""
quiet_setup "" "" "0.5"
check "an unreadable clock sample refuses (never a pass)" \
  "! env $QENV \"$BASH\" -c 'source \"\$0\"; host_quiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\""
quiet_setup "" "" ""
# dry run 10 (2026-10-16): the stage refused "cannot sample the clocks ('' then '')" -- every case above fakes the
# clock, so none ran the default sampler, whose quoting had broken. This one runs the real sampler (python3's
# CLOCK_REALTIME and CLOCK_MONOTONIC, both on macOS and Linux)
check "the DEFAULT clock sampler (no T3_CLOCKDELTA) samples, and a still clock passes" \
  "env -u T3_CLOCKDELTA T3_SYSTEMCTL=$T/systemctl T3_QUIET_WATCH_S=0 T3_QUIET_MAX_S=0.05 \"\$BASH\" -c 'source \"\$0\"; host_quiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\""
quiet_setup "" "" "0.5 0.5"; touch "$T/sc/timers.broken"
check "systemctl list-timers failing refuses (the guard cannot read its input)" \
  "! env $QENV \"$BASH\" -c 'source \"\$0\"; host_quiet \"\$1\"' \"$HERE/t3lib.sh\" \"$T/quiet.tsv\""

# the owed ours fixture record (2026-10-16): ours times against gen_seed's parent, as every competitor does, and its
# fixture.json is comp's fixture.py write over ours' own read-back (fastest_profile --dump-trunk). The driver's part is
# faked here by its outputs: summary.json's fixture.live_branches and a dump made from gen_seed's own stream
mkdir -p "$T/fx"
check "ours_parent writes gen_seed's two streams and their digests, as the competitors' seeds feed them" \
  '( source "$HERE/t3lib.sh"; ours_parent "$T/fx/d/parent" 300 7 ) && [ "$(grep -c "" "$T/fx/d/parent/age.sql")" = 7 ] &&
   head -1 "$T/fx/d/parent/seed.sql" | grep -q "^CREATE TABLE t " && [ -s "$T/fx/d/parent/sql.sha256" ] &&
   [ -s "$T/fx/d/parent/age.sha256" ]'
fxdump() { # fxdump D ROWS AGE: D/parent/trunk.tsv as the aged parent's rows, from gen_seed's own lines
  python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import gen_seed as g
r, k = int(sys.argv[2]), int(sys.argv[3]); t = {}
for ln in g.sql_lines(r):
    if ln.startswith("INSERT"):
        for tup in ln[ln.index("VALUES ") + 7:-1].split("),("):
            i, v, pad = tup.strip("()").split(",", 2); t[int(i)] = [int(v), pad.strip(chr(39))]
for ln in g.age_lines(r, k, 1):
    v, i = ln[len("UPDATE t SET v = "):-1].split(" WHERE id = "); t[int(i)][0] = int(v)
sys.stdout.write("".join("%d\t%d\t%s\n" % (i, v, p) for i, (v, p) in sorted(t.items())))' "$HERE/../competitors" "$2" "$3" \
    > "$1/parent/trunk.tsv"; }
mkdir -p "$T/fx/d/result" "$T/fx/store"
printf '{"fixture": {"live_branches": 4, "prebranch": 4}}\n' > "$T/fx/d/result/summary.json"
fxdump "$T/fx/d" 300 7
check "ours_fixture_record writes fixture.json: comp's schema, the MEASURED live count, ours' read-back equal to the generator's" \
  '( source "$HERE/t3lib.sh"; ours_fixture_record "$T/fx/d" 300 7 4 "$T/fx/store" ) && python3 -B -c "import json, sys
j = json.load(open(\"$T/fx/d/result/fixture.json\"))
sys.exit(not (j[\"system\"] == \"ours\" and j[\"rows\"] == 300 and j[\"age_updates\"] == 7 and j[\"prebranch\"] == 4
              and j[\"live_branches\"] == 4 and j[\"readback\"] == j[\"expected_readback\"]
              and j[\"stream_sha256\"] == j[\"expected_streams\"] and len(j[\"gen_seed_sha256\"]) == 64))"'
awk -F '\t' -v OFS='\t' 'NR == 1 { $2 = $2 + 1 } { print }' "$T/fx/d/parent/trunk.tsv" > "$T/fx/t.tsv" &&
  mv "$T/fx/t.tsv" "$T/fx/d/parent/trunk.tsv"  # one row's v moved: another parent than the generator's
check "ours_fixture_record refuses (rc != 0) when ours read back another parent, and still writes the record" \
  '! ( source "$HERE/t3lib.sh"; ours_fixture_record "$T/fx/d" 300 7 4 "$T/fx/store" ) && python3 -B -c "import json, sys
j = json.load(open(\"$T/fx/d/result/fixture.json\")); sys.exit(not (j[\"readback\"] != j[\"expected_readback\"]))"'
printf '{"windows": []}\n' > "$T/fx/d/result/summary.json"
check "ours_fixture_record refuses a summary.json with no fixture record (the driver ran without the parent)" \
  '! ( source "$HERE/t3lib.sh"; ours_fixture_record "$T/fx/d" 300 7 4 "$T/fx/store" ) && grep -q "no fixture" "$T/out.txt"'

# linux-rev-uw1 LOW 6: root_check told a missing or broken sudo from one that needs a password only by "sudo -n true
# failed". sudo_state classifies it (T3_SUDO_BIN names the sudo to ask; t3run unsets it): absent and error are FAILs
# of the root case, password alone is N/A-on-noroot
mkdir -p "$T/sudo"
printf '#!/bin/sh\necho "sudo: a password is required" >&2\nexit 1\n' > "$T/sudo/pw"
printf '#!/bin/sh\nexit 0\n' > "$T/sudo/ok"
printf '#!/bin/sh\necho "sudo: unable to resolve host x" >&2\nexit 1\n' > "$T/sudo/err"
chmod +x "$T/sudo/pw" "$T/sudo/ok" "$T/sudo/err"
check "LOW 6: sudo_state: no sudo at all is 'absent'" '[ "$(T3_SUDO_BIN=$T/sudo/none sudo_state)" = absent ]'
check "LOW 6: sudo_state: a sudo that needs a password is 'password'" '[ "$(T3_SUDO_BIN=$T/sudo/pw sudo_state)" = password ]'
check "LOW 6: sudo_state: passwordless sudo is 'ok'" '[ "$(T3_SUDO_BIN=$T/sudo/ok sudo_state)" = ok ]'
# linux-rev-uw2 LOW 7: sudo's message is localised (de_DE gave "Ein Passwort ist notwendig"); sudo_state asks it in C
printf '#!/bin/sh\nif [ "${LC_ALL:-}" = C ]; then echo "sudo: a password is required" >&2; else echo "sudo: Ein Passwort ist notwendig" >&2; fi\nexit 1\n' > "$T/sudo/de"
chmod +x "$T/sudo/de"
check "uw2 LOW 7: sudo_state reads a localised sudo (de_DE) as 'password', asking it in the C locale" \
  '[ "$(LANG=de_DE.UTF-8 LC_ALL=de_DE.UTF-8 T3_SUDO_BIN=$T/sudo/de sudo_state)" = password ]'
check "LOW 6: sudo_state: any other sudo failure is an error, named" \
  'case "$(T3_SUDO_BIN=$T/sudo/err sudo_state)" in error:*resolve*) true ;; *) false ;; esac'
# found on ecetesla3 (linux-u4-green-ecetesla3, 2026-10-18): a sudo that REFUSES this user (not in sudoers; that host
# printed a sudoers parse error and "I'm sorry r226lin. I'm afraid I can't do that") is no root, like one that wants a
# password: 'denied', N/A-on-noroot, not an error FAIL. The text below is that host's, verbatim
printf '#!/bin/sh\necho "/etc/sudoers:42:9: expected host name" >&2\necho "sudo: I'"'"'m sorry r226lin. I'"'"'m afraid I can'"'"'t do that" >&2\nexit 1\n' > "$T/sudo/denied"
printf '#!/bin/sh\necho "r226lin is not in the sudoers file.  This incident will be reported." >&2\nexit 1\n' > "$T/sudo/notin"
chmod +x "$T/sudo/denied" "$T/sudo/notin"
check "u4 host: sudo_state reads a sudo that refuses this user (ecetesla3's text) as 'denied'" \
  '[ "$(T3_SUDO_BIN=$T/sudo/denied sudo_state)" = denied ]'
check "u4 host: sudo_state reads 'not in the sudoers file' as 'denied'" '[ "$(T3_SUDO_BIN=$T/sudo/notin sudo_state)" = denied ]'
check "u4 host: root_check counts a root case N/A-on-noroot under a denying sudo, and runs nothing" \
  '( T3_SUDO_BIN=$T/sudo/denied; na=0; root_check "probe" "touch $T/sudo/ran" > "$T/sudo/rc.out"; [ $na = 1 ] && [ ! -e "$T/sudo/ran" ] && grep -q "N/A-on-noroot: probe" "$T/sudo/rc.out" )'

echo "TALLY ran=$((pass + fail)) pass=$pass na=${na:-0} fail=$fail"  # gates/tally.py
echo "T3LIB SELF-TEST $pass/$((pass + fail)) $([ $fail = 0 ] && echo PASS || echo FAIL)$([ $na = 0 ] || echo " ($na N/A: off Linux or without root)")"
# an N/A is its own outcome, exit 3, never a pass (linux-rev-uw1 HIGH 1): 0 only when every case ran and held
[ $fail = 0 ] || exit 1
[ $na = 0 ] || exit 3
