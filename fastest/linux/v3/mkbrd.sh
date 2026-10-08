#!/bin/bash
# mkbrd.sh xfs|btrfs MOUNTPOINT -- the V3 fire-check's block-device cells on a GitHub-hosted runner, which has no
# spare disk: XFS or btrfs made directly on brd's /dev/ram0 (no loop), so the probe's flush path has one layer and
# the mount's source is a block device, as on a rental's data disk. FIRE-CHECK ONLY: brd is a RAM disk with no cache
# and no drive behind it, the probe accepts it only with V3FLOOR_BRD=1 (firecheck.sh sets it on these cells) and
# run.sh's bound mode refuses it, so nothing measured here is ever credited (review 2 items 1(a) and 4).
# brd is loaded with two devices: ram0 for the cell, ram1 for mkfixtures.sh's brd refusal fixture.
# Proof before returning 0: the mount table reports the fstype with source /dev/ram0, reflink works, it is writable.
set -euo pipefail
fs=${1:?usage: mkbrd.sh xfs|btrfs MOUNTPOINT}
mnt=${2:?usage: mkbrd.sh xfs|btrfs MOUNTPOINT}
me="$(id -u):$(id -g)"
case $fs in xfs|btrfs) ;; *) echo "mkbrd: xfs or btrfs, not '$fs'" >&2; exit 2 ;; esac
[ -e /dev/ram0 ] || sudo modprobe brd rd_nr=2 rd_size=3145728
[ -e /dev/ram0 ] && [ -e /dev/ram1 ] || { echo "mkbrd: brd did not give /dev/ram0 and /dev/ram1" >&2; exit 1; }
if [ "$fs" = xfs ]; then sudo mkfs.xfs -f -q -m reflink=1 /dev/ram0; else sudo mkfs.btrfs -f -q /dev/ram0; fi
sudo mkdir -p "$mnt"
sudo mount /dev/ram0 "$mnt"
sudo chown "$me" "$mnt"
got=$(findmnt -n -o FSTYPE -T "$mnt") src=$(findmnt -n -o SOURCE -T "$mnt")
[ "$got" = "$fs" ] && [ "$src" = /dev/ram0 ] || { echo "mkbrd: $mnt is $got on $src" >&2; exit 1; }
probe="$mnt/.mkbrd-probe-$$"
head -c 65536 /dev/urandom > "$probe"
cp --reflink=always "$probe" "$probe.clone" || { echo "mkbrd: reflink refused on $fs" >&2; exit 1; }
rm -f "$probe" "$probe.clone"
echo "mkbrd: fs=$fs source=$src write_cache=[$(cat /sys/block/ram0/queue/write_cache)] fua=[$(cat /sys/block/ram0/queue/fua)] mount=$(findmnt -n -o OPTIONS -T "$mnt")"
