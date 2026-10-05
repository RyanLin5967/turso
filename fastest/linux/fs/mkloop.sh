#!/bin/bash
# Make a filesystem for a run and prove what was made.
#
# usage: mkloop.sh <ext4|ext4loop|xfs|btrfs> <mountpoint> [loop size, default 12G]
#   ext4      the runner's own ext4 (no loop): <mountpoint> becomes a directory on it
#   ext4loop  ext4 on a loop device (same device path as xfs and btrfs, for a like-for-like cell)
#   xfs       XFS on a loop device, mkfs -m reflink=1
#   btrfs     btrfs on a loop device
# The loop's backing file sits on whichever of / and /mnt has more free space. A loop device turns
# a flush into an fsync of its backing file, so a sync on the loop filesystem still reaches the
# runner's disk; the loop's write-cache and direct-io modes are printed so the record shows it.
#
# Proof before returning 0 (else it exits 1 and says which):
#   - the mount table reports <fstype> for <mountpoint>;
#   - reflink (cp --reflink=always) succeeds on xfs and btrfs and is refused on ext4/ext4loop;
#   - the mountpoint is writable by the calling user.
set -euo pipefail
fs=${1:?usage: mkloop.sh <ext4|ext4loop|xfs|btrfs> <mountpoint> [size]}
mnt=${2:?usage: mkloop.sh <fs> <mountpoint> [size]}
size=${3:-12G}
me="$(id -u):$(id -g)"

case $fs in
  ext4)
    sudo mkdir -p "$mnt"
    sudo chown "$me" "$mnt"
    want=ext4 ;;
  ext4loop|xfs|btrfs)
    best=$(df --output=avail,target -B1 / /mnt 2>/dev/null | tail -n +2 | sort -n | tail -1 | awk '{print $2}')
    back="${best%/}/fastest-loop-$fs.img"
    sudo rm -f "$back"
    sudo truncate -s "$size" "$back"
    dev=$(sudo losetup --find --show "$back")
    case $fs in
      ext4loop) sudo mkfs.ext4 -F -q "$dev"; want=ext4 ;;
      xfs) sudo mkfs.xfs -f -q -m reflink=1 "$dev"; want=xfs ;;
      btrfs) sudo mkfs.btrfs -f -q "$dev"; want=btrfs ;;
    esac
    sudo mkdir -p "$mnt"
    sudo mount "$dev" "$mnt"
    sudo chown "$me" "$mnt"
    echo "loop: dev=$dev backing=$back backing_fs=$(findmnt -n -o FSTYPE -T "$best")"
    losetup -l -O NAME,BACK-FILE,DIO,LOG-SEC "$dev"
    echo "loop write_cache=[$(cat /sys/block/${dev##*/}/queue/write_cache 2>/dev/null)]" ;;
  *) echo "mkloop: unknown filesystem '$fs' (ext4|ext4loop|xfs|btrfs)" >&2; exit 2 ;;
esac

got=$(findmnt -n -o FSTYPE -T "$mnt")
[ "$got" = "$want" ] || { echo "mkloop: $mnt is $got, wanted $want" >&2; exit 1; }
probe="$mnt/.mkloop-probe-$$"
head -c 65536 /dev/urandom > "$probe" || { echo "mkloop: $mnt not writable" >&2; exit 1; }
if cp --reflink=always "$probe" "$probe.clone" 2>/dev/null; then reflink=yes; else reflink=no; fi
rm -f "$probe" "$probe.clone"
case $want in
  xfs|btrfs) [ $reflink = yes ] || { echo "mkloop: $want at $mnt refused a reflink" >&2; exit 1; } ;;
  ext4) [ $reflink = no ] || { echo "mkloop: ext4 at $mnt accepted a reflink" >&2; exit 1; } ;;
esac
echo "mkloop: fs=$fs fstype=$got reflink=$reflink mount=$(findmnt -n -o SOURCE,OPTIONS -T "$mnt")"
