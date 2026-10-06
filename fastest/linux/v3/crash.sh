#!/bin/bash
# crash.sh V3FLOOR xfs|btrfs|ext4 OUT -- does one copy-arm op survive a crash right after it? (review 2 item 3)
# Each case gets a fresh 1 GiB loop with a fresh filesystem; the probe (V3FLOOR_FIRECHECK=1 --crash-op ARM) sets the
# arm up (a 1 MiB durable source, the clones' directory, both fsynced) and runs op 0 only, with no teardown; then:
#   xfs, ext4  xfs_io -x -c shutdown (no -f: the in-memory log is NOT flushed) -- what op 0's own fsyncs forced to the
#              device survives, nothing else; umount; mount (log recovery)
#   btrfs      the filesystem sits on dm-flakey (passing everything); right after op 0 the table is swapped, under
#              dmsetup suspend --nolockfs (no freeze, so nothing is synced), for drop_writes: every later write,
#              including the unmount's commit, is acknowledged and dropped; then the passing table, mount (log replay)
# and the clone c0 is read back: "survived" = it is 1 MiB and byte-equal to the source, else "lost"; the source's
# own bytes must be intact (src_ok), else the rig itself broke. Cases (check.py CRASH, predictions pre-registered in
# frontier/fastest/linux/v3/RESUME.md before the first run):
#   xfs    clone2b, cfr2b (controls: must survive), clone1b and clone1b-aim (an unrelated file dirtied and fsynced
#          between the create and the FICLONE; recorded), clone2b-mutant (--mutant-nosync: no fsync; must be lost)
#   btrfs  clone2b, cfr2b (must survive), clone1b (recorded), clone2b-mutant (must be lost)
#   ext4   cfr2b (must survive), cfr2b-mutant (must be lost)
# Blind spots, stated: a loop's backing file lives in the root filesystem's page cache, so this models "what the
# filesystem wrote before the crash point", not a drive's volatile cache; a write the device acknowledged is kept.
set -u
V3=$(readlink -f "${1:?usage: crash.sh V3FLOOR xfs|btrfs|ext4 OUT}") K=${2:?} OUT=${3:?}
case $K in xfs|btrfs|ext4) ;; *) echo "crash: xfs, btrfs or ext4, not $K" >&2; exit 2 ;; esac
mkdir -p "$OUT"
me="$(id -u):$(id -g)"
MIB=1048576

one() { # name arm [probe flags]
  local name=$1 arm=$2
  shift 2
  local img=/var/tmp/v3crash-$K-$name.img mnt=/mnt/v3crash-$K-$name dm=v3crash-$K-$name
  local dev fsdev sz="" oprc=-1 crc=-1 mrc=-1 pre=-1 post=missing same=no srcsha="" postsha="" src clone
  sudo rm -f "$img"
  sudo truncate -s 1G "$img"
  dev=$(sudo losetup --find --show "$img")
  fsdev=$dev
  case $K in
    xfs) sudo mkfs.xfs -f -q -m reflink=1 "$dev" ;;
    btrfs) sudo mkfs.btrfs -f -q "$dev" ;;
    ext4) sudo mkfs.ext4 -F -q "$dev" ;;
  esac
  if [ "$K" = btrfs ]; then
    sz=$(sudo blockdev --getsz "$dev")
    sudo dmsetup create "$dm" --table "0 $sz flakey $dev 0 86400 0"
    fsdev=/dev/mapper/$dm
  fi
  sudo mkdir -p "$mnt"
  sudo mount "$fsdev" "$mnt"
  sudo chown "$me" "$mnt"
  mkdir "$mnt/w"
  src=$mnt/w/$arm.src clone=$mnt/w/$arm.clones/c0
  V3FLOOR_FIRECHECK=1 timeout 120 "$V3" --crash-op "$arm" "$@" --dir "$mnt/w" --out "$OUT/$name.op"
  oprc=$?
  pre=$(stat -c %s "$clone" 2>/dev/null || echo missing)
  srcsha=$(sha256sum "$src" 2>/dev/null | cut -d' ' -f1)
  case $K in
    xfs|ext4) sudo xfs_io -x -c shutdown "$mnt"; crc=$? ;;
    btrfs) sudo dmsetup suspend --nolockfs "$dm" && sudo dmsetup load "$dm" --table "0 $sz flakey $dev 0 0 86400 1 drop_writes" \
             && sudo dmsetup resume "$dm"; crc=$? ;;
  esac
  sudo umount "$mnt"
  if [ "$K" = btrfs ]; then
    sudo dmsetup suspend "$dm" && sudo dmsetup load "$dm" --table "0 $sz linear $dev 0" && sudo dmsetup resume "$dm"
  fi
  sudo mount "$fsdev" "$mnt"
  mrc=$?
  if [ "$mrc" -eq 0 ]; then
    post=$(sudo stat -c %s "$clone" 2>/dev/null || echo missing)
    postsha=$(sudo sha256sum "$mnt/w/$arm.src" 2>/dev/null | cut -d' ' -f1)
    sudo cmp -s "$mnt/w/$arm.src" "$clone" 2>/dev/null && same=yes
    ls -la "$mnt/w" "$mnt/w/$arm.clones" 2>&1
    sudo umount "$mnt"
  fi
  [ "$K" = btrfs ] && sudo dmsetup remove "$dm"
  sudo losetup -d "$dev"
  sudo rm -f "$img"
  python3 -B - "$OUT/$name.json" "$name" "$arm" "$K" "$*" "$oprc" "$crc" "$mrc" "$pre" "$post" "$same" "$srcsha" "$postsha" <<'PY'
import json, sys
out, name, arm, kind, flags, oprc, crc, mrc, pre, post, same, s0, s1 = sys.argv[1:]
survived = post == "1048576" and same == "yes"
json.dump({"case": name, "arm": arm, "kind": kind, "flags": flags, "op_rc": int(oprc), "crash_rc": int(crc),
           "remount_rc": int(mrc), "clone_size_before": pre, "clone_size_after": post, "clone_equals_source": same == "yes",
           "src_ok": bool(s0) and s0 == s1 and int(oprc) == 0 and int(crc) == 0 and int(mrc) == 0,
           "result": "survived" if survived else "lost",
           "method": "dm-flakey drop_writes after op 0" if kind == "btrfs" else "xfs_io shutdown without a log flush"},
          open(out, "w"), indent=1)
print(name, "survived" if survived else "lost", "size", post, "equal", same)
PY
}

case $K in
  xfs)
    one clone2b clone2b
    one cfr2b cfr2b
    one clone1b clone1b
    one clone1b-aim clone1b --crash-aim
    one clone2b-mutant clone2b --mutant-nosync ;;
  btrfs)
    one clone2b clone2b
    one cfr2b cfr2b
    one clone1b clone1b
    one clone2b-mutant clone2b --mutant-nosync ;;
  ext4)
    one cfr2b cfr2b
    one cfr2b-mutant cfr2b --mutant-nosync ;;
esac
