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
# own bytes must be intact (src_ok), else the rig itself broke. The crash is PROVEN per case: a sentinel file written
# after the crash point must not exist after the remount (xfs/ext4: the write fails on the shut-down filesystem;
# btrfs: it is acknowledged and dropped). A case is refused, and nothing is run, unless the filesystem under test is
# mounted where the op and the crash act: a failed mount would otherwise run the op and the shutdown on the ROOT
# filesystem (fresh review B-H1). A failed umount refuses too (a remount would read the uncrashed filesystem).
# Cases (check.py CRASH, predictions pre-registered in frontier/fastest/linux/v3/RESUME.md before the first run):
#   xfs    clone2b, cfr2b (controls: must survive), clone1b and clone1b-aim (1 B appended to an unrelated file and
#          fsynced between the create and the FICLONE, so the log is forced past the create; recorded),
#          clone2b-mutant (--mutant-nosync: no fsync; must be lost)
#   btrfs  clone2b, cfr2b (must survive), clone1b (recorded), clone2b-mutant (must be lost)
#   ext4   cfr2b (must survive), cfr2b-mutant (must be lost)
# ext4 is made with lazy_itable_init=0: a lazyinit commit in the crash window would commit the op's transaction too.
# Blind spots, stated: a loop's backing file lives in the root filesystem's page cache, so this models "what the
# filesystem wrote before the crash point", not a drive's volatile cache; a write the device acknowledged is kept.
set -u
V3=$(readlink -f "${1:?usage: crash.sh V3FLOOR xfs|btrfs|ext4 OUT}") K=${2:?} OUT=${3:?}
case $K in xfs|btrfs|ext4) ;; *) echo "crash: xfs, btrfs or ext4, not $K" >&2; exit 2 ;; esac
mkdir -p "$OUT"
me="$(id -u):$(id -g)"

mounted_on() { # dev mountpoint -> 0 iff dev (by path or its canonical /dev node) is mounted exactly there
  local s
  s=$(findmnt -n -o SOURCE --mountpoint "$2" 2>/dev/null | tail -1)
  [ -n "$s" ] && { [ "$s" = "$1" ] || [ "$(readlink -f "$s")" = "$(readlink -f "$1")" ]; }
}

one() { # name arm [probe flags]
  local name=$1 arm=$2
  shift 2
  local img=/var/tmp/v3crash-$K-$name.img mnt=/mnt/v3crash-$K-$name dm=v3crash-$K-$name
  local dev fsdev sz="" oprc=-1 crc=-1 urc=-1 mrc=-1 pre=missing post=missing same=no srcsha="" postsha="" src clone
  local why="" sentinel_written=no sentinel_after=unknown t_mount="" t_op="" t_crash=""
  sudo rm -f "$img"
  sudo truncate -s 1G "$img"
  dev=$(sudo losetup --find --show "$img")
  fsdev=$dev
  case $K in
    xfs) sudo mkfs.xfs -f -q -m reflink=1 "$dev" ;;
    btrfs) sudo mkfs.btrfs -f -q "$dev" ;;
    ext4) sudo mkfs.ext4 -F -q -E lazy_itable_init=0,lazy_journal_init=0 "$dev" ;;
  esac
  if [ "$K" = btrfs ]; then
    sz=$(sudo blockdev --getsz "$dev")
    sudo dmsetup create "$dm" --table "0 $sz flakey $dev 0 86400 0" && sudo udevadm settle
    fsdev=/dev/mapper/$dm
  fi
  sudo mkdir -p "$mnt"
  sudo mount "$fsdev" "$mnt"
  t_mount=$(date -u +%FT%T.%NZ)
  src=$mnt/w/$arm.src clone=$mnt/w/$arm.clones/c0
  if ! mounted_on "$fsdev" "$mnt"; then
    why="the filesystem under test is not mounted at $mnt: nothing run"
  else
    sudo chown "$me" "$mnt"
    mkdir "$mnt/w"
    V3FLOOR_FIRECHECK=1 timeout 120 "$V3" --crash-op "$arm" "$@" --dir "$mnt/w" --out "$OUT/$name.op"
    oprc=$?
    t_op=$(date -u +%FT%T.%NZ)
    pre=$(stat -c %s "$clone" 2>/dev/null || echo missing)
    srcsha=$(sha256sum "$src" 2>/dev/null | cut -d' ' -f1)
    if [ "$oprc" -ne 0 ] || ! mounted_on "$fsdev" "$mnt"; then
      why="op rc $oprc, or $fsdev no longer at $mnt: no crash run"
    else
      case $K in
        xfs|ext4) sudo xfs_io -x -c shutdown "$mnt"; crc=$? ;;
        btrfs) sudo dmsetup suspend --nolockfs "$dm" && sudo dmsetup load "$dm" --table "0 $sz flakey $dev 0 0 86400 1 drop_writes" \
                 && sudo dmsetup resume "$dm"; crc=$? ;;
      esac
      t_crash=$(date -u +%FT%T.%NZ)
      # the crash proof: a sentinel written now must not survive (xfs/ext4 refuse it; btrfs drops it)
      if sudo sh -c "echo sentinel > '$mnt/w/sentinel' && sync -f '$mnt/w/sentinel'" 2>/dev/null; then sentinel_written=yes; fi
      sudo umount "$mnt"
      urc=$?
      if [ "$K" = btrfs ]; then
        sudo dmsetup suspend "$dm" && sudo dmsetup load "$dm" --table "0 $sz linear $dev 0" && sudo dmsetup resume "$dm"
        sudo udevadm settle
      fi
      if [ "$urc" -ne 0 ] || mountpoint -q "$mnt"; then
        why="umount rc $urc or $mnt still mounted: a remount would read the uncrashed filesystem"
      else
        sudo blockdev --flushbufs "$fsdev" 2>/dev/null
        sudo mount "$fsdev" "$mnt"
        mrc=$?
        if [ "$mrc" -eq 0 ] && mounted_on "$fsdev" "$mnt"; then
          post=$(sudo stat -c %s "$clone" 2>/dev/null || echo missing)
          postsha=$(sudo sha256sum "$mnt/w/$arm.src" 2>/dev/null | cut -d' ' -f1)
          sudo cmp -s "$mnt/w/$arm.src" "$clone" 2>/dev/null && same=yes
          if sudo test -e "$mnt/w/sentinel"; then sentinel_after=present; else sentinel_after=absent; fi
          ls -la "$mnt/w" "$mnt/w/$arm.clones" 2>&1
        else
          why="remount failed (rc $mrc) or $fsdev not at $mnt"
          [ "$mrc" -eq 0 ] && mrc=9
        fi
      fi
    fi
  fi
  mountpoint -q "$mnt" && sudo umount "$mnt"
  [ "$K" = btrfs ] && sudo dmsetup remove "$dm"
  sudo losetup -d "$dev"
  sudo rm -f "$img"
  python3 -B - "$OUT/$name.json" "$name" "$arm" "$K" "$*" "$oprc" "$crc" "$urc" "$mrc" "$pre" "$post" "$same" "$srcsha" \
    "$postsha" "$sentinel_written" "$sentinel_after" "$why" "$t_mount" "$t_op" "$t_crash" "$fsdev" <<'PY'
import json, sys
(out, name, arm, kind, flags, oprc, crc, urc, mrc, pre, post, same, s0, s1, sw, sa, why, tm, top, tc, fsdev) = sys.argv[1:]
survived = post == "1048576" and same == "yes"
ok_rcs = int(oprc) == 0 and int(crc) == 0 and int(urc) == 0 and int(mrc) == 0
json.dump({"case": name, "arm": arm, "kind": kind, "flags": flags, "op_rc": int(oprc), "crash_rc": int(crc),
           "umount_rc": int(urc), "remount_rc": int(mrc), "fs_device": fsdev, "clone_size_before": pre,
           "clone_size_after": post, "clone_equals_source": same == "yes",
           "src_ok": bool(s0) and s0 == s1 and ok_rcs and not why,
           "sentinel_written_after_crash": sw, "sentinel_after_remount": sa,
           "crash_proven": sa == "absent" and ok_rcs and not why,
           "result": "survived" if survived else "lost", "refused": why or None,
           "utc": {"mounted": tm, "op_done": top, "crashed": tc},
           "method": "dm-flakey drop_writes after op 0" if kind == "btrfs" else "xfs_io shutdown without a log flush"},
          open(out, "w"), indent=1)
print(name, "survived" if survived else "lost", "size", post, "equal", same, "sentinel", sw, "->", sa, why or "")
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
