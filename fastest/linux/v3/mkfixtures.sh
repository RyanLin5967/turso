#!/bin/bash
# mkfixtures.sh BASE -- the V3 fire-check's refusal fixtures (sudo; for CI or a scratch box, never a measured device).
# Each fixture is a directory BASE/<name>/w (or the path in BASE/<name>.dir) that one F4 plant runs the probe on, and
# BASE/<name>.ok exists only when the fixture proved what it is. A fixture that cannot be made is reported and its
# plant then fails in check.py (no .ok), so a missing fixture can never read as a passing refusal.
#   nb    ext4 loop mounted -o nobarrier (backing on the root fs)                       R_nobarrier
#   nbx   XFS loop whose backing file sits on nb's ext4                                  R_nobarrier_below
#   ht    an XFS loop at ht/sub, then a tmpfs mounted over ht (backing outside ht): D = ht/sub/w is on the tmpfs,
#         while the hidden XFS mount is the longest mount-table prefix                   R_hidden_tmpfs (10a)
#   hn    an ext4 barrier loop at hn/sub, then an ext4 nobarrier loop over hn: D = hn/sub/w is on the nobarrier one,
#         the hidden barrier mount is the longest prefix                                 R_hidden_nobarrier (10b)
#   lz    loop L2 backed by a file on loop L1's ext4 (lz/a); lz/a is then lazily unmounted (umount -l), and a decoy
#         file is made at the path sysfs now shows for L2's backing                     R_lazy (10, 12)
#   del   an ext4 loop whose backing file was deleted                                    R_deleted (12a)
#   n3/n4 3 and 4 nested loops (ext4 in ext4 ...): 4 layers accepted, 5 refused          P_nest3, R_nest4 (12b)
#         (the first image on V3_NEST_DIR when set: a dir on the filesystem that holds the cell's leaf, i.e. the
#         cell's mount for a block cell, the directory of its loop backing file for a loop cell; else on /)
#   ds    an ext4 loop mounted -o dirsync (an option outside the allowlist)              R_dirsync (15)
#   ld    XFS with an external log (-l logdev=, mounted -o logdev=)                      R_logdev (15)
#   ej    ext4 with an external journal (mkfs -J device=)                                R_extjournal (15)
#   md    btrfs over two loop devices                                                    R_multidev (15)
#   wt    an ext4 loop whose queue/write_cache is set to write through                   R_loop_wt (9)
#   brd   ext4 on /dev/ram1 (brd)                                                        R_brd (1a)
#   dm    ext4 on a device-mapper linear target over a loop                              R_driver (1a)
#   root  a directory on the root filesystem itself (leaf = the runner's own disk)       R_leaf_flip, R_clocksource
#   dj    an ext4 loop whose superblock default is data=journal (tune2fs -o journal_data): mountinfo omits it,
#         /proc/fs/ext4/<dev>/options shows it                                          R_datajournal (fourth review L6)
#   sdbg  ext4 on a scsi_debug disk (RAM posing as a SCSI drive; its caching page says WCE=1, so sd reads write
#         back): the MODE SENSE WCE=1 branch                                            P_sdbg_wb, R_sdbg_flip, R_sdbg_noenv (M2)
# Exit 0 when every fixture proved itself, 1 otherwise (the workflow records it and runs the fire-check anyway).
#
# Two modes for t3run's per-block runs (fastest-linux; the nest chain must start on the block's own filesystem, which
# exists only inside the block, while scsi_debug, /dev/ram1 and v3fx-dm stay in use from the first run):
#   V3_FIXTURES=nest V3_NEST_DIR=DIR mkfixtures.sh BASE   makes n1..n4 only, the first image on DIR (required: no
#                                                        default to /); an n4..n1 chain already under BASE is torn
#                                                        down first. Exit 0 iff n1..n4 all proved themselves.
#   mkfixtures.sh --teardown-nest BASE                   unmounts n4..n1 under BASE, detaches their loops, deletes
#                                                        n1's image and the n*.ok files, so the block's own
#                                                        filesystem can be unmounted. Exit 0 iff afterwards none of
#                                                        n1..n4 is mounted and every loop it detached is gone (a BASE
#                                                        with no chain is already in that state: exit 0, said so).
# V3_FIXTURES is all (the default) or nest; anything else refuses (exit 2).
set -u
if [ "${1:-}" = --teardown-nest ]; then MODE=teardown; base=${2:?usage: mkfixtures.sh --teardown-nest BASE}
else MODE=${V3_FIXTURES:-all}; base=${1:?usage: mkfixtures.sh BASE}; fi
case $MODE in
  teardown) [ "${1:-}" = --teardown-nest ] || { echo "mkfixtures: REFUSED: V3_FIXTURES='teardown' is not all or nest" >&2; exit 2; } ;;
  all) ;;
  nest) [ -n "${V3_NEST_DIR:-}" ] && [ -d "$V3_NEST_DIR" ] \
          || { echo "mkfixtures: REFUSED: V3_FIXTURES=nest needs V3_NEST_DIR, a directory on the block's filesystem (got '${V3_NEST_DIR:-}')" >&2; exit 2; } ;;
  *) echo "mkfixtures: REFUSED: V3_FIXTURES='$MODE' is not all or nest" >&2; exit 2 ;;
esac
# the one nest-chain loop matcher and a losetup listing that fails loudly (V3 review 12 item 1), shared with firecheck.sh
. "$(cd "$(dirname "$0")" && pwd)/nestloops.sh" || { echo "mkfixtures: REFUSED: cannot source nestloops.sh" >&2; exit 2; }
me="$(id -u):$(id -g)"
# every fixture ext4 is made whole now (no lazyinit thread writing and committing later, which would put foreign
# flush requests on the loop devices during the fire-check)
MKE4="mkfs.ext4 -F -q -E lazy_itable_init=0,lazy_journal_init=0"
sudo mkdir -p "$base"
fail=0
ok() { sudo touch "$base/$1.ok"; echo "fixture $1: OK $2"; }
bad() { echo "fixture $1: FAILED: $2"; fail=1; }
newloop() { # file size -> /dev/loopN
  sudo rm -f "$1"
  sudo truncate -s "$2" "$1"
  sudo losetup --find --show "$1"
}
opts() { findmnt -n -o OPTIONS -T "$1"; }
mkw() { sudo mkdir -p "$1/w" && sudo chown "$me" "$1/w"; }

f_nb() {
  local d1 d2
  sudo mkdir -p "$base/nb" "$base/nbx"
  d1=$(newloop /v3fx-nb.img 3G) && sudo $MKE4 "$d1" && sudo mount -o nobarrier "$d1" "$base/nb" && mkw "$base/nb" || return 1
  case ",$(opts "$base/nb/w")," in *,nobarrier,*) ok nb "$d1 $(opts "$base/nb/w")" ;; *) bad nb "not nobarrier"; return 1 ;; esac
  sudo truncate -s 1G "$base/nb/x.img"
  d2=$(sudo losetup --find --show "$base/nb/x.img") && sudo mkfs.xfs -f -q "$d2" && sudo mount "$d2" "$base/nbx" && mkw "$base/nbx" || return 1
  case ",$(opts "$base/nbx/w")," in *,nobarrier,*) bad nbx "itself nobarrier"; return 1 ;; esac
  case "$(losetup -n -O BACK-FILE "$d2" | xargs)" in "$base/nb/"*) ok nbx "$d2 backing $(losetup -n -O BACK-FILE "$d2" | xargs)" ;; *) bad nbx "not backed under nb" ;; esac
}
f_ht() {
  local d
  sudo mkdir -p "$base/ht/sub"
  d=$(newloop /v3fx-ht.img 1G) && sudo mkfs.xfs -f -q "$d" && sudo mount "$d" "$base/ht/sub" || return 1
  sudo mount -t tmpfs -o size=64m tmpfs "$base/ht" && sudo mkdir -p "$base/ht/sub" && mkw "$base/ht/sub" || return 1
  echo "$base/ht/sub/w" | sudo tee "$base/ht.dir" > /dev/null
  # proof by statfs and device numbers, not findmnt -T (which walks path prefixes, the very trap planted here)
  if [ "$(stat -f -c %T "$base/ht/sub/w")" = tmpfs ] && [ "$(stat -c %d "$base/ht/sub/w")" = "$(stat -c %d "$base/ht")" ] \
     && grep -q " $base/ht/sub " /proc/self/mountinfo; then
    ok ht "D on tmpfs (dev $(stat -c %d "$base/ht/sub/w")); hidden xfs still in the table: $(grep " $base/ht/sub " /proc/self/mountinfo | head -1); findmnt -T says $(findmnt -n -o FSTYPE -T "$base/ht/sub/w")"
  else
    bad ht "layout not as planted: $(stat -f -c %T "$base/ht/sub/w") dev $(stat -c %d "$base/ht/sub/w") vs $(stat -c %d "$base/ht")"
  fi
}
f_hn() {
  local d1 d2
  sudo mkdir -p "$base/hn/sub"
  d1=$(newloop /v3fx-hn1.img 1G) && sudo $MKE4 "$d1" && sudo mount "$d1" "$base/hn/sub" || return 1
  d2=$(newloop /v3fx-hn2.img 1G) && sudo $MKE4 "$d2" && sudo mount -o nobarrier "$d2" "$base/hn" || return 1
  sudo mkdir -p "$base/hn/sub" && mkw "$base/hn/sub" || return 1
  echo "$base/hn/sub/w" | sudo tee "$base/hn.dir" > /dev/null
  # proof by device numbers: D sits on the nobarrier fs mounted at hn, while findmnt -T (path prefix) names the
  # hidden barrier mount at hn/sub -- the trap the old probe fell into
  if [ "$(stat -c %d "$base/hn/sub/w")" = "$(stat -c %d "$base/hn")" ] && grep -q " $base/hn/sub " /proc/self/mountinfo \
     && case ",$(findmnt -n -o OPTIONS "$base/hn" | tail -1)," in *,nobarrier,*) true ;; *) false ;; esac; then
    ok hn "D dev $(stat -c %d "$base/hn/sub/w") = the nobarrier $d2 at hn; hidden barrier $d1 at hn/sub; findmnt -T says $(findmnt -n -o SOURCE,OPTIONS -T "$base/hn/sub/w")"
  else
    bad hn "D is not on the nobarrier mount: dev $(stat -c %d "$base/hn/sub/w") vs $(stat -c %d "$base/hn"); $(findmnt -n -o OPTIONS "$base/hn")"
  fi
}
f_lz() {
  local d1 d2 shown
  sudo mkdir -p "$base/lz/a" "$base/lz/b"
  d1=$(newloop /v3fx-lz.img 1G) && sudo $MKE4 "$d1" && sudo mount "$d1" "$base/lz/a" || return 1
  sudo truncate -s 512M "$base/lz/a/x.img"
  d2=$(sudo losetup --find --show "$base/lz/a/x.img") && sudo $MKE4 "$d2" && sudo mount "$d2" "$base/lz/b" && mkw "$base/lz/b" || return 1
  echo "$base/lz/b/w" | sudo tee "$base/lz.dir" > /dev/null
  sudo umount -l "$base/lz/a" || return 1
  shown=$(cat "/sys/block/${d2##*/}/loop/backing_file")
  case $shown in /*) [ -e "$shown" ] || { sudo mkdir -p "$(dirname "$shown")"; sudo truncate -s 512M "$shown"; } ;; *) bad lz "sysfs shows '$shown'"; return 1 ;; esac
  echo "$shown" | sudo tee "$base/lz.decoy" > /dev/null
  ok lz "loop ${d2##*/} backing now shown as $shown, decoy inode $(stat -c '%d:%i' "$shown")"
}
f_del() {
  local d
  sudo mkdir -p "$base/del"
  d=$(newloop /v3fx-del.img 1G) && sudo $MKE4 "$d" && sudo mount "$d" "$base/del" && mkw "$base/del" || return 1
  sudo rm -f /v3fx-del.img
  case "$(cat "/sys/block/${d##*/}/loop/backing_file")" in *"(deleted)"*) ok del "$(cat "/sys/block/${d##*/}/loop/backing_file")" ;; *) bad del "backing not shown deleted" ;; esac
}
f_nest() {
  # the chain starts on the CELL's filesystem when V3_NEST_DIR names it (seventh review M3: P_nest3 must be accepted,
  # so its leaf must be the cell's drive, not a root on md or LVM, which the probe refuses)
  local prev=${V3_NEST_DIR:-}/v3fx-n1.img size=1200M d k
  for k in 1 2 3 4; do
    sudo mkdir -p "$base/n$k"
    if [ $k = 1 ]; then d=$(newloop "$prev" $size); else sudo truncate -s $size "$prev" && d=$(sudo losetup --find --show "$prev"); fi
    [ -n "$d" ] && sudo $MKE4 "$d" && sudo mount "$d" "$base/n$k" && mkw "$base/n$k" || { bad "n$k" "level $k"; return 1; }
    prev="$base/n$k/x.img"
    size=$(( ${size%M} * 2 / 3 ))M
    ok "n$k" "$d on $(losetup -n -O BACK-FILE "$d" | xargs)"
  done
}
# the nest chain's teardown (t3run per block): n4 first, each level's loop detached after its unmount
# the loops backed by a file of the chain: nestloops.sh's one matcher (under $base/n1/ .. $base/n4/, anchored, or an
# image named v3fx-n1.img); returns 2 when losetup fails (V3 review 12 item 1: the prefix test here matched nbx's
# $base/nb/x.img, and a failing losetup read as "nothing attached")
chain_loops() {
  nest_loops "$base"
}
detach_loop() {  # detach_loop DEV LABEL: detach and wait until its backing file is gone
  local dev=$1 gone=0
  sudo losetup -d "$dev" || { echo "teardown $2: losetup -d $dev FAILED"; return 1; }
  for _ in $(seq 1 20); do [ -e "/sys/block/${dev##*/}/loop/backing_file" ] || { gone=1; break; }; sleep 0.5; done
  [ "$gone" = 1 ] || { echo "teardown $2: $dev still has a backing file"; return 1; }
}
teardown_nest() {
  local k mp dev back rc=0 found=0 stray strays left ll
  for k in 4 3 2 1; do
    mp="$base/n$k"
    sudo rm -f "$base/n$k.ok"
    # tenth review MED 2: a loop backed by a file ON this level, attached but not mounted (a level whose mount failed,
    # a stray), would hold the level busy: detach it first, mounted ones above were unmounted in the previous pass
    # the listing first, on its own: this script has no pipefail, so a pipe would hide a failing losetup
    if ll=$(loop_list); then
      strays=$(printf '%s\n' "$ll" | awk -v b="$mp/" '{ d = $1; $1 = ""; sub(/^ +/, ""); if (index($0, b) == 1) print d }')
    else
      echo "teardown n$k: cannot list the loops (losetup failed)"; rc=1; strays=""
    fi
    for stray in $strays; do
      found=1
      mountpoint -q "$(findmnt -n -o TARGET -S "$stray" | tail -1)" 2>/dev/null && sudo umount "$(findmnt -n -o TARGET -S "$stray" | tail -1)"
      detach_loop "$stray" "n$k (an unmounted loop on it)" && echo "teardown n$k: detached $stray, backed by a file on $mp" || rc=1
    done
    mountpoint -q "$mp" 2>/dev/null || continue
    found=1
    dev=$(findmnt -n -o SOURCE "$mp" | tail -1)
    back=$(losetup -n -O BACK-FILE "$dev" 2>/dev/null | xargs)
    sudo umount "$mp" || { echo "teardown n$k: umount $mp FAILED"; rc=1; continue; }
    case $dev in
      /dev/loop*) detach_loop "$dev" "n$k" || rc=1 ;;
      *) echo "teardown n$k: $mp was on $dev, not a loop device"; rc=1 ;;
    esac
    [ "$k" = 1 ] && [ -n "$back" ] && sudo rm -f "$back"
    echo "teardown n$k: unmounted $mp, detached $dev (backing $back)"
  done
  # n1's image held by a loop that is not n1's mount (a failed mount, a second attach): detach it too
  strays=$(chain_loops) || { echo "teardown: cannot list the loops (losetup failed)"; rc=1; strays=""; }
  for stray in $strays; do
    found=1
    detach_loop "$stray" "n1's image" && echo "teardown: detached $stray, still backed by the chain" || rc=1
  done
  for k in 1 2 3 4; do
    mountpoint -q "$base/n$k" 2>/dev/null && { echo "teardown: $base/n$k is still mounted"; rc=1; }
  done
  # the outcome, not the steps: no loop may still be backed by a file of the chain (a listing that fails is a failure)
  if left=$(chain_loops); then
    [ -z "$left" ] || { echo "teardown: loops still backed by the chain: $(echo $left)"; rc=1; }
  else
    echo "teardown: cannot list the loops at the end (losetup failed)"; rc=1
  fi
  [ "$found" = 1 ] || echo "teardown: no nest chain under $base"
  return $rc
}
f_ds() {
  local d
  sudo mkdir -p "$base/ds"
  d=$(newloop /v3fx-ds.img 1G) && sudo $MKE4 "$d" && sudo mount -o dirsync "$d" "$base/ds" && mkw "$base/ds" || return 1
  case ",$(findmnt -n -o OPTIONS "$base/ds")," in *,dirsync,*) ok ds "$(findmnt -n -o OPTIONS "$base/ds")" ;; *) bad ds "no dirsync in the options" ;; esac
}
f_ld() {
  local dd dl
  sudo mkdir -p "$base/ld"
  dd=$(newloop /v3fx-ld.img 1G) && dl=$(newloop /v3fx-ldlog.img 128M) || return 1
  sudo mkfs.xfs -f -q -l logdev="$dl",size=64m "$dd" && sudo mount -o logdev="$dl" "$dd" "$base/ld" && mkw "$base/ld" || return 1
  grep " $base/ld " /proc/self/mountinfo | grep -q 'logdev=' && ok ld "$(grep " $base/ld " /proc/self/mountinfo)" || bad ld "no logdev= in mountinfo"
}
f_ej() {
  local dj dd
  sudo mkdir -p "$base/ej"
  dj=$(newloop /v3fx-ejj.img 128M) && dd=$(newloop /v3fx-ejd.img 1G) || return 1
  sudo mkfs.ext4 -F -q -O journal_dev -b 4096 "$dj" && sudo $MKE4 -b 4096 -J device="$dj" "$dd" && sudo mount "$dd" "$base/ej" && mkw "$base/ej" || return 1
  ls /proc/fs/jbd2/ | grep -q "^${dd##*/}-" && bad ej "an internal journal is listed" || ok ej "jbd2: $(ls /proc/fs/jbd2/ | xargs)"
}
f_md() {
  local a b
  sudo mkdir -p "$base/md"
  a=$(newloop /v3fx-mda.img 512M) && b=$(newloop /v3fx-mdb.img 512M) || return 1
  sudo mkfs.btrfs -f -q -d single -m single "$a" "$b" || return 1
  sudo btrfs device scan > /dev/null 2>&1
  sudo mount "$a" "$base/md" && mkw "$base/md" || return 1
  ok md "devices: $(ls /sys/fs/btrfs/*/devices/ 2>/dev/null | xargs)"
}
f_wt() {
  local d
  sudo mkdir -p "$base/wt"
  d=$(newloop /v3fx-wt.img 1G) && sudo $MKE4 "$d" && sudo mount "$d" "$base/wt" && mkw "$base/wt" || return 1
  echo "write through" | sudo tee "/sys/block/${d##*/}/queue/write_cache" > /dev/null
  [ "$(cat "/sys/block/${d##*/}/queue/write_cache")" = "write through" ] && ok wt "${d##*/} write_cache=write through" || bad wt "write_cache did not change"
}
f_brd() {
  sudo mkdir -p "$base/brd"
  [ -e /dev/ram1 ] || sudo modprobe brd rd_nr=2 rd_size=3145728 || return 1
  [ -e /dev/ram1 ] || { bad brd "no /dev/ram1"; return 1; }
  sudo $MKE4 /dev/ram1 && sudo mount /dev/ram1 "$base/brd" && mkw "$base/brd" && ok brd "ext4 on /dev/ram1"
}
f_dm() {
  local d sz
  sudo mkdir -p "$base/dm"
  d=$(newloop /v3fx-dm.img 1G) || return 1
  sz=$(sudo blockdev --getsz "$d")
  sudo dmsetup create v3fx-dm --table "0 $sz linear $d 0" && sudo $MKE4 /dev/mapper/v3fx-dm \
    && sudo mount /dev/mapper/v3fx-dm "$base/dm" && mkw "$base/dm" && ok dm "ext4 on $(readlink -f /dev/mapper/v3fx-dm) over $d"
}
f_root() {
  sudo mkdir -p /var/tmp/v3fx-root/w && sudo chown "$me" /var/tmp/v3fx-root/w && echo /var/tmp/v3fx-root/w | sudo tee "$base/root.dir" > /dev/null \
    && ok root "$(findmnt -n -o SOURCE,FSTYPE,OPTIONS -T /var/tmp/v3fx-root/w)"
}
f_dj() {
  local d
  sudo mkdir -p "$base/dj"
  d=$(newloop /v3fx-dj.img 1G) && sudo $MKE4 "$d" && sudo tune2fs -o journal_data "$d" > /dev/null \
    && sudo mount "$d" "$base/dj" && mkw "$base/dj" || return 1
  # the premise: data=journal is effective, and mountinfo does not show it (so only the effective-options pass sees it)
  if grep -qx 'data=journal' "/proc/fs/ext4/${d##*/}/options" && ! findmnt -n -o OPTIONS "$base/dj" | tr , '\n' | grep -qx 'data=journal'; then
    ok dj "${d##*/} effective $(grep '^data=' "/proc/fs/ext4/${d##*/}/options"); mountinfo: $(findmnt -n -o OPTIONS "$base/dj")"
  else
    bad dj "data=journal not effective, or shown in mountinfo: $(grep '^data=' "/proc/fs/ext4/${d##*/}/options" 2>&1); $(findmnt -n -o OPTIONS "$base/dj")"
  fi
}
f_sdbg() {
  local b d="" ct k
  sudo mkdir -p "$base/sdbg"
  if ! sudo modprobe scsi_debug dev_size_mb=256 2>/dev/null; then
    timeout 600 sudo apt-get install -y -q "linux-modules-extra-$(uname -r)" > /dev/null 2>&1
    sudo modprobe scsi_debug dev_size_mb=256 || { bad sdbg "no scsi_debug module for $(uname -r)"; return 1; }
  fi
  for k in $(seq 1 30); do
    sudo udevadm settle 2>/dev/null
    for b in /sys/block/sd*; do
      case "$(readlink -f "$b/device")" in */pseudo_*) d=${b##*/} ;; esac
    done
    [ -n "$d" ] && [ -b "/dev/$d" ] && break
    sleep 1
  done
  [ -n "$d" ] && [ -b "/dev/$d" ] || { bad sdbg "no scsi_debug disk appeared"; return 1; }
  echo "$d" | sudo tee "$base/sdbg.disk" > /dev/null
  ct=$(cat /sys/block/"$d"/device/scsi_disk/*/cache_type)
  [ "$ct" = "write back" ] || { bad sdbg "$d cache_type '$ct', not write back (scsi_debug's caching page default is WCE=1)"; return 1; }
  sudo $MKE4 "/dev/$d" && sudo mount "/dev/$d" "$base/sdbg" && mkw "$base/sdbg" || return 1
  sudo setfacl -m "u:$(id -un):r" "/dev/$d" 2>/dev/null || sudo chmod o+r "/dev/$d"
  ok sdbg "/dev/$d cache_type '$ct' write_cache '$(cat /sys/block/"$d"/queue/write_cache)' hosts $(cat /sys/class/scsi_host/host*/proc_name 2>/dev/null | xargs) path $(readlink -f "/sys/block/$d/device")"
}
if [ "$MODE" = teardown ]; then
  teardown_nest; trc=$?
  echo "mkfixtures --teardown-nest: rc=$trc"
  exit $trc
fi
if [ "$MODE" = nest ]; then
  teardown_nest || { echo "mkfixtures: the existing nest chain under $base could not be torn down"; exit 1; }
  f_nest || bad nest "a step failed"
  for k in 1 2 3 4; do [ -f "$base/n$k.ok" ] || { echo "fixture n$k: not proved"; fail=1; }; done
  echo "mkfixtures (nest only, first image on $V3_NEST_DIR): fail=$fail"
  exit $fail
fi
for f in nb ht hn lz del nest ds ld ej md wt brd dm root dj sdbg; do
  "f_$f" || bad "$f" "a step failed"
done
echo "mkfixtures: $(ls "$base"/*.ok 2>/dev/null | wc -l) fixtures proved; fail=$fail"
exit $fail
