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
#   ds    an ext4 loop mounted -o dirsync (an option outside the allowlist)              R_dirsync (15)
#   ld    XFS with an external log (-l logdev=, mounted -o logdev=)                      R_logdev (15)
#   ej    ext4 with an external journal (mkfs -J device=)                                R_extjournal (15)
#   md    btrfs over two loop devices                                                    R_multidev (15)
#   wt    an ext4 loop whose queue/write_cache is set to write through                   R_loop_wt (9)
#   brd   ext4 on /dev/ram1 (brd)                                                        R_brd (1a)
#   dm    ext4 on a device-mapper linear target over a loop                              R_driver (1a)
#   root  a directory on the root filesystem itself (leaf = the runner's own disk)       R_leaf_flip, R_clocksource
# Exit 0 when every fixture proved itself, 1 otherwise (the workflow records it and runs the fire-check anyway).
set -u
base=${1:?usage: mkfixtures.sh BASE}
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
  local prev=/v3fx-n1.img size=1200M d k
  for k in 1 2 3 4; do
    sudo mkdir -p "$base/n$k"
    if [ $k = 1 ]; then d=$(newloop "$prev" $size); else sudo truncate -s $size "$prev" && d=$(sudo losetup --find --show "$prev"); fi
    [ -n "$d" ] && sudo $MKE4 "$d" && sudo mount "$d" "$base/n$k" && mkw "$base/n$k" || { bad "n$k" "level $k"; return 1; }
    prev="$base/n$k/x.img"
    size=$(( ${size%M} * 2 / 3 ))M
    ok "n$k" "$d on $(losetup -n -O BACK-FILE "$d" | xargs)"
  done
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
for f in nb ht hn lz del nest ds ld ej md wt brd dm root; do
  "f_$f" || bad "$f" "a step failed"
done
echo "mkfixtures: $(ls "$base"/*.ok 2>/dev/null | wc -l) fixtures proved; fail=$fail"
exit $fail
