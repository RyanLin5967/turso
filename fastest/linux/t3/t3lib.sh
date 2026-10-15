# t3lib.sh -- sourced by t3run.sh: checks that only a real run reaches, factored out so t3lib_test.sh can fire each on
# fake inputs (fourth lane review LOW 8, 11, 12 and 16; T3 runner review LOW 16).
#
#   drift_ok RC                         batchgate.py drift's rc may let the block go on: 0 (pass) or 3 (a VOID, published,
#                                       not a gate on T3: PREREG :180, departure 3, A20). Anything else, 2 (refused), a
#                                       traceback, a kill or a missing interpreter, fails the block: an allowlist.
#   fslist_ok LIST                      the block list is not empty
#   registered_ok SYS REG DEV PLP LIST  the A17 registration a bound batch in rental mode refuses without, checked before
#                                       deps, build and fire-checks are paid for: REG has a non-empty frame_arm; DEV's
#                                       queue/write_cache under SYS reads write back or write through; on write back
#                                       without PLP, REG has d0_threshold/<fs>/wb/bare for every fs in LIST (write-through
#                                       and PLP drives need none, A14). Prints the reason and returns 2 on a refusal.
#   plp_drive_id SYS DEV                "model<TAB>firmware" from SYS/block/DEV/device: firmware_rev (NVMe) or rev (SCSI,
#                                       SATA); returns 2 when either field is missing or empty
#   plp_listed SYS DEV FILE             DEV's identity is a whole line of FILE (t3/PLP-DRIVES)
#   dev_unmoved PATH REAL               PATH still resolves to REAL, the device node preflight resolved. Path text
#                                       only: for a plain /dev/nvmeXnY it always holds, so it is never the identity
#                                       check (review 5 MED 3); dev_unchanged is. Returns 2 on a refusal
#   dev_identity SYS DEV                the drive behind DEV, one field per line: wwid (block/wwid, else device/wwid:
#                                       the NVMe EUI/NGUID or SCSI VPD 0x83 name), dev (MAJ:MIN), model, serial
#                                       (device/serial, else the printable part of device/vpd_pg80), firmware
#                                       (firmware_rev, else rev). Any empty field refuses (rc 2, naming it)
#   dev_unchanged PATH REAL SYS DEV ID  PATH is unmoved AND the drive behind DEV is the one preflight recorded in ID
#                                       (device-id.txt). A controller reset keeps the instance number (inferred); a
#                                       remove plus re-add can give the name to another drive, which only the identity
#                                       shows. t3run re-checks it before every mkfs, mount and wipefs. Returns 2
#   t3_blkid TAG DEV                    one tag's value from blkid's low-level probe of DEV (`sudo -n blkid -p`);
#                                       t3lib_test.sh redefines the function, production reads no override (LOW 22)
#   fs_probe_is REAL FS UUID            the probe finds exactly FS and UUID on REAL (an empty UUID refuses). Returns 2
#   fs_is_ours PATH REAL FS UUID        block_cleanup may wipe REAL: PATH is unmoved, and the probe finds exactly the FS
#                                       this block made AND the UUID its mkfs gave it (review 5 MED 3). Returns 2
#
# The device branch (review 6 MED 1): every root command it runs is one hook function, which t3lib_test.sh redefines
# to log the order; production bodies below, their argv pinned by t3lib_test.sh with sudo stubbed.
#   t3_pin DEV / t3_unpin               hold DEV open (O_RDONLY, no O_EXCL, so mkfs, mount and wipefs still claim it) in
#                                       a root helper, so the name cannot pass to another drive while the checks and
#                                       the command run (review 6 LOW 4; the kernel keeps an open disk's name and
#                                       instance: inferred, owed run). The helper exits on EOF, so a dead t3run
#                                       releases it too. t3_pin returns 2 when it cannot hold DEV
#   t3_devguard DEV                     devguard.py check DEV, as root
#   t3_mkfs FS DEV UUID                 mkfs.FS on DEV giving it UUID (xfs -m uuid=, btrfs -U, ext4 -U; review 6 LOW 6)
#   t3_mount FS DEV MNT                 mount -t FS (btrfs nodiscard) DEV on MNT; t3_mounted MNT prints "SOURCE FSTYPE"
#   t3_wipe DEV                         wipefs -a DEV
#   device_make PATH REAL SYS DEV ID PLP REG FS UUID MNT MADE
#                                       the one way a device block makes and mounts its filesystem, REAL held open
#                                       throughout: devguard; then the drive re-read (dev_unchanged; registered_ok, as
#                                       write_cache can flip); "FS UUID" written to MADE, so block_cleanup checks even
#                                       a failed mkfs; mkfs with UUID; the probe finds FS and UUID; the re-read; mount
#                                       -t FS; findmnt names REAL with FS, and the probe and the re-read hold. A
#                                       refusal returns 2 before the next step, a failed command 1. The PLP listing
#                                       is not re-read: its model and firmware are in the identity (review 6 LOW 19)
#   device_wipe PATH REAL SYS DEV ID FS UUID
#                                       the one way block_cleanup wipes, REAL held open: dev_unchanged, fs_is_ours,
#                                       wipefs. Returns 2 on a refusal, 1 when wipefs fails

T3LIB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

drift_ok() { case $1 in 0|3) return 0 ;; *) return 1 ;; esac; }

fslist_ok() { [ -n "$(echo $1)" ]; }

registered_ok() { # registered_ok SYS REG DEV PLP LIST
  local sys=$1 reg=$2 dev=$3 plp=$4 list=$5 wc f
  fslist_ok "$list" || { echo "REFUSED: no filesystem block to check the registration for"; return 2; }
  awk -F '\t' '$1 == "frame_arm" && $2 != "" { ok = 1 } END { exit !ok }' "$reg" 2>/dev/null ||
    { echo "REFUSED: $reg registers no frame_arm (PREREG section 4: fixed in the Registration annex)"; return 2; }
  wc=$(cat "$sys/block/$dev/queue/write_cache" 2>/dev/null)
  case $wc in "write back"|"write through") ;; *) echo "REFUSED: cannot read $dev's queue/write_cache ('$wc')"; return 2 ;; esac
  if [ "$wc" = "write back" ] && [ "$plp" != yes ]; then
    for f in $list; do
      awk -F '\t' -v k="d0_threshold/$f/wb/bare" '$1 == k && $2 != "" { ok = 1 } END { exit !ok }' "$reg" ||
        { echo "REFUSED: $reg registers no d0_threshold/$f/wb/bare (A17: rental mode refuses a provisional one)"; return 2; }
    done
  fi
  return 0
}

plp_drive_id() { # plp_drive_id SYS DEV
  local d=$1/block/$2/device model fw
  model=$(xargs < "$d/model" 2>/dev/null)
  fw=$(xargs < "$d/firmware_rev" 2>/dev/null)
  [ -n "$fw" ] || fw=$(xargs < "$d/rev" 2>/dev/null)
  [ -n "$model" ] && [ -n "$fw" ] || { echo "REFUSED: cannot read $2's model ('$model') and firmware ('$fw')" >&2; return 2; }
  printf '%s\t%s\n' "$model" "$fw"
}

plp_listed() { # plp_listed SYS DEV FILE
  local id
  id=$(plp_drive_id "$1" "$2") || return 2
  grep -qxF "$id" "$3"
}

dev_unmoved() { # dev_unmoved PATH REAL
  local now
  now=$(readlink -f "$1")
  [ -n "$2" ] && [ "$now" = "$2" ] ||
    { echo "REFUSED: $1 resolved to '$2' at preflight and resolves to '$now' now: not the device devguard checked"; return 2; }
}

t3_first_line() { [ -r "$1" ] && awk 'NR == 1 { gsub(/^[ \t]+|[ \t]+$/, ""); print }' "$1"; }

dev_identity() { # dev_identity SYS DEV
  local b=$1/block/$2 d=$1/block/$2/device wwid dev model serial fw f
  wwid=$(t3_first_line "$b/wwid"); [ -n "$wwid" ] || wwid=$(t3_first_line "$d/wwid")
  dev=$(t3_first_line "$b/dev")
  model=$(t3_first_line "$d/model")
  serial=$(t3_first_line "$d/serial")
  [ -n "$serial" ] || serial=$(tr -cd '[:print:]' 2>/dev/null < "$d/vpd_pg80" | awk '{ gsub(/^ +| +$/, ""); print }')
  fw=$(t3_first_line "$d/firmware_rev"); [ -n "$fw" ] || fw=$(t3_first_line "$d/rev")
  for f in wwid dev model serial fw; do
    [ -n "${!f}" ] || { echo "REFUSED: cannot read $2's ${f/fw/firmware} under $1/block/$2" >&2; return 2; }
  done
  printf 'wwid=%s\ndev=%s\nmodel=%s\nserial=%s\nfirmware=%s\n' "$wwid" "$dev" "$model" "$serial" "$fw"
}

dev_unchanged() { # dev_unchanged PATH REAL SYS DEV ID
  local now
  dev_unmoved "$1" "$2" || return 2
  [ -s "$5" ] || { echo "REFUSED: no identity recorded for $4 ($5)"; return 2; }
  now=$(dev_identity "$3" "$4") || return 2
  [ "$now" = "$(cat "$5")" ] ||
    { echo "REFUSED: $4 is not the drive preflight recorded in $5; now: $(printf '%s\n' "$now" | grep -vxF -f "$5" | tr '\n' ' ')"
      return 2; }
}

t3_blkid() { # t3_blkid TAG DEV
  sudo -n blkid -p -o value -s "$1" "$2"
}

fs_probe_is() { # fs_probe_is REAL FS UUID
  local t u
  [ -n "$3" ] || { echo "REFUSED: no UUID recorded for the '$2' this block made"; return 2; }
  t=$(t3_blkid TYPE "$1"); u=$(t3_blkid UUID "$1")
  [ -n "$2" ] && [ "$t" = "$2" ] || { echo "REFUSED: $1 carries '$t', not the '$2' this block made"; return 2; }
  [ "$u" = "$3" ] || { echo "REFUSED: $1's UUID is '$u', not the '$3' this block's mkfs was given"; return 2; }
}

fs_is_ours() { # fs_is_ours PATH REAL FS UUID
  dev_unmoved "$1" "$2" || return 2
  fs_probe_is "$2" "$3" "$4" || { echo "REFUSED: $2 not wiped"; return 2; }
}

t3_pin() { # t3_pin DEV
  coproc T3PIN { sudo -n python3 -B -c 'import os, sys
os.open(sys.argv[1], os.O_RDONLY)
print("held", flush=True)
sys.stdin.read()' "$1" 2>&1; }
  local line=""
  read -r -t 30 line <&"${T3PIN[0]:-}"
  [ "$line" = held ] || { echo "REFUSED: cannot hold $1 open ('$line')"; t3_unpin; return 2; }
}

t3_unpin() { # EOF on the helper's stdin; it returns, and the node is closed when it exits
  local pid=${T3PIN_PID:-}
  [ -n "${T3PIN[1]:-}" ] && eval "exec ${T3PIN[1]}>&-"
  [ -n "$pid" ] && wait "$pid" 2> /dev/null
  T3PIN_PID=""
  return 0
}

t3_devguard() { sudo -n python3 -B "$T3LIB_DIR/devguard.py" check "$1"; }

t3_mkfs() { # t3_mkfs FS DEV UUID
  case $1 in
    xfs) sudo mkfs.xfs -f -m reflink=1,uuid="$3" "$2" ;;
    btrfs) sudo mkfs.btrfs -f -U "$3" "$2" ;;
    ext4) sudo mkfs.ext4 -F -U "$3" -E lazy_itable_init=0,lazy_journal_init=0 "$2" ;;
    *) echo "unknown fs $1"; return 1 ;;
  esac
}

# btrfs defaults to discard=async on SSDs, which discards freed extents 10-120 s later, inside a later run
t3_mount() { sudo mkdir -p "$3" && sudo mount -t "$1" $([ "$1" = btrfs ] && echo "-o nodiscard") "$2" "$3"; }

t3_mounted() { findmnt -n -v -o SOURCE,FSTYPE --mountpoint "$1" | awk '{ print $1, $2 }'; }

t3_wipe() { sudo -n wipefs -a "$1"; }

t3_pinned() { # t3_pinned DEV CMD...: CMD with DEV held open, released on every return
  local rc
  t3_pin "$1" || return 2
  shift
  "$@"; rc=$?
  t3_unpin
  return $rc
}

_device_make() { # _device_make PATH REAL SYS DEV ID PLP REG FS UUID MNT MADE
  local path=$1 real=$2 sys=$3 dev=$4 id=$5 plp=$6 reg=$7 fs=$8 uuid=$9 mnt=${10} made=${11} now
  [ -n "$uuid" ] || { echo "REFUSED: no UUID to give the $fs on $real"; return 2; }
  t3_devguard "$real" || { echo "REFUSED: devguard refuses $real before mkfs"; return 2; }
  # after devguard, so whatever happened while it ran is seen before mkfs (review 6 LOW 4)
  dev_unchanged "$path" "$real" "$sys" "$dev" "$id" || return 2
  registered_ok "$sys" "$reg" "$dev" "$plp" "$fs" || return 2
  printf '%s %s\n' "$fs" "$uuid" > "$made" || { echo "cannot record what this block makes in $made"; return 1; }
  t3_mkfs "$fs" "$real" "$uuid" || { echo "mkfs.$fs failed on $real"; return 1; }
  fs_probe_is "$real" "$fs" "$uuid" || return 2
  dev_unchanged "$path" "$real" "$sys" "$dev" "$id" || return 2
  t3_mount "$fs" "$real" "$mnt" || { echo "mount -t $fs $real $mnt failed"; return 1; }
  now=$(t3_mounted "$mnt")
  [ "$now" = "$real $fs" ] || { echo "REFUSED: findmnt says $mnt is '$now', not '$real $fs'"; return 2; }
  fs_probe_is "$real" "$fs" "$uuid" && dev_unchanged "$path" "$real" "$sys" "$dev" "$id" || return 2
}

device_make() { t3_pinned "$2" _device_make "$@"; }

_device_wipe() { # _device_wipe PATH REAL SYS DEV ID FS UUID
  dev_unchanged "$1" "$2" "$3" "$4" "$5" || return 2
  fs_is_ours "$1" "$2" "$6" "$7" || return 2
  t3_wipe "$2" || { echo "wipefs failed on $2"; return 1; }
}

device_wipe() { t3_pinned "$2" _device_wipe "$@"; }
