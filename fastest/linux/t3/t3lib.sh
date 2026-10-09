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
#   fs_is_ours PATH REAL FS UUID        block_cleanup may wipe REAL: PATH is unmoved, and the probe finds exactly the FS
#                                       this block made AND the UUID its mkfs gave it (review 5 MED 3). Returns 2

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

fs_is_ours() { # fs_is_ours PATH REAL FS UUID
  local t u
  dev_unmoved "$1" "$2" || return 2
  [ -n "$4" ] || { echo "REFUSED: no UUID recorded for the '$3' this block made: not wiped"; return 2; }
  t=$(t3_blkid TYPE "$2"); u=$(t3_blkid UUID "$2")
  [ -n "$3" ] && [ "$t" = "$3" ] ||
    { echo "REFUSED: $2 carries '$t', not the '$3' this block made: not wiped"; return 2; }
  [ "$u" = "$4" ] || { echo "REFUSED: $2's UUID is '$u', not the '$4' this block's mkfs gave it: not wiped"; return 2; }
}
