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
#                                       (device/serial, else the printable part of device/vpd_pg80 after its 4-byte
#                                       header, review 6 LOW 21), firmware (firmware_rev, else rev). Any empty field
#                                       refuses (rc 2, naming it)
#   t3_rdev NODE                        NODE's own MAJ:MIN (st_rdev), in decimal; t3lib_test.sh redefines it
#   dev_unchanged PATH REAL SYS DEV ID  PATH is unmoved, the drive behind DEV is the one preflight recorded in ID
#                                       (device-id.txt), AND the node REAL is that drive's MAJ:MIN (st_rdev against
#                                       the recorded dev=; review 6 LOW 5: a node elsewhere carrying another disk's
#                                       numbers is not the drive). A controller reset keeps the instance number
#                                       (inferred); a remove plus re-add can give the name to another drive, which
#                                       only the identity shows. device_make and device_wipe run it. Returns 2
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
#
# Ours' timed run under the shared sweeper (the lead's MED 12 ruling, DECISIONS 2026-10-15T04:21:48Z: SAME INSTRUMENT FOR
# EVERY SYSTEM): gates/sweeper.sh, which t3run.sh sources and the competitors' trace.sh sources too, and its one reader,
# gates/sweeper_record.py. Tested by t3lib_test.sh on the box's real /proc (Linux, bash 5).
#   ours_timed_run D CMD...             CMD inside a wrapper whose process tree the sweeper sweeps into
#                                       D/timed.tracer.tsv: the wrapper waits (at most SWEEP_START_WAIT_S) for the
#                                       start sweep, stamps t0, runs CMD, stamps t1, so the start sweep is at or before
#                                       t0 and the end sweep (after the wrapper is reaped) at or after t1.
#                                       D/timed.window: "t0=T t1=T rc=R". Returns CMD's rc; 2, with CMD never run,
#                                       when the sweeper is not sourced or its start sweep never came. Blind spot,
#                                       stated: the window is fastest_profile's whole process (setup and warm-up
#                                       included), not the measured windows it times, as the competitors' window is;
#                                       those realtime bounds are fastest_profile's to stamp (Rust, HELD: LOUD-OWED)
#   ours_timed_exec D CMD...            ours_timed_run in an EXEC'D bash that sources gates/sweeper.sh (beside this
#                                       file's t3/) and this file: foreign_cpu.py classes a process as the run's own
#                                       (sut) by FASTEST_CELL in /proc/PID/environ, which is the environment at exec,
#                                       so a sweeper forked straight from t3run's shell would read as foreign while the
#                                       competitors' (inside the exec'd run_system.sh) reads as sut. t3run calls this.
#                                       Returns 2, CMD never run, when gates/sweeper.sh is not there
#   ours_timed_record D READER          D/timed.json from D/timed.window and READER's (sweeper_record.py) reading of
#                                       D/timed.tracer.tsv over [t0, t1]: sweeper_cpu_s and the reader's other fields,
#                                       sweeper_window "process"; on a missing window or a refused record,
#                                       sweeper_cpu_s null and sweeper_refused the reason, and rc 1

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
  [ -n "$serial" ] || serial=$(tail -c +5 "$d/vpd_pg80" 2>/dev/null | tr -cd '[:print:]' | awk '{ gsub(/^ +| +$/, ""); print }')
  fw=$(t3_first_line "$d/firmware_rev"); [ -n "$fw" ] || fw=$(t3_first_line "$d/rev")
  for f in wwid dev model serial fw; do
    [ -n "${!f}" ] || { echo "REFUSED: cannot read $2's ${f/fw/firmware} under $1/block/$2" >&2; return 2; }
  done
  printf 'wwid=%s\ndev=%s\nmodel=%s\nserial=%s\nfirmware=%s\n' "$wwid" "$dev" "$model" "$serial" "$fw"
}

t3_rdev() { # t3_rdev NODE
  local ma mi
  read -r ma mi < <(stat -L -c '%t %T' "$1" 2> /dev/null) && [ -n "$mi" ] || return 1
  echo "$((16#$ma)):$((16#$mi))"
}

dev_unchanged() { # dev_unchanged PATH REAL SYS DEV ID
  local now want rd
  dev_unmoved "$1" "$2" || return 2
  [ -s "$5" ] || { echo "REFUSED: no identity recorded for $4 ($5)"; return 2; }
  now=$(dev_identity "$3" "$4") || return 2
  [ "$now" = "$(cat "$5")" ] ||
    { echo "REFUSED: $4 is not the drive preflight recorded in $5; now: $(printf '%s\n' "$now" | grep -vxF -f "$5" | tr '\n' ' ')"
      return 2; }
  want=$(sed -n 's/^dev=//p' "$5")
  rd=$(t3_rdev "$2")
  [ -n "$want" ] && [ "$rd" = "$want" ] ||
    { echo "REFUSED: $2 is device '$rd' (its st_rdev), not the dev=$want preflight recorded for $4"; return 2; }
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

SWEEP_START_WAIT_S=5
ours_timed_run() { # ours_timed_run D CMD...
  local d=$1 pid rc=0
  shift
  declare -F sweeper_start > /dev/null && declare -F sweeper_wait > /dev/null ||
    { echo "REFUSED: the shared sweeper (gates/sweeper.sh) is not sourced: ours' timed run would go unswept"; return 2; }
  rm -f "$d/timed.window" "$d/timed.tracer.tsv"
  (
    st="" t0=""
    for ((i = 0; i < SWEEP_START_WAIT_S * 20; i++)); do
      { IFS= read -r -d '' st < "$d/timed.tracer.tsv"; } 2> /dev/null
      [[ $st == *$'\nSWEEP start '* ]] && break
      sleep 0.05
    done
    [[ $st == *$'\nSWEEP start '* ]] ||
      { echo "no start sweep within ${SWEEP_START_WAIT_S}s: the command was not run" > "$d/timed.window"; exit 125; }
    t0=$EPOCHREALTIME
    "$@"
    rc=$?
    echo "t0=$t0 t1=$EPOCHREALTIME rc=$rc" > "$d/timed.window"
    exit $rc
  ) &
  pid=$!
  sweeper_start "$d/timed" "$pid" ""
  wait "$pid" || rc=$?
  sweeper_wait
  if grep -q '^no start sweep' "$d/timed.window" 2> /dev/null; then
    echo "REFUSED: $(cat "$d/timed.window")"; return 2
  fi
  return $rc
}

ours_timed_exec() { # ours_timed_exec D CMD...
  local sw=$T3LIB_DIR/../gates/sweeper.sh
  [ -f "$sw" ] || { echo "REFUSED: no shared sweeper at $sw: ours' timed run would go unswept"; return 2; }
  bash -c 'source "$1" && source "$2" && shift 2 && ours_timed_run "$@"' ours-timed "$sw" "$T3LIB_DIR/t3lib.sh" "$@"
}

ours_timed_record() { # ours_timed_record D READER
  python3 -B - "$1" "$2" << 'EOF'
import json, os, sys
d, reader = sys.argv[1:3]
sys.path.insert(0, os.path.dirname(os.path.abspath(reader)))
import sweeper_record  # noqa: E402
rec = {"system": "ours", "sweeper_window": "process", "t0": None, "t1": None, "rc": None, "sweeper_cpu_s": None,
       "sweeper_cpu_span_s": None, "sweeper_nice": None, "sweeper_clk_tck": None, "sweeper_refused": None}
try:
    w = open(os.path.join(d, "timed.window")).read().split()
except OSError:
    w = []
kv = dict(x.split("=", 1) for x in w if "=" in x)
try:
    rec["t0"], rec["t1"], rec["rc"] = float(kv["t0"]), float(kv["t1"]), int(kv["rc"])
except (KeyError, ValueError):
    rec["sweeper_refused"] = f"no timed window in timed.window ({' '.join(w)!r})"
if rec["sweeper_refused"] is None:
    try:
        lines = open(os.path.join(d, "timed.tracer.tsv")).read().splitlines()
    except OSError as e:
        lines, rec["sweeper_refused"] = None, f"timed.tracer.tsv unreadable ({type(e).__name__})"
    if lines is not None:
        r, why = sweeper_record.sweeper_record(lines, rec["t0"], rec["t1"])
        if r is None:
            rec["sweeper_refused"] = "; ".join(why)
        else:
            rec.update(r)
with open(os.path.join(d, "timed.json"), "w") as f:
    json.dump(rec, f)
print("timed.json: sweeper_cpu_s", rec["sweeper_cpu_s"], rec["sweeper_refused"] or "")
sys.exit(0 if rec["sweeper_cpu_s"] is not None else 1)
EOF
}
