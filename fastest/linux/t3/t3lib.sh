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
#                                       when the sweeper is not sourced or its start sweep never came. That window is
#                                       the wrapper's PROCESS window; the sweeper's cost is read over the measured span
#                                       inside it (ours_timed_record), as the competitors' is
#   ours_timed_exec D CMD...            ours_timed_run in an EXEC'D bash that sources gates/sweeper.sh (beside this
#                                       file's t3/) and this file: foreign_cpu.py classes a process as the run's own
#                                       (sut) by FASTEST_CELL in /proc/PID/environ, which is the environment at exec,
#                                       so a sweeper forked straight from t3run's shell would read as foreign while the
#                                       competitors' (inside the exec'd run_system.sh) reads as sut. t3run calls this.
#                                       Returns 2, CMD never run, when gates/sweeper.sh is not there or fails to source.
#                                       Blind spot, stated: a CMD that itself exits 2 reads as rc 2 too; adapter.txt's
#                                       REFUSED line tells them apart (summarize marks either as no result)
#   ours_timed_record D READER          D/timed.json: READER's (sweeper_record.py) reading of D/timed.tracer.tsv over
#                                       fastest_profile's MEASURED span (D/result/summary.json tm0/tm1_realtime_s, since
#                                       bb96279f3; the competitors' names), which must lie inside the process window of
#                                       D/timed.window (kept as process_t0/process_t1, with its rc): sweeper_cpu_s and
#                                       the reader's other fields, sweeper_window "measured"; on a missing window or
#                                       span, a span outside it, or a refused record, sweeper_cpu_s null and
#                                       sweeper_refused the reason, and rc 1

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
  local b=$1/block/$2 d=$1/block/$2/device wwid dev model serial fw f seq
  # a DRY-VIRT run's virtual disk (t3run sets T3_IDENTITY_MODE=virtio in its --dry-block branch alone, and drops any
  # inherited value first): virtio-blk exposes no wwid, model, serial or firmware, so it is identified by the diskseq
  # the kernel gives each disk it attaches and does not reuse while it runs, beside its MAJ:MIN and the virtio vendor
  if [ "${T3_IDENTITY_MODE:-}" = virtio ] && [[ $(t3_first_line "$d/modalias") == virtio:d00000002* ]]; then
    seq=$(t3_first_line "$b/diskseq"); dev=$(t3_first_line "$b/dev")
    [[ $seq =~ ^[0-9]+$ ]] || { echo "REFUSED: cannot read $2's diskseq under $1/block/$2 (a virtio disk's identity)" >&2; return 2; }
    [ -n "$dev" ] || { echo "REFUSED: cannot read $2's dev under $1/block/$2" >&2; return 2; }
    printf 'wwid=virtio-diskseq:%s\ndev=%s\nmodel=virtio-blk %s\nserial=diskseq-%s\nfirmware=virtio\n' "$seq" "$dev" \
      "$(t3_first_line "$d/vendor")" "$seq"
    return 0
  fi
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
  # REG = DRY-VIRT: a --dry-block run on a VM's virtual disk (never credited: stamped DRY-VIRT, which summarize refuses
  # for credit); only the A17 registration is skipped, every other step and refusal stands
  if [ "$reg" = DRY-VIRT ]; then
    echo "DRY-VIRT: the A17 registration is not asked of a dry run on a virtual disk ($dev, $fs)"
  else
    registered_ok "$sys" "$reg" "$dev" "$plp" "$fs" || return 2
  fi
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

virt_ok() { # virt_ok MODE VIRT: MODE real refuses any virtualization (gate-6 review 8: what a flush reaches behind a
  # hypervisor is unknown, so a T3 box must be bare metal); MODE dryblock allows it (a --dry-block run on a VM's
  # virtual disk, stamped DRY-VIRT). Any other mode refuses. Returns 2 on a refusal
  case $1 in
    real) [ "$2" = none ] || { echo "REFUSED: systemd-detect-virt says '${2:-unknown}': a T3 box must be bare metal"; return 2; } ;;
    dryblock) return 0 ;;
    *) echo "REFUSED: virt_ok mode '$1' is neither real nor dryblock"; return 2 ;;
  esac
}

HOST_MIN_FREE_GB=35  # the lead's --dry-block ruling: no VM dry run builds while the Mac host has less free
host_room_ok() { # host_room_ok FREE_KB [MIN_GB]: the host has at least MIN_GB (35) GiB free. Returns 2 otherwise
  local free=$1 min=${2:-$HOST_MIN_FREE_GB}
  [[ $free =~ ^[0-9]+$ ]] || { echo "REFUSED: the host's free space reads '$free', not a number of KiB"; return 2; }
  [ "$free" -ge $((min * 1024 * 1024)) ] ||
    { echo "REFUSED: the host has $((free / 1024 / 1024)) GiB free, under the $min GiB a VM dry run needs"; return 2; }
}

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
  bash -c 'source "$1" || { echo "REFUSED: $1 failed to source"; exit 2; }
           source "$2" || { echo "REFUSED: $2 failed to source"; exit 2; }
           shift 2; ours_timed_run "$@"' ours-timed "$sw" "$T3LIB_DIR/t3lib.sh" "$@"
}

ours_timed_record() { # ours_timed_record D READER
  python3 -B - "$1" "$2" << 'EOF'
import json, os, sys
d, reader = sys.argv[1:3]
sys.path.insert(0, os.path.dirname(os.path.abspath(reader)))
import sweeper_record  # noqa: E402
rec = {"system": "ours", "sweeper_window": "measured", "t0": None, "t1": None, "process_t0": None, "process_t1": None,
       "rc": None, "sweeper_cpu_s": None, "sweeper_cpu_span_s": None, "sweeper_nice": None, "sweeper_clk_tck": None,
       "sweeper_refused": None}
try:
    w = open(os.path.join(d, "timed.window")).read().split()
except OSError:
    w = []
kv = dict(x.split("=", 1) for x in w if "=" in x)
try:
    rec["process_t0"], rec["process_t1"], rec["rc"] = float(kv["t0"]), float(kv["t1"]), int(kv["rc"])
except (KeyError, ValueError):
    rec["sweeper_refused"] = f"no timed window in timed.window ({' '.join(w)!r})"
if rec["sweeper_refused"] is None:
    try:
        sj = json.load(open(os.path.join(d, "result", "summary.json")))
        tm = (float(sj["tm0_realtime_s"]), float(sj["tm1_realtime_s"]))
    except (OSError, ValueError, KeyError, TypeError):
        tm = None
        rec["sweeper_refused"] = "result/summary.json carries no tm0_realtime_s/tm1_realtime_s: no measured span"
    if tm is not None and not rec["process_t0"] <= tm[0] <= tm[1] <= rec["process_t1"]:
        rec["sweeper_refused"] = (f"the measured span {tm} lies outside the process window "
                                  f"{(rec['process_t0'], rec['process_t1'])}")
    elif tm is not None:
        rec["t0"], rec["t1"] = tm
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

# host_quiet REC / host_unquiet REC -- the class "the host acts during a measured window" (second full --dry-block
# run, 2026-10-15: apt-daily.timer's unattended-upgrade voided a cell as foreign CPU; lima-guestagent's SyncTime
# stepped CLOCK_REALTIME every 10-20 s and stracecount refused the windows). Before the first cell, host_quiet stops
# EVERY active systemd timer (no allowlist: nothing scheduled may start mid-run), the service each one activates when
# it is running, and every active clock adjuster in T3_CLOCK_ADJUSTERS, recording each as "<kind>\t<unit>" in REC.
# Then it judges the OUTCOME, not the list: CLOCK_REALTIME minus CLOCK_MONOTONIC (T3_CLOCKDELTA) must move by at most
# T3_QUIET_MAX_S over a watch of T3_QUIET_WATCH_S, or it refuses, naming the movement; so an adjuster nobody listed
# is caught by its effect. Refuses (rc 1) also when list-timers fails, a stopped unit is still active, or a clock
# sample is unreadable. host_unquiet starts the recorded timers and clock adjusters again (a timer starts its own
# service). Blind spots, stated: cron and anacron jobs and non-systemd daemons are seen only by foreign_cpu.py
# (VOID). A slew (adjtime, the kernel PLL) moves CLOCK_REALTIME and CLOCK_MONOTONIC together, so it is invisible here
# and equally to stracecount's realtime-against-monotonic check; only a step (clock_settime, ADJ_SETOFFSET) moves the
# difference, and that is what both refuse.
T3_CLOCK_ADJUSTERS=${T3_CLOCK_ADJUSTERS:-"systemd-timesyncd chrony chronyd ntp ntpd ntpsec openntpd lima-guestagent"}
t3_clockdelta() { # CLOCK_REALTIME minus CLOCK_MONOTONIC, in seconds: the default sampler (T3_CLOCKDELTA overrides it)
  python3 -B -c 'import time; print(time.clock_gettime(time.CLOCK_REALTIME) - time.clock_gettime(time.CLOCK_MONOTONIC))'
}
host_quiet() {
  local rec=$1 sc=${T3_SYSTEMCTL:-sudo -n systemctl} timers u svc bad="" d0 d1
  local clk=${T3_CLOCKDELTA:-t3_clockdelta}
  : > "$rec" || return 1
  timers=$(${sc} list-timers --all --no-legend --plain) || { echo "REFUSED: host_quiet: systemctl list-timers failed"; return 1; }
  while read -r u svc; do
    [ -n "$u" ] || continue
    if [ "$(${sc} is-active "$u" 2> /dev/null)" = active ]; then
      ${sc} stop "$u"; printf 'timer\t%s\n' "$u" >> "$rec"
    fi
    if [ -n "$svc" ] && [ "$(${sc} is-active "$svc" 2> /dev/null)" = active ]; then
      ${sc} stop "$svc"; printf 'service\t%s\n' "$svc" >> "$rec"
    fi
  done < <(printf '%s\n' "$timers" | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /\.timer$/) { print $i, $(i + 1); break } }')
  for u in $T3_CLOCK_ADJUSTERS; do
    if [ "$(${sc} is-active "$u" 2> /dev/null)" = active ]; then
      ${sc} stop "$u"; printf 'clock\t%s\n' "$u" >> "$rec"
    fi
  done
  while IFS=$'\t' read -r _ u; do
    [ "$(${sc} is-active "$u" 2> /dev/null)" = active ] && bad="$bad $u"
  done < "$rec"
  [ -z "$bad" ] || { echo "REFUSED: host_quiet: still active after stop:$bad"; return 1; }
  d0=$($clk) && sleep "${T3_QUIET_WATCH_S:-30}" && d1=$($clk) && [ -n "$d0" ] && [ -n "$d1" ] ||
    { echo "REFUSED: host_quiet: cannot sample the clocks ('${d0:-}' then '${d1:-}')"; return 1; }
  python3 -B -c 'import sys
d0, d1, w, m = float(sys.argv[1]), float(sys.argv[2]), sys.argv[3], float(sys.argv[4])
if abs(d1 - d0) > m:
    sys.exit(f"REFUSED: host_quiet: CLOCK_REALTIME moved {d1 - d0:+.6f} s against CLOCK_MONOTONIC in {w} s with every"
             f" timer and listed clock adjuster stopped (limit {m} s): something still adjusts the clock")
print(f"host_quiet: CLOCK_REALTIME - CLOCK_MONOTONIC held within {abs(d1 - d0):.6f} s over {w} s")' \
    "$d0" "$d1" "${T3_QUIET_WATCH_S:-30}" "${T3_QUIET_MAX_S:-0.002}" 2>&1 || return 1
  echo "host_quiet: stopped $(grep -c . "$rec") unit(s) (recorded in $rec)"
}

host_unquiet() {
  local rec=$1 sc=${T3_SYSTEMCTL:-sudo -n systemctl} kind u rc=0
  [ -f "$rec" ] || return 0
  while IFS=$'\t' read -r kind u; do
    case $kind in timer|clock) ${sc} start "$u" || rc=1 ;; esac
  done < "$rec"
  return $rc
}

# ours_parent DIR ROWS AGE / ours_fixture_record D ROWS AGE LIVE STORE -- ONE parent for every system (gate-6 t3run
# HIGH 4; the owed ours fixture record, 2026-10-16: run 11's ours ran fastest_profile's own 1000-row seed beside the
# competitors' 10000-row gen_seed parent). ours_parent writes comp's gen_seed.py streams for (ROWS, AGE): DIR/seed.sql
# and DIR/age.sql with their digests (--digest-out), which fastest_profile --seed-sql/--age-sql feeds statement by
# statement, as the competitors' seeds do. ours_fixture_record writes D/result/fixture.json through comp's own
# fixture.py write: the plan's rows, age and prebranch LIVE, the live count fastest_profile MEASURED after its
# prebranch (summary.json fixture.live_branches), du over STORE, the streams' digests, and ours' read-back of the
# trunk (ours_readback.py over D/parent/trunk.tsv, fastest_profile --dump-trunk). fixture.py write compares the
# read-back and the streams with gen_seed's facts and exits non-zero on a difference, after writing the record; so
# does this, and summarize judges the record itself (readback_problems, one_parent).
ours_parent() {
  local d=$1 r=$2 k=$3 g="$T3LIB_DIR/../competitors/gen_seed.py"
  mkdir -p "$d" || return 1
  python3 -B "$g" sql --rows "$r" --digest-out "$d/sql.sha256" > "$d/seed.sql" &&
    python3 -B "$g" age --rows "$r" --updates "$k" --digest-out "$d/age.sha256" > "$d/age.sql"
}

ours_fixture_record() {
  local d=$1 r=$2 k=$3 n=$4 st=$5 live rb
  live=$(python3 -B -c 'import json, sys
fx = json.load(open(sys.argv[1])).get("fixture")
v = fx.get("live_branches") if isinstance(fx, dict) else None
sys.exit("no fixture record in " + sys.argv[1]) if type(v) is not int else print(v)' "$d/result/summary.json") ||
    { echo "REFUSED: ours_fixture_record: no fixture record in $d/result/summary.json (fastest_profile ran without --seed-sql)"; return 1; }
  rb=$(python3 -B "$T3LIB_DIR/ours_readback.py" "$d/parent/trunk.tsv") ||
    { echo "REFUSED: ours_fixture_record: no read-back of $d/parent/trunk.tsv"; return 1; }
  python3 -B "$T3LIB_DIR/../competitors/fixture.py" write "$d/result/fixture.json" --system ours --rows "$r" --age "$k" \
    --prebranch "$n" --live "$live" --du "$st" --engine-why "ours: a branch store of several files, sized by du" \
    --streams "$d/parent/sql.sha256" "$d/parent/age.sha256" --readback "$rb" > /dev/null
}

# sudo_state -- whether this host gives passwordless root (linux-rev-uw1 LOW 6): 'ok'; 'password' (sudo is there and
# asks for one: a no-root host, so a root case is N/A-on-noroot there, which t3run's selftests refuse on the T3 box);
# 'absent' (no sudo at all) or 'error: <text>' (anything else), each a FAIL of a root case, never an N/A.
# T3_SUDO_BIN names the sudo to ask (tests only; t3run unsets it)
sudo_state() {
  local s=${T3_SUDO_BIN:-sudo} e
  command -v "$s" > /dev/null 2>&1 || { echo absent; return 0; }
  e=$("$s" -n true 2>&1) && { echo ok; return 0; }
  case $e in *"password is required"*|*"a terminal is required"*) echo password ;; *) echo "error: ${e:-no output}" ;; esac
}
