#!/bin/bash
# ONE-COMMAND T3 RUNNER (FASTEST T3-READY gate item 6): a fresh Ubuntu 24.04 box -> finished raws.
#
#   curl -sSfL https://raw.githubusercontent.com/RyanLin5967/turso/<SHA>/fastest/linux/t3/t3run.sh \
#     | bash -s -- --sha <SHA> --out ~/t3-out [--dry-run [--block loop|brd] [--plant NAME] [--fs "xfs btrfs"]] \
#                  [--manifest <path in repo>] [--device /dev/nvmeXnY --destroy /dev/nvmeXnY --plp yes|no] [--seed N]
#
# Run from outside a checkout, it clones github.com/RyanLin5967/turso at --sha into <out>.src and runs
# its own copy from there, so the script, the engine, the tools and the cell manifest are all one commit.
#
# Stages (each timed into <out>/stages.tsv; the whole run's wall time is the last row):
#   preflight  Ubuntu, passwordless sudo, free disk, the out dir new, the mode's refusals (below)
#   selftests  the harness's own self-tests and the devguard root fire, before anything is paid for
#   deps       apt (build tools, fio, strace, nvme-cli, smartmontools, fs tools, PGDG postgresql-18 without a
#              cluster, MariaDB client), rustup with the repo's pinned toolchain
#   build      the engine driver fastest_profile (release, debug symbols), bbload/clonebench/sqlite3
#              (competitors/build.sh), Dolt + Doltgres release binaries (competitors/fetch_dolt.sh), v3floor and
#              the fire-check's statfs shim
#   envchecks  firecheck.sh's environment with t3run's V3ENV, positive and negative (needs the build), and the
#              three drivers' warm-up decision against PREREG annex A23 (gates/warmup_conformance.py run on the
#              built bbload, clonebench and fastest_profile: all three must match every case, rc 0; fourth lane
#              review LOW 14: a run whose drivers disagree on the warm-up is not creditable for it)
#   hwid       hwid.sh (machine, NVMe id-ctrl VWC, feature 0x06, smartctl text naming power loss, every mount)
#   per filesystem in --fs (one block): make it, hw record with fio, hwid of that target, the V3 probe's
#              fire-check on the block's explicit V3 cell, a V3 batch BEFORE, then the manifest's runs for that fs in
#              the plan's section-7 blocks K = 1..k with V3L at every block boundary (b0 before block 1, bK after
#              block K, which is also before K+1; T3 runner review LOW 24), each run with the foreign-CPU sampler
#              and its void decision made before any result of the run is read (a void run is replaced once, at
#              most twice per cell), then a V3 batch AFTER, then the batches' drift (batchgate.py drift: published, not a gate on T3; a REFUSED
#              drift fails). Each batch is judged by blockgate.py (review 2 item 5, rulings A14 and A16: every VOID
#              fails), and the A16 plants run on copies of the BEFORE batch's real record; a batch blockgate fails, a
#              plant that does not fire, or a V3L that is not VALID (v3l.py) fails the block's stage (items 5 and 6).
#              Before each run (every attempt), settle.sh fstrims and waits for the filesystem's device to go quiet
#              (btrfs mounts nodiscard; a discard mount refuses). A failed block
#              is recorded, its filesystem torn down, and the NEXT block runs (gate-6 review H1: one block's void
#              must not lose the rest of a rental); the run exits 1 at the end.
#   package    <out>.tar.gz + SHA256SUMS + summary.json (stages, blocks with their V3/V3L records, cells,
#              verdicts, wall time)
#
# V3 cells (review 2 item 4; fastest/linux/v3/v3cell.py), never inferred from the fs name:
#   --dry-run --block loop (default)  <fs>loop: the fs on a loop device whose backing file is on the root disk
#   --dry-run --block brd             <fs> on brd's /dev/ram0 (v3/mkbrd.sh; xfs and btrfs): a block device with no
#                                     loop, as a rental's data disk; brd is fire-check only, so its V3 batches run
#                                     V3_SMOKE=1 (run.sh's bound mode refuses brd)
#   real run                          <fs> on --device, V3 batches bound to the block's fire-check verdict, with
#                                     V3_REQUIRE_T3=1 (the governor is set to performance in deps, and the T3
#                                     preconditions are checked there, before the build is paid for: review L1);
#                                     --plp yes|no is required: the operator's declaration of power-loss protection
#                                     (a fact of the rental: --plp yes takes a write-back drive out of A14's timing
#                                     control, and needs the drive's model and firmware in t3/PLP-DRIVES); preflight
#                                     checks the registered frame arm and D0 thresholds (A17) before anything is paid
# Modes. --dry-run: smoke manifest allowed, loop or brd filesystems, the root remounted with barrier, results
# never credited; --plant NAME (dry runs only) breaks one thing on purpose so the run must fail:
#   v3-verdict-missing  the BEFORE V3 batch is bound to a verdict file that does not exist (run.sh rc 2)
#   v3l-fsync-half      the BEFORE V3L's fio syncs every 2nd write (V3L_PLANT=fsync2: V1L and fio must VOID it)
#   v3l-cache-lie       a write-cache lie under V3L BEFORE, on a write-back drive (a write-through draw is NOT-RUN:
#                       the kernel will not let its queue claim write back, T3 runner review item 10): the block's
#                       loop is set to write through (fsyncs stop reaching the drive, and the drive's flush counter
#                       gate must VOID it). The original is saved, restored right after under an EXIT trap, and read
#                       back.
# A plant applies to the FIRST block only, so the second block shows that a failed block does not stop the run.
# Without --dry-run (a real T3 rental) it REFUSES unless: the manifest's sha256 is listed in
# fastest/linux/t3/REGISTERED-MANIFESTS (append-only; empty until the T3 registration), --device and --destroy
# name the same device and devguard.py allows it (an allowlist: a whole NVMe/SCSI/virtio disk, not the root disk or
# on its controller, nothing mounted, held or claimed, no signature or partition table; review M4, devguard round-2
# attack MED 2 and LOW 3), --plp is given, --fs is not (the manifest names the blocks; review M5), and every target
# mounts with barriers. The device is resolved once at preflight (DEV_REAL) and the drive's identity recorded
# (device-id.txt: wwid, MAJ:MIN, model, serial, firmware); every block re-checks, before its mkfs, its mount and the
# cleanup's wipefs, that --device still resolves there to that same drive, still PLP-listed and registered, and
# re-runs devguard before the mkfs; mkfs, mount and wipefs act on DEV_REAL only (devguard round-2 attack LOW 4,
# review 5 MED 3). So a block starts from a disk that passes devguard:
# block_cleanup wipes the filesystem its block made (after checking it is that one), and the operator wipes the
# first by hand after reading what it holds.
# Every file the run calls must be in the commit (preflight lists them; review 2 item 18). One warm-up rule for every
# system (competitors/timedrun.py rule at the 1800 s cap, OPS:S:MAX_S) goes to fastest_profile and run_system.sh alike.
# PREREG citations as ':N' or 'line N' are lines of artie frontier/fastest/PREREG-v1-FINAL-CANDIDATE.md, the text the
# rulings cite, until PREREG-v1.md is registered (fourth lane review LOW 26).
# Exit: 0 every stage ran and every cell has a verdict; 1 a stage failed; 2 refused before anything ran.
set -uo pipefail
REPO_URL=https://github.com/RyanLin5967/turso
SHA="" OUT="" DRY=0 MANIFEST="" FSLIST="" DEVICE="" DESTROY="" SEED=20261005 BLOCK="" PLANT="" PLP="" DEV_REAL=""
DEV_NAME=""
while [ $# -gt 0 ]; do
  case $1 in
    --sha) SHA=$2; shift ;;
    --out) OUT=$2; shift ;;
    --dry-run) DRY=1 ;;
    --manifest) MANIFEST=$2; shift ;;
    --fs) FSLIST=$2; shift ;;
    --device) DEVICE=$2; shift ;;
    --destroy) DESTROY=$2; shift ;;
    --seed) SEED=$2; shift ;;
    --block) BLOCK=$2; shift ;;
    --plant) PLANT=$2; shift ;;
    --plp) PLP=$2; shift ;;
    *) echo "t3run: unknown argument $1" >&2; exit 2 ;;
  esac
  shift
done
[ -n "$SHA" ] && [ -n "$OUT" ] || { echo "usage: t3run.sh --sha SHA --out DIR [--dry-run] ..." >&2; exit 2; }
OUT=$(realpath -m "$OUT")

# Re-exec from a checkout of --sha, so everything below is that one commit.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" 2>/dev/null && pwd || true)
if [ -z "${T3RUN_INSIDE:-}" ]; then
  [ -e "$OUT" ] && { echo "t3run: REFUSED: $OUT exists (every run starts empty)" >&2; exit 2; }
  SRC="$OUT.src"
  if [ -n "$HERE" ] && [ -f "$HERE/t3run.sh" ] && git -C "$HERE" rev-parse --git-dir >/dev/null 2>&1 &&
     [ "$(git -C "$HERE" rev-parse HEAD)" = "$(git -C "$HERE" rev-parse "$SHA^{commit}" 2>/dev/null)" ]; then
    SRC=$(git -C "$HERE" rev-parse --show-toplevel)
  else
    command -v git >/dev/null || { sudo apt-get update -q && sudo apt-get install -y -q git; }
    git clone -q --filter=blob:none "$REPO_URL" "$SRC" && git -C "$SRC" checkout -q --detach "$SHA" ||
      { echo "t3run: REFUSED: cannot check out $SHA from $REPO_URL" >&2; exit 2; }
  fi
  T3RUN_INSIDE=1 exec bash "$SRC/fastest/linux/t3/t3run.sh" --sha "$SHA" --out "$OUT" \
    $([ $DRY = 1 ] && echo --dry-run) ${MANIFEST:+--manifest "$MANIFEST"} ${FSLIST:+--fs "$FSLIST"} \
    ${DEVICE:+--device "$DEVICE"} ${DESTROY:+--destroy "$DESTROY"} --seed "$SEED" \
    ${BLOCK:+--block "$BLOCK"} ${PLANT:+--plant "$PLANT"} ${PLP:+--plp "$PLP"}
fi

SRC=$(git -C "$HERE" rev-parse --show-toplevel)
L=$SRC/fastest/linux
# settle_dev and settle (the quiet gap before every run); a missing file is caught by NEEDS before anything runs
[ -f "$L/t3/settle.sh" ] && source "$L/t3/settle.sh"
# drift_ok, fslist_ok, registered_ok, plp_listed: checks only a real run reaches, tested by t3lib_test.sh
[ -f "$L/t3/t3lib.sh" ] && source "$L/t3/t3lib.sh"
SETTLE_DEV=""
if [ $DRY = 1 ]; then
  case ${BLOCK:=loop} in loop|brd) ;; *) echo "t3run: REFUSED: --block is loop or brd, not '$BLOCK'" >&2; exit 2 ;; esac
  case $PLANT in ''|v3-verdict-missing|v3l-fsync-half|v3l-cache-lie) ;; *) echo "t3run: REFUSED: unknown plant '$PLANT'" >&2; exit 2 ;; esac
  # brd batches are smoke (unbound) and brd has no loop, so these plants would plant nothing there
  case $PLANT:$BLOCK in v3-verdict-missing:brd|v3l-cache-lie:brd)
    echo "t3run: REFUSED: plant $PLANT needs --block loop" >&2; exit 2 ;; esac
  case ${PLP:=no} in no) ;; *) echo "t3run: REFUSED: --plp is for real runs (a dry run's drives are what they are)" >&2; exit 2 ;; esac
else
  [ -z "$BLOCK" ] || { echo "t3run: REFUSED: --block is for dry runs; a real run uses --device" >&2; exit 2; }
  [ -z "$PLANT" ] || { echo "t3run: REFUSED: --plant is for dry runs only" >&2; exit 2; }
  [ -z "$FSLIST" ] || { echo "t3run: REFUSED: --fs is for dry runs; a real run runs every block its manifest names" >&2; exit 2; }
  case $PLP in yes|no) ;; *) echo "t3run: REFUSED: a real run needs --plp yes|no (the drive's power-loss protection, per the rental)" >&2; exit 2 ;; esac
  BLOCK=device
fi
[ -z "$MANIFEST" ] && MANIFEST=fastest/linux/t3/cells-smoke.tsv
MAN=$SRC/$MANIFEST
mkdir -p "$OUT" || exit 2
STAGES=$OUT/stages.tsv
printf 'stage\tstart_utc\tend_utc\tseconds\trc\n' > "$STAGES"
T_ALL=$(date +%s)
DIST=$OUT/dist
mkdir -p "$DIST" "$OUT/cells"
exec > >(tee -a "$OUT/t3run.log") 2>&1
echo "# t3run sha=$SHA src=$SRC out=$OUT dry=$DRY block=$BLOCK plant=${PLANT:-none} manifest=$MANIFEST seed=$SEED start=$(date -u +%FT%TZ)"
printf 'dry=%s\nblock=%s\nplant=%s\nplp=%s\n' "$DRY" "$BLOCK" "$PLANT" "$PLP" > "$OUT/mode.txt"

stage() { # stage <name> <function>: time it, record rc; a failed stage ends the run (rc 1)
  local name=$1 s e rc
  s=$(date +%s)
  echo "== stage $name $(date -u +%FT%TZ)"
  "$2"
  rc=$?
  e=$(date +%s)
  printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$(date -u -d @"$s" +%FT%TZ)" "$(date -u -d @"$e" +%FT%TZ)" $((e - s)) $rc >> "$STAGES"
  if [ $rc -ne 0 ]; then
    echo "t3run: stage $name FAILED rc=$rc"
    # a preflight refusal (rc 2) ran nothing; any other failure is a failed stage
    [ "$name" = preflight ] && [ $rc = 2 ] && finish 2
    finish 1
  fi
}

finish() {
  local rc=$1 t
  t=$(( $(date +%s) - T_ALL ))
  printf '%s\t%s\t%s\t%s\t%s\n' TOTAL "$(date -u -d @"$T_ALL" +%FT%TZ)" "$(date -u +%FT%TZ)" "$t" "$rc" >> "$STAGES"
  python3 -B "$L/t3/summarize.py" "$OUT" "$SHA" "$DRY" "$MANIFEST" > "$OUT/summary.json" 2> "$OUT/summarize.stderr" ||
    { [ "$rc" = 0 ] && rc=1; }
  cat "$OUT/summarize.stderr"
  if ! ( cd "$(dirname "$OUT")" && tar --exclude="$(basename "$OUT")/work" -czf "$OUT.tar.gz" "$(basename "$OUT")" &&
         sha256sum "$(basename "$OUT").tar.gz" > "$OUT.tar.gz.sha256" ); then
    echo "t3run: package failed (tar or sha256sum)"; rc=1  # gate-6 review 17
  fi
  echo "# t3run done rc=$rc wall_s=$t package=$OUT.tar.gz"
  exit "$rc"
}

# Every file this run calls, by path in the commit (an allowlist: a missing one refuses here with its name,
# not hours later; review 2 item 18 found the competitors absent from the runner's home branch).
NEEDS="t3/hwid.sh t3/foreign_cpu.py t3/cells.py t3/summarize.py t3/v3l.py t3/blockgate.py t3/devguard.py t3/PLP-DRIVES t3/testdata
  t3/settle.sh t3/settle_test.sh t3/t3lib.sh t3/t3lib_test.sh gates/warmup_conformance.py
  hw/record.sh fs/mkloop.sh
  competitors/build.sh competitors/fetch_dolt.sh competitors/firecheck_strace.sh competitors/run_system.sh
  competitors/common.sh competitors/pg18.sh competitors/dolt.sh competitors/doltgres.sh competitors/stracecount.py
  competitors/fthelp.py competitors/gen_seed.py competitors/reduce.py competitors/trace.sh competitors/timedrun.py
  v3/v3floor.c v3/statfs_shim.c v3/noop_shim.c v3/v3cell.py v3/firecheck.sh v3/run.sh v3/mkfixtures.sh v3/mkbrd.sh v3/check.py
  v3/batchgate.py v3/blkflush.py v3/stamp.py v3/crash.sh v3/nsfake.sh v3/postplant.py v3/red.py v3/build.sh
  v3/REGISTERED.tsv v3/testdata"
preflight() {
  [ -f "$MAN" ] || { echo "no manifest $MAN"; return 2; }
  # the files the run calls must be in the COMMIT (review M8: an untracked leftover satisfied a disk check), and
  # the tree they run from must be that commit, unmodified
  local f miss="" head dirty
  head=$(git -C "$SRC" rev-parse HEAD) || { echo "REFUSED: $SRC is not a git checkout"; return 2; }
  for f in $NEEDS; do git -C "$SRC" cat-file -e "${head}:fastest/linux/$f" 2>/dev/null || miss="$miss fastest/linux/$f"; done
  [ -z "$miss" ] || { echo "REFUSED: commit $head lacks files the run calls:$miss"; return 2; }
  # the WHOLE tree (lane review LOW 8: a modified core/ would be built and recorded as sha=$SHA)
  dirty=$(git -C "$SRC" status --porcelain --untracked-files=all)
  [ -z "$dirty" ] || { echo "REFUSED: the checkout differs from commit $head: $(echo "$dirty" | head -5)"; return 2; }
  grep -q 'Ubuntu 24' /etc/os-release || { echo "not Ubuntu 24.04"; return 2; }
  sudo -n true || { echo "needs passwordless sudo"; return 2; }
  # one warm-up rule for every system, from the competitors' own implementation (gate-6 review 3: PREREG :210's
  # min(max(1000 ops, 10 s), 10% of the cap) as OPS:S:MAX_S), passed verbatim to fastest_profile and run_system.sh
  WARMUP=$(python3 -B "$L/competitors/timedrun.py" rule "$RUN_CAP_S") &&
    [[ $WARMUP =~ ^[0-9]+:[0-9]+(\.[0-9]+)?:[0-9]+(\.[0-9]+)?$ ]] ||
    { echo "REFUSED: competitors/timedrun.py rule $RUN_CAP_S gave '$WARMUP', not OPS:S:MAX_S"; return 2; }
  echo "warm-up rule $WARMUP (cap ${RUN_CAP_S}s)" | tee "$OUT/warmup.txt"
  local msha; msha=$(sha256sum "$MAN" | cut -c1-64)
  echo "manifest $MANIFEST sha256=$msha"
  echo "$msha" > "$OUT/manifest.sha256"
  cp "$MAN" "$OUT/manifest.tsv"
  if [ $DRY = 0 ]; then
    grep -qx "$msha" "$L/t3/REGISTERED-MANIFESTS" 2>/dev/null ||
      { echo "REFUSED: manifest $msha is not registered (fastest/linux/t3/REGISTERED-MANIFESTS)"; return 2; }
    [ -n "$DEVICE" ] && [ "$DEVICE" = "$DESTROY" ] ||
      { echo "REFUSED: a real run needs --device D --destroy D naming the same device"; return 2; }
    [ -b "$DEVICE" ] || { echo "REFUSED: $DEVICE is not a block device"; return 2; }
    sudo -n python3 -B "$L/t3/devguard.py" check "$DEVICE" > "$OUT/devguard.txt" 2>&1 || { cat "$OUT/devguard.txt"; return 2; }
    # the node devguard just checked; every block's mkfs re-checks against it (devguard round-2 attack LOW 4)
    DEV_REAL=$(readlink -f "$DEVICE")
    [ -b "$DEV_REAL" ] || { echo "REFUSED: $DEVICE resolves to '$DEV_REAL', not a block device"; return 2; }
    echo "$DEV_REAL" > "$OUT/device-real.txt"
    # the drive itself, which every mkfs, mount and wipefs re-reads (t3lib.sh dev_unchanged; review 5 MED 3): the
    # path alone is a tautology for a plain /dev/nvmeXnY, and the PLP and registration checks below hold for this drive
    DEV_NAME=$(basename "$DEV_REAL")
    dev_identity /sys "$DEV_NAME" > "$OUT/device-id.txt" 2> "$OUT/device-id.err" ||
      { cat "$OUT/device-id.err"; echo "REFUSED: cannot record $DEV_NAME's identity"; return 2; }
    # a virtualized box cannot be T3 hardware: what a flush reaches behind a hypervisor is unknown (gate-6 review 8)
    local virt; virt=$(systemd-detect-virt 2>/dev/null || true)
    [ "$virt" = none ] || { echo "REFUSED: systemd-detect-virt says '${virt:-unknown}': a T3 box must be bare metal"; return 2; }
    # --plp yes takes the drive out of the timing control (A14/A16), so it must name a registered drive: model and
    # firmware listed in fastest/linux/t3/PLP-DRIVES (append-only; lane review MED 1)
    local dn=$DEV_NAME fsl
    # --plp yes names a registered drive: model and firmware (NVMe firmware_rev, SCSI/SATA rev) in t3/PLP-DRIVES
    if [ "$PLP" = yes ]; then
      plp_listed /sys "$dn" "$L/t3/PLP-DRIVES" ||
        { echo "REFUSED: --plp yes but $dn ($(plp_drive_id /sys "$dn" 2>&1)) is not in fastest/linux/t3/PLP-DRIVES"; return 2; }
    fi
    # the A17 registration (frame arm; the D0 threshold of every block on a write-back drive without PLP), before
    # deps, build and fire-checks are paid for (t3lib.sh registered_ok, tested by t3lib_test.sh)
    fsl=$(python3 -B "$L/t3/cells.py" fslist "$MAN") && fslist_ok "$fsl" ||
      { echo "REFUSED: the manifest names no filesystem block (cells.py fslist: '$fsl')"; return 2; }
    registered_ok /sys "$L/v3/REGISTERED.tsv" "$dn" "$PLP" "$fsl" || return 2
  fi
  df -h "$(dirname "$OUT")"
  return 0
}

deps() {
  sudo apt-get update -q || return 1
  sudo apt-get install -y -q postgresql-common ca-certificates curl git || return 1
  grep -q '^create_main_cluster = false' /etc/postgresql-common/createcluster.conf 2>/dev/null ||
    echo 'create_main_cluster = false' | sudo tee -a /etc/postgresql-common/createcluster.conf >/dev/null
  sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y || return 1
  sudo apt-get install -y -q build-essential clang cmake pkg-config python3 fio strace lsof nvme-cli \
    smartmontools dmidecode xfsprogs btrfs-progs libpq-dev libmariadb-dev mariadb-client libmariadb3 \
    postgresql-18 libpq5 || return 1
  sudo systemctl stop postgresql 2>/dev/null || true
  # The competitor servers listen on fixed ports inside Linux's ephemeral range (32768-60999); a client
  # socket that drew one as its source port made PG's bind fail with EADDRINUSE after the listener-only
  # free-port check passed (dry run 37256446468, pg18-defaults on btrfs). Reserved ports are never handed
  # out as ephemeral ones. Ports: competitors/run_system.sh (PG 55432, Doltgres 55433, Dolt 53306).
  sudo sysctl -w net.ipv4.ip_local_reserved_ports=53306,55432-55433 || return 1
  if ! command -v rustup >/dev/null && [ ! -x "$HOME/.cargo/bin/rustup" ]; then
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none || return 1
  fi
  export PATH="$HOME/.cargo/bin:$PATH"
  (cd "$SRC" && rustup show active-toolchain >/dev/null 2>&1 || rustup toolchain install) || return 1
  if [ $DRY = 0 ]; then
    # the registered T3 precondition (review 2 item 17): every CPU on the performance governor; run.sh's
    # V3_REQUIRE_T3=1 refuses the V3 batches if this did not take (or the box has no cpufreq)
    local g
    for g in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor; do
      [ -e "$g" ] && echo performance | sudo tee "$g" > /dev/null
    done
    grep -H . /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor > "$OUT/governors.txt" 2>&1 || true
    # the registered T3 preconditions now, before the build and fire-checks are paid for (review L1)
    python3 -B "$L/v3/batchgate.py" t3pre > "$OUT/t3pre.json" 2>&1 || { echo "T3 preconditions do not hold:"; cat "$OUT/t3pre.json"; return 1; }
  fi
  return 0
}

build() {
  export PATH="$HOME/.cargo/bin:$PATH"
  ( cd "$SRC" && CARGO_PROFILE_RELEASE_DEBUG=true CARGO_INCREMENTAL=0 timeout 7200 \
      cargo build --release --locked -p turso_core --example fastest_profile ) > "$OUT/build-engine.txt" 2>&1 || return 1
  cp "$SRC/target/release/examples/fastest_profile" "$DIST/" || return 1
  timeout 1800 bash "$L/competitors/build.sh" "$DIST" > "$OUT/build-competitors.txt" 2>&1 || return 1
  timeout 1800 bash "$L/competitors/fetch_dolt.sh" "$DIST/dolt-bin" "$OUT/dolt-fetch.txt" > "$OUT/build-dolt.txt" 2>&1 || return 1
  # the V3 probe's ONE build (v3/build.sh, which fastest-v3.yml also calls, so the two cannot drift: dry run
  # 37798270085 failed every fire-check on t3run's own stale copy): v3floor static, v3floor.dyn, both shims
  bash "$L/v3/build.sh" "$DIST" > "$OUT/build-v3.txt" 2>&1 || return 1
  { echo "sha=$SHA"; rustc -V; gcc --version | head -1; /usr/lib/postgresql/18/bin/postgres --version
    fio --version; strace -V | head -1
    ( cd "$DIST" && sha256sum fastest_profile bbload clonebench sqlite3 v3floor v3floor.dyn statfs_shim.so noop_shim.so ); } > "$OUT/binaries.txt"
  return 0
}

hwid() { bash "$L/t3/hwid.sh" "$OUT/hwid" /; }

# The V3 fire-check's refusal fixtures (scratch loops on the root disk, never the target device), made
# once for every filesystem block.
V3FX=/mnt/t3-v3fx
# ONE environment for the V3 tools (T3 runner review HIGH 1: 2d42982a0 gave V3_PLP to run.sh only, so firecheck.sh
# refused every block): the fire-check gets all of it, run.sh its V3_PLP from the same array, and the selftests stage
# runs firecheck.sh's own environment checks with it before any block (fcenv_check).
V3ENV=(V3_PLP="$PLP" V3_FX="$V3FX" V3_SHIM="$DIST/statfs_shim.so" V3_DYN="$DIST/v3floor.dyn" V3_NOOP="$DIST/noop_shim.so")
# firecheck.sh checks its arguments, then every required variable, then refuses an OUT that exists (exit 2, "exists"):
# so with an existing OUT, reaching that refusal proves the environment passed, and any other exit-2 message names
# the variable it lacks.
fcenv_check() {
  local o=$OUT/fcenv-check rc x e
  local -a sub
  mkdir -p "$o"
  timeout 60 env "${V3ENV[@]}" bash "$L/v3/firecheck.sh" "$DIST/v3floor" ext4loop "$o/w" "$o" > "$o.txt" 2>&1
  rc=$?
  [ $rc = 2 ] && grep -qxF "firecheck: $o exists" "$o.txt" ||
    { echo "fire-check environment refused (rc $rc): $(cat "$o.txt")"; return 1; }
  # the negative control (fourth lane review LOW 9): each entry left out must be refused, naming its variable, or the
  # positive check above proves nothing about this firecheck.sh
  for e in "${V3ENV[@]%%=*}"; do
    sub=()
    for x in "${V3ENV[@]}"; do [ "${x%%=*}" = "$e" ] || sub+=("$x"); done
    timeout 60 env -u "$e" "${sub[@]}" bash "$L/v3/firecheck.sh" "$DIST/v3floor" ext4loop "$o/w" "$o" \
      > "$o-no-$e.txt" 2>&1
    rc=$?
    [ $rc = 2 ] && grep -qF "$e" "$o-no-$e.txt" && ! grep -qxF "firecheck: $o exists" "$o-no-$e.txt" ||
      { echo "fire-check environment check: without $e it gave rc $rc: $(cat "$o-no-$e.txt")"; return 1; }
  done
  rmdir "$o"
}
# The nest fixture (P_nest3) must start on the filesystem that holds the cell's leaf (V3 eighth review M5, ninth
# review HIGH), or an md/LVM root makes the probe refuse it. A loop block's leaf is the drive under its backing file,
# so the backing directory is chosen ONCE here (mkloop.sh's own rule: / or /mnt, whichever has more free space) and
# passed to both: mkfixtures.sh as V3_NEST_DIR and every mkloop.sh as LOOP_BACKING_DIR. A brd block keeps the default
# (the root fs; brd is fire-check only). A real run's leaf is --device, whose filesystem exists only inside its
# block, so fs_block re-makes the nest chain per block on it (mkfixtures.sh V3_FIXTURES=nest) and block_cleanup tears
# it down (--teardown-nest) before the umount.
LOOPDIR=""
v3fixtures() {
  local nest=""
  if [ "$BLOCK" = loop ]; then
    LOOPDIR=$(bash "$L/fs/mkloop.sh" backing-dir)
    [ -d "$LOOPDIR" ] || { echo "v3fixtures: no backing directory for the loop blocks ('$LOOPDIR')"; return 1; }
    nest=$LOOPDIR
  fi
  echo "nest dir: ${nest:-/ (default)}; loop backing dir: ${LOOPDIR:-n/a}" | tee "$OUT/v3nest.txt"
  ${nest:+env V3_NEST_DIR="$nest"} bash "$L/v3/mkfixtures.sh" "$V3FX" > "$OUT/v3fixtures.txt" 2>&1
}

# One V3 batch of a block, judged by blockgate.py (review 2 item 5, ruling A14). The rc goes to v3.rc whatever happens,
# so summarize.py re-judges every batch, including a refused one. After the BEFORE batch the A16 plants run on
# copies of its real record and its batch directory (blockgate.py plants): every one must be decided as planted.
v3batch() { # v3batch before|after DIR
  local when=$1 dir=$2 o=$OUT/fs-$FS_NOW rc frame arms
  local -a env=(V3_CELL="$V3CELL" "${V3ENV[@]}")  # the whole V3 environment (run.sh ignores the fire-check's names)
  # the registered V3 shape (PREREG section 4; V3 gate-6 MED 6; run.sh refuses a bound batch of any other):
  # N = 10000 and arms append25, fdatasync4k, nosync25 plus the registered frame arm, if one is registered
  frame=$(awk -F '\t' '$1 == "frame_arm" { v = $2 } END { print v }' "$L/v3/REGISTERED.tsv")
  arms="append25,fdatasync4k,nosync25${frame:+,$frame}"
  if [ "$BLOCK" = brd ]; then
    env+=(V3_SMOKE=1 V3FLOOR_BRD=1)
  else
    local verdict=$o/v3-firecheck/verdict.json
    [ "$PLANT_NOW:$when" = v3-verdict-missing:before ] && verdict=$o/v3-firecheck/planted-missing-verdict.json
    env+=(V3_FIRECHECK_VERDICT="$verdict")
    [ $DRY = 0 ] && env+=(V3_REQUIRE_T3=1)
  fi
  echo "v3 $when: ${env[*]}"
  # a bound batch runs the registered shape (N=10000, run.sh refuses any other); a brd batch is smoke (never bound,
  # never credited), so it runs N=200 like the fire-check's F3, not 50x that (T3 runner review LOW 25)
  local n=10000; [ "$BLOCK" = brd ] && n=200
  env "${env[@]}" timeout 3600 bash "$L/v3/run.sh" "$DIST/v3floor" "$dir" "$o/v3-$when" "$n" --arms "$arms" \
    > "$o/v3-$when.txt" 2>&1
  rc=$?
  echo "$when rc=$rc" >> "$o/v3.rc"
  python3 -B "$L/t3/blockgate.py" batch "$o/v3-$when" "$rc" "$BLOCK" "$PLP" > "$o/v3-$when.blockgate.json"
  local g=$?
  echo "V3 $when batch on $V3CELL: rc $rc, blockgate $(cat "$o/v3-$when.blockgate.json")"
  [ $g = 0 ] || { echo "V3 $when batch on $V3CELL FAILS its block (run.sh: $(tail -1 "$o/v3-$when.txt"))"; return 1; }
  if [ "$when" = before ]; then
    python3 -B "$L/t3/blockgate.py" plants "$o/v3-$when" "$rc" "$PLP" "$o/blockgate-plants.json" ||
      { echo "A14 plants on $V3CELL: not every plant was decided as planted"; return 1; }
  fi
  return 0
}

# V3L at a block boundary (review 2 item 6; PREREG line 180; gate-6 review 9: per section-7 block, not per
# filesystem): b0 before block 1, then bK after block K, which is also before block K+1 (nothing runs between).
# VOID or refused fails the stage.
v3l() { # v3l bK MNT
  local when=$1 mnt=$2 o=$OUT/fs-$FS_NOW rc knob="" orig=""
  local -a env=()
  [ $DRY = 0 ] && env+=(V3L_REAL=1)
  [ "$PLANT_NOW:$when" = v3l-fsync-half:b0 ] && env+=(V3L_PLANT=fsync2)
  if [ "$PLANT_NOW:$when" = v3l-cache-lie:b0 ]; then
    # the lie goes where the gate for this drive class can see it: a write-back drive behind a write-through loop
    # (no fsync reaches the drive, so its flush counter gate fires). A write-through drive cannot be planted: the
    # kernel refuses (6.8: EINVAL) or ignores (6.11) a write-back write to its queue, so that draw is NOT-RUN, not a
    # failure (T3 runner review item 10); its cross-check is fired on copies by v3l.py's drive-mismatch plant.
    local lo disk wc
    lo=$(basename "$(findmnt -n -o SOURCE "$mnt")")
    disk=$(python3 -B -c 'import json,sys; print(json.load(open(sys.argv[1]))["leaf"]["disk"])' "$o/v3-before/summary.json") || return 1
    wc=$(cat "/sys/block/$disk/queue/write_cache") || return 1
    case $wc:$lo in
      "write back:loop"*) knob=/sys/block/$lo/queue/write_cache ;;
      "write through:"*)
        echo "plant v3l-cache-lie: NOT-RUN: $disk is write through; the kernel will not let its queue claim write back" |
          tee "$o/plant.txt" "$OUT/plant-notrun.txt" ;;
      *) echo "plant v3l-cache-lie: no lie for $wc on $lo"; return 1 ;;
    esac
    if [ -n "$knob" ]; then
      orig=$(cat "$knob") || return 1
      [ "$orig" = "write back" ] || { echo "plant v3l-cache-lie: $knob reads '$orig' before the lie, not write back"; return 1; }
      # restored whatever ends the measurement, the script included
      trap 'printf "%s\n" "'"$orig"'" | sudo tee "'"$knob"'" > /dev/null' EXIT
      printf '%s\n' "write through" | sudo tee "$knob" > /dev/null || { echo "plant v3l-cache-lie: cannot write $knob"; return 1; }
      [ "$(cat "$knob")" = "write through" ] || { echo "plant v3l-cache-lie: $knob did not take"; return 1; }
      echo "plant v3l-cache-lie: $knob = write through (was $orig)" | tee "$o/plant.txt"
      env+=(V3L_PLANT=cache-lie)
    fi
  fi
  env "${env[@]}" timeout 1800 python3 -B "$L/t3/v3l.py" measure "$mnt/v3l-$when" "$o/v3l-$when" \
    "$o/v3-before/summary.json" > "$o/v3l-$when.txt" 2>&1
  rc=$?
  if [ -n "$knob" ]; then  # restore the saved original at once, read it back, and drop the trap
    printf '%s\n' "$orig" | sudo tee "$knob" > /dev/null
    trap - EXIT
    [ "$(cat "$knob")" = "$orig" ] || { echo "plant v3l-cache-lie: $knob did not return to '$orig'"; return 1; }
  fi
  echo "$when rc=$rc" >> "$o/v3l.rc"
  [ $rc = 0 ] || { echo "V3L $when on $FS_NOW: rc $rc ($(tail -1 "$o/v3l-$when.txt"))"; return 1; }
  # the counter and agreement gates forced to fire on copies of this real record (v3l.py plants)
  python3 -B "$L/t3/v3l.py" plants "$o/v3l-$when/v3l.json" "$o/v3l-$when-plants.json" > /dev/null ||
    { echo "V3L $when plants on $FS_NOW: not every plant fired"; return 1; }
  return 0
}

# One block: make the fs, record it, fire-check the V3 probe on the block's cell, V3 and V3L before, the
# cells, V3L and V3 after.
FS_NOW="" V3CELL="" PLANT_NOW="" MADE_FS="" MADE_UUID=""
# the drive preflight recorded, unmoved and unchanged, still allowed by devguard, still listed for its --plp and still
# registered for this block's filesystem (review 5 MED 3: those were read at preflight only); every mkfs and mount
dev_recheck() { # dev_recheck OUTFILE
  dev_unchanged "$DEVICE" "$DEV_REAL" /sys "$DEV_NAME" "$OUT/device-id.txt" >> "$1" 2>&1 || return 1
  if [ "$PLP" = yes ]; then
    plp_listed /sys "$DEV_NAME" "$L/t3/PLP-DRIVES" >> "$1" 2>&1 ||
      { echo "REFUSED: $DEV_NAME is no longer a listed PLP drive" >> "$1"; return 1; }
  fi
  registered_ok /sys "$L/v3/REGISTERED.tsv" "$DEV_NAME" "$PLP" "$FS_NOW" >> "$1" 2>&1 || return 1
}
RUN_CAP_S=1800  # the registered per-run cap (30 min); the warm-up is at most 10% of it
fs_block() {
  local fs=$FS_NOW mnt=/mnt/t3-$FS_NOW o=$OUT/fs-$FS_NOW
  mkdir -p "$o"
  case $BLOCK in loop) V3CELL=${fs}loop ;; *) V3CELL=$fs ;; esac
  printf 'cell=%s\nblock=%s\n' "$V3CELL" "$BLOCK" > "$o/block.txt"
  case $BLOCK in
    loop)
      LOOP_BACKING_DIR=$LOOPDIR bash "$L/fs/mkloop.sh" "$([ "$fs" = ext4 ] && echo ext4loop || echo "$fs")" "$mnt" 60G \
        > "$o/mkfs.txt" 2>&1 || return 1
      # a fresh ext4's lazyinit thread writes and commits in the background, inside the probe's windows
      [ "$fs" = ext4 ] && { sudo mount -o remount,noinit_itable "$mnt" || return 1; } ;;
    brd)
      bash "$L/v3/mkbrd.sh" "$fs" "$mnt" > "$o/mkfs.txt" 2>&1 || return 1 ;;
    device)
      # immediately before the mkfs: --device still names the node preflight checked, and devguard still allows it
      # (hours may have passed; devguard round-2 attack LOW 4). mkfs and mount act on that node, never on the name
      : > "$o/devguard.txt"
      dev_recheck "$o/devguard.txt" || { cat "$o/devguard.txt"; echo "fs-$fs: $DEV_REAL is not the drive preflight checked"; return 1; }
      sudo -n python3 -B "$L/t3/devguard.py" check "$DEV_REAL" >> "$o/devguard.txt" 2>&1 ||
        { cat "$o/devguard.txt"; echo "fs-$fs: devguard refuses $DEV_REAL before mkfs"; return 1; }
      case $fs in
        xfs) sudo mkfs.xfs -f -m reflink=1 "$DEV_REAL" ;;
        btrfs) sudo mkfs.btrfs -f "$DEV_REAL" ;;
        ext4) sudo mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 "$DEV_REAL" ;;
        *) echo "unknown fs $fs"; false ;;
      esac > "$o/mkfs.txt" 2>&1 || return 1
      # from here block_cleanup must wipe what this block made, so the next block's devguard check passes; the UUID
      # this mkfs gave it is what lets the wipe tell it from another drive's filesystem of the same type (MED 3)
      MADE_FS=$fs
      MADE_UUID=$(t3_blkid UUID "$DEV_REAL" 2>> "$o/mkfs.txt")
      echo "fs_uuid=$MADE_UUID" >> "$o/mkfs.txt"
      [ -n "$MADE_UUID" ] || { echo "fs-$fs: blkid reads no UUID on the $fs just made on $DEV_REAL"; return 1; }
      dev_recheck "$o/devguard.txt" || { cat "$o/devguard.txt"; echo "fs-$fs: the drive changed before mount"; return 1; }
      # btrfs defaults to discard=async on SSDs, which discards freed extents 10-120 s later, inside a later run
      sudo mkdir -p "$mnt" && sudo mount $([ "$fs" = btrfs ] && echo "-o nodiscard") "$DEV_REAL" "$mnt" &&
        sudo chown "$(id -u):$(id -g)" "$mnt" || return 1 ;;
  esac
  findmnt -n -o SOURCE,FSTYPE,OPTIONS -T "$mnt" | tee -a "$o/mkfs.txt"
  # no online discard on the filesystem under test (fourth lane review MED 5): settle.sh fstrims between runs instead
  case ",$(findmnt -n -o OPTIONS -T "$mnt")," in
    *,discard,*|*,discard=*) echo "REFUSED: $mnt mounts with online discard; mount it nodiscard"; return 1 ;;
  esac
  SETTLE_DEV=$(settle_dev "$mnt") || { echo "fs-$fs: settle cannot read its device's counters"; return 1; }
  bash "$L/t3/hwid.sh" "$o/hwid" "$mnt" > "$o/hwid.stdout" 2>&1 || return 1
  if [ $DRY = 0 ] && grep -q '"barrier": false' "$o/hwid/hwid.json"; then
    echo "REFUSED: $mnt has a nobarrier layer on its flush path"; return 1
  fi
  bash "$L/hw/record.sh" "$o/hw" "$mnt/hw" 15 > "$o/hw.stdout" 2>&1 || return 1
  # a real run's P_nest3 must start on THIS block's filesystem (the cell's leaf is --device; on an md or LVM root a
  # chain on / is refused by the probe: V3 ninth review HIGH): made here, per block, by mkfixtures.sh's nest mode
  # (V3 lane f99ff6546; any older chain under V3FX is torn down first), and torn down in block_cleanup
  if [ "$BLOCK" = device ]; then
    V3_FIXTURES=nest V3_NEST_DIR="$mnt" timeout 600 bash "$L/v3/mkfixtures.sh" "$V3FX" > "$o/v3nest.txt" 2>&1 ||
      { echo "fs-$fs: the per-block nest fixture failed: $(tail -2 "$o/v3nest.txt")"; return 1; }
  fi
  env "${V3ENV[@]}" timeout 3900 bash "$L/v3/firecheck.sh" "$DIST/v3floor" "$V3CELL" \
    "$mnt/v3fc" "$o/v3-firecheck" > "$o/v3-firecheck.txt" 2>&1 || { echo "V3 fire-check failed on $V3CELL"; return 1; }
  mkdir -p "$mnt/v3b" "$mnt/v3a"
  v3batch before "$mnt/v3b" || return 1
  v3l b0 "$mnt" || return 1
  # flush counter fire-check for the competitor cells on this filesystem (run_system.sh requires its verdict)
  sudo sysctl -w kernel.yama.ptrace_scope=0 > /dev/null
  timeout 900 bash "$L/competitors/firecheck_strace.sh" "$o/strace-firecheck" "$mnt/strace-fc.noindex" \
    > "$o/strace-firecheck.txt" 2>&1 || { echo "strace fire-check failed on $fs"; return 1; }
  python3 -B "$L/t3/cells.py" plan "$MAN" "$fs" "$SEED" > "$o/plan.tsv" || return 1
  local cell system clients ops runs class blk age live cur=1
  # fd 3, so nothing a cell runs can read the plan from stdin
  while IFS=$'\t' read -r cell system clients ops runs class blk age live <&3; do
    if [ "$blk" != "$cur" ]; then v3l "b$cur" "$mnt" || return 1; cur=$blk; fi
    run_cell "$fs" "$mnt" "$o" "$cell" "$system" "$clients" "$ops" "$class" "$age" "$live"
  done 3< "$o/plan.tsv"
  v3l "b$cur" "$mnt" || return 1
  v3batch after "$mnt/v3a" || return 1
  # The batches' start-to-end drift (batchgate.py drift; third lane review MED 2): PUBLISHED, not a gate on T3
  # (PREREG :180 and departure 3 :553 make T3's drift descriptive; A20), so a VOID (rc 3) is recorded and the block
  # goes on. Every other rc fails the block: 2 (the two batches are not the same kind of batch), and a traceback, a
  # kill or a missing interpreter alike (t3lib.sh drift_ok, an allowlist; fourth lane review LOW 8).
  python3 -B "$L/v3/batchgate.py" drift "$o/v3-before" "$o/v3-after" > "$o/v3-drift.json" 2> "$o/v3-drift.err"
  local drc=$?
  echo "$drc" > "$o/v3-drift.rc"
  echo "V3 drift on $V3CELL: rc $drc $(cat "$o/v3-drift.json")"
  drift_ok "$drc" || { echo "V3 drift on $V3CELL: rc $drc (only 0 or a published void, 3, lets the block go on)"; return 1; }
  return 0
}

# After every block, passed or failed (review H1): unmount and detach, so the next block can make its filesystem.
block_unmount() {
  local mnt=/mnt/t3-$FS_NOW dev back t0 k
  findmnt -n "$mnt" > /dev/null 2>&1 || return 0
  dev=$(findmnt -n -o SOURCE "$mnt")
  # a real run's nest chain lives on this filesystem (its first image under $mnt): unmount and detach it first, or
  # the umount below is busy (mkfixtures.sh --teardown-nest, V3 lane f99ff6546)
  if [ "$BLOCK" = device ]; then
    timeout 300 bash "$L/v3/mkfixtures.sh" --teardown-nest "$V3FX" > "$OUT/fs-$FS_NOW/v3nest-teardown.txt" 2>&1 ||
      { echo "cleanup: the nest chain on $mnt did not tear down: $(tail -2 "$OUT/fs-$FS_NOW/v3nest-teardown.txt")"; return 1; }
  fi
  # a process still holding the test filesystem (a competitor server under setsid, a stray cell) is ours: kill it, wait
  # until no holder is left (at most 60 s; a holder still exiting made a single umount fail with EBUSY: T3 runner
  # review item 8), then a plain umount, retried; a lazy umount would report success over a live filesystem
  sudo fuser -k -m "$mnt" > /dev/null 2>&1
  t0=$(date +%s)
  while sudo fuser -m "$mnt" > /dev/null 2>&1; do
    [ $(( $(date +%s) - t0 )) -ge 60 ] && break
    sleep 1
  done
  for k in 1 2 3; do sudo umount "$mnt" 2>/dev/null && break; sleep 2; done
  echo "cleanup fs-$FS_NOW: holders gone after $(( $(date +%s) - t0 )) s, umount attempts $k" >> "$OUT/cleanup.txt"
  findmnt -n "$mnt" > /dev/null 2>&1 &&
    { echo "cleanup: cannot unmount $mnt: $(sudo fuser -v -m "$mnt" 2>&1 | tail -3)"; return 1; }
  case $BLOCK:$dev in loop:/dev/loop*)
    back=$(losetup -n -O BACK-FILE "$dev" | xargs)
    sudo losetup -d "$dev" && sudo rm -f "$back" ;;
  esac
  return 0
}
# Unmount (even when the block failed after its mkfs and before or after its mount), then on a device block wipe the
# filesystem the block made, so the next block's devguard check before its mkfs sees a blank disk (devguard round-2
# attack MED 2). It wipes only DEV_REAL, only while --device still resolves there to the drive preflight recorded
# (t3lib.sh dev_unchanged: wwid, MAJ:MIN, model, serial, firmware), and only when blkid finds exactly the filesystem
# this block made, type and UUID (fs_is_ours; review 5 MED 3); anything else stops the run with the device untouched.
block_cleanup() {
  local w=$OUT/fs-$FS_NOW/wipe.txt
  block_unmount || return 1
  [ "$BLOCK" = device ] && [ -n "$MADE_FS" ] || return 0
  dev_unchanged "$DEVICE" "$DEV_REAL" /sys "$DEV_NAME" "$OUT/device-id.txt" > "$w" 2>&1 &&
    fs_is_ours "$DEVICE" "$DEV_REAL" "$MADE_FS" "$MADE_UUID" >> "$w" 2>&1 && sudo -n wipefs -a "$DEV_REAL" >> "$w" 2>&1 ||
    { echo "cleanup: $DEV_REAL was not wiped: $(tail -2 "$w")"; return 1; }
  MADE_FS="" MADE_UUID=""
  return 0
}

# The quiet gap before every run: settle.sh (sourced after L is set), called at the top of each attempt.
# One cell run (the plan already holds one row per run): the sampler runs around the adapter, and the
# void decision is made from its record BEFORE the cell's results are read. A VOID run is replaced once,
# at most twice per cell (amendment 8); void runs are kept.
run_cell() {
  local fs=$1 mnt=$2 o=$3 cell=$4 system=$5 clients=$6 ops=$7 class=$8 age=$9 live=${10} attempt d id rc v
  for attempt in 1 2 3; do
    id="$fs-$cell-a$attempt"
    d="$o/cells/$cell/a$attempt"
    mkdir -p "$d"
    # the gap BEFORE this run, so settle.txt belongs to the run it precedes: the block's first run (after V3L and
    # the strace fire-check) and every replacement attempt included (fourth lane review MED 5)
    settle "$mnt" "$SETTLE_DEV" "$d"
    python3 -B "$L/t3/foreign_cpu.py" sample "$d/foreign.tsv" --cell "$id" --stop-file "$d/.stop" &
    local sampler=$!
    sleep 2
    export FASTEST_CELL=$id
    case $system in
      ours)
        # ops is the run's TOTAL and the warm-up is PREREG's rule for every system (gate-6 review 12 and 3)
        timeout 7200 "$DIST/fastest_profile" --dir "$mnt/work-$id" --class "$class" --clients "$clients" \
          --ops-total "$ops" --warmup "$WARMUP" --mode phases --out "$d/result" > "$d/adapter.txt" 2>&1 ;;
      pg18-d2|dolt|doltgres|b1)
        # Each attempt gets its own directory on the filesystem under test (run_system.sh keeps its
        # servers' data under MNT/<system>.noindex, which a replacement run must not find in place).
        mkdir -p "$mnt/work-$id"
        # the plan's run total, fixture (age, live branches) and mode: comp's run_system.sh refuses a real run
        # (FT_DRY=0, its default) given any of them by default (comp d8fef669b, 76242a76a)
        FT_CLIENTS=$clients FT_OPS_TOTAL=$ops FT_AGE=$age FT_PREBRANCH=$live FT_DRY=$DRY FT_CAP_S=$RUN_CAP_S FT_WARMUP=$WARMUP \
          FT_BBLOAD=$DIST/bbload FT_CLONEBENCH=$DIST/clonebench \
          FT_SQLITE3=$DIST/sqlite3 FT_FIRECHECK=$o/strace-firecheck/firecheck.txt FT_BIN=$DIST/dolt-bin \
          timeout 10800 bash "$L/competitors/run_system.sh" "$system" "$mnt/work-$id" "$d/result" > "$d/adapter.txt" 2>&1 ;;
      *) echo "unknown system $system" > "$d/adapter.txt"; false ;;
    esac
    rc=$?
    unset FASTEST_CELL
    touch "$d/.stop"
    wait "$sampler"
    python3 -B "$L/t3/foreign_cpu.py" decide "$d/foreign.tsv" --json "$d/void.json" > /dev/null
    v=$?
    # A refused class (rc 4, NOT AVAILABLE) measured nothing: no void decision applies and nothing is re-run.
    if [ $rc = 4 ] && grep -q '^NOT AVAILABLE' "$d/adapter.txt" 2>/dev/null; then
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$fs" "$cell" "$system" "$clients" "$attempt" "$rc" "N/A" >> "$OUT/cells.tsv"
      break
    fi
    rm -rf "$mnt/work-$id"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$fs" "$cell" "$system" "$clients" "$attempt" "$rc" \
      "$([ $v = 0 ] && echo VALID || echo VOID)" >> "$OUT/cells.tsv"
    [ $v = 3 ] || break
  done
}

# The checks that need nothing built, BEFORE deps and build are paid for (T3 runner review LOW 17): the harness's own
# self-tests and the devguard root fire on this box's real disks
selftests() {
  local t
  for t in foreign_cpu v3l summarize blockgate devguard; do
    python3 -B "$L/t3/$t.py" self-test > "$OUT/$t-selftest.txt" 2>&1 || { echo "self-test $t FAILED"; return 1; }
  done
  timeout 120 bash "$L/t3/settle_test.sh" > "$OUT/settle-selftest.txt" 2>&1 || { echo "self-test settle FAILED"; return 1; }
  timeout 60 bash "$L/t3/t3lib_test.sh" > "$OUT/t3lib-selftest.txt" 2>&1 || { echo "self-test t3lib FAILED"; return 1; }
  timeout 120 python3 -B "$L/gates/warmup_conformance.py" self-test > "$OUT/warmup-conformance-selftest.txt" 2>&1 ||
    { echo "self-test warmup_conformance FAILED"; return 1; }
  # devguard on this box's real lsblk: the root disk must be refused (a fire on real input, not a fixture)
  local rd want d rc; rd=$(python3 -B "$L/t3/devguard.py" rootdisk 2> "$OUT/devguard-rootdisk.txt") ||
    { cat "$OUT/devguard-rootdisk.txt"; echo "selftests: devguard rootdisk cannot tell the root disk, so its root refusal cannot be fired"; return 1; }
  [ -n "$rd" ] || { echo "selftests: devguard rootdisk printed no disk"; return 1; }
  # the disks to fire on must also come from a second instrument (the kernel's sysfs links, not the lsblk walk under
  # test), so a walk naming the wrong disk, or one member of an md, fails here (fresh review of be76cf6ee)
  want=$(python3 -B "$L/t3/devguard.py" rootdisk-sysfs 2> "$OUT/devguard-rootdisk-sysfs.txt") ||
    { cat "$OUT/devguard-rootdisk-sysfs.txt"; echo "selftests: sysfs cannot name the root disk, so rootdisk cannot be checked"; return 1; }
  [ "$rd" = "$want" ] || { echo "selftests: devguard rootdisk names '${rd//$'\n'/ }' but sysfs names '${want//$'\n'/ }'"; return 1; }
  # every disk under / (an md root has several): exit 2 AND, for that disk, the root rule's own text, the O_EXCL rule's
  # (the kernel claims a disk any of whose partitions is mounted or held) and the signature rule's (a root disk carries
  # a partition table, a filesystem or a member signature). Those two rules are the ones that cover what the
  # holder graph cannot see, so they too are fired on real input here, not only on fixtures (devguard round-2 attack
  # MED 1 and MED 2). Any other exit (a sudo or Python crash), or another rule refusing alone, is not these rules
  # firing (T3 runner review MED 13)
  local want_text t
  for d in $rd; do
    sudo -n python3 -B "$L/t3/devguard.py" check "/dev/$d" > "$OUT/devguard-root-$d.txt" 2>&1
    rc=$?
    for t in "holds the root filesystem" "cannot be opened exclusively" "carries a signature"; do
      want_text="REFUSED: $d $t"
      if [ $rc != 2 ] || ! grep -qF "$want_text" "$OUT/devguard-root-$d.txt"; then
        cat "$OUT/devguard-root-$d.txt"
        echo "selftests: devguard's root fire on /dev/$d needs exit 2 and '$want_text'; got exit $rc"; return 1
      fi
    done
  done
}
stage preflight preflight
stage selftests selftests
stage deps deps
export PATH="$HOME/.cargo/bin:$PATH"
stage build build
stage hwid hwid
stage v3fixtures v3fixtures
# The checks that need the build (the V3 binaries): firecheck.sh's environment, positive and negative
# the warm-up rule is one rule across the three drivers this run built (PREREG annex A23; fourth lane review LOW 14):
# each replays its own live decision on the shared cases, and anything but rc 0 (all three, every case) fails the stage
warmup_conformance() {
  local rc=0
  timeout 300 python3 -B "$L/gates/warmup_conformance.py" run --bbload "$DIST/bbload" --clonebench "$DIST/clonebench" \
    --fastest-profile "$DIST/fastest_profile" > "$OUT/warmup-conformance.txt" 2>&1 || rc=$?
  [ $rc = 0 ] || { tail -5 "$OUT/warmup-conformance.txt"; echo "envchecks: warm-up conformance rc $rc, want 0"; return 1; }
}
envchecks() { fcenv_check && warmup_conformance; }
stage envchecks envchecks
if [ $DRY = 1 ]; then
  echo "dry run: the runner image mounts / nobarrier; remount with barrier as a T3 box has it"
  sudo mount -o remount,barrier / && findmnt -n -o OPTIONS / | tee "$OUT/root-mount.txt"
fi
printf 'fs\tcell\tsystem\tclients\tattempt\tadapter_rc\tvoid\n' > "$OUT/cells.tsv"
if [ -z "$FSLIST" ]; then
  FSLIST=$(python3 -B "$L/t3/cells.py" fslist "$MAN") || { echo "t3run: cells.py fslist failed"; finish 1; }
fi
fslist_ok "$FSLIST" || { echo "t3run: no filesystem block to run"; finish 1; }
echo $FSLIST > "$OUT/fslist.txt"
# Blocks (review H1): a failed block is recorded and torn down, and the next block runs; the run's rc is 1 at the end.
BLOCKS_FAILED=0 BLOCKNO=0
for FS_NOW in $FSLIST; do
  BLOCKNO=$((BLOCKNO + 1))
  PLANT_NOW=""; [ $BLOCKNO = 1 ] && PLANT_NOW=$PLANT
  s=$(date +%s)
  echo "== stage fs-$FS_NOW $(date -u +%FT%TZ)${PLANT_NOW:+ (plant $PLANT_NOW)}"
  fs_block
  rc=$?
  cleaned=1
  block_cleanup || { rc=1; cleaned=0; }
  e=$(date +%s)
  printf '%s\t%s\t%s\t%s\t%s\n' "fs-$FS_NOW" "$(date -u -d @"$s" +%FT%TZ)" "$(date -u -d @"$e" +%FT%TZ)" $((e - s)) $rc >> "$STAGES"
  [ $rc = 0 ] || { echo "t3run: block fs-$FS_NOW FAILED rc=$rc; the next block runs"; BLOCKS_FAILED=$((BLOCKS_FAILED + 1)); }
  # every device block runs mkfs on the same --device: a block that could not be torn down would fail every later one
  # at mkfs, so the run stops here with that reason (T3 runner review item 8)
  if [ $cleaned = 0 ] && [ "$BLOCK" = device ]; then
    echo "t3run: STOPPED: fs-$FS_NOW could not be torn down, so $DEVICE cannot be re-made for the next block"; break
  fi
done
[ $BLOCKS_FAILED = 0 ] && finish 0
finish 1
