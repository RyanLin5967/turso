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
#   deps       apt (build tools, fio, strace, nvme-cli, smartmontools, fs tools, PGDG postgresql-18 without a
#              cluster, MariaDB client), rustup with the repo's pinned toolchain
#   build      the engine driver fastest_profile (release, debug symbols), bbload/clonebench/sqlite3
#              (competitors/build.sh), Dolt + Doltgres release binaries (competitors/fetch_dolt.sh), v3floor and
#              the fire-check's statfs shim
#   hwid       hwid.sh (machine, NVMe id-ctrl VWC, feature 0x06, smartctl text naming power loss, every mount)
#   per filesystem in --fs (one block): make it, hw record with fio, hwid of that target, the V3 probe's
#              fire-check on the block's explicit V3 cell, a V3 batch BEFORE, V3L BEFORE, every manifest cell for
#              that fs in a seeded shuffle (each with the foreign-CPU sampler and its void decision made before
#              any result of the cell is read; a void run is replaced once, at most twice per cell), V3L AFTER,
#              a V3 batch AFTER, then the batches' drift (batchgate.py drift: published, not a gate on T3; a REFUSED
#              drift fails). Each batch is judged by blockgate.py (review 2 item 5, rulings A14 and A16: every VOID
#              fails), and the A16 plants run on copies of the BEFORE batch's real record; a batch blockgate fails, a
#              plant that does not fire, or a V3L that is not VALID (v3l.py) fails the block's stage (items 5 and 6).
#              After each run's teardown, settle() waits for its filesystem's device to go quiet. A failed block
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
#   v3l-cache-lie       a write-cache lie under V3L BEFORE, on real hardware, whichever drive the runner draws: on a
#                       write-back drive the block's loop is set to write through (fsyncs stop reaching the drive, and
#                       the drive's flush counter gate must VOID it); on a write-through drive the drive's own queue is
#                       set to write back (the kernel/drive cross-check must VOID it). Restored right after.
# A plant applies to the FIRST block only, so the second block shows that a failed block does not stop the run.
# Without --dry-run (a real T3 rental) it REFUSES unless: the manifest's sha256 is listed in
# fastest/linux/t3/REGISTERED-MANIFESTS (append-only; empty until the T3 registration), --device and --destroy
# name the same device and devguard.py allows it (an allowlist: a whole NVMe/SCSI/virtio disk, not the root disk,
# nothing mounted or held; review M4), --plp is given, --fs is not (the manifest names the blocks; review M5), and
# every target mounts with barriers.
# Every file the run calls must be in the commit (preflight lists them; review 2 item 18). One warm-up rule for every
# system (competitors/timedrun.py rule at the 1800 s cap, OPS:S:MAX_S) goes to fastest_profile and run_system.sh alike.
# Exit: 0 every stage ran and every cell has a verdict; 1 a stage failed; 2 refused before anything ran.
set -uo pipefail
REPO_URL=https://github.com/RyanLin5967/turso
SHA="" OUT="" DRY=0 MANIFEST="" FSLIST="" DEVICE="" DESTROY="" SEED=20261005 BLOCK="" PLANT="" PLP=""
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
    # a virtualized box cannot be T3 hardware: what a flush reaches behind a hypervisor is unknown (gate-6 review 8)
    local virt; virt=$(systemd-detect-virt 2>/dev/null || true)
    [ "$virt" = none ] || { echo "REFUSED: systemd-detect-virt says '${virt:-unknown}': a T3 box must be bare metal"; return 2; }
    # --plp yes takes the drive out of the timing control (A14/A16), so it must name a registered drive: model and
    # firmware listed in fastest/linux/t3/PLP-DRIVES (append-only; lane review MED 1)
    if [ "$PLP" = yes ]; then
      local dn model fw; dn=$(basename "$(readlink -f "$DEVICE")")
      # NVMe exposes device/firmware_rev, SCSI/SATA sd device/rev (third lane review LOW 9)
      model=$(xargs < "/sys/block/$dn/device/model" 2>/dev/null)
      fw=$(xargs < "/sys/block/$dn/device/firmware_rev" 2>/dev/null || xargs < "/sys/block/$dn/device/rev" 2>/dev/null)
      grep -qxF "$(printf '%s\t%s' "$model" "$fw")" "$L/t3/PLP-DRIVES" ||
        { echo "REFUSED: --plp yes but '$model' firmware '$fw' is not in fastest/linux/t3/PLP-DRIVES"; return 2; }
    fi
    # the registered values a bound batch in rental mode refuses without (A17; third lane review LOW 10), checked
    # before deps, build and fire-checks are paid for: the frame arm, and on a write-back drive without PLP the D0
    # threshold of every filesystem block (key d0_threshold/<fstype>/wb/bare; write-through and PLP need none, A14)
    local reg=$L/v3/REGISTERED.tsv wc f fsl; wc=$(cat "/sys/block/$(basename "$(readlink -f "$DEVICE")")/queue/write_cache" 2>/dev/null)
    awk -F '\t' '$1 == "frame_arm" && $2 != "" { ok = 1 } END { exit !ok }' "$reg" ||
      { echo "REFUSED: $reg registers no frame_arm (PREREG section 4: fixed in the Registration annex)"; return 2; }
    case $wc in "write back"|"write through") ;; *) echo "REFUSED: cannot read $DEVICE's queue/write_cache ('$wc')"; return 2 ;; esac
    if [ "$wc" = "write back" ] && [ "$PLP" != yes ]; then
      fsl=${FSLIST:-$(python3 -B "$L/t3/cells.py" fslist "$MAN")}
      for f in $fsl; do
        awk -F '\t' -v k="d0_threshold/$f/wb/bare" '$1 == k && $2 != "" { ok = 1 } END { exit !ok }' "$reg" ||
          { echo "REFUSED: $reg registers no d0_threshold/$f/wb/bare (A17: rental mode refuses a provisional one)"; return 2; }
      done
    fi
    # P_nest3 must start on --device's own filesystem, which exists only per block (V3 ninth review HIGH)
    echo "REFUSED: a real run's V3 nest fixture cannot be placed on $DEVICE yet (needs mkfixtures.sh's per-block nest"
    echo "  and teardown modes from the V3 lane); on an md or LVM root the fire-check's P_nest3 would fail every block"
    return 2
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
  local o=$OUT/fcenv-check rc
  mkdir -p "$o"
  env "${V3ENV[@]}" bash "$L/v3/firecheck.sh" "$DIST/v3floor" ext4loop "$o/w" "$o" > "$o.txt" 2>&1
  rc=$?
  [ $rc = 2 ] && grep -qx "firecheck: $o exists" "$o.txt" ||
    { echo "fire-check environment refused (rc $rc): $(cat "$o.txt")"; return 1; }
  rmdir "$o"
}
# The nest fixture (P_nest3) must start on the filesystem that holds the cell's leaf (V3 eighth review M5, ninth
# review HIGH), or an md/LVM root makes the probe refuse it. A loop block's leaf is the drive under its backing file,
# so the backing directory is chosen ONCE here (mkloop.sh's own rule: / or /mnt, whichever has more free space) and
# passed to both: mkfixtures.sh as V3_NEST_DIR and every mkloop.sh as LOOP_BACKING_DIR. A brd block keeps the default
# (the root fs; brd is fire-check only). A real run's leaf is --device, whose filesystem exists only inside its
# block, so its nest chain has to be made per block and torn down before the block's umount; mkfixtures.sh has no
# nest-only or teardown mode yet (asked of the V3 lane), so preflight REFUSES a real run until it does.
LOOPDIR=""
v3fixtures() {
  local nest=""
  if [ "$BLOCK" = loop ]; then
    LOOPDIR=$(df --output=avail,target -B1 / /mnt 2>/dev/null | tail -n +2 | sort -n | tail -1 | awk '{print $2}')
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
  local -a env=(V3_CELL="$V3CELL" "${V3ENV[0]}")  # V3ENV[0] is V3_PLP
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
  env "${env[@]}" timeout 3600 bash "$L/v3/run.sh" "$DIST/v3floor" "$dir" "$o/v3-$when" 10000 --arms "$arms" \
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
  local when=$1 mnt=$2 o=$OUT/fs-$FS_NOW rc lie=""
  local -a env=()
  [ $DRY = 0 ] && env+=(V3L_REAL=1)
  [ "$PLANT_NOW:$when" = v3l-fsync-half:b0 ] && env+=(V3L_PLANT=fsync2)
  if [ "$PLANT_NOW:$when" = v3l-cache-lie:b0 ]; then
    # the lie goes where the gate for this drive class can see it: a write-back drive behind a write-through loop
    # (no fsync reaches the drive), or a write-through drive whose kernel queue claims write back
    local lo disk wc
    lo=$(basename "$(findmnt -n -o SOURCE "$mnt")")
    disk=$(python3 -B -c 'import json,sys; print(json.load(open(sys.argv[1]))["leaf"]["disk"])' "$o/v3-before/summary.json") || return 1
    wc=$(cat "/sys/block/$disk/queue/write_cache") || return 1
    case $wc:$lo in
      "write back:loop"*) lie="/sys/block/$lo/queue/write_cache=write through" ;;
      "write through:"*) lie="/sys/block/$disk/queue/write_cache=write back" ;;
      *) echo "plant v3l-cache-lie: no lie for $wc on $lo"; return 1 ;;
    esac
    printf '%s\n' "${lie#*=}" | sudo tee "${lie%%=*}" > /dev/null || { echo "plant v3l-cache-lie: cannot write ${lie%%=*}"; return 1; }
    [ "$(cat "${lie%%=*}")" = "${lie#*=}" ] || { echo "plant v3l-cache-lie: ${lie%%=*} did not take"; return 1; }
    echo "plant v3l-cache-lie: ${lie%%=*} = ${lie#*=} (was $( [ "${lie#*=}" = "write back" ] && echo "write through" || echo "write back"))" | tee "$o/plant.txt"
    env+=(V3L_PLANT=cache-lie)
  fi
  env "${env[@]}" timeout 1800 python3 -B "$L/t3/v3l.py" measure "$mnt/v3l-$when" "$o/v3l-$when" \
    "$o/v3-before/summary.json" > "$o/v3l-$when.txt" 2>&1
  rc=$?
  if [ -n "$lie" ]; then  # restore at once, so the rest of the run sees the drive as it is
    printf '%s\n' "$( [ "${lie#*=}" = "write back" ] && echo "write through" || echo "write back")" | sudo tee "${lie%%=*}" > /dev/null
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
FS_NOW="" V3CELL="" PLANT_NOW=""
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
      case $fs in
        xfs) sudo mkfs.xfs -f -m reflink=1 "$DEVICE" ;;
        btrfs) sudo mkfs.btrfs -f "$DEVICE" ;;
        ext4) sudo mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 "$DEVICE" ;;
        *) echo "unknown fs $fs"; false ;;
      esac > "$o/mkfs.txt" 2>&1 || return 1
      sudo mkdir -p "$mnt" && sudo mount "$DEVICE" "$mnt" && sudo chown "$(id -u):$(id -g)" "$mnt" || return 1 ;;
  esac
  findmnt -n -o SOURCE,FSTYPE,OPTIONS -T "$mnt" | tee -a "$o/mkfs.txt"
  bash "$L/t3/hwid.sh" "$o/hwid" "$mnt" > "$o/hwid.stdout" 2>&1 || return 1
  if [ $DRY = 0 ] && grep -q '"barrier": false' "$o/hwid/hwid.json"; then
    echo "REFUSED: $mnt has a nobarrier layer on its flush path"; return 1
  fi
  bash "$L/hw/record.sh" "$o/hw" "$mnt/hw" 15 > "$o/hw.stdout" 2>&1 || return 1
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
  local cell system clients ops runs class blk cur=1
  # fd 3, so nothing a cell runs can read the plan from stdin
  while IFS=$'\t' read -r cell system clients ops runs class blk <&3; do
    if [ "$blk" != "$cur" ]; then v3l "b$cur" "$mnt" || return 1; cur=$blk; fi
    run_cell "$fs" "$mnt" "$o" "$cell" "$system" "$clients" "$ops" "$class"
  done 3< "$o/plan.tsv"
  v3l "b$cur" "$mnt" || return 1
  v3batch after "$mnt/v3a" || return 1
  # The batches' start-to-end drift (batchgate.py drift; third lane review MED 2): PUBLISHED, not a gate on T3
  # (PREREG :180 and departure 3 :553 make T3's drift descriptive; V3 review 2 item 6 keeps the T1 60 us void off
  # T3), so a VOID (rc 3) is recorded and the block goes on. A REFUSED drift (rc 2: the two batches are not the same
  # kind of batch, or one failed its own gate) fails the block.
  python3 -B "$L/v3/batchgate.py" drift "$o/v3-before" "$o/v3-after" > "$o/v3-drift.json" 2> "$o/v3-drift.err"
  local drc=$?
  echo "$drc" > "$o/v3-drift.rc"
  echo "V3 drift on $V3CELL: rc $drc $(cat "$o/v3-drift.json")"
  [ $drc = 2 ] && { echo "V3 drift REFUSED on $V3CELL: the before and after batches cannot be compared"; return 1; }
  return 0
}

# After every block, passed or failed (review H1): unmount and detach, so the next block can make its filesystem.
block_cleanup() {
  local mnt=/mnt/t3-$FS_NOW dev back
  findmnt -n "$mnt" > /dev/null 2>&1 || return 0
  dev=$(findmnt -n -o SOURCE "$mnt")
  # a process still holding the test filesystem (a competitor server under setsid, a stray cell) is ours: kill it,
  # then a plain umount; a lazy umount would report success over a live filesystem (lane review LOW 9)
  sudo fuser -k -m "$mnt" > /dev/null 2>&1
  sleep 1
  sudo umount "$mnt" || { echo "cleanup: cannot unmount $mnt: $(sudo fuser -v -m "$mnt" 2>&1 | tail -3)"; return 1; }
  case $BLOCK:$dev in loop:/dev/loop*)
    back=$(losetup -n -O BACK-FILE "$dev" | xargs)
    sudo losetup -d "$dev" && sudo rm -f "$back" ;;
  esac
  return 0
}

# After a run's teardown, before the next run starts (gate-6 review 11; third lane review MED 5): sync the test
# filesystem, then wait until the filesystem's own block device (the device itself, the loop of a loop block, the
# ram disk of a brd block) has no request in flight, the page cache holds under 1 MiB dirty, AND that device's
# completed write, discard and flush counters (stat fields 5, 12 and 16) have not moved, for 2 s running (11 polls
# 0.2 s apart, every one equal to the first), at most 60 s. (Its own device, not the leaf: a loop block's leaf is
# the runner's root disk, whose own traffic is not this run's.) A burst between polls
# (btrfs async discard after the rm, a late writeback) moves a counter and restarts the window, where an instantaneous
# in-flight sample could miss it. The settle time, whether it went quiet, and the counters are recorded per run
# (settle.txt); summarize lists every run that did not go quiet.
settle() { # settle MNT RUNDIR
  local mnt=$1 d=$2 t0 q=0 n=0 inflight dirty dev st c c0=""
  dev=$(findmnt -n -o SOURCE -T "$mnt" | sed 's/\[.*//')
  dev=$(basename "$(readlink -f "$dev")")
  sync -f "$mnt" 2>/dev/null || sync
  t0=$(date +%s%N)
  while [ $n -lt 300 ]; do
    st=$(cat "/sys/class/block/$dev/stat" 2>/dev/null)
    inflight=$(echo "$st" | awk '{print $9}')
    c=$(echo "$st" | awk 'NF >= 16 {print $5 "/" $12 "/" $16}')
    dirty=$(awk '/^Dirty:/ {print $2}' /proc/meminfo)
    if [ "${inflight:-1}" = 0 ] && [ "${dirty:-999999}" -lt 1024 ] && [ -n "$c" ] && { [ $q = 0 ] || [ "$c" = "$c0" ]; }; then
      [ $q = 0 ] && c0=$c
      q=$((q + 1))
    else
      q=0
    fi
    [ $q -ge 11 ] && break
    sleep 0.2
    n=$((n + 1))
  done
  printf 'dev=%s settle_s=%s quiet=%s inflight=%s dirty_kb=%s writes/discards/flushes=%s\n' "$dev" \
    "$(awk -v a="$t0" -v b="$(date +%s%N)" 'BEGIN { printf "%.2f", (b - a) / 1e9 }')" \
    "$([ $q -ge 11 ] && echo yes || echo no)" "$inflight" "$dirty" "${c:-unreadable}" > "$d/settle.txt"
}

# One cell run (the plan already holds one row per run): the sampler runs around the adapter, and the
# void decision is made from its record BEFORE the cell's results are read. A VOID run is replaced once,
# at most twice per cell (amendment 8); void runs are kept.
run_cell() {
  local fs=$1 mnt=$2 o=$3 cell=$4 system=$5 clients=$6 ops=$7 class=$8 attempt d id rc v
  for attempt in 1 2 3; do
    id="$fs-$cell-a$attempt"
    d="$o/cells/$cell/a$attempt"
    mkdir -p "$d"
    python3 -B "$L/t3/foreign_cpu.py" sample "$d/foreign.tsv" --cell "$id" --stop-file "$d/.stop" &
    local sampler=$!
    sleep 2
    export FASTEST_CELL=$id
    case $system in
      ours)
        # ops is the run's TOTAL and the warm-up is PREREG's rule for every system (gate-6 review 12 and 3)
        timeout 7200 "$DIST/fastest_profile" --dir "$mnt/work-$id" --class "$class" --clients "$clients" \
          --ops-total "$ops" --warmup "$WARMUP" --mode phases --out "$d/result" > "$d/adapter.txt" 2>&1 ;;
      pg18-d2|pg18-defaults|dolt|doltgres|b1)
        # Each attempt gets its own directory on the filesystem under test (run_system.sh keeps its
        # servers' data under MNT/<system>.noindex, which a replacement run must not find in place).
        mkdir -p "$mnt/work-$id"
        FT_CLIENTS=$clients FT_N1=$ops FT_N4=$ops FT_CAP_S=$RUN_CAP_S FT_WARMUP=$WARMUP \
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
    settle "$mnt" "$d"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$fs" "$cell" "$system" "$clients" "$attempt" "$rc" \
      "$([ $v = 0 ] && echo VALID || echo VOID)" >> "$OUT/cells.tsv"
    [ $v = 3 ] || break
  done
}

stage preflight preflight
stage deps deps
export PATH="$HOME/.cargo/bin:$PATH"
stage build build
stage hwid hwid
stage v3fixtures v3fixtures
selftests() {
  local t
  for t in foreign_cpu v3l summarize blockgate devguard; do
    python3 -B "$L/t3/$t.py" self-test > "$OUT/$t-selftest.txt" 2>&1 || { echo "self-test $t FAILED"; return 1; }
  done
  # devguard on this box's real lsblk: the root disk must be refused (a fire on real input, not a fixture)
  local rd; rd=$(lsblk -no PKNAME "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1)
  [ -n "$rd" ] || { echo "selftests: cannot tell the root disk, so devguard's root refusal cannot be fired"; return 1; }
  if sudo -n python3 -B "$L/t3/devguard.py" check "/dev/$rd" > "$OUT/devguard-root.txt" 2>&1; then
    echo "devguard ALLOWED the root disk /dev/$rd"; return 1
  fi
  fcenv_check
}
stage selftests selftests
if [ $DRY = 1 ]; then
  echo "dry run: the runner image mounts / nobarrier; remount with barrier as a T3 box has it"
  sudo mount -o remount,barrier / && findmnt -n -o OPTIONS / | tee "$OUT/root-mount.txt"
fi
printf 'fs\tcell\tsystem\tclients\tattempt\tadapter_rc\tvoid\n' > "$OUT/cells.tsv"
[ -z "$FSLIST" ] && FSLIST=$(python3 -B "$L/t3/cells.py" fslist "$MAN")
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
  block_cleanup || rc=1
  e=$(date +%s)
  printf '%s\t%s\t%s\t%s\t%s\n' "fs-$FS_NOW" "$(date -u -d @"$s" +%FT%TZ)" "$(date -u -d @"$e" +%FT%TZ)" $((e - s)) $rc >> "$STAGES"
  [ $rc = 0 ] || { echo "t3run: block fs-$FS_NOW FAILED rc=$rc; the next block runs"; BLOCKS_FAILED=$((BLOCKS_FAILED + 1)); }
done
[ $BLOCKS_FAILED = 0 ] && finish 0
finish 1
