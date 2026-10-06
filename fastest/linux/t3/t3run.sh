#!/bin/bash
# ONE-COMMAND T3 RUNNER (FASTEST T3-READY gate item 6): a fresh Ubuntu 24.04 box -> finished raws.
#
#   curl -sSfL https://raw.githubusercontent.com/RyanLin5967/turso/<SHA>/fastest/linux/t3/t3run.sh \
#     | bash -s -- --sha <SHA> --out ~/t3-out [--dry-run [--block loop|brd] [--plant NAME]] \
#                  [--manifest <path in repo>] [--fs "xfs btrfs"] [--device /dev/nvmeXnY --destroy /dev/nvmeXnY] [--seed N]
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
#              a V3 batch AFTER. A V3 batch with rc != 0 or without summary.json and raw.tsv, and a V3L that is
#              not VALID (v3l.py: VOID voids the block), fail the stage (review 2 items 5 and 6).
#   package    <out>.tar.gz + SHA256SUMS + summary.json (stages, blocks with their V3/V3L records, cells,
#              verdicts, wall time)
#
# V3 cells (review 2 item 4; fastest/linux/v3/v3cell.py), never inferred from the fs name:
#   --dry-run --block loop (default)  <fs>loop: the fs on a loop device whose backing file is on the root disk
#   --dry-run --block brd             <fs> on brd's /dev/ram0 (v3/mkbrd.sh; xfs and btrfs): a block device with no
#                                     loop, as a rental's data disk; brd is fire-check only, so its V3 batches run
#                                     V3_SMOKE=1 (run.sh's bound mode refuses brd)
#   real run                          <fs> on --device, V3 batches bound to the block's fire-check verdict, with
#                                     V3_REQUIRE_T3=1 (the governor is set to performance in deps)
# Modes. --dry-run: smoke manifest allowed, loop or brd filesystems, the root remounted with barrier, results
# never credited; --plant NAME (dry runs only) breaks one thing on purpose so the run must fail:
#   v3-verdict-missing  the BEFORE V3 batch is bound to a verdict file that does not exist (run.sh rc 2)
#   v3l-fsync-half      the BEFORE V3L's fio syncs every 2nd write (V3L_PLANT=fsync2: V1L and fio must VOID it)
# Without --dry-run (a real T3 rental) it REFUSES unless: the manifest's sha256 is listed in
# fastest/linux/t3/REGISTERED-MANIFESTS (append-only; empty until the T3 registration), --device and --destroy
# name the same unmounted block device that is not the root disk, and every target mounts with barriers.
# Every file the run calls must be in the commit (preflight lists them; review 2 item 18).
# Exit: 0 every stage ran and every cell has a verdict; 1 a stage failed; 2 refused before anything ran.
set -uo pipefail
REPO_URL=https://github.com/RyanLin5967/turso
SHA="" OUT="" DRY=0 MANIFEST="" FSLIST="" DEVICE="" DESTROY="" SEED=20261005 BLOCK="" PLANT=""
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
    ${BLOCK:+--block "$BLOCK"} ${PLANT:+--plant "$PLANT"}
fi

SRC=$(git -C "$HERE" rev-parse --show-toplevel)
L=$SRC/fastest/linux
if [ $DRY = 1 ]; then
  case ${BLOCK:=loop} in loop|brd) ;; *) echo "t3run: REFUSED: --block is loop or brd, not '$BLOCK'" >&2; exit 2 ;; esac
  case $PLANT in ''|v3-verdict-missing|v3l-fsync-half) ;; *) echo "t3run: REFUSED: unknown plant '$PLANT'" >&2; exit 2 ;; esac
  # brd batches are smoke (unbound), so a verdict plant there would plant nothing
  [ "$PLANT:$BLOCK" != v3-verdict-missing:brd ] || { echo "t3run: REFUSED: plant v3-verdict-missing needs --block loop" >&2; exit 2; }
else
  [ -z "$BLOCK" ] || { echo "t3run: REFUSED: --block is for dry runs; a real run uses --device" >&2; exit 2; }
  [ -z "$PLANT" ] || { echo "t3run: REFUSED: --plant is for dry runs only" >&2; exit 2; }
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
printf 'dry=%s\nblock=%s\nplant=%s\n' "$DRY" "$BLOCK" "$PLANT" > "$OUT/mode.txt"

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
  ( cd "$(dirname "$OUT")" && tar --exclude="$(basename "$OUT")/work" -czf "$OUT.tar.gz" "$(basename "$OUT")" &&
    sha256sum "$(basename "$OUT").tar.gz" > "$OUT.tar.gz.sha256" )
  echo "# t3run done rc=$rc wall_s=$t package=$OUT.tar.gz"
  exit "$rc"
}

# Every file this run calls, by path in the commit (an allowlist: a missing one refuses here with its name,
# not hours later; review 2 item 18 found the competitors absent from the runner's home branch).
NEEDS="t3/hwid.sh t3/foreign_cpu.py t3/cells.py t3/summarize.py t3/v3l.py hw/record.sh fs/mkloop.sh
  competitors/build.sh competitors/fetch_dolt.sh competitors/firecheck_strace.sh competitors/run_system.sh
  v3/v3floor.c v3/statfs_shim.c v3/v3cell.py v3/firecheck.sh v3/run.sh v3/mkfixtures.sh v3/mkbrd.sh v3/check.py
  v3/batchgate.py v3/blkflush.py v3/stamp.py"
preflight() {
  [ -f "$MAN" ] || { echo "no manifest $MAN"; return 2; }
  local f miss=""
  for f in $NEEDS; do [ -f "$L/$f" ] || miss="$miss fastest/linux/$f"; done
  [ -z "$miss" ] || { echo "REFUSED: this commit lacks files the run calls:$miss"; return 2; }
  grep -q 'Ubuntu 24' /etc/os-release || { echo "not Ubuntu 24.04"; return 2; }
  sudo -n true || { echo "needs passwordless sudo"; return 2; }
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
    case $(basename "$DEVICE") in loop*|ram*|zram*) echo "REFUSED: $DEVICE is not a physical device"; return 2 ;; esac
    if lsblk -n -o MOUNTPOINTS "$DEVICE" | grep -q .; then echo "REFUSED: $DEVICE (or a partition) is mounted"; return 2; fi
    local rootdisk; rootdisk=$(lsblk -n -o PKNAME "$(findmnt -n -o SOURCE /)" 2>/dev/null)
    [ "/dev/$rootdisk" != "$DEVICE" ] || { echo "REFUSED: $DEVICE holds the root filesystem"; return 2; }
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
  gcc -O2 -std=gnu11 -Wall -Wextra -Werror -o "$DIST/v3floor" "$L/v3/v3floor.c" > "$OUT/build-v3.txt" 2>&1 || return 1
  gcc -O2 -shared -fPIC -o "$DIST/statfs_shim.so" "$L/v3/statfs_shim.c" -ldl >> "$OUT/build-v3.txt" 2>&1 || return 1
  { echo "sha=$SHA"; rustc -V; gcc --version | head -1; /usr/lib/postgresql/18/bin/postgres --version
    fio --version; strace -V | head -1
    ( cd "$DIST" && sha256sum fastest_profile bbload clonebench sqlite3 v3floor statfs_shim.so ); } > "$OUT/binaries.txt"
  return 0
}

hwid() { bash "$L/t3/hwid.sh" "$OUT/hwid" /; }

# The V3 fire-check's refusal fixtures (scratch loops on the root disk, never the target device), made
# once for every filesystem block.
V3FX=/mnt/t3-v3fx
v3fixtures() { bash "$L/v3/mkfixtures.sh" "$V3FX" > "$OUT/v3fixtures.txt" 2>&1; }

# One V3 batch of a block (review 2 item 5): rc != 0, or no summary.json or raw.tsv, fails the stage. The rc
# goes to v3.rc whatever happens, so summarize.py reads every batch's outcome, including a refused one.
v3batch() { # v3batch before|after DIR
  local when=$1 dir=$2 o=$OUT/fs-$FS_NOW rc
  local -a env=(V3_CELL="$V3CELL")
  if [ "$BLOCK" = brd ]; then
    env+=(V3_SMOKE=1 V3FLOOR_BRD=1)
  else
    local verdict=$o/v3-firecheck/verdict.json
    [ "$PLANT:$when" = v3-verdict-missing:before ] && verdict=$o/v3-firecheck/planted-missing-verdict.json
    env+=(V3_FIRECHECK_VERDICT="$verdict")
    [ $DRY = 0 ] && env+=(V3_REQUIRE_T3=1)
  fi
  echo "v3 $when: ${env[*]}"
  env "${env[@]}" timeout 1800 bash "$L/v3/run.sh" "$DIST/v3floor" "$dir" "$o/v3-$when" 200 > "$o/v3-$when.txt" 2>&1
  rc=$?
  echo "$when rc=$rc" >> "$o/v3.rc"
  # brd only (dry runs): a VOID batch with complete raws is recorded, not a failed stage. brd has no drive, so the
  # probe's timing control (fsync vs no-sync p50 ratio) cannot discriminate there: dry run 37517239631 read ow4k
  # 9.5 against its threshold of 10 on xfs/brd. Every other block, and any other rc, keeps item 5's rule.
  if [ $rc = 3 ] && [ "$BLOCK" = brd ] && [ -f "$o/v3-$when/summary.json" ] && [ -f "$o/v3-$when/raw.tsv" ]; then
    echo "V3 $when batch on $V3CELL (brd): VOID recorded, not a stage failure (smoke; no drive behind brd)"
    return 0
  fi
  [ $rc = 0 ] || { echo "V3 $when batch on $V3CELL: rc $rc (run.sh: $(tail -1 "$o/v3-$when.txt"))"; return 1; }
  [ -f "$o/v3-$when/summary.json" ] && [ -f "$o/v3-$when/raw.tsv" ] ||
    { echo "V3 $when batch on $V3CELL: rc 0 but no summary.json or raw.tsv"; return 1; }
  return 0
}

# V3L before or after a block (review 2 item 6; PREREG line 180): VOID or refused fails the stage.
v3l() { # v3l before|after MNT
  local when=$1 mnt=$2 o=$OUT/fs-$FS_NOW rc
  local -a env=()
  [ "$PLANT:$when" = v3l-fsync-half:before ] && env=(V3L_PLANT=fsync2)
  env "${env[@]}" timeout 1800 python3 -B "$L/t3/v3l.py" measure "$mnt/v3l-$when" "$o/v3l-$when" > "$o/v3l-$when.txt" 2>&1
  rc=$?
  echo "$when rc=$rc" >> "$o/v3l.rc"
  [ $rc = 0 ] || { echo "V3L $when on $FS_NOW: rc $rc ($(tail -1 "$o/v3l-$when.txt"))"; return 1; }
  return 0
}

# One block: make the fs, record it, fire-check the V3 probe on the block's cell, V3 and V3L before, the
# cells, V3L and V3 after.
FS_NOW="" V3CELL=""
fs_block() {
  local fs=$FS_NOW mnt=/mnt/t3-$FS_NOW o=$OUT/fs-$FS_NOW
  mkdir -p "$o"
  case $BLOCK in loop) V3CELL=${fs}loop ;; *) V3CELL=$fs ;; esac
  printf 'cell=%s\nblock=%s\n' "$V3CELL" "$BLOCK" > "$o/block.txt"
  case $BLOCK in
    loop)
      bash "$L/fs/mkloop.sh" "$([ "$fs" = ext4 ] && echo ext4loop || echo "$fs")" "$mnt" 60G > "$o/mkfs.txt" 2>&1 || return 1
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
  V3_FX=$V3FX V3_SHIM=$DIST/statfs_shim.so timeout 3900 bash "$L/v3/firecheck.sh" "$DIST/v3floor" "$V3CELL" \
    "$mnt/v3fc" "$o/v3-firecheck" > "$o/v3-firecheck.txt" 2>&1 || { echo "V3 fire-check failed on $V3CELL"; return 1; }
  mkdir -p "$mnt/v3b" "$mnt/v3a"
  v3batch before "$mnt/v3b" || return 1
  v3l before "$mnt" || return 1
  # flush counter fire-check for the competitor cells on this filesystem (run_system.sh requires its verdict)
  sudo sysctl -w kernel.yama.ptrace_scope=0 > /dev/null
  timeout 900 bash "$L/competitors/firecheck_strace.sh" "$o/strace-firecheck" "$mnt/strace-fc.noindex" \
    > "$o/strace-firecheck.txt" 2>&1 || { echo "strace fire-check failed on $fs"; return 1; }
  python3 -B "$L/t3/cells.py" plan "$MAN" "$fs" "$SEED" > "$o/plan.tsv" || return 1
  local cell system clients ops runs class
  while IFS=$'\t' read -r cell system clients ops runs class; do
    run_cell "$fs" "$mnt" "$o" "$cell" "$system" "$clients" "$ops" "$class"
  done < "$o/plan.tsv"
  v3l after "$mnt" || return 1
  v3batch after "$mnt/v3a" || return 1
  local dev; dev=$(findmnt -n -o SOURCE "$mnt")
  sudo umount "$mnt" || return 1
  case $BLOCK:$dev in loop:/dev/loop*)
    local back; back=$(losetup -n -O BACK-FILE "$dev" | xargs)
    sudo losetup -d "$dev" && sudo rm -f "$back" ;;
  esac
  return 0
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
        timeout 7200 "$DIST/fastest_profile" --dir "$mnt/work-$id" --class "$class" --clients "$clients" \
          --ops "$ops" --warmup 20 --mode phases --out "$d/result" > "$d/adapter.txt" 2>&1 ;;
      pg18-d2|pg18-defaults|dolt|doltgres|b1)
        # Each attempt gets its own directory on the filesystem under test (run_system.sh keeps its
        # servers' data under MNT/<system>.noindex, which a replacement run must not find in place).
        mkdir -p "$mnt/work-$id"
        FT_CLIENTS=$clients FT_N1=$ops FT_N4=$ops FT_BBLOAD=$DIST/bbload FT_CLONEBENCH=$DIST/clonebench \
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

stage preflight preflight
stage deps deps
export PATH="$HOME/.cargo/bin:$PATH"
stage build build
stage hwid hwid
stage v3fixtures v3fixtures
selftests() {
  python3 -B "$L/t3/foreign_cpu.py" self-test > "$OUT/foreign_cpu-selftest.txt" 2>&1 || return 1
  python3 -B "$L/t3/v3l.py" self-test > "$OUT/v3l-selftest.txt" 2>&1 || return 1
  python3 -B "$L/t3/summarize.py" self-test > "$OUT/summarize-selftest.txt" 2>&1
}
stage selftests selftests
if [ $DRY = 1 ]; then
  echo "dry run: the runner image mounts / nobarrier; remount with barrier as a T3 box has it"
  sudo mount -o remount,barrier / && findmnt -n -o OPTIONS / | tee "$OUT/root-mount.txt"
fi
printf 'fs\tcell\tsystem\tclients\tattempt\tadapter_rc\tvoid\n' > "$OUT/cells.tsv"
[ -z "$FSLIST" ] && FSLIST=$(python3 -B "$L/t3/cells.py" fslist "$MAN")
echo $FSLIST > "$OUT/fslist.txt"
for FS_NOW in $FSLIST; do
  stage "fs-$FS_NOW" fs_block
done
finish 0
