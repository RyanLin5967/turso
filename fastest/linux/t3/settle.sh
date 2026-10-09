# settle.sh -- sourced by t3run.sh: the quiet gap before every measured run (gate-6 review 11; third lane review MED 5;
# fourth lane review MED 5 and LOW 25; T3 runner review item 9).
#
#   settle_dev MNT       the block device MNT's filesystem sits on (the device itself, the loop of a loop block, the
#                        ram disk of a brd block; btrfs [subvol] stripped), printed; exit 1 when its stat file is
#                        missing or has fewer than 17 fields (no discard and flush counters), so the block fails
#                        once instead of every run paying the full cap
#   settle MNT DEV DIR   before a run (called at the top of every attempt, so DIR is the run the gap precedes,
#                        including the first run of a block and every replacement attempt): fstrim the filesystem
#                        (its discards happen here, outside any timed window, and are recorded), sync it, then wait
#                        until DEV has no request in flight, the page cache holds under 1 MiB dirty, AND DEV's
#                        completed write, discard and flush counters (stat fields 5, 12, 16) have not moved, for 2 s
#                        running (11 polls 0.2 s apart, every one equal to the first), bounded by SETTLE_CAP_S of
#                        elapsed time from before the sync. Writes DIR/settle.txt; summarize refuses a measured run
#                        with no settle.txt or with quiet other than yes.
# Its own device, not the leaf: a loop block's leaf is the runner's root disk, whose traffic is not this run's.
# btrfs is mounted nodiscard (mkloop.sh, t3run's device branch) and fs_block refuses a discard mount, so freed
# extents are never discarded asynchronously inside a later run (btrfs discard=async delays them 10-120 s); fstrim
# here issues them synchronously instead.
# Overrides, for settle_test.sh only: SETTLE_SYS (/sys), SETTLE_PROC (/proc), SETTLE_FSTRIM (sudo -n fstrim -v),
# SETTLE_SYNC (sync -f), SETTLE_CAP_S (60), SETTLE_FINDMNT (findmnt).

settle_dev() { # settle_dev MNT
  local src dev
  src=$(${SETTLE_FINDMNT:-findmnt} -n -o SOURCE -T "$1" | sed 's/\[.*//')
  dev=$(basename "$(readlink -f "$src")")
  [ -n "$dev" ] || { echo "settle_dev: no block device under $1" >&2; return 1; }
  awk 'NF < 17 { exit 1 }' "${SETTLE_SYS:-/sys}/class/block/$dev/stat" 2>/dev/null ||
    { echo "settle_dev: ${SETTLE_SYS:-/sys}/class/block/$dev/stat is missing or has fewer than 17 fields" >&2; return 1; }
  echo "$dev"
}

settle() { # settle MNT DEV DIR
  local mnt=$1 dev=$2 d=$3 t0 now q=0 inflight dirty st c c0="" trim cap=${SETTLE_CAP_S:-60}
  t0=$(date +%s%N)
  trim=$(${SETTLE_FSTRIM:-sudo -n fstrim -v} "$mnt" 2>&1 | tail -1)
  ${SETTLE_SYNC:-sync -f} "$mnt" 2>/dev/null || sync
  while :; do
    st=$(cat "${SETTLE_SYS:-/sys}/class/block/$dev/stat" 2>/dev/null)
    inflight=$(echo "$st" | awk '{print $9}')
    c=$(echo "$st" | awk 'NF >= 16 {print $5 "/" $12 "/" $16}')
    dirty=$(awk '/^Dirty:/ {print $2}' "${SETTLE_PROC:-/proc}/meminfo")
    if [ "${inflight:-1}" = 0 ] && [ "${dirty:-999999}" -lt 1024 ] && [ -n "$c" ]; then
      if [ $q = 0 ] || [ "$c" != "$c0" ]; then c0=$c; q=1; else q=$((q + 1)); fi
    else
      q=0
    fi
    [ $q -ge 11 ] && break
    now=$(date +%s%N)
    [ $(( (now - t0) / 1000000000 )) -ge "$cap" ] && break
    sleep 0.2
  done
  printf 'dev=%s settle_s=%s quiet=%s inflight=%s dirty_kb=%s writes/discards/flushes=%s fstrim=%s\n' "$dev" \
    "$(awk -v a="$t0" -v b="$(date +%s%N)" 'BEGIN { printf "%.2f", (b - a) / 1e9 }')" \
    "$([ $q -ge 11 ] && echo yes || echo no)" "$inflight" "$dirty" "${c:-unreadable}" "\"${trim}\"" > "$d/settle.txt"
}
