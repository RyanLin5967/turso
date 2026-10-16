#!/bin/bash
# vm_dryblock.sh SHA OUTDIR [FS...] -- T3 gate item 6's dry run as amended by the lead (DECISIONS 2026-10-15): t3run
# --dry-block on a lima VM's real non-loop virtual disk, on this Mac, for each FS (default: xfs btrfs). Every output is
# stamped DRY-VIRT, which summarize refuses for credit. Item 6 is MET only when this passes end to end on xfs AND btrfs.
#
# Refuses (exit 2), before anything is created or started:
#   - the Mac host has under VM_HOST_MIN_GIB free (default 35, t3lib.sh host_room_ok; never under HOST_STOP_GIB);
#   - the VM asks for more than 8 GiB of memory or 4 CPUs (the ruled ceiling);
#   - limactl is missing.
# Then: the VM fl-t3 (Ubuntu 24.04, made if absent) gets the data disk t3data (qcow2, sparse, DATA_GIB, made if
# absent, never formatted by lima), starts, and runs t3run.sh from a clone of the fork at SHA with --dry-block on the
# one unmounted virtio disk of DATA_GIB. The VM's own root must have ROOT_MIN_GIB free for the build. The package
# (<out>.tar.gz and its sha256) is copied to OUTDIR. The VM is stopped afterwards; nothing is deleted.
# OUTDIR/host-disk.tsv traces the host's free space and the VM images every 30 s; under HOST_STOP_GIB (30) free the
# VM is stopped mid-run and the exit is 3.
# Environment (bounded, never above the ruling): VM_CPUS (default 4), VM_MEM_GIB (8), DATA_GIB (16), ROOT_MIN_GIB (25).
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/t3lib.sh"
SHA=${1:?SHA} OUTDIR=${2:?OUTDIR}; shift 2
FSL=${*:-xfs btrfs}
VM=fl-t3 DISK=t3data
# DATA_GIB 16 (the lead's ruling, DECISIONS 2026-10-16): a btrfs block's copy-on-write fsyncs leave every touched
# block allocated in the sparse image (run 15: 8.6 GiB in one minute), so the device size is the bound on the host's
# data image; any ENOSPC this causes refuses the block (summarize enospc_problems), never retried smaller
CPUS=${VM_CPUS:-4} MEM=${VM_MEM_GIB:-8} DATA=${DATA_GIB:-16} ROOT_MIN=${ROOT_MIN_GIB:-25}
command -v limactl > /dev/null || { echo "vm_dryblock: REFUSED: limactl is not installed"; exit 2; }
[[ $CPUS =~ ^[0-9]+$ && $MEM =~ ^[0-9]+$ ]] && [ "$CPUS" -le 4 ] && [ "$MEM" -le 8 ] ||
  { echo "vm_dryblock: REFUSED: the VM may have at most 4 CPUs and 8 GiB (asked $CPUS CPUs, $MEM GiB)"; exit 2; }
# the launch guard, VM_HOST_MIN_GIB (default t3lib's 35; the lead's derived guard for run 12 was 32.7, so 33): never
# below the in-run stop (HOST_STOP_GIB, 30), or the run would launch straight into its own stop. (An env
# HOST_MIN_FREE_GB was silently overwritten by t3lib.sh's assignment: the guard is passed as host_room_ok's argument.)
GUARD=${VM_HOST_MIN_GIB:-$HOST_MIN_FREE_GB}
[[ $GUARD =~ ^[0-9]+$ ]] && [ "$GUARD" -ge "${HOST_STOP_GIB:-30}" ] ||
  { echo "vm_dryblock: REFUSED: VM_HOST_MIN_GIB [$GUARD] is not a whole GiB at or above the in-run stop (${HOST_STOP_GIB:-30})"; exit 2; }
free_kb=$(df -k / | awk 'NR == 2 { print $4 }')
host_room_ok "$free_kb" "$GUARD" || exit 2
mkdir -p "$OUTDIR" || exit 2
# the same self-tests on THIS platform first, from the commit the VM will run (gates/run_everywhere.py over a git
# archive of SHA): the Mac half of "every gate runs on both Mac and Linux"; t3run's selftests stage is the Linux half
mac=$(mktemp -d)
# from the repository's top: git archive's pathspecs are relative to the directory it runs in (its first run, from
# t3/, matched nothing: the platform class again, a path inherited from where the script happens to sit)
top=$(git -C "$HERE" rev-parse --show-toplevel) && git -C "$top" archive "$SHA" fastest .github | tar -x -C "$mac" ||
  { echo "vm_dryblock: REFUSED: cannot archive $SHA here for the Mac self-tests"; rm -rf "$mac"; exit 2; }
gnu=""; for g in /opt/homebrew/opt/coreutils/libexec/gnubin /usr/local/opt/coreutils/libexec/gnubin; do [ -d "$g" ] && gnu=$g:; done
PATH=$gnu/opt/homebrew/bin:$PATH timeout 3600 python3 -B "$mac/fastest/linux/gates/run_everywhere.py" "$mac/fastest/linux" \
  > "$OUTDIR/mac-run-everywhere.txt" 2>&1 ||
  { tail -6 "$OUTDIR/mac-run-everywhere.txt"; echo "vm_dryblock: REFUSED: the Mac self-tests at $SHA fail"; rm -rf "$mac"; exit 2; }
tail -1 "$OUTDIR/mac-run-everywhere.txt"
rm -rf "$mac"
mkdir -p "$OUTDIR" || exit 2
log=$OUTDIR/vm_dryblock.log
exec > >(tee -a "$log") 2>&1
echo "# vm_dryblock sha=$SHA fs=[$FSL] cpus=$CPUS mem=${MEM}GiB data=${DATA}GiB host_free_kb=$free_kb start=$(date -u +%FT%TZ)"
if ! limactl list -q 2>/dev/null | grep -qx "$VM"; then
  timeout 900 limactl create --name="$VM" --cpus="$CPUS" --memory="$MEM" --disk=40 --mount-none --tty=false \
    template://ubuntu-24.04 || { echo "vm_dryblock: limactl create failed"; exit 1; }
fi
# (limactl disk list has no -q: the name is the first column of its table, 2026-10-15)
limactl disk list 2>/dev/null | awk 'NR > 1 { print $1 }' | grep -qx "$DISK" ||
  { timeout 300 limactl disk create "$DISK" --size "${DATA}GiB" || { echo "vm_dryblock: disk create failed"; exit 1; }; }
timeout 300 limactl stop "$VM" > /dev/null 2>&1 || true
timeout 120 limactl edit "$VM" --set ".cpus=$CPUS | .memory=\"${MEM}GiB\" | .additionalDisks=[{\"name\":\"$DISK\",\"format\":false}]" ||
  { echo "vm_dryblock: cannot configure $VM"; exit 1; }
timeout 900 limactl start "$VM" || { echo "vm_dryblock: $VM did not start"; exit 1; }
# the host-disk trace (the lead's derived guard, 2026-10-16: run 11 recorded no host peak, only host_free_kb at its
# start): every 30 s the host's free KiB and the VM's root and data images' allocated KiB, so the next run's guard is
# 28 GiB (buildslot's floor) + the MEASURED peak growth + 2. And an in-run stop, an outcome guard on the fleet's room:
# when the host's free space falls under HOST_STOP_GIB (default 30: buildslot's 28 + 2), the VM is stopped and the run
# ends there, recorded as STOP in the trace
STOP_GIB=${HOST_STOP_GIB:-30}
( printf 'utc\tfree_kb\troot_img_kb\tdata_img_kb\n'
  while :; do
    f=$(df -k / | awk 'NR == 2 { print $4 }')
    printf '%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$f" "$(du -sk "$HOME/.lima/$VM" | cut -f1)" \
      "$(du -sk "$HOME/.lima/_disks/$DISK" | cut -f1)"
    if [[ $f =~ ^[0-9]+$ ]] && [ "$f" -lt $((STOP_GIB * 1024 * 1024)) ]; then
      echo "STOP: host free $((f / 1024 / 1024)) GiB under $STOP_GIB GiB: stopping $VM"
      timeout 300 limactl stop -f "$VM" > /dev/null 2>&1
      break
    fi
    sleep 30
  done ) > "$OUTDIR/host-disk.tsv" 2>&1 &
watch=$!
rc=0
timeout 43200 limactl shell "$VM" -- bash -s "$SHA" "$FSL" "$DATA" "$ROOT_MIN" << 'VMEOF' || rc=$?
set -uo pipefail
SHA=$1 FSL=$2 DATA=$3 ROOT_MIN=$4
root_free=$(df -k / | awk 'NR == 2 { print $4 }')
[ "$root_free" -ge $((ROOT_MIN * 1024 * 1024)) ] ||
  { echo "vm_dryblock (VM): REFUSED: the VM's root has $((root_free / 1024 / 1024)) GiB free, under $ROOT_MIN"; exit 2; }
# the data disk: the one virtio disk of DATA GiB with no partition and nothing mounted (devguard re-checks it)
want=$((DATA * 1024 * 1024 * 1024))
mapfile -t cand < <(lsblk -bdn -o NAME,SIZE,TYPE | awk -v w="$want" '$3 == "disk" && $2 == w && $1 ~ /^vd/ { print $1 }')
[ ${#cand[@]} = 1 ] || { echo "vm_dryblock (VM): REFUSED: ${#cand[@]} virtio disks of $DATA GiB (${cand[*]:-none}), want 1"; exit 2; }
dev=/dev/${cand[0]}
command -v git > /dev/null || { sudo apt-get update -q && sudo apt-get install -y -q git; } || exit 1
rm -rf ~/t3-out ~/t3-out.src ~/t3-out.tar.gz ~/t3-out.tar.gz.sha256
git clone -q --filter=blob:none https://github.com/RyanLin5967/turso ~/t3-out.src && git -C ~/t3-out.src checkout -q --detach "$SHA" ||
  { echo "vm_dryblock (VM): cannot check out $SHA"; exit 2; }
T3RUN_INSIDE=1 bash ~/t3-out.src/fastest/linux/t3/t3run.sh --sha "$SHA" --out ~/t3-out --dry-block "$dev" --fs "$FSL"
VMEOF
kill "$watch" 2> /dev/null; wait "$watch" 2> /dev/null
grep -q '^STOP' "$OUTDIR/host-disk.tsv" && { echo "vm_dryblock: STOPPED by the host-disk guard ($OUTDIR/host-disk.tsv)"; rc=3; }
echo "vm_dryblock: t3run in $VM rc=$rc"
timeout 600 limactl copy "$VM:t3-out.tar.gz" "$OUTDIR/" && timeout 60 limactl copy "$VM:t3-out.tar.gz.sha256" "$OUTDIR/" ||
  echo "vm_dryblock: no package to copy back"
timeout 300 limactl stop "$VM" > /dev/null 2>&1 || true
echo "# vm_dryblock done rc=$rc end=$(date -u +%FT%TZ)"
exit "$rc"
