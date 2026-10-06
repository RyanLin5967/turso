#!/bin/bash
# envprobe.sh OUT -- record the hosted-runner facts the V3 probe's design depends on (lane fastest-linux-v3, review 2):
# sysfs driver links and device-reported caches per disk, clocksource and cpufreq, brd / dm-flakey / dm-log-writes
# availability, tracefs block_rq_issue with trace_clock mono_raw and an rwbs filter on a real fsync, and whether the
# shutdown ioctl works on XFS and btrfs loops. Record only; uses sudo; every command bounded.
set -u
out=${1:?usage: envprobe.sh OUT}
mkdir -p "$out"
run() { echo "\$ $*"; timeout 120 "$@" 2>&1; echo "[rc=$?]"; }
{
  run uname -a
  run cat /proc/cmdline
  run ldd --version
  run python3 -V
  run nproc
  run ls -l /sys/block/
  for d in /sys/block/*; do
    n=${d##*/}
    echo "== disk $n"
    echo "realpath=$(readlink -f "$d")"
    echo "device=$(readlink -f "$d/device" 2>/dev/null)"
    echo "device/driver=$(readlink -f "$d/device/driver" 2>/dev/null)"
    echo "device/device/driver=$(readlink -f "$d/device/device/driver" 2>/dev/null)"
    echo "device/subsystem=$(readlink -f "$d/device/subsystem" 2>/dev/null)"
    echo "dev=$(cat "$d/dev") wc=[$(cat "$d/queue/write_cache")] fua=[$(cat "$d/queue/fua")]"
    for c in "$d"/device/scsi_disk/*/cache_type "$d/cache_type"; do [ -e "$c" ] && echo "cache_type $c=[$(cat "$c")]"; done
    ls -d "$d"/multipath/* 2>/dev/null
    [ -e "$d/loop/backing_file" ] && echo "loop backing=$(cat "$d/loop/backing_file")"
  done
  run cat /proc/devices
  run sudo -n nvme list
  for c in /dev/nvme[0-9]; do [ -e "$c" ] && run sudo -n nvme id-ctrl "$c" -o json; done
  run ls -l /sys/class/nvme/ /sys/class/nvme-subsystem/
  run cat /sys/devices/system/clocksource/clocksource0/current_clocksource
  run cat /sys/devices/system/clocksource/clocksource0/available_clocksource
  run ls /sys/devices/system/cpu/cpu0/cpufreq/
  run cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
  run ls /sys/devices/system/cpu/cpu0/cpuidle/
  for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do [ -d "$s" ] && echo "$s name=$(cat "$s/name") disable=$(cat "$s/disable")"; done
  run cat /sys/module/cpuidle/parameters/off
  run ls /sys/devices/system/cpu/cpuidle/
  run cat /sys/devices/system/cpu/cpuidle/current_driver
  run apt-cache policy "linux-modules-extra-$(uname -r)"
  run sudo modprobe brd rd_nr=1 rd_size=2097152
  run ls -l /dev/ram0
  run sudo modprobe dm-flakey
  run sudo modprobe dm-log-writes
  run sudo modprobe null_blk nr_devices=0
  run sudo dmsetup targets
  run ls "/lib/modules/$(uname -r)/kernel/drivers/block/" "/lib/modules/$(uname -r)/kernel/drivers/md/"
  run findmnt -n -o SOURCE,FSTYPE,OPTIONS /
  run cat /proc/fs/ext4/*/options
  run ls /proc/fs/ext4/ /proc/fs/jbd2/
  run mount -t tracefs
  run sudo -n cat /sys/kernel/tracing/trace_clock
  run sudo -n cat /sys/kernel/tracing/events/block/block_rq_issue/format
  run sudo -n cat /sys/kernel/tracing/buffer_size_kb
} > "$out/env.txt" 2>&1

# tracefs: an instance with trace_clock mono_raw and an rwbs filter, around fsyncs on the root fs (barrier on)
sudo mount -o remount,barrier / 2>&1
cat > "$out/tracetest.py" <<'PY'
import os, subprocess, sys, time
T = "/sys/kernel/tracing/instances/envprobe"
def w(p, v):
    r = subprocess.run(["sudo", "-n", "tee", p], input=v, capture_output=True, text=True)
    print("write", p, repr(v), "rc", r.returncode, r.stderr.strip())
def r(p):
    return subprocess.run(["sudo", "-n", "cat", p], capture_output=True, text=True).stdout
subprocess.run(["sudo", "-n", "mkdir", T])
w(T + "/trace_clock", "mono_raw")
print("clock:", r(T + "/trace_clock").strip())
w(T + "/buffer_size_kb", "16384")
w(T + "/events/block/block_rq_issue/filter", 'rwbs ~ "*F*"')
print("filter:", r(T + "/events/block/block_rq_issue/filter").strip())
w(T + "/events/block/block_rq_issue/enable", "1")
w(T + "/tracing_on", "1")
v = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
w(T + "/trace_marker", "probe-start mono_raw_ns=%d" % v)
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
ts = []
for i in range(20):
    t0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
    os.pwrite(fd, b"x" * 25, 25 * i)
    os.fsync(fd)
    ts.append((t0, time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)))
os.close(fd)
w(T + "/tracing_on", "0")
print("windows", ts[:3])
print("TRACE")
print(r(T + "/trace"))
for c in sorted(os.listdir(T + "/per_cpu")):
    print(c, r(T + "/per_cpu/%s/stats" % c).replace("\n", " | "))
subprocess.run(["sudo", "-n", "rmdir", T])
PY
{ timeout 120 python3 -B "$out/tracetest.py" /var/tmp/envprobe-fsync.dat; echo "[rc=$?]"; rm -f /var/tmp/envprobe-fsync.dat; } > "$out/tracetest.txt" 2>&1

# the shutdown ioctl and dm-flakey on throwaway loops
{
  for fs in xfs btrfs; do
    img=/var/tmp/envprobe-$fs.img
    sudo rm -f "$img"; sudo truncate -s 1G "$img"
    dev=$(sudo losetup --find --show "$img")
    if [ $fs = xfs ]; then sudo mkfs.xfs -f -q -m reflink=1 "$dev"; else sudo mkfs.btrfs -f -q "$dev"; fi
    sudo mkdir -p /mnt/envprobe-$fs && sudo mount "$dev" /mnt/envprobe-$fs
    run sudo xfs_io -x -c "shutdown" /mnt/envprobe-$fs
    run sudo umount /mnt/envprobe-$fs
    run sudo mount "$dev" /mnt/envprobe-$fs
    run sudo umount /mnt/envprobe-$fs
    sz=$(sudo blockdev --getsz "$dev")
    run sudo dmsetup create envprobe-flakey-$fs --table "0 $sz flakey $dev 0 180 0"
    run sudo dmsetup table
    run sudo dmsetup remove envprobe-flakey-$fs
    sudo losetup -d "$dev"; sudo rm -f "$img"
  done
} > "$out/crashtest.txt" 2>&1
echo "envprobe done: $(wc -l < "$out/env.txt") lines"
