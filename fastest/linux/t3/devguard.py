#!/usr/bin/env python3
"""devguard.py -- may a real T3 run destroy this device? An ALLOWLIST (gate-6 review M4: the old check was a basename
denylist, and a partition of the root disk or a symlink to /dev/ram0 passed it).

  devguard.py check DEVICE     exit 0 and print the resolved disk when every rule holds; else exit 2 with every reason
  devguard.py self-test        the rules on synthetic lsblk trees; exit 0 iff every case passes

Rules (all must hold):
  - DEVICE resolves (symlinks followed) to /dev/<name> with name a whole NVMe namespace (nvmeXnY), SCSI disk (sdX) or
    virtio disk (vdX); lsblk TYPE is "disk" (a partition, loop, ram, dm, md, nbd, pmem or anything else is refused);
  - no partition or holder of it is mounted or in use: lsblk MOUNTPOINTS anywhere in its tree; every descendant is a
    bare partition (an LVM volume, md array, crypt or dm child refuses); the disk's AND each partition's
    /sys/block/<disk>/<part>/holders are empty; and an exclusive open (O_EXCL) of the disk and of each partition
    succeeds, so nothing in the kernel claims them (lane review MED 5; needs root, so t3run runs it under sudo -n);
  - it is not the disk that holds "/" (compared by disk, so a spare partition of the root disk cannot pass);
  - it is not a native-multipath NVMe head (/sys/block/<name>/multipath non-empty): its flush counter may live on the
    path devices, which V3L does not read (stated blind spot, refused rather than guessed).
"""
import json
import os
import re
import subprocess
import sys

ALLOWED = re.compile(r"^(nvme\d+n\d+|sd[a-z]+|vd[a-z]+)$")


def tree_mounts(node):
    m = [x for x in (node.get("mountpoints") or []) if x]
    for c in node.get("children") or []:
        m += tree_mounts(c)
    return m


def root_disk(devices):
    for d in devices:
        if "/" in tree_mounts(d):
            return d["name"]
    return None


def descendants(node):
    out = []
    for c in node.get("children") or []:
        out.append(c)
        out += descendants(c)
    return out


def problems(name, devices, holders, multipath, part_holders=None, busy=None):
    bad = []
    if not ALLOWED.match(name or ""):
        bad.append(f"{name!r} is not an NVMe namespace, SCSI disk or virtio disk (allowlist nvmeXnY, sdX, vdX)")
    node = next((d for d in devices if d.get("name") == name), None)
    if node is None:
        bad.append(f"{name!r} is not a top-level block device in lsblk (a partition or a missing device)")
        return bad
    if node.get("type") != "disk":
        bad.append(f"{name} has lsblk TYPE {node.get('type')!r}, not disk")
    m = tree_mounts(node)
    if m:
        bad.append(f"{name} or a partition is mounted at {m}")
    if holders:
        bad.append(f"{name} is held by {holders} (dm/md)")
    for c in descendants(node):
        if c.get("type") != "part":
            bad.append(f"{name} carries {c.get('name')} of TYPE {c.get('type')!r} (LVM/md/crypt/dm): in use")
    for part, h in sorted((part_holders or {}).items()):
        if h:
            bad.append(f"partition {part} of {name} is held by {h}")
    for dev, why in sorted((busy or {}).items()):
        bad.append(f"{dev} cannot be opened exclusively ({why}): the kernel claims it")
    if multipath:
        bad.append(f"{name} is a multipath NVMe head ({multipath}): its flush counter may be on the path devices")
    rd = root_disk(devices)
    if rd is None:
        bad.append("cannot tell which disk holds / (refused rather than guessed)")
    elif rd == name:
        bad.append(f"{name} holds the root filesystem")
    return bad


def check(dev):
    real = os.path.realpath(dev)
    name = os.path.basename(real)
    if not real.startswith("/dev/") or not os.path.exists(real):
        return name, [f"{dev} does not resolve to a device node under /dev ({real})"]
    out = subprocess.run(["lsblk", "-J", "-o", "NAME,TYPE,MOUNTPOINTS"], capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        return name, [f"lsblk failed: {out.stderr.strip()[:200]}"]
    devices = json.loads(out.stdout).get("blockdevices") or []
    hp = f"/sys/block/{name}/holders"
    holders = sorted(os.listdir(hp)) if os.path.isdir(hp) else []
    mp = f"/sys/block/{name}/multipath"
    multipath = sorted(os.listdir(mp)) if os.path.isdir(mp) else []
    node = next((d for d in devices if d.get("name") == name), {}) or {}
    parts = [c.get("name") for c in descendants(node) if c.get("type") == "part"]
    part_holders = {}
    for p in parts:
        ph = f"/sys/block/{name}/{p}/holders"
        part_holders[p] = sorted(os.listdir(ph)) if os.path.isdir(ph) else []
    busy = {}
    for dev in [name] + parts:
        try:
            fd = os.open(f"/dev/{dev}", os.O_RDONLY | os.O_EXCL)
            os.close(fd)
        except OSError as e:
            busy[dev] = e.strerror
    return name, problems(name, devices, holders, multipath, part_holders, busy)


def self_test():
    root = {"name": "nvme0n1", "type": "disk", "mountpoints": [None],
            "children": [{"name": "nvme0n1p1", "type": "part", "mountpoints": ["/"]},
                         {"name": "nvme0n1p3", "type": "part", "mountpoints": [None]}]}
    spare = {"name": "nvme1n1", "type": "disk", "mountpoints": [None]}
    mounted = {"name": "sdb", "type": "disk", "mountpoints": [None],
               "children": [{"name": "sdb1", "type": "part", "mountpoints": ["/mnt"]}]}
    ram = {"name": "ram0", "type": "disk", "mountpoints": [None]}
    loop = {"name": "loop3", "type": "loop", "mountpoints": [None]}
    devs = [root, spare, mounted, ram, loop]
    cases = [
        ("a spare NVMe disk passes", problems("nvme1n1", devs, [], []) == []),
        ("the root disk is refused", problems("nvme0n1", devs, [], []) != []),
        ("a spare partition of the root disk is refused (M4)", problems("nvme0n1p3", devs, [], []) != []),
        ("a disk with a mounted partition is refused", problems("sdb", devs, [], []) != []),
        ("ram0 is refused (not on the allowlist)", problems("ram0", devs, [], []) != []),
        ("a loop is refused", problems("loop3", devs, [], []) != []),
        ("a held disk (dm/md) is refused", problems("nvme1n1", devs, ["dm-0"], []) != []),
        ("a multipath NVMe head is refused", problems("nvme1n1", devs, [], ["nvme1c1n1"]) != []),
        ("no disk holding / is refused", problems("nvme1n1", [spare], [], []) != []),
        ("MED 5: a partition carrying an unmounted LVM volume is refused",
         problems("nvme2n1", devs + [{"name": "nvme2n1", "type": "disk", "mountpoints": [None], "children": [
             {"name": "nvme2n1p1", "type": "part", "mountpoints": [None],
              "children": [{"name": "vg-lv", "type": "lvm", "mountpoints": [None]}]}]}], [], []) != []),
        ("MED 5: a partition held by an md array is refused",
         problems("nvme1n1", devs, [], [], {"nvme1n1p1": ["md0"]}) != []),
        ("MED 5: a device the kernel claims (EBUSY on O_EXCL) is refused",
         problems("nvme1n1", devs, [], [], {}, {"nvme1n1": "Device or resource busy"}) != []),
    ]
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"DEVGUARD self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"DEVGUARD SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:] == ["self-test"]:
        return self_test()
    if len(a) == 3 and a[1] == "check":
        name, bad = check(a[2])
        if bad:
            for b in bad:
                print(f"devguard: REFUSED: {b}")
            return 2
        print(name)
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
