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


def _node(name, typ, pkname=None, mounts=None, children=None, kname=None):
    """one node as `lsblk -J -o NAME,KNAME,PKNAME,TYPE,MOUNTPOINTS` prints it (a "children" key only when it has some)"""
    n = {"name": name, "kname": kname or name, "pkname": pkname, "type": typ, "mountpoints": mounts or [None]}
    if children:
        n["children"] = children
    return n


def self_test():
    """Every refusal case matches its OWN rule's text: another rule refusing the same device does not count (T3 runner
    review MED 13: the mount rule refused the root disk first, so `elif False:` on the root rule survived)."""
    N = _node
    # box A: / on a plain partition
    root = N("nvme0n1", "disk", children=[N("nvme0n1p1", "part", "nvme0n1", ["/"]), N("nvme0n1p3", "part", "nvme0n1")])
    spare = N("nvme1n1", "disk")
    mounted = N("sdb", "disk", children=[N("sdb1", "part", "sdb", ["/mnt"])])
    devs = [root, spare, mounted, N("ram0", "disk"), N("loop3", "loop"), N("sdc", "rom")]
    lvm_spare = N("nvme2n1", "disk", children=[N("nvme2n1p1", "part", "nvme2n1", children=[
        N("vg-lv", "lvm", "nvme2n1p1", kname="dm-1")])])
    # box B: / on an LVM volume over a partition (Ubuntu server's default); its NAME is the mapper name, its KNAME dm-0
    lvm = [N("nvme0n1", "disk", children=[
               N("nvme0n1p1", "part", "nvme0n1", ["/boot/efi"]),
               N("nvme0n1p2", "part", "nvme0n1", children=[
                   N("ubuntu--vg-ubuntu--lv", "lvm", "nvme0n1p2", ["/"], kname="dm-0")])]),
           N("nvme1n1", "disk")]
    # box C: / on md RAID1 over a partition of each of two disks (a common rental default); lsblk prints md0 under both
    md = [N(d, "disk", children=[N(d + "p1", "part", d, ["/boot/efi"] if d == "nvme0n1" else None),
                                 N(d + "p2", "part", d, children=[N("md0", "raid1", d + "p2", ["/"])])])
          for d in ("nvme0n1", "nvme1n1")] + [N("nvme2n1", "disk")]
    # box D: / on a whole disk with no partition table
    whole = [N("vda", "disk", mounts=["/"]), N("vdb", "disk")]
    # lsblk run without KNAME and PKNAME (the columns the walk needs)
    bare = [{"name": "nvme0n1", "type": "disk", "mountpoints": [None],
             "children": [{"name": "nvme0n1p1", "type": "part", "mountpoints": ["/"]}]},
            {"name": "nvme1n1", "type": "disk", "mountpoints": [None]}]
    ROOT = "holds the root filesystem"
    ALLOW = "is not an NVMe namespace, SCSI disk or virtio disk"
    TELL = "cannot tell which disk holds /"

    def refuses(text, name, devices, *rest):
        got = problems(name, devices, *rest)
        return any(text in b for b in got), got

    def passes(name, devices, *rest):
        got = problems(name, devices, *rest)
        return got == [], got

    def lsblk_text(devices):
        return devices if isinstance(devices, str) else json.dumps({"blockdevices": devices})

    def rootdisk(want, devices):
        got = rootdisk_report(lsblk_text(devices))
        return got == (0, want), got

    def rootdisk_refuses(text, devices):
        rc, out = rootdisk_report(lsblk_text(devices))
        return rc != 0 and any(text in o for o in out), (rc, out)

    cases = [
        ("a spare NVMe disk passes", lambda: passes("nvme1n1", devs, [], [])),
        ("the root disk is refused by the root rule", lambda: refuses(f"nvme0n1 {ROOT}", "nvme0n1", devs, [], [])),
        ("a spare partition of the root disk is refused by the allowlist (M4)",
         lambda: refuses(f"'nvme0n1p3' {ALLOW}", "nvme0n1p3", devs, [], [])),
        ("a spare partition of the root disk is refused by the root rule too (compared by disk)",
         lambda: refuses(f"nvme0n1p3 is on nvme0n1, which {ROOT}", "nvme0n1p3", devs, [], [])),
        ("a device lsblk does not list is refused",
         lambda: refuses("'nvme9n1' is not a top-level block device", "nvme9n1", devs, [], [])),
        ("a disk with a mounted partition is refused by the mount rule",
         lambda: refuses("sdb or a partition is mounted at ['/mnt']", "sdb", devs, [], [])),
        ("ram0 is refused by the allowlist", lambda: refuses(f"'ram0' {ALLOW}", "ram0", devs, [], [])),
        ("a loop is refused by the allowlist", lambda: refuses(f"'loop3' {ALLOW}", "loop3", devs, [], [])),
        ("a top-level device not of TYPE disk is refused",
         lambda: refuses("sdc has lsblk TYPE 'rom', not disk", "sdc", devs, [], [])),
        ("a held disk (dm/md) is refused", lambda: refuses("nvme1n1 is held by ['dm-0'] (dm/md)", "nvme1n1", devs,
                                                           ["dm-0"], [])),
        ("a multipath NVMe head is refused",
         lambda: refuses("nvme1n1 is a multipath NVMe head", "nvme1n1", devs, [], ["nvme1c1n1"])),
        ("no disk holding / refuses every device", lambda: refuses(TELL, "nvme1n1", [spare], [], [])),
        ("lsblk without KNAME/PKNAME refuses every device", lambda: refuses(TELL, "nvme1n1", bare, [], [])),
        ("MED 5: a partition carrying an unmounted LVM volume is refused",
         lambda: refuses("nvme2n1 carries vg-lv of TYPE 'lvm'", "nvme2n1", devs + [lvm_spare], [], [])),
        ("MED 5: a partition held by an md array is refused",
         lambda: refuses("partition nvme1n1p1 of nvme1n1 is held by ['md0']", "nvme1n1", devs, [], [],
                         {"nvme1n1p1": ["md0"]})),
        ("MED 5: a device the kernel claims (EBUSY on O_EXCL) is refused",
         lambda: refuses("nvme1n1 cannot be opened exclusively", "nvme1n1", devs, [], [], {},
                         {"nvme1n1": "Device or resource busy"})),
        ("LVM root: the disk under it is refused by the root rule",
         lambda: refuses(f"nvme0n1 {ROOT}", "nvme0n1", lvm, [], [])),
        ("LVM root: the partition under it (the old fire's PKNAME target) is refused by the root rule",
         lambda: refuses(f"nvme0n1p2 is on nvme0n1, which {ROOT}", "nvme0n1p2", lvm, [], [])),
        ("LVM root: a spare disk passes", lambda: passes("nvme1n1", lvm, [], [])),
        ("md RAID1 root: the first member disk is refused by the root rule",
         lambda: refuses(f"nvme0n1 {ROOT}", "nvme0n1", md, [], [])),
        ("md RAID1 root: the second member disk is refused by the root rule",
         lambda: refuses(f"nvme1n1 {ROOT}", "nvme1n1", md, [], [])),
        ("md RAID1 root: a spare disk passes", lambda: passes("nvme2n1", md, [], [])),
        ("whole-disk root: the disk is refused by the root rule", lambda: refuses(f"vda {ROOT}", "vda", whole, [], [])),
        ("rootdisk: / on a plain partition walks to its disk", lambda: rootdisk(["nvme0n1"], devs)),
        ("rootdisk: / on LVM over a partition walks dm-0, the partition, the disk", lambda: rootdisk(["nvme0n1"], lvm)),
        ("rootdisk: / on md RAID1 over two partitions walks to both disks",
         lambda: rootdisk(["nvme0n1", "nvme1n1"], md)),
        ("rootdisk: / on a whole disk is that disk", lambda: rootdisk(["vda"], whole)),
        ("rootdisk: nothing mounted at / cannot tell",
         lambda: rootdisk_refuses("no device in lsblk is mounted at /", [spare])),
        ("rootdisk: two devices mounted at / cannot tell",
         lambda: rootdisk_refuses("several devices are mounted at /", [N("sda", "disk", mounts=["/"]),
                                                                       N("sdb", "disk", mounts=["/"])])),
        ("rootdisk: / on a loop (top-level ancestor not a disk) cannot tell",
         lambda: rootdisk_refuses("has TYPE 'loop', not disk", [N("loop0", "loop", mounts=["/"])])),
        ("rootdisk: lsblk without KNAME/PKNAME cannot tell", lambda: rootdisk_refuses("lacks KNAME or PKNAME", bare)),
        ("rootdisk: a PKNAME that is not the device lsblk nests it under cannot tell",
         lambda: rootdisk_refuses("but sits under", [N("nvme0n1", "disk", children=[
             N("nvme0n1p1", "part", "nvme9n1", ["/"])])])),
        ("rootdisk: lsblk output that is not JSON cannot tell",
         lambda: rootdisk_refuses("is not JSON", "lsblk: unknown column: MOUNTPOINTS")),
        ("rootdisk: JSON with no blockdevices list cannot tell", lambda: rootdisk_refuses("no blockdevices list", "{}")),
    ]
    bad = []
    for n, fn in cases:
        try:
            ok, got = fn()
        except Exception as e:  # a case that raises is a FAIL, never a crash of the whole self-test
            ok, got = False, f"{type(e).__name__}: {e}"
        print(f"DEVGUARD self-test {'PASS' if ok else 'FAIL'}: {n}" + ("" if ok else f" (got {got})"))
        if not ok:
            bad.append(n)
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
