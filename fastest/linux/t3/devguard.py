#!/usr/bin/env python3
"""devguard.py -- may a real T3 run destroy this device? An ALLOWLIST (gate-6 review M4: the old check was a basename
denylist, and a partition of the root disk or a symlink to /dev/ram0 passed it).

  devguard.py check DEVICE     exit 0 and print the resolved disk when every rule holds; else exit 2 with every reason
  devguard.py rootdisk         print the top-level disk(s) under "/", one per line, and exit 0; when it cannot tell,
                               exit 2 with the reason on stderr. t3run fires `check` on each disk it prints, and
                               requires exit 2 with the root rule's own text
  devguard.py self-test        the rules on synthetic lsblk trees; exit 0 iff every case passes

Rules (all must hold):
  - it is not a disk that holds "/", and it does not sit on one (T3 runner review MED 13). The walk starts at the one
    device lsblk shows mounted at "/". It climbs lsblk's PKNAME chain through partitions, LVM, dm and md (an md or dm
    device with several parents climbs all of them) to the top-level ancestors, and each one must be TYPE disk. So
    an LVM or md root names the disk(s) under it, not a partition, and a spare partition of a root disk is refused
    by this rule as well as by the allowlist. When the walk cannot tell, every device is refused. It cannot tell
    when nothing or several devices are at "/", when a top-level ancestor is not a disk, when lsblk prints no KNAME
    or PKNAME, or when a PKNAME is not the device lsblk nests the node under. This rule is evaluated first, so no
    rule that returns early can skip it;
  - DEVICE resolves (symlinks followed) to /dev/<name> with name a whole NVMe namespace (nvmeXnY), SCSI disk (sdX) or
    virtio disk (vdX); lsblk TYPE is "disk" (a partition, loop, ram, dm, md, nbd, pmem or anything else is refused);
  - no partition or holder of it is mounted or in use: lsblk MOUNTPOINTS anywhere in its tree; every descendant is a
    bare partition (an LVM volume, md array, crypt or dm child refuses); the disk's AND each partition's
    /sys/block/<disk>/<part>/holders are empty; and an exclusive open (O_EXCL) of the disk and of each partition
    succeeds, so nothing in the kernel claims them (lane review MED 5; needs root, so t3run runs it under sudo -n);
  - it is not a native-multipath NVMe head (/sys/block/<name>/multipath non-empty): its flush counter may live on the
    path devices, which V3L does not read (stated blind spot, refused rather than guessed).
Stated blind spot of the root rule: "/" is found only through lsblk's MOUNTPOINTS. A root that lsblk does not list
(overlay, NFS, tmpfs) cannot be told apart, so every device is refused and none passes.
"""
import json
import os
import re
import subprocess
import sys

ALLOWED = re.compile(r"^(nvme\d+n\d+|sd[a-z]+|vd[a-z]+)$")
# KNAME and PKNAME carry the walk (PKNAME names the parent's KERNEL name: dm-0, not the mapper name)
LSBLK = ["lsblk", "-J", "-o", "NAME,KNAME,PKNAME,TYPE,MOUNTPOINTS"]


class CannotTell(Exception):
    """the root walk cannot name the disk(s) under "/"; every caller refuses rather than guesses"""


def lsblk_text():
    try:
        out = subprocess.run(LSBLK, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise CannotTell(f"lsblk did not run: {e}")
    if out.returncode != 0:
        raise CannotTell(f"lsblk failed: {out.stderr.strip()[:200]}")
    return out.stdout


def parse_lsblk(text):
    try:
        doc = json.loads(text)
    except ValueError:
        raise CannotTell(f"lsblk output is not JSON: {text.strip()[:200]!r}")
    devices = doc.get("blockdevices") if isinstance(doc, dict) else None
    if not isinstance(devices, list):
        raise CannotTell("lsblk JSON has no blockdevices list")
    return devices


def lsblk_graph(devices):
    """(kname -> set of PKNAMEs, kname -> TYPE, knames mounted at "/") over every place lsblk's tree prints a node.
    An md or dm device with several parents is printed under each one, so it has several PKNAMEs. Raises CannotTell
    when a node lacks KNAME or PKNAME, or when its PKNAME is not the device it is nested under: the two would be
    two answers to the same question."""
    parents, types, at_root = {}, {}, set()

    def walk(node, under):
        if not isinstance(node, dict) or not node.get("kname") or "pkname" not in node:
            raise CannotTell(f"lsblk node {node.get('name') if isinstance(node, dict) else node!r} lacks KNAME or PKNAME")
        k = node["kname"]
        if node["pkname"] != under:
            raise CannotTell(f"{k} has PKNAME {node['pkname']!r} but sits under {under!r}")
        parents.setdefault(k, set())
        if under:
            parents[k].add(under)
        types[k] = node.get("type")
        if "/" in (node.get("mountpoints") or []):
            at_root.add(k)
        for c in node.get("children") or []:
            walk(c, k)

    for d in devices:
        walk(d, None)
    return parents, types, at_root


def top_ancestors(parents, kname):
    """the top-level ancestors of KNAME up the PKNAME chain: KNAME itself when it is top-level, none when lsblk
    does not list it"""
    seen, todo, tops = set(), [kname], set()
    while todo:
        k = todo.pop()
        if k in seen or k not in parents:
            continue
        seen.add(k)
        if parents[k]:
            todo += sorted(parents[k])
        else:
            tops.add(k)
    return tops


def root_disks(devices):
    """(the top-level disk(s) under "/", sorted; the device mounted at "/"; the PKNAME graph), or raise CannotTell"""
    parents, types, at_root = lsblk_graph(devices)
    if not at_root:
        raise CannotTell("no device in lsblk is mounted at /")
    if len(at_root) > 1:
        raise CannotTell(f"several devices are mounted at / ({sorted(at_root)})")
    src = next(iter(at_root))
    disks = sorted(top_ancestors(parents, src))
    if not disks:
        raise CannotTell(f"/ is on {src}, which has no top-level ancestor in lsblk")
    for d in disks:
        if types.get(d) != "disk":
            raise CannotTell(f"/ is on {src}, whose top-level ancestor {d} has TYPE {types.get(d)!r}, not disk")
    return disks, src, parents


def root_rule(name, devices):
    """the root rule's reasons for NAME: it is a disk that holds "/", or it sits on one (compared by disk)"""
    try:
        disks, src, parents = root_disks(devices)
    except CannotTell as e:
        return [f"cannot tell which disk holds / ({e}): refused rather than guessed"]
    if name in disks:
        return [f"{name} holds the root filesystem (/ is on {src}, walked up lsblk's PKNAME chain)"]
    # a top-level disk is its own top-level ancestor: excluding NAME keeps this branch from standing in for the one
    # above, so disabling that one is caught (MED 13's mutant, planted on this code)
    return [f"{name} is on {d}, which holds the root filesystem (/ is on {src})"
            for d in sorted((top_ancestors(parents, name) & set(disks)) - {name})]


def rootdisk_report(text=None):
    """the rootdisk subcommand on `lsblk -J` output TEXT (run lsblk when None; injectable so the self-test feeds
    fixtures): (0, [disk, ...]) or (2, [why it cannot tell])"""
    try:
        disks, _, _ = root_disks(parse_lsblk(lsblk_text() if text is None else text))
    except CannotTell as e:
        return 2, [f"cannot tell which disk holds /: {e}"]
    return 0, disks


def tree_mounts(node):
    m = [x for x in (node.get("mountpoints") or []) if x]
    for c in node.get("children") or []:
        m += tree_mounts(c)
    return m


def descendants(node):
    out = []
    for c in node.get("children") or []:
        out.append(c)
        out += descendants(c)
    return out


def problems(name, devices, holders, multipath, part_holders=None, busy=None):
    # the root rule first, so the early return below cannot skip it (MED 13: a partition under an LVM root was
    # refused only as "not top-level", and the root rule never ran)
    bad = root_rule(name, devices)
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
    return bad


def check(dev):
    real = os.path.realpath(dev)
    name = os.path.basename(real)
    if not real.startswith("/dev/") or not os.path.exists(real):
        return name, [f"{dev} does not resolve to a device node under /dev ({real})"]
    try:
        devices = parse_lsblk(lsblk_text())
    except CannotTell as e:
        return name, [str(e)]
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
    if a[1:] == ["rootdisk"]:
        rc, out = rootdisk_report()
        for line in out:
            print(line if rc == 0 else f"devguard: rootdisk: {line}", file=sys.stdout if rc == 0 else sys.stderr)
        return rc
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
