#!/usr/bin/env python3
"""devguard.py -- may a real T3 run destroy this device? An ALLOWLIST (gate-6 review M4: the old check was a basename
denylist, and a partition of the root disk or a symlink to /dev/ram0 passed it).

  devguard.py check DEVICE     exit 0 and print the resolved disk when every rule holds; else exit 2 with every reason
  devguard.py rootdisk         print the top-level disk(s) under "/", one per line, and exit 0; when it cannot tell,
                               exit 2 with the reason on stderr. t3run fires `check` on each disk it prints, and
                               requires exit 2 with the root, O_EXCL and signature rules' own texts for that disk
  devguard.py rootdisk-sysfs   the same answer from the kernel's sysfs links, with no lsblk: the second instrument
                               t3run requires `rootdisk` to equal. It is independent in how it LOCATES "/" ("/"'s
                               st_dev, not lsblk's mount table) and in its code, NOT in topology: lsblk builds its
                               tree from the same holders/slaves links the climb reads (round-2 attack MED 2)
  devguard.py self-test       the rules on synthetic lsblk trees; exit 0 iff every case passes

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
  - it shares no controller with a disk that holds "/": /sys/block/<name>/device resolves to a different NVMe
    controller or SCSI device than every root disk's does, so a second namespace of the root drive is refused (its
    queue and flush stream carry the root's writes; round-2 attack LOW 3). An unreadable link refuses;
  - it carries no signature at all: `blkid -p` (libblkid probing the device itself, not udev's database, which can lag
    a write) finds nothing on the disk or on any partition lsblk lists, so a partition table, a filesystem, an LVM
    PV, an md member, a LUKS header, an external journal or log, a btrfs or zfs member, swap: any of them refuses.
    This covers what neither the mount, holder nor O_EXCL rule sees: a PV of the root volume group holding no root
    extents, a detached LUKS header, a filesystem in fstab not mounted now (round-2 attack MED 2). A partition table
    alone refuses too, because a partition may hold raw data with no signature. A T3 disk must be blank: read what
    it holds, then `wipefs -a` it by hand. A failed probe refuses;
  - it is not a native-multipath NVMe head (/sys/block/<name>/multipath non-empty): its flush counter may live on the
    path devices, which V3L does not read (stated blind spot, refused rather than guessed).
Stated blind spots of the root rule. "/" is found only through lsblk's MOUNTPOINTS. A root that lsblk does not list
(overlay, NFS, tmpfs) cannot be told apart, so every device is refused and none passes. A btrfs "/" spread over
several disks shows "/" on one member only, so this rule does not refuse the other members. On a real box btrfs
claims every member exclusively, so the O_EXCL rule refuses them instead. sysfs cannot name any btrfs root (its
device number is anonymous), so t3run's fire refuses to run on such a box. Neither instrument names a disk that
carries part of "/" without a holder link: an XFS logdev= or rtdev=, an ext4 journal_dev, a DRBD backing disk, a
detached LUKS header, a PV of the root volume group with no root extents (round-2 attack MED 2). The kernel claims
the first three, so the O_EXCL rule refuses them; the signature rule refuses all five. A signature libblkid does not
know (a raw partition with no metadata, a disk holding only ciphertext) is not seen, and the partition-table rule
covers it only when the raw data sits in a partition.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

ALLOWED = re.compile(r"^(nvme\d+n\d+|sd[a-z]+|vd[a-z]+)$")
# an NVMe multipath path device nvme<subsystem>c<controller>n<namespace>; its head is nvme<subsystem>n<namespace>
NVME_PATH = re.compile(r"^nvme(\d+)c\d+n(\d+)$")
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
        if (not isinstance(node, dict) or not isinstance(node.get("kname"), str) or not node["kname"]
                or "pkname" not in node or not (node["pkname"] is None or isinstance(node["pkname"], str))):
            raise CannotTell(f"lsblk node {node.get('name') if isinstance(node, dict) else node!r} lacks KNAME or PKNAME")
        k = node["kname"]
        if node["pkname"] != under:
            raise CannotTell(f"{k} has PKNAME {node['pkname']!r} but sits under {under!r}")
        # shapes lsblk cannot print, refused rather than read (a string MOUNTPOINTS would match "/" as a substring)
        mounts, children = node.get("mountpoints"), node.get("children")
        if mounts is not None and not isinstance(mounts, list):
            raise CannotTell(f"{k} has MOUNTPOINTS {mounts!r}, not a list")
        if children is not None and not isinstance(children, list):
            raise CannotTell(f"{k} has children {children!r}, not a list")
        parents.setdefault(k, set())
        if under:
            parents[k].add(under)
        types[k] = node.get("type")
        if "/" in (mounts or []):
            at_root.add(k)
        for c in children or []:
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


def root_info(devices):
    """root_disks(devices), or the CannotTell it raised: computed once per check and handed to every rule that needs
    the root disks (review 5 MED 1: they were computed three times)"""
    try:
        return root_disks(devices)
    except CannotTell as e:
        return e


def root_rule(name, devices, info=None):
    """the root rule's reasons for NAME: it is a disk that holds "/", or it sits on one (compared by disk)"""
    info = root_info(devices) if info is None else info
    if isinstance(info, CannotTell):
        return [f"cannot tell which disk holds / ({info}): refused rather than guessed"]
    disks, src, parents = info
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


SYS_ROOT = "/sys"


def sysfs_root_disks(sys_root=None, majmin=None):
    """the top-level disk(s) under "/" from the kernel's sysfs links alone, with no lsblk. This is the second
    instrument t3run compares `rootdisk` against, because a test's expected disks must not come from the walk under
    test. It is independent of the walk in locating "/" and in code, not in topology: both read the kernel's
    holders/slaves graph, so a disk outside that graph is missed by both (see the module's blind spots). "/"'s
    device number (st_dev) names /sys/dev/block/MAJ:MIN. A partition climbs to the disk directory it sits in, and a
    device with slaves (md, dm) climbs every slave. An NVMe path device (nvmeXcYnZ) in a multipath head's slaves names
    its head nvmeXnZ, the name lsblk prints (round-2 attack LOW 2). Raises CannotTell when "/" has no block device:
    btrfs, overlay, NFS and tmpfs report an anonymous device number. SYS_ROOT and MAJMIN are injectable for the
    self-test."""
    sys_root = sys_root or SYS_ROOT
    if majmin is None:
        st = os.stat("/").st_dev
        majmin = f"{os.major(st)}:{os.minor(st)}"
    start = os.path.join(sys_root, "dev", "block", majmin)
    if not os.path.exists(start):
        raise CannotTell(f"/ is on device {majmin}, which has no block device in sysfs ({start})")
    disks, seen = set(), set()

    def climb(p):
        p = os.path.realpath(p)
        if p in seen:
            return
        seen.add(p)
        if os.path.isfile(os.path.join(p, "partition")):
            return climb(os.path.dirname(p))
        sl = os.path.join(p, "slaves")
        slaves = sorted(os.listdir(sl)) if os.path.isdir(sl) else []
        for s in slaves:
            climb(os.path.join(sl, s))
        if not slaves:
            path = NVME_PATH.match(os.path.basename(p))
            disks.add(f"nvme{path[1]}n{path[2]}" if path else os.path.basename(p))

    try:
        climb(start)
    except OSError as e:
        raise CannotTell(f"the sysfs walk from {start} failed: {e}")
    return sorted(disks)


def emit(rc, lines, cmd):
    """a rootdisk-style subcommand's output: the disks on stdout at rc 0, else the reason on stderr"""
    for line in lines:
        print(line if rc == 0 else f"devguard: {cmd}: {line}", file=sys.stdout if rc == 0 else sys.stderr)
    return rc


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


def problems(name, devices, holders, multipath, part_holders=None, busy=None, ctrl=None, sigs=None, info=None):
    """every reason NAME may not be destroyed. HOLDERS, MULTIPATH, PART_HOLDERS and BUSY are the sysfs and O_EXCL
    readings; CTRL maps disk -> realpath of /sys/block/<disk>/device; SIGS maps disk or partition -> blkid -p's
    (rc, stdout); INFO is root_info(devices), computed here when not given. check() always passes all of them; the
    self-test passes None to leave a rule out of a case"""
    info = root_info(devices) if info is None else info
    # the root rule first, so the early return below cannot skip it (MED 13: a partition under an LVM root was
    # refused only as "not top-level", and the root rule never ran)
    bad = root_rule(name, devices, info)
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
    if ctrl is not None:
        bad += controller_rule(name, info, ctrl)
    if sigs is not None:
        for dev in [name] + [c.get("name") for c in descendants(node)]:
            rc, out = sigs.get(dev, (None, "not probed"))
            found = " ".join((out or "").split())
            if rc == 0 and found:
                bad.append(f"{dev} {SIG_TEXT} ({found}): a T3 disk must be blank; read what it holds, then wipefs -a it")
            elif not (rc == 2 and not found):
                bad.append(f"cannot probe {dev}'s signatures (blkid -p rc {rc}: {found or 'no output'}): refused")
    if multipath:
        bad.append(f"{name} is a multipath NVMe head ({multipath}): its flush counter may be on the path devices")
    return bad


SIG_TEXT = "carries a signature"


def controller_rule(name, info, ctrl):
    """NAME shares an NVMe controller or SCSI device with a disk that holds "/" (round-2 attack LOW 3); INFO is
    root_info(devices); CTRL maps disk -> realpath of /sys/block/<disk>/device. When the root disks cannot be told,
    the root rule already refuses"""
    if isinstance(info, CannotTell):
        return []
    roots = info[0]
    bad = [f"cannot read {d}'s controller (/sys/block/{d}/device): refused rather than guessed"
           for d in sorted({name, *roots}) if not ctrl.get(d)]
    if ctrl.get(name):
        bad += [f"{name} shares its controller ({ctrl[name]}) with {r}, which holds the root filesystem"
                for r in roots if r != name and ctrl.get(r) == ctrl[name]]
    return bad


def blkid_probe(kname, dev_root="/dev"):
    """blkid's low-level probe of DEV_ROOT/KNAME, (rc, stdout): rc 2 with nothing printed is a blank device. A missing
    node, or anything on stderr, is a failed probe (rc None): blkid also exits 2 when it cannot open the device"""
    path = os.path.join(dev_root, kname or "")
    if not kname or not os.path.exists(path):
        return None, f"no device node {path}"
    try:
        r = subprocess.run(["blkid", "-p", "-o", "export", path], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        return None, str(e)
    if r.stderr.strip():
        return None, r.stderr.strip()[:200]
    return r.returncode, r.stdout


def excl_open(path):
    """an exclusive open of PATH, closed at once; OSError (EBUSY) when the kernel claims the device"""
    os.close(os.open(path, os.O_RDONLY | os.O_EXCL))


def check(dev, sys_root="/sys", dev_root="/dev", lsblk=None, blkid=None, excl=excl_open):
    """every rule on DEV, as `devguard.py check` runs it: (name, reasons). Every input is injectable so the self-test
    runs this allow path on a fake box before a paid one does (review 5 MED 1): SYS_ROOT and DEV_ROOT, LSBLK (lsblk's
    JSON text; None runs lsblk), BLKID (kname -> (rc, stdout); None probes DEV_ROOT/kname with blkid -p), EXCL (the
    O_EXCL opener)."""
    droot = os.path.realpath(dev_root)
    real = os.path.realpath(dev)
    name = os.path.basename(real)
    if not real.startswith(droot.rstrip("/") + "/") or not os.path.exists(real):
        return name, [f"{dev} does not resolve to a device node under {dev_root} ({real})"]
    try:
        devices = parse_lsblk(lsblk_text() if lsblk is None else lsblk)
    except CannotTell as e:
        return name, [str(e)]
    blk = os.path.join(sys_root, "block", name)
    hp = os.path.join(blk, "holders")
    holders = sorted(os.listdir(hp)) if os.path.isdir(hp) else []
    mp = os.path.join(blk, "multipath")
    multipath = sorted(os.listdir(mp)) if os.path.isdir(mp) else []
    node = next((d for d in devices if d.get("name") == name), {}) or {}
    parts = [c.get("name") for c in descendants(node) if c.get("type") == "part"]
    part_holders = {}
    for p in parts:
        ph = os.path.join(blk, p, "holders")
        part_holders[p] = sorted(os.listdir(ph)) if os.path.isdir(ph) else []
    busy = {}
    for d in [name] + parts:
        try:
            excl(os.path.join(droot, d))
        except OSError as e:
            busy[d] = e.strerror
    info = root_info(devices)  # once, for the root rule, the controller rule and the links below
    roots = [] if isinstance(info, CannotTell) else info[0]
    ctrl = {}
    for d in [name] + roots:
        link = os.path.join(sys_root, "block", d, "device")
        if os.path.exists(link):
            ctrl[d] = os.path.realpath(link)
    # every descendant lsblk lists, partition or not, by its kernel name: the rule refuses any it could not probe
    probe = blkid or (lambda k: blkid_probe(k, droot))
    sigs = {name: probe(name)}
    sigs.update({c.get("name"): probe(c.get("kname")) for c in descendants(node)})
    return name, problems(name, devices, holders, multipath, part_holders, busy, ctrl, sigs, info)


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
    # round-2 attack LOW 1: a partitioned md (/ on md127p1, md127 over a partition of each of two disks), nested dm
    # (/ on dm-crypt dm-1 over LVM dm-0 over a partition) and md over two WHOLE disks (members with no partition)
    md_part = [N(d, "disk", children=[N(d + "p2", "part", d, children=[
                   N("md127", "raid1", d + "p2", children=[N("md127p1", "part", "md127", ["/"])])])])
               for d in ("nvme0n1", "nvme1n1")]
    crypt_lvm = [N("nvme0n1", "disk", children=[N("nvme0n1p3", "part", "nvme0n1", children=[
                     N("vg-root", "lvm", "nvme0n1p3", kname="dm-0", children=[
                         N("cryptroot", "crypt", "dm-0", ["/"], kname="dm-1")])])]),
                 N("nvme1n1", "disk")]
    md_whole = [N(d, "disk", children=[N("md0", "raid1", d, ["/"])]) for d in ("nvme0n1", "nvme1n1")]
    # round-2 attack MED 2: a spare disk that is not blank (a PV of the root volume group with no root extents, an
    # unmounted btrfs member, any partition table) is neither mounted nor claimed, so only its signature tells: what
    # `blkid -p -o export /dev/<dev>` answers for the disk and each partition, as (rc, stdout)
    blank = {"nvme1n1": (2, "")}
    parted = N("nvme1n1", "disk", children=[N("nvme1n1p1", "part", "nvme1n1")])
    # round-2 attack LOW 3: two namespaces of one controller; the controller each /sys/block/<disk>/device names
    ns2 = N("nvme0n2", "disk")
    c0, c1 = "/sys/devices/pci0000:00/0000:00:01.0/nvme/nvme0", "/sys/devices/pci0000:00/0000:00:02.0/nvme/nvme1"
    ROOT = "holds the root filesystem"
    SIG = "carries a signature"
    CTRL = "shares its controller"
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

    tmp = tempfile.mkdtemp(prefix="devguard-selftest-")

    def fake_sysfs(tree, root_dev):
        """a fake /sys: TREE maps a device dir (under devices/) to "disk", "part" (it gets a partition file) or the
        list of its slaves' dirs; dev/block/1:0 links to ROOT_DEV (None: "/" has no block device)"""
        top = tempfile.mkdtemp(dir=tmp)
        for d in sorted(tree):
            os.makedirs(os.path.join(top, "devices", d, "slaves"))
            if tree[d] == "part":
                with open(os.path.join(top, "devices", d, "partition"), "w") as f:
                    f.write("1\n")
            elif isinstance(tree[d], list):
                for s in tree[d]:
                    os.symlink(os.path.join(top, "devices", s),
                               os.path.join(top, "devices", d, "slaves", os.path.basename(s)))
        os.makedirs(os.path.join(top, "dev", "block"))
        if root_dev:
            os.symlink(os.path.join(top, "devices", root_dev), os.path.join(top, "dev", "block", "1:0"))
        return top

    def sysfs(want, tree, root_dev):
        got = sysfs_root_disks(fake_sysfs(tree, root_dev), "1:0")
        return got == want, got

    def sysfs_refuses(text, tree, root_dev):
        try:
            got = sysfs_root_disks(fake_sysfs(tree, root_dev), "1:0")
        except CannotTell as e:
            return text in str(e), f"CannotTell: {e}"
        return False, got

    def cli_rootdisk(devices):
        """the real `devguard.py rootdisk` command line, with a fake lsblk first on PATH printing DEVICES"""
        d = tempfile.mkdtemp(dir=tmp)
        fx = os.path.join(d, "lsblk.json")
        with open(fx, "w") as f:
            json.dump({"blockdevices": devices}, f)
        with open(os.path.join(d, "lsblk"), "w") as f:
            f.write(f"#!/bin/sh\ncat '{fx}'\n")
        os.chmod(os.path.join(d, "lsblk"), 0o755)
        r = subprocess.run([sys.executable, "-B", os.path.abspath(__file__), "rootdisk"], capture_output=True,
                           text=True, timeout=60, env=dict(os.environ, PATH=d + os.pathsep + os.environ.get("PATH", "")))
        return r.returncode, r.stdout, r.stderr

    def fake_box():
        """a whole fake box for check() (review 5 MED 1): /dev nodes, sysfs block dirs whose device links resolve to two
        controllers (nvme0 carries the root disk nvme0n1 and a second namespace nvme0n2; nvme1 carries the spare
        nvme1n1), lsblk's JSON, blkid's answers and an O_EXCL opener that finds the root disk claimed"""
        top = tempfile.mkdtemp(dir=tmp)
        droot, sroot = os.path.join(top, "dev"), os.path.join(top, "sys")
        os.makedirs(droot)
        for d in ("nvme0n1", "nvme0n1p1", "nvme0n2", "nvme1n1"):
            open(os.path.join(droot, d), "w").close()
        os.symlink(os.path.join(droot, "nvme1n1"), os.path.join(droot, "by-id-spare"))
        for c in ("nvme0", "nvme1"):
            os.makedirs(os.path.join(sroot, "devices", "pci", c))
        for d, c in (("nvme0n1", "nvme0"), ("nvme0n2", "nvme0"), ("nvme1n1", "nvme1")):
            os.makedirs(os.path.join(sroot, "block", d, "holders"))
            os.symlink(os.path.join("..", "..", "devices", "pci", c), os.path.join(sroot, "block", d, "device"))
        os.makedirs(os.path.join(sroot, "block", "nvme0n1", "nvme0n1p1", "holders"))
        lsblk = json.dumps({"blockdevices": [N("nvme0n1", "disk", children=[N("nvme0n1p1", "part", "nvme0n1", ["/"])]),
                                             N("nvme0n2", "disk"), N("nvme1n1", "disk")]})
        answers = {"nvme0n1": (0, "DEVNAME=/dev/nvme0n1\nPTTYPE=gpt\n"), "nvme0n1p1": (0, "TYPE=ext4\n")}

        def excl(path):
            if os.path.basename(path) in ("nvme0n1", "nvme0n1p1"):
                raise OSError(16, "Device or resource busy")

        def run(dev):
            return check(os.path.join(droot, dev) if not os.path.isabs(dev) else dev, sys_root=sroot, dev_root=droot,
                         lsblk=lsblk, blkid=lambda k: answers.get(k, (2, "")), excl=excl)
        return run

    box = fake_box()
    pci = {"pci/nvme0n1": "disk", "pci/nvme0n1/nvme0n1p1": "part", "pci/nvme0n1/nvme0n1p2": "part",
           "pci/nvme0n1/nvme0n1p3": "part", "pci/nvme1n1": "disk", "pci/nvme1n1/nvme1n1p2": "part", "pci/nvme2n1": "disk"}
    # round-2 attack LOW 2: an NVMe multipath head whose slaves are its path devices (the 4.15-era layout)
    mpath = {"virtual/nvme-subsys0/nvme0n1": ["pci/nvme0/nvme0c0n1", "pci/nvme0/nvme0c1n1"],
             "virtual/nvme-subsys0/nvme0n1/nvme0n1p1": "part", "pci/nvme0/nvme0c0n1": "disk",
             "pci/nvme0/nvme0c1n1": "disk"}
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
        ("rootdisk: MOUNTPOINTS that is not a list cannot tell (a string would match / as a substring)",
         lambda: rootdisk_refuses("MOUNTPOINTS '/srv', not a list", [N("nvme0n1", "disk", children=[
             dict(N("nvme0n1p1", "part", "nvme0n1"), mountpoints="/srv")])])),
        ("rootdisk: children that is not a list cannot tell",
         lambda: rootdisk_refuses("children 5, not a list", [dict(N("nvme0n1", "disk", mounts=["/"]), children=5)])),
        ("rootdisk: a KNAME that is not a string cannot tell",
         lambda: rootdisk_refuses("lacks KNAME or PKNAME", [N("nvme0n1", "disk", mounts=["/"], kname=["nvme0n1"])])),
        ("rootdisk CLI: an md root prints both disks on stdout, nothing on stderr, exit 0",
         lambda: (lambda r: (r == (0, "nvme0n1\nnvme1n1\n", ""), r))(cli_rootdisk(md))),
        ("rootdisk CLI: nothing at / exits 2 with the reason on stderr and nothing on stdout",
         lambda: (lambda r: (r[0] == 2 and r[1] == "" and TELL in r[2], r))(cli_rootdisk([spare]))),
        # the second instrument t3run compares rootdisk against: the kernel's sysfs links, no lsblk
        ("sysfs: / on a plain partition climbs to its disk", lambda: sysfs(["nvme0n1"], pci, "pci/nvme0n1/nvme0n1p1")),
        ("sysfs: / on LVM over a partition climbs dm-0's slave, then the partition, to the disk",
         lambda: sysfs(["nvme0n1"], dict(pci, **{"virtual/dm-0": ["pci/nvme0n1/nvme0n1p2"]}), "virtual/dm-0")),
        ("sysfs: / on md RAID1 over two partitions climbs both slaves to both disks",
         lambda: sysfs(["nvme0n1", "nvme1n1"],
                       dict(pci, **{"virtual/md0": ["pci/nvme0n1/nvme0n1p2", "pci/nvme1n1/nvme1n1p2"]}), "virtual/md0")),
        ("sysfs: / on a whole disk is that disk", lambda: sysfs(["nvme2n1"], pci, "pci/nvme2n1")),
        ("sysfs: / with no block device (btrfs, overlay, NFS, tmpfs) cannot tell",
         lambda: sysfs_refuses("has no block device in sysfs", pci, None)),
        # round-2 attack LOW 1: the three layouts the fakes missed, through both instruments
        ("rootdisk: / on a partitioned md walks md127p1, md127, both member partitions, to both disks",
         lambda: rootdisk(["nvme0n1", "nvme1n1"], md_part)),
        ("rootdisk: / on dm-crypt over LVM over a partition walks dm-1, dm-0, the partition, the disk",
         lambda: rootdisk(["nvme0n1"], crypt_lvm)),
        ("rootdisk: / on md over two whole disks walks to both disks", lambda: rootdisk(["nvme0n1", "nvme1n1"], md_whole)),
        ("sysfs: / on a partitioned md climbs md127p1 to md127, then both slaves to both disks",
         lambda: sysfs(["nvme0n1", "nvme1n1"],
                       dict(pci, **{"virtual/md127": ["pci/nvme0n1/nvme0n1p2", "pci/nvme1n1/nvme1n1p2"],
                                    "virtual/md127/md127p1": "part"}), "virtual/md127/md127p1")),
        ("sysfs: / on dm-crypt over LVM climbs dm-1's slave dm-0, then dm-0's slave, to the disk",
         lambda: sysfs(["nvme0n1"], dict(pci, **{"virtual/dm-0": ["pci/nvme0n1/nvme0n1p3"],
                                                 "virtual/dm-1": ["virtual/dm-0"]}), "virtual/dm-1")),
        ("sysfs: / on md over two whole disks climbs both slaves, which are the disks",
         lambda: sysfs(["nvme0n1", "nvme1n1"], dict(pci, **{"virtual/md0": ["pci/nvme0n1", "pci/nvme1n1"]}),
                       "virtual/md0")),
        # round-2 attack LOW 2: path devices in a head's slaves name the head, as lsblk does
        ("sysfs: / on an NVMe multipath head whose slaves are its path devices names the head",
         lambda: sysfs(["nvme0n1"], mpath, "virtual/nvme-subsys0/nvme0n1/nvme0n1p1")),
        # round-2 attack MED 2: the signature rule, on blkid's low-level probe (libblkid on the device itself, not
        # udev's database, which can lag a write) of the disk and each partition
        ("MED 2: a spare disk with a partition table is refused by the signature rule (a partition may hold raw data)",
         lambda: refuses(f"nvme1n1 {SIG} (DEVNAME=/dev/nvme1n1 PTUUID=5e1f PTTYPE=gpt)", "nvme1n1", devs, [], [], {}, {},
                         None, {"nvme1n1": (0, "DEVNAME=/dev/nvme1n1\nPTUUID=5e1f\nPTTYPE=gpt\n")})),
        ("MED 2: a whole-disk PV of the root volume group holding no root extents is refused by the signature rule",
         lambda: refuses(f"nvme1n1 {SIG} (DEVNAME=/dev/nvme1n1 TYPE=LVM2_member)", "nvme1n1", devs, [], [], {}, {},
                         None, {"nvme1n1": (0, "DEVNAME=/dev/nvme1n1\nTYPE=LVM2_member\n")})),
        ("MED 2: an unmounted whole-disk btrfs member is refused by the signature rule",
         lambda: refuses(f"nvme1n1 {SIG} (DEVNAME=/dev/nvme1n1 TYPE=btrfs)", "nvme1n1", devs, [], [], {}, {},
                         None, {"nvme1n1": (0, "DEVNAME=/dev/nvme1n1\nTYPE=btrfs\n")})),
        ("MED 2: a partition carrying an LVM2_member signature is refused by the signature rule",
         lambda: refuses(f"nvme1n1p1 {SIG} (DEVNAME=/dev/nvme1n1p1 TYPE=LVM2_member)", "nvme1n1",
                         [root, parted], [], [], {}, {}, None,
                         {"nvme1n1": (0, "DEVNAME=/dev/nvme1n1\nPTTYPE=gpt\n"),
                          "nvme1n1p1": (0, "DEVNAME=/dev/nvme1n1p1\nTYPE=LVM2_member\n")})),
        ("MED 2: a partition lsblk lists that was not probed refuses", lambda: refuses(
            "cannot probe nvme1n1p1's signatures", "nvme1n1", [root, parted], [], [], {}, {}, None, blank)),
        ("MED 2: a probe that failed (blkid rc 4) refuses", lambda: refuses(
            "cannot probe nvme1n1's signatures", "nvme1n1", devs, [], [], {}, {}, None, {"nvme1n1": (4, "")})),
        ("MED 2: a probe that did not run refuses", lambda: refuses(
            "cannot probe nvme1n1's signatures", "nvme1n1", devs, [], [], {}, {}, None, {"nvme1n1": (None, "timeout")})),
        ("MED 2: no probe of the device at all refuses", lambda: refuses(
            "cannot probe nvme1n1's signatures", "nvme1n1", devs, [], [], {}, {}, None, {})),
        ("MED 2: 'nothing found' (rc 2) that still prints something refuses", lambda: refuses(
            "cannot probe nvme1n1's signatures", "nvme1n1", devs, [], [], {}, {}, None,
            {"nvme1n1": (2, "DEVNAME=/dev/nvme1n1\nTYPE=xfs\n")})),
        ("MED 2: a blank spare disk (rc 2, nothing printed) passes the signature rule",
         lambda: passes("nvme1n1", devs, [], [], {}, {}, None, blank)),
        # round-2 attack LOW 3: the root rule compares drives, not only namespaces
        ("LOW 3: another namespace on the root disk's controller is refused by the controller rule",
         lambda: refuses(f"nvme0n2 {CTRL} ({c0}) with nvme0n1, which {ROOT}", "nvme0n2", devs + [ns2], [], [], {}, {},
                         {"nvme0n1": c0, "nvme0n2": c0})),
        ("LOW 3: a disk on another controller passes the controller rule", lambda: passes(
            "nvme1n1", devs, [], [], {}, {}, {"nvme0n1": c0, "nvme1n1": c1})),
        ("LOW 3: a device whose controller cannot be read is refused", lambda: refuses(
            "cannot read nvme1n1's controller", "nvme1n1", devs, [], [], {}, {}, {"nvme0n1": c0})),
        ("LOW 3: a root disk whose controller cannot be read refuses every device", lambda: refuses(
            "cannot read nvme0n1's controller", "nvme1n1", devs, [], [], {}, {}, {"nvme1n1": c1})),
        # review 5 MED 1: check() itself, on a whole fake box, so its allow path runs before a paid one does
        ("MED 1: check() allows a blank spare on another controller (no reason at all)",
         lambda: (lambda r: (r == ("nvme1n1", []), r))(box("nvme1n1"))),
        ("MED 1: check() follows a symlink under the dev root to the spare and allows it",
         lambda: (lambda r: (r == ("nvme1n1", []), r))(box("by-id-spare"))),
        ("MED 1: check() refuses nvme0n2, a second namespace on the root drive's controller, by the controller rule",
         lambda: (lambda r: (any("nvme0n2 shares its controller" in b for b in r[1]), r))(box("nvme0n2"))),
        ("MED 1: check() on the root disk gives the root, O_EXCL and signature texts and no 'cannot read' line",
         lambda: (lambda r: (all(any(t in b for b in r[1]) for t in (f"nvme0n1 {ROOT}", "nvme0n1 cannot be opened "
                                                                     "exclusively", f"nvme0n1 {SIG}"))
                             and not any("cannot read" in b for b in r[1]), r))(box("nvme0n1"))),
        ("MED 1: check() refuses a path that does not resolve under the dev root",
         lambda: (lambda r: (any("does not resolve to a device node under" in b for b in r[1]), r))(box("/etc/hosts"))),
    ]
    bad = []
    try:
        for n, fn in cases:
            try:
                ok, got = fn()
            except Exception as e:  # a case that raises is a FAIL, never a crash of the whole self-test
                ok, got = False, f"{type(e).__name__}: {e}"
            print(f"DEVGUARD self-test {'PASS' if ok else 'FAIL'}: {n}" + ("" if ok else f" (got {got})"))
            if not ok:
                bad.append(n)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"DEVGUARD SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    if a[1:] == ["self-test"]:
        return self_test()
    if a[1:] == ["rootdisk"]:
        return emit(*rootdisk_report(), "rootdisk")
    if a[1:] == ["rootdisk-sysfs"]:
        try:
            return emit(0, sysfs_root_disks(), "rootdisk-sysfs")
        except CannotTell as e:
            return emit(2, [f"cannot tell which disk holds / from sysfs: {e}"], "rootdisk-sysfs")
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
