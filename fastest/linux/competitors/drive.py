#!/usr/bin/env python3
"""drive.py -- the drive class under a run's filesystem (lane fastest-linux-comp; lead ruling, artie DECISIONS
6b0bef481b, SMOKE erratum E3: 5 of 20 GitHub jobs of run 37809124979 ran on a write-back NVMe with FUA, the rest on a
write-through virtual disk, and only a per-job record says which).

The class is read from the device that actually takes the run's writes, never from the machine's device list: MNT's
device (os.stat st_dev; for a filesystem with an anonymous device, btrfs, its mount source via findmnt); a loop device
is followed to its backing file (sysfs loop/backing_file) and that file's own device, a partition to its disk, and a
device-mapper device to every slave. Each leaf disk's sysfs queue/write_cache and queue/fua give its class:
  write-through        write_cache "write through" (a flush has nothing to drain)
  write-back+fua       write_cache "write back", fua 1
  write-back-no-fua    write_cache "write back", fua 0
  ram-volatile         a RAM disk (ram<N> from brd, zram<N>), whatever its write_cache says: nothing survives power
Anything else, leaves of different classes, a chain deeper than 8, or any step that cannot be read is REFUSED: no
class is guessed.

Whether a flush REACHES that drive is recorded beside it (review of e11a3c993, finding 3): a loop device turns a flush
into an fsync of its backing file, and a filesystem mounted nobarrier (or barrier=0) on the way sends neither a flush
nor FUA down. "mounts" lists the filesystem of MNT and of every backing file (findmnt TARGET, SOURCE, FSTYPE,
OPTIONS). Comp review 1 LOW 13 (v3floor.c's walk and cross-check): every loop and dm layer's own write_cache is read
("layers"; a write-through layer strips the flush), and each leaf's write_cache is checked against the drive's own
cache report (NVMe Identify Controller VWC through the admin ioctl, sd's cache_type, virtio_blk's cache_type;
disks[].drive_report): a disagreement is REFUSED (one of them was overridden). flush_reaches_drive is one of FLUSH:
"yes", "no" (barriers off, or a write-through layer), "n/a: write-through" (a write-through leaf is sent no flush),
"n/a: volatile RAM", or "unknown" (the drive's report was not read), with flush_why.

  drive.py record MNT OUT.json  OUT.json = {"mnt", "chain", "disks", "drive_class", "layers", "mounts",
                                "flush_reaches_drive", "flush_why"}; prints one line "drive_class=C
                                flush_reaches_drive=F disks=NAME(model)[,...] chain=A>B... [barrier_off=...]" (F
                                without spaces); exit 2 (no OUT) when refused
  drive.py selftest             known-answer fixtures over a fake sysfs tree; exit 0 only if every verdict is as expected
"""
import json
import os
import re
import stat
import subprocess
import sys
import tempfile

MAX_DEPTH = 8
CLASSES = ("write-through", "write-back+fua", "write-back-no-fua", "ram-volatile")
RAM_DISK = re.compile(r"^(ram|zram)\d+$")


class Undetermined(Exception):
    pass


class Unread(Exception):
    """The drive's own cache report could not be read (comp review 1 LOW 13): the flush is then 'unknown'."""


# comp review 1 LOW 13: whether a flush reaches the drive. "n/a: write-through": the kernel sends a write-through leaf
# no flush (it has nothing to drain); "unknown": the drive's own cache report was not read, so the kernel's
# write_cache (and so the class) may be an override hiding a volatile cache
FLUSH = ("yes", "no", "n/a: write-through", "n/a: volatile RAM", "unknown")
NVME_IOCTL_ADMIN_CMD = 0xC0484E41  # _IOWR('N', 0x41, struct nvme_admin_cmd), 72 bytes


def nvme_report(d):
    """An NVMe leaf's own cache report, as v3floor.c's nvme_vwc reads it (fastest/linux/v3/v3floor.c, ported):
    Identify Controller (opcode 06h, CNS 01h) through NVME_IOCTL_ADMIN_CMD on every controller of the namespace
    (/dev/nvmeN; a multipath subsystem's every path), each over a pcie transport, VWC bit 0 = a volatile write cache is
    PRESENT. Returns ("write back" | "write through", source); Unread when it cannot be read (the node unreadable, a
    fabrics or loopback transport); Undetermined when the controllers disagree. Blind spot, as v3floor's: whether the
    cache is ENABLED is Get Features 06h, which needs CAP_SYS_ADMIN."""
    import ctypes
    import fcntl
    import struct
    r = os.path.realpath(os.path.join(d, "device"))
    b = os.path.basename(r)
    if re.fullmatch(r"nvme\d+", b):
        ctrls = [b]
    elif b.startswith("nvme-subsys"):
        ctrls = sorted(e for e in os.listdir(r) if re.fullmatch(r"nvme\d+", e))
    else:
        ctrls = []
    if not ctrls:
        raise Unread(f"cannot map {os.path.basename(d)} to an NVMe controller ({r})")
    vwc = set()
    for c in ctrls:
        tr = read(f"/sys/class/nvme/{c}/transport")
        if tr != "pcie":
            raise Unread(f"NVMe controller {c} has transport {tr!r}, not pcie: not a local drive")
        node = f"/dev/{c}"
        try:
            fd = os.open(node, os.O_RDONLY | os.O_CLOEXEC)
        except OSError as e:
            raise Unread(f"open {node}: {e.strerror} (Identify Controller is unprivileged once the node is readable)")
        try:
            buf = ctypes.create_string_buffer(4096)
            cmd = bytearray(struct.pack("=BBHIIIQQII6III", 0x06, 0, 0, 0, 0, 0, 0, ctypes.addressof(buf), 0, 4096,
                                        1, 0, 0, 0, 0, 0, 0, 0))
            rc = fcntl.ioctl(fd, NVME_IOCTL_ADMIN_CMD, cmd, True)
        except OSError as e:
            raise Unread(f"Identify Controller on {node}: {e.strerror}")
        finally:
            os.close(fd)
        if rc != 0:
            raise Unread(f"Identify Controller on {node}: NVMe status {rc}")
        vwc.add(buf.raw[525] & 1)
    if len(vwc) != 1:
        raise Undetermined(f"the controllers of {os.path.basename(d)} disagree on VWC")
    return ("write back" if vwc.pop() else "write through"), f"NVMe Identify Controller VWC bit 0 via /dev/{ctrls[0]}"


NVME_REPORT = nvme_report  # (the selftest puts fake_nvme here: no ioctl off a real NVMe)


def drive_report(d, name):
    """A leaf disk's own cache report, ("write back" | "write through", source), or Unread: NVMe through NVME_REPORT;
    sd through device/scsi_disk/*/cache_type (the kernel's copy of the drive's MODE SENSE caching page at probe: a
    blind spot against v3floor, which sends MODE SENSE itself through SG_IO; an override written to queue/write_cache
    leaves it, so the cross-check still sees one); virtio_blk through its cache_type."""
    if name.startswith("nvme"):
        return NVME_REPORT(d)
    sd = os.path.join(d, "device", "scsi_disk")
    if os.path.isdir(sd):
        cts = [c for c in (read(os.path.join(sd, e, "cache_type")) for e in sorted(os.listdir(sd))) if c]
        if len(cts) != 1:
            raise Unread(f"{len(cts)} cache_type files under {sd}")
        if cts[0].startswith("write back"):
            return "write back", f"sd cache_type {cts[0]!r}"
        if cts[0] in ("write through", "none"):
            return "write through", f"sd cache_type {cts[0]!r}"
        raise Unread(f"cannot parse sd cache_type {cts[0]!r}")
    ct = read(os.path.join(d, "cache_type"))
    if ct in ("write back", "write through"):
        return ct, "virtio_blk cache_type"
    raise Unread(f"no cache report for {name}: not NVMe, sd or virtio_blk")


def read(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def mount_of(path):
    """findmnt's TARGET, SOURCE (without a btrfs subvolume suffix, "/dev/sda1[/@]"), FSTYPE and OPTIONS for the
    filesystem holding PATH."""
    try:
        out = subprocess.run(["findmnt", "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "-T", path], capture_output=True,
                             text=True, timeout=30, check=True).stdout
        fs = json.loads(out)["filesystems"][0]
        m = {k: fs[k] for k in ("target", "source", "fstype", "options")}
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, IndexError, TypeError) as e:
        raise Undetermined(f"findmnt -T {path}: {e.__class__.__name__}: {e}")
    if not all(isinstance(v, str) for v in m.values()):
        raise Undetermined(f"findmnt -T {path}: unreadable row {m}")
    m["source"] = strip_subvol(m["source"])
    return m


def strip_subvol(src):
    return re.sub(r"\[[^\]]*\]$", "", src.strip())


def barrier_off(options):
    return any(t in ("nobarrier", "barrier=0") for t in options.split(","))


def dev_of(path):
    """MAJOR:MINOR of the block device holding PATH."""
    try:
        st = os.stat(path)
    except OSError as e:
        raise Undetermined(f"stat {path}: {e}")
    if os.major(st.st_dev) != 0:
        return f"{os.major(st.st_dev)}:{os.minor(st.st_dev)}"
    src = mount_of(path)["source"]  # an anonymous device (btrfs; tmpfs has no device): the mount's source node
    if not src.startswith("/"):
        raise Undetermined(f"{path} is on anonymous device 0:{os.minor(st.st_dev)} with source {src!r}, not a device")
    try:
        sst = os.stat(src)
    except OSError as e:
        raise Undetermined(f"{path} is on anonymous device 0:{os.minor(st.st_dev)} and its source {src!r}: {e}")
    if not stat.S_ISBLK(sst.st_mode):
        raise Undetermined(f"{path}'s mount source {src!r} is not a block device")
    return f"{os.major(sst.st_rdev)}:{os.minor(sst.st_rdev)}"


def disk_record(d):
    rec = {"name": os.path.basename(d), "model": read(os.path.join(d, "device", "model")) or "",
           "vendor": read(os.path.join(d, "device", "vendor")) or "",
           "write_cache": read(os.path.join(d, "queue", "write_cache")),
           "fua": read(os.path.join(d, "queue", "fua")), "rotational": read(os.path.join(d, "queue", "rotational"))}
    rec["class"] = classify(rec["write_cache"], rec["fua"], rec["name"])
    if rec["class"] == "ram-volatile":
        rec["drive_report"] = None  # no drive
        return rec
    try:  # comp review 1 LOW 13: the drive's own report beside the kernel's write_cache (v3floor's cross-check)
        rep, src = drive_report(d, rec["name"])
    except Unread as e:
        rec["drive_report"] = {"unread": str(e)}
        return rec
    rec["drive_report"] = {"cache": rep, "source": src}
    if rep != rec["write_cache"]:
        raise Undetermined(f"the leaf {rec['name']}'s kernel queue/write_cache says {rec['write_cache']!r} but the drive "
                           f"reports {rep!r} ({src}): one of them was overridden, so whether a flush reaches the drive "
                           f"cannot be determined")
    return rec


def classify(write_cache, fua, name="?"):
    if RAM_DISK.match(name):
        return "ram-volatile"
    if write_cache == "write through":
        return "write-through"
    if write_cache == "write back" and fua == "1":
        return "write-back+fua"
    if write_cache == "write back" and fua == "0":
        return "write-back-no-fua"
    raise Undetermined(f"disk {name}: write_cache {write_cache!r} fua {fua!r} is not a known class")


def layer_wc(d, name, layers):
    """A loop or device-mapper layer's own queue/write_cache, appended to LAYERS (comp review 1 LOW 13: a write-through
    layer strips a flush before it reaches anything below it)."""
    wc = read(os.path.join(d, "queue", "write_cache"))
    if wc not in ("write back", "write through"):
        raise Undetermined(f"layer {name}: queue/write_cache {wc!r} is neither write back nor write through")
    layers.append({"name": name, "write_cache": wc})


def leaves(sysfs, majmin, devof, chain, backs, layers=None, depth=0):
    """The leaf disks under block device MAJMIN (appending each step to CHAIN, each loop's backing file to BACKS, each
    loop or device-mapper layer's write_cache to LAYERS)."""
    layers = [] if layers is None else layers
    if depth > MAX_DEPTH:
        raise Undetermined(f"device chain deeper than {MAX_DEPTH}: {'>'.join(chain)}")
    link = os.path.join(sysfs, "dev", "block", majmin)
    if not os.path.exists(link):
        raise Undetermined(f"no {link}")
    d = os.path.realpath(link)
    if os.path.exists(os.path.join(d, "partition")):
        chain.append(os.path.basename(d))
        d = os.path.dirname(d)
    name = os.path.basename(d)
    chain.append(name)
    if os.path.isdir(os.path.join(d, "loop")):
        back = read(os.path.join(d, "loop", "backing_file"))
        if not back:
            raise Undetermined(f"loop {name} has no backing file")
        layer_wc(d, name, layers)
        chain.append(back)
        backs.append(back)
        return leaves(sysfs, devof(back), devof, chain, backs, layers, depth + 1)
    sl = os.path.join(d, "slaves")
    slaves = sorted(os.listdir(sl)) if os.path.isdir(sl) else []
    if slaves:
        layer_wc(d, name, layers)
        out = []
        for s in slaves:
            mm = read(os.path.join(sl, s, "dev"))
            if not mm:
                raise Undetermined(f"{name}: slave {s} has no dev")
            out += leaves(sysfs, mm, devof, chain, backs, layers, depth + 1)
        return out
    return [disk_record(d)]


def flush_verdict(cls, disks, layers, mounts):
    """(flush_reaches_drive, why), one of FLUSH (comp review 1 LOW 13), in this order: a RAM disk has no drive; a
    write-through leaf is sent no flush ('unknown' when its own report was not read: the write-through may be an
    override hiding a cache); a filesystem with barriers off sends none down; a write-through loop or dm layer strips
    it; an unread report leaves it unknown; else it reaches the drive."""
    unread = [f"{x['name']}: {x['drive_report']['unread']}" for x in disks if "unread" in (x.get("drive_report") or {})]
    off = [f"{m['target']}({m['fstype']})" for m in mounts if m["barrier_off"]]
    wt = [x["name"] for x in layers if x["write_cache"] == "write through"]
    if cls == "ram-volatile":
        return "n/a: volatile RAM", "a RAM disk: nothing survives power, and there is no drive to flush"
    if cls == "write-through" and unread:
        return "unknown", f"write-through leaf whose own cache report was not read ({'; '.join(unread)})"
    if cls == "write-through":
        return "n/a: write-through", "the leaf is write-through, as its own report says: the kernel sends it no flush"
    if off:
        return "no", f"barriers off on {', '.join(off)}: no flush or FUA is sent down"
    if wt:
        return "no", f"write-through layer {', '.join(wt)}: the kernel strips the flush there"
    if unread:
        return "unknown", f"the drive's own cache report was not read ({'; '.join(unread)})"
    return "yes", "every layer is write-back, barriers are on, and the drive's own report agrees with its leaf"


def resolve(mnt, sysfs="/sys", devof=dev_of, mountof=mount_of):
    chain, backs, layers = [], [], []
    disks = leaves(sysfs, devof(mnt), devof, chain, backs, layers)
    classes = sorted({x["class"] for x in disks})
    if len(classes) != 1:
        raise Undetermined(f"leaf disks of different classes: {[(x['name'], x['class']) for x in disks]}")
    mounts = []
    for p in [mnt] + backs:
        m = dict(mountof(p))
        m["path"] = p
        m["barrier_off"] = barrier_off(m["options"])
        mounts.append(m)
    flush, why = flush_verdict(classes[0], disks, layers, mounts)
    return {"mnt": mnt, "chain": chain, "disks": disks, "drive_class": classes[0], "layers": layers, "mounts": mounts,
            "flush_reaches_drive": flush, "flush_why": why}


def real_problem(r):
    """Comp review 1 LOW 35: why a REAL run (run_system.sh FT_DRY=0) may not run on the record R's storage (None = it
    may): a volatile RAM disk, or a loop device anywhere in the chain (a file on another filesystem, whose flush is an
    fsync of that file: not the drive the class names)."""
    if r.get("drive_class") == "ram-volatile":
        return "the drive class is ram-volatile: nothing survives power"
    loops = [x for x in r.get("chain", []) if re.fullmatch(r"loop\d+", str(x))]
    if loops:
        return f"loop device(s) {loops} in the chain {'>'.join(map(str, r.get('chain', [])))}"
    return None


def summary(r):
    off = [f"{m['target']}({m['fstype']})" for m in r["mounts"] if m["barrier_off"]]
    return (f"drive_class={r['drive_class']} flush_reaches_drive={r['flush_reaches_drive'].replace(' ', '')} disks=" +
            ",".join(f"{x['name']}({x['model']})" for x in r["disks"]) + " chain=" + ">".join(r["chain"]) +
            (" barrier_off=" + ",".join(off) if off else ""))


# ---------------------------------------------------------------- selftest
def fake_disk(sysfs, path, majmin, wc, fua, model="Virtual Disk", parts=(), report="same"):
    """A disk at SYSFS/devices/PATH with optional partitions [(name, majmin)]; registers /sys/dev/block links. REPORT
    (comp review 1 LOW 13) is the drive's own cache report: an sd disk's device/scsi_disk/0:0:0:0/cache_type, an NVMe
    disk's VWC for fake_nvme ("1", "0" or "unreadable: why", in a fake_vwc file); "same": the report agreeing with WC;
    None: no report."""
    d = os.path.join(sysfs, "devices", path)
    os.makedirs(os.path.join(d, "queue"))
    os.makedirs(os.path.join(d, "device"))
    name = os.path.basename(d)
    if report == "same":
        report = None if wc not in ("write back", "write through") else (
            ("1" if wc == "write back" else "0") if name.startswith("nvme") else wc)
    for f, v in (("queue/write_cache", wc), ("queue/fua", fua), ("device/model", model), ("queue/rotational", "0")):
        if v is not None:
            open(os.path.join(d, f), "w").write(v + "\n")
    if report is not None and name.startswith("nvme"):
        open(os.path.join(d, "fake_vwc"), "w").write(report + "\n")
    elif report is not None:
        os.makedirs(os.path.join(d, "device", "scsi_disk", "0:0:0:0"))
        open(os.path.join(d, "device", "scsi_disk", "0:0:0:0", "cache_type"), "w").write(report + "\n")
    link(sysfs, majmin, d)
    for pn, pmm in parts:
        p = os.path.join(d, pn)
        os.makedirs(p)
        open(os.path.join(p, "partition"), "w").write("1\n")
        open(os.path.join(p, "dev"), "w").write(pmm + "\n")
        link(sysfs, pmm, p)
    return d


def link(sysfs, majmin, target):
    b = os.path.join(sysfs, "dev", "block")
    os.makedirs(b, exist_ok=True)
    os.symlink(os.path.relpath(target, b), os.path.join(b, majmin))


def fake_loop(sysfs, n, majmin, backing, wc="write back"):
    """A loop device; its own queue/write_cache is WC (a real loop device reads "write back")."""
    d = os.path.join(sysfs, "devices", "virtual", "block", f"loop{n}")
    os.makedirs(os.path.join(d, "loop"))
    os.makedirs(os.path.join(d, "queue"))
    open(os.path.join(d, "loop", "backing_file"), "w").write(backing + "\n")
    open(os.path.join(d, "queue", "write_cache"), "w").write(wc + "\n")
    link(sysfs, majmin, d)
    return d


def fake_dm(sysfs, majmin, slave_dirs, wc="write back"):
    d = os.path.join(sysfs, "devices", "virtual", "block", "dm-0")
    os.makedirs(os.path.join(d, "slaves"))
    os.makedirs(os.path.join(d, "queue"))
    open(os.path.join(d, "queue", "write_cache"), "w").write(wc + "\n")
    for s in slave_dirs:
        os.symlink(os.path.relpath(s, os.path.join(d, "slaves")), os.path.join(d, "slaves", os.path.basename(s)))
    link(sysfs, majmin, d)
    return d


def fake_nvme(disk_dir):
    """The selftest's NVMe cache report (comp review 1 LOW 13): the fake disk's fake_vwc in place of the Identify
    Controller ioctl -- ("write back" | "write through", source), or Unread for "unreadable: why" or no file."""
    v = read(os.path.join(disk_dir, "fake_vwc"))
    if v in ("0", "1"):
        return ("write back" if v == "1" else "write through"), "fake Identify Controller VWC bit 0"
    raise Unread(v[len("unreadable: "):] if v and v.startswith("unreadable: ") else f"no fake VWC under {disk_dir}")


def selftest():
    # (name, builder(sysfs) -> (devof map, mount map or None for "rw,relatime" everywhere), want class or None for
    #  refused, want chain or None, want flush_reaches_drive or None)
    # Comp review 1 LOW 13: flush_reaches_drive is "yes", "no", "n/a: write-through" (the kernel sends a write-through
    # leaf no flush: it has nothing to drain), "n/a: volatile RAM" or "unknown" (the drive's own cache report was not
    # read, so the kernel's write_cache may be an override); every layer's write_cache is read, and a drive report
    # that disagrees with its leaf's write_cache is refused. NVMe reports come from fake_nvme, not the ioctl.
    globals()["NVME_REPORT"] = fake_nvme
    cases = []
    MNTX, BACK = "/mnt/fastest-xfs", "/fastest-loop-xfs.img"

    def loop_on(wc, fua, model, disk="sda", part="sda1", mounts=None, loop_wc="write back", report="same"):
        def b(s):
            fake_disk(s, f"pci0/host0/block/{disk}", "8:0", wc, fua, model, parts=[(part, "8:1")], report=report)
            fake_loop(s, 0, "7:0", BACK, wc=loop_wc)
            return {MNTX: "7:0", BACK: "8:1"}, mounts
        return b
    # (LOW 13 flips this case: True claimed a flush reaches a write-through leaf, from which the kernel strips it)
    cases.append(("xfs loop on a write-through sda (GitHub's Virtual Disk)",
                  loop_on("write through", "0", "Virtual Disk"), "write-through",
                  ["loop0", BACK, "sda1", "sda"], "n/a: write-through"))
    cases.append(("xfs loop on a write-back NVMe with FUA (MSFT NVMe Accelerator)",
                  loop_on("write back", "1", "MSFT NVMe Accelerator v1.0", "nvme0n1", "nvme0n1p1"), "write-back+fua",
                  ["loop0", BACK, "nvme0n1p1", "nvme0n1"], "yes"))
    cases.append(("write back without FUA", loop_on("write back", "0", "x"), "write-back-no-fua", None, None))
    # finding 3: the loop's flush is an fsync of the backing file, and a nobarrier filesystem sends nothing down
    nob = {MNTX: ("/mnt/fastest-xfs", "/dev/loop0", "xfs", "rw,relatime,attr2,inode64"),
           BACK: ("/", "/dev/root", "ext4", "rw,relatime,nobarrier,discard")}
    cases.append(("NVMe with FUA under a backing filesystem mounted nobarrier",
                  loop_on("write back", "1", "MSFT NVMe Accelerator v1.0", "nvme0n1", "nvme0n1p1", nob),
                  "write-back+fua", None, "no"))
    b0 = {MNTX: ("/mnt/fastest-xfs", "/dev/loop0", "btrfs", "rw,barrier=0"), BACK: ("/", "/dev/sda1", "ext4", "rw")}
    cases.append(("barrier=0 on the run's own filesystem over a write-through leaf (nothing to drain either way)",
                  loop_on("write through", "0", "Virtual Disk", mounts=b0), "write-through", None, "n/a: write-through"))
    cases.append(("mount options cannot be read", loop_on("write through", "0", "Virtual Disk", mounts={}), None, None,
                  None))

    def whole(s):
        fake_disk(s, "pci0/nvme/block/nvme1n1", "259:0", "write back", "1", "Datacenter NVMe")
        return {"/data/work": "259:0"}, None
    cases.append(("a real data disk, no loop, no partition (a T3 cell)", whole, "write-back+fua", ["nvme1n1"], "yes"))

    def ram(s):  # finding 7: brd's ram0 says "write through" and is volatile
        fake_disk(s, "virtual/block/ram0", "1:0", "write through", "0", None, report=None)
        return {"/mnt/brd": "1:0"}, None
    cases.append(("a RAM disk (brd ram0) is volatile, not write-through", ram, "ram-volatile", ["ram0"],
                  "n/a: volatile RAM"))

    def dm(wc2):
        def b(s):
            a = fake_disk(s, "pci0/h0/block/sda", "8:0", "write through", "0", parts=[("sda2", "8:2")])
            c = fake_disk(s, "pci0/h1/block/sdb", "8:16", wc2, "0", parts=[("sdb1", "8:17")])
            fake_dm(s, "253:0", [os.path.join(a, "sda2"), os.path.join(c, "sdb1")])
            return {"/mnt/x": "253:0"}, None
        return b
    cases.append(("device-mapper over two write-through disks", dm("write through"), "write-through",
                  ["dm-0", "sda2", "sda", "sdb1", "sdb"], "n/a: write-through"))
    # LOW 13's new cases: a write-through LAYER strips the flush before the drive; a drive report that disagrees with
    # its leaf's write_cache means one was overridden (v3floor's refusal); an unread report leaves the flush unknown
    cases.append(("a write-through loop layer over an NVMe with FUA (the flush stops at the loop)",
                  loop_on("write back", "1", "MSFT NVMe Accelerator v1.0", "nvme0n1", "nvme0n1p1",
                          loop_wc="write through"), "write-back+fua", None, "no"))

    def nvme_override(s):
        fake_disk(s, "pci0/nvme/block/nvme1n1", "259:0", "write through", "0", "Datacenter NVMe", report="1")
        return {"/data/work": "259:0"}, None
    cases.append(("an NVMe whose queue says write through while the drive reports a volatile cache (VWC 1)",
                  nvme_override, None, None, None))
    cases.append(("an sd whose queue says write through while its cache_type says write back",
                  loop_on("write through", "0", "Virtual Disk", report="write back"), None, None, None))

    def nvme_unread(s):
        fake_disk(s, "pci0/nvme/block/nvme1n1", "259:0", "write back", "1", "Datacenter NVMe",
                  report="unreadable: open /dev/nvme1: Permission denied")
        return {"/data/work": "259:0"}, None
    cases.append(("an NVMe whose own cache report cannot be read", nvme_unread, "write-back+fua", None, "unknown"))
    cases.append(("a write-through sd with no cache report (an override could hide a cache)",
                  loop_on("write through", "0", "Virtual Disk", report=None), "write-through", None, "unknown"))
    cases.append(("device-mapper over disks of different classes", dm("write back"), None, None, None))
    cases.append(("no write_cache file", loop_on(None, "0", "x"), None, None, None))
    cases.append(("write back with no fua file", loop_on("write back", None, "x"), None, None, None))
    cases.append(("an unknown write_cache value", loop_on("write around", "0", "x"), None, None, None))

    def no_link(s):
        os.makedirs(os.path.join(s, "dev", "block"))
        return {"/mnt/x": "8:5"}, None
    cases.append(("no /sys/dev/block entry", no_link, None, None, None))

    def detached(s):
        fake_loop(s, 0, "7:0", "")
        return {"/mnt/x": "7:0"}, None
    cases.append(("a loop device with no backing file", detached, None, None, None))

    def cycle(s):
        fake_loop(s, 0, "7:0", "/mnt/x/img")
        return {"/mnt/x": "7:0", "/mnt/x/img": "7:0"}, None
    cases.append(("a loop whose backing file is on itself (cycle)", cycle, None, None, None))

    def unresolved(s):
        fake_loop(s, 0, "7:0", "/somewhere.img")
        return {"/mnt/x": "7:0"}, None  # devof raises for the backing file
    cases.append(("the backing file's device cannot be read", unresolved, None, None, None))

    bad = 0
    for name, build, want, want_chain, want_flush in cases:
        with tempfile.TemporaryDirectory() as root:
            sysfs = os.path.join(root, "sys")
            os.makedirs(sysfs)
            m, mm = build(sysfs)

            def devof(p, m=m):
                if p not in m:
                    raise Undetermined(f"no device for {p}")
                return m[p]

            def mountof(p, mm=mm):
                if mm is None:
                    return {"target": p, "source": "/dev/x", "fstype": "xfs", "options": "rw,relatime"}
                if p not in mm:
                    raise Undetermined(f"no mount row for {p}")
                return dict(zip(("target", "source", "fstype", "options"), mm[p]))
            mnt = next(iter(m))
            try:
                r = resolve(mnt, sysfs, devof, mountof)
                got, chain, flush, why = r["drive_class"], r["chain"], r["flush_reaches_drive"], summary(r)
            except Undetermined as e:
                got, chain, flush, why = None, None, None, f"refused: {e}"
        ok = (got == want and (want_chain is None or chain == want_chain) and
              (want_flush is None or flush == want_flush) and (got is None or got in CLASSES))
        print(("PASS" if ok else "FAIL"), name, "->", why)
        bad += not ok
    n = len(cases)
    for src, want in (("/dev/sda1[/@]", "/dev/sda1"), ("/dev/loop3", "/dev/loop3"), ("/dev/root\n", "/dev/root")):
        got = strip_subvol(src)
        print(("PASS" if got == want else "FAIL"), f"strip_subvol({src!r}) = {got!r}")
        bad += got != want
        n += 1
    for opts, want in (("rw,relatime", False), ("rw,nobarrier", True), ("rw,barrier=0", True), ("rw,barrier=1", False),
                       ("rw,nobarrierx", False)):
        got = barrier_off(opts)
        print(("PASS" if got == want else "FAIL"), f"barrier_off({opts!r}) = {got}")
        bad += got != want
        n += 1
    # comp review 1 LOW 35: a REAL run (FT_DRY=0) refuses a volatile RAM disk and any loop device in the chain
    rp = globals().get("real_problem")
    for name, rec, want_ok in (
            ("a data disk", {"drive_class": "write-back+fua", "chain": ["nvme1n1"]}, True),
            ("a RAM disk", {"drive_class": "ram-volatile", "chain": ["ram0"]}, False),
            ("a loop device in the chain", {"drive_class": "write-through", "chain": ["loop0", "/x.img", "sda1", "sda"]},
             False),
            ("a partition of a data disk", {"drive_class": "write-back+fua", "chain": ["nvme1n1p1", "nvme1n1"]}, True)):
        why = rp(rec) if rp else "no real_problem() in drive.py"
        ok_ = (why is None) == want_ok
        print(("PASS" if ok_ else "FAIL"), f"real run on {name} -> {why or 'allowed'}")
        bad += not ok_
        n += 1
    print(f"TALLY ran={n} pass={n - bad} na=0 fail={bad}")  # gates/tally.py (linux-rev-uw2 MED 1)
    print(f"drive selftest: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "record":
        try:
            r = resolve(sys.argv[2])
        except Undetermined as e:
            print(f"REFUSED: drive class under {sys.argv[2]}: {e}")
            sys.exit(2)
        with open(sys.argv[3], "w") as f:
            json.dump(r, f, indent=1)
        print(summary(r))
        sys.exit(0)
    if len(sys.argv) == 3 and sys.argv[1] == "real-check":  # DRIVE.JSON: may a real run use this storage? (LOW 35)
        try:
            why = real_problem(json.load(open(sys.argv[2])))
        except (OSError, ValueError) as e:
            why = f"unreadable drive record: {e}"
        print("ok" if why is None else f"REFUSED: a real run (FT_DRY=0) on {why}")
        sys.exit(0 if why is None else 2)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
