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
OPTIONS); flush_reaches_drive is false when any of them has barriers off.

  drive.py record MNT OUT.json  OUT.json = {"mnt", "chain", "disks", "drive_class", "mounts", "flush_reaches_drive"};
                                prints one line "drive_class=C flush_reaches_drive=yes|no disks=NAME(model)[,...]
                                chain=A>B... [barrier_off=TARGET(FSTYPE),...]"; exit 2 (no OUT) when refused
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


def leaves(sysfs, majmin, devof, chain, backs, depth=0):
    """The leaf disks under block device MAJMIN (appending each step to CHAIN, each loop's backing file to BACKS)."""
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
        chain.append(back)
        backs.append(back)
        return leaves(sysfs, devof(back), devof, chain, backs, depth + 1)
    sl = os.path.join(d, "slaves")
    slaves = sorted(os.listdir(sl)) if os.path.isdir(sl) else []
    if slaves:
        out = []
        for s in slaves:
            mm = read(os.path.join(sl, s, "dev"))
            if not mm:
                raise Undetermined(f"{name}: slave {s} has no dev")
            out += leaves(sysfs, mm, devof, chain, backs, depth + 1)
        return out
    return [disk_record(d)]


def resolve(mnt, sysfs="/sys", devof=dev_of, mountof=mount_of):
    chain, backs = [], []
    disks = leaves(sysfs, devof(mnt), devof, chain, backs)
    classes = sorted({x["class"] for x in disks})
    if len(classes) != 1:
        raise Undetermined(f"leaf disks of different classes: {[(x['name'], x['class']) for x in disks]}")
    mounts = []
    for p in [mnt] + backs:
        m = dict(mountof(p))
        m["path"] = p
        m["barrier_off"] = barrier_off(m["options"])
        mounts.append(m)
    return {"mnt": mnt, "chain": chain, "disks": disks, "drive_class": classes[0], "mounts": mounts,
            "flush_reaches_drive": not any(m["barrier_off"] for m in mounts)}


def summary(r):
    off = [f"{m['target']}({m['fstype']})" for m in r["mounts"] if m["barrier_off"]]
    return (f"drive_class={r['drive_class']} flush_reaches_drive={'yes' if r['flush_reaches_drive'] else 'no'} disks=" +
            ",".join(f"{x['name']}({x['model']})" for x in r["disks"]) + " chain=" + ">".join(r["chain"]) +
            (" barrier_off=" + ",".join(off) if off else ""))


# ---------------------------------------------------------------- selftest
def fake_disk(sysfs, path, majmin, wc, fua, model="Virtual Disk", parts=()):
    """A disk at SYSFS/devices/PATH with optional partitions [(name, majmin)]; registers /sys/dev/block links."""
    d = os.path.join(sysfs, "devices", path)
    os.makedirs(os.path.join(d, "queue"))
    os.makedirs(os.path.join(d, "device"))
    for f, v in (("queue/write_cache", wc), ("queue/fua", fua), ("device/model", model), ("queue/rotational", "0")):
        if v is not None:
            open(os.path.join(d, f), "w").write(v + "\n")
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


def fake_loop(sysfs, n, majmin, backing):
    d = os.path.join(sysfs, "devices", "virtual", "block", f"loop{n}")
    os.makedirs(os.path.join(d, "loop"))
    open(os.path.join(d, "loop", "backing_file"), "w").write(backing + "\n")
    link(sysfs, majmin, d)
    return d


def fake_dm(sysfs, majmin, slave_dirs):
    d = os.path.join(sysfs, "devices", "virtual", "block", "dm-0")
    os.makedirs(os.path.join(d, "slaves"))
    for s in slave_dirs:
        os.symlink(os.path.relpath(s, os.path.join(d, "slaves")), os.path.join(d, "slaves", os.path.basename(s)))
    link(sysfs, majmin, d)
    return d


def selftest():
    # (name, builder(sysfs) -> (devof map, mount map or None for "rw,relatime" everywhere), want class or None for
    #  refused, want chain or None, want flush_reaches_drive or None)
    cases = []
    MNTX, BACK = "/mnt/fastest-xfs", "/fastest-loop-xfs.img"

    def loop_on(wc, fua, model, disk="sda", part="sda1", mounts=None):
        def b(s):
            fake_disk(s, f"pci0/host0/block/{disk}", "8:0", wc, fua, model, parts=[(part, "8:1")])
            fake_loop(s, 0, "7:0", BACK)
            return {MNTX: "7:0", BACK: "8:1"}, mounts
        return b
    cases.append(("xfs loop on a write-through sda (GitHub's Virtual Disk)",
                  loop_on("write through", "0", "Virtual Disk"), "write-through",
                  ["loop0", BACK, "sda1", "sda"], True))
    cases.append(("xfs loop on a write-back NVMe with FUA (MSFT NVMe Accelerator)",
                  loop_on("write back", "1", "MSFT NVMe Accelerator v1.0", "nvme0n1", "nvme0n1p1"), "write-back+fua",
                  ["loop0", BACK, "nvme0n1p1", "nvme0n1"], True))
    cases.append(("write back without FUA", loop_on("write back", "0", "x"), "write-back-no-fua", None, None))
    # finding 3: the loop's flush is an fsync of the backing file, and a nobarrier filesystem sends nothing down
    nob = {MNTX: ("/mnt/fastest-xfs", "/dev/loop0", "xfs", "rw,relatime,attr2,inode64"),
           BACK: ("/", "/dev/root", "ext4", "rw,relatime,nobarrier,discard")}
    cases.append(("NVMe with FUA under a backing filesystem mounted nobarrier",
                  loop_on("write back", "1", "MSFT NVMe Accelerator v1.0", "nvme0n1", "nvme0n1p1", nob),
                  "write-back+fua", None, False))
    b0 = {MNTX: ("/mnt/fastest-xfs", "/dev/loop0", "btrfs", "rw,barrier=0"), BACK: ("/", "/dev/sda1", "ext4", "rw")}
    cases.append(("barrier=0 on the run's own filesystem", loop_on("write through", "0", "Virtual Disk", mounts=b0),
                  "write-through", None, False))
    cases.append(("mount options cannot be read", loop_on("write through", "0", "Virtual Disk", mounts={}), None, None,
                  None))

    def whole(s):
        fake_disk(s, "pci0/nvme/block/nvme1n1", "259:0", "write back", "1", "Datacenter NVMe")
        return {"/data/work": "259:0"}, None
    cases.append(("a real data disk, no loop, no partition (a T3 cell)", whole, "write-back+fua", ["nvme1n1"], True))

    def ram(s):  # finding 7: brd's ram0 says "write through" and is volatile
        fake_disk(s, "virtual/block/ram0", "1:0", "write through", "0", None)
        return {"/mnt/brd": "1:0"}, None
    cases.append(("a RAM disk (brd ram0) is volatile, not write-through", ram, "ram-volatile", ["ram0"], None))

    def dm(wc2):
        def b(s):
            a = fake_disk(s, "pci0/h0/block/sda", "8:0", "write through", "0", parts=[("sda2", "8:2")])
            c = fake_disk(s, "pci0/h1/block/sdb", "8:16", wc2, "0", parts=[("sdb1", "8:17")])
            fake_dm(s, "253:0", [os.path.join(a, "sda2"), os.path.join(c, "sdb1")])
            return {"/mnt/x": "253:0"}, None
        return b
    cases.append(("device-mapper over two write-through disks", dm("write through"), "write-through",
                  ["dm-0", "sda2", "sda", "sdb1", "sdb"], None))
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
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
