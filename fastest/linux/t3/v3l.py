#!/usr/bin/env python3
"""V3L, the registered T3 floor (PREREG-v1-FINAL-CANDIDATE line 180): one measurement, before or after a block.

  v3l.py measure DIR OUT [LEAFREC]
                                run it on the filesystem holding DIR; write OUT/v3l.json and the raw fio and strace
                                files; exit 0 VALID, 3 VOID (a registered gate failed), 2 refused (nothing measured).
                                LEAFREC: the block's V3 batch summary.json, whose leaf record carries the drive's own
                                cache report (the probe's NVMe VWC / SCSI WCE / virtio read): V3L refuses another disk,
                                and VOIDs a kernel write_cache that disagrees with the drive (gate-6 review M6). A real
                                run (V3L_REAL=1) without it is refused (T3 runner review item 14)
  v3l.py block BEFORE AFTER     one block's record from its two v3l.json files: the pooled fsync p50 (the block's
                                normaliser), the ratio and the drift (published, never gates); exit 0 when both are
                                VALID, 3 when either is VOID, 2 when either is missing or unreadable
  v3l.py registration           exit 0 when the floor rule's SLACK is registered by a PREREG annex row, 2 with the
                                reason when it is not (review 5 LOW 29: t3run's preflight refuses a real run on it,
                                before deps and build are paid for; measure() refuses a real run on it too)
  v3l.py self-test              the parsers and the gates on planted inputs; exit 0 iff every case passes

As registered: fio `--rw=write --bs=4k --ioengine=sync --fsync=1`, exactly 10,000 writes (`--number_ios=10000`),
and a D0 control without `--fsync`, each on a fresh file in DIR. PLUS `--end_fsync=1` on the fsync arm: measured on
fio 3.36 (dry run 37517239631, xfs on a loop), `--fsync=1 --number_ios=10000` makes 9,999 fsyncs, because fio queues
a write's fsync at the start of its next I/O and the job ends at its 10,000th; V1L then reads 9,999, not "1 fsync per
write", and every block VOIDs. end_fsync supplies the last one. The registered command line does not name the flag
(reported to the lead: PREREG line 180 needs it, or its gate needs restating). Each arm runs TWICE, with identical fio commands:
  labelling  under V1L, the registered strace (`-f -y -ttt -e trace=%file,%desc,ioctl,copy_file_range,
             sync_file_range,syncfs,msync,fallocate`): the syscall gate only, because tracing changes the timing
             (line 173: timed T3 runs carry no tracer);
  timed      untraced: the latencies, and the drive's flush counter (/sys/block/<disk>/stat field 16, flush requests
             completed) read just before and just after.
Every run also reads the drive's sectors-written counter (stat field 7) just before and just after (gated on the
fsync arm's two runs), and every run is preceded, outside its window, by a sync of every filesystem (sync(1)) and then
`sync -f` (syncfs) on each filesystem under DIR, top-down: DIR's, then each loop layer's backing file's. So dirty data
written earlier (the labelling run's, through a buffered loop) is not written back inside a later window and does not
pad its count (annex A24, the lead's ruling). sync(1) calls the void sync(2), so its rc is 0 by construction; the rc
that carries a writeback error is each sync -f's (review 6 MED 2, annex A24-AM1 as proposed). The record keeps each
one's path, rc and seconds, and /proc/meminfo's Dirty and Writeback after them; a failed one refuses the measurement.
Gates (any failure makes the measurement VOID, and the block with it):
  - V1L: exactly 10,000 writes to the data file on each arm; 10,000 fsync of the data file on the fsync arm and no
    fsync on the control; no fdatasync, sync_file_range, syncfs or msync on either; no failed write or fsync; no
    io_uring_setup (line 173 refuses such a labelling run);
  - fio reports exactly 10,000 writes on every run, no sync on the control's, and on the fsync arm the same sync count
    in the timed run as in the V1L-checked labelling run (the timed run has no tracer; this ties it to the checked one);
  - the drive's queue/write_cache reads "write back" or "write through" (anything else VOIDs); on "write back" the flush
    counter rises by at least 10,000 across the timed fsync run AND across the strace-checked labelling run (a lower
    bound: another process's flushes can pad it, never shrink it; T3 runner review item 3), and the timed run's
    flushes per fsync are at least the labelling run's x (1 - SLACK) (item 18; SLACK = 0.05, registered as PREREG
    annex A27 and bound to a measurement: the fsync arm takes two labelling runs back to back, and a real run refuses
    before any timed run when their drive flush counts differ by more than SLACK/2 of the larger; a dry run records
    the pair); on "write through" it reads 0 in both runs. Every LOOP layer is gated the same way on its own
    counter (gate-6 review M3), so a write-through layer above the drive shows as a drive counter that did not rise;
  - the leaf's queue/iostats reads 1 (recorded in leaf.iostats; measure() refuses otherwise, and gates() VOIDs a
    record without it on its own text): the sectors-written counter moves only with iostats on (review 5 MED 2);
  - the drive's sectors-written counter rises by at least the fsynced data, N x 4 KiB = N x 8 sectors, across EACH
    fsync run, timed and labelling (T3 runner review item 10): over a write-through drive the flush counter reads 0
    whether or not an fsync reached the drive (a write-through loop above it sends no flush either, so its writes can
    sit in the backing file's page cache), and only the write count tells. A lower bound: another writer on the same
    drive (on a loop block, the runner's root disk) can pad it, never shrink it. A missing or non-int count VOIDs. A
    ram disk is exempt (brd: dry runs only, never credited);
  - the timed fsync run carries fio's own sync count (N-1 or N; fio 3.36 does not count the end_fsync), a p50 and
    the histogram bins (review M1: no measurement, no normaliser, no VALID).
Published, not gates (lines 180 and 553): both runs' drive flushes per fsync and every loop layer's, with the slack
(item 18), the fsync p50, the fsync/control write p50 ratio, the before-to-after
drift. floor_kind names only what was checked, one text per state (T3 runner review item 19): a ram disk, "brd: no
drive (dry runs only, never credited)"; a write-back drive, "write-back drive: flush requests completed by the
driver (issued and acknowledged, not persistence)", or, when its flush counts VOID, "write-back drive with flushes
short (VOID)" (review 5 LOW 23); a write-through drive whose counter read 0, "no volatile cache:
no flush request sent (counter 0 checked in both runs)", or, for a record without a labelling count, "no volatile
cache: no flush request sent (counter 0 checked in the timed run; no labelling count)": the block layer sends such a
drive no flush, so the fsync latency is not a drive flush (GitHub-hosted runners' disks are such drives); a
write-through queue whose counter rose, "write-through queue with flushes counted (VOID)"; any other write_cache,
"write cache unknown (VOID)". A virtualized leaf appends "; virtualized <kind>: reach to media unknown".
The pooled p50 merges the two timed fsync runs' latency histograms (fio json+ bins, fio's own ~1.5% buckets).

Blind spots, stated: the labelling and timed runs are separate executions of one command; the flush counter is the
whole drive's, so concurrent work on that drive pads it; a filesystem behind device-mapper or md is refused, not
resolved, and so is a native-multipath NVMe head (its counter may live on the path devices); a loop is followed
through its backing file to the drive under it. Every fio argv is recorded in v3l.json (review M7).
"""
import copy
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from fractions import Fraction

N = 10000
SLACK = Fraction("0.05")  # registered: annex A27. The floor rule's allowance, timed vs labelling flushes per fsync
# the PREREG annex row that registers SLACK (review 5 LOW 29); while it is None a real run (V3L_REAL=1) refuses. A27
# (DECISIONS cb6074aa3a) binds SLACK to a labelling pair (lab_pair_problem); SLACK changes only by an appended row
SLACK_REGISTERED = "A27"
V1L = ["strace", "-f", "-y", "-ttt",
       "-e", "trace=%file,%desc,ioctl,copy_file_range,sync_file_range,syncfs,msync,fallocate"]
WRITES = ("write", "pwrite64", "writev", "pwritev", "pwritev2")
OTHER_SYNCS = ("fdatasync", "sync_file_range", "syncfs", "msync")


def sh(*a, timeout=60):
    r = subprocess.run(a, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


def mount_source(path):
    rc, out, _ = sh("findmnt", "-n", "-o", "SOURCE", "-T", path)
    src = re.sub(r"\[.*\]$", "", out.strip())  # btrfs prints /dev/X[/subvol]
    if rc != 0 or not src.startswith("/dev/") or not os.path.exists(src):
        raise RuntimeError(f"cannot resolve the block device under {path} (findmnt source {out.strip()!r})")
    return src


def resolve_leaf(d):
    """DIR's mount followed down to a whole disk: (chain of device names, disk, paths), paths being one path on each
    filesystem under DIR, top-down (DIR, then each loop's backing file), which the pre-run sync syncs one by one
    (annex A24-AM1). Loops are followed through their backing file; device-mapper and md are refused."""
    chain, path, paths = [], d, [d]
    for _ in range(6):
        name = os.path.basename(os.path.realpath(mount_source(path)))
        chain.append(name)
        if name.startswith("loop"):
            try:
                path = open(f"/sys/block/{name}/loop/backing_file").read().strip()
            except OSError as e:
                raise RuntimeError(f"loop {name} has no readable backing file: {e}")
            paths.append(path)
            continue
        if name.startswith("dm-") or name.startswith("md"):
            raise RuntimeError(f"{name}: device-mapper and md are not resolved to a drive (refused)")
        sysp = os.path.realpath(f"/sys/class/block/{name}")
        disk = os.path.basename(os.path.dirname(sysp)) if os.path.exists(f"{sysp}/partition") else name
        mp = f"/sys/block/{disk}/multipath"
        if os.path.isdir(mp) and os.listdir(mp):
            raise RuntimeError(f"{disk} is a native-multipath NVMe head ({sorted(os.listdir(mp))}): its flush counter "
                               "may live on the path devices (refused, not resolved)")
        return chain, disk, paths
    raise RuntimeError(f"more than 5 layers under {d}")


def disk_attr(disk, rel):
    try:
        return open(f"/sys/block/{disk}/{rel}").read().strip()
    except OSError:
        return None


def disk_stat(disk):
    """/sys/block/<disk>/stat's fields (Documentation/block/stat.rst); refuses a kernel that keeps no flush counter."""
    f = open(f"/sys/block/{disk}/stat").read().split()
    if len(f) < 17:
        raise RuntimeError(f"/sys/block/{disk}/stat has {len(f)} fields: this kernel keeps no flush counter")
    return f


def flush_ios(disk):
    """Field 16 of /sys/block/<disk>/stat: flush requests completed."""
    return int(disk_stat(disk)[15])


def sectors_written(disk):
    """Field 7 of /sys/block/<disk>/stat: sectors written (512-byte units, whatever the logical block size)."""
    return int(disk_stat(disk)[6])


SECTORS_PER_WRITE = 4096 // 512  # fio --bs=4k


LINE = re.compile(r"^(?:\d+\s+)?\d+\.\d+\s+(?:(\w+)\((.*)|<\.\.\. (\w+) resumed>(.*))$")


def parse_v1l(text, data):
    """Count, from a V1L trace, the calls the gates read. `data` is the data file's path (as -y prints it)."""
    c = {"data_writes": 0, "data_fsyncs": 0, "other_fsyncs": 0, "failed": 0, "io_uring_setup": 0}
    for k in OTHER_SYNCS:
        c[k] = 0
    for line in text.splitlines():
        m = LINE.match(line)
        if not m:
            continue
        name, rest = (m.group(1), m.group(2)) if m.group(1) else (m.group(3), m.group(4))
        resumed = m.group(3) is not None
        if name not in WRITES + ("fsync",) + OTHER_SYNCS + ("io_uring_setup",):
            continue
        if not rest.endswith("<unfinished ...>") and re.search(r"\) += -1 ", rest + " "):
            c["failed"] += 1
        if resumed:
            continue  # counted at its start line
        if name == "io_uring_setup":
            c["io_uring_setup"] += 1
            continue
        fd = re.match(r"\d+<([^>]*)>", rest)
        on_data = bool(fd) and fd.group(1) == data
        if name in WRITES and on_data:
            c["data_writes"] += 1
        elif name == "fsync":
            c["data_fsyncs" if on_data else "other_fsyncs"] += 1
        elif name in OTHER_SYNCS:
            c[name] += 1
    return c


def fio_cmd(name, path, fsync, out_json):
    c = ["fio", f"--name={name}", f"--filename={path}", "--rw=write", "--bs=4k", "--ioengine=sync",
         f"--number_ios={N}", "--size=64m", "--output-format=json+", f"--output={out_json}"]
    if fsync:
        # V3L_PLANT=fsync2 (t3run.sh --plant v3l-fsync-half, dry runs only) syncs every 2nd write: the V1L and fio
        # gates must VOID it. Inert unless set; recorded in v3l.json.
        c.append("--fsync=2" if os.environ.get("V3L_PLANT") == "fsync2" else "--fsync=1")
        c.append("--end_fsync=1")
    return c


def pct50(lat):
    p = (lat or {}).get("percentile") or {}
    v = p.get("50.000000")
    return round(v / 1e3, 2) if v is not None else None


def fio_numbers(path):
    t = open(path).read()
    j = json.loads(t[t.find("{"):])  # fio may print notes before the JSON
    job = j["jobs"][0]
    w, s = job["write"], job.get("sync", {})
    out = {"fio_error": job.get("error"), "writes": w.get("total_ios"),
           "write_p50_us": pct50(w.get("lat_ns")) or pct50(w.get("clat_ns"))}
    sl = s.get("lat_ns") or {}
    out["syncs"] = sl.get("N", 0)
    if out["syncs"]:
        out["fsync_p50_us"] = pct50(sl)
        out["fsync_p99_us"] = round(sl["percentile"]["99.000000"] / 1e3, 2) if "99.000000" in sl.get("percentile", {}) else None
        out["fsync_bins_ns"] = sl.get("bins")  # json+ only; the pooled p50 needs them
    return out


def labelling_problems(arm, want, v, fio):
    """One strace-checked labelling run's checks (line 173), by V1L and by fio: exactly N writes to the data file, WANT
    fsyncs of it, no fsync of another file, no other sync call, no failed call, no io_uring; fio's own error and
    write count. gates() runs them on the record's labelling run; measure() and gates() run them on annex A27's second
    one (review 7 finding 3), named by ARM."""
    if not isinstance(v, dict) or not isinstance(fio, dict):
        return [f"V1L {arm}: no trace parse or no fio record for this labelling run ({v!r:.60}, {fio!r:.60})"]
    bad = []
    if v.get("data_writes") != N:
        bad.append(f"V1L {arm}: {v.get('data_writes')} writes to the data file, registered exactly {N}")
    if v.get("data_fsyncs") != want:
        bad.append(f"V1L {arm}: {v.get('data_fsyncs')} fsync of the data file, registered {want}")
    if v.get("other_fsyncs"):
        bad.append(f"V1L {arm}: {v['other_fsyncs']} fsync of other files")
    for k in OTHER_SYNCS:
        if v.get(k):
            bad.append(f"V1L {arm}: {v[k]} {k}, registered 0")
    if v.get("failed"):
        bad.append(f"V1L {arm}: {v['failed']} failed write/sync calls")
    if v.get("io_uring_setup"):
        bad.append(f"V1L {arm}: io_uring_setup in a labelling run (refused, line 173)")
    run = arm.split(" ")[0] + "/labelling_fio" + arm[len(arm.split(" ")[0]):]
    if fio.get("fio_error"):
        bad.append(f"fio {run}: error {fio['fio_error']}")
    if fio.get("writes") != N:
        bad.append(f"fio {run}: {fio.get('writes')} writes, registered exactly {N}")
    return bad


def second_labelling_problems(a):
    """Annex A27's second labelling run (the fsync arm's v1l_2 and labelling_fio_2) judged as a labelling run, and
    fio's sync count equal to the first labelling run's (one command): review 7 finding 3"""
    bad = labelling_problems("fsync (A27 second labelling)", N, a.get("v1l_2"), a.get("labelling_fio_2"))
    s1, s2 = (a.get("labelling_fio") or {}).get("syncs"), (a.get("labelling_fio_2") or {}).get("syncs")
    if s2 != s1:
        bad.append(f"fio fsync (A27 second labelling): {s2} syncs, the first labelling run {s1} (one command; they must "
                   "agree)")
    return bad


def gates(rec):
    """The registered gates over a record; the list of failures (empty = VALID)."""
    bad = []
    for arm, want in (("fsync", N), ("control", 0)):
        a = rec["arms"][arm]
        bad += labelling_problems(arm, want, a["v1l"], a["labelling_fio"])
        f = a["timed"]
        if f.get("fio_error"):
            bad.append(f"fio {arm}/timed: error {f['fio_error']}")
        if f.get("writes") != N:
            bad.append(f"fio {arm}/timed: {f.get('writes')} writes, registered exactly {N}")
        if arm == "control":
            for run in ("labelling_fio", "timed"):
                if a[run].get("syncs", 0):
                    bad.append(f"fio control/{run}: {a[run]['syncs']} syncs, registered 0")
        else:
            if a["timed"].get("syncs") != a["labelling_fio"].get("syncs"):
                bad.append(f"fio fsync: the timed run made {a['timed'].get('syncs')} syncs, the V1L-checked labelling "
                           f"run {a['labelling_fio'].get('syncs')} (one command; they must agree)")
            if a["timed"].get("syncs") not in (N - 1, N):
                bad.append(f"fio fsync/timed: {a['timed'].get('syncs')} syncs, not {N - 1} or {N}")
            if a["timed"].get("fsync_p50_us") is None or not a["timed"].get("fsync_bins_ns"):
                bad.append("fio fsync/timed: no fsync p50 or no latency histogram (the block's normaliser is unmeasured)")
    # annex A27's second labelling run, wherever a record carries it, is judged as a labelling run (review 7 finding 3)
    if "v1l_2" in rec["arms"]["fsync"] or "lab_pair" in rec["arms"]["fsync"]:
        bad += second_labelling_problems(rec["arms"]["fsync"])
    for lay in rec["leaf"].get("layers") or []:
        bad += counter_gate(f"loop layer {lay.get('name')}", lay.get("write_cache"), lay.get("flush_ios_delta"),
                            lay.get("lab_flush_ios_delta"))
    # A16 (1): the kernel's state and the drive's own report (the V3 batch's leaf record) must AGREE; an absent or
    # unknown report is not agreement (third lane review LOW 7). A ram disk (brd, dry runs only) has no report.
    dr = rec["leaf"].get("drive_reports")
    if dr != drive_report_for(rec["leaf"].get("disk"), rec["leaf"]["write_cache"]):
        bad.append(f"drive {rec['leaf']['disk']}: the kernel's write_cache ({rec['leaf']['write_cache']}) disagrees with "
                   f"the drive's own report ({dr!r})")
    wc = rec["leaf"]["write_cache"]
    if wc in ("write back", "write through"):
        bad += counter_gate(f"drive {rec['leaf']['disk']}", wc, rec["arms"]["fsync"]["timed"].get("flush_ios_delta"),
                            rec["arms"]["fsync"]["timed"].get("lab_flush_ios_delta"))
    else:
        bad.append(f"drive {rec['leaf']['disk']}: queue/write_cache unreadable or unknown ({wc!r})")
    if not str(rec["leaf"].get("disk") or "").startswith("ram"):
        # the write count only moves with queue/iostats 1; without it the write rule's verdict would name the wrong cause
        ip = iostats_problem(rec["leaf"].get("iostats"))
        if ip:
            bad.append(f"drive {rec['leaf'].get('disk')}: {ip}")
        else:
            bad += write_gate(rec["leaf"].get("disk"), rec["arms"]["fsync"]["timed"])
        bad += sync_gate(rec["leaf"].get("disk"), rec["arms"]["fsync"].get("sync"))
        bad += sync_cover_gate(rec["leaf"], rec["arms"]["fsync"].get("sync"))
    return bad


def iostats_problem(v):
    """Why the leaf's queue/iostats value V makes its sectors-written counter useless, or None when it is 1 (review 5
    MED 2: flushes are counted whatever iostats says; sectors written are counted only with iostats on)"""
    if v is None or str(v).strip() == "":
        return "cannot read the leaf's queue/iostats: whether its sectors-written counter advances is unknown"
    if str(v).strip() != "1":
        return f"iostats off ({v!r}): the sectors-written counter does not advance"
    return None


def sync_gate(disk, sy):
    """Annex A24: each fsync run on a drive was preceded by a sync that ran and exited 0, as its record says, and
    (A24-AM1) each filesystem's sync -f in it exited 0; without that, earlier dirty data may have been written back
    inside the window and padded the write count."""
    bad = []
    for run in ("timed", "labelling"):
        r = sy.get(run) if isinstance(sy, dict) else None
        if not isinstance(r, dict):
            bad.append(f"drive {disk}: no sync record for the {run} fsync run (annex A24)")
        elif r.get("ran") is not True or type(r.get("rc")) is not int or r["rc"] != 0:
            bad.append(f"drive {disk}: the sync before the {run} fsync run did not run clean ({r!r}): earlier dirty "
                       "data may pad its counts (annex A24)")
        else:
            for x in r.get("layers") or []:
                if not isinstance(x, dict) or type(x.get("rc")) is not int or x["rc"] != 0:
                    bad.append(f"drive {disk}: the sync before the {run} fsync run failed on "
                               f"{x.get('path') if isinstance(x, dict) else x!r} ({x!r}): that filesystem's dirty "
                               "data may pad its counts (annex A24-AM1)")
    return bad


def sync_cover_gate(leaf, sy):
    """Annex A24-AM1: the sync before each fsync run reached every filesystem under DIR, one sync -f each: DIR's and
    each loop layer's backing file's (1 + the loop layers), so a buffered loop's dirty pages in its backing file's
    cache are not written back inside a later window. A missing record is sync_gate's to report."""
    want, bad = 1 + len(leaf.get("layers") or []), []
    for run in ("timed", "labelling"):
        r = sy.get(run) if isinstance(sy, dict) else None
        lay = r.get("layers") if isinstance(r, dict) else None
        if isinstance(r, dict) and (not isinstance(lay, list) or len(lay) != want):
            bad.append(f"drive {leaf.get('disk')}: the sync before the {run} fsync run synced "
                       f"{len(lay) if isinstance(lay, list) else 'no'} filesystems, not the {want} under DIR "
                       "(annex A24-AM1)")
    return bad


def meminfo_dirty():
    """/proc/meminfo's Dirty and Writeback in kB, or None where it cannot be read (published, not a gate)"""
    try:
        m = dict(ln.split(":", 1) for ln in open("/proc/meminfo") if ":" in ln)
        return {k: int(m[k].split()[0]) for k in ("Dirty", "Writeback")}
    except (OSError, KeyError, ValueError, IndexError):
        return None


def sync_record(runner=subprocess.run, paths=()):
    """The sync before a V3L run, outside its window (annex A24, A24-AM1): sync(1), whose rc is 0 by construction
    (it calls the void sync(2)), then `sync -f P` (syncfs, whose rc does carry a writeback error) for each P in
    PATHS, one per filesystem under DIR, top-down (resolve_leaf's paths). Each is recorded with its seconds, and
    /proc/meminfo's Dirty and Writeback after them. Any that fails, times out or cannot start refuses the
    measurement, naming it."""
    def one(argv):
        t = time.monotonic()
        try:
            r = runner(argv, capture_output=True, text=True, timeout=600)
        except (OSError, subprocess.TimeoutExpired) as e:
            raise RuntimeError(f"the sync before a V3L run failed: {' '.join(argv)}: {type(e).__name__}: {e}")
        if r.returncode != 0:
            raise RuntimeError(f"the sync before a V3L run failed: {' '.join(argv)}: rc {r.returncode}: "
                               f"{(r.stderr or '').strip()[:200]}")
        return r.returncode, round(time.monotonic() - t, 3)

    t0 = time.monotonic()
    grc, gsecs = one(["sync"])
    layers = []
    for p in paths:
        rc, secs = one(["sync", "-f", p])
        layers.append({"path": p, "rc": rc, "secs": secs})
    return {"ran": True, "rc": max([grc] + [x["rc"] for x in layers]), "secs": round(time.monotonic() - t0, 3),
            "how": "sync(1), then sync -f per filesystem under DIR, top-down (annex A24-AM1)",
            "global": {"rc": grc, "secs": gsecs, "note": "sync(2) is void: this rc is 0 by construction"},
            "layers": layers, "meminfo_after": meminfo_dirty()}


def write_gate(disk, t):
    """T3 runner review item 10: the fsynced data, N x 4 KiB, reached the drive inside each fsync run, by its
    sectors-written counter (a lower bound; see the module docstring). Exact integers, so N x 8 itself passes."""
    bad, need = [], N * SECTORS_PER_WRITE
    for run, key in (("timed", "sectors_written_delta"), ("labelling", "lab_sectors_written_delta")):
        v = t.get(key)
        if not isinstance(v, int) or isinstance(v, bool):
            bad.append(f"drive {disk}: no sectors-written count in the {run} fsync run ({v!r}): whether the fsynced "
                       "data reached the drive is unmeasured")
        elif v < need:
            bad.append(f"drive {disk}: its sectors-written counter rose {v} in the {run} fsync run, below the {need} "
                       f"its {N} fsynced 4 KiB writes need: the data did not reach the drive inside the run (a "
                       "write-through layer or a cache above it)")
    return bad


def drive_report_for(disk, wc):
    """The drive report that agrees with a kernel write_cache of `wc` on `disk`: the probe's 'none (RAM)' on a ram
    disk (brd has no drive to ask), else the same state. gates() and the plant base both use it."""
    return "none (RAM)" if str(disk or "").startswith("ram") else wc


def counter_gate(what, wc, timed, lab):
    """One device's flush counter over the timed fsync run (`timed`) and the strace-checked labelling run (`lab`), A16:
    write back: at least one flush per fsync in the timed run; at least one per fsync in the labelling run (a count
      below N VOIDs, whatever the timed run did: PREREG line 180 voids "fewer than one per fsync" in the run whose
      fsyncs strace checked; T3 runner review item 3); a count that is not an int is unreadable and VOIDs; and the
      timed run's flushes per fsync at least the labelling run's x (1 - SLACK), so a timed run that lost part of its
      fsyncs cannot pass on a stack that makes 2 per fsync, nor ~47% of them at 1.9 per fsync as the whole-number
      floor lab // N let it (gate-6 review 7, lane review MED 3, T3 runner review item 18). Exact rationals, so the
      edge (timed = 0.95 x labelling) passes. SLACK absorbs the labelling run's padding by another process's
      flushes; its value is registered (annex A27) and bound to the labelling pair. Blind spot, stated: a record with NO labelling count
      (None) gets the timed rule only; measure() has written one for the drive and every loop layer since 7dc4d4c04,
      so only a record from before then, or a hand-made one, is judged that way;
    write through: the counter must read 0 in both runs (the block layer sends no flush; a count contradicts the
      recorded state and VOIDs), each run judged on its own and reported in its own words (T3 runner review item 4)."""
    bad = []
    if wc == "write back":
        # review 5 LOW 24: a timed count that is not an int is unreadable, VOID as a labelling one is (it raised
        # TypeError, which block() reported as MISSING)
        if timed is not None and (not isinstance(timed, int) or isinstance(timed, bool)):
            bad.append(f"write-back {what}: timed count unreadable ({timed!r}): not absent, so not judged as absent")
            return bad
        short = timed is None or timed < N
        if short:
            bad.append(f"write-back {what}: its flush counter rose {timed} across {N} fsyncs (fewer than one per fsync)")
        if lab is not None and (not isinstance(lab, int) or isinstance(lab, bool)):
            bad.append(f"write-back {what}: labelling count unreadable ({lab!r}): not absent, so not judged as absent")
        elif isinstance(lab, int) and lab < N:
            bad.append(f"write-back {what}: its flush counter rose {lab} across the {N} fsyncs of the labelling run "
                       "(fewer than one per fsync in the strace-checked run)")
        elif isinstance(lab, int) and not short and Fraction(timed) < Fraction(lab) * (1 - SLACK):
            bad.append(f"write-back {what}: {timed / N:.4f} flushes per fsync in the timed run, below (1 - "
                       f"{float(SLACK)}) x the {lab / N:.4f} per fsync the labelling run showed ({timed} and {lab} "
                       f"across {N})")
    elif wc == "write through":
        if timed != 0:
            bad.append(f"write-through {what}: its flush counter rose {timed} in the timed run; a write-through queue "
                       "gets no flush request, so a count contradicts the recorded state (A16)")
        if lab is not None and lab != 0:
            bad.append(f"write-through {what}: its flush counter rose {lab} in the labelling run; a write-through queue "
                       "gets no flush request, so a count contradicts the recorded state (A16)")
    return bad


def real_mode_problem(real, chain, disk, leafrec):
    """Why a measurement may not run, or None. A real run (V3L_REAL=1; t3run sets it when not --dry-run) is on a
    drive only, never a loop or ram disk (A16), and carries the V3 batch's leaf record (LEAFREC), so the drive's own
    cache report is always there for gates() to cross-check (T3 runner review item 14). A dry run needs neither."""
    if real != "1":
        return None
    if any(c.startswith("loop") for c in chain) or disk.startswith("ram"):
        return f"a real run on {chain} -> {disk}: loop and ram devices are dry-run only (A16)"
    if not leafrec:
        return "a real run needs the V3 batch's leaf record (LEAFREC): the drive's own cache report is cross-checked"
    return registration_problem()


def lab_pair_problem(first, second):
    """Annex A27: why two back-to-back labelling runs' drive flush counts do not bind SLACK, or None. They may differ
    by at most SLACK/2 of the larger, so the padding SLACK allows sits well inside the allowance it justifies; a
    count that is not an int cannot bind it."""
    if any(not isinstance(v, int) or isinstance(v, bool) for v in (first, second)):
        return f"the labelling pair's flush counts are unreadable ({first!r}, {second!r}): SLACK is not bound (annex A27)"
    big = max(first, second)
    if Fraction(abs(first - second)) > SLACK / 2 * big:
        return (f"the two labelling runs' drive flush counts {first} and {second} differ by {abs(first - second)}, more "
                f"than SLACK/2 ({float(SLACK / 2)}) of the larger ({big}): the padding SLACK allows is not bound "
                "(annex A27); refused before any timed run")
    return None


def registration_problem():
    """Why a real V3L run may not use the floor rule's SLACK, or None (review 5 LOW 29): it can gate a billed run, so a
    PREREG annex row must register it first (SLACK_REGISTERED names that row)"""
    if not SLACK_REGISTERED:
        return (f"SLACK {float(SLACK)} is unregistered (no PREREG annex row names it): a real V3L run may not use it "
                "(review 5 LOW 29)")
    return None


def measure(d, out, leafrec=None):
    if os.path.exists(out):
        raise RuntimeError(f"{out} exists")
    os.makedirs(d, exist_ok=True)
    chain, disk, sync_paths = resolve_leaf(d)
    why = real_mode_problem(os.environ.get("V3L_REAL"), chain, disk, leafrec)
    if why:
        raise RuntimeError(why)
    drive_reports, virt = None, None
    if leafrec:
        lj = json.load(open(leafrec))
        lr = lj.get("leaf") or {}
        if lr.get("disk") != disk:
            raise RuntimeError(f"the leaf record {leafrec} names disk {lr.get('disk')!r}, V3L resolved {disk!r}")
        drive_reports = lr.get("drive_reports")
        virt = lj.get("virtualization")
    rc, sv, _ = sh("strace", "-V")
    rec = {"instrument": "V3L (PREREG-v1-FINAL-CANDIDATE line 180; V1L line 173)", "dir": os.path.realpath(d),
           "n": N, "fio_version": sh("fio", "--version")[1].strip(),
           "strace_version": sv.splitlines()[0] if rc == 0 and sv else None,
           "leaf": {"chain": chain, "disk": disk, "write_cache": disk_attr(disk, "queue/write_cache"),
                    "iostats": disk_attr(disk, "queue/iostats"),
                    "fua": disk_attr(disk, "queue/fua"), "rotational": disk_attr(disk, "queue/rotational"),
                    "model": disk_attr(disk, "device/model"), "drive_reports": drive_reports,
                    "drive_reports_from": leafrec,
                    "layers": [{"name": c, "write_cache": disk_attr(c, "queue/write_cache")}
                               for c in chain if c.startswith("loop")]},
           "virtualization": virt, "arms": {}, "plant": os.environ.get("V3L_PLANT") or None, "argv": {}}
    if not rec["fio_version"] or not rec["strace_version"]:
        raise RuntimeError("fio or strace is not installed")
    ip = None if disk.startswith("ram") else iostats_problem(rec["leaf"]["iostats"])
    if ip:
        raise RuntimeError(f"{disk}: {ip} (review 5 MED 2; refused before any run)")
    os.makedirs(out)
    for arm, fs in (("fsync", True), ("control", False)):
        a = {}
        f = os.path.join(os.path.realpath(d), f"v3l-{arm}.dat")
        if os.path.exists(f):
            os.unlink(f)
        tr, lj = os.path.join(out, f"{arm}.v1l"), os.path.join(out, f"{arm}-labelling.json")
        cmd = V1L + ["-o", tr] + fio_cmd(f"v3l-{arm}", f, fs, lj)
        rec["argv"][f"{arm}/labelling"] = cmd
        # outside the window: earlier dirty data cannot pad this run's counts (annex A24, A24-AM1)
        sy_lab = sync_record(paths=sync_paths)
        k0 = {lay["name"]: flush_ios(lay["name"]) for lay in rec["leaf"]["layers"]}
        g0, gs0 = flush_ios(disk), sectors_written(disk)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        g1, gs1 = flush_ios(disk), sectors_written(disk)
        k1 = {lay["name"]: flush_ios(lay["name"]) for lay in rec["leaf"]["layers"]}
        if r.returncode != 0:
            raise RuntimeError(f"labelling {arm} run rc {r.returncode}: {r.stderr[-400:]}")
        a["v1l"] = parse_v1l(open(tr).read(), f)
        a["labelling_fio"] = fio_numbers(lj)
        a["labelling_fio"].pop("fsync_bins_ns", None)
        os.unlink(f)
        if fs:
            # annex A27: a second labelling run, back to back (its own sync first, A24), binds SLACK to a measurement:
            # the two runs' drive flush counts may differ by at most SLACK/2 of the larger. A real run refuses here,
            # before any timed run; a dry run records the pair, so this path runs on every dry run before a paid box
            tr2, lj2 = os.path.join(out, f"{arm}-2.v1l"), os.path.join(out, f"{arm}-labelling-2.json")
            cmd2 = V1L + ["-o", tr2] + fio_cmd(f"v3l-{arm}", f, fs, lj2)
            rec["argv"][f"{arm}/labelling-2"] = cmd2
            sy_lab2 = sync_record(paths=sync_paths)
            h0 = flush_ios(disk)
            r2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=3600)
            h1 = flush_ios(disk)
            if r2.returncode != 0:
                raise RuntimeError(f"second labelling {arm} run rc {r2.returncode}: {r2.stderr[-400:]}")
            a["v1l_2"] = parse_v1l(open(tr2).read(), f)
            a["labelling_fio_2"] = fio_numbers(lj2)
            a["labelling_fio_2"].pop("fsync_bins_ns", None)
            os.unlink(f)
            # judged as a labelling run first (review 7 finding 3): only a valid one may bind SLACK or let a timed run
            # start; rc 0 is not the V1L shape proof the first run gets
            run_bad = second_labelling_problems(a)
            pair = {"first": g1 - g0, "second": h1 - h0, "allowance": f"SLACK/2 = {float(SLACK / 2)} of the larger",
                    "sync": sy_lab2, "run_problems": run_bad}
            pair["problem"] = lab_pair_problem(pair["first"], pair["second"])
            a["lab_pair"] = pair
            if run_bad and os.environ.get("V3L_REAL") == "1":
                raise RuntimeError("the A27 second labelling run is not a valid labelling run, so it binds nothing "
                                   "(review 7 finding 3): " + "; ".join(run_bad))
            if pair["problem"] and os.environ.get("V3L_REAL") == "1":
                raise RuntimeError(pair["problem"])
        tj = os.path.join(out, f"{arm}-timed.json")
        cmd = fio_cmd(f"v3l-{arm}", f, fs, tj)
        rec["argv"][f"{arm}/timed"] = cmd
        sy_timed = sync_record(paths=sync_paths)
        l0 = {lay["name"]: flush_ios(lay["name"]) for lay in rec["leaf"]["layers"]}
        f0, fs0 = flush_ios(disk), sectors_written(disk)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        f1, fs1 = flush_ios(disk), sectors_written(disk)
        l1 = {lay["name"]: flush_ios(lay["name"]) for lay in rec["leaf"]["layers"]}
        if arm == "fsync":
            for lay in rec["leaf"]["layers"]:
                lay["flush_ios_delta"] = l1[lay["name"]] - l0[lay["name"]]
                lay["lab_flush_ios_delta"] = k1[lay["name"]] - k0[lay["name"]]
        if r.returncode != 0:
            raise RuntimeError(f"timed {arm} run rc {r.returncode}: {r.stderr[-400:]}")
        a["timed"] = fio_numbers(tj)
        a["timed"]["flush_ios_delta"] = f1 - f0
        a["timed"]["lab_flush_ios_delta"] = g1 - g0
        a["timed"]["sectors_written_delta"] = fs1 - fs0
        a["timed"]["lab_sectors_written_delta"] = gs1 - gs0
        a["sync"] = {"labelling": sy_lab, "timed": sy_timed}
        os.unlink(f)
        rec["arms"][arm] = a
    rec["published"] = publish(rec)
    rec["floor_kind"] = floor_kind(rec)
    bad = gates(rec)
    rec["verdict"] = "VOID" if bad else "VALID"
    rec["void_reasons"] = bad
    json.dump(rec, open(os.path.join(out, "v3l.json"), "w"), indent=1)
    print(json.dumps({"verdict": rec["verdict"], "leaf": rec["leaf"], "published": rec["published"], "void": bad}))
    return 3 if bad else 0


def ratio(a, b):
    return round(a / b, 2) if a and b else None


def per_fsync(count):
    """A flush count over the N fsyncs, or None when the count is absent or not an int."""
    return round(count / N, 4) if isinstance(count, int) and not isinstance(count, bool) else None


def publish(rec):
    """The record's published numbers (not gates): the fsync p50s, and both runs' flushes per fsync for the drive and
    for every loop layer, with the floor's slack (T3 runner review item 18)."""
    ft, ct = rec["arms"]["fsync"]["timed"], rec["arms"]["control"]["timed"]
    return {"fsync_p50_us": ft.get("fsync_p50_us"), "fsync_p99_us": ft.get("fsync_p99_us"),
            "control_write_p50_us": ct.get("write_p50_us"),
            "fsync_over_control_write_p50": ratio(ft.get("fsync_p50_us"), ct.get("write_p50_us")),
            "flush_ios_per_fsync": per_fsync(ft.get("flush_ios_delta")),
            "lab_flush_ios_per_fsync": per_fsync(ft.get("lab_flush_ios_delta")),
            "layers": {lay.get("name"): {"flush_ios_per_fsync": per_fsync(lay.get("flush_ios_delta")),
                                         "lab_flush_ios_per_fsync": per_fsync(lay.get("lab_flush_ios_delta"))}
                       for lay in rec["leaf"].get("layers") or []},
            "floor_slack": float(SLACK),
            "floor_rule": "write back: timed flushes per fsync >= labelling flushes per fsync x (1 - floor_slack); "
                          "the slack is annex A27, bound to a labelling pair within floor_slack/2"}


def floor_kind(rec):
    """What the record's floor is, naming only what was checked (T3 runner review item 19): one text per state, and an
    unknown or VOID state never reads "counter 0 checked". The module docstring lists every text."""
    leaf, t = rec["leaf"], rec["arms"]["fsync"]["timed"]
    wc, timed, lab = leaf.get("write_cache"), t.get("flush_ios_delta"), t.get("lab_flush_ios_delta")
    if str(leaf.get("disk", "")).startswith("ram"):
        k = "brd: no drive (dry runs only, never credited)"
    elif wc == "write back" and not counter_gate("drive", wc, timed, lab):
        k = "write-back drive: flush requests completed by the driver (issued and acknowledged, not persistence)"
    elif wc == "write back":
        k = "write-back drive with flushes short (VOID)"
    elif wc == "write through" and timed == 0 and lab == 0:
        k = "no volatile cache: no flush request sent (counter 0 checked in both runs)"
    elif wc == "write through" and timed == 0 and lab is None:
        k = "no volatile cache: no flush request sent (counter 0 checked in the timed run; no labelling count)"
    elif wc == "write through":
        k = "write-through queue with flushes counted (VOID)"
    else:
        k = "write cache unknown (VOID)"
    virt = rec.get("virtualization")
    if virt and virt.get("virtualized") is not False:
        k += f"; virtualized {virt.get('virtualized')!r}: reach to media unknown"
    return k


def pooled_p50_ns(bins_list):
    """The p50 of the merged fio histograms (bins: latency ns -> count), or None if any is missing."""
    if not bins_list or any(not b for b in bins_list):
        return None
    merged = {}
    for b in bins_list:
        for k, v in b.items():
            merged[int(k)] = merged.get(int(k), 0) + int(v)
    total, acc = sum(merged.values()), 0
    for k in sorted(merged):
        acc += merged[k]
        if 2 * acc >= total:
            return k
    return None


def block(b, a):
    """One block's V3L record from its before and after v3l.json (either may be absent: MISSING)."""
    rec = {"before": None, "after": None}
    for k, p in (("before", b), ("after", a)):
        try:
            r = json.load(open(p))
            rec[k] = {"verdict": r["verdict"], "void_reasons": list(r["void_reasons"]), "published": r["published"],
                      "floor_kind": r["floor_kind"], "leaf": r["leaf"], "plant": r.get("plant"),
                      "fsync_bins_ns": r["arms"]["fsync"]["timed"].get("fsync_bins_ns")}
            # review L5: the verdict is re-derived from the record's own arms, never trusted as a string
            again = gates(r)
            if bool(again) != (r["verdict"] == "VOID") or r["verdict"] not in ("VALID", "VOID"):
                rec[k]["verdict"] = "VOID"
                rec[k]["void_reasons"].append(f"recorded verdict {r['verdict']!r} disagrees with the gates re-run on its "
                                              f"own arms ({again or 'VALID'})")
        except (OSError, ValueError, KeyError, TypeError) as e:
            rec[k] = {"verdict": "MISSING", "why": f"{p}: {type(e).__name__}: {e}"}
    vb, va = rec["before"]["verdict"], rec["after"]["verdict"]
    # a VOID measurement voids the block whatever the other is; MISSING otherwise wins over VALID
    rec["verdict"] = "VOID" if "VOID" in (vb, va) else "MISSING" if "MISSING" in (vb, va) else "VALID"
    if "MISSING" not in (vb, va):
        pb, pa = rec["before"]["published"]["fsync_p50_us"], rec["after"]["published"]["fsync_p50_us"]
        p = pooled_p50_ns([rec["before"]["fsync_bins_ns"], rec["after"]["fsync_bins_ns"]])
        if p is None and rec["verdict"] == "VALID":
            rec["verdict"] = "VOID"  # review M1: no pooled p50, no normaliser for the block
            rec["after"]["void_reasons"].append("no pooled fsync p50 (a histogram is missing)")
        rec["published"] = {"pooled_fsync_p50_us": round(p / 1e3, 2) if p is not None else None,
                            "pooled_note": None if p is not None else "fio emitted no sync histogram bins",
                            "drift_fsync_p50_us": round(pa - pb, 2) if pa is not None and pb is not None else None,
                            "ratio_before": rec["before"]["published"]["fsync_over_control_write_p50"],
                            "ratio_after": rec["after"]["published"]["fsync_over_control_write_p50"],
                            "not_gates": "the pooled p50, the ratio and the drift are published per block (lines 180, 553)"}
    for k in ("before", "after"):
        rec[k].pop("fsync_bins_ns", None)
    return rec


def as_state(rec, wc):
    """The plant base: a copy of `rec` with its drive and every loop layer in state `wc` (T3 runner review item 3).
    A device's counts are synthesised only where the base is DERIVED, i.e. that device's real state is the other one
    (write back: 2 per fsync in both runs; write through: 0 in both), or where the record carries no labelling count
    at all (filled from the device's own timed count, so both runs show one flushes-per-fsync). A count a real record
    carries for its own state is never rewritten: a real labelling count below N stays below N, and the control VOIDs."""
    r = copy.deepcopy(rec)

    def put(d, real):
        if real != wc:
            d["flush_ios_delta"] = d["lab_flush_ios_delta"] = 2 * N if wc == "write back" else 0
        elif d.get("lab_flush_ios_delta") is None:
            d["lab_flush_ios_delta"] = d.get("flush_ios_delta")

    put(r["arms"]["fsync"]["timed"], rec["leaf"].get("write_cache"))
    r["leaf"]["write_cache"] = wc
    # the report the gate wants for this state: 'none (RAM)' on brd (fourth lane review HIGH 3)
    r["leaf"]["drive_reports"] = drive_report_for(r["leaf"].get("disk"), wc)
    for lay in r["leaf"].get("layers") or []:
        put(lay, lay.get("write_cache"))
        lay["write_cache"] = wc
    return r


def plants(rec):
    """The V3L counter and agreement gates forced to fire on copies of a real record (gate-6 review 7, lane review
    MED 3/4): each plant is one field changed on a base whose gates pass. The branch the real drive is not on is
    reached through as_state(), which sets the leaf (and its loop layers) to that state with consistent counts.
    A plant fires only on a failure message carrying every one of its `want` texts (its own rule's text).
    Returns (results, ok); a control that does not pass makes every plant NOT-RUN (ok False)."""
    out = []
    again = gates(rec)
    out.append({"plant": "control", "want": "VALID", "got": again or "VALID", "fired": not again})
    if again:
        out.append({"plant": "all", "fired": False, "why": "NOT-RUN: the real record is not VALID"})
        return out, False

    def arm(name, base_wc, mutate, want, extra=None):
        r = as_state(rec, base_wc)
        if extra:
            extra(r)  # part of the base, so the base check below covers it
        if gates(r):
            out.append({"plant": name, "fired": False, "why": f"NOT-RUN: the {base_wc} base does not pass: {gates(r)}"})
            return
        mutate(r)
        got = gates(r)
        wants = (want,) if isinstance(want, str) else tuple(want)
        out.append({"plant": name, "want": want, "got": got, "fired": any(all(w in x for w in wants) for x in got)})

    def half_timed(r):
        t = r["arms"]["fsync"]["timed"]
        t["flush_ios_delta"] = t["flush_ios_delta"] // 2

    disk = rec["leaf"].get("disk")
    arm("wb-half-flushes", "write back", half_timed, "write-back drive")
    arm("wb-lab-short", "write back",
        lambda r: r["arms"]["fsync"]["timed"].__setitem__("lab_flush_ios_delta", N // 2),
        (f"write-back drive {disk}:", "fewer than one per fsync in the strace-checked run"))
    arm("wt-one-flush", "write through", lambda r: r["arms"]["fsync"]["timed"].__setitem__("flush_ios_delta", 1),
        "write-through drive")
    # item 4 (a): the labelling half of the write-through rule, on its own text
    arm("wt-lab-flush", "write through",
        lambda r: r["arms"]["fsync"]["timed"].__setitem__("lab_flush_ios_delta", 1),
        (f"write-through drive {disk}:", "in the labelling run; a write-through queue"))

    # item 4 (b): the per-layer floor rule. Real T3 records have no loop layer (V3L_REAL refuses loops), so the base
    # carries one synthetic write-back layer at 2 per fsync in both runs; the plant drops its timed run to 1.5 per fsync
    def plant_layer(r):
        r["leaf"].setdefault("layers", []).append({"name": "loop-plant", "write_cache": "write back",
                                                   "flush_ios_delta": 2 * N, "lab_flush_ios_delta": 2 * N})
        # and its backing filesystem synced, as a real loop's would be (annex A24-AM1's coverage rule)
        for y in (r["arms"]["fsync"].get("sync") or {}).values():
            if isinstance(y, dict) and isinstance(y.get("layers"), list):
                y["layers"].append({"path": "/plant/loop-plant.img", "rc": 0, "secs": 0.0})

    arm("wb-layer-half", "write back",
        lambda r: r["leaf"]["layers"][-1].__setitem__("flush_ios_delta", 3 * N // 2),
        ("write-back loop layer loop-plant:", "per fsync the labelling run showed"), extra=plant_layer)
    # item 10: the write rule, on a write-through base (the state whose flush counter cannot see an fsync that never
    # reached the drive); a ram disk is exempt from the rule, so it gets no such plant
    if not str(disk or "").startswith("ram"):
        arm("wt-unwritten", "write through",
            lambda r: r["arms"]["fsync"]["timed"].__setitem__("sectors_written_delta", N * SECTORS_PER_WRITE // 2),
            (f"drive {disk}:", "did not reach the drive"))
        # the labelling half of the same rule, on its own text (review 5 MED 4)
        arm("wt-unwritten-lab", "write through",
            lambda r: r["arms"]["fsync"]["timed"].__setitem__("lab_sectors_written_delta", N * SECTORS_PER_WRITE // 2),
            (f"drive {disk}:", "in the labelling fsync run", "did not reach the drive"))
        # annex A24: the sync rule, forced to fire by a failed sync before the timed run
        arm("unsynced", rec["leaf"]["write_cache"],
            lambda r: r["arms"]["fsync"].setdefault("sync", {}).__setitem__("timed", {"ran": True, "rc": 1}),
            (f"drive {disk}:", "the sync before the timed fsync run"))
        # and before the labelling run, on its own text (review 6 LOW 10)
        arm("unsynced-lab", rec["leaf"]["write_cache"],
            lambda r: r["arms"]["fsync"].setdefault("sync", {}).__setitem__("labelling", {"ran": True, "rc": 1}),
            (f"drive {disk}:", "the sync before the labelling fsync run"))
        # A24-AM1: the deepest filesystem's sync -f failed (the one shape of a failed sync measure() can see)
        arm("unsynced-layer", rec["leaf"]["write_cache"],
            lambda r: r["arms"]["fsync"]["sync"]["timed"]["layers"][-1].__setitem__("rc", 1),
            (f"drive {disk}:", "the sync before the timed fsync run", "failed on"))
    arm("timed-half-syncs", rec["leaf"]["write_cache"],
        lambda r: r["arms"]["fsync"]["timed"].__setitem__("syncs", r["arms"]["fsync"]["timed"]["syncs"] // 2),
        "they must agree")
    arm("drive-mismatch", rec["leaf"]["write_cache"],
        lambda r: r["leaf"].__setitem__("drive_reports", "write through" if r["leaf"]["write_cache"] == "write back"
                                        else "write back"), "disagrees with the drive's own report")
    return out, all(p["fired"] for p in out)


def _nobins(r):
    r["arms"]["fsync"]["timed"]["fsync_bins_ns"] = None
    return r


def _lab(r, lab):
    r["arms"]["fsync"]["timed"]["lab_flush_ios_delta"] = lab
    return r


def _ok(f):
    """A case evaluated inside a guard: an exception is that case's FAIL (named), not a crash of the self-test."""
    try:
        return bool(f())
    except Exception as e:  # noqa: BLE001
        print(f"V3L self-test case raised {type(e).__name__}: {e}")
        return False


def _has(got, *subs):
    """Some one failure message carries every one of `subs` (a rule's own text, not a prefix)."""
    return any(all(s in x for s in subs) for x in got or [])


# a real brd V3L record (fourth lane review HIGH 3); provenance in testdata/README.md
_BRD_B0 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata", "v3l-37812992594-brd-xfs-b0", "v3l.json")


def _raises(f, text):
    """f() raises, with `text` in the message"""
    try:
        f()
    except Exception as e:  # noqa: BLE001
        return text in str(e)
    return False


def _block_of(r):
    """block() over one record written as both its before and its after file (a temp dir, removed)"""
    d = tempfile.mkdtemp(prefix="v3l-st-")
    try:
        p = os.path.join(d, "v3l.json")
        with open(p, "w") as f:
            json.dump(r, f)
        return block(p, p)
    finally:
        os.unlink(os.path.join(d, "v3l.json"))
        os.rmdir(d)


def _measure_with(patches, env=None, leaf=None):
    """measure() on a fake box (review 5 LOW 25, review 6 LOW 10): PATCHES replaces module functions (resolve_leaf,
    disk_attr, sh, ...) for one call and ENV sets variables; LEAF, when given, is written as the V3 leaf record and
    passed as LEAFREC. Returns (the exception's text or None, whether OUT was made). Both are restored, and the temp
    dir removed, whatever the call did."""
    g, d = globals(), tempfile.mkdtemp(prefix="v3l-m-")
    saved, env0 = {k: g[k] for k in patches}, {k: os.environ.get(k) for k in (env or {})}
    out = os.path.join(d, "out")
    lr = None
    if leaf is not None:
        lr = os.path.join(d, "leaf.json")
        with open(lr, "w") as f:
            json.dump(leaf, f)
    try:
        g.update(patches)
        os.environ.update(env or {})
        try:
            import contextlib
            import io
            with contextlib.redirect_stdout(io.StringIO()):  # measure() prints its summary line
                measure(os.path.join(d, "dir"), out, lr)
            j = os.path.join(out, "v3l.json")
            return None, os.path.exists(out), (json.load(open(j)) if os.path.exists(j) else None)
        except Exception as e:  # noqa: BLE001
            return f"{type(e).__name__}: {e}", os.path.exists(out), None
    finally:
        g.update(saved)
        for k, v in env0.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(d, ignore_errors=True)


def _runs_box(lab1, lab2, calls, bad_second=None):
    """_fake_box plus the runs (annex A27's labelling pair): a fake subprocess whose run() records each command in
    CALLS and writes what the command would: a VALID trace (N writes to the data file, and N fsyncs of it on the fsync
    arm), fio's JSON (N writes; N - 1 syncs on the fsync arm) and the data file. BAD_SECOND ("empty" or "uring") spoils
    only the A27 second labelling run's trace. A flush counter rises LAB1 across the first labelling run and LAB2
    across the second (then N + 3 per timed run, 0 on the control), and the sync record passes. Nothing real runs.
    TEST EDIT, flagged (review 7 finding 3): the traces were empty, so the control proved command order only"""
    import types

    def fio(fsync):
        job = {"write": {"total_ios": N, "lat_ns": {"percentile": {"50.000000": 100000.0}}}}
        if fsync:
            job["sync"] = {"lat_ns": {"N": N - 1, "percentile": {"50.000000": 300000.0, "99.000000": 900000.0},
                                      "bins": {"300000": N - 1}}}
        return {"jobs": [job]}

    def trace(data, fsync, bad):
        if bad == "empty":
            return ""
        lines = []
        for i in range(N):
            lines.append(f'4100  1700000000.{i:06d} write(3<{data}>, "\\0"..., 4096) = 4096')
            if fsync:
                lines.append(f"4100  1700000000.{i:06d} fsync(3<{data}>) = 0")
        if bad == "uring":
            lines.append("4100  1700000001.000000 io_uring_setup(8, 0x7ff) = 3<anon_inode:[io_uring]>")
        return "\n".join(lines) + "\n"

    def run(cmd, **_k):
        calls.append(list(cmd))
        fsync, data = "--fsync=1" in cmd, None
        for x in cmd:
            if x.startswith("--output="):
                with open(x[len("--output="):], "w") as f:
                    json.dump(fio(fsync), f)
            if x.startswith("--filename="):
                data = x[len("--filename="):]
                open(data, "w").close()
        if "-o" in cmd:
            tr = cmd[cmd.index("-o") + 1]
            with open(tr, "w") as f:
                f.write(trace(data, fsync, bad_second if tr.endswith("-2.v1l") else None))
        return subprocess.CompletedProcess(cmd, 0, "", "")
    seq = iter([0, lab1, lab1, lab1 + lab2, lab1 + lab2, lab1 + lab2 + N + 3])
    last = [0]

    def flush(_disk):
        last[0] = next(seq, last[0])
        return last[0]
    box = _fake_box()
    box.update({"subprocess": types.SimpleNamespace(run=run, CompletedProcess=subprocess.CompletedProcess,
                                                    TimeoutExpired=subprocess.TimeoutExpired),
                "flush_ios": flush, "sectors_written": lambda _d: 0,
                "sync_record": lambda **_k: {"ran": True, "rc": 0, "layers": [{"path": "/fs0", "rc": 0, "secs": 0.0}]}})
    return box


_LEAF = {"leaf": {"disk": "nvme1n1", "drive_reports": "write back"}, "virtualization": {"virtualized": False}}


def _with_global(name, value, f):
    """f() with module global NAME set to VALUE, restored after (absent before: removed after)"""
    g, had = globals(), name in globals()
    old = g.get(name)
    g[name] = value
    try:
        return f()
    finally:
        if had:
            g[name] = old
        else:
            g.pop(name, None)


def _main_out(argv):
    """main(ARGV) with its stdout captured: (rc, stdout)"""
    import contextlib
    import io
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = main(argv)
    return rc, buf.getvalue()


def _fake_box(iostats="1", chain=("nvme1n1",), disk="nvme1n1"):
    """the module functions measure() reads the box through, faked: a drive under DIR, fio and strace present"""
    return {"resolve_leaf": lambda d: (list(chain), disk, [d]),
            "disk_attr": lambda dk, rel: {"queue/iostats": iostats, "queue/write_cache": "write back"}.get(rel, "x"),
            "sh": lambda *a, **k: (0, {"fio": "fio-3.36\n", "strace": "strace -- version 6.8\n"}.get(a[0], ""), "")}


def _plant(res, name):
    """The named plant's entry in plants()' results, or {} when it was never run."""
    return next((p for p in res if p.get("plant") == name), {})


def self_test():
    data = "/mnt/t3-xfs/v3l-before/v3l-fsync.dat"
    trace = "\n".join([
        "4100  1700000000.000001 openat(AT_FDCWD</root>, \"/mnt/t3-xfs/v3l-before/v3l-fsync.dat\", O_RDWR|O_CREAT, 0644) = 3</mnt/t3-xfs/v3l-before/v3l-fsync.dat>",
        "4100  1700000000.000002 write(3</mnt/t3-xfs/v3l-before/v3l-fsync.dat>, \"\\0\"..., 4096) = 4096",
        "4100  1700000000.000003 fsync(3</mnt/t3-xfs/v3l-before/v3l-fsync.dat> <unfinished ...>",
        "4101  1700000000.000004 write(1</dev/pts/0>, \"x\", 1) = 1",
        "4100  1700000000.000005 <... fsync resumed>) = 0",
        "4100  1700000000.000006 write(3</mnt/t3-xfs/v3l-before/v3l-fsync.dat>, \"\\0\"..., 4096) = 4096",
        "4100  1700000000.000007 fsync(3</mnt/t3-xfs/v3l-before/v3l-fsync.dat>) = 0",
        "4100  1700000000.000008 fsync(5</mnt/t3-xfs/other>) = 0",
        "4100  1700000000.000009 fdatasync(3</mnt/t3-xfs/v3l-before/v3l-fsync.dat>) = -1 EIO (Input/output error)",
        "4100  1700000000.000010 +++ exited with 0 +++",
    ])
    c = parse_v1l(trace, data)
    cases = [("V1L parse: 2 data writes, 2 data fsyncs (one split by <unfinished>), 1 other fsync, 1 fdatasync, "
              "1 failed call, the tty write not counted",
              c == {"data_writes": 2, "data_fsyncs": 2, "other_fsyncs": 1, "failed": 1, "io_uring_setup": 0,
                    "fdatasync": 1, "sync_file_range": 0, "syncfs": 0, "msync": 0})]
    c2 = parse_v1l("4100  1700000000.1 fsync(3</d>) = -1 EIO (Input/output error)\n"
                   "4100  1700000000.2 fsync(3</d> <unfinished ...>\n4100  1700000000.3 <... fsync resumed>) = -1 EIO (x)\n"
                   "4100  1700000000.4 io_uring_setup(8, 0x7ff) = 3<anon_inode:[io_uring]>", "/d")
    cases.append(("V1L parse: failed fsyncs counted on the start line and the resumed line; io_uring_setup seen",
                  c2["failed"] == 2 and c2["data_fsyncs"] == 2 and c2["io_uring_setup"] == 1))

    def rec(fsyncs=N, ctl_fsyncs=0, wc="write back", delta=N + 3, writes=N, other=0, failed=0, uring=0, fio_w=N,
            timed_syncs=N - 1, ctl_fio_syncs=0, layers=None, drive="same", lab_syncs=N - 1, disk="nvme1n1",
            sectors=2 * N * 8, lab_sectors=2 * N * 8, sync="auto", iostats="1", mode="dry", pair=None, slack="A27",
            sync_paths="auto"):
        def arm(fc, syncs, tsyncs):
            v = {"data_writes": writes, "data_fsyncs": fc, "other_fsyncs": other, "failed": failed,
                 "io_uring_setup": uring, "fdatasync": 0, "sync_file_range": 0, "syncfs": 0, "msync": 0}
            return {"v1l": v, "labelling_fio": {"writes": fio_w, "syncs": syncs},
                    "timed": {"writes": fio_w, "syncs": tsyncs, "fsync_p50_us": 300.0, "fsync_bins_ns": {"300000": tsyncs}}}
        # fio's own sync count is whatever it reports (9,999 or 10,000 with end_fsync); only agreement is gated
        r = {"leaf": {"disk": disk, "write_cache": wc, "layers": layers or [],
                      "drive_reports": wc if drive == "same" else drive, "iostats": iostats},
             "arms": {"fsync": arm(fsyncs, lab_syncs, timed_syncs),
                      "control": arm(ctl_fsyncs, ctl_fio_syncs, ctl_fio_syncs)}}
        r["arms"]["fsync"]["timed"]["flush_ios_delta"] = delta
        # the drive's sectors-written count over each fsync run, as measure() records it since item 10's remainder
        # (data plus filesystem metadata: 2 x the data by default)
        r["arms"]["fsync"]["timed"]["sectors_written_delta"] = sectors
        r["arms"]["fsync"]["timed"]["lab_sectors_written_delta"] = lab_sectors
        # the sync before each run, as measure() records it since annex A24. TEST EDIT, flagged (review 6 MED 2,
        # annex A24-AM1): the default record also carries one `sync -f` entry per filesystem under DIR (DIR's, plus one
        # per loop layer's backing file), each rc 0: a new required field at its passing value; no expectation changes
        if sync == "auto":
            sync = {"ran": True, "rc": 0, "layers": [{"path": f"/fs{i}", "rc": 0, "secs": 0.0}
                                                     for i in range(1 + len(layers or []))]}
        if sync is not None:
            r["arms"]["fsync"]["sync"] = {"labelling": json.loads(json.dumps(dict(sync))),
                                          "timed": json.loads(json.dumps(dict(sync)))}
        # TEST EDIT, flagged (review 7 / C21 findings 4, 5): the record names its mode (default "dry": no A27 pair is
        # required, so every existing verdict stands), its SLACK registration, and the sync paths resolve_leaf named
        # (default: the auto sync entries' own paths, in order); PAIR = (first, second[, stored problem]) adds
        # annex A27's labelling pair with a valid second labelling run (a copy of the first)
        if mode is not None:
            r["mode"] = mode
        r["slack_registered"] = slack
        if sync_paths == "auto":
            sync_paths = [f"/fs{i}" for i in range(1 + len(layers or []))]
        if sync_paths is not None:
            r["leaf"]["sync_paths"] = sync_paths
        if pair is not None:
            fa = r["arms"]["fsync"]
            fa["lab_pair"] = {"first": pair[0], "second": pair[1], "problem": pair[2] if len(pair) > 2 else None}
            fa["v1l_2"], fa["labelling_fio_2"] = dict(fa["v1l"]), dict(fa["labelling_fio"])
        return r

    def fake_sync(rc_f=0):
        """a sync runner recording each argv; `sync -f` exits rc_f"""
        calls = []

        def run(argv, **_k):
            calls.append(list(argv))
            f = "-f" in argv
            return subprocess.CompletedProcess(argv, rc_f if f else 0, "", "boom" if f and rc_f else "")
        return calls, run

    def sync_argv_case():
        calls, run = fake_sync()
        r = sync_record(runner=run, paths=["/mnt/t3-xfs/v3l", "/var/tmp/b.img"])
        return (calls == [["sync"], ["sync", "-f", "/mnt/t3-xfs/v3l"], ["sync", "-f", "/var/tmp/b.img"]]
                and [(x["path"], x["rc"]) for x in r["layers"]] == [("/mnt/t3-xfs/v3l", 0), ("/var/tmp/b.img", 0)]
                and all(isinstance(x["secs"], float) for x in r["layers"]))

    def sync_mut(r, run, f):
        f(r["arms"]["fsync"]["sync"][run])
        return r

    cases += [
        ("a clean write-back record is VALID", gates(rec()) == []),
        ("a clean write-through record (counter 0 in the timed run) is VALID", gates(rec(wc="write through", delta=0)) == []),
        ("9,999 fsyncs VOIDs", gates(rec(fsyncs=N - 1)) != []),
        ("one control fsync VOIDs", gates(rec(ctl_fsyncs=1)) != []),
        ("an fsync of another file VOIDs", gates(rec(other=1)) != []),
        ("a failed call VOIDs", gates(rec(failed=1)) != []),
        ("io_uring_setup VOIDs", gates(rec(uring=1)) != []),
        ("10,001 data writes VOIDs", gates(rec(writes=N + 1)) != []),
        ("fio reporting 9,999 writes VOIDs", gates(rec(fio_w=N - 1)) != []),
        ("a timed run whose fio sync count differs from the labelling run's VOIDs", gates(rec(timed_syncs=N - 2)) != []),
        ("a control run where fio reports a sync VOIDs", gates(rec(ctl_fio_syncs=1)) != []),
        ("write-back with 9,999 flushes VOIDs", gates(rec(delta=N - 1)) != []),
        ("write-back with no counter reading VOIDs", gates(rec(delta=None)) != []),
        ("an unreadable write_cache VOIDs", gates(rec(wc=None)) != []),
        ("M1: a timed run with no histogram VOIDs", gates(_nobins(rec())) != []),
        ("M1: a timed run with fio reporting 0 syncs VOIDs (0 == 0 agreement is not enough)",
         gates(rec(timed_syncs=0, lab_syncs=0)) != []),
        ("M3: a write-back loop layer whose counter rose 0 VOIDs",
         gates(rec(layers=[{"name": "loop3", "write_cache": "write back", "flush_ios_delta": 0}])) != []),
        ("M3: a write-back loop layer with 10,000 flushes is VALID",
         gates(rec(layers=[{"name": "loop3", "write_cache": "write back", "flush_ios_delta": N}])) == []),
        ("M3: a write-through loop layer whose counter reads 0 is VALID",
         gates(rec(layers=[{"name": "loop3", "write_cache": "write through", "flush_ios_delta": 0}])) == []),
        ("M6: kernel write through over a drive reporting write back VOIDs",
         gates(rec(wc="write through", delta=0, drive="write back")) != []),
        ("M6: kernel and drive agreeing on write back is VALID", gates(rec(drive="write back")) == []),
        ("LOW 7: a write-through drive with no drive report VOIDs", gates(rec(wc="write through", delta=0, drive=None)) != []),
        ("LOW 7: a write-back drive with an 'unknown' drive report VOIDs", gates(rec(drive="unknown")) != []),
        ("LOW 7: a ram disk (brd) with the probe's 'none (RAM)' is VALID",
         gates(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0")) == []),
        ("LOW 7: a ram disk claiming a drive report VOIDs",
         gates(rec(wc="write through", delta=0, drive="write through", disk="ram0")) != []),
        ("A16: write-through drive with a non-zero counter VOIDs", gates(rec(wc="write through", delta=1)) != []),
        ("A16: write-through loop layer with a non-zero counter VOIDs",
         gates(rec(layers=[{"name": "loop3", "write_cache": "write through", "flush_ios_delta": 3}])) != []),
        ("floor rule: labelling 2 per fsync, timed 1.5 per fsync VOIDs", gates(_lab(rec(delta=15000), 20004)) != []),
        ("floor rule: labelling 2 per fsync, timed 2 per fsync is VALID", gates(_lab(rec(delta=20003), 20004)) == []),
        ("plants on a VALID write-back record: control passes and every plant fires", plants(rec(delta=20003))[1]),
        ("plants on a VALID write-through record: every plant fires", plants(rec(wc="write through", delta=0))[1]),
        ("plants on a write-through record carrying labelling count 0 (as measured now): every plant fires",
         plants(_lab(rec(wc="write through", delta=0), 0))[1]),
        ("plants on a write-back record carrying its labelling count: every plant fires",
         plants(_lab(rec(delta=20003), 20004))[1]),
        ("plants on a VOID record: NOT-RUN", not plants(rec(fsyncs=N - 1))[1]),
        # item 3 (T3 runner review MED 3): the write-back rules apply whatever the labelling count is; a count below N
        # is a labelling run with fewer than one flush per fsync, and VOIDs (PREREG line 180), drive and loop layer
        ("item 3: a write-back drive whose labelling run rose N//2 (timed N+3) VOIDs, on the labelling rule's text",
         _has(gates(_lab(rec(delta=N + 3), N // 2)), "write-back drive nvme1n1",
              "fewer than one per fsync in the strace-checked run")),
        ("item 3: a write-back drive whose labelling count is 0 (timed N+3) VOIDs, on the labelling rule's text",
         _has(gates(_lab(rec(delta=N + 3), 0)), "write-back drive nvme1n1",
              "fewer than one per fsync in the strace-checked run")),
        ("item 3: a write-back drive whose labelling count is N-1 (timed N+3) VOIDs",
         _has(gates(_lab(rec(delta=N + 3), N - 1)), "fewer than one per fsync in the strace-checked run")),
        ("item 3: a write-back drive whose labelling count is N (timed N+3) is VALID", gates(_lab(rec(delta=N + 3), N)) == []),
        ("item 3: a write-back drive whose labelling count is not an int ('20004') VOIDs (unreadable is not absent)",
         _has(gates(_lab(rec(delta=20003), "20004")), "write-back drive nvme1n1", "labelling count unreadable")),
        ("item 3: a write-back loop layer whose labelling run rose N//2 (timed N+3) VOIDs, on the labelling rule's text",
         _has(gates(rec(layers=[{"name": "loop3", "write_cache": "write back", "flush_ios_delta": N + 3,
                                 "lab_flush_ios_delta": N // 2}])),
              "write-back loop layer loop3", "fewer than one per fsync in the strace-checked run")),
        ("item 3: plant wb-lab-short fires on a write-back record, on the labelling rule's text",
         _ok(lambda: _plant(plants(rec(delta=20003))[0], "wb-lab-short").get("fired") is True
             and _has(_plant(plants(rec(delta=20003))[0], "wb-lab-short")["got"],
                      "fewer than one per fsync in the strace-checked run"))),
        ("item 3: plant wb-lab-short fires on a write-back record carrying its labelling count",
         _ok(lambda: _plant(plants(_lab(rec(delta=20003), 20004))[0], "wb-lab-short").get("fired") is True)),
        ("item 3: plant wb-lab-short fires on a write-through record carrying labelling count 0",
         _ok(lambda: _plant(plants(_lab(rec(wc="write through", delta=0), 0))[0], "wb-lab-short").get("fired") is True)),
        ("item 3: the write-back plant base keeps a real write-back drive's labelling count (N//2 stays N//2)",
         _ok(lambda: as_state(_lab(rec(delta=N + 3), N // 2), "write back")
             ["arms"]["fsync"]["timed"]["lab_flush_ios_delta"] == N // 2)),
        ("item 3: the write-back plant base keeps a real write-back layer's labelling count (N//2 stays N//2)",
         _ok(lambda: as_state(rec(layers=[{"name": "loop3", "write_cache": "write back", "flush_ios_delta": N + 3,
                                           "lab_flush_ios_delta": N // 2}]), "write back")
             ["leaf"]["layers"][0]["lab_flush_ios_delta"] == N // 2)),
        ("item 3: a write-back base derived from a write-through record (counts 0) synthesises 2 per fsync in both runs",
         _ok(lambda: [as_state(_lab(rec(wc="write through", delta=0), 0), "write back")["arms"]["fsync"]["timed"][k]
                      for k in ("flush_ios_delta", "lab_flush_ios_delta")] == [2 * N, 2 * N])),
        ("item 3: a write-back base over a record with no labelling count fills it from the timed count (20003)",
         _ok(lambda: as_state(rec(delta=20003), "write back")["arms"]["fsync"]["timed"]["lab_flush_ios_delta"] == 20003)),
        # item 4 (T3 runner review MED 4): each half of A16 has a case that only it can fail, matched on its own text
        ("item 4a: a write-through drive with timed 0, labelling 1 VOIDs, on the labelling half's text",
         _has(gates(_lab(rec(wc="write through", delta=0), 1)), "write-through drive nvme1n1",
              "in the labelling run; a write-through queue")),
        ("item 4a: a write-through drive with timed 1, labelling 0 VOIDs, on the timed half's text",
         _has(gates(_lab(rec(wc="write through", delta=1), 0)), "write-through drive nvme1n1",
              "in the timed run; a write-through queue")),
        ("item 4a: a write-through loop layer with timed 0, labelling 1 VOIDs, on the labelling half's text",
         _has(gates(rec(layers=[{"name": "loop3", "write_cache": "write through", "flush_ios_delta": 0,
                                 "lab_flush_ios_delta": 1}])),
              "write-through loop layer loop3", "in the labelling run; a write-through queue")),
        ("item 4b: a write-back loop layer with labelling 2N, timed 1.5N VOIDs, on the floor rule's text",
         _has(gates(rec(layers=[{"name": "loop3", "write_cache": "write back", "flush_ios_delta": 3 * N // 2,
                                 "lab_flush_ios_delta": 2 * N}])),
              "write-back loop layer loop3", "per fsync the labelling run showed")),
        ("item 4: plant wt-lab-flush fires on a write-back record, on the labelling half's text",
         _ok(lambda: _plant(plants(rec(delta=20003))[0], "wt-lab-flush").get("fired") is True
             and _has(_plant(plants(rec(delta=20003))[0], "wt-lab-flush")["got"], "write-through drive nvme1n1",
                      "in the labelling run; a write-through queue"))),
        ("item 4: plant wt-lab-flush fires on a write-through record carrying labelling count 0",
         _ok(lambda: _plant(plants(_lab(rec(wc="write through", delta=0), 0))[0], "wt-lab-flush").get("fired") is True)),
        ("item 4: plant wb-layer-half fires on a record with no loop layer, on the floor rule's text for a loop layer",
         _ok(lambda: _plant(plants(rec(delta=20003))[0], "wb-layer-half").get("fired") is True
             and _has(_plant(plants(rec(delta=20003))[0], "wb-layer-half")["got"], "write-back loop layer",
                      "per fsync the labelling run showed"))),
        ("item 4: plant wb-layer-half fires on a record with a real write-back loop layer",
         _ok(lambda: _plant(plants(_lab(rec(delta=20003, layers=[{"name": "loop3", "write_cache": "write back",
                                                                  "flush_ios_delta": 20003,
                                                                  "lab_flush_ios_delta": 20004}]), 20004))[0],
                            "wb-layer-half").get("fired") is True)),
        ("item 4: plant wb-layer-half fires on a write-through record carrying labelling count 0",
         _ok(lambda: _plant(plants(_lab(rec(wc="write through", delta=0), 0))[0], "wb-layer-half").get("fired") is True)),
        # item 18 (T3 runner review LOW 18): the floor is proportional, timed per fsync >= labelling per fsync x (1 -
        # SLACK), not the whole-number floor lab // N that let ~47% flush loss pass at 1.9 per fsync
        ("item 18: labelling 1.9 per fsync (19000), timed 1.0003 per fsync (10003) VOIDs, on the floor rule's text",
         _has(gates(_lab(rec(delta=N + 3), 19000)), "write-back drive nvme1n1", "per fsync the labelling run showed")),
        ("item 18: a loop layer at labelling 1.9 per fsync, timed 1.0003 VOIDs, on the floor rule's text",
         _has(gates(rec(layers=[{"name": "loop3", "write_cache": "write back", "flush_ios_delta": N + 3,
                                 "lab_flush_ios_delta": 19000}])),
              "write-back loop layer loop3", "per fsync the labelling run showed")),
        ("item 18: labelling 2 per fsync (20000), timed exactly 0.95 of it (19000) is VALID (the slack's edge)",
         gates(_lab(rec(delta=19000), 20000)) == []),
        ("item 18: labelling 2 per fsync (20000), timed 18999 (just under 0.95 of it) VOIDs",
         _has(gates(_lab(rec(delta=18999), 20000)), "per fsync the labelling run showed")),
        # TEST EDIT, flagged as a SPEC CHANGE (annex A27 registers SLACK): the constant's comment now names its row
        ("item 18: SLACK is one named constant, 0.05, marked registered as annex A27",
         _ok(lambda: float(SLACK) == 0.05 and re.search(r"^SLACK = .*registered: annex A27",
                                                         open(__file__).read(), re.M) is not None)),
        ("item 18: the record publishes both ratios and the slack (drive 2.0003 timed, 2.0004 labelling; layer 1.5, 2.0)",
         _ok(lambda: (lambda p: p["flush_ios_per_fsync"] == 2.0003 and p["lab_flush_ios_per_fsync"] == 2.0004
                      and p["floor_slack"] == 0.05
                      and p["layers"] == {"loop3": {"flush_ios_per_fsync": 1.5, "lab_flush_ios_per_fsync": 2.0}})(
             publish(_lab(rec(delta=20003, layers=[{"name": "loop3", "write_cache": "write back",
                                                    "flush_ios_delta": 15000, "lab_flush_ios_delta": 20000}]),
                          20004))))),
        ("item 18: a missing count publishes None, not a crash",
         _ok(lambda: (lambda p: p["flush_ios_per_fsync"] is None and p["lab_flush_ios_per_fsync"] is None)(
             publish(rec(delta=None))))),
        # item 19 (T3 runner review LOW 19): floor_kind names only what was checked, one branch per state; an unknown
        # or VOID state never reads "counter 0 checked"; the module docstring names every text the code writes
        ("item 19: floor_kind of a ram disk (brd) is the brd text",
         _ok(lambda: floor_kind(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0"))
             == "brd: no drive (dry runs only, never credited)")),
        ("item 19: floor_kind of a write-back drive is the write-back text",
         _ok(lambda: floor_kind(_lab(rec(delta=20003), 20004)) == "write-back drive: flush requests completed by the "
             "driver (issued and acknowledged, not persistence)")),
        ("item 19: floor_kind of a write-through drive at 0 in both runs says counter 0 checked in both runs",
         _ok(lambda: floor_kind(_lab(rec(wc="write through", delta=0), 0))
             == "no volatile cache: no flush request sent (counter 0 checked in both runs)")),
        ("item 19: floor_kind of a write-through drive with no labelling count says the timed run only",
         _ok(lambda: floor_kind(rec(wc="write through", delta=0))
             == "no volatile cache: no flush request sent (counter 0 checked in the timed run; no labelling count)")),
        ("item 19: floor_kind of a write-through drive whose timed counter rose is VOID, not 'counter 0 checked'",
         _ok(lambda: floor_kind(_lab(rec(wc="write through", delta=1), 0))
             == "write-through queue with flushes counted (VOID)")),
        ("item 19: floor_kind of a write-through drive whose labelling counter rose is VOID, not 'counter 0 checked'",
         _ok(lambda: floor_kind(_lab(rec(wc="write through", delta=0), 1))
             == "write-through queue with flushes counted (VOID)")),
        ("item 19: floor_kind of an unreadable write_cache (None) is 'write cache unknown (VOID)'",
         _ok(lambda: floor_kind(rec(wc=None, delta=0)) == "write cache unknown (VOID)")),
        ("item 19: floor_kind of an unknown write_cache ('write through, sometimes') is 'write cache unknown (VOID)'",
         _ok(lambda: floor_kind(rec(wc="write through, sometimes", delta=0)) == "write cache unknown (VOID)")),
        ("item 19: floor_kind appends the virtualization note to its branch's text",
         _ok(lambda: floor_kind(dict(_lab(rec(delta=20003), 20004), virtualization={"virtualized": "kvm"}))
             == "write-back drive: flush requests completed by the driver (issued and acknowledged, not persistence); "
                "virtualized 'kvm': reach to media unknown")),
        # TEST EDIT, flagged (review 5 LOW 23): the new VOID text joins the list; a strengthening
        ("item 19: the module docstring names every floor_kind text the code writes",
         all(s in " ".join((__doc__ or "").split()) for s in (
             "brd: no drive (dry runs only, never credited)",
             "write-back drive: flush requests completed by the driver (issued and acknowledged, not persistence)",
             "write-back drive with flushes short (VOID)",
             "no volatile cache: no flush request sent (counter 0 checked in both runs)",
             "no volatile cache: no flush request sent (counter 0 checked in the timed run; no labelling count)",
             "write-through queue with flushes counted (VOID)",
             "write cache unknown (VOID)"))),
        # fourth lane review HIGH 3: on a ram disk the gate wants the probe's 'none (RAM)', so a plant base must keep it;
        # the banked record is dry run 37812992594's brd job, xfs, b0 (testdata/README.md)
        ("HIGH 3: the banked brd record (run 37812992594, xfs b0: ram0, write through, 'none (RAM)') is VALID",
         _ok(lambda: gates(json.load(open(_BRD_B0))) == [])),
        ("HIGH 3: plants() on the banked brd record runs every plant and every one fires (ok True)",
         _ok(lambda: plants(json.load(open(_BRD_B0)))[1] is True)),
        ("HIGH 3: drive-mismatch on the banked brd record flips the report and fires on the agreement rule's text",
         _ok(lambda: (lambda pl: pl.get("fired") is True and _has(pl["got"], "drive ram0:",
                                                                  "disagrees with the drive's own report"))(
             _plant(plants(json.load(open(_BRD_B0)))[0], "drive-mismatch")))),
        ("HIGH 3: plants() on a hand-made ram0 record (write through, 'none (RAM)', counts 0) is ok True",
         _ok(lambda: plants(_lab(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0"), 0))[1] is True)),
        ("HIGH 3: a ram0 plant base keeps 'none (RAM)' in both states",
         _ok(lambda: [as_state(_lab(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0"), 0), wc)
                      ["leaf"]["drive_reports"] for wc in ("write back", "write through")] == ["none (RAM)"] * 2)),
        # T3 runner review item 10 (its remainder): over a write-through drive the flush counter reads 0 whether or not
        # an fsync reached the drive (a write-through loop above it sends none either), so the drive's sectors-written
        # counter must show at least the fsynced data, N x 4 KiB = N x 8 sectors, inside EACH fsync run
        ("item 10: a write-through drive whose timed fsync run wrote half the data's sectors VOIDs, on the write rule's text",
         _has(gates(rec(wc="write through", delta=0, sectors=N * 4)), "drive nvme1n1:", "in the timed fsync run",
              "did not reach the drive")),
        ("item 10: a write-through drive whose labelling run wrote half the data's sectors VOIDs, on the write rule's text",
         _has(gates(rec(wc="write through", delta=0, lab_sectors=N * 4)), "drive nvme1n1:", "in the labelling fsync run",
              "did not reach the drive")),
        ("item 10: a write-back drive whose timed run wrote half the data's sectors VOIDs (the rule holds in every state)",
         _has(gates(rec(sectors=N * 4)), "drive nvme1n1:", "did not reach the drive")),
        ("item 10: a record with no sectors-written count VOIDs", _has(gates(rec(sectors=None)), "drive nvme1n1:",
                                                                       "no sectors-written count")),
        ("item 10: a sectors-written count that is not an int ('160000') VOIDs",
         _has(gates(rec(lab_sectors="160000")), "drive nvme1n1:", "no sectors-written count")),
        ("item 10: exactly N x 8 sectors (the data alone) in both runs is VALID (the rule's edge)",
         gates(rec(sectors=N * 8, lab_sectors=N * 8)) == []),
        ("item 10: N x 8 - 1 sectors in the timed run VOIDs", _has(gates(rec(sectors=N * 8 - 1)), "did not reach the drive")),
        ("item 10: a ram disk (brd) with no sectors-written count is VALID (brd: dry runs only, never credited)",
         gates(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0", sectors=None, lab_sectors=None)) == []),
        ("item 10: plant wt-unwritten fires on a write-back record, on the write rule's text",
         _ok(lambda: (lambda pl: pl.get("fired") is True and _has(pl["got"], "drive nvme1n1:", "did not reach the drive"))(
             _plant(plants(rec(delta=20003))[0], "wt-unwritten")))),
        ("item 10: plant wt-unwritten fires on a write-through record carrying labelling count 0",
         _ok(lambda: _plant(plants(_lab(rec(wc="write through", delta=0), 0))[0], "wt-unwritten").get("fired") is True)),
        # review 5 MED 2: with queue/iostats 0 the sectors-written counter never moves (flushes are counted either
        # way), so such a leaf VOIDs on its own text, not as data that "did not reach the drive"
        ("MED 2: a leaf with iostats 0 VOIDs on the iostats text, not on 'did not reach the drive'",
         _ok(lambda: (lambda g: _has(g, "drive nvme1n1:", "iostats") and not _has(g, "did not reach the drive"))(
             gates(rec(iostats="0", sectors=0, lab_sectors=0))))),
        ("MED 2: a leaf with no iostats recorded VOIDs", _has(gates(rec(iostats=None)), "drive nvme1n1:", "iostats")),
        ("MED 2: a ram disk (brd) with no iostats recorded is VALID (exempt with the write rule)",
         gates(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0", iostats=None)) == []),
        # TEST EDIT, flagged (review 6 LOW 10): renamed; it calls iostats_problem(), not measure(); the measure() case
        # follows it. Its body is unchanged
        ("MED 2: iostats_problem() names iostats 0 and an unreadable iostats; 1 passes",
         _ok(lambda: iostats_problem("1") is None and "iostats off" in (iostats_problem("0") or "")
             and "cannot read" in (iostats_problem(None) or ""))),
        # annex A24 (the lead's ruling): every run is preceded by a sync outside its window, and the record says it ran
        # and its rc; on a drive, an fsync-arm run without a sync record, or with a failed one, VOIDs
        ("A24: an fsync-arm record with no sync record VOIDs, on the sync rule's text",
         _has(gates(rec(sync=None)), "drive nvme1n1:", "no sync record")),
        ("A24: a sync that failed (rc 1) before the timed run VOIDs",
         _ok(lambda: _has(gates((lambda r: (r["arms"]["fsync"]["sync"]["timed"].__setitem__("rc", 1), r)[1])(rec())),
                          "drive nvme1n1:", "the sync before the timed fsync run"))),
        ("A24: a ram disk (brd) with no sync record is VALID (exempt, as from the write rule)",
         gates(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0", sync=None)) == []),
        ("A24: sync_record() on a sync that exits 0 records it ran and rc 0",
         _ok(lambda: (lambda r: r["ran"] is True and r["rc"] == 0 and isinstance(r["secs"], float))(
             sync_record(lambda *a, **k: subprocess.CompletedProcess(a, 0, "", ""))))),
        ("A24: sync_record() on a sync that exits 1 refuses the measurement",
         _ok(lambda: _raises(lambda: sync_record(lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "boom")),
                             "sync before a V3L run failed"))),
        # review 6 MED 2 (annex A24-AM1): sync(1) with no operand calls the void sync(2), so its rc is 0 by construction;
        # the rc that carries a writeback error is `sync -f` (syncfs) on each filesystem under DIR, top-down
        ("A24-AM1: sync_record(paths) runs sync(1), then sync -f per filesystem under DIR, top-down, recording each",
         _ok(sync_argv_case)),
        ("A24-AM1: a sync -f that exits 1 refuses the measurement, naming the filesystem",
         _ok(lambda: _raises(lambda: sync_record(runner=fake_sync(1)[1], paths=["/var/tmp/b.img"]), "/var/tmp/b.img"))),
        ("A24-AM1: a record whose sync -f failed on one filesystem VOIDs, naming it",
         _ok(lambda: _has(gates(sync_mut(rec(), "timed", lambda y: y["layers"][-1].__setitem__("rc", 1))),
                          "drive nvme1n1:", "the sync before the timed fsync run", "/fs0"))),
        ("A24-AM1: a loop record whose labelling sync skipped the backing file's filesystem VOIDs",
         _ok(lambda: _has(gates(sync_mut(rec(layers=[{"name": "loop3", "write_cache": "write through",
                                                      "flush_ios_delta": 0}]), "labelling", lambda y: y["layers"].pop())),
                          "drive nvme1n1:", "the sync before the labelling fsync run", "filesystems"))),
        # review 6 LOW 10: three checks each deletable with the self-test green
        ("LOW 10: a record whose labelling sync failed (rc 1) VOIDs on the labelling run's text",
         _ok(lambda: _has(gates(sync_mut(rec(), "labelling", lambda y: y.__setitem__("rc", 1))),
                          "drive nvme1n1:", "the sync before the labelling fsync run did not run clean"))),
        ("LOW 10: plant unsynced fires on a write-back record, on the timed run's sync text",
         _ok(lambda: (lambda pl: pl.get("fired") is True and _has(pl["got"], "drive nvme1n1:",
                                                                  "the sync before the timed fsync run"))(
             _plant(plants(rec(delta=20003))[0], "unsynced")))),
        ("LOW 10: plant unsynced-lab fires on a write-back record, on the labelling run's sync text",
         _ok(lambda: (lambda pl: pl.get("fired") is True and _has(pl["got"], "drive nvme1n1:",
                                                                  "the sync before the labelling fsync run"))(
             _plant(plants(rec(delta=20003))[0], "unsynced-lab")))),
        ("LOW 10: measure() on a leaf with iostats 0 refuses before it makes OUT",
         _ok(lambda: (lambda r: r[0] is not None and "iostats off" in r[0] and r[1] is False)(
             _measure_with(_fake_box(iostats="0"), env={"V3L_REAL": ""})))),
        # review 5 LOW 25: measure()'s real-mode refusal (V3L_REAL=1 needs a drive and the V3 leaf record)
        ("LOW 25: measure() with V3L_REAL=1 and no leaf record refuses before it makes OUT",
         _ok(lambda: (lambda r: r[0] is not None and "needs the V3 batch's leaf record" in r[0] and r[1] is False)(
             _measure_with(_fake_box(), env={"V3L_REAL": "1"})))),
        ("LOW 25: measure() with V3L_REAL=1 on a loop refuses before it makes OUT",
         _ok(lambda: (lambda r: r[0] is not None and "dry-run only" in r[0] and r[1] is False)(
             _measure_with(_fake_box(chain=("loop0", "sda1"), disk="sda"), env={"V3L_REAL": "1"})))),
        # review 5 LOW 29: SLACK can gate a billed run, so a real run refuses until a PREREG annex row registers it;
        # the registration check is also t3run's preflight (v3l.py registration), before deps and build are paid for
        # TEST EDIT, flagged as a SPEC CHANGE (annex A27 registers SLACK): the two LOW 29 cases that read the
        # unregistered state now set SLACK_REGISTERED to None for their call; expectations unchanged
        ("LOW 29: a real run on a drive with its leaf record refuses while SLACK is unregistered",
         _ok(lambda: "SLACK" in (_with_global("SLACK_REGISTERED", None,
                                              lambda: real_mode_problem("1", ["nvme1n1"], "nvme1n1", "leaf.json"))
                                 or ""))),
        ("LOW 29: with SLACK registered (an annex row named), the same real run is allowed",
         _ok(lambda: _with_global("SLACK_REGISTERED", "A99 (self-test)",
                                  lambda: real_mode_problem("1", ["nvme1n1"], "nvme1n1", "leaf.json")) is None)),
        ("LOW 29: registration_problem() names the unregistered SLACK; with a row named it is None",
         _ok(lambda: "unregistered" in (_with_global("SLACK_REGISTERED", None, registration_problem) or "")
             and _with_global("SLACK_REGISTERED", "A99 (self-test)", registration_problem) is None)),
        # annex A27 (the lead's ruling, DECISIONS cb6074aa3a): SLACK = 0.05 is registered, bound to a measurement: every
        # real run takes two labelling runs back to back and refuses (rc 2, before any timed run) when their drive
        # flush counts differ by more than SLACK/2 of the larger
        ("A27: SLACK_REGISTERED is 'A27', so registration_problem() is None",
         _ok(lambda: SLACK_REGISTERED == "A27" and registration_problem() is None)),
        ("A27: `v3l.py registration` exits 0 and names A27",
         _ok(lambda: (lambda r: r[0] == 0 and "A27" in r[1])(_main_out(["v3l.py", "registration"])))),
        ("A27: a labelling pair of 20000 and 20000 binds SLACK", _ok(lambda: lab_pair_problem(20000, 20000) is None)),
        ("A27: 20000 and 20500 (500 <= 0.025 x 20500 = 512.5) binds SLACK",
         _ok(lambda: lab_pair_problem(20000, 20500) is None)),
        ("A27: 20000 and 20600 (600 > 0.025 x 20600 = 515) refuses, naming A27",
         _ok(lambda: "A27" in (lab_pair_problem(20000, 20600) or ""))),
        ("A27: 0 and 0 (a write-through drive) binds SLACK", _ok(lambda: lab_pair_problem(0, 0) is None)),
        ("A27: a pair with a count that is not an int refuses", _ok(lambda: lab_pair_problem("20000", 20000) is not None)),
        ("A27: measure(), real, a labelling pair 20000 and 21000 refuses (A27) after exactly the two labelling runs",
         _ok(lambda: (lambda calls: (lambda r: r[0] is not None and "A27" in r[0] and len(calls) == 2
                                     and all(c[0] == "strace" for c in calls))(
             _measure_with(_runs_box(20000, 21000, calls), env={"V3L_REAL": "1"}, leaf=_LEAF)))([]))),
        # review 7 finding 3: A27's second labelling run is judged AS a labelling run (V1L and fio) before it can bind
        # SLACK or start a timed run; a subprocess rc 0 is not that proof
        ("finding 3: measure(), real, pair 20000/20000 but the second labelling trace empty: refuses naming the "
         "second labelling run, after only the two labelling runs",
         _ok(lambda: (lambda calls: (lambda r: r[0] is not None and "second labelling" in r[0] and len(calls) == 2)(
             _measure_with(_runs_box(20000, 20000, calls, "empty"), env={"V3L_REAL": "1"}, leaf=_LEAF)))([]))),
        ("finding 3: measure(), real, the second labelling run shows io_uring_setup: refuses before the timed run",
         _ok(lambda: (lambda calls: (lambda r: r[0] is not None and "second labelling" in r[0] and len(calls) == 2)(
             _measure_with(_runs_box(20000, 20000, calls, "uring"), env={"V3L_REAL": "1"}, leaf=_LEAF)))([]))),
        ("finding 3: a record whose second labelling run made 9,999 data fsyncs VOIDs on the second run's text",
         _ok(lambda: _has(gates(dict(rec(), arms=dict(rec()["arms"], fsync=dict(
             rec()["arms"]["fsync"], v1l_2=dict(rec()["arms"]["fsync"]["v1l"], data_fsyncs=N - 1),
             labelling_fio_2=dict(rec()["arms"]["fsync"]["labelling_fio"]))))),
             "V1L fsync (A27 second labelling):", "fsync of the data file"))),
        # review 7 / C21 finding 4: a stored record earns A27 credit only from its own counts: it names its mode; a real
        # one names the registration this code implements and carries a pair whose first count is its labelling
        # count and which re-derives within SLACK/2 (a stored problem text is never read); a dry one keeps its label
        ("finding 4: a real record with no A27 labelling pair VOIDs",
         _ok(lambda: _has(gates(rec(mode="real")), "drive nvme1n1:", "no A27 labelling pair"))),
        ("finding 4: a real record whose pair is 20004/21004 with a stored problem of null VOIDs on the re-derived A27 text",
         _ok(lambda: _has(gates(_lab(rec(delta=20003, mode="real", pair=(20004, 21004, None)), 20004)),
                          "drive nvme1n1:", "annex A27"))),
        ("finding 4: a real record with a bound pair (20004/20004, its labelling count) is VALID",
         _ok(lambda: gates(_lab(rec(delta=20003, mode="real", pair=(20004, 20004)), 20004)) == [])),
        ("finding 4: a real record whose pair's first count (19000) is not its labelling count (20004) VOIDs",
         _ok(lambda: _has(gates(_lab(rec(delta=20003, mode="real", pair=(19000, 19000)), 20004)),
                          "drive nvme1n1:", "is not its labelling count"))),
        ("finding 4: a real record naming another SLACK registration (A99) VOIDs",
         _ok(lambda: _has(gates(_lab(rec(delta=20003, mode="real", pair=(20004, 20004), slack="A99"), 20004)),
                          "drive nvme1n1:", "SLACK registration"))),
        ("finding 4: a drive record that names no mode VOIDs",
         _ok(lambda: _has(gates(rec(mode=None)), "drive nvme1n1:", "names no mode"))),
        ("finding 4: a dry record keeps its label: no pair needed, and publish says slack_bound false",
         _ok(lambda: gates(rec()) == [] and publish(rec())["slack_bound"] is False)),
        ("finding 4: publish of a real record with a bound pair says slack_bound true",
         _ok(lambda: publish(_lab(rec(delta=20003, mode="real", pair=(20004, 20004)), 20004))["slack_bound"] is True)),
        ("finding 4/5: measure() records mode real, slack_registered A27 and leaf.sync_paths (DIR alone, a drive)",
         _ok(lambda: (lambda r: r[0] is None and r[2] is not None and r[2].get("mode") == "real"
                      and r[2].get("slack_registered") == "A27"
                      and [p.endswith("/dir") for p in r[2]["leaf"].get("sync_paths") or []] == [True])(
             _measure_with(_runs_box(20000, 20000, []), env={"V3L_REAL": "1"}, leaf=_LEAF)))),
        # review 7 / C21 finding 5: A24-AM1's coverage is judged by path identity, in order, not by a count
        ("finding 5: a loop record whose two sync entries are both DIR (the backing file's filesystem missing) VOIDs",
         _ok(lambda: _has(gates(sync_mut(rec(layers=[{"name": "loop3", "write_cache": "write through",
                                                      "flush_ios_delta": 0}]), "timed",
                                         lambda y: y.__setitem__("layers", [dict(y["layers"][0]), dict(y["layers"][0])]))),
                          "drive nvme1n1:", "the sync before the timed fsync run synced", "in order"))),
        ("finding 5: a direct-drive record whose one sync entry has no path VOIDs",
         _ok(lambda: _has(gates(sync_mut(rec(), "timed", lambda y: y["layers"][0].pop("path"))),
                          "drive nvme1n1:", "in order"))),
        ("finding 5: a record whose leaf names no sync paths VOIDs",
         _ok(lambda: _has(gates(rec(sync_paths=None)), "drive nvme1n1:", "names no sync paths"))),
        ("finding 5: plants sync-wrong-path, sync-missing-path and sync-dup-path fire, each on the path text",
         _ok(lambda: (lambda res: all(_plant(res, n).get("fired") is True
                                      and _has(_plant(res, n)["got"], "drive nvme1n1:", "in order")
                                      for n in ("sync-wrong-path", "sync-missing-path", "sync-dup-path")))(
             plants(rec(delta=20003))[0]))),
        ("A27: measure(), real, a labelling pair 20000 and 20000 goes on: the third run is the timed fio",
         _ok(lambda: (lambda calls: (lambda r: r[0] is None and len(calls) >= 3 and calls[0][0] == calls[1][0] == "strace"
                                     and calls[2][0] == "fio")(
             _measure_with(_runs_box(20000, 20000, calls), env={"V3L_REAL": "1"}, leaf=_LEAF)))([]))),
        # review 5 LOW 23: a write-back record whose flush counts VOID never reads as "flush requests completed"
        ("LOW 23: floor_kind of a write-back record with 0 flushes is the VOID text",
         _ok(lambda: floor_kind(rec(delta=0)) == "write-back drive with flushes short (VOID)")),
        # review 5 LOW 24: a timed count that is not an int is unreadable and VOIDs, as a labelling one does (it raised
        # TypeError, which block() reports as MISSING)
        ("LOW 24: a write-back record whose timed count is the string '20003' VOIDs on the unreadable text",
         _ok(lambda: _has(gates(rec(delta="20003")), "write-back drive nvme1n1:", "timed count unreadable"))),
        ("A24-AM1: plant unsynced-layer fires on a write-back record, on the failed filesystem's text",
         _ok(lambda: (lambda pl: pl.get("fired") is True and _has(pl["got"], "drive nvme1n1:",
                                                                  "the sync before the timed fsync run", "failed on"))(
             _plant(plants(rec(delta=20003))[0], "unsynced-layer")))),
        # review 5 MED 4: the write rule's labelling half gets its own plant, matched on its own text
        ("MED 4: plant wt-unwritten-lab fires on a write-back record, on the labelling half's text",
         _ok(lambda: (lambda pl: pl.get("fired") is True and _has(pl["got"], "drive nvme1n1:", "in the labelling fsync run",
                                                                  "did not reach the drive"))(
             _plant(plants(rec(delta=20003))[0], "wt-unwritten-lab")))),
        ("MED 4: plant wt-unwritten-lab fires on a write-through record carrying labelling count 0",
         _ok(lambda: _plant(plants(_lab(rec(wc="write through", delta=0), 0))[0], "wt-unwritten-lab").get("fired") is True)),
        ("item 10: plants() on a ram record runs no wt-unwritten plant and is still ok",
         _ok(lambda: (lambda res: res[1] is True and _plant(res[0], "wt-unwritten") == {})(
             plants(_lab(rec(wc="write through", delta=0, drive="none (RAM)", disk="ram0"), 0))))),
        ("pooled p50 of {100:3} and {200:3, 300:1}: 200", pooled_p50_ns([{"100": 3}, {"200": 3, "300": 1}]) == 200),
        ("pooled p50 with a missing histogram: None", pooled_p50_ns([{"100": 3}, None]) is None),
        ("block with a missing after file: MISSING", block("/nonexistent/b.json", "/nonexistent/a.json")["verdict"] == "MISSING"),
        # T3 runner review item 6: block() re-derives the verdict from the record's own arms (review L5), so a file
        # labelled VALID over VOID arms reads VOID on that rule's text; a truthful VALID file is the negative control
        ("item 6: block() over a file labelled VALID with VOID arms (9,999 fsyncs) is VOID, on the re-derivation's text",
         _ok(lambda: (lambda r: r["verdict"] == "VOID"
                      and _has(r["before"]["void_reasons"], "disagrees with the gates re-run"))(
             _block_of(dict(rec(fsyncs=N - 1), verdict="VALID", void_reasons=[], floor_kind="x",
                            published={"fsync_p50_us": 300.0, "fsync_over_control_write_p50": 1.0}))))),
        # T3 runner review item 14: a real run (V3L_REAL=1) must carry the V3 batch's leaf record, so the drive's own
        # cache report is always there to cross-check; a dry run may go without it
        ("item 14: a real run without a leaf record is refused",
         _ok(lambda: "leaf record" in (real_mode_problem("1", ["nvme1n1"], "nvme1n1", None) or ""))),
        # TEST EDIT, flagged as a SPEC CHANGE (review 5 LOW 29): a real run now also needs SLACK registered, so this
        # case states that premise (SLACK_REGISTERED set for the call); its expectation (passes) is unchanged, and the
        # LOW 29 case above pins the unregistered refusal
        ("item 14: a real run with a leaf record on a drive passes (SLACK registered)",
         _ok(lambda: _with_global("SLACK_REGISTERED", "A99 (self-test)",
                                  lambda: real_mode_problem("1", ["nvme1n1"], "nvme1n1", "/x/summary.json")) is None)),
        ("item 14: a real run on a loop or a ram disk is refused (the earlier rule, kept)",
         _ok(lambda: real_mode_problem("1", ["loop3", "nvme1n1"], "nvme1n1", "/x/summary.json") is not None
             and real_mode_problem("1", ["ram0"], "ram0", "/x/summary.json") is not None)),
        ("item 14: a dry run (V3L_REAL unset) needs no leaf record",
         _ok(lambda: real_mode_problem(None, ["loop3", "nvme1n1"], "nvme1n1", None) is None)),
        ("item 6: block() over a truthful VALID file is VALID (the negative control)",
         _ok(lambda: _block_of(dict(rec(), verdict="VALID", void_reasons=[], floor_kind="x",
                                    published={"fsync_p50_us": 300.0, "fsync_over_control_write_p50": 1.0}))
             ["verdict"] == "VALID")),
    ]
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"V3L self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"V3L SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(a):
    try:
        if a[1:] == ["self-test"]:
            return self_test()
        if len(a) in (4, 5) and a[1] == "measure":
            return measure(a[2], a[3], a[4] if len(a) == 5 else None)
        if len(a) == 4 and a[1] == "plants":
            res, ok = plants(json.load(open(a[2])))
            json.dump({"plants": res, "all_fired": ok}, open(a[3], "w"), indent=1, default=str)
            print(json.dumps({"all_fired": ok, "fired": {p["plant"]: p["fired"] for p in res}}))
            return 0 if ok else 1
        if a[1:] == ["registration"]:
            why = registration_problem()
            print(why or f"SLACK {float(SLACK)} registered: {SLACK_REGISTERED}")
            return 2 if why else 0
        if len(a) == 4 and a[1] == "block":
            r = block(a[2], a[3])
            print(json.dumps(r, indent=1))
            return {"VALID": 0, "VOID": 3}.get(r["verdict"], 2)
    except Exception as e:
        print(f"v3l: REFUSED: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
