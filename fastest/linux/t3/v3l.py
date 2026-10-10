#!/usr/bin/env python3
"""V3L, the registered T3 floor (PREREG-v1-FINAL-CANDIDATE line 180): one measurement, before or after a block.

  v3l.py measure DIR OUT [LEAFREC]
                                run it on the filesystem holding DIR; write OUT/v3l.json and the raw fio and strace
                                files; exit 0 VALID, 3 VOID (a registered gate failed), 2 refused (nothing measured).
                                LEAFREC: the block's V3 batch summary.json, whose leaf record carries the drive's own
                                cache report (the probe's NVMe VWC / SCSI WCE / virtio read): V3L refuses another disk,
                                and VOIDs a kernel write_cache that disagrees with the drive (gate-6 review M6)
  v3l.py block BEFORE AFTER     one block's record from its two v3l.json files: the pooled fsync p50 (the block's
                                normaliser), the ratio and the drift (published, never gates); exit 0 when both are
                                VALID, 3 when either is VOID, 2 when either is missing or unreadable
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
Gates (any failure makes the measurement VOID, and the block with it):
  - V1L: exactly 10,000 writes to the data file on each arm; 10,000 fsync of the data file on the fsync arm and no
    fsync on the control; no fdatasync, sync_file_range, syncfs or msync on either; no failed write or fsync; no
    io_uring_setup (line 173 refuses such a labelling run);
  - fio reports exactly 10,000 writes on every run, no sync on the control's, and on the fsync arm the same sync count
    in the timed run as in the V1L-checked labelling run (the timed run has no tracer; this ties it to the checked one);
  - the drive's queue/write_cache reads "write back" or "write through" (anything else VOIDs); on "write back" the flush
    counter rises by at least 10,000 across the timed fsync run (a lower bound: another process's flushes can pad
    it, never shrink it). Every LOOP layer whose queue reads "write back" is gated the same way on its own counter
    (gate-6 review M3), so a write-through layer above the drive shows as a drive counter that did not rise;
  - the timed fsync run carries fio's own sync count (N-1 or N; fio 3.36 does not count the end_fsync), a p50 and
    the histogram bins (review M1: no measurement, no normaliser, no VALID).
Published, not gates (lines 180 and 553): the fsync p50, the fsync/control write p50 ratio, the before-to-after
drift. On a write-through drive floor_kind says "no volatile cache: no drive flush": the block layer sends such a
drive no flush, so the fsync latency is not a drive flush (GitHub-hosted runners' disks are such drives).
The pooled p50 merges the two timed fsync runs' latency histograms (fio json+ bins, fio's own ~1.5% buckets).

Blind spots, stated: the labelling and timed runs are separate executions of one command; the flush counter is the
whole drive's, so concurrent work on that drive pads it; a filesystem behind device-mapper or md is refused, not
resolved, and so is a native-multipath NVMe head (its counter may live on the path devices); a loop is followed
through its backing file to the drive under it. Every fio argv is recorded in v3l.json (review M7).
"""
import json
import os
import re
import subprocess
import sys

N = 10000
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
    """DIR's mount followed down to a whole disk: (chain of device names, disk). Loops are followed through their
    backing file; device-mapper and md are refused."""
    chain, path = [], d
    for _ in range(6):
        name = os.path.basename(os.path.realpath(mount_source(path)))
        chain.append(name)
        if name.startswith("loop"):
            try:
                path = open(f"/sys/block/{name}/loop/backing_file").read().strip()
            except OSError as e:
                raise RuntimeError(f"loop {name} has no readable backing file: {e}")
            continue
        if name.startswith("dm-") or name.startswith("md"):
            raise RuntimeError(f"{name}: device-mapper and md are not resolved to a drive (refused)")
        sysp = os.path.realpath(f"/sys/class/block/{name}")
        disk = os.path.basename(os.path.dirname(sysp)) if os.path.exists(f"{sysp}/partition") else name
        mp = f"/sys/block/{disk}/multipath"
        if os.path.isdir(mp) and os.listdir(mp):
            raise RuntimeError(f"{disk} is a native-multipath NVMe head ({sorted(os.listdir(mp))}): its flush counter "
                               "may live on the path devices (refused, not resolved)")
        return chain, disk
    raise RuntimeError(f"more than 5 layers under {d}")


def disk_attr(disk, rel):
    try:
        return open(f"/sys/block/{disk}/{rel}").read().strip()
    except OSError:
        return None


def flush_ios(disk):
    """Field 16 of /sys/block/<disk>/stat: flush requests completed (Documentation/block/stat.rst)."""
    f = open(f"/sys/block/{disk}/stat").read().split()
    if len(f) < 17:
        raise RuntimeError(f"/sys/block/{disk}/stat has {len(f)} fields: this kernel keeps no flush counter")
    return int(f[15])


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


def gates(rec):
    """The registered gates over a record; the list of failures (empty = VALID)."""
    bad = []
    for arm, want in (("fsync", N), ("control", 0)):
        a = rec["arms"][arm]
        v = a["v1l"]
        if v["data_writes"] != N:
            bad.append(f"V1L {arm}: {v['data_writes']} writes to the data file, registered exactly {N}")
        if v["data_fsyncs"] != want:
            bad.append(f"V1L {arm}: {v['data_fsyncs']} fsync of the data file, registered {want}")
        if v["other_fsyncs"]:
            bad.append(f"V1L {arm}: {v['other_fsyncs']} fsync of other files")
        for k in OTHER_SYNCS:
            if v.get(k):
                bad.append(f"V1L {arm}: {v[k]} {k}, registered 0")
        if v["failed"]:
            bad.append(f"V1L {arm}: {v['failed']} failed write/sync calls")
        if v["io_uring_setup"]:
            bad.append(f"V1L {arm}: io_uring_setup in a labelling run (refused, line 173)")
        for run in ("labelling_fio", "timed"):
            f = a[run]
            if f.get("fio_error"):
                bad.append(f"fio {arm}/{run}: error {f['fio_error']}")
            if f.get("writes") != N:
                bad.append(f"fio {arm}/{run}: {f.get('writes')} writes, registered exactly {N}")
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
    for lay in rec["leaf"].get("layers") or []:
        bad += counter_gate(f"loop layer {lay.get('name')}", lay.get("write_cache"), lay.get("flush_ios_delta"),
                            lay.get("lab_flush_ios_delta"))
    # A16 (1): the kernel's state and the drive's own report (the V3 batch's leaf record) must AGREE; an absent or
    # unknown report is not agreement (third lane review LOW 7). A ram disk (brd, dry runs only) has no report.
    dr = rec["leaf"].get("drive_reports")
    want_dr = "none (RAM)" if str(rec["leaf"].get("disk", "")).startswith("ram") else rec["leaf"]["write_cache"]
    if dr != want_dr:
        bad.append(f"drive {rec['leaf']['disk']}: the kernel's write_cache ({rec['leaf']['write_cache']}) disagrees with "
                   f"the drive's own report ({dr!r})")
    wc = rec["leaf"]["write_cache"]
    if wc in ("write back", "write through"):
        bad += counter_gate(f"drive {rec['leaf']['disk']}", wc, rec["arms"]["fsync"]["timed"].get("flush_ios_delta"),
                            rec["arms"]["fsync"]["timed"].get("lab_flush_ios_delta"))
    else:
        bad.append(f"drive {rec['leaf']['disk']}: queue/write_cache unreadable or unknown ({wc!r})")
    return bad


def counter_gate(what, wc, timed, lab):
    """One device's flush counter over the timed fsync run (A16):
    write back: at least one flush per fsync, AND at least the whole flushes per fsync the strace-checked labelling run
      showed this stack making (floor(lab / N), e.g. 2 through a loop), so a timed run that lost part of its fsyncs
      cannot pass on a stack that makes 2 per fsync (gate-6 review 7, lane review MED 3). Absent a labelling count
      (records before it was taken) only the first rule applies;
    write through: the counter must read 0 in both runs (the block layer sends no flush; a count contradicts the
      recorded state and VOIDs)."""
    bad = []
    if wc == "write back":
        if timed is None or timed < N:
            bad.append(f"write-back {what}: its flush counter rose {timed} across {N} fsyncs (fewer than one per fsync)")
        elif isinstance(lab, int) and lab >= N and timed < (lab // N) * N:
            bad.append(f"write-back {what}: its flush counter rose {timed} in the timed run, fewer than the "
                       f"{lab // N} per fsync the labelling run showed ({lab} across {N})")
    elif wc == "write through":
        if timed != 0 or (lab is not None and lab != 0):
            bad.append(f"write-through {what}: its flush counter rose {timed} (labelling {lab}); a write-through queue "
                       "gets no flush request, so a count contradicts the recorded state (A16)")
    return bad


def measure(d, out, leafrec=None):
    if os.path.exists(out):
        raise RuntimeError(f"{out} exists")
    os.makedirs(d, exist_ok=True)
    chain, disk = resolve_leaf(d)
    if os.environ.get("V3L_REAL") == "1" and (any(c.startswith("loop") for c in chain) or disk.startswith("ram")):
        raise RuntimeError(f"a real run on {chain} -> {disk}: loop and ram devices are dry-run only (A16)")
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
                    "fua": disk_attr(disk, "queue/fua"), "rotational": disk_attr(disk, "queue/rotational"),
                    "model": disk_attr(disk, "device/model"), "drive_reports": drive_reports,
                    "drive_reports_from": leafrec,
                    "layers": [{"name": c, "write_cache": disk_attr(c, "queue/write_cache")}
                               for c in chain if c.startswith("loop")]},
           "virtualization": virt, "arms": {}, "plant": os.environ.get("V3L_PLANT") or None, "argv": {}}
    if not rec["fio_version"] or not rec["strace_version"]:
        raise RuntimeError("fio or strace is not installed")
    os.makedirs(out)
    for arm, fs in (("fsync", True), ("control", False)):
        a = {}
        f = os.path.join(os.path.realpath(d), f"v3l-{arm}.dat")
        if os.path.exists(f):
            os.unlink(f)
        tr, lj = os.path.join(out, f"{arm}.v1l"), os.path.join(out, f"{arm}-labelling.json")
        cmd = V1L + ["-o", tr] + fio_cmd(f"v3l-{arm}", f, fs, lj)
        rec["argv"][f"{arm}/labelling"] = cmd
        k0 = {lay["name"]: flush_ios(lay["name"]) for lay in rec["leaf"]["layers"]}
        g0 = flush_ios(disk)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        g1 = flush_ios(disk)
        k1 = {lay["name"]: flush_ios(lay["name"]) for lay in rec["leaf"]["layers"]}
        if r.returncode != 0:
            raise RuntimeError(f"labelling {arm} run rc {r.returncode}: {r.stderr[-400:]}")
        a["v1l"] = parse_v1l(open(tr).read(), f)
        a["labelling_fio"] = fio_numbers(lj)
        a["labelling_fio"].pop("fsync_bins_ns", None)
        os.unlink(f)
        tj = os.path.join(out, f"{arm}-timed.json")
        cmd = fio_cmd(f"v3l-{arm}", f, fs, tj)
        rec["argv"][f"{arm}/timed"] = cmd
        l0 = {lay["name"]: flush_ios(lay["name"]) for lay in rec["leaf"]["layers"]}
        f0 = flush_ios(disk)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        f1 = flush_ios(disk)
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
        os.unlink(f)
        rec["arms"][arm] = a
    ft, ct = rec["arms"]["fsync"]["timed"], rec["arms"]["control"]["timed"]
    rec["published"] = {"fsync_p50_us": ft.get("fsync_p50_us"), "fsync_p99_us": ft.get("fsync_p99_us"),
                        "control_write_p50_us": ct.get("write_p50_us"),
                        "fsync_over_control_write_p50": ratio(ft.get("fsync_p50_us"), ct.get("write_p50_us")),
                        "flush_ios_per_fsync": round(ft["flush_ios_delta"] / N, 3)}
    rec["floor_kind"] = ("brd: no drive (dry runs only, never credited)" if disk.startswith("ram")
                         else "write-back drive: flush requests completed by the driver (issued and acknowledged, not "
                              "persistence)" if rec["leaf"]["write_cache"] == "write back"
                         else "no volatile cache: no flush request sent (counter 0 checked)")
    if virt and virt.get("virtualized") is not False:
        rec["floor_kind"] += f"; virtualized {virt.get('virtualized')!r}: reach to media unknown"
    bad = gates(rec)
    rec["verdict"] = "VOID" if bad else "VALID"
    rec["void_reasons"] = bad
    json.dump(rec, open(os.path.join(out, "v3l.json"), "w"), indent=1)
    print(json.dumps({"verdict": rec["verdict"], "leaf": rec["leaf"], "published": rec["published"], "void": bad}))
    return 3 if bad else 0


def ratio(a, b):
    return round(a / b, 2) if a and b else None


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


def plants(rec):
    """The V3L counter and agreement gates forced to fire on copies of a real record (gate-6 review 7, lane review
    MED 3/4): each plant is one field changed on a record whose control passes gates(). The branch the real drive is
    not on is reached by setting the leaf (and its loop layers) to that state with consistent counts (write back: the
    real per-fsync counts if any, else 2 per fsync; write through: 0), then changing one field.
    Returns (results, ok); a control that does not pass makes every plant NOT-RUN (ok False)."""
    import copy as _c
    out = []
    again = gates(rec)
    out.append({"plant": "control", "want": "VALID", "got": again or "VALID", "fired": not again})
    if again:
        out.append({"plant": "all", "fired": False, "why": "NOT-RUN: the real record is not VALID"})
        return out, False

    def as_state(r, wc):
        r = _c.deepcopy(r)
        r["leaf"]["write_cache"] = wc
        r["leaf"]["drive_reports"] = wc
        t = r["arms"]["fsync"]["timed"]
        if wc == "write through":
            t["flush_ios_delta"] = t["lab_flush_ios_delta"] = 0
        else:
            if not t.get("flush_ios_delta"):
                t["flush_ios_delta"] = 2 * N
            # a write-through record's labelling count is 0, and older records have none: the base takes the timed
            # count, so the labelling run shows the same flushes per fsync (dry run 37812992594: a 0 kept here left
            # the floor rule unarmed and the half-flushes plant could not fire)
            if not t.get("lab_flush_ios_delta") or t["lab_flush_ios_delta"] < N:
                t["lab_flush_ios_delta"] = t["flush_ios_delta"]
        for lay in r["leaf"].get("layers") or []:
            lay["write_cache"] = wc
            if wc == "write through":
                lay["flush_ios_delta"] = lay["lab_flush_ios_delta"] = 0
            else:
                if not lay.get("flush_ios_delta"):
                    lay["flush_ios_delta"] = 2 * N
                if not lay.get("lab_flush_ios_delta") or lay["lab_flush_ios_delta"] < N:
                    lay["lab_flush_ios_delta"] = lay["flush_ios_delta"]
        return r

    def arm(name, base_wc, mutate, want):
        r = as_state(rec, base_wc)
        if gates(r):
            out.append({"plant": name, "fired": False, "why": f"NOT-RUN: the {base_wc} base does not pass: {gates(r)}"})
            return
        mutate(r)
        got = gates(r)
        out.append({"plant": name, "want": want, "got": got, "fired": any(want in x for x in got)})

    def half_timed(r):
        t = r["arms"]["fsync"]["timed"]
        t["flush_ios_delta"] = t["flush_ios_delta"] // 2

    arm("wb-half-flushes", "write back", half_timed, "write-back drive")
    arm("wt-one-flush", "write through", lambda r: r["arms"]["fsync"]["timed"].__setitem__("flush_ios_delta", 1),
        "write-through drive")
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
            timed_syncs=N - 1, ctl_fio_syncs=0, layers=None, drive="same", lab_syncs=N - 1, disk="nvme1n1"):
        def arm(fc, syncs, tsyncs):
            v = {"data_writes": writes, "data_fsyncs": fc, "other_fsyncs": other, "failed": failed,
                 "io_uring_setup": uring, "fdatasync": 0, "sync_file_range": 0, "syncfs": 0, "msync": 0}
            return {"v1l": v, "labelling_fio": {"writes": fio_w, "syncs": syncs},
                    "timed": {"writes": fio_w, "syncs": tsyncs, "fsync_p50_us": 300.0, "fsync_bins_ns": {"300000": tsyncs}}}
        # fio's own sync count is whatever it reports (9,999 or 10,000 with end_fsync); only agreement is gated
        r = {"leaf": {"disk": disk, "write_cache": wc, "layers": layers or [],
                      "drive_reports": wc if drive == "same" else drive},
             "arms": {"fsync": arm(fsyncs, lab_syncs, timed_syncs),
                      "control": arm(ctl_fsyncs, ctl_fio_syncs, ctl_fio_syncs)}}
        r["arms"]["fsync"]["timed"]["flush_ios_delta"] = delta
        return r

    cases += [
        ("a clean write-back record is VALID", gates(rec()) == []),
        ("a clean write-through record is VALID (no counter gate)", gates(rec(wc="write through", delta=0)) == []),
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
        ("M3: a write-through loop layer is not gated on its own counter",
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
        ("plants on a VALID write-back record: control passes and all four fire", plants(rec(delta=20003))[1]),
        ("plants on a VALID write-through record: all four fire", plants(rec(wc="write through", delta=0))[1]),
        ("plants on a write-through record carrying labelling count 0 (as measured now): all four fire",
         plants(_lab(rec(wc="write through", delta=0), 0))[1]),
        ("plants on a write-back record carrying its labelling count: all four fire",
         plants(_lab(rec(delta=20003), 20004))[1]),
        ("plants on a VOID record: NOT-RUN", not plants(rec(fsyncs=N - 1))[1]),
        ("pooled p50 of {100:3} and {200:3, 300:1}: 200", pooled_p50_ns([{"100": 3}, {"200": 3, "300": 1}]) == 200),
        ("pooled p50 with a missing histogram: None", pooled_p50_ns([{"100": 3}, None]) is None),
        ("block with a missing after file: MISSING", block("/nonexistent/b.json", "/nonexistent/a.json")["verdict"] == "MISSING"),
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
