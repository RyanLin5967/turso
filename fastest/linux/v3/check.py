#!/usr/bin/env python3
"""check.py OUT CELL -- the verdicts of a V3 fire-check, read ONLY from the raw files firecheck.sh wrote under OUT.
CELL is explicit (v3cell.py: ext4|xfs|btrfs on a block device, ext4loop|xfsloop|btrfsloop on a loop), never inferred.

  check.py OUT CELL            write OUT/verdict.json and OUT/verdict.bind.pending (and OUT/red.json, the base
                               column; OUT/prev.json, the previous tip's column) and print PASS/FAIL lines
  check.py --self-test         the checker's own leaf, virtualization, linkage, qualifier, harness and MODE SENSE
                               byte rules on planted records; exit 0 iff all fire as written
  check.py --bind OUT CELL     after run.sh was bound to OUT/verdict.json (firecheck.sh's last step): write
                               OUT/verdict.bind.json (the binding record run.sh requires) and remove the pending one
  check.py --plan CELL ARCH LEAF VIRT FLIP PLP   print the check ids a verdict for that cell, arch, leaf class
                               (wb|wt|brd) and box (VIRT vm|bare from systemd-detect-virt; FLIP yes|no: can the leaf
                               disk's write cache be made to disagree with the drive; PLP yes|no, V3_PLP) must hold
  check.py --box OUT           print the box firecheck.sh recorded in OUT/info.txt, as check.py reads it

Every expectation below comes from the arm definitions (v3floor.c's header, PREREG section 11 M0 exit 1), written
here by hand. None is read from the probe's summary: the summary is a subject, checked against numbers recomputed
from raw.tsv. The sequence checker is itself fire-checked first: planted breaches in a copy of a real trace must each
be rejected. The checks a verdict holds are fixed in advance by plan(cell, arch, leaf class): a verdict whose ids
differ from its plan fails, and run.sh refuses to bind one. Exit 0 all pass, 1 any fail, 2 usage.
"""
import gzip, hashlib, json, os, re, sys, zlib
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import v3cell  # noqa: E402

# ---- the spec -----------------------------------------------------------------------------------------------
# Syscalls each arm issues per op (per round, one op per arm), by definition. Copy arms: openat (create the clone in
# the clones' directory), the copy (ioctl FICLONE or copy_file_range), close, fsync of the directory; clone2b and
# cfr2b also fsync the clone; unlinkat is the teardown's, one per clone. Every syscall not listed for an arm is DEFINED
# to be 0 per op (an allowlist: an extra syscall in the loop fails, whatever it is).
OP = {
    "append25": {"pwrite64": 1, "fsync": 1},
    "append64": {"pwrite64": 1, "fsync": 1},
    "ow4k": {"pwrite64": 1, "fsync": 1},
    "ow64k": {"pwrite64": 1, "fsync": 1},
    "ow1m": {"pwrite64": 1, "fsync": 1},
    "fdatasync4k": {"pwrite64": 1, "fdatasync": 1},
    "clone1b": {"openat": 1, "ioctl": 1, "close": 1, "fsync": 1, "unlinkat": 1},
    "clone2b": {"openat": 1, "ioctl": 1, "close": 1, "fsync": 2, "unlinkat": 1},
    "cfr2b": {"openat": 1, "copy_file_range": 1, "close": 1, "fsync": 2, "unlinkat": 1},
    "clean": {"fsync": 1},
    "nosync25": {"pwrite64": 1},
}
# Flushes in each arm's setup (before the loop), by definition: append/nosync init write + fsync; ow*/fdatasync4k/
# clean preallocate + fsync; copy arms preallocate the source + fsync and fsync the new clones' directory.
SETUP_FLUSH = {"append25": 1, "append64": 1, "nosync25": 1, "ow4k": 1, "ow64k": 1, "ow1m": 1, "fdatasync4k": 1,
               "clean": 1, "clone1b": 2, "clone2b": 2, "cfr2b": 2}  # all fsync
TEARDOWN_FLUSH = 1  # one fsync of D after the unlinks
APPEND = {"append25": 25, "append64": 64, "nosync25": 25}
APPEND_BASE = 4096  # the append arms' files start one 4 KiB block long (setup), so op i writes at 4096 + rec * i
REC = {"ow4k": 4096, "ow64k": 65536, "ow1m": 1 << 20, "fdatasync4k": 4096}
CAP = {"ow4k": 16 << 20, "ow64k": 16 << 20, "ow1m": 128 << 20, "fdatasync4k": 16 << 20}
FLUSHED = ["append25", "append64", "ow4k", "ow64k", "ow1m", "fdatasync4k", "clone1b", "clone2b", "cfr2b"]
# flush-gated: every op must issue a device flush and its own sync (post's gates); fdatasync4k since it is an A18
# floor candidate (eighth review M1; measured: it issues a flush-carrying request in 200/200 windows on the three
# write-back cells of run 37812355435). The TIMING control gates append25 only (A17).
GATED = ["append25", "append64", "ow4k", "ow64k", "ow1m", "clone2b", "cfr2b", "fdatasync4k"]
CLONES = ["clone1b", "clone2b"]  # FICLONE: refused on ext4
COPIES = ["clone1b", "clone2b", "cfr2b"]
FLUSH_FAMILY = ["fsync", "fdatasync", "sync", "syncfs", "sync_file_range", "msync"]
INSTRUMENT = "clock_gettime"  # 2 per op when the vDSO does not serve CLOCK_MONOTONIC_RAW; allowed, recorded
ALL = "append25,append64,ow4k,ow64k,ow1m,clone1b,clone2b,cfr2b,clean,fdatasync4k,nosync25"
F1_SETS = ["nosync25", "clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
F2_SETS = ["clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
NS = [1, 2, 3, 40]
SEQ = {"real-all": (ALL, 300, False), "real-4k": ("ow4k,fdatasync4k,nosync25", 4100, False),
       "mutant-all": (ALL, 40, True)}
# The frame arm (review 2 item 11; PREREG section 4: the smallest bytes per flush >= the M1 build's median create
# frame among the M0 append and overwrite arms). A named create's flight is about 56-60 B (review 2 item 11; not
# measured here), so the frame arm must be an append of at least 60 B and less than a 4 KiB page.
FRAME_ARM = "append64"
M1_FLIGHT_MAX_B = 60
CLOCKSOURCES = ["tsc", "arch_sys_counter"]
# A flush an op issues itself shows in EVERY window of its arm (append25 reads 1.00 per op on write-back NVMe, run
# 37475543956). The leaf drive of a hosted runner is shared with the whole system (its root filesystem's jbd2
# commits, other processes' fsyncs), so a foreign flush request can land in an occasional window: run 37475543956's
# x86 ext4 cell saw one jbd2 commit (1 flush + 1 FUA) inside 1 of nosync25's 200 windows. On that shared drive a
# no-flush arm may therefore show a request in at most this share of its windows (self-issued would be 100%);
# devices private to the cell (its loop devices, brd) and write-through devices are held to exactly 0.
SHARED_MAX_FRAC = 0.05
LEAF_DRIVERS = ["nvme", "sd", "virtio_blk"]
# crash.sh cases per filesystem kind: (case, arm, flags, rule). "survive" and "lost" are gated (the controls and the
# rig's own fire-check); "record" cases are recorded with their prediction (review 2 item 3), never gated.
CRASH = {
    "xfs": [("clone2b", "survive"), ("cfr2b", "survive"), ("clone1b", "record"), ("clone1b-aim", "record"),
            ("clone2b-mutant", "lost")],
    "btrfs": [("clone2b", "survive"), ("cfr2b", "survive"), ("clone1b", "record"), ("clone2b-mutant", "lost")],
    "ext4": [("cfr2b", "survive"), ("cfr2b-mutant", "lost")],
}
CRASH_PREDICT = {"xfs/clone1b": "unknown", "xfs/clone1b-aim": "lost (review 2: the aimed fsync forces the create "
                 "out, the FICLONE lands in a later checkpoint the directory fsync does not force)",
                 "btrfs/clone1b": "lost (review 2: size 0)"}
# Probe refusals: tag -> substring the message must contain (rc 2, and no out dir made). Cell-independent ones.
REFUSALS = {
    "R_tmpfs": "not ext4, xfs or btrfs", "R_nobarrier": "nobarrier: layer 0", "R_nobarrier_below": "nobarrier: layer 1",
    "R_outexists": "must not exist", "R_n0": "usage:", "R_nalpha": "not a whole number", "R_ntrail": "not a whole number",
    "R_unknown": "unknown arm", "R_twice": "twice", "R_nod0": "without nosync25", "R_mutnod0": "without nosync25",
    "R_nice": "nice is 5", "R_ionice": "I/O priority", "R_schedidle": "scheduling policy",
    "R_schedbatch": "scheduling policy", "R_badarg": "bad argument", "R_noout": "usage:", "R_pathlong": "too long",
    "R_mutant_noenv": "for the fire-check only", "R_traceclock_noenv": "for the fire-check only",
    "R_crashop_noenv": "for the fire-check only",
    "R_symlink": "left over", "R_leftover_file": "left over",
    "R_hidden_tmpfs": "not ext4, xfs or btrfs", "R_hidden_nobarrier": "nobarrier: layer 0",
    "R_lazy": "not the loop's backing inode", "R_deleted": "cannot read a live backing file",
    "R_nest4": "more than 3 nested loop devices (4 layers)", "R_statfs_shim": "but statfs magic",
    "R_dirsync": "mount option 'dirsync' is not in the known-safe list", "R_logdev": "external log", "R_extjournal": "no internal journal",
    "R_multidev": "multi-device btrfs", "R_loop_wt": "above the leaf", "R_brd": "brd is fire-check only",
    "R_driver": "not in the leaf allowlist",
    "R_chattr": "outside the allowlist (extents, directory index)", "R_ldpreload": "LD_PRELOAD is set",
    # fourth review L6: an ext4 whose superblock default is data=journal (tune2fs -o journal_data): mountinfo omits it
    "R_datajournal": "effective option 'data=journal'",
    # fourth review M2: a scsi_debug leaf outside the fire-check
    "R_sdbg_noenv": "is a scsi_debug disk",
    # ninth review L13, M6, M7, L9: the probe's own registration and rental refusals, each on a planted --registered
    "R_rental_noreg": "--require-registered without --registered",
    "R_rental_noframe": "rental mode: no registered frame arm",
    "R_rental_novariant": "has no fdatasync variant arm",
    "R_reg_frame25": "other than append25",
    "R_reg_nonascii": "breaks the one strict rule",
    "R_reg_longline": "breaks the one strict rule",
}
# run.sh refusals: tag -> the refusing rule's own reason PREFIX (rc 2, no out dir, no probe run). Every planted
# verdict also carries "planted", so a bare word ("plan", "cell", "arch") would match whatever rule fired (fresh
# review H2): each want is the start of its own rule's reason in batchgate.verdict_problems.
RUNSH = {
    "R_runsh_none": "set V3_FIRECHECK_VERDICT=", "R_runsh_both": "not both", "R_runsh_sha": "v3floor_sha256: the verdict is for",
    "R_runsh_fail": "all_pass is not true", "R_runsh_fs": "fstype: the verdict is for", "R_runsh_arch": "arch: the verdict is for",
    "R_runsh_mutant": "not allowed through run.sh", "R_runsh_traceclock": "not allowed through run.sh",
    "R_runsh_crashop": "not allowed through run.sh", "R_runsh_crashaim": "not allowed through run.sh",
    "R_runsh_dir": "not allowed through run.sh", "R_runsh_out": "not allowed through run.sh",
    "R_runsh_n": "not allowed through run.sh", "R_runsh_fcenv": "V3FLOOR_FIRECHECK is set", "R_runsh_brdenv": "V3FLOOR_BRD is set",
    "R_runsh_nocell": "V3_CELL=''", "R_runsh_badcell": "V3_CELL='bogus'", "R_runsh_planted": "planted: a fixture verdict",
    "R_runsh_shape": "pass/total:", "R_runsh_count": "plan: the check ids differ", "R_runsh_cellv": "cell: the verdict is for",
    "R_runsh_harness": "harness: the verdict was made by", "R_runsh_ldpreload": "LD_PRELOAD is set",
    "R_runsh_t3": "T3 preconditions do not hold",
    # fourth review L3: the fire-check's own binding record
    "R_runsh_bindfail": "bind: the fire-check's own binding check failed", "R_runsh_nobind": "bind: no binding record",
    # fifth review L1: a pending record binds only for firecheck.sh's own bind step
    "R_runsh_pending": "bind: pending",
    # annex A14: the operator's PLP declaration is required
    "R_runsh_noplp": "V3_PLP='' is not yes or no",
    # ninth review L13: a cell name of the other layout (block vs loop) refuses before the probe runs
    "R_runsh_celllayout": "cell layout:",
}
# the probe ran; run.sh refused after it (rc 2): tag -> reasons that must all appear
RUNSH_POST = {"R_runsh_post": ["exe_sha256: the probe that ran", "mutant_nosync=1"],
              "R_runsh_nogated": ["no gated arm ran"], "R_runsh_gatecrash": ["the batch gate failed"]}
# batchgate.py post on a copy of the F3 batch with one field planted (bound mode): tag -> its reason prefix
# (fourth review M3, M5, L9: every post-run rule planted, and a control with nothing planted that must refuse nothing)
POST_PLANTS = {"R_post_traceclock": "trace_clock=1 in the summary", "R_post_cell": "layout:",
               "R_post_leaf": "leaf class: the batch's leaf is", "R_post_brd": "leaf brd:", "R_post_stack": "stack:",
               "R_post_driver": "leaf drive:", "R_post_virt": "virtualization:",
               "R_post_verdictswap": "verdict: changed during the run",
               "R_post_verdictbad": "verdict: unreadable after the run", "R_post_nostamp": "no stamp_end.json",
               "R_post_noblk": "no blkflush report", "R_post_blkrefused": "blkflush refused:",
               # fifth review M3: the leaf-kind rule, the model half of the drive rule
               "R_post_leafkind": "leaf: the summary's leaf record", "R_post_model": "leaf drive:",
               # gate-6 review v3 #1 (A16), MED 3, A14 binding, A17 rental mode: VOID <prefix> is rc 3 by that void
               "R_post_nofsync": "VOID fsync:", "R_post_wtflush": "write-through leaf:", "R_post_wtmismatch": "drive report:",
               "R_post_layerflush": "VOID flush-carrying:", "R_post_plp": "plp:", "R_post_unregistered": "registration:",
               "P_post_none": None}
# post plants planned only where their premise exists: a write-back layer on the flush path
POST_COND = {"R_post_layerflush": lambda cell, leaf: v3cell.is_loop(cell) or leaf == "wb"}
# the fire-check harness a verdict vouches for (fourth review L9: the fixtures' and plants' sources, the banked test
# data, the loop maker and the workflow too); a missing file is a mismatch
HARNESS = ["run.sh", "batchgate.py", "check.py", "blkflush.py", "stamp.py", "v3cell.py", "firecheck.sh", "crash.sh",
           "mkfixtures.sh", "mkbrd.sh", "red.py", "postplant.py", "nsfake.sh", "build.sh", "REGISTERED.tsv",
           "statfs_shim.c", "noop_shim.c",
           "../fs/mkloop.sh",
           "../../../.github/workflows/fastest-v3.yml"]


REGISTRY_FILE = "REGISTERED.tsv"  # the self-test points this at a fixed snapshot (eighth review M7)


REG_LINE_MAX, REG_KEY_MAX, REG_VAL_MAX, REG_REF_MAX = 512, 120, 60, 200  # v3floor.c's caps (ninth review M7)
FRAME_ARMS_ALLOWED = ["append64", "ow4k", "ow64k", "ow1m"]  # M0 append/overwrite arms; not append25 (ninth review L9)


def registered(here=HERE, path="REGISTERED.tsv"):
    """REGISTERED.tsv: key -> (value, registration ref) (annex A17; gate-6 review MED 5). The probe's one strict rule
    (reg_lookup; eighth review L5, ninth review M7): printable ASCII and TAB only (no CR, NUL or byte >= 0x7f, which the
    probe would write as one \\u00XX per byte), every line at most 512 bytes, comments included; data lines exactly
    three non-empty fields, key <= 120, value <= 60, ref <= 200 bytes; a d0 threshold a plain decimal above 1; a frame
    arm one of append64, ow4k, ow64k, ow1m."""
    out = {}
    # bytes, so that every byte is seen (text mode would translate a CR away); an unreadable registry raises (OSError),
    # never reads as an empty one (tenth review LOW 4)
    raw = open(os.path.join(here, path), "rb").read()
    lines = raw.split(b"\n")
    if lines and lines[-1] == b"":
        lines = lines[:-1]
    for b in lines:
        if len(b) > REG_LINE_MAX or any(not (c == 9 or 0x20 <= c <= 0x7e) for c in b):
            raise ValueError("REGISTERED.tsv: a line over %d bytes or with a byte outside printable ASCII and TAB: %r"
                             % (REG_LINE_MAX, b[:80]))
        line = b.decode("ascii")
        if not line or line.startswith("#"):
            continue
        f = line.split("\t")
        if len(f) != 3 or not all(f) or len(f[0]) > REG_KEY_MAX or len(f[1]) > REG_VAL_MAX or len(f[2]) > REG_REF_MAX:
            raise ValueError("REGISTERED.tsv: a line that is not key<TAB>value<TAB>ref within the caps: %r" % line[:80])
        # tenth review LOW 2: the keys are an allowlist, and a key twice is refused (no last-wins), as in the probe
        if f[0] != "frame_arm" and not re.fullmatch(r"d0_threshold/(ext4|xfs|btrfs)/(wb|wt|brd)/(vm|bare|nr)", f[0]):
            raise ValueError("REGISTERED.tsv: key %r is neither frame_arm nor d0_threshold/<fs>/<wb|wt|brd>/<vm|bare|nr>" % f[0])
        if f[0] in out:
            raise ValueError("REGISTERED.tsv: key %s twice" % f[0])
        if f[0].startswith("d0_threshold/") and not (re.fullmatch(r"[0-9]+(\.[0-9]*)?", f[1]) and float(f[1]) > 1.0):
            raise ValueError("REGISTERED.tsv: %s = %r is not a plain decimal above 1" % (f[0], f[1]))
        if f[0] == "frame_arm" and f[1] not in FRAME_ARMS_ALLOWED:
            raise ValueError("REGISTERED.tsv: frame_arm %r is not one of %s" % (f[1], FRAME_ARMS_ALLOWED))
        out[f[0]] = (f[1], f[2])
    return out


def timing_expect(sj, p50):
    """The timing control and the D0 control the probe must record, recomputed from raw p50s and the leaf, written
    by hand from the rulings: A14 (not applicable on no volatile cache, declared PLP, brd), A17 (append25 only, the
    registered threshold or a provisional 10), PREREG section 4 (nosync25 p50 >= 50 us voids). -> (expect, voids)."""
    reg = registered(path=REGISTRY_FILE)
    lf = sj.get("leaf") or {}
    lc = "brd" if lf.get("kind") == "brd" else "wb" if sj.get("leaf_write_cache") == "write back" else "wt"
    vz = (sj.get("virtualization") or {}).get("virtualized")
    key = "d0_threshold/%s/%s/%s" % (sj.get("fstype"), lc, "vm" if vz is True else "bare" if vz is False else "nr")
    t = float(reg[key][0]) if key in reg else 10.0
    d0 = p50.get("nosync25")
    voids = []
    if lc == "brd":
        tc = "not applicable: brd (no drive)"
    elif lc == "wt":
        tc = "not applicable: no volatile cache"
    elif sj.get("plp") == "yes":
        tc = "not applicable: PLP"
    elif "append25" not in p50:
        tc = "not run: append25 not selected"
    else:
        r = p50["append25"] / d0 if d0 else 1e18
        tc = "pass" if r > t else "FAIL: run void (append25 ratio"
        if r <= t:
            voids.append("timing")
    dc = None
    if d0 is not None:
        if sj.get("traced") is True:  # a tracer's stops, not a foreign writer (eighth review H2)
            dc = "not applicable: traced"
        else:
            dc = "FAIL: run void (nosync25 p50" if d0 / 1e3 >= 50 else "pass (nosync25 p50"
            if d0 / 1e3 >= 50:
                voids.append("d0")
    return {"key": key, "threshold": t, "ref": reg[key][1] if key in reg else None, "timing": tc, "d0": dc}, voids


def harness_files(here=HERE):
    fs = list(HARNESS)
    td = os.path.join(here, "testdata")
    for dp, dn, fn in os.walk(td):
        dn.sort()
        for f in sorted(fn):
            fs.append(os.path.relpath(os.path.join(dp, f), here))
    return fs


def harness_sha256(here=HERE):
    out = {}
    for f in harness_files(here):
        try:
            out[f] = hashlib.sha256(open(os.path.join(here, f), "rb").read()).hexdigest()
        except OSError:
            out[f] = None
    return out


def kind_of(cell):
    return v3cell.kind(cell)


def ran(arms, kind):
    return [a for a in arms if not (kind == "ext4" and a in CLONES)]


def all_flushed_refused(arms, kind):
    return any(a in FLUSHED for a in arms) and not any(a in FLUSHED for a in ran(arms, kind))


def tagof(s):
    return "all" if s == ALL else s.replace(",", "+")


def plant_names(kind):
    p = ["append25's fsync moved out of its timed window", "ow1m fsyncs the clean arm's file", "ow4k writes 25 B",
         "ow1m does not wrap at 128 MiB", "fdatasync4k issues fsync instead", "nosync25 gains a flush",
         "a syscall between two ops", "one timed window dropped", "append64 writes at append25's offset",
         "cfr2b copies 4096 B", "cfr2b skips the clone's own fsync",
         "nosync25 gains a split fsync (<unfinished ...> / <... resumed>)"]
    if kind != "ext4":
        p += ["clone1b fsyncs its source, not the directory", "FICLONERANGE in place of FICLONE",
              "clone2b skips the clone's own fsync"]
    return p


def plan(cell, arch, leaf, box):
    """The check ids a verdict for (cell, arch, leaf class wb|wt|brd, box {virt, flip}) holds, in order. Pure: the
    spec only."""
    k = kind_of(cell)
    ids = ["cell:fstype", "cell:source", "cell:layers", "cell:aslr", "cell:leaf", "cell:box"]
    for s in F1_SETS:
        ids.append("F1:%s%s" % (tagof(s), ":refused" if all_flushed_refused(s.split(","), k) else ""))
    ids += ["F1b:real-all", "F1b:real-4k", "F2d:mutant-all", "F1b:selftest:count"]
    ids += ["F1b:plant:" + p for p in plant_names(k)]
    ids.append("F2d:fires")
    for s in F2_SETS:
        ids.append("F2:%s%s" % (tagof(s), ":refused" if all_flushed_refused(s.split(","), k) else ""))
    for a in FLUSHED:
        if not all_flushed_refused([a, "nosync25"], k):
            ids.append("F2c:" + a)
    ids += ["F2b", "T:untraced", "F3:complete", "F3:devflush", "F3:merge", "F3:gate", "F3:record"]
    if leaf == "wb" and box.get("plp") == "no":
        ids.append("F2b:discriminates")
    ids.append("frame:append")
    if v3cell.is_loop(cell):
        ids += ["C:%s" % c for c, _ in CRASH[k]]
    ids += ["B:selftest", "B:fsync", "B:quiet", "B:overflow", "B:misuse", "S:batchgate", "S:check"]
    for t in REFUSALS:
        ids.append("F4:" + t)
    ids.append("F4:P_nest3")
    ids.append("F4:P_nest_modes")
    ids.append("F4:R_ficlone_accept" if k == "ext4" else "F4:X_allclones")
    ids.append("F4:R_leftover")
    if box.get("flip") == "yes":
        ids.append("F4:R_leaf_flip")
    if arch == "x86_64":
        ids.append("F4:R_clocksource")
    for t in RUNSH:
        ids.append("F4:" + t)
    for t in RUNSH_POST:
        ids.append("F4:" + t)
    for t in POST_PLANTS:
        if t in POST_COND and not POST_COND[t](cell, leaf):
            continue
        ids.append("F4:" + t)
    ids += ["F4:P_sdbg_wb", "F4:R_sdbg_flip"]
    if box.get("virt") == "vm":
        ids.append("F4:R_virt_hidden")
    ids += ["F4:R_virt_planted", "F4:P_virt_bare", "F4:P_virt_bare_wt", "F4:R_leaf_remote", "F4:R_tc_wb", "F4:P_tc_plp",
            "F4:P_tc_wt", "F4:R_ldso_preload", "F4:P_ldso_static"]
    ids += ["work:empty", "harness:stable"]
    return ids


# ---- state ----------------------------------------------------------------------------------------------------
OUT = CELL = KIND = W = None
results = []


def check(cid, ok, detail, desc=""):
    results.append({"id": cid, "check": desc or cid, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + cid + (" -- " + desc if desc else "") + ("" if ok else ": " + json.dumps(detail)[:900]),
          flush=True)


def rd(p):
    """The file's text, or None when it is missing or cannot be read whole (eleventh review MED 2: a truncated or
    corrupt gzip raised EOFError or zlib.error past the old OSError catch; a caller treats None as missing)."""
    try:
        if p.endswith(".gz"):
            with gzip.open(p, "rt") as f:
                return f.read()
        with open(p) as f:
            return f.read()
    except (OSError, EOFError, zlib.error, UnicodeDecodeError):
        return None


def rj(p):
    t = rd(p)
    try:
        return json.loads(t) if t is not None else None
    except ValueError:
        return None


def rc_of(p):
    t = rd(p)
    try:
        return int(t) if t is not None else None
    except ValueError:
        return None


# ---- strace -c ------------------------------------------------------------------------------------------------
def strace_counts(p):
    """strace -c table -> {syscall: (calls, errors)}; None if the file is missing or holds no row."""
    t = rd(p)
    if t is None:
        return None
    c = {}
    for line in t.splitlines():
        tok = line.split()
        if not tok or line.startswith("%") or line.lstrip().startswith("-") or tok[-1] == "total":
            continue
        if len(tok) not in (5, 6):
            continue
        try:
            c[tok[-1]] = (int(tok[3]), int(tok[4]) if len(tok) == 6 else 0)
        except ValueError:
            continue
    return c or None


def per_round(arms, mutant):
    c = Counter()
    for a in ran(arms, KIND):
        spec = OP[a]
        if mutant and a in FLUSHED:
            spec = {k: v for k, v in spec.items() if k not in ("fsync", "fdatasync")}
        c.update(spec)
    return c


def count_mismatches(counts, arms, mutant):
    """Every way the strace counts at NS differ from the definition; [] means exact."""
    bad = []
    exp = per_round(arms, mutant)
    fixed = sum(SETUP_FLUSH[a] for a in ran(arms, KIND)) + TEARDOWN_FLUSH
    k = len(ran(arms, KIND))
    for x, y in zip(NS, NS[1:]):
        cx, cy = counts[x], counts[y]
        for name in set(cx) | set(cy) | set(exp):
            d = cy.get(name, (0, 0))[0] - cx.get(name, (0, 0))[0]
            de = cy.get(name, (0, 0))[1] - cx.get(name, (0, 0))[1]
            want = exp.get(name, 0) * (y - x)
            if name == INSTRUMENT and name not in exp:
                if d not in (0, 2 * k * (y - x)):
                    bad.append(("instrument", name, x, y, d))
            elif d != want:
                bad.append(("per-op", name, x, y, d, want))
            if de != 0:
                bad.append(("errors per op", name, x, y, de))
    for n in NS:
        for name in FLUSH_FAMILY:
            got = counts[n].get(name, (0, 0))
            want = (fixed if name == "fsync" else 0) + exp.get(name, 0) * n
            if got[0] != want:
                bad.append(("absolute flush", name, n, got[0], want))
            if got[1]:
                bad.append(("flush errors", name, n, got[1]))
    return bad


def raw_rows(p):
    """raw.tsv -> {arm: [(i, ns, t0_ns)]}; None if missing or not the 4-column format."""
    t = rd(p)
    if t is None:
        return None
    lines = t.splitlines()
    if not lines or lines[0] != "arm\ti\tns\tt0_ns":
        return None
    rows = {}
    for line in lines[1:]:
        a, i, ns, t0 = line.split("\t")
        rows.setdefault(a, []).append((int(i), int(ns), int(t0)))
    return rows


def pct(v, p):  # the probe's definition: index floor(p*n), clamped
    k = int(p * len(v))
    return v[min(k, len(v) - 1)]


def counted_set(stage, s, mutant):
    """F1/F2 for one arm set: (counts by n, problems). On ext4 a set whose flushed arms are all clones must refuse."""
    counts, probs = {}, []
    arms = s.split(",")
    for n in NS:
        base = os.path.join(OUT, stage, "%s.n%d" % (tagof(s), n))
        rc = rc_of(base + ".rc")
        txt = rd(base + ".txt") or ""
        if all_flushed_refused(arms, KIND):
            if rc != 2 or "every flushed arm" not in txt or "FICLONE" not in txt or os.path.exists(base + ".out"):
                probs.append(("expected the all-refused rc 2", n, rc, txt[-200:]))
            continue
        c = strace_counts(base + ".strace")
        sj = rj(os.path.join(base + ".out", "summary.json"))
        rows = raw_rows(os.path.join(base + ".out", "raw.tsv"))
        if rc is None or c is None or sj is None or rows is None:
            probs.append(("missing output", n, rc, c is None, sj is None, rows is None, txt[-200:]))
            continue
        # rc 3 exactly when the summary records a void: the timing control (append25) or the D0 control, which strace's
        # slowdown can trip on nosync25 (run 37808860197); anything else is 0
        void = str(sj.get("timing_control", "")).startswith("FAIL") or str(sj.get("d0_control", "")).startswith("FAIL")
        if rc != (3 if void else 0):
            probs.append(("rc", n, rc, sj.get("timing_control"), sj.get("d0_control")))
        want_ref = sorted(a for a in arms if a not in ran(arms, KIND))
        if sorted(sj.get("refused_arms", {})) != want_ref:
            probs.append(("refused_arms", n, sorted(sj.get("refused_arms", {})), want_ref))
        if sorted(rows) != sorted(ran(arms, KIND)) or any(len(v) != n for v in rows.values()):
            probs.append(("raw rows", n, {a: len(v) for a, v in rows.items()}))
        if int(sj.get("mutant_nosync", -1)) != int(mutant) or int(sj.get("trace_clock", -1)) != 0:
            probs.append(("flags", n, sj.get("mutant_nosync"), sj.get("trace_clock")))
        if sj.get("traced") is not True:  # ninth review M4: under strace -f -c the probe must see its tracer
            probs.append(("traced", n, sj.get("traced")))
        counts[n] = c
    return counts, probs


# ---- strace -f -y sequences -------------------------------------------------------------------------------------
LINE = re.compile(r"^(\d+)\s+([a-z0-9_]+)\((.*)\)\s+=\s+(\S+)(.*)$")
EXITED = re.compile(r"^(\d+)\s+\+\+\+ exited with (\d+) \+\+\+$")
FD = re.compile(r"^-?\d+<(.*)>$")
CLOCK = "clock_gettime CLOCK_MONOTONIC_RAW = 0"


def split_args(s):
    out, cur, depth, q, i = [], [], 0, False, 0
    while i < len(s):
        ch = s[i]
        if q:
            cur.append(ch)
            if ch == "\\" and i + 1 < len(s):
                cur.append(s[i + 1])
                i += 2
                continue
            if ch == '"':
                q = False
        elif ch == '"':
            q = True
            cur.append(ch)
        elif ch in "{[<(":
            depth += 1
            cur.append(ch)
        elif ch in "}]>)":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0 and s[i:i + 2] == ", ":
            out.append("".join(cur))
            cur = []
            i += 2
            continue
        else:
            cur.append(ch)
        i += 1
    if cur or out:
        out.append("".join(cur))
    return out


def canon(a):
    a = a.strip()
    if a.startswith("AT_FDCWD<"):  # strace -y decorates AT_FDCWD with the cwd (run 37245436013)
        return "AT_FDCWD"
    m = FD.match(a)
    if m:
        return "<" + m.group(1) + ">"
    if a.startswith('"'):
        return a[1:a.rfind('"')]
    return a


def parse_trace(text):
    """-> (calls, other, pids): every syscall line as (name, canonical text), the lines that are not syscalls, and the
    pids seen. strace 6.8 prints FICLONE as "BTRFS_IOC_CLONE or FICLONE" (one ioctl number) with its source fd as a
    bare integer (run 37245436013), so the source is resolved to its path from the fd that an earlier call returned."""
    calls, other, fds, pids = [], [], {}, set()
    for line in text.splitlines():
        m = LINE.match(line)
        if not m or "<unfinished ...>" in line or "resumed>" in line:
            other.append(line)
            continue
        pids.add(m.group(1))
        name, args, ret = m.group(2), split_args(m.group(3)), canon(m.group(4))
        r = FD.match(m.group(4))
        if r:
            fds[m.group(4).split("<", 1)[0]] = r.group(1)
        if name == "pwrite64" and len(args) == 4:
            t = "pwrite64 %s %s %s = %s" % (canon(args[0]), args[2].strip(), args[3].strip(), ret)
        elif name == "clock_gettime":
            t = "clock_gettime %s = %s" % (args[0].strip() if args else "", ret)
        elif name == "ioctl" and len(args) == 3:
            cmd = args[1].strip()
            cmd = "FICLONE" if "FICLONE" in cmd.split(" or ") else cmd
            src = args[2].strip()
            src = "<%s>" % fds[src] if re.fullmatch(r"\d+", src) and src in fds else canon(src)
            t = "ioctl %s %s %s = %s" % (canon(args[0]), cmd, src, ret)
        else:
            t = name + " " + " ".join(canon(x) for x in args) + " = " + ret
        calls.append((name, t))
    return calls, other, pids


def other_problems(other, pids, rc):
    """Review 2 item 14: the only non-syscall line a single-threaded probe's trace may hold is one exit line, from the
    one pid every syscall line carries, with the run's rc."""
    bad = []
    ex = [EXITED.match(l) for l in other]
    if len(other) != 1 or not ex[0]:
        bad.append(("lines that are not whole syscalls (a split <unfinished ...> line, a signal, a second pid)",
                    [l[:120] for l in other if not EXITED.match(l)][:4], len(other)))
    elif len(pids) != 1 or ex[0].group(1) not in pids:
        bad.append(("pids", sorted(pids)[:4], ex[0].group(1)))
    elif rc is not None and int(ex[0].group(2)) != rc:
        bad.append(("exit status", ex[0].group(2), rc))
    if len(pids) > 1:
        bad.append(("more than one pid", sorted(pids)[:4]))
    return bad


def expected_op(a, i, mutant):
    f = "%s/%s" % (W, a)
    fl = not mutant
    if a in APPEND:
        r = APPEND[a]
        seq = ["pwrite64 <%s> %d %d = %d" % (f, r, APPEND_BASE + r * i, r)]
        if a != "nosync25" and fl:
            seq.append("fsync <%s> = 0" % f)
    elif a in REC:
        rec, cap = REC[a], CAP[a]
        seq = ["pwrite64 <%s> %d %d = %d" % (f, rec, (i % (cap // rec)) * rec, rec)]
        if fl:
            seq.append("%s <%s> = 0" % ("fdatasync" if a == "fdatasync4k" else "fsync", f))
    elif a == "clean":
        seq = ["fsync <%s> = 0" % f]
    else:
        d, src = "%s/%s.clones" % (W, a), "%s/%s.src" % (W, a)
        c = "%s/c%d" % (d, i)
        seq = ["openat <%s> c%d O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW 0644 = <%s>" % (d, i, c)]
        if a == "cfr2b":
            seq.append("copy_file_range <%s> [0] <%s> NULL 1048576 0 = 1048576" % (src, c))
        else:
            seq.append("ioctl <%s> FICLONE <%s> = 0" % (c, src))
        if a in ("clone2b", "cfr2b") and fl:
            seq.append("fsync <%s> = 0" % c)
        seq.append("close <%s> = 0" % c)
        if fl:
            seq.append("fsync <%s> = 0" % d)
    return seq


def sequence_problems(calls, arms, n, mutant, other=None, pids=None, rc=None):
    """Every way the loop's syscalls differ from the definition: windows, gaps, rounds, stray lines. [] means exact."""
    bad = []
    if other is not None:
        bad += other_problems(other, pids or set(), rc)
    run = ran(arms, KIND)
    clocks = [k for k, (name, _) in enumerate(calls) if name == "clock_gettime"]
    odd = [calls[k][1] for k in clocks if calls[k][1] != CLOCK]
    if odd:
        bad.append(("a clock read is not CLOCK_MONOTONIC_RAW = 0", odd[:2]))
    if len(clocks) != 2 * n * len(run):
        bad.append(("clock reads", len(clocks), 2 * n * len(run)))
        return bad
    wins = [[t for _, t in calls[clocks[2 * w] + 1:clocks[2 * w + 1]]] for w in range(n * len(run))]
    for g in range(n * len(run) - 1):
        gap = calls[clocks[2 * g + 1] + 1:clocks[2 * g + 2]]
        if gap:
            bad.append(("syscalls between ops", g, [t for _, t in gap][:3]))
    for r in range(n):
        left = set(run)
        for w in wins[r * len(run):(r + 1) * len(run)]:
            hit = [a for a in left if expected_op(a, r, mutant) == w]
            if len(hit) != 1:
                bad.append(("window matches no remaining arm", r, w[:5], sorted(left)))
                continue
            left.discard(hit[0])
        if len(bad) > 12:
            break
    return bad


def round_orders(calls, arms, n):
    run = ran(arms, KIND)
    clocks = [k for k, (name, _) in enumerate(calls) if name == "clock_gettime"]
    orders = set()
    for r in range(n):
        o = []
        for w in range(r * len(run), (r + 1) * len(run)):
            first = calls[clocks[2 * w] + 1][1] if clocks[2 * w] + 1 < clocks[2 * w + 1] else ""
            o.append(next((a for a in run if "/%s>" % a in first or "/%s." % a in first), "?"))
        orders.add(tuple(o))
    return len(orders)


def plants(calls, text):
    """Planted breaches of a real trace: (name, calls, other). Each must be rejected by sequence_problems."""
    out = []
    clocks = [j for j, (name, _) in enumerate(calls) if name == "clock_gettime"]
    lo, hi = (clocks[0], clocks[-1]) if clocks else (0, -1)

    def find(pred):  # inside the loop only: a planted breach in setup or teardown is outside what the check covers
        return next((k for k in range(lo, hi) if pred(calls[k][1])), None)

    def with_text(k, t):
        c = list(calls)
        c[k] = (t.split(" ", 1)[0], t)
        return c

    k = find(lambda t: t.startswith("fsync <%s/append25>" % W))
    if k is not None:  # the barrier moved past the closing clock read, out of the timed window
        c = list(calls)
        e = c.pop(k)
        c.insert(k + 1, e)
        out.append(("append25's fsync moved out of its timed window", c, None))
    k = find(lambda t: t.startswith("fsync <%s/ow1m>" % W))
    if k is not None:
        out.append(("ow1m fsyncs the clean arm's file", with_text(k, "fsync <%s/clean> = 0" % W), None))
    k = find(lambda t: t.startswith("pwrite64 <%s/ow4k> 4096 " % W))
    if k is not None:
        out.append(("ow4k writes 25 B", with_text(k, calls[k][1].replace(" 4096 ", " 25 ", 1).replace("= 4096", "= 25")), None))
    hits = [j for j in range(lo, hi) if calls[j][1].startswith("pwrite64 <%s/ow1m> 1048576 %d " % (W, 2 << 20))]
    if len(hits) >= 2:  # in the loop, i=2 and, after the wrap at 128 MiB, i=130 both write at 2 MiB
        out.append(("ow1m does not wrap at 128 MiB", with_text(hits[1], calls[hits[1]][1].replace(
            " %d = " % (2 << 20), " %d = " % (130 << 20))), None))
    k = find(lambda t: t.startswith("fdatasync <%s/fdatasync4k>" % W))
    if k is not None:
        out.append(("fdatasync4k issues fsync instead", with_text(k, "fsync <%s/fdatasync4k> = 0" % W), None))
    k = find(lambda t: t.startswith("pwrite64 <%s/nosync25>" % W))
    if k is not None:
        c = list(calls)
        c.insert(k + 1, ("fsync", "fsync <%s/nosync25> = 0" % W))
        out.append(("nosync25 gains a flush", c, None))
    if len(clocks) >= 4:
        c = list(calls)
        c.insert(clocks[1] + 1, ("getpid", "getpid = 4242"))
        out.append(("a syscall between two ops", c, None))
        out.append(("one timed window dropped", calls[:clocks[2]] + calls[clocks[3] + 1:], None))
    k = find(lambda t: t.startswith("pwrite64 <%s/append64> 64 " % W))
    if k is not None:  # 64 B at append25's offset pattern (4096 + 25 i) instead of 4096 + 64 i
        parts = calls[k][1].split(" ")
        i = (int(parts[3]) - APPEND_BASE) // 64
        out.append(("append64 writes at append25's offset",
                    with_text(k, "pwrite64 <%s/append64> 64 %d = 64" % (W, APPEND_BASE + 25 * (i + 1))), None))
    k = find(lambda t: t.startswith("copy_file_range <%s/cfr2b.src>" % W))
    if k is not None:
        out.append(("cfr2b copies 4096 B", with_text(k, calls[k][1].replace(" 1048576 0 = 1048576", " 4096 0 = 4096")), None))
    k = find(lambda t: t.startswith("fsync <%s/cfr2b.clones/c" % W))
    if k is not None:
        c = list(calls)
        c.pop(k)
        out.append(("cfr2b skips the clone's own fsync", c, None))
    # item 14: a flush hidden in a split line. Text level: the parser must not drop it silently.
    lines = text.splitlines()
    j = next((x for x, l in enumerate(lines) if re.match(r"^\d+\s+pwrite64\(\d+<%s/nosync25>" % re.escape(W), l)), None)
    if j is not None:
        pid = lines[j].split()[0]
        fd = re.match(r"^\d+\s+pwrite64\((\d+)<", lines[j]).group(1)
        planted = lines[:j + 1] + ["%s fsync(%s<%s/nosync25> <unfinished ...>" % (pid, fd, W),
                                   "%s <... fsync resumed>) = 0" % pid] + lines[j + 1:]
        c2, o2, p2 = parse_trace("\n".join(planted))
        out.append(("nosync25 gains a split fsync (<unfinished ...> / <... resumed>)", c2, (o2, p2)))
    if KIND != "ext4":
        k = find(lambda t: t.startswith("fsync <%s/clone1b.clones>" % W))
        if k is not None:
            out.append(("clone1b fsyncs its source, not the directory", with_text(k, "fsync <%s/clone1b.src> = 0" % W), None))
        k = find(lambda t: t.startswith("ioctl ") and " FICLONE " in t)
        if k is not None:
            out.append(("FICLONERANGE in place of FICLONE", with_text(k, calls[k][1].replace(" FICLONE ", " FICLONERANGE ")), None))
        k = find(lambda t: t.startswith("fsync <%s/clone2b.clones/c" % W))
        if k is not None:
            c = list(calls)
            c.pop(k)
            out.append(("clone2b skips the clone's own fsync", c, None))
    return out


# ---- a full batch's summary against its raw.tsv -----------------------------------------------------------------
def summary_vs_raw(sj, rows, n):
    bad, p50 = [], {}
    wins = []
    for a, v in rows.items():
        if sorted(i for i, _, _ in v) != list(range(n)) or any(ns <= 0 for _, ns, _ in v):
            bad.append(("rows", a, len(v)))
        wins += [(t0, t0 + ns, a, i) for i, ns, t0 in v]
        x = sorted(ns for _, ns, _ in v)
        p50[a] = pct(x, .5)
        mine = {"min_us": x[0], "p1_us": pct(x, .01), "p10_us": pct(x, .1), "p50_us": pct(x, .5),
                "p90_us": pct(x, .9), "p99_us": pct(x, .99), "max_us": x[-1], "mean_us": sum(x) / len(x)}
        q = len(v) // 4
        if q >= 2:  # stationarity: the first and last quarter of the arm's ops, in op order
            by_i = [ns for _, ns, _ in sorted(v)]
            mine["p50_q1_us"] = pct(sorted(by_i[:q]), .5)
            mine["p50_q4_us"] = pct(sorted(by_i[-q:]), .5)
        theirs = sj.get("arms", {}).get(a, {})
        for k, ns in mine.items():
            if k not in theirs or abs(theirs[k] - ns / 1e3) > 0.051:
                bad.append(("summary disagrees with raw", a, k, theirs.get(k), round(ns / 1e3, 2)))
    wins.sort()
    for x, y in zip(wins, wins[1:]):
        if y[0] < x[1]:
            bad.append(("op windows overlap", x, y))
            break
    d0 = p50.get("nosync25")
    ratios = {a: (p50[a] / d0 if d0 else 1e18) for a in p50 if a != "nosync25"}
    ex, fail = timing_expect(sj, p50)
    fca = sj.get("flush_control_arms", {})
    for a, r in ratios.items():
        s = fca.get(a, {})
        if abs(s.get("ratio", -1) - r) > 0.051 or s.get("gated") != (a in GATED) or \
                s.get("timing_gated") != (a == "append25") or s.get("pass") != (r > ex["threshold"]):
            bad.append(("flush_control_arms disagrees with raw", a, s, round(r, 2)))
    tc = str(sj.get("timing_control", ""))
    if not tc.startswith(ex["timing"]) or (ex["timing"] in ("pass",) and tc != "pass") or sj.get("flush_control") != tc:
        bad.append(("timing_control disagrees with raw and the leaf (A14, A17)", tc, ex["timing"], sj.get("flush_control")))
    if ex["d0"] is not None and not str(sj.get("d0_control", "")).startswith(ex["d0"]):
        bad.append(("d0_control disagrees with raw", sj.get("d0_control"), ex["d0"]))
    if sj.get("d0_threshold_key") != ex["key"] or abs((sj.get("d0_threshold") or 0) - ex["threshold"]) > 0.0051 or \
            sj.get("d0_threshold_ref") != ex["ref"] or sj.get("timing_gated_arms") != ["append25"]:
        bad.append(("the d0 threshold is not the registered one for this class (A17)", sj.get("d0_threshold_key"),
                    sj.get("d0_threshold"), sj.get("d0_threshold_ref"), ex))
    if "append25" in p50 and d0 and abs(sj.get("flush_d0_p50_ratio", -1) - p50["append25"] / d0) > 0.051:
        bad.append(("flush_d0_p50_ratio", sj.get("flush_d0_p50_ratio"), p50["append25"] / d0))
    if "append25" in p50 and "clean" in p50:
        r = p50["append25"] / p50["clean"]
        fast = sum(1 for _, ns, _ in rows["clean"] if ns < 100000) / n
        if abs(sj.get("dirty_clean_p50_ratio", -1) - r) > 0.051 or abs(sj.get("clean_fast_frac", -1) - fast) > 0.00006:
            bad.append(("dirty/clean fields", sj.get("dirty_clean_p50_ratio"), r, sj.get("clean_fast_frac"), fast))
        if sj.get("dirty_clean_m0") != ("met" if r > 10 else "not met (report only)"):
            bad.append(("dirty_clean_m0", sj.get("dirty_clean_m0")))
    return bad, ratios, fail, p50


def leaf_class_of(sj):
    if not sj or not isinstance(sj.get("leaf"), dict):
        return None
    if sj["leaf"].get("kind") == "brd":
        return "brd"
    if sj["leaf"].get("kind") != "drive":
        return None
    wc = sj["leaf"].get("write_cache")
    return "wb" if wc == "write back" else "wt" if wc == "write through" else None


def wce_from_hex(src):
    """The WCE bit re-read from the MODE SENSE(10) bytes the probe recorded (fourth review M2): 'header b0 .. b7;
    page at OFF p0 p1 p2 ...' -> (wce, problems)."""
    m = re.search(r"header((?: [0-9a-f]{2}){8}); page at (\d+)((?: [0-9a-f]{2})+)", src or "")
    if not m:
        return None, ["no MODE SENSE bytes in the drive report source"]
    hdr = [int(x, 16) for x in m.group(1).split()]
    off = int(m.group(2))
    page = [int(x, 16) for x in m.group(3).split()]
    bad = []
    if off != 8 + (hdr[6] << 8 | hdr[7]):
        bad.append(("page offset vs block descriptor length", off, hdr[6:8]))
    if len(page) < 3 or page[0] & 0x3F != 0x08 or page[0] & 0x40 or page[1] < 1:
        bad.append(("not a caching page", page[:3]))
        return None, bad
    return (page[2] >> 2) & 1, bad


# the SCSI hosts an sd leaf may sit on, written by hand from the probe's header (fifth review M1, M2; sixth review M1:
# no SAS or RAID HBA, whose disk may be a controller logical volume answering for the drive)
SD_HOSTS = ["ahci", "ata_piix", "storvsc_host", "virtio_scsi", "vmw_pvscsi", "ibmvscsi"]
QUAL = {True: "a virtual drive", None: "virtualization not ruled out"}


def cell_box_check(box, leaf, f3, kv, out):
    """cell:box (called by main; the self-test drives it on planted records)."""
    bbad = []
    if box["virt"] not in ("vm", "bare"):
        bbad.append(("systemd-detect-virt gave nothing", kv.get("detect_virt")))
    if box["flip"] not in ("yes", "no"):
        bbad.append(("no leafdisk record", kv.get("leafdisk")))
    if box["plp"] not in ("yes", "no"):
        bbad.append(("no plp declaration (V3_PLP)", kv.get("plp")))
    fp3 = f3.get("flush_path") or []
    want_disk = (kv.get("root_disk") or "").split(" ")[0] if leaf == "brd" else (fp3[-1].get("disk") if fp3 else None)
    if not box["leafdisk"] or box["leafdisk"] != want_disk:
        bbad.append(("the leaf disk the plants used is not the cell's", box["leafdisk"], want_disk))
    lf3 = f3.get("leaf") or {}
    if leaf in ("wb", "wt"):
        wflip = "yes" if leaf == "wb" or lf3.get("driver") == "sd" else "no"
        if box["flip"] != wflip:
            bbad.append(("flip from the leaf disk's record vs the probe's leaf", box["flip"], wflip, lf3.get("driver")))
    if (kv.get("box") or "").strip() != "virt=%s,flip=%s,plp=%s" % (box["virt"], box["flip"], box["plp"]):
        bbad.append(("firecheck.sh's box line differs from check.py's reading of the same facts", kv.get("box"), box))
    if box["flip"] == "no" and not os.path.exists(os.path.join(out, "F4", "R_leaf_flip.na")):
        bbad.append("flip no, yet firecheck.sh's flip() did not record that it could not plant (R_leaf_flip.na)")
    check("cell:box", not bbad, {"box": box, "bad": bbad},
          "the box is known: systemd-detect-virt answered (vm or bare), and the disk the leaf plants used is this cell's "
          "leaf (the root disk on a brd cell), its flip class read from its write cache and driver")


def cell_leaf_check(lf, leaf, leaf_src, kv):
    """cell:leaf (called by main; the self-test calls it on planted leaf records)."""
    lbad = []
    if leaf is None:
        lbad.append("no leaf class (no F3 or F1 summary)")
    elif leaf == "brd":
        if v3cell.is_loop(CELL) or kv.get("brd_cell") != "1":
            lbad.append("a brd leaf outside a brd cell")
        if lf.get("creditable") is not False:
            lbad.append("a brd leaf not marked creditable false")
    else:
        lbad += leaf_problems(lf, kv.get("brd_cell") == "1")
    check("cell:leaf", not lbad, {"leaf": lf, "class": leaf, "from": leaf_src, "bad": lbad},
          "the leaf is an allowlisted drive (kind drive, creditable; NVMe over pcie; sd on an allowlisted host) whose own "
          "cache report agrees with the kernel, an sd report re-read from its MODE SENSE bytes (or brd on a brd cell)")


def leaf_problems(lf, brd_cell):
    """A drive leaf record's problems (cell:leaf), from the record alone."""
    bad = []
    if lf.get("driver") not in LEAF_DRIVERS:
        bad.append(("driver", lf.get("driver")))
    if lf.get("kind") != "drive" or lf.get("creditable") is not True:
        bad.append(("kind/creditable", lf.get("kind"), lf.get("creditable")))
    if lf.get("drive_reports") != lf.get("write_cache"):
        bad.append(("drive report vs kernel", lf.get("drive_reports"), lf.get("write_cache")))
    if lf.get("driver") == "sd":  # the WCE bit re-read from the recorded reply bytes (fourth review M2)
        w, hp = wce_from_hex(lf.get("drive_report_source"))
        if hp or w is None or ("write back" if w else "write through") != lf.get("drive_reports"):
            bad.append(("MODE SENSE bytes vs the drive report", w, hp, lf.get("drive_reports")))
        if lf.get("sd_host") not in SD_HOSTS:
            bad.append(("sd host outside the allowlist", lf.get("sd_host")))
    if lf.get("driver") == "nvme" and (not lf.get("nvme_transport") or any(t != "pcie" for t in lf.get("nvme_transport"))):
        bad.append(("nvme transport", lf.get("nvme_transport")))
    if brd_cell:
        bad.append("a brd cell whose leaf is not brd")
    return bad


def virt_problems(vz, dvirt, arch):
    """virtualization against systemd-detect-virt (an instrument outside the probe; fourth review M1)."""
    vm = vz.get("virtualized", "absent")
    bad = []
    if vm not in (True, False, None):
        bad.append("virtualized is %r, not true, false or null" % (vm,))
    if (vm is True) != bool(vz.get("evidence")):
        bad.append("virtualized %r with %d evidence items" % (vm, len(vz.get("evidence") or [])))
    if not dvirt:
        bad.append("systemd-detect-virt gave nothing (the independent instrument is missing)")
    elif dvirt != "none" and vm is not True:
        bad.append("systemd-detect-virt says %r, the probe says virtualized %r" % (dvirt, vm))
    if vm is False and (arch != "x86_64" or dvirt != "none"):
        bad.append("bare metal claimed on %s with systemd-detect-virt %r" % (arch, dvirt))
    return bad


def linkage_problems(sj):
    """the binary that ran is the static build: only its own file mapped (fourth review M4)."""
    mf = sj.get("mapped_files")
    if sj.get("linkage") != "static" or not isinstance(mf, list) or len(mf) != 1:
        return [("linkage/mapped_files", sj.get("linkage"), mf)]
    return []


def qualifier_problems(text, vm, leaf, what):
    """a label on a wb or wt leaf carries the VM qualifier unless bare metal is shown, and only then (L2, L3)."""
    if leaf not in ("wb", "wt"):
        return []
    q = QUAL.get(vm) if vm is not False else None
    if q and q not in text:
        return [("%s lacks the qualifier" % what, text, q)]
    if vm is False and "virtual" in text:
        return [("%s qualified on bare metal" % what, text)]
    return []


def harness_moved(start, now):
    """files whose hash differs, or that are missing on either side (both key sets: fifth review L2)."""
    if not isinstance(start, dict):
        return ["no start-time harness record"]
    return sorted(f for f in set(start) | set(now) if start.get(f) != now.get(f) or now.get(f) is None)


VIRT_KIND = {  # leaf class -> virtualized (True, False, None) -> floor_kind, written by hand from the probe's header
    "wb": {False: "drive flush", True: "virtual drive flush: reach to media unknown",
           None: "drive flush, virtualization not ruled out: reach to media unknown"},
    "wt": {False: "no volatile cache: no drive flush",
           True: "no volatile cache (a virtual drive's report): no drive flush, host caching unknown",
           None: "no volatile cache (virtualization not ruled out): no drive flush, host caching unknown"},
}


FLUSH_SENT = {  # leaf class -> virtualized -> flush_sent_to_device prefix, written by hand from the probe's texts
    "wb": {False: "yes: the leaf reports a volatile write cache and the drive agrees",
           True: "yes, to the virtual device:", None: "yes, to the device (virtualization not ruled out):"},
    "wt": {False: "no: the leaf reports write-through and the drive agrees",
           True: "no: the (virtual) leaf reports write-through",
           None: "no: the leaf reports write-through and its device report agrees"},
}
BRD_LABELS = ("brd: no drive (fire-check only, never credited)", "no: the leaf is brd")
FLUSH_SENT_QUAL = {  # the qualifier each virtual or not-ruled-out flush_sent_to_device text must carry, by hand
    "wb": {True: "whether the host forwards it to media is unknown",
           None: "that it is a drive and not a host's emulation is not shown"},
    "wt": {True: "the host's own caching is unknown", None: "a host's own caching would be unknown"},
}


def label_problems(sj, leaf, vm):
    """floor_kind, flush_sent_to_device and the probe's floor_claim against the hand-written tables for this leaf
    class and virtualization (sixth review M2: every one of the six wb/wt x true/false/null texts is compared)."""
    if leaf == "brd":
        fk, fs = BRD_LABELS
    elif leaf in VIRT_KIND and (vm is True or vm is False or vm is None):  # identity: 1 and 0 are not answers
        fk, fs = VIRT_KIND[leaf][vm], FLUSH_SENT[leaf][vm]
    else:
        return [("labels", "no labels for leaf %r virtualized %r" % (leaf, vm))]
    bad = []
    if sj.get("floor_kind") != fk:
        bad.append(("floor_kind", sj.get("floor_kind"), fk))
    fstxt = str(sj.get("flush_sent_to_device", ""))
    q = FLUSH_SENT_QUAL.get(leaf, {}).get(vm) if vm is not False else None
    if not fstxt.startswith(fs) or (q and q not in fstxt):
        bad.append(("flush_sent_to_device", fstxt, fs, q))
    if leaf != "brd":
        bad += qualifier_problems(str(sj.get("floor_claim", "")), vm, leaf, "floor_claim")
    return bad


def box_of(kv):
    """The box the fire-check ran on, from facts firecheck.sh recorded (sixth review H1): virt = vm when
    systemd-detect-virt names a hypervisor, bare when it says none; flip = yes when the leaf disk's write cache can be
    made to disagree with the drive (write back: set it write through; an sd disk: sd's 'temporary write back')."""
    dv = (kv.get("detect_virt") or "").strip()
    m = re.match(r"(\S+) write_cache=\[(write back|write through)\] scsi=([01])$", (kv.get("leafdisk") or "").strip())
    plp = (kv.get("plp") or "").strip()
    return {"virt": "bare" if dv == "none" else "vm" if dv else "unknown",
            "flip": ("yes" if m.group(2) == "write back" or m.group(3) == "1" else "no") if m else "unknown",
            "plp": plp if plp in ("yes", "no") else "unknown",
            "leafdisk": m.group(1) if m else None}


def find_leaf_class():
    """The leaf class from the F3 batch, else from F1's first run (every probe run on the work dir sees one leaf)."""
    for p in (os.path.join(OUT, "F3", "summary.json"), os.path.join(OUT, "F1", "nosync25.n1.out", "summary.json")):
        c = leaf_class_of(rj(p))
        if c:
            return c, p
    return None, None


# ---- main -------------------------------------------------------------------------------------------------------
def self_test():
    """The checker's own rules on planted records, expectations by hand (fifth review M3)."""
    res = []

    def chk(name, ok, detail=""):
        res.append(bool(ok))
        print(("PASS " if ok else "FAIL ") + name + ("" if ok else ": " + str(detail)[:300]), flush=True)

    hexs = ("SCSI MODE SENSE(10) caching page WCE=0 via SG_IO on /dev/sda (reply header 00 12 00 10 00 00 00 00; page "
            "at 8 08 0a 00 00 00 00 00 00 00 00 00 00; sd's cache_type reads 'write through')")
    sd = {"driver": "sd", "kind": "drive", "creditable": True, "write_cache": "write through",
          "drive_reports": "write through", "drive_report_source": hexs, "sd_host": "storvsc_host"}
    nv = {"driver": "nvme", "kind": "drive", "creditable": True, "write_cache": "write back", "drive_reports": "write back",
          "nvme_transport": ["pcie"]}
    chk("leaf: a write-through Hyper-V sd leaf passes", leaf_problems(sd, False) == [], leaf_problems(sd, False))
    chk("leaf: a pcie NVMe leaf passes", leaf_problems(nv, False) == [], leaf_problems(nv, False))
    for name, lf in (("sd host tcm_loopback", dict(sd, sd_host="tcm_loopback")), ("sd host unreadable", dict(sd, sd_host="")),
                     ("NVMe over tcp", dict(nv, nvme_transport=["pcie", "tcp"])), ("no NVMe transport", dict(nv, nvme_transport=[])),
                     ("MODE SENSE bytes say WCE=1 under a write-through report",
                      dict(sd, drive_report_source=hexs.replace("08 0a 00", "08 0a 04"))),
                     ("no MODE SENSE bytes", dict(sd, drive_report_source="SCSI x")),
                     ("a scsi_debug kind", dict(sd, kind="scsi_debug", creditable=False)), ("driver dm", dict(nv, driver="dm"))):
        chk("leaf refused: %s" % name, leaf_problems(lf, False) != [], lf)
    chk("leaf refused: a drive leaf on a brd cell", leaf_problems(sd, True) != [])
    ev = ["leaf: drive model 'Virtual Disk' names 'Virtual'"]
    chk("virt: a VM with evidence and detect-virt microsoft passes",
        virt_problems({"virtualized": True, "evidence": ev}, "microsoft", "aarch64") == [])
    chk("virt: x86 bare metal with detect-virt none passes", virt_problems({"virtualized": False, "evidence": []}, "none", "x86_64") == [])
    chk("virt: arm64 not ruled out with detect-virt none passes", virt_problems({"virtualized": None, "evidence": []}, "none", "aarch64") == [])
    for name, vz, dv, ar in (("false under detect-virt microsoft", {"virtualized": False, "evidence": []}, "microsoft", "x86_64"),
                             ("null under detect-virt kvm", {"virtualized": None, "evidence": []}, "kvm", "x86_64"),
                             ("false on aarch64", {"virtualized": False, "evidence": []}, "none", "aarch64"),
                             ("true without evidence", {"virtualized": True, "evidence": []}, "microsoft", "x86_64"),
                             ("false with evidence", {"virtualized": False, "evidence": ev}, "none", "x86_64"),
                             ("detect-virt missing", {"virtualized": True, "evidence": ev}, "", "x86_64"),
                             ("a bool-less value", {"virtualized": "yes", "evidence": ev}, "microsoft", "x86_64")):
        chk("virt refused: %s" % name, virt_problems(vz, dv, ar) != [], (vz, dv, ar))
    chk("linkage: static with one mapped file passes", linkage_problems({"linkage": "static", "mapped_files": ["/x/v3floor"]}) == [])
    for name, sj in (("dynamic", {"linkage": "not static", "mapped_files": ["/x/v3floor"]}),
                     ("two mapped files", {"linkage": "static", "mapped_files": ["/x/v3floor", "/lib/libc.so.6"]}),
                     ("no record", {})):
        chk("linkage refused: %s" % name, linkage_problems(sj) != [], sj)
    for vm, leaf, text, ok in ((True, "wb", "x; a virtual drive: reach to media unknown", True), (True, "wb", "x", False),
                               (None, "wt", "x (virtualization not ruled out: ...)", True), (None, "wt", "x (a virtual drive)", False),
                               (False, "wb", "x", True), (False, "wt", "x (a virtual drive: ...)", False),
                               (True, "brd", "none: a brd floor", True)):
        chk("qualifier: %r on %s, %r -> %s" % (vm, leaf, text, "ok" if ok else "refused"),
            (qualifier_problems(text, vm, leaf, "t") == []) == ok, qualifier_problems(text, vm, leaf, "t"))
    a = {"run.sh": "1", "check.py": "2"}
    chk("harness: equal sets pass", harness_moved(dict(a), dict(a)) == [])
    chk("harness: a changed file", harness_moved(dict(a), dict(a, **{"check.py": "3"})) == ["check.py"])
    chk("harness: a file missing now", harness_moved(dict(a), dict(a, **{"check.py": None})) == ["check.py"])
    chk("harness: a file only at the start (deleted since)", harness_moved(dict(a, extra="4"), dict(a)) == ["extra"])
    chk("harness: a file only now (added since)", harness_moved(dict(a), dict(a, extra="4")) == ["extra"])
    chk("harness: no start record", harness_moved(None, dict(a)) != [])
    w1 = hexs.replace("WCE=0", "WCE=1").replace("08 0a 00", "08 0a 04")
    chk("MODE SENSE bytes: WCE=0 and WCE=1 read back", wce_from_hex(hexs)[0] == 0 and wce_from_hex(w1)[0] == 1)
    # eighth review L5: REGISTERED.tsv has one strict rule (three non-empty fields, a plain decimal threshold)
    import tempfile as _tf
    rd_ = _tf.mkdtemp(prefix="check-reg-")
    for name, text, ok in (("a good line", "d0_threshold/ext4/wb/bare\t9.5\tDECISIONS x\n", True),
                           ("two fields", "d0_threshold/ext4/wb/bare\t9.5\n", False),
                           ("a CR", "d0_threshold/ext4/wb/bare\t9.5\tref\r\n", False),
                           ("an exponent", "d0_threshold/ext4/wb/bare\t1e1\tref\n", False),
                           ("inf", "d0_threshold/ext4/wb/bare\tinf\tref\n", False),
                           # ninth review M7: what C's jstr and check.py's UTF-8 decode would read differently, and
                           # what C's buffers would split or truncate
                           ("a non-ASCII ref", "d0_threshold/ext4/wb/bare\t9.5\tDECISIONS \u2026 (PREREG \u00a74)\n", False),
                           ("a ref of 201 bytes", "d0_threshold/ext4/wb/bare\t9.5\t" + "r" * 201 + "\n", False),
                           ("a ref of 200 bytes", "d0_threshold/ext4/wb/bare\t9.5\t" + "r" * 200 + "\n", True),
                           ("a comment line of 1100 bytes", "#" + "c" * 1099 + "\nd0_threshold/ext4/wb/bare\t9.5\tref\n", False),
                           ("a value of 61 bytes", "frame_arm\t" + "o" * 61 + "\tref\n", False),
                           ("a NUL byte", "d0_threshold/ext4/wb/bare\t9.5\tref\x00x\n", False),
                           # ninth review L9: append25 is already in the bound shape; a frame arm append25 names it twice
                           ("frame_arm append25", "frame_arm\tappend25\tref\n", False),
                           ("frame_arm ow64k", "frame_arm\tow64k\tref\n", True),
                           ("frame_arm clean (not an M0 append or overwrite arm)", "frame_arm\tclean\tref\n", False),
                           # tenth review LOW 2: every line under the one rule; a key twice is refused, not last-wins
                           ("frame_arm twice", "frame_arm\tow4k\tref\nframe_arm\tow64k\tref\n", False),
                           ("a threshold twice", "d0_threshold/ext4/wb/bare\t9.5\tr\nd0_threshold/ext4/wb/bare\t9.6\tr\n", False),
                           ("an unknown key", "frame_arms\tow4k\tref\n", False)):
        open(os.path.join(rd_, "r.tsv"), "w").write("# comment\n" + text)
        try:
            registered(rd_, "r.tsv")
            got = True
        except ValueError:
            got = False
        chk("REGISTERED.tsv rule: %s -> %s" % (name, "read" if ok else "refused"), got is ok)
    try:  # tenth review LOW 4: an unreadable registry refuses, never reads as an empty one
        registered(rd_, "no-such-registry.tsv")
        unread = "read as %r" % (registered(rd_, "no-such-registry.tsv"),)
    except (ValueError, OSError) as e:
        unread = "refused: %r" % e
    chk("REGISTERED.tsv rule: an unreadable registry -> refused", unread.startswith("refused"), unread)
    import shutil as _sh
    _sh.rmtree(rd_)
    # tenth review HIGH 2: main() on an empty OUT, for every cell, arch, leaf class and box, evaluates exactly plan()'s
    # ids in plan()'s order (run 37845193906 failed "plan" on 12/12 cells on the order alone)
    import contextlib as _cl, io as _io, itertools as _it, tempfile as _tf2
    global CELL, KIND, W, OUT, results
    saved_g = (CELL, KIND, W, OUT, results)
    orig_flc, orig_box = find_leaf_class, box_of
    pbad, pruns = [], 0
    try:
        for cell, arch, lc, virt, flip, plp in _it.product(sorted(v3cell.CELLS), ("x86_64", "aarch64"), ("wb", "wt", "brd"),
                                                           ("vm", "bare"), ("yes", "no"), ("yes", "no")):
            tdp = _tf2.mkdtemp(prefix="check-plan-")
            open(os.path.join(tdp, "info.txt"), "w").write("arch=%s\n" % arch)
            bx = {"virt": virt, "flip": flip, "plp": plp}
            globals()["find_leaf_class"] = lambda lc=lc: (lc, "planted")
            globals()["box_of"] = lambda kv, bx=bx: dict(bx, leafdisk=None)
            results = []
            try:
                with _cl.redirect_stdout(_io.StringIO()), _cl.redirect_stderr(_io.StringIO()):
                    main([tdp, cell])
                ids = [r["id"] for r in results]
                pruns += 1
                if ids != plan(cell, arch, lc, bx):
                    pbad.append((cell, arch, lc, virt, flip, plp))
            except Exception as e:  # noqa: BLE001
                pbad.append((cell, arch, lc, virt, flip, plp, repr(e)[:120]))
            finally:
                _sh.rmtree(tdp, ignore_errors=True)
    finally:
        globals()["find_leaf_class"], globals()["box_of"] = orig_flc, orig_box
        CELL, KIND, W, OUT, results = saved_g
    chk("plan order: main() on an empty OUT evaluates exactly plan()'s ids in order, for all %d cell/arch/leaf/box "
        "combinations" % pruns, pruns == 288 and not pbad, pbad[:4])
    # ninth review H1, L10, M6: the A18 floor reference as the probe picks it -- among append25 (fsync) and
    # fdatasync4k only (the frame arm runs with fsync and is not a candidate, even when registered), on raw nanosecond
    # p50s (two arms within 0.1 us must not split), and the frame variant label for the registered frame arm

    def frp(sj, p50ns):
        try:
            return floor_reference_problems(sj, p50ns)
        except TypeError as e:  # the base takes no raw p50s
            return [("the floor rule reads no raw p50s", repr(e))]

    def fsj(p50us, fr, frame="ow4k", variant="fdatasync4k (ow4k + fdatasync)"):
        return {"arms": {a: {"p50_us": v} for a, v in p50us.items()}, "frame_arm": frame, "floor_frame_variant": variant,
                "floor_reference": fr}
    cheap_ow4k = {"append25": 875.1, "fdatasync4k": 900.0, "ow4k": 800.3}
    cheap_ns = {"append25": 875100, "fdatasync4k": 900000, "ow4k": 800300}
    for name, sj, ns, ok in (
            ("frame_arm ow4k registered and cheapest, the reference append25 (the probe's pick)",
             fsj(cheap_ow4k, {"arm": "append25", "barrier": "fsync", "p50_us": 875.1}), cheap_ns, True),
            ("frame_arm ow4k registered and cheapest, the reference ow4k (fsync, not a candidate)",
             fsj(cheap_ow4k, {"arm": "ow4k", "barrier": "fsync", "p50_us": 800.3}), cheap_ns, False),
            ("a near tie: fdatasync4k 181151 ns, append25 181249 ns (both 181.2 us), the reference fdatasync4k",
             fsj({"append25": 181.2, "fdatasync4k": 181.2}, {"arm": "fdatasync4k", "barrier": "fdatasync", "p50_us": 181.2},
                 None, "no frame arm registered"), {"append25": 181249, "fdatasync4k": 181151}, True),
            ("the same near tie, the reference append25 (not the raw minimum)",
             fsj({"append25": 181.2, "fdatasync4k": 181.2}, {"arm": "append25", "barrier": "fsync", "p50_us": 181.2},
                 None, "no frame arm registered"), {"append25": 181249, "fdatasync4k": 181151}, False),
            ("frame_arm ow64k registered: its variant recorded missing",
             fsj({"append25": 400.0, "fdatasync4k": 200.0, "ow64k": 500.0}, {"arm": "fdatasync4k", "barrier": "fdatasync",
                 "p50_us": 200.0}, "ow64k", "none in this probe: the registered frame arm has no fdatasync variant arm "
                 "(A18 needs one)"), {"append25": 400000, "fdatasync4k": 200000, "ow64k": 500000}, True),
            ("frame_arm ow4k registered, the variant label saying none registered",
             fsj(cheap_ow4k, {"arm": "append25", "barrier": "fsync", "p50_us": 875.1}, "ow4k", "no frame arm registered"),
             cheap_ns, False),
            ("no frame arm registered, the variant label missing",
             fsj({"append25": 400.0, "fdatasync4k": 200.0}, {"arm": "fdatasync4k", "barrier": "fdatasync", "p50_us": 200.0},
                 None, None), {"append25": 400000, "fdatasync4k": 200000}, False)):
        got = frp(sj, ns)
        chk("floor reference: %s -> %s" % (name, "pass" if ok else "refused"), (got == []) is ok, got)
    chk("MODE SENSE bytes: a sub-page (SPF) page is not a caching page",
        wce_from_hex(hexs.replace("page at 8 08", "page at 8 48"))[0] is None)
    # sixth review H1: the box and the plan it selects
    kvb = {"detect_virt": "none", "leafdisk": "nvme1n1 write_cache=[write through] scsi=0", "plp": "yes"}
    chk("box: detect-virt none, a write-through NVMe leaf, PLP declared -> bare, no flip, plp yes",
        box_of(kvb) == {"virt": "bare", "flip": "no", "plp": "yes", "leafdisk": "nvme1n1"}, box_of(kvb))
    chk("box: an unreadable write cache is unknown, never 'no flip' (seventh review M2)",
        box_of({"detect_virt": "none", "leafdisk": "nvme1n1 write_cache=[] scsi=0"})["flip"] == "unknown")
    chk("box: microsoft and a write-through sd leaf -> vm, flip (sd's temporary write back)",
        box_of({"detect_virt": "microsoft", "leafdisk": "sda write_cache=[write through] scsi=1"})["flip"] == "yes"
        and box_of({"detect_virt": "microsoft"})["virt"] == "vm")
    chk("box: nothing recorded -> unknown everywhere",
        box_of({}) == {"virt": "unknown", "flip": "unknown", "plp": "unknown", "leafdisk": None}, box_of({}))
    pb = plan("xfs", "x86_64", "wt", {"virt": "bare", "flip": "no"})
    pv = plan("xfs", "x86_64", "wt", {"virt": "vm", "flip": "yes"})
    chk("plan: a bare box with a write-through NVMe leaf plans neither R_virt_hidden nor R_leaf_flip, and R_virt_planted",
        "F4:R_virt_hidden" not in pb and "F4:R_leaf_flip" not in pb and "F4:R_virt_planted" in pb and "cell:box" in pb)
    chk("plan: a VM box with a flippable leaf plans both", "F4:R_virt_hidden" in pv and "F4:R_leaf_flip" in pv)
    # sixth review M2: every label text against its table, on all six wb/wt x virtualization combinations
    for lc in ("wb", "wt"):
        for vm in (True, False, None):
            good = {"floor_kind": VIRT_KIND[lc][vm],
                    "flush_sent_to_device": FLUSH_SENT[lc][vm] + " ... " + (FLUSH_SENT_QUAL[lc].get(vm) or ""),
                    "floor_claim": "per stack ..." + {True: "; on a virtual drive: reach to media unknown",
                                                      None: "; virtualization not ruled out: reach to media unknown",
                                                      False: ""}[vm]}
            others = [v for v in (True, False, None) if v is not vm]
            swapped = dict(good, flush_sent_to_device=FLUSH_SENT[lc][others[0]] + " ... " + (FLUSH_SENT_QUAL[lc].get(others[0]) or ""))
            unq = dict(good, flush_sent_to_device=FLUSH_SENT[lc][vm] + " ...")
            chk("labels: %s, virtualized %r: its own texts pass, another virtualization's flush_sent_to_device is "
                "refused%s" % (lc, vm, ", and so is its own prefix without the qualifier" if vm is not False else ""),
                label_problems(good, lc, vm) == [] and
                [t[0] for t in label_problems(swapped, lc, vm)] == ["flush_sent_to_device"] and
                (vm is False or [t[0] for t in label_problems(unq, lc, vm)] == ["flush_sent_to_device"]),
                (label_problems(good, lc, vm), label_problems(swapped, lc, vm)))
    chk("labels: virtualized 1 is not an answer", label_problems({}, "wb", 1) != [])
    # ninth review L12: stamp.py end compares the write cache of the flush path's devices only, and an unreadable one
    # is a problem of its own, never "a change"
    import importlib.util as _ilu
    _sp = _ilu.spec_from_file_location("v3stamp", os.path.join(HERE, "stamp.py"))
    _st = _ilu.module_from_spec(_sp)
    _sp.loader.exec_module(_st)
    wcp = getattr(_st, "write_cache_problems", None)
    a0 = {"nvme0n1": {"write_cache": "write back"}, "loop0": {"write_cache": "write back"}, "sdb": {"write_cache": "write back"}}
    for name, b1, devs, want in (
            ("the leaf flipped write back -> write through", dict(a0, nvme0n1={"write_cache": "write through"}),
             ["loop0", "nvme0n1"], "changed"),
            ("a device off the flush path flipped", dict(a0, sdb={"write_cache": "write through"}), ["loop0", "nvme0n1"], None),
            ("the leaf unreadable at the end", dict(a0, nvme0n1={"write_cache": ""}), ["loop0", "nvme0n1"], "unreadable"),
            ("the leaf gone at the end", {k: v for k, v in a0.items() if k != "nvme0n1"}, ["loop0", "nvme0n1"], "unreadable"),
            ("no flush-path devices named", a0, [], "no flush-path devices"),
            ("nothing changed", a0, ["loop0", "nvme0n1"], None)):
        got = wcp(a0, b1, devs) if wcp else ["(no write_cache_problems in stamp.py)"]
        ok = (got == []) if want is None else (bool(got) and all(want in x for x in got))
        chk("stamp write cache (ninth review L12): %s -> %s" % (name, "no problem" if want is None else want), ok, got)
    # V3 review 12 item 1 (HIGH): ONE anchored nest-chain matcher, nestloops.sh, sourced by mkfixtures.sh and
    # firecheck.sh. Its own self-test runs canned `losetup --list -n -O NAME,BACK-FILE` listings (/mnt/v3fx/nb/x.img,
    # nbx's backing, is NOT the chain; n1..n4's images and v3fx-n1.img are) and a losetup that fails (which must
    # never read as "nothing attached"); and neither script may keep a copy of the old prefix matcher
    import subprocess as _sp12
    nl = os.path.join(HERE, "nestloops.sh")
    r12 = _sp12.run(["bash", nl, "--self-test"], capture_output=True, text=True, timeout=120) if os.path.exists(nl) else None
    chk("nestloops.sh --self-test: the one anchored nest-chain matcher on canned listings (nb/x.img is not the chain; a "
        "failing losetup is an error)", r12 is not None and r12.returncode == 0 and "NESTLOOPS SELF-TEST" in r12.stdout
        and " PASS" in r12.stdout, (r12.returncode, r12.stdout[-300:], r12.stderr[-200:]) if r12 else "nestloops.sh absent")
    copies = []
    for nm12 in ("mkfixtures.sh", "firecheck.sh"):
        t12 = rd(os.path.join(HERE, nm12))
        if t12 is None or 'awk -v b="$base/n"' in t12 or 'awk -v b="$FX/n"' in t12 or '/nestloops.sh"' not in t12:
            copies.append(nm12)
    chk("one matcher: mkfixtures.sh and firecheck.sh source nestloops.sh and keep no copy of the old prefix matcher",
        not copies, copies)
    # sixth review M3: the call paths themselves -- check_real and cell_leaf_check on planted copies of a banked
    # write-back batch (run 37528595878, x86 ext4loop on NVMe, a VM)
    real_selftest(chk)
    ok = all(res) and len(res) > 0
    print("CHECK SELF-TEST %d/%d %s" % (sum(res), len(res), "PASS" if ok else "FAIL"))
    return 0 if ok else 1


BANKED_F3 = "f3-37812355435-x86-ext4loop"     # write-back MSFT NVMe, real (four format fields: its README)
BANKED_F3_WT = "f3-37812355435-arm-ext4loop"  # write-through Hyper-V sd, real (the same four fields)


def real_selftest(chk):
    import contextlib, io, shutil, tempfile
    global CELL, KIND, W, OUT, results, REGISTRY_FILE
    REGISTRY_FILE = os.path.join("testdata", "REGISTERED.selftest.tsv")
    src = os.path.join(HERE, "testdata", BANKED_F3)
    info = rd(os.path.join(src, "info.txt")) or ""
    kv = dict(l.split("=", 1) for l in info.splitlines() if "=" in l and not l.startswith(("loop ", "block ")))
    saved = (CELL, KIND, W, OUT, results)
    td = tempfile.mkdtemp(prefix="check-selftest-")

    def put_f1b(out, srcd, f1b):
        """eleventh review MED 2: OUT/F1b/real-all.trace.gz as a real verdict's OUT holds it: the same cell and run's
        F1b strace (testdata/<cell>/F1b, copied unchanged from artie), planted by f1b(text), or removed ("absent")."""
        fb = os.path.join(out, "F1b", "real-all.trace.gz")
        os.makedirs(os.path.dirname(fb), exist_ok=True)
        if os.path.exists(fb):
            os.remove(fb)
        if f1b == "absent":
            return
        t = rd(os.path.join(srcd, "F1b", "real-all.trace.gz"))
        with gzip.open(fb, "wt") as fh:
            fh.write(f1b(t) if f1b else t)

    def run(name, probe=None, merged=None, report=None, files=None, kvmut=None, f1b=None):
        global CELL, KIND, W, OUT, results
        put_f1b(td, src, f1b)
        d = os.path.join(td, name)
        shutil.copytree(os.path.join(src, "F3"), d)
        muts = [("summary.probe.json", probe), ("summary.json", merged), (os.path.join("blkflush", "report.json"), report)]
        muts += list((files or {}).items())
        for f, mut in muts:
            if not mut:
                continue
            fp_ = os.path.join(d, f)
            if f.endswith(".json"):
                j = rj(fp_)
                mut(j)
                with open(fp_, "w") as fh:
                    json.dump(j, fh)
            else:
                with open(fp_, "w") as fh:
                    fh.write(mut(rd(fp_)))
        k2 = dict(kv)
        if kvmut:
            kvmut(k2)
        CELL, KIND, W, OUT, results = "ext4loop", "ext4", k2.get("work", ""), td, []
        with contextlib.redirect_stdout(io.StringIO()):
            check_real(d, 0, k2, "wb")
        got = {r["id"]: r for r in results}
        return got

    def tags(r):
        return [x[0] if isinstance(x, (list, tuple)) else x for x in (r.get("detail") or {}).get("bad", [])]

    try:
        F3IDS = ["F3:complete", "F3:devflush", "F3:merge", "F3:gate", "F3:record"]
        g = run("control")
        chk("real: the banked write-back batch passes all five F3 checks unplanted",
            [i for i in F3IDS if g.get(i, {}).get("pass") is not True] == [], {i: tags(g.get(i, {})) for i in F3IDS})

        def both(f):  # a probe field: planted in the probe's summary and in the merged one, so only its rule fires
            return {"probe": f, "merged": f}

        # eleventh review MED 2's F1b plants, on the banked strace text (expectations by hand)
        def f1b_fd99(t):  # the first in-loop cfr2b clone fsync names fd 99 (the other 299 keep the real fd)
            return re.sub(r"fsync\(\d+<([^>]*/cfr2b\.clones/c\d+)>\)", r"fsync(99<\1>)", t, count=1)

        def f1b_lines(t):
            ls = t.split("\n")
            return ls, next(k for k, l in enumerate(ls) if "clock_gettime(CLOCK_MONOTONIC_RAW" in l)

        def f1b_nosync(t):  # an fsync of nosync25's own file right after its first in-loop pwrite64 (inside its window)
            ls, c0 = f1b_lines(t)
            j = next(k for k in range(c0, len(ls)) if re.match(r"^\d+\s+pwrite64\(\d+<[^>]*/nosync25>", ls[k]))
            m = re.match(r"^(\d+)\s+pwrite64\((\d+)<([^>]*)>", ls[j])
            return "\n".join(ls[:j + 1] + ["%s  fsync(%s<%s>) = 0" % m.groups()] + ls[j + 1:])

        def f1b_setup(t):  # an fsync on fd 42 of append25's file BEFORE the first timed window (setup)
            ls, c0 = f1b_lines(t)
            m = next(mm for mm in (re.match(r"^(\d+)\s+fsync\(\d+<([^>]*/append25)>\)", l) for l in ls[:c0]) if mm)
            return "\n".join(ls[:c0] + ["%s  fsync(42<%s>) = 0" % m.groups()] + ls[c0:])

        cases = [
            ("floor_kind", both(lambda j: j.update(floor_kind=VIRT_KIND["wb"][False])), "F3:record", ["floor_kind"]),
            ("flush_sent", both(lambda j: j.update(flush_sent_to_device=FLUSH_SENT["wb"][False] + " (planted)")),
             "F3:record", ["flush_sent_to_device"]),
            ("floor_claim", both(lambda j: j.update(floor_claim=j["floor_claim"].split("; on a virtual drive")[0])),
             "F3:record", ["floor_claim lacks the qualifier"]),
            ("linkage", both(lambda j: j.update(linkage="not static: other files mapped (fire-check only)")), "F3:record",
             ["linkage/mapped_files"]),
            ("clocksource", both(lambda j: j.update(clocksource="hpet")), "F3:record", ["clocksource", "stamp clocksource"]),
            ("claim", {"merged": lambda j: j.update(floor_claim_from_counts=j["floor_claim_from_counts"].split("; a virtual drive")[0])},
             "F3:record", ["floor_claim_from_counts lacks the qualifier"]),
            ("fuaonly", {"report": lambda j: j["windows"]["arms"]["append25"]["devices"]["nvme0n1"].update(flush_carrying_zero_windows=1)},
             "F3:devflush", ["a gated op's window holds no flush-carrying request to a write-back layer"]),
            # seventh review M1: one planted case per rule, each expecting its own tag
            ("bare-flush 189/200", {"report": lambda j: j["windows"]["arms"]["clean"]["devices"]["nvme0n1"].update(bare_flush_zero_windows=11)},
             "F3:record", ["floor_claim_from_counts"]),
            ("probe rc 3", {"files": {"rc": lambda t: t.replace("probe_rc=0", "probe_rc=3")}}, "F3:complete",
             ["probe rc vs the control recomputed from raw"]),
            ("a flush on the private loop in nosync25", {"report": lambda j: j["windows"]["arms"]["nosync25"]["devices"].update(
                loop0={"events": 1, "zero_windows": 199, "flush_carrying_zero_windows": 199, "bare_flush_zero_windows": 199})},
             "F3:devflush", ["nosync25 windows hold flush requests"]),
            ("gate outcome", {"files": {"gate.json": lambda j: j["flush_gate"].update(outcome="FAIL")},
                              "merged": lambda j: j["flush_gate"].update(outcome="FAIL")}, "F3:gate", ["flush gate"]),
            ("a merged field changed", {"merged": lambda j: j.update(n=201)}, "F3:merge", ["a probe field changed"]),
            ("arms_gated reordered", both(lambda j: j.update(arms_gated=list(reversed(j["arms_gated"])))), "F3:record", ["arms_gated"]),
            ("durability removed", both(lambda j: j.update(durability={})), "F3:record", ["durability"]),
            ("exe_sha256 not the binary's", {"kvmut": lambda k: k.update(v3floor_sha256="00" * 32)}, "F3:record", ["exe_sha256"]),
            ("an ext4 layer without its journal", both(lambda j: j["flush_path"][0]["ext4"].pop("journal")), "F3:record",
             ["ext4 layer lacks data=/commit=/journal_async_commit/journal"]),
            ("no cpufreq in the stamp", {"files": {"stamp_start.json": lambda j: j.pop("cpufreq")}}, "F3:record", ["stamp lacks"]),
            ("stamp problems", {"files": {"stamp_end.json": lambda j: j.update(problems=["planted"])}}, "F3:complete", ["stamp problems"]),
            ("clone1b not report-only", both(lambda j: j.update(arms_report_only={})), "F3:record", ["clone1b is not report-only"]),
            ("timing control says PLP", both(lambda j: j.update(timing_control="not applicable: PLP", flush_control="not applicable: PLP")),
             "F3:complete", ["timing_control disagrees with raw and the leaf (A14, A17)"]),
            ("another class's threshold key", both(lambda j: j.update(d0_threshold_key="d0_threshold/xfs/wb/vm")), "F3:complete",
             ["the d0 threshold is not the registered one for this class (A17)"]),
            ("the floor reference on append25", both(lambda j: j.update(floor_reference=dict(j["floor_reference"], arm="append25"))),
             "F3:record", ["floor_reference"]),
            ("the sync record for another pid", {"report": lambda j: j["syscalls"].update(pid=1)}, "F3:devflush",
             ["no per-window sync record for the probe's pid"]),
            ("a window without the probe's fsync", {"report": lambda j: j["syscalls"]["arms"]["append25"].update(windows_without_a_sync=1)},
             "F3:devflush", ["a flush-gated op's window holds no fsync by the probe"]),
            ("PLP declared yes to the fire-check", {"kvmut": lambda k: k.update(plp="yes")}, "F3:record", ["plp"]),
            # amended at the ninth review (M4): the plant now also trips F3's own untraced rule, beside the D0 rule
            ("traced, the D0 control still judged", both(lambda j: j.update(traced=True)), "F3:complete",
             ["d0_control disagrees with raw", "F3 ran traced or did not record it"]),
            # ninth review M4: F3 runs untraced, and says so
            ("traced not recorded", both(lambda j: j.pop("traced")), "F3:complete", ["F3 ran traced or did not record it"]),
            # ninth review M5: F3:gate derives the gated set, the requirement and the blkflush gate's arms by hand
            ("the gate without fdatasync4k (the old post)", {
                "files": {"gate.json": lambda j: j["flush_gate"].update(
                    gated_arms_run=[a for a in j["flush_gate"]["gated_arms_run"] if a != "fdatasync4k"],
                    required_flushes=j["flush_gate"]["n"] * (len(j["flush_gate"]["gated_arms_run"]) - 1),
                    blkflush_leaf_gate=dict(j["flush_gate"]["blkflush_leaf_gate"], arms={
                        a: v for a, v in j["flush_gate"]["blkflush_leaf_gate"]["arms"].items() if a != "fdatasync4k"}))},
                "merged": lambda j: j["flush_gate"].update(
                    gated_arms_run=[a for a in j["flush_gate"]["gated_arms_run"] if a != "fdatasync4k"],
                    required_flushes=j["flush_gate"]["n"] * (len(j["flush_gate"]["gated_arms_run"]) - 1),
                    blkflush_leaf_gate=dict(j["flush_gate"]["blkflush_leaf_gate"], arms={
                        a: v for a, v in j["flush_gate"]["blkflush_leaf_gate"]["arms"].items() if a != "fdatasync4k"}))},
             "F3:gate", ["gated_arms_run", "required_flushes", "blkflush gate arms"]),
            ("a miscounted requirement", {"files": {"gate.json": lambda j: j["flush_gate"].update(required_flushes=1399)},
                                          "merged": lambda j: j["flush_gate"].update(required_flushes=1399)},
             "F3:gate", ["required_flushes"]),
            # tenth review HIGH 1: the fd record and the per-fd shortfall
            ("cfr2b's sync_fds one fd short", both(lambda j: j["sync_fds"].update(cfr2b={k: v for k, v in list(j["sync_fds"]["cfr2b"].items())[:1]})),
             "F3:devflush", ["sync_fds disagrees with the arm definitions"]),
            ("a cfr2b window short of its directory fsync", {"report": lambda j: j["syscalls"]["arms"]["cfr2b"].update(windows_short=1)},
             "F3:devflush", ["a flush-gated op's window lacks one of its own syncs"]),
            # ninth review L15: fdatasync4k's own windows, for the flush-carrying gate and the sync gate
            ("fdatasync4k: a window without a flush-carrying request",
             {"report": lambda j: j["windows"]["arms"]["fdatasync4k"]["devices"]["nvme0n1"].update(flush_carrying_zero_windows=1)},
             "F3:devflush", ["a gated op's window holds no flush-carrying request to a write-back layer"]),
            ("fdatasync4k: a window without the probe's fdatasync",
             {"report": lambda j: j["syscalls"]["arms"]["fdatasync4k"].update(windows_without_a_sync=1)},
             "F3:devflush", ["a flush-gated op's window holds no fsync by the probe"]),
            # eleventh review MED 1: the pid's syncs lying wholly inside a window, counted on ANY fd: nosync25's must be
            # 0, and none may lie wholly inside a window on an fd that window's arm does not own (the review's plant: an
            # fsync on fd 7 inside a nosync25 window, which the fd rule alone gives to no window)
            ("an fsync on fd 7 wholly inside a nosync25 window",
             {"report": lambda j: (j["syscalls"]["arms"]["nosync25"].update(syncs_inside_any_fd=1),
                                   j["syscalls"].setdefault("unattributed", {}).update(inside_foreign_fd=1))},
             "F3:devflush", ["nosync25's windows hold a sync by the probe on some fd",
                             "a sync by the probe lies wholly inside a window on an fd its arm does not own, or on none"]),
            # eleventh review MED 2: the F1b cross-check refuses without its trace, and compares the probe's sync_fds
            # with the fds strace saw synced INSIDE each arm's timed windows, as sets (setup syncs excluded)
            ("no F1b real-all trace", {"f1b": "absent"}, "F3:devflush",
             ["no F1b real-all trace: sync_fds cannot be cross-checked"]),
            ("an F1b trace whose first cfr2b clone fsync names fd 99", {"f1b": f1b_fd99}, "F3:devflush",
             ["sync_fds disagrees with F1b's strace"]),
            ("an F1b trace whose nosync25 window fsyncs nosync25's own file", {"f1b": f1b_nosync}, "F3:devflush",
             ["sync_fds disagrees with F1b's strace"]),
        ]
        for name, muts, cid, want in cases:
            g = run(name, muts.get("probe"), muts.get("merged"), muts.get("report"), muts.get("files"), muts.get("kvmut"),
                    muts.get("f1b"))
            t = tags(g.get(cid, {}))
            others = [i for i in F3IDS if i != cid and g.get(i, {}).get("pass") is not True]
            chk("real: planted %s -> %s fails with %s only, the other F3 checks pass" % (name, cid, want),
                g.get(cid, {}).get("pass") is False and sorted(set(t)) == sorted(want) and not others, (t, others))
        # eleventh review MED 2: a sync outside every timed window (setup) on another fd is not the arm's (the old
        # whole-trace rule counted setup syncs); all five F3 checks pass
        g = run("f1b-setup-fsync", f1b=f1b_setup)
        chk("real (MED 2): an F1b fsync of append25's file on fd 42 before the first timed window is not counted -> all "
            "five F3 checks pass", [i for i in F3IDS if g.get(i, {}).get("pass") is not True] == [],
            {i: tags(g.get(i, {})) for i in F3IDS})
        m = both(lambda j: j["virtualization"].update(virtualized=False, evidence=[]))
        g = run("virtualized", m["probe"], m["merged"])
        want_v = sorted(["virtualization", "floor_kind", "flush_sent_to_device", "floor_claim qualified on bare metal",
                         "floor_claim_from_counts qualified on bare metal"])
        chk("real: planted virtualized false on a VM (detect-virt microsoft) -> F3:record fails with exactly the "
            "virtualization rule and the four labels that follow from it", g["F3:record"]["pass"] is False
            and sorted(set(tags(g["F3:record"]))) == want_v, sorted(set(tags(g["F3:record"]))))
        # the write-through rules through check_real, on a real write-through batch (seventh review M1)
        src_wt = os.path.join(HERE, "testdata", BANKED_F3_WT)
        kv_wt = dict(l.split("=", 1) for l in (rd(os.path.join(src_wt, "info.txt")) or "").splitlines()
                     if "=" in l and not l.startswith(("loop ", "block ")))

        def run_wt(name, probe=None, merged=None, report=None):
            global CELL, KIND, W, OUT, results
            d = os.path.join(td, "wt-" + name)
            shutil.copytree(os.path.join(src_wt, "F3"), d)
            for f, mut in (("summary.probe.json", probe), ("summary.json", merged), (os.path.join("blkflush", "report.json"), report)):
                if mut:
                    j = rj(os.path.join(d, f))
                    mut(j)
                    with open(os.path.join(d, f), "w") as fh:
                        json.dump(j, fh)
            # eleventh review MED 2: this cell's own F1b strace, under an OUT of its own
            wto = os.path.join(td, "wtout")
            put_f1b(wto, src_wt, None)
            CELL, KIND, W, OUT, results = "ext4loop", "ext4", kv_wt.get("work", ""), wto, []
            with contextlib.redirect_stdout(io.StringIO()):
                check_real(d, 0, kv_wt, "wt")
            return {r["id"]: r for r in results}

        g = run_wt("control")
        chk("real: the banked write-through batch passes all five F3 checks unplanted",
            [i for i in F3IDS if g.get(i, {}).get("pass") is not True] == [], {i: tags(g.get(i, {})) for i in F3IDS})
        wtl = lambda j: j["flush_path"][-1]["disk"]  # noqa: E731
        for name, muts, cid, want in (
                ("a flush request on the write-through leaf",
                 {"report": lambda j: j["devices"].update({"sda": {"total": 3, "by_kind": {"flush": 3}, "comms": {}}})},
                 "F3:devflush", ["flush requests issued to a write-through (or brd) device"]),
                ("the write-back label on a write-through leaf",
                 both(lambda j: j.update(floor_kind=VIRT_KIND["wb"][True])), "F3:record", ["floor_kind"]),
                ("flush_sent without the host-caching qualifier",
                 both(lambda j: j.update(flush_sent_to_device=FLUSH_SENT["wt"][True] + " and its device report agrees")),
                 "F3:record", ["flush_sent_to_device"]),
                ("timing control applied to a write-through leaf", both(lambda j: j.update(timing_control="pass", flush_control="pass")),
                 "F3:complete", ["timing_control disagrees with raw and the leaf (A14, A17)"])):
            g = run_wt(name.replace(" ", "_")[:24], muts.get("probe"), muts.get("merged"), muts.get("report"))
            t = tags(g.get(cid, {}))
            others = [i for i in F3IDS if i != cid and g.get(i, {}).get("pass") is not True]
            chk("real (write-through): planted %s -> %s fails with %s only, the other F3 checks pass" % (name, cid, want),
                g.get(cid, {}).get("pass") is False and sorted(set(t)) == sorted(want) and not others, (t, others))
        # cell:leaf through its own function
        f3 = rj(os.path.join(src, "F3", "summary.probe.json"))
        sdl = (rj(os.path.join(src_wt, "F3", "summary.probe.json")) or {}).get("leaf")  # the real Hyper-V sd leaf
        for name, lf, ok in (("the banked NVMe leaf", f3["leaf"], True), ("the banked Hyper-V sd leaf", sdl, True),
                             ("NVMe over tcp", dict(f3["leaf"], nvme_transport=["tcp"]), False),
                             ("a scsi_debug kind", dict(sdl, kind="scsi_debug", creditable=False), False),
                             ("sd on megaraid_sas", dict(sdl, sd_host="megaraid_sas"), False),
                             ("sd bytes saying WCE=1 under a write-through report",
                              dict(sdl, drive_report_source=sdl["drive_report_source"].replace("08 0a 00", "08 0a 04")), False)):
            CELL, KIND, results = "ext4loop", "ext4", []
            with contextlib.redirect_stdout(io.StringIO()):
                cell_leaf_check(lf, "wb" if lf.get("write_cache") == "write back" else "wt", "planted", {"brd_cell": "0"})
            chk("real: cell:leaf on %s -> %s" % (name, "pass" if ok else "fail"), results[0]["pass"] is ok, results[0]["detail"])
        for name, lf, lc, cellname, brdc, ok in (("a drive leaf on a brd cell", f3["leaf"], "wb", "xfs", "1", False),
                                                 ("a brd leaf outside a brd cell", {"kind": "brd", "creditable": False}, "brd", "xfs", "0", False),
                                                 ("a brd leaf on a loop cell", {"kind": "brd", "creditable": False}, "brd", "xfsloop", "1", False),
                                                 ("a brd leaf marked creditable", {"kind": "brd", "creditable": True}, "brd", "xfs", "1", False),
                                                 ("a brd leaf on a brd cell", {"kind": "brd", "creditable": False}, "brd", "xfs", "1", True)):
            CELL, KIND, results = cellname, "xfs", []
            with contextlib.redirect_stdout(io.StringIO()):
                cell_leaf_check(lf, lc, "planted", {"brd_cell": brdc})
            chk("real: cell:leaf with %s -> %s" % (name, "pass" if ok else "fail"), results[0]["pass"] is ok, results[0]["detail"])
        # cell:box through its own function, on the banked batch's box
        kvb = dict(kv)
        boxb = box_of(kvb)
        os.makedirs(os.path.join(td, "F4"), exist_ok=True)
        for name, bx, k2, lc, ok in (("the banked box", boxb, kvb, "wb", True),
                                     ("no detect-virt answer", dict(boxb, virt="unknown"), kvb, "wb", False),
                                     ("no plp", dict(boxb, plp="unknown"), kvb, "wb", False),
                                     ("flip no on a write-back leaf (its .na present)", dict(boxb, flip="no"),
                                      dict(kvb, box="virt=vm,flip=no,plp=no"), "wb", False),
                                     ("flip no where it is right but no .na recorded", dict(boxb, flip="no"),
                                      dict(kvb, box="virt=vm,flip=no,plp=no"), "wt-nvme", False),
                                     ("firecheck.sh's box line differing", boxb, dict(kvb, box="virt=bare,flip=yes,plp=no"), "wb", False),
                                     ("another leaf disk", dict(boxb, leafdisk="sdz"), kvb, "wb", False)):
            CELL, KIND, results = "ext4loop", "ext4", []
            na = os.path.join(td, "F4", "R_leaf_flip.na")
            if lc == "wb" and bx.get("flip") == "no":
                open(na, "w").write("planted\n")  # so that only the flip rule can fire
            elif os.path.exists(na):
                os.remove(na)
            with contextlib.redirect_stdout(io.StringIO()):
                cell_box_check(bx, "wt" if lc == "wt-nvme" else lc, f3, k2, td)
            chk("real: cell:box with %s -> %s" % (name, "pass" if ok else "fail"), results[0]["pass"] is ok, results[0]["detail"])
    finally:
        CELL, KIND, W, OUT, results = saved
        REGISTRY_FILE = "REGISTERED.tsv"
        shutil.rmtree(td)


def main(argv):
    global OUT, CELL, KIND, W
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) == 7 and argv[0] == "--plan":
        if argv[1] not in v3cell.CELLS or argv[4] not in ("vm", "bare") or argv[5] not in ("yes", "no") or \
                argv[6] not in ("yes", "no"):
            return 2
        print("\n".join(plan(argv[1], argv[2], argv[3], {"virt": argv[4], "flip": argv[5], "plp": argv[6]})))
        return 0
    if len(argv) == 2 and argv[0] == "--box":
        info = rd(os.path.join(argv[1], "info.txt")) or ""
        print(json.dumps(box_of(dict(l.split("=", 1) for l in info.splitlines() if "=" in l
                                     and not l.startswith(("loop ", "block "))))))
        return 0
    if len(argv) == 3 and argv[0] == "--bind":
        return bind(argv[1], argv[2])
    if len(argv) != 2 or argv[1] not in v3cell.CELLS:
        print(__doc__, file=sys.stderr)
        return 2
    OUT, CELL = argv
    KIND = kind_of(CELL)
    info = rd(os.path.join(OUT, "info.txt")) or ""
    kv = dict(l.split("=", 1) for l in info.splitlines() if "=" in l and not l.startswith(("loop ", "block ")))
    W = kv.get("work", "")
    arch = kv.get("arch", "")
    leaf, leaf_src = find_leaf_class()
    box = box_of(kv)
    the_plan = plan(CELL, arch, leaf or "unknown", box)

    # cell: the explicit layout (review 2 item 4)
    check("cell:fstype", kv.get("work_fstype") == KIND, {"work_fstype": kv.get("work_fstype"), "want": KIND},
          "the work dir is on the cell's filesystem type (findmnt)")
    src = kv.get("work_mount", "").split(" ")[0]
    check("cell:source", src.startswith("/dev/") and src.startswith("/dev/loop") == v3cell.is_loop(CELL), {"source": src, "cell": CELL},
          "cell %s means %s" % (CELL, "a loop device" if v3cell.is_loop(CELL) else "a block device, no loop"))
    f3 = rj(os.path.join(OUT, "F3", "summary.probe.json")) or rj(os.path.join(OUT, "F3", "summary.json")) or {}
    lp = v3cell.layout_problems(CELL, f3.get("fstype"), f3.get("mount_source"), f3.get("flush_path"))
    check("cell:layers", not lp, {"problems": lp}, "the F3 batch's flush path has the cell's layer count and source")
    pers = kv.get("personality_under_setarch_R", "")
    check("cell:aslr", re.fullmatch(r"[0-9a-fA-F]+", pers or "x") is not None and int(pers, 16) & 0x0040000 != 0,
          {"personality": pers}, "setarch -R turns ASLR off for the traced runs (ADDR_NO_RANDOMIZE 0x0040000)")
    cell_leaf_check(f3.get("leaf") or {}, leaf, leaf_src, kv)
    cell_box_check(box, leaf, f3, kv, OUT)

    # F1: per-op syscall counts under strace -f -c at n = 1, 2, 3, 40
    f1 = {}
    for s in F1_SETS:
        counts, probs = counted_set("F1", s, False)
        arms = s.split(",")
        if all_flushed_refused(arms, KIND):
            check("F1:%s:refused" % tagof(s), not probs, {"bad": probs[:4]},
                  "[%s] on ext4: refused at every n (rc 2, the trial FICLONE's reason, no out dir)" % s)
            continue
        bad = probs or count_mismatches(counts, arms, False)
        f1[s] = counts
        check("F1:%s" % tagof(s), not bad, {"bad": bad[:10]},
              "strace -f -c: per-op syscalls of [%s] equal the definition %s" % (s, json.dumps(dict(per_round(arms, False)), sort_keys=True)))
    c = f1.get("nosync25", {})
    if 1 in c and 40 in c:
        d = c[40].get(INSTRUMENT, (0, 0))[0] - c[1].get(INSTRUMENT, (0, 0))[0]
        clock_note = "vDSO (0 clock_gettime syscalls per op)" if d == 0 else "%d clock_gettime syscalls per 39 ops" % d
    else:
        clock_note = "unknown (nosync25 counts missing)"

    # F1b / F2d: the exact sequence inside every timed window, and the sequence checker's own fire-check
    traces = {}
    for tag, (s, n, mutant) in SEQ.items():
        base = os.path.join(OUT, "F1b", tag)
        rc = rc_of(base + ".rc")
        text = rd(base + ".trace.gz")
        sj = rj(os.path.join(base + ".out", "summary.json"))
        arms = s.split(",")
        cid = ("F2d:" if mutant else "F1b:") + tag
        if text is None or sj is None or rc not in (0, 3):
            check(cid, False, {"rc": rc, "trace": text is not None, "summary": sj is not None}, "the traced run completed")
            continue
        calls, other, pids = parse_trace(text)
        traces[tag] = (calls, other, pids, text, rc)
        bad = sequence_problems(calls, arms, n, mutant, other, pids, rc)
        if int(sj.get("trace_clock", 0)) != 1 or int(sj.get("mutant_nosync", -1)) != int(mutant):
            bad.append(("flags", sj.get("trace_clock"), sj.get("mutant_nosync")))
        if sj.get("traced") is not True:  # ninth review M4
            bad.append(("traced", sj.get("traced")))
        check(cid, not bad, {"bad": bad[:8]},
              "strace -f -y --trace-clock [%s] n=%d: every timed window holds exactly its arm's syscalls on its own files, "
              "sizes and offsets; nothing between ops; each arm once per round (%d distinct round orders); the only other "
              "line is the one exit" % (tag, n, round_orders(calls, arms, n) if not bad else -1))
    if "real-all" in traces:
        calls, other, pids, text, rc = traces["real-all"]
        arms, n = ALL.split(","), SEQ["real-all"][1]
        clean_ok = sequence_problems(calls, arms, n, False, other, pids, rc) == []
        pl = plants(calls, text)
        want = plant_names(KIND)
        check("F1b:selftest:count", [p for p, _, _ in pl] == want and clean_ok,
              {"planted": [p for p, _, _ in pl], "want": want, "unplanted_trace_passes": clean_ok},
              "%d planted breaches built from the real trace (want %d)" % (len(pl), len(want)))
        got = {name: (c, o) for name, c, o in pl}
        for name in want:
            if name not in got:
                check("F1b:plant:" + name, False, "not planted", "the sequence check rejects '%s'" % name)
                continue
            c, o = got[name]
            oth, pp = o if o else (other, pids)
            check("F1b:plant:" + name, sequence_problems(c, arms, n, False, oth, pp, rc) != [], {},
                  "the sequence check rejects '%s'" % name)
    else:
        check("F1b:selftest:count", False, "no real-all trace", "planted breaches built from the real trace")
        for name in plant_names(KIND):
            check("F1b:plant:" + name, False, "no real-all trace", "the sequence check rejects '%s'" % name)
    if "mutant-all" in traces:
        calls, other, pids, _, rc = traces["mutant-all"]
        check("F2d:fires", sequence_problems(calls, ALL.split(","), SEQ["mutant-all"][1], False, other, pids, rc) != [], {},
              "the real spec rejects the mutant's trace (the sequence check fires on real mutant data)")
    else:
        check("F2d:fires", False, "no mutant trace", "the real spec rejects the mutant's trace")

    # F2: the mutant -- strace sees no flush in the flushed arms; clean keeps its fsync
    f2 = {}
    for s in F2_SETS:
        counts, probs = counted_set("F2", s, True)
        arms = s.split(",")
        if all_flushed_refused(arms, KIND):
            check("F2:%s:refused" % tagof(s), not probs, {"bad": probs[:4]}, "[%s] on ext4: refused at every n (rc 2)" % s)
            continue
        bad = probs or count_mismatches(counts, arms, True)
        f2[s] = counts
        check("F2:%s" % tagof(s), not bad, {"bad": bad[:10]},
              "--mutant-nosync, strace -f -c: per-op syscalls of [%s] equal %s" % (s, json.dumps(dict(per_round(arms, True)), sort_keys=True)))
    for a in FLUSHED:
        s = a + ",nosync25"
        if all_flushed_refused(s.split(","), KIND):
            continue
        ok = s in f1 and s in f2 and len(f1[s]) == len(NS) and len(f2[s]) == len(NS)
        fires = ok and count_mismatches(f2[s], s.split(","), False) != [] and count_mismatches(f1[s], s.split(","), True) != []
        check("F2c:" + a, fires, {"have_counts": ok}, "the count check fires: the real %s spec rejects the mutant's counts and vice versa" % a)

    # F2b: the mutant, unwatched, n=200, every arm: the flush control voids it (rc 3)
    f2b = rc_of(os.path.join(OUT, "F2b.rc"))
    s2 = rj(os.path.join(OUT, "F2b.out", "summary.json")) or {}
    r2 = raw_rows(os.path.join(OUT, "F2b.out", "raw.tsv"))
    mut_ratios, bad2 = {}, ["missing raw"]
    if r2 and "nosync25" in r2:
        bad2, ratios2, _, _ = summary_vs_raw(s2, r2, 200)
        mut_ratios = {a: round(r, 2) for a, r in ratios2.items()}
    lc2 = leaf_class_of(s2)
    want2 = "FAIL" if (lc2 == "wb" and box["plp"] == "no") else \
        {"wt": "not applicable: no volatile cache", "brd": "not applicable: brd", "wb": "not applicable: PLP"}.get(lc2)
    thr2 = s2.get("d0_threshold") if isinstance(s2.get("d0_threshold"), (int, float)) else 10.0
    f2bv = summary_vs_raw(s2, r2, 200)[2] if r2 and "nosync25" in r2 else ["missing raw"]
    check("F2b", want2 is not None and str(s2.get("timing_control", "")).startswith(want2) and s2.get("plp") == box["plp"]
          and f2b == (3 if f2bv else 0) and (want2 != "FAIL" or f2b == 3) and mut_ratios.get("append25") is not None
          and mut_ratios["append25"] <= thr2 and not bad2 and s2.get("traced") is False,
          {"rc": f2b, "timing_control": s2.get("timing_control"), "want": want2, "plp": s2.get("plp"),
           "ratios_from_raw": mut_ratios, "bad": bad2[:6], "traced": s2.get("traced")},
          "--mutant-nosync n=200: append25/nosync25 <= the threshold from raw, summary == raw; on a write-back leaf "
          "without PLP the timing control voids it (rc 3), on a write-through, brd or declared-PLP leaf it records "
          "'not applicable' (A14); untraced, and the probe says so (ninth review M4)")
    # T: the untraced twin of F3 (stage T) must say it ran untraced (ninth review M4)
    tsj = rj(os.path.join(OUT, "T.out", "summary.json")) or {}
    trc = rc_of(os.path.join(OUT, "T.rc"))
    check("T:untraced", trc in (0, 3) and tsj.get("traced") is False, {"rc": trc, "traced": tsj.get("traced")},
          "stage T ran the probe alone (rc 0 or 3) and its summary records traced: false")

    # F3: the real run, n=200, through run.sh (stamps, blkflush, the batch gate)
    f3rec = check_real(os.path.join(OUT, "F3"), rc_of(os.path.join(OUT, "F3.rc")), kv, leaf)
    if leaf == "wb" and box["plp"] == "no":
        check("F2b:discriminates", f3rec.get("probe_rc") == 0, {"F3_probe_rc": f3rec.get("probe_rc"), "F3_control": f3rec.get("flush_control")},
              "the same arms and seed without the mutant (F3) do not fail the control (probe rc 0)")
    # the D0 threshold's separation on this cell (review 2 item 17: re-derived from the first T3 fire-check)
    real_r = f3rec.get("ratios_vs_nosync25_from_raw") or {}
    gm = [mut_ratios[a] for a in GATED if a in mut_ratios]
    gr = [real_r[a] for a in GATED if a in real_r]
    d0 = {"rule": "A17: the timing control gates append25 only; its threshold t separates when the mutant's append25 "
                  "ratio (F2b) < t < the real one (F3), and this cell's candidate is their geometric mean (per_arm: "
                  "append25). The registered threshold for a class (REGISTERED.tsv) is taken from such candidates "
                  "before any rental; the other arms are descriptive and left to the flush gate",
          "registered_key": (rj(os.path.join(OUT, "F3", "summary.probe.json")) or {}).get("d0_threshold_key"),
          "max_mutant_gated": max(gm) if gm else None, "min_real_gated": min(gr) if gr else None, "per_arm": {},
          "pooled_over_flush_gated_arms_descriptive": True}
    for a in GATED:
        if a in mut_ratios and a in real_r:
            mu, re_ = mut_ratios[a], real_r[a]
            d0["per_arm"][a] = {"mutant": mu, "real": re_, "separates": mu < re_,
                                "candidate": round((mu * re_) ** 0.5, 2) if mu < re_ and mu > 0 else None,
                                "threshold_10_separates": mu < 10 < re_}
    # the registered candidate is append25's alone (A17; eighth review M3): the pooled max/min above are descriptive
    a25 = d0["per_arm"].get("append25")
    if a25:
        d0["separates"] = a25["separates"]
        d0["candidate"] = a25["candidate"]
        d0["threshold_10_separates"] = a25["threshold_10_separates"]
        d0["candidate_arm"] = "append25"

    # frame arm (item 11; gate-6 review MED 5): the registered one, else none with the rule's candidate; append64 (an
    # append of >= the ~60 B flight and < 4 KiB) runs in F3, descriptive until registered
    fbad = []
    regf = registered().get("frame_arm")
    if not (M1_FLIGHT_MAX_B <= APPEND[FRAME_ARM] < 4096):
        fbad.append(("bytes", APPEND[FRAME_ARM], M1_FLIGHT_MAX_B))
    if f3.get("frame_arm") != (regf[0] if regf else None) or f3.get("frame_arm_ref") != (regf[1] if regf else None):
        fbad.append(("frame_arm vs REGISTERED.tsv", f3.get("frame_arm"), f3.get("frame_arm_ref"), regf))
    if "ow4k" not in str(f3.get("frame_candidate", "")) or "PREREG section 4" not in str(f3.get("frame_rule", "")):
        fbad.append(("the rule and its candidate", f3.get("frame_rule"), f3.get("frame_candidate")))
    if FRAME_ARM not in (f3rec.get("p50_us_from_raw") or {}):
        fbad.append("append64 did not run in F3")
    check("frame:append", not fbad, {"bad": fbad},
          "the frame arm is the registered one (REGISTERED.tsv) or none, the rule and its candidate (ow4k) recorded; "
          "append64 (>= %d B, < 4 KiB) ran in F3, descriptive until registered" % M1_FLIGHT_MAX_B)

    # C: crash arms (item 3) on the loop cells
    crash = {}
    if v3cell.is_loop(CELL):
        for case, rule in CRASH[KIND]:
            r = rj(os.path.join(OUT, "crash", case + ".json"))
            res = (r or {}).get("result")
            crash[case] = {"rule": rule, "result": res, "predicted": CRASH_PREDICT.get("%s/%s" % (KIND, case), rule),
                           "detail": r}
            # the clone existed (1 MiB) before the crash, and the crash itself is shown by a sentinel written after the
            # crash point that is absent after the remount (fresh reviews: I-L1, B-L10)
            ok = r is not None and r.get("src_ok") is True and r.get("clone_size_before") == "1048576" and \
                r.get("crash_proven") is True and (
                res == "survived" if rule == "survive" else res == "lost" if rule == "lost" else res in ("survived", "lost"))
            check("C:" + case, ok, {"record": r},
                  "crash after one %s op (%s): %s" % (case, "xfs_io shutdown, no log flush" if KIND != "btrfs" else
                                                      "dm-flakey drop_writes", {"survive": "the control survives (size and "
                                                      "bytes equal the source)", "lost": "the no-flush mutant is lost (the rig "
                                                      "crashes)", "record": "recorded, predicted %s" % crash[case]["predicted"]}[rule]))

    # B: blkflush.py's own fire-check
    bt = rd(os.path.join(OUT, "B", "selftest.txt")) or ""
    m = re.search(r"BLKFLUSH SELF-TEST (\d+)/(\d+) PASS\s*$", bt)
    check("B:selftest", rc_of(os.path.join(OUT, "B", "selftest.rc")) == 0 and m is not None and m.group(1) == m.group(2)
          and int(m.group(2)) > 0, {"tail": bt[-300:], "rc": rc_of(os.path.join(OUT, "B", "selftest.rc"))},
          "blkflush.py self-test: the parser and the window attribution on planted text (rc 0, n/n with n > 0)")
    br = rj(os.path.join(OUT, "B", "report.json")) or {}
    lp_dev = (rd(os.path.join(OUT, "B", "loopdev.txt")) or "").strip().replace("/dev/", "")
    arms = (br.get("windows") or {}).get("arms") or {}
    fs = arms.get("devfsync", {})
    d = (fs.get("devices") or {}).get(lp_dev, {})
    check("B:fsync", fs.get("ops") == 50 and d.get("events") == 50 and d.get("zero_windows") == 0 and d.get("max_in_window") == 1
          and (d.get("by_kind") or {}).get("flush") == 50,
          {"loop": lp_dev, "devfsync": fs, "refused": br.get("refused")},
          "50 fsync(2)s of a raw write-back loop device: exactly one flush request issued to it in each window")
    rootd = (kv.get("root_disk") or "").split(" ")[0]
    qbad, qrec = [], {}
    for a in ("devwrite", "idle"):
        r = arms.get(a, {})
        if r.get("ops") != 50:
            qbad.append((a, "ops", r.get("ops")))
        for dev, x in (r.get("devices") or {}).items():
            hit = r.get("ops", 50) - x.get("zero_windows", 50)
            qrec["%s/%s" % (a, dev)] = hit
            if dev == lp_dev or (dev == rootd and hit > SHARED_MAX_FRAC * r.get("ops", 50)):
                qbad.append((a, dev, hit))
    ambd = (br.get("windows") or {}).get("ambiguous_by_device") or {}
    lptotal = ((br.get("devices") or {}).get(lp_dev) or {}).get("total")
    check("B:quiet", not qbad and not ambd.get(lp_dev) and lp_dev != "" and lptotal == 50,
          {"bad": qbad, "windows_hit": qrec, "ambiguous_by_device": ambd, "root_disk": rootd, "loop_total": lptotal},
          "50 buffered writes and 50 empty windows: no flush request on the loop, at most %d%% of windows with a foreign "
          "one on the shared root drive (others' devices recorded); no event at a window edge on the loop"
          % int(SHARED_MAX_FRAC * 100))
    ov = rj(os.path.join(OUT, "B", "overflow.json")) or {}
    check("B:overflow", rc_of(os.path.join(OUT, "B", "overflow.rc")) == 2 and "lost" in str(ov.get("refused", "")),
          {"rc": rc_of(os.path.join(OUT, "B", "overflow.rc")), "report": ov},
          "a 4 KiB ring buffer overrun by 3000 flushes: report refuses (events lost), never a short count")
    mbad = [(t, rc_of(os.path.join(OUT, "B", t + ".rc"))) for t in ("stop_unstarted", "start_twice")
            if rc_of(os.path.join(OUT, "B", t + ".rc")) != 2]
    check("B:misuse", not mbad, {"bad": mbad}, "stop without start and start into an existing record both refuse (rc 2)")
    st = rd(os.path.join(OUT, "B", "batchgate-selftest.txt")) or ""
    m = re.search(r"BATCHGATE SELF-TEST (\d+)/(\d+) PASS\s*$", st)
    check("S:batchgate", rc_of(os.path.join(OUT, "B", "batchgate-selftest.rc")) == 0 and m is not None
          and m.group(1) == m.group(2) and int(m.group(2)) > 0, {"tail": st[-400:]},
          "batchgate.py self-test in this cell (the flush gate on the banked cells, every verdict-shape refusal by its "
          "own reason, the T3 rule): the binding's rules are tested in the verdict that relies on them")
    st = rd(os.path.join(OUT, "B", "check-selftest.txt")) or ""
    m = re.search(r"CHECK SELF-TEST (\d+)/(\d+) PASS\s*$", st)
    check("S:check", rc_of(os.path.join(OUT, "B", "check-selftest.rc")) == 0 and m is not None
          and m.group(1) == m.group(2) and int(m.group(2)) > 0, {"tail": st[-400:]},
          "check.py --self-test in this cell: the checker's own leaf, virtualization, linkage, qualifier, MODE SENSE "
          "byte and harness rules each fire on planted records")

    # F4: refusals
    for tag, want in REFUSALS.items():
        refusal(tag, want)
    rc = rc_of(os.path.join(OUT, "F4", "P_nest3.rc"))
    nj = rj(os.path.join(OUT, "F4", "P_nest3.out", "summary.json")) or {}
    check("F4:P_nest3", rc in (0, 3) and nj.get("layers") == 4 and nj.get("loop_layers") == 3,
          {"rc": rc, "layers": nj.get("layers"), "text": (rd(os.path.join(OUT, "F4", "P_nest3.txt")) or "")[-300:]},
          "3 nested loops (4 layers) are followed to the leaf and accepted")
    # P_nest_modes after P_nest3: the plan's order (run 37845193906 failed "plan" with it before)
    nm = os.path.join(OUT, "F4", "P_nest_modes")
    pj = rj(os.path.join(nm, "probe.out", "summary.json")) or {}
    at = rd(os.path.join(nm, "after_teardown.txt"))
    ar = rd(os.path.join(nm, "after_rebuild.txt"))
    rb1 = (rd(os.path.join(nm, "rebuilt_n1_backing.txt")) or "").strip()
    wb1 = (rd(os.path.join(nm, "want_n1_backing.txt")) or "MISSING").strip()
    ast = rd(os.path.join(nm, "after_stray.txt"))
    # V3 review 12 item 1's plant: an unrelated loop under $FX/nb/, mounted, survives --teardown-nest untouched (the
    # old prefix matcher took nbx's /mnt/v3fx/nb/x.img for the chain and failed every teardown)
    pb = (rd(os.path.join(nm, "plant_before.txt")) or "").strip()
    pa = (rd(os.path.join(nm, "plant_after.txt")) or "").strip()
    plant_ok = bool(pb) and pa == pb and (rd(os.path.join(nm, "plant_mounted.txt")) or "").strip() == "yes"
    check("F4:P_nest_modes", rc_of(os.path.join(nm, "teardown.rc")) == 0 and at is not None and at.strip() == ""
          and plant_ok
          and rc_of(os.path.join(nm, "rebuild.rc")) == 0 and ar is not None
          and sorted(ar.split("\n")[:-1]) == ["n%d mounted ok" % k for k in (1, 2, 3, 4)]
          and rb1 == wb1  # tenth review MED 1: the rebuilt n1's image is the nest dir's
          and rc_of(os.path.join(nm, "probe.rc")) in (0, 3) and pj.get("layers") == 4 and pj.get("loop_layers") == 3
          # tenth review MED 2: a stray loop on n1, never mounted, is detached by the teardown, which leaves no loop on
          # the chain and nothing mounted
          and bool((rd(os.path.join(nm, "stray.dev")) or "").strip()) and rc_of(os.path.join(nm, "stray_teardown.rc")) == 0
          and ast is not None and ast.strip() == "",
          {"teardown_rc": rc_of(os.path.join(nm, "teardown.rc")), "after_teardown": at, "rebuild_rc": rc_of(os.path.join(nm, "rebuild.rc")),
           "rebuilt_n1_backing": rb1, "want_n1_backing": wb1, "after_stray": ast,
           "plant_before": pb, "plant_after": pa, "plant_mounted": (rd(os.path.join(nm, "plant_mounted.txt")) or "").strip(),
           "stray_teardown_rc": rc_of(os.path.join(nm, "stray_teardown.rc")),
           "after_rebuild": ar, "probe_rc": rc_of(os.path.join(nm, "probe.rc")), "layers": pj.get("layers"),
           "na": rd(os.path.join(nm, "na.txt")), "teardown": (rd(os.path.join(nm, "teardown.txt")) or "")[-300:]},
          "mkfixtures.sh --teardown-nest leaves nothing of n1..n4 (no mount, no .ok, no n1 image); V3_FIXTURES=nest "
          "rebuilds all four on the same directory; the probe accepts the rebuilt n3 (4 layers)")
    if KIND == "ext4":
        rc = rc_of(os.path.join(OUT, "F4", "R_ficlone_accept.rc"))
        txt = rd(os.path.join(OUT, "F4", "R_ficlone_accept.txt")) or ""
        made = os.path.exists(os.path.join(OUT, "F4", "R_ficlone_accept.out"))
        left = rd(os.path.join(OUT, "F4", "R_ficlone_accept.left"))
        left = "MISSING" if left is None else left.strip()  # an empty file is the pass case
        check("F4:R_ficlone_accept", rc == 1 and "ACCEPTED a trial FICLONE" in txt and not made and left == "",
              {"rc": rc, "text": txt[-300:], "out_made": made, "work_left": left},
              "strace inject makes ext4's trial FICLONE succeed: rc 1 'ACCEPTED a trial FICLONE', no out dir, empty work dir")
    else:
        rc = rc_of(os.path.join(OUT, "F4", "X_allclones.rc"))
        rows = raw_rows(os.path.join(OUT, "F4", "X_allclones.out", "raw.tsv")) or {}
        check("F4:X_allclones", rc in (0, 3) and sorted(rows) == ["cfr2b", "clone1b", "clone2b", "nosync25"],
              {"rc": rc, "arms": sorted(rows)}, "on %s the copy arms with nosync25 run (rc 0/3, all in raw)" % KIND)
    refusal("R_leftover", "every flushed arm" if KIND == "ext4" else "left over")
    if box["flip"] == "yes":  # wb: the kernel's write_cache disabled; sd: sd's "temporary write back" (fresh review H2)
        refusal("R_leaf_flip", "but the drive reports", state=True)
    if arch == "x86_64":
        refusal("R_clocksource", "the clocksource is", state=True)
    for tag, want in RUNSH.items():
        refusal(tag, want, runsh=True)
    for tag, wants in RUNSH_POST.items():
        rc = rc_of(os.path.join(OUT, "F4", tag + ".rc"))
        txt = rd(os.path.join(OUT, "F4", tag + ".txt")) or ""
        check("F4:" + tag, rc == 2 and all(w in txt for w in wants), {"rc": rc, "text": txt[-400:]},
              "run.sh refuses after the run (rc 2), for its own reason: %s" % wants)
    for tag, want in POST_PLANTS.items():
        if tag in POST_COND and not POST_COND[tag](CELL, leaf):
            continue
        rc = rc_of(os.path.join(OUT, "F4", tag + ".rc"))
        g = rj(os.path.join(OUT, "F4", tag, "gate.json"))
        if want is None:  # the control: the copy itself trips no rule (a brd batch: only the brd rule, bound mode)
            refs = (g or {}).get("refusals")
            ok = isinstance(g, dict) and (rc in (0, 3) and refs == [] if leaf != "brd" else
                                          rc == 2 and bool(refs) and all(str(r).startswith("leaf brd:") for r in refs))
            check("F4:" + tag, ok, {"rc": rc, "refusals": refs},
                  "batchgate.py post on an unplanted copy of the F3 batch refuses nothing but what the batch itself is "
                  "(the brd rule on a brd cell): the control for the plants")
            continue
        g = g or {}
        if want.startswith("VOID "):  # on a brd cell the copy is also refused by the brd rule (bound mode), and only by it
            refs = g.get("refusals") or []
            ok = any(str(x).startswith(want[5:]) for x in g.get("voids") or []) and (
                rc == 3 and refs == [] if leaf != "brd" else rc == 2 and bool(refs) and all(str(r).startswith("leaf brd:") for r in refs))
        else:
            ok = rc == 2 and any(str(r).startswith(want) for r in g.get("refusals") or [])
        check("F4:" + tag, ok, {"rc": rc, "refusals": g.get("refusals"), "voids": g.get("voids")},
              "batchgate.py post on a copy of the F3 batch with one planted field %s '%s'" %
              (("voids it (rc 3) with", want[5:]) if want.startswith("VOID ") else ("refuses (rc 2) with", want)))
    fourth_review_plants(arch, box)
    left = rd(os.path.join(OUT, "work-leftover.txt"))
    lleft = rd(os.path.join(OUT, "leafw-leftover.txt"))
    check("work:empty", left is not None and left.strip() == "" and lleft is not None and lleft.strip() == "",
          {"leftover": (left or "MISSING")[:400], "leaf_plant_dir_leftover": (lleft or "MISSING")[:400]},
          "the work dir and the leaf plants' dir are empty after every run (teardown and the ext4 FICLONE trial clean up)")
    # the harness this verdict vouches for is the one that ran: hashed at the start (info.txt) and now (L9)
    moved = harness_moved(rj(os.path.join(OUT, "harness_start.json")), harness_sha256())
    check("harness:stable", not moved, {"changed_or_missing": moved[:8]},
          "every fire-check harness file hashes the same at the start of the run and at check time, and none is missing")

    # the plan: exactly these ids, in this order (run.sh binds only a verdict whose ids equal its plan)
    ids = [r["id"] for r in results]
    if ids != the_plan:
        missing = [i for i in the_plan if i not in ids]
        extra = [i for i in ids if i not in the_plan]
        for i in missing:
            check(i, False, "never evaluated", "in the plan, not evaluated")
        results.append({"id": "plan", "check": "the checks evaluated equal the plan", "pass": False,
                        "detail": {"missing": missing, "extra": extra}})
        print("FAIL plan: missing %s extra %s" % (missing[:5], extra[:5]))
    npass = sum(r["pass"] for r in results)
    v = {"cell": CELL, "fstype": KIND, "arch": arch, "leaf_class": leaf, "v3floor_sha256": kv.get("v3floor_sha256"),
         "run_id": kv.get("run_id"), "utc": kv.get("utc"), "pass": npass, "total": len(results),
         "all_pass": npass == len(results) and len(results) > 0 and ids == the_plan, "clock": clock_note,
         "F2b_mutant_ratios_from_raw": mut_ratios, "F3": f3rec, "d0_threshold_derivation": d0, "crash": crash,
         "tracing_cost": tracing_cost(),
         "t3_positive": {"rule_holds_on_this_box": kv.get("t3_rule_holds"),
                         "P_runsh_t3_rc": rc_of(os.path.join(OUT, "F4", "P_runsh_t3.rc")),
                         "note": "recorded, not a check: only some runners expose cpufreq on 'performance'"},
         "box": {"virt": box["virt"], "flip": box["flip"], "plp": box["plp"]},
         "unplanted_refusals": unplanted(arch, leaf, box), "harness_sha256": harness_sha256(), "checks": results}
    with open(os.path.join(OUT, "verdict.json"), "w") as f:
        json.dump(v, f, indent=1)
    # the binding check (firecheck.sh's last step) is pending for this verdict; check.py --bind replaces this record
    # with its result, and run.sh binds only a verdict whose record passed or is pending (fourth review L3)
    with open(os.path.join(OUT, "verdict.json"), "rb") as f:
        vsha = hashlib.sha256(f.read()).hexdigest()
    with open(os.path.join(OUT, "verdict.bind.pending"), "w") as f:
        json.dump({"verdict_sha256": vsha, "note": "the binding check (check.py --bind) has not finished"}, f)
    red(kv, leaf, arch)
    prev(kv, leaf, arch)
    print("V3 FIRE-CHECK (%s, %s, leaf %s) %d/%d %s; clock: %s; F3 flush control: %s" %
          (CELL, arch, leaf, npass, len(results), "PASS" if v["all_pass"] else "FAIL", clock_note, f3rec.get("flush_control")))
    return 0 if v["all_pass"] else 1


def fourth_review_plants(arch, box):
    """F4 plants for the fourth review's probe fixes: a write-back SCSI drive (scsi_debug, WCE=1) accepted and its
    override refused (M2), the VM detector with cpuinfo and DMI hidden (M1), /etc/ld.so.preload (M4)."""
    f4 = os.path.join(OUT, "F4")
    # M2: the WCE=1 branch, accepted, with the bit re-read from the recorded bytes
    rc = rc_of(os.path.join(f4, "P_sdbg_wb.rc"))
    sj = rj(os.path.join(f4, "P_sdbg_wb.out", "summary.json")) or {}
    lf = sj.get("leaf") or {}
    w, hp = wce_from_hex(lf.get("drive_report_source"))
    check("F4:P_sdbg_wb", rc in (0, 3) and lf.get("driver") == "sd" and lf.get("sd_host") == "scsi_debug"
          and lf.get("kind") == "scsi_debug" and lf.get("creditable") is False and lf.get("drive_reports") == "write back"
          and lf.get("write_cache") == "write back" and w == 1 and not hp,
          {"rc": rc, "leaf": lf, "wce_from_bytes": w, "bytes_problems": hp,
           "text": (rd(os.path.join(f4, "P_sdbg_wb.txt")) or "")[-300:]},
          "a scsi_debug disk (WCE=1) under V3FLOOR_FIRECHECK=1: accepted as write-back, MODE SENSE's bytes say WCE=1, "
          "the leaf marked scsi_debug and not creditable")
    refusal("R_sdbg_flip", "but the drive reports 'write back'", state=True)
    # M1: cpuinfo's hypervisor flag and DMI's names hidden in a mount namespace; the leaf still shows the VM (a VM box
    # only: a bare-metal leaf has nothing to show, sixth review H1)
    if box["virt"] == "vm":
        rc = rc_of(os.path.join(f4, "R_virt_hidden.rc"))
        sj = rj(os.path.join(f4, "R_virt_hidden.out", "summary.json")) or {}
        vz = sj.get("virtualization") or {}
        ev = vz.get("evidence") or []
        prem = rd(os.path.join(f4, "R_virt_hidden.premise")) or ""
        lc = leaf_class_of(sj)
        premise = "product_name=PowerEdge R650" in prem and "sys_vendor=Dell Inc." in prem and "hypervisor_flags=0" in prem
        hb = [] if lc in VIRT_KIND else [("leaf class", lc)]
        hb += label_problems(sj, lc, True) if lc in VIRT_KIND else []
        check("F4:R_virt_hidden", rc in (0, 3) and premise and vz.get("virtualized") is True
              and any(str(e).startswith("leaf:") for e in ev) and not any(str(e).startswith(("cpuinfo:", "DMI:")) for e in ev)
              and vz.get("dmi_product_name") == "PowerEdge R650" and not hb,
              {"rc": rc, "premise": prem.strip(), "virtualization": vz, "labels": hb,
               "text": (rd(os.path.join(f4, "R_virt_hidden.txt")) or "")[-300:]},
              "with cpuinfo's hypervisor flag and DMI's names hidden (a bind mount in a private namespace, read back), "
              "the leaf alone (driver, model, host path) still makes the run virtualized, and its labels say so")
    # sixth review H1, M2: on any box, a leaf with no VM evidence (scsi_debug) with a hypervisor flag planted in
    # cpuinfo and a VM planted in DMI: each detector fires on its own, and the virtual labels follow
    rc = rc_of(os.path.join(f4, "R_virt_planted.rc"))
    sj = rj(os.path.join(f4, "R_virt_planted.out", "summary.json")) or {}
    vz = sj.get("virtualization") or {}
    ev = [str(e) for e in vz.get("evidence") or []]
    prem = rd(os.path.join(f4, "R_virt_planted.premise")) or ""
    premise = "product_name=Virtual Machine" in prem and "sys_vendor=QEMU" in prem and \
        re.search(r"hypervisor_flags=[1-9]", prem) is not None
    pb = [] if (rc in (0, 3) and premise) else [("rc/premise", rc, prem.strip())]
    if vz.get("virtualized") is not True or not any(e.startswith("cpuinfo:") for e in ev) or \
            not any(e.startswith("DMI: sys_vendor") for e in ev) or not any(e.startswith("DMI: product_name") for e in ev) \
            or any(e.startswith("leaf:") for e in ev):
        pb.append(("virtualization", vz))
    if (sj.get("leaf") or {}).get("kind") != "scsi_debug":
        pb.append(("leaf", sj.get("leaf")))
    pb += label_problems(sj, "wb", True)
    check("F4:R_virt_planted", not pb, {"bad": pb, "text": (rd(os.path.join(f4, "R_virt_planted.txt")) or "")[-300:]},
          "a scsi_debug leaf (no VM evidence of its own) with a hypervisor flag planted in cpuinfo and a VM in DMI "
          "(read back): virtualized true with both a cpuinfo and a DMI item and no leaf item, and the virtual labels")
    # fifth review L6: a leaf with no VM evidence of its own (scsi_debug), cpuinfo and DMI hidden: bare metal on x86_64,
    # not ruled out on arm64, and the labels that go with each
    rc = rc_of(os.path.join(f4, "P_virt_bare.rc"))
    sj = rj(os.path.join(f4, "P_virt_bare.out", "summary.json")) or {}
    vz = sj.get("virtualization") or {}
    prem = rd(os.path.join(f4, "P_virt_bare.premise")) or ""
    want = False if arch == "x86_64" else None
    premise = "product_name=PowerEdge R650" in prem and "sys_vendor=Dell Inc." in prem and "hypervisor_flags=0" in prem
    for tag, lc in (("P_virt_bare", "wb"), ("P_virt_bare_wt", "wt")):
        rc = rc_of(os.path.join(f4, tag + ".rc"))
        sj = rj(os.path.join(f4, tag + ".out", "summary.json")) or {}
        vz = sj.get("virtualization") or {}
        prem = rd(os.path.join(f4, tag + ".premise")) or ""
        premise = "product_name=PowerEdge R650" in prem and "sys_vendor=Dell Inc." in prem and "hypervisor_flags=0" in prem
        vb = []
        if not (rc in (0, 3) and premise):
            vb.append(("rc/premise", rc, prem.strip()))
        if lc == "wt":  # scsi_debug's caching page set to WCE=0 by MODE SELECT, read back (sixth review M2)
            st = rd(os.path.join(f4, tag + ".state")) or ""
            if "changed=1" not in st:
                vb.append(("state", st.strip()))
            rs = rd(os.path.join(f4, tag + ".restore")) or ""
            if not rs.strip().endswith("write back"):  # the fixture is shared by later blocks (seventh review L7)
                vb.append(("restore", rs.strip()))
        if "virtualized" not in vz or vz.get("virtualized") is not want or vz.get("evidence") != []:
            vb.append(("virtualization", vz))
        lf = sj.get("leaf") or {}
        if lf.get("kind") != "scsi_debug" or leaf_class_of(dict(sj, leaf=dict(lf, kind="drive"))) != lc:
            vb.append(("leaf", lf))
        if lf.get("drive_reports") != {"wb": "write back", "wt": "write through"}[lc]:
            vb.append(("drive report", lf.get("drive_reports")))
        vb += label_problems(sj, lc, want)
        check("F4:" + tag, not vb, {"bad": vb, "text": (rd(os.path.join(f4, tag + ".txt")) or "")[-300:]},
              "a %s leaf with no VM evidence (scsi_debug%s) with cpuinfo's flag and DMI's names hidden: virtualized %r "
              "with no evidence, floor_kind %r, and the matching flush_sent_to_device and floor_claim" %
              ("write-back" if lc == "wb" else "write-through", "" if lc == "wb" else ", WCE cleared", want,
               VIRT_KIND[lc][want]))
    # fifth review M1, M2: the root disk's leaf made remote (an sd host or an NVMe transport outside its allowlist)
    rc = rc_of(os.path.join(f4, "R_leaf_remote.rc"))
    txt = rd(os.path.join(f4, "R_leaf_remote.txt")) or ""
    what = (rd(os.path.join(f4, "R_leaf_remote.what")) or "").strip()
    prem = rd(os.path.join(f4, "R_leaf_remote.premise")) or ""
    kind = what.split(" ")[0].split("=", 1)[-1]  # firecheck.sh writes "faked=<sd_host|nvme_transport> <host|ctrl>"
    exp = {"sd_host": ("proc_name=tcm_loopback", "is on SCSI host 'tcm_loopback'"),
           "nvme_transport": ("transport=tcp", "has transport 'tcp', not pcie")}.get(kind)
    check("F4:R_leaf_remote", exp is not None and rc == 2 and exp[0] in prem and exp[1] in txt
          and not os.path.exists(os.path.join(f4, "R_leaf_remote.out")),
          {"rc": rc, "faked": what, "premise": prem.strip(), "text": txt[-300:]},
          "the cell's leaf disk with its SCSI host named tcm_loopback (sd) or its NVMe transport tcp, read back inside a "
          "private mount namespace: refused by the host or transport allowlist (rc 2)")
    # gate-6 review v3 #1 (A14): the mutant (no fsync: the fastest possible "fsync") on scsi_debug, write-back without
    # PLP -> the timing control voids it; the same declared PLP -> not applicable; write-through -> not applicable
    for tag, lc, plp, want, wrc in (("R_tc_wb", "wb", "no", "FAIL: run void (append25 ratio", 3),
                                    ("P_tc_plp", "wb", "yes", "not applicable: PLP", 0),
                                    ("P_tc_wt", "wt", "no", "not applicable: no volatile cache", 0)):
        rc = rc_of(os.path.join(f4, tag + ".rc"))
        sj = rj(os.path.join(f4, tag + ".out", "summary.json")) or {}
        lf = sj.get("leaf") or {}
        tb = []
        if lf.get("kind") != "scsi_debug" or leaf_class_of(dict(sj, leaf=dict(lf, kind="drive"))) != lc:
            tb.append(("leaf", lf.get("kind"), lf.get("write_cache")))
        if lc == "wt" and "changed=1" not in (rd(os.path.join(f4, tag + ".state")) or ""):
            tb.append(("state", rd(os.path.join(f4, tag + ".state"))))
        if sj.get("plp") != plp or int(sj.get("mutant_nosync", 0)) != 1:
            tb.append(("plp/mutant", sj.get("plp"), sj.get("mutant_nosync")))
        if not str(sj.get("timing_control", "")).startswith(want) or rc != wrc:
            tb.append(("timing_control/rc", sj.get("timing_control"), rc, want, wrc))
        check("F4:" + tag, not tb, {"bad": tb, "text": (rd(os.path.join(f4, tag + ".txt")) or "")[-300:]},
              "the no-fsync mutant on a %s scsi_debug leaf, --plp %s: timing_control '%s', rc %d (A14)" % (lc, plp, want, wrc))
    # M4: /etc/ld.so.preload names a library; the dynamic build loads it and refuses, the static build never loads it
    rc = rc_of(os.path.join(f4, "R_ldso_preload.rc"))
    txt = rd(os.path.join(f4, "R_ldso_preload.txt")) or ""
    mark = rd(os.path.join(f4, "R_ldso_preload.mark"))
    st = rd(os.path.join(f4, "R_ldso_preload.state")) or ""
    check("F4:R_ldso_preload", rc == 2 and "is mapped into it" in txt and "noop_shim.so" in txt and "changed=1" in st
          and mark is not None and "v3floor" in mark and not os.path.exists(os.path.join(f4, "R_ldso_preload.out")),
          {"rc": rc, "text": txt[-400:], "mark": (mark or "MISSING")[-200:], "state": st.strip()},
          "/etc/ld.so.preload naming a library: the dynamic build loaded it (its mark) and refused, naming it (rc 2)")
    rc = rc_of(os.path.join(f4, "P_ldso_static.rc"))
    mark = rd(os.path.join(f4, "P_ldso_static.mark"))
    st = rd(os.path.join(f4, "P_ldso_static.state")) or ""
    sj = rj(os.path.join(f4, "P_ldso_static.out", "summary.json")) or {}
    check("F4:P_ldso_static", rc in (0, 3) and mark is not None and mark.strip() == "" and "changed=1" in st
          and sj.get("linkage") == "static" and len(sj.get("mapped_files") or []) == 1,
          {"rc": rc, "mark": (mark if mark is not None else "MISSING")[-200:], "state": st.strip(),
           "linkage": sj.get("linkage"), "mapped_files": sj.get("mapped_files")},
          "/etc/ld.so.preload naming the same library: the static build never loads it (empty mark), runs, and maps "
          "only its own file")


def tracing_cost():
    """gate-6 review LOW 10: F3's p50s (traced: blkflush's tracefs instance on) against an untraced run of the same
    arms, n and seed (stage T). Descriptive: recorded in the verdict, never a check (one pair per cell)."""
    t = rj(os.path.join(OUT, "T.out", "summary.json")) or {}
    f = rj(os.path.join(OUT, "F3", "summary.probe.json")) or {}
    rec = {"rule": "descriptive (gate-6 review LOW 10): p50 traced (F3, via run.sh) minus untraced (T, the probe "
                   "alone), same arms, n = 200, seed 11; one pair per cell, adjacent in time, not interleaved",
           "untraced_rc": rc_of(os.path.join(OUT, "T.rc")), "arms": {}}
    for a, x in (f.get("arms") or {}).items():
        y = (t.get("arms") or {}).get(a)
        if y:
            rec["arms"][a] = {"traced_p50_us": x.get("p50_us"), "untraced_p50_us": y.get("p50_us"),
                              "delta_us": round(x.get("p50_us", 0) - y.get("p50_us", 0), 1)}
    return rec


def unplanted(arch, leaf, box):
    u = ["a mount whose mountinfo line is malformed or too long", "statx returning no mount id",
         "an unreadable /proc/fs/ext4 options file or /proc/fs/jbd2", "a SCSI or virtio cache_type that cannot be read "
         "or parsed", "NVMe controllers of one subsystem disagreeing on VWC", "a brd leaf whose write_cache is not "
         "write-through", "the drive's report unreadable (a closed NVMe or sd node, MODE SENSE failing or without a "
         "caching page; the CI grants read access)", "inode flags unreadable", "D's mount id changing between the "
         "lookup and the run", "whichever of the sd-host and NVMe-transport allowlists the cell's leaf disk does not "
         "exercise (R_leaf_remote plants the one it does; check.py --self-test covers both rules in the checker)",
         "a MODE SENSE reply that is short (resid), sub-page format (SPF) or has a "
         "zero page length (scsi_debug always answers whole)", "/proc/self/maps unreadable",
         "a hypervisor that hides the CPUID bit and presents non-virtual DMI and drive identities (the stated blind spot; "
         "the plant hides cpuinfo and DMI only)"]
    if box.get("flip") != "yes":
        u.append("the kernel's write_cache disagreeing with the drive: the leaf disk reads write-through and is not sd, "
                 "so neither direction can be planted (the kernel refuses 'write back' on a queue without a volatile "
                 "cache: recalled, unverified)")
    u.append("run.sh's rental-mode loop-cell refusal (on a runner without the performance governor t3pre refuses first; "
             "post's rental rules are self-tested: batchgate self-test), and the probe's rental no-threshold refusal, "
             "which needs a write-back leaf without PLP (its other --require-registered refusals are planted: "
             "R_rental_noreg, R_rental_noframe, R_rental_novariant)")
    if leaf != "wt":
        u.append("A16's write-through refusals on this cell's own batch (its leaf is not write-through; the post plants "
                 "R_post_wtflush and R_post_wtmismatch make a copy of it write-through)")
    if box.get("virt") != "vm":
        u.append("the leaf's own VM evidence (driver, model, SCSI host, device path) on this box: a bare-metal leaf has "
                 "none to hide (R_virt_planted fires the cpuinfo and DMI detectors; the CI VMs fire the leaf detector)")
    if leaf == "brd":
        u.append("post's brd rule as a plant (this cell's batch is brd before anything is planted; the drive cells' "
                 "R_post_brd discriminates it)")
    if arch != "x86_64":
        u.append("a clocksource other than tsc/arch_sys_counter (arm64 runners offer only arch_sys_counter)")
    return u


def refusal(tag, want, runsh=False, state=False):
    rc = rc_of(os.path.join(OUT, "F4", tag + ".rc"))
    txt = rd(os.path.join(OUT, "F4", tag + ".txt")) or ""
    o = os.path.join(OUT, "F4", tag + ".out")
    if tag == "R_outexists":
        untouched = os.path.isdir(o) and sorted(os.listdir(o)) == ["sentinel"]
        check("F4:" + tag, rc == 2 and want in txt and untouched, {"rc": rc, "text": txt[-300:], "untouched": untouched},
              "refuse -> rc 2, says '%s', the existing dir untouched" % want)
        return
    made = os.path.exists(o) or os.path.exists(o + ".stamp_start.json") or os.path.exists(o + ".blkflush")
    extra = {}
    ok = rc == 2 and want in txt and not made
    if tag == "R_symlink":  # the planted symlink's target must not have been created
        tgt = rd(os.path.join(OUT, "F4", "R_symlink.target")) or "MISSING"
        extra["target_after"] = tgt.strip()
        ok = ok and tgt.strip() == "absent"
    if state:  # the planted state was read back from sysfs while the probe ran (fourth review L9)
        st = rd(os.path.join(OUT, "F4", tag + ".state")) or ""
        extra["state"] = st.strip()
        ok = ok and "changed=1" in st
    check("F4:" + tag, ok, dict({"rc": rc, "out_dir_made": made, "text": txt[-300:]}, **extra),
          "%s refuses -> rc 2, says '%s', no out dir" % ("run.sh" if runsh else "the probe", want))


def check_real(o3, rc, kv, leaf):
    """F3: the batch through run.sh: probe output vs raw, the device flush record, the merge, the gate, the record."""
    sj = rj(os.path.join(o3, "summary.probe.json"))
    merged = rj(os.path.join(o3, "summary.json"))
    rows = raw_rows(os.path.join(o3, "raw.tsv"))
    st0, st1 = rj(os.path.join(o3, "stamp_start.json")), rj(os.path.join(o3, "stamp_end.json"))
    b = rd(os.path.join(o3, "binary.txt")) or ""
    rcl = rd(os.path.join(o3, "rc")) or ""
    m = re.search(r"probe_rc=(\d+)", rcl)
    probe_rc = int(m.group(1)) if m else None
    n, arms = 200, ALL.split(",")
    rec = {"rc": rc, "probe_rc": probe_rc}
    if sj is None or rows is None:
        for cid in ("F3:complete", "F3:devflush", "F3:merge", "F3:gate", "F3:record"):
            check(cid, False, dict(rec, rc_file=rcl), "F3 real run n=200: summary.probe.json and raw.tsv exist")
        return rec
    bad = []
    if sorted(rows) != sorted(ran(arms, KIND)):
        bad.append(("arms in raw", sorted(rows), sorted(ran(arms, KIND))))
    want_ref = sorted(a for a in arms if a not in ran(arms, KIND))
    if sorted(sj.get("refused_arms", {})) != want_ref:
        bad.append(("refused_arms", sorted(sj.get("refused_arms", {})), want_ref))
    for a, why in sj.get("refused_arms", {}).items():
        if "FICLONE" not in why:
            bad.append(("refusal reason does not name the trial FICLONE", a, why))
    sb, ratios, fail, p50 = summary_vs_raw(sj, rows, n)
    bad += sb
    if probe_rc != (3 if fail else 0):
        bad.append(("probe rc vs the control recomputed from raw", probe_rc, fail))
    if int(sj.get("mutant_nosync", -1)) != 0 or int(sj.get("trace_clock", -1)) != 0:
        bad.append(("flags", sj.get("mutant_nosync"), sj.get("trace_clock")))
    if sj.get("traced") is not False:  # ninth review M4: F3 runs untraced (run.sh), and the probe must say so
        bad.append(("F3 ran traced or did not record it", sj.get("traced")))
    lp = v3cell.layout_problems(CELL, sj.get("fstype"), sj.get("mount_source"), sj.get("flush_path"))
    if lp:
        bad.append(("layout", lp))
    if "bound=smoke: V3_SMOKE=1" not in b or ("cell=%s" % CELL) not in b:
        bad.append(("binary.txt binding", b))
    if st0 is None or st1 is None:
        bad.append(("stamps missing",))
    elif st1.get("problems"):
        bad.append(("stamp problems", st1["problems"]))
    check("F3:complete", not bad, {"bad": bad[:10]},
          "F3 real run n=200 via run.sh: complete, summary == raw, probe rc matches the control recomputed from raw, "
          "the cell's layout, stamps taken (the control's verdict is recorded, not gated)")
    rec.update({"flush_control": "pass" if not fail else "FAIL (%s)" % ",".join(fail),
                "ratios_vs_nosync25_from_raw": {a: round(r, 1) for a, r in ratios.items()},
                "p50_us_from_raw": {a: round(v / 1e3, 1) for a, v in p50.items()},
                "refused_arms": sj.get("refused_arms"), "fstype": sj.get("fstype"), "mount_source": sj.get("mount_source"),
                "flush_path": [(l.get("mount"), l.get("fstype"), l.get("source"), l.get("disk"), l.get("write_cache"))
                               for l in sj.get("flush_path") or []],
                "leaf": sj.get("leaf"), "virtualization": sj.get("virtualization"), "floor_kind": sj.get("floor_kind"),
                "flush_sent_to_device": sj.get("flush_sent_to_device")})

    # the device flush record (item 2)
    rep = rj(os.path.join(o3, "blkflush", "report.json")) or {}
    dbad = []
    if not rep or rep.get("refused") or "windows" not in rep:
        dbad.append(("no device flush record", rep.get("refused")))
    else:
        w = rep["windows"]
        nrun = len(ran(arms, KIND))
        if w.get("n_windows") != n * nrun:
            dbad.append(("windows", w.get("n_windows"), n * nrun))
        wa = w.get("arms") or {}
        layers = sj.get("flush_path") or []
        leafinfo = sj.get("leaf") or {}
        amb = w.get("ambiguous_by_device") or {}
        rootd = (kv.get("root_disk") or "").split(" ")[0]
        shared = []  # the leaf drive only when it is the runner's root disk (fresh review I-L6: a T3 data disk is private)
        for k, l in enumerate(layers):
            names = [l.get("disk")]
            if k == len(layers) - 1:
                names += [p.get("disk") for p in leafinfo.get("multipath") or []]
            is_brd = k == len(layers) - 1 and leafinfo.get("kind") == "brd"
            private = k < len(layers) - 1 or is_brd or l.get("disk") != rootd
            if not private:
                shared += names
            n0 = wa.get("nosync25", {})
            for x in names:
                r = (n0.get("devices") or {}).get(x)
                hit = (n0.get("ops", n) - r.get("zero_windows", n)) if r else 0
                # an event that may be nosync25's but cannot be placed (a window edge, a sub-us window) counts too
                # (fourth review L7)
                namb = (n0.get("ambiguous_by_device") or {}).get(x, 0) if isinstance(n0.get("ambiguous_by_device"), dict) else n
                if (hit + namb) and (private or hit + namb > SHARED_MAX_FRAC * n0.get("ops", n)):
                    dbad.append(("nosync25 windows hold flush requests", x, "private" if private else "shared", r,
                                 {"ambiguous": namb}))
                if private and amb.get(x):
                    dbad.append(("events at a window edge on a private device", x, amb.get(x), w.get("ambiguous_sample")))
            total = sum((rep.get("devices") or {}).get(x, {}).get("total", 0) for x in names)
            if l.get("write_cache") == "write back" and not is_brd:
                for a in GATED:
                    if a not in rows:
                        continue
                    hit = 0
                    zero = None
                    for x in names:  # a window counts only with a flush-carrying request (fourth review L9)
                        dv = (wa.get(a, {}).get("devices") or {}).get(x)
                        if dv:
                            hit += dv.get("events", 0)
                            z = dv.get("flush_carrying_zero_windows")
                            z = n if not isinstance(z, int) else z
                            zero = z if zero is None else min(zero, z)
                    if zero is None or zero > 0:
                        dbad.append(("a gated op's window holds no flush-carrying request to a write-back layer", k,
                                     names, a, {"events": hit, "flush_carrying_zero_windows": zero}))
            elif total:
                dbad.append(("flush requests issued to a write-through (or brd) device", k, names, total))
        dfp = merged.get("device_flushes_per_op") if merged else None
        for a in GATED:
            if a in rows and (not isinstance(dfp, dict) or a not in dfp):
                dbad.append(("device_flushes_per_op lacks a gated arm", a))
        # the app's own syncs (A16): every flush-gated op's window holds an fsync or fdatasync by the probe's pid
        sy = rep.get("syscalls") or {}
        if sy.get("pid") != sj.get("pid") or not isinstance(sy.get("arms"), dict):
            dbad.append(("no per-window sync record for the probe's pid", sy.get("pid"), sj.get("pid")))
        else:
            for a in GATED:
                if a in rows and ((sy["arms"].get(a) or {}).get("windows_without_a_sync") != 0 or
                                  (sy["arms"].get(a) or {}).get("ops") != n):
                    dbad.append(("a flush-gated op's window holds no fsync by the probe", a, sy["arms"].get(a)))
                # tenth review HIGH 1: syncs are attributed by fd, so each window must hold ALL its own (clone2b and
                # cfr2b sync the clone and the directory)
                if a in rows and (sy["arms"].get(a) or {}).get("windows_short") != 0:
                    dbad.append(("a flush-gated op's window lacks one of its own syncs", a, sy["arms"].get(a)))
            if (sy["arms"].get("nosync25") or {}).get("syncs") != 0:
                dbad.append(("nosync25's windows hold a sync by the probe", sy["arms"].get("nosync25")))
            # eleventh review MED 1: nosync25 owns no fd, so the fd rule above can never give it a sync; blkflush also
            # counts the pid's syncs wholly inside each arm's windows on ANY fd, and nosync25's must be 0; and no sync
            # may lie wholly inside a window on an fd that window's arm does not own, or on none (a missing count is
            # not a zero)
            if (sy["arms"].get("nosync25") or {}).get("syncs_inside_any_fd") != 0:
                dbad.append(("nosync25's windows hold a sync by the probe on some fd", sy["arms"].get("nosync25")))
            ua = sy.get("unattributed") if isinstance(sy.get("unattributed"), dict) else {}
            if ua.get("inside_foreign_fd") != 0 or ua.get("inside_no_fd") != 0:
                dbad.append(("a sync by the probe lies wholly inside a window on an fd its arm does not own, or on none",
                             sy.get("unattributed")))
        dbad += sync_fds_problems(sj, rows, n, rd(os.path.join(OUT, "F1b", "real-all.trace.gz")))
        rec["device_flushes_per_op"] = dfp
        rec["layer_device_flushes_per_op"] = (merged or {}).get("layer_device_flushes_per_op")
        rec["shared_devices"] = shared
        rec["ambiguous_by_device"] = amb
        n0ops = wa.get("nosync25", {}).get("ops", n)
        rec["nosync25_windows_hit"] = {x: n0ops - r.get("zero_windows", n0ops)
                                       for x, r in (wa.get("nosync25", {}).get("devices") or {}).items()}
    check("F3:devflush", not dbad, {"bad": dbad[:8]},
          "blkflush: every flush-gated op's window holds the probe's own fsync and nosync25's none (A16); "
          "device_flushes_per_op for every gated arm; nosync25's windows hold no flush request on a device "
          "private to the cell (and at most %d%% of them, ambiguous events included, a foreign one on the shared drive); "
          "every gated op's window holds a flush-carrying request (flush, preflush, preflush+fua) to each write-back "
          "layer and none reaches a write-through one; no edge event on a private device" % int(SHARED_MAX_FRAC * 100))
    mb = []
    if merged is None:
        mb.append("no summary.json")
    else:
        extra = sorted(set(merged) - set(sj))
        if extra != ["app_syncs_per_op", "device_flushes", "device_flushes_per_op", "floor_claim_from_counts", "flush_gate",
                     "layer_device_flushes_per_op"]:
            mb.append(("added keys", extra))
        if any(merged.get(k) != v for k, v in sj.items()):
            mb.append(("a probe field changed", [k for k, v in sj.items() if merged.get(k) != v][:5]))
    check("F3:merge", not mb, {"bad": mb}, "summary.json is the probe's summary.probe.json plus the device flush and gate keys only")
    g = rj(os.path.join(o3, "gate.json")) or {}
    fg = g.get("flush_gate") or {}
    want = {"wb": "pass", "wt": "not applicable: no volatile cache: no drive flush", "brd": "not applicable: brd"}.get(leaf)
    gb = []
    if g.get("refusals"):
        gb.append(("refusals", g.get("refusals")))
    if fg.get("outcome") != want:
        gb.append(("flush gate", fg, want))
    if (merged or {}).get("flush_gate") != fg:
        gb.append("summary.json's flush_gate differs from gate.json's")
    # ninth review M5: the gate's inputs derived here by hand, never trusted from the gate's own record
    ran_g = sorted(a for a in GATED if a in (sj.get("arms") or {}))
    if sorted(fg.get("gated_arms_run") or []) != ran_g:
        gb.append(("gated_arms_run", fg.get("gated_arms_run"), ran_g))
    if not isinstance(sj.get("n"), int) or fg.get("required_flushes") != sj["n"] * len(ran_g):
        gb.append(("required_flushes", fg.get("required_flushes"), sj.get("n"), len(ran_g)))
    bk = fg.get("blkflush_leaf_gate") or {}
    if not str(bk.get("outcome", "")).startswith("not applicable") and sorted(bk.get("arms") or {}) != ran_g:
        gb.append(("blkflush gate arms", sorted(bk.get("arms") or {}), ran_g))
    check("F3:gate", not gb, {"bad": gb, "gate": g},
          "run.sh's gate: no post-run refusal; the diskstats leaf flush gate passes on a write-back leaf (>= n x gated "
          "arms) and labels a write-through or brd leaf, never passes it")
    rb = record_problems(sj, merged, rep, st0, kv, leaf, p50)
    check("F3:record", not rb, {"bad": rb},
          "the batch records the clocksource (allowlisted, in summary and stamp), cpufreq/cpuidle, the binary's own sha256 "
          "(static, nothing else mapped), per-layer write_cache/fua and ext4 data=/commit=/async commit, virtualization "
          "consistent with systemd-detect-virt, floor_kind and flush_sent_to_device for its leaf and virtualization, the "
          "claim re-derived from the clean arm's windows with its qualifier, the gated set (clone1b report-only) and "
          "'durability unverified on Linux'")
    rec["flush_gate"] = fg
    return rec


FRAME_VARIANT = {None: "no frame arm registered", "ow4k": "fdatasync4k (ow4k + fdatasync)"}
FRAME_VARIANT_NONE = "none in this probe: the registered frame arm has no fdatasync variant arm (A18 needs one)"


def f1b_window_syncs(text):
    """eleventh review MED 2: ({arm: fds}, problems) for the fsync and fdatasync calls INSIDE the timed windows of an
    F1b strace -f -y trace: between a window's opening and closing clock_gettime(CLOCK_MONOTONIC_RAW), the only clock
    reads the loop makes (sequence_problems holds F1b:real-all to exactly 2 x n x arms of them). Setup and teardown
    syncs are outside every window and not counted. A sync split by strace (<unfinished ...>) counts by its opening
    half. The arm is the synced path's: <work>/<arm>, <work>/<arm>.clones or <work>/<arm>.clones/c<i>."""
    seen, clocks, probs = {}, 0, []
    for line in text.splitlines():
        m = LINE.match(line)
        if m and "<unfinished ...>" not in line and "resumed>" not in line:
            name, args = m.group(2), m.group(3)
        else:
            u = re.match(r"^\d+\s+(fsync|fdatasync)\((.*?)\s*<unfinished \.\.\.>$", line)
            if not u:
                continue
            name, args = u.group(1), u.group(2)
        if name == "clock_gettime":
            clocks += 1
            continue
        if name not in ("fsync", "fdatasync") or clocks % 2 == 0:
            continue
        a = re.fullmatch(r"(\d+)<([^>]+)>", args.strip())
        if not a:
            probs.append("an in-window sync without fd<path>: " + line[:120])
            continue
        path, base = a.group(2), os.path.basename(a.group(2))
        arm = (os.path.basename(os.path.dirname(path))[:-len(".clones")] if re.fullmatch(r"c\d+", base)
               else base[:-len(".clones")] if base.endswith(".clones") else base)
        seen.setdefault(arm, set()).add(int(a.group(1)))
    if clocks == 0 or clocks % 2:
        probs.append("%d clock reads: the trace holds no whole timed windows" % clocks)
    return seen, probs


def sync_fds_problems(sj, rows, n, f1b_trace):
    """tenth review HIGH 1: the probe's sync_fds (the fds each arm's ops synced, the key blkflush attributes by) must
    match the arm definitions (check.OP: the syncs per op, one fd per synced file: the clone and the directory for
    clone2b and cfr2b, the directory for clone1b, the arm's own file otherwise, none for nosync25) and EQUAL, per arm,
    the set of fds F1b's real-all strace saw synced inside that arm's timed windows (strace -y prints each sync as
    fd<path>). Eleventh review MED 2: F1b:real-all is planned on every cell, so a missing or unreadable trace refuses
    (it skipped the check silently), and only in-window syncs count (setup syncs every arm's file, nosync25's
    included); equality, not a subset, so an in-window sync the probe did not record (nosync25's, say) shows."""
    bad = []
    sf = sj.get("sync_fds")
    if sj.get("sync_fds_overflow") is not False or not isinstance(sf, dict):
        return [("sync_fds disagrees with the arm definitions", "missing or overflowed", sj.get("sync_fds_overflow"))]
    for a in rows:
        spec = OP.get(a, {})
        per_op = spec.get("fsync", 0) + spec.get("fdatasync", 0)
        nfd = 2 if a in ("clone2b", "cfr2b") else (1 if per_op else 0)
        got = sf.get(a)
        if not isinstance(got, dict) or len(got) != nfd or sum(got.values()) != n * per_op or \
                any(v != n for v in got.values()):
            bad.append(("sync_fds disagrees with the arm definitions", a, got, nfd, n * per_op))
    if f1b_trace is None:
        bad.append(("no F1b real-all trace: sync_fds cannot be cross-checked", os.path.join("F1b", "real-all.trace.gz")))
        return bad
    seen, probs = f1b_window_syncs(f1b_trace)
    if probs:
        bad.append(("the F1b trace's timed windows cannot be read", probs[:3]))
        return bad
    for a in rows:
        want = {int(k) for k in (sf.get(a) or {})}
        if want != seen.get(a, set()):
            bad.append(("sync_fds disagrees with F1b's strace", a, sorted(want), sorted(seen.get(a, set()))))
    return bad


def floor_reference_problems(sj, p50ns):
    """A18 as the probe applies it (ninth review H1, L10, M6): the floor reference is the arm with the least raw p50
    (nanoseconds, from raw.tsv: two arms within 0.1 us must not split on rounding) among append25 (fsync) and
    fdatasync4k (ow4k + fdatasync: the frame arm's fdatasync variant when the frame arm is ow4k) that ran. The frame
    arm itself runs with fsync and is never a candidate, registered or not. floor_frame_variant names the variant for
    the registered frame arm, or records that this probe has none."""
    bad = []
    arms = sj.get("arms") or {}
    fr = sj.get("floor_reference")
    want_v = FRAME_VARIANT.get(sj.get("frame_arm"), FRAME_VARIANT_NONE)
    if sj.get("floor_frame_variant") != want_v:
        bad.append(("floor_frame_variant", sj.get("floor_frame_variant"), want_v))
    cands = [a for a in ("append25", "fdatasync4k") if a in arms]
    if not cands:
        return bad + ([] if fr is None else [("floor_reference without a candidate arm", fr)])
    if not isinstance(p50ns, dict) or any(not isinstance(p50ns.get(a), (int, float)) for a in cands):
        return bad + [("floor_reference: no raw p50 for the candidates", cands)]
    least = min(p50ns[a] for a in cands)
    best = [a for a in cands if p50ns[a] == least]  # an exact tie in nanoseconds: either is the minimum
    if not isinstance(fr, dict) or fr.get("arm") not in best or \
            abs((fr.get("p50_us") or -1) - p50ns[fr.get("arm")] / 1e3) > 0.051 or \
            fr.get("barrier") != ("fdatasync" if fr.get("arm") == "fdatasync4k" else "fsync"):
        bad.append(("floor_reference", fr, best, round(least / 1e3, 3)))
    return bad


def record_problems(sj, merged, rep, st0, kv, leaf, p50=None):
    """F3:record's rules on the batch's own records (check_real calls it; the self-test drives check_real itself on
    planted copies of a banked batch, so the call is covered too: sixth review M3)."""
    rep = rep or {}
    rb = []
    if sj.get("clocksource") not in CLOCKSOURCES:
        rb.append(("clocksource", sj.get("clocksource")))
    if st0 and st0.get("clocksource") != sj.get("clocksource"):
        rb.append(("stamp clocksource", (st0 or {}).get("clocksource")))
    for k in ("cpufreq", "cpuidle"):
        if not st0 or k not in st0:
            rb.append(("stamp lacks", k))
    if sj.get("exe_sha256") != kv.get("v3floor_sha256"):
        rb.append(("exe_sha256", sj.get("exe_sha256"), kv.get("v3floor_sha256")))
    for k, l in enumerate(sj.get("flush_path") or []):
        if not l.get("write_cache") or not l.get("fua") or not l.get("disk"):
            rb.append(("layer write_cache/fua", k))
        if l.get("fstype") == "ext4":
            e = l.get("ext4") or {}
            if not (e.get("data") and e.get("commit_s") and isinstance(e.get("journal_async_commit"), bool) and e.get("journal")):
                rb.append(("ext4 layer lacks data=/commit=/journal_async_commit/journal", k, e))
    # virtualization (fourth review M1): checked against an instrument outside the probe, systemd-detect-virt,
    # recorded by firecheck.sh; "not virtualized" needs x86_64 and detect-virt saying none
    vz = sj.get("virtualization") or {}
    vm = vz.get("virtualized", "absent")
    vbad = virt_problems(vz, (kv.get("detect_virt") or "").strip(), arch_of(kv))
    if vbad:
        rb.append(("virtualization", vbad, vz))
    rb += label_problems(sj, leaf, vm)
    rb += linkage_problems(sj)
    # A14/A18: the declaration, the pid the sync record is keyed on, the floor reference
    if sj.get("plp") != (kv.get("plp") or "").strip():
        rb.append(("plp", sj.get("plp"), kv.get("plp")))
    if not isinstance(sj.get("pid"), int):
        rb.append(("pid", sj.get("pid")))
    rb += floor_reference_problems(sj, p50)
    # the claim the batch may make, re-derived here from the device flush record's clean windows (fresh reviews
    # P-H1, B-H2; fourth review L1, L2, L9)
    fpl = sj.get("flush_path") or []
    lnames = ([fpl[-1].get("disk")] if fpl else []) + [p.get("disk") for p in (sj.get("leaf") or {}).get("multipath") or []]
    ca = ((rep.get("windows") or {}).get("arms") or {}).get("clean") if rep else None
    clean_k = None
    if leaf == "wb":
        if not ca or not ca.get("ops"):
            want_claim = "no bare-flush baseline: the clean arm did not run"
        else:
            zero = min([((ca.get("devices") or {}).get(x) or {}).get("bare_flush_zero_windows", ca["ops"]) for x in lnames]
                       or [ca["ops"]])
            clean_k = (ca["ops"] - zero, ca["ops"])
            bare = clean_k[0] >= 0.95 * clean_k[1] and sj.get("fstype") in ("ext4", "xfs")
            want_claim = "clean fsync (a bare flush" if bare else "no bare-flush baseline"
    else:
        want_claim = "none: a brd floor" if leaf == "brd" else "no drive flush"
    claim = str((merged or {}).get("floor_claim_from_counts", ""))
    if not claim.startswith(want_claim):
        rb.append(("floor_claim_from_counts", claim, want_claim, clean_k))
    rb += qualifier_problems(claim, vm, leaf, "floor_claim_from_counts")
    if sj.get("fstype") == "btrfs" and "bare flush" in str(sj.get("floor_claim", "")) and "no bare-flush" not in str(sj.get("floor_claim", "")):
        rb.append(("btrfs floor_claim promises a bare flush", sj.get("floor_claim")))
    if sj.get("arms_gated") != GATED:
        rb.append(("arms_gated", sj.get("arms_gated")))
    if "clone1b" not in (sj.get("arms_report_only") or {}):
        rb.append("clone1b is not report-only")
    du = sj.get("durability") or {}
    if "unverified on Linux" not in du.get("clone2b", "") or "unverified on Linux" not in du.get("cfr2b", ""):
        rb.append(("durability", du))
    return rb


def arch_of(kv):
    return kv.get("arch", "")


def bind(out, cell):
    """firecheck.sh's last step: run.sh bound to the real verdict (P_runsh_ok, or P_runsh_brd on a brd cell)."""
    v = rj(os.path.join(out, "verdict.json")) or {}
    res = []
    if v.get("leaf_class") == "brd":
        rc = rc_of(os.path.join(out, "F4", "P_runsh_brd.rc"))
        txt = rd(os.path.join(out, "F4", "P_runsh_brd.txt")) or ""
        ok = rc == 2 and "leaf_class brd: a brd fire-check" in txt  # its own rule's reason (fourth review L4)
        res.append({"id": "bind:P_runsh_brd", "pass": ok, "detail": {"rc": rc, "text": txt[-300:]},
                    "check": "a brd cell's own passing verdict does not bind a batch, refused by the brd rule itself"})
    else:
        rc = rc_of(os.path.join(out, "F4", "P_runsh_ok.rc"))
        bt = rd(os.path.join(out, "F4", "P_runsh_ok.out", "binary.txt")) or ""
        vs = hashlib.sha256(open(os.path.join(out, "verdict.json"), "rb").read()).hexdigest() if v else None
        g = rj(os.path.join(out, "F4", "P_runsh_ok.out", "gate.json"))
        rcl = rd(os.path.join(out, "F4", "P_runsh_ok.out", "rc")) or ""
        ok = (rc in (0, 3) and "bound=fire-checked: " in bt and ("verdict_sha256=%s" % vs) in bt and
              ("v3floor_sha256=%s" % v.get("v3floor_sha256")) in bt and ("cell=%s" % cell) in bt and
              "bind_basis=pending record, bind step" in bt and
              isinstance(g, dict) and g.get("refusals") == [] and re.search(r"gate_rc=(0|3) ", rcl) is not None)
        g = g or {}
        # the batch ran append25 and nosync25 only, so its claim must say the clean arm did not run (fourth review L1)
        sm = rj(os.path.join(out, "F4", "P_runsh_ok.out", "summary.json")) or {}
        claim = str(sm.get("floor_claim_from_counts", ""))
        want = {"wb": "no bare-flush baseline: the clean arm did not run", "wt": "no drive flush"}.get(v.get("leaf_class"))
        cok = bool(want) and claim.startswith(want) and "issued no" not in claim
        res.append({"id": "bind:P_runsh_ok", "pass": ok and cok, "detail": {"rc": rc, "binary.txt": bt, "gate": g,
                                                                           "claim": claim, "claim_want": want},
                    "check": "run.sh binds a batch to this cell's real passing verdict, records its sha256 and run id, "
                             "and the batch's claim says its clean arm did not run"})
        for tag, what in (("R_runsh_boundshape", "N=5"), ("R_runsh_boundarms", "N=10000 without fdatasync4k")):
            rc = rc_of(os.path.join(out, "F4", tag + ".rc"))
            txt = rd(os.path.join(out, "F4", tag + ".txt")) or ""
            res.append({"id": "bind:" + tag, "pass": rc == 2 and "bound shape: a bound batch runs N=10000" in txt
                        and not os.path.exists(os.path.join(out, "F4", tag + ".out")),
                        "detail": {"rc": rc, "text": txt[-300:]},
                        "check": "the same verdict, bound, but %s: run.sh refuses before the probe runs (the registered "
                                 "V3 shape: gate-6 review MED 6, eighth review L4)" % what})
    vsha = hashlib.sha256(open(os.path.join(out, "verdict.json"), "rb").read()).hexdigest() \
        if os.path.exists(os.path.join(out, "verdict.json")) else None
    b = {"cell": cell, "verdict_sha256": vsha, "verdict_all_pass": v.get("all_pass"), "checks": res,
         "all_pass": all(r["pass"] for r in res) and bool(res)}
    # the binding record run.sh requires next to the verdict (fourth review L3); it replaces the pending one
    with open(os.path.join(out, "verdict.bind.json"), "w") as f:
        json.dump(b, f, indent=1)
    try:
        os.remove(os.path.join(out, "verdict.bind.pending"))
    except OSError:
        pass
    for r in res:
        print(("PASS " if r["pass"] else "FAIL ") + r["id"] + " -- " + r["check"] + ("" if r["pass"] else ": " + json.dumps(r["detail"])[:600]))
    return 0 if b["all_pass"] and v.get("all_pass") else 1


# ---- the red column: the same plants against the base (df4b39e53) probe and scripts ----------------------------
RED = [  # tag, review item, what the base does that the fix stops, how the outcome is read
    ("red_1a_brd", "1(a)", "the base accepts a brd leaf", "rc0"),
    ("red_1a_driver", "1(a)", "the base accepts a dm leaf (no driver)", "rc0"),
    ("red_1b_leafflip", "1(b)", "the base runs with the kernel's write_cache overridden against the drive", "rc0state"),
    ("red_2_devflush", "2", "the base summary has no device_flushes_per_op", "nodevflush"),
    ("red_3_clone1b_gated", "3", "the base gates clone1b", "clone1b_gated"),
    ("red_7_mutant", "7", "base run.sh forwards --mutant-nosync into a bound batch", "rc0bound"),
    ("red_7_traceclock", "7", "base run.sh forwards --trace-clock into a bound batch", "rc0bound"),
    ("red_7_dir", "7", "base run.sh forwards a second --dir, so the batch runs elsewhere", "rc0bound"),
    ("red_8_planted", "8", "base run.sh binds the 4-field planted verdict", "rc0bound"),
    ("red_9_loopwt", "9", "the base runs on a loop that reads write-through", "rc0"),
    ("red_10a_hidden_tmpfs", "10(a)", "predicted refused at base too (statfs magic)", "rc2"),
    ("red_10b_hidden_nobarrier", "10(b)", "the base runs on a nobarrier mount hidden behind a barrier one", "rc0"),
    ("red_10c_lazy", "10", "the base follows a lazily unmounted loop's backing path to a decoy", "rc0decoy"),
    ("red_11_append64", "11", "the base has no append arm >= 60 B", "unknownarm"),
    ("red_12a_deleted", "12(a)", "predicted refused at base too (the plant was missing, not the refusal)", "rc2"),
    ("red_12b_nest4", "12(b)", "the base's message says 'more than 4 loop layers'", "oldmsg"),
    ("red_12d_shim", "12(d)", "predicted refused at base too", "rc2"),
    ("red_13_symlink", "13", "the base follows a planted symlink out of D", "rc0target"),
    ("red_15_dirsync", "15", "the base runs on a dirsync ext4", "rc0"),
    ("red_15_logdev", "15", "the base runs on XFS with an external log", "rc0"),
    ("red_15_extjournal", "15", "the base runs on ext4 with an external journal", "rc0"),
    ("red_15_multidev", "15", "the base runs on a two-device btrfs", "rc0"),
    ("red_15_fields", "15", "the base summary has no per-layer data=/commit=/async commit", "noext4fields"),
    ("red_15_chattr", "15", "the base runs on a directory carrying chattr +S (per-file sync)", "rc0"),
    ("red_16_clocksource", "16", "the base runs on a non-TSC clocksource (x86 only)", "rc0state"),
    ("red_17_t3", "17", "base run.sh ignores V3_REQUIRE_T3 while the T3 rule is false", "rc0"),
]


def red(kv, leaf, arch):
    rows = []
    d = os.path.join(OUT, "red")
    if not os.path.isdir(d):
        with open(os.path.join(OUT, "red.json"), "w") as f:
            json.dump({"base": None, "rows": [], "note": "no red stage (V3_BASE unset)"}, f, indent=1)
        return
    for tag, item, claim, how in RED:
        rc = rc_of(os.path.join(d, tag + ".rc"))
        txt = rd(os.path.join(d, tag + ".txt")) or ""
        if rc is None and not os.path.exists(os.path.join(d, tag + ".na")):
            rows.append({"tag": tag, "item": item, "claim": claim, "observed": "missing", "red": None})
            continue
        if os.path.exists(os.path.join(d, tag + ".na")):
            rows.append({"tag": tag, "item": item, "claim": claim, "observed": "not applicable: " + (rd(os.path.join(d, tag + ".na")) or "").strip(), "red": None})
            continue
        sj = rj(os.path.join(d, tag + ".out", "summary.json")) or {}
        bt = rd(os.path.join(d, tag + ".out", "binary.txt")) or ""
        state = rd(os.path.join(d, tag + ".state")) or ""
        if how == "rc0":
            r = rc in (0, 3)
        elif how == "rc0state":  # the planted state was read back from sysfs while the base ran
            r = rc in (0, 3) and "changed=1" in state
        elif how == "rc0target":  # the base created the symlink's target outside D
            r = rc in (0, 3) and (rd(os.path.join(d, tag + ".target")) or "").strip() == "present"
        elif how == "rc0decoy":  # the base followed the stale backing path to the decoy
            decoy = (rd(os.path.join(d, tag + ".decoy")) or "").strip()
            fp = sj.get("flush_path") or []
            r = rc in (0, 3) and bool(decoy) and bool(fp) and fp[0].get("loop_backing") == decoy
        elif how == "rc0bound":
            r = rc in (0, 3) and "bound=fire-checked" in bt
        elif how == "rc2":
            r = rc != 2  # red would mean the base did NOT refuse
        elif how == "unknownarm":
            r = rc == 2 and "unknown arm" in txt
        elif how == "oldmsg":
            r = rc == 2 and "more than 4 loop layers" in txt
        elif how == "nodevflush":
            r = rc in (0, 3) and bool(sj) and "device_flushes_per_op" not in sj
        elif how == "clone1b_gated":
            r = bool(sj) and (sj.get("flush_control_arms") or {}).get("clone1b", {}).get("gated") is True
        elif how == "noext4fields":  # an ext4 layer exists, and none carries the fields
            fp = sj.get("flush_path") or []
            r = bool(sj) and any(l.get("fstype") == "ext4" for l in fp) and not any("ext4" in l for l in fp)
        else:
            r = None
        rows.append({"tag": tag, "item": item, "claim": claim, "rc": rc, "red": r, "text": txt[-240:]})
    off = rj(os.path.join(d, "offline.json"))
    with open(os.path.join(OUT, "red.json"), "w") as f:
        json.dump({"base": kv.get("base_sha"), "base_v3floor_sha256": kv.get("base_v3floor_sha256"), "cell": CELL,
                   "arch": arch, "leaf_class": leaf, "rows": rows, "offline": off}, f, indent=1)
    for r in rows:
        print("RED %-26s item %-6s %s: %s" % (r["tag"], r["item"], {True: "RED (bug shown at base)", False: "NOT RED",
                                                                  None: "n/a"}[r["red"]], r.get("observed", r.get("rc"))))


# ---- the second red column: the fourth review's plants against the previous tip (40a3c9502) --------------------
PREV = [  # tag, fourth-review item, what the previous tip does that the fix stops, how the outcome is read
    ("prev_M1_virt", "M1", "with cpuinfo's flag and DMI's names hidden, the previous probe says not virtualized", "novirt"),
    ("prev_M2_sdbg", "M2", "the previous probe accepts a scsi_debug RAM disk as a drive outside the fire-check", "rc0"),
    ("prev_M3_driver", "M3", "the previous gate binds a batch whose leaf driver differs from the verdict's",
     "nopostrule:leaf drive:"),
    ("prev_M4_preload", "M4", "the previous (dynamic) probe runs with a library from /etc/ld.so.preload loaded", "rc0mark"),
    ("prev_L1_claim", "L1", "the previous claim says the clean arm issued no flush when it did not run", "text:issued no"),
    ("prev_L6_datajournal", "L6", "predicted refused at the previous tip too (the rule existed, unplanted)", "rc2"),
    ("prev_L9_verdictswap", "L9", "the previous gate reads the verdict after the run without re-hashing it",
     "nopostrule:verdict: changed"),
]


def prev(kv, leaf, arch):
    d = os.path.join(OUT, "prev")
    if not os.path.isdir(d):
        with open(os.path.join(OUT, "prev.json"), "w") as f:
            json.dump({"prev": None, "rows": [], "note": "no prev stage (V3_PREV unset)"}, f, indent=1)
        return
    rows = []
    for tag, item, claim, how in PREV:
        rc = rc_of(os.path.join(d, tag + ".rc"))
        txt = rd(os.path.join(d, tag + ".txt")) or ""
        if os.path.exists(os.path.join(d, tag + ".na")):
            rows.append({"tag": tag, "item": item, "claim": claim,
                         "observed": "not applicable: " + (rd(os.path.join(d, tag + ".na")) or "").strip(), "red": None})
            continue
        if rc is None:
            rows.append({"tag": tag, "item": item, "claim": claim, "observed": "missing", "red": None})
            continue
        sj = rj(os.path.join(d, tag + ".out", "summary.json")) or {}
        if how == "novirt":
            prem = rd(os.path.join(d, tag + ".premise")) or ""
            r = rc in (0, 3) and "hypervisor_flags=0" in prem and "product_name=PowerEdge R650" in prem and \
                (sj.get("virtualization") or {}).get("virtualized") is False
        elif how == "rc0":
            r = rc in (0, 3)
        elif how.startswith("nopostrule:"):
            g = rj(os.path.join(d, tag, "gate.json"))
            r = isinstance(g, dict) and not any(str(x).startswith(how.split(":", 1)[1]) for x in g.get("refusals") or [])
        elif how == "rc0mark":
            r = rc in (0, 3) and "v3floor" in (rd(os.path.join(d, tag + ".mark")) or "")
        elif how.startswith("text:"):
            r = how.split(":", 1)[1] in txt
        elif how == "rc2":
            r = rc != 2
        else:
            r = None
        rows.append({"tag": tag, "item": item, "claim": claim, "rc": rc, "red": r, "text": txt[-240:]})
    with open(os.path.join(OUT, "prev.json"), "w") as f:
        json.dump({"prev": kv.get("prev_sha"), "prev_v3floor_sha256": kv.get("prev_v3floor_sha256"), "cell": CELL,
                   "arch": arch, "leaf_class": leaf, "rows": rows}, f, indent=1)
    for r in rows:
        print("PREV %-22s item %-4s %s: %s" % (r["tag"], r["item"], {True: "RED (bug shown at the previous tip)",
                                                                     False: "NOT RED", None: "n/a"}[r["red"]],
                                                r.get("observed", r.get("rc"))))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
