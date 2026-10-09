#!/usr/bin/env python3
"""batchgate.py -- run.sh's binding and post-run gates for one V3 batch (review 2 items 1, 2, 7, 8, 16, 17).

  batchgate.py verdict VERDICT CELL SHA ARCH FSTYPE   may this fire-check verdict bind a batch? (JSON; exit 0 yes, 2 no)
  batchgate.py t3pre                                  the registered T3 preconditions on this box (exit 0 met, 2 not)
  batchgate.py post OUT CELL SHA MODE [VERDICT]       after the probe: refusals, the device flush merge, the flush gates
                                                      and the claim the counts allow (OUT/gate.json; exit 0 ok, 2 refused,
                                                      3 void). MODE is bound or smoke; a bound batch names its verdict
  batchgate.py flushgate SUMMARY STAMP_END            the diskstats leaf flush gate alone (JSON)
  batchgate.py drift START_OUT END_OUT                a batch's start and end V3 p50 drift on the 25 B and frame arms
                                                      (PREREG section 4: > 60 us voids; exit 0 pass, 3 void, 2 refused)
  batchgate.py leafclass SUMMARY                      wb | wt | brd for a probe summary (exit 2 if it has none)
  batchgate.py fixture OUT CELL ARCH LEAF SHA FSTYPE BOX [MOD]   a PLANTED full-shape verdict for firecheck.sh's F4 plants
                                                      (BOX: virt=vm|bare,flip=yes|no, as check.py --box reads it),
                                                      always "planted": true (MOD: allfail | fail-one | drop-one |
                                                      cell=<c> | harness | bindfail: also a failed binding record |
                                                      pending: also a pending one)
  batchgate.py self-test DATA                         the gates on banked and planted inputs; exit 0 iff all as expected

Binding (item 8): a verdict binds only if it has check.py's whole shape -- a "checks" list whose ids equal
check.plan(cell, arch, leaf class, box), every check passing, pass == total, all_pass true -- for this cell, binary sha256,
arch and fstype, with "unplanted_refusals" listed, a leaf class that is not brd, no "planted" key, and the sha256 of
every fire-check harness file equal to this run.sh's own copies (a verdict vouches only for the checker that wrote
it), and the fire-check's own binding record next to it (<verdict>.bind.json, check.py --bind) passed for this very
verdict; a pending record (<verdict>.bind.pending, written with the verdict) binds only firecheck.sh's own bind step,
which names the verdict's sha256 in V3_BIND_PENDING_SHA (fourth review L3, fifth review L1). Each
refusal reason starts with its rule's own name. Its sha256 and run id go into binary.txt. Threat model, stated: this
stops a wrong, failed, truncated, foreign, stale or planted verdict; it does not stop a forged one.

Post-run (item 7 and the fresh reviews): refused if the summary carries mutant_nosync or trace_clock != 0, names
another binary, another layout, has no drive or brd leaf record, no gated arm ran, or (bound) a brd leaf, another
leaf class, another layer stack, another leaf driver or drive model, or another virtualization verdict than the
verdict's F3 batch (fourth review M3), or the verdict file no longer hashes to what binary.txt bound (L9).
Annex A16 on a write-through leaf: refused when the drive's own report and the kernel's write cache disagree, or
when the leaf's flush counter moved (a write-through drive receives no flush); on every leaf, VOID when a
flush-gated op's window holds no fsync/fdatasync by the probe's pid (blkflush's syscall tracepoints). A plp
declaration other than the verdict's refuses (A14); in rental mode (V3_REQUIRE_T3=1) a provisional d0 threshold
or an unregistered frame arm refuses (A17). Every write-back layer, not only the leaf, must show a flush-carrying
request in every flush-gated window (gate-6 MED 3). A flush gate that cannot determine refuses (LOW 9).
Flush gate (item 1): on a write-back leaf, the leaf's FLUSH requests completed in the batch window (diskstats fields
19-20, from stamp.py) must be >= n x the gated arms run, else the batch is VOID (rc 3). It is a lower bound: other
processes' flushes pad it. A write-through leaf is labelled "not applicable: no volatile cache: no drive flush" (the
batch is labelled, not voided, and can never back a drive-flush sentence); a brd leaf, "not applicable: brd". On a
write-back leaf the blkflush record must also show every gated op's window holding a flush-carrying request (flush,
preflush or preflush+fua; a FUA-only write flushes nothing else) at the leaf. The claim the counts allow is written
as floor_claim_from_counts: "clean fsync (a bare flush ...) plus the measured delta" only on ext4/XFS where a bare
flush request (REQ_OP_FLUSH) reached the leaf in >= 95% of the clean arm's windows, each window counted once (review 2
item 2; fourth review L9); else no bare-flush baseline, saying so when the clean arm did not run (L1). On a leaf that
is virtual, or not shown bare metal, the claim carries that qualifier on both leaf classes (L2).
"""
import copy as _copy, glob, hashlib, json, os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import v3cell  # noqa: E402

GOVERNOR_T3 = "performance"
CLOCKSOURCES = ["tsc", "arch_sys_counter"]


def load(p):
    with open(p) as f:
        return json.load(f)


def plan(cell, arch, leaf, box):
    import check  # check.py is importable: its work runs only under __main__
    return check.plan(cell, arch, leaf, box)


BOX_VALUES = {"virt": ("vm", "bare"), "flip": ("yes", "no"), "plp": ("yes", "no")}


def box_problems(box):
    """the verdict's box (sixth review H1: the plan depends on the box the fire-check ran on)."""
    if not isinstance(box, dict) or any(box.get(k) not in vals for k, vals in BOX_VALUES.items()):
        return ["box: %r is not {virt: vm|bare, flip: yes|no, plp: yes|no}" % (box,)]
    return []


def parse_box(spec):
    """'virt=vm,flip=yes,plp=no' -> dict (firecheck.sh's fixture argument)."""
    try:
        return dict(kv.split("=", 1) for kv in spec.split(","))
    except ValueError:
        return {}


def gated_list():
    import check
    return check.GATED


def harness():
    import check
    return check.harness_sha256(HERE)


def leaf_class(sj):
    """wb | wt | brd, or None. A leaf record whose kind is not drive or brd (scsi_debug, unknown) has no class. A
    summary with no leaf record at all is a pre-review-2 one (the self-test's banked cells): classed by its
    write_cache; post() refuses a batch without a leaf record."""
    lf = sj.get("leaf")
    if isinstance(lf, dict):
        if lf.get("kind") == "brd":
            return "brd"
        if lf.get("kind") != "drive":
            return None
    elif lf is not None:
        return None
    wc = sj.get("leaf_write_cache")
    return "wb" if wc == "write back" else "wt" if wc == "write through" else None


def bind_path(verdict_path):
    """The fire-check's own binding record for a verdict (check.py --bind): <verdict minus .json>.bind.json, and
    .bind.pending between check.py writing the verdict and the binding check finishing."""
    b = verdict_path[:-5] if verdict_path.endswith(".json") else verdict_path
    return b + ".bind.json", b + ".bind.pending"


def bind_problems(verdict_path, vsha, pending_sha=None):
    """Fourth review L3: a verdict binds only if the fire-check's own binding check (run.sh bound to this verdict,
    check.py --bind) passed for THIS verdict. Between check.py writing the verdict (and a pending record for it) and
    check.py --bind replacing that record with the result, only firecheck.sh's own bind step may bind it: it names the
    verdict's sha256 in V3_BIND_PENDING_SHA (fifth review L1: a fire-check that dies in that window leaves a pending
    record that binds nothing). A failed, foreign or missing record refuses."""
    if pending_sha is None:
        pending_sha = os.environ.get("V3_BIND_PENDING_SHA", "")
    bj, bp = bind_path(verdict_path)
    if os.path.exists(bj):
        try:
            b = load(bj)
        except (OSError, ValueError) as e:
            return ["bind: the binding record %s is unreadable: %r" % (bj, e)]
        if not isinstance(b, dict) or b.get("verdict_sha256") != vsha:
            return ["bind: the binding record %s is for verdict %r, not this one (%s)" %
                    (bj, b.get("verdict_sha256") if isinstance(b, dict) else None, vsha)]
        chks = b.get("checks") if isinstance(b.get("checks"), list) else []
        if b.get("all_pass") is not True or not chks or not all(isinstance(c, dict) and c.get("pass") is True for c in chks):
            return ["bind: the fire-check's own binding check failed for this verdict (%s)" % bj]
        return []
    if os.path.exists(bp):
        try:
            pend = load(bp)
        except (OSError, ValueError) as e:
            return ["bind: the pending record %s is unreadable: %r" % (bp, e)]
        if not isinstance(pend, dict) or pend.get("verdict_sha256") != vsha:
            return ["bind: the pending record %s is for another verdict" % bp]
        if pending_sha != vsha:
            return ["bind: pending: the fire-check's own binding check has not finished for this verdict (%s); only "
                    "firecheck.sh's bind step (V3_BIND_PENDING_SHA naming it) may bind it before it has" % bp]
        return []
    return ["bind: no binding record next to the verdict (%s): the fire-check's own binding check never ran for it" % bj]


def verdict_problems(v, cell, sha, arch, fstype):
    bad = []
    if not isinstance(v, dict):
        return ["not a JSON object"]
    if "planted" in v:
        bad.append("planted: a fixture verdict carries 'planted' and never binds")
    checks = v.get("checks")
    if not isinstance(checks, list) or not checks or not all(isinstance(c, dict) and "id" in c and "pass" in c for c in checks):
        bad.append("checks: no list of {id, pass} checks (not check.py's shape)")
        checks = []
    npass = sum(1 for c in checks if c.get("pass") is True)
    if v.get("all_pass") is not True:
        bad.append("all_pass is not true")
    if v.get("pass") != npass or v.get("total") != len(checks) or npass != len(checks):
        bad.append("pass/total: pass %r, total %r, but %d of %d checks pass" % (v.get("pass"), v.get("total"), npass, len(checks)))
    if v.get("cell") != cell:
        bad.append("cell: the verdict is for %r, the batch is %r" % (v.get("cell"), cell))
    if v.get("v3floor_sha256") != sha:
        bad.append("v3floor_sha256: the verdict is for %r, the binary is %s" % (v.get("v3floor_sha256"), sha))
    if v.get("arch") != arch:
        bad.append("arch: the verdict is for %r, this is %s" % (v.get("arch"), arch))
    if v.get("fstype") != fstype or (cell in v3cell.CELLS and v3cell.kind(cell) != fstype):
        bad.append("fstype: the verdict is for %r, DIR is on %s, cell %s" % (v.get("fstype"), fstype, cell))
    if not isinstance(v.get("unplanted_refusals"), list):
        bad.append("unplanted_refusals: absent")
    mine = harness()
    theirs = v.get("harness_sha256") if isinstance(v.get("harness_sha256"), dict) else {}
    diff = sorted(f for f in set(mine) | set(theirs) if theirs.get(f) != mine.get(f) or mine.get(f) is None)
    if diff:  # a verdict vouches only for the checker that wrote it (fresh review I-M2)
        bad.append("harness: the verdict was made by a different fire-check harness than this run.sh's (%s differ)" %
                   ", ".join(diff[:6]))
    lc = v.get("leaf_class")
    bad += box_problems(v.get("box"))
    if lc not in ("wb", "wt", "brd"):
        bad.append("leaf_class: %r" % lc)
    elif lc == "brd":
        bad.append("leaf_class brd: a brd fire-check is fire-check only and never binds a batch")
    # the verdict's ids against the plan for this cell and the verdict's own arch and leaf, so that a wrong arch or
    # leaf is refused by its own rule above and a truncated verdict by this one
    if cell in v3cell.CELLS and lc in ("wb", "wt") and checks and not box_problems(v.get("box")):
        va = v.get("arch") if isinstance(v.get("arch"), str) else arch
        want = plan(cell, va, lc, v["box"])
        ids = [c.get("id") for c in checks]
        if ids != want:
            miss = [i for i in want if i not in ids]
            extra = [i for i in ids if i not in want]
            bad.append("plan: the check ids differ from check.py's plan for %s/%s/%s (%d of %d; missing %s; extra %s)" %
                       (cell, va, lc, len(ids), len(want), miss[:3], extra[:3]))
    return bad


def cmd_verdict(p, cell, sha, arch, fstype):
    try:
        raw = open(p, "rb").read()
        v = json.loads(raw)
    except (OSError, ValueError) as e:
        print(json.dumps({"ok": False, "reasons": ["unreadable verdict: %r" % e]}))
        return 2
    vsha = hashlib.sha256(raw).hexdigest()
    bad = verdict_problems(v, cell, sha, arch, fstype) + bind_problems(p, vsha)
    basis = "binding record" if os.path.exists(bind_path(p)[0]) else "pending record, bind step" if not bad else None
    print(json.dumps({"ok": not bad, "reasons": bad, "verdict_sha256": vsha, "bind_basis": basis,
                      "run_id": v.get("run_id") if isinstance(v, dict) else None,
                      "leaf_class": v.get("leaf_class") if isinstance(v, dict) else None}))
    return 0 if not bad else 2


def t3_state():
    st = {"governors": {}, "cur_freq_khz": {}, "clocksource": None}
    for c in sorted(glob.glob("/sys/devices/system/cpu/cpu[0-9]*")):
        g = os.path.join(c, "cpufreq", "scaling_governor")
        try:
            st["governors"][os.path.basename(c)] = open(g).read().strip()
        except OSError:
            st["governors"][os.path.basename(c)] = None
    try:
        st["clocksource"] = open("/sys/devices/system/clocksource/clocksource0/current_clocksource").read().strip()
    except OSError:
        pass
    return st


def t3_problems(st):
    bad = []
    gov = st.get("governors") or {}
    if not gov:
        bad.append("governor: no CPU found in sysfs")
    missing = sorted(c for c, g in gov.items() if g is None)
    if missing:
        bad.append("governor: %d CPUs have no cpufreq (%s...): the registered T3 rule needs scaling_governor = %s on "
                   "every CPU, and a box that does not expose it cannot show it" % (len(missing), missing[0], GOVERNOR_T3))
    other = sorted("%s=%s" % (c, g) for c, g in gov.items() if g is not None and g != GOVERNOR_T3)
    if other:
        bad.append("governor: %s, not %s" % (", ".join(other[:4]), GOVERNOR_T3))
    if st.get("clocksource") not in CLOCKSOURCES:
        bad.append("clocksource: %r, not tsc or arch_sys_counter" % st.get("clocksource"))
    return bad


def cmd_t3pre():
    st = t3_state()
    bad = t3_problems(st)
    print(json.dumps({"ok": not bad, "reasons": bad, "state": st}))
    return 0 if not bad else 2


def flush_gate(sj, st1):
    """-> dict with outcome pass | FAIL | not applicable: ...; inputs read by hand from the batch's own record."""
    lc = leaf_class(sj)
    fp = sj.get("flush_path") or []
    leaf = os.path.basename(fp[-1].get("sys", "")) if fp else None
    n = sj.get("n")
    gated = sorted(a for a, r in (sj.get("flush_control_arms") or {}).items() if isinstance(r, dict) and r.get("gated"))
    need = n * len(gated) if isinstance(n, int) else None
    delta = ((st1 or {}).get("diskstats_delta") or {}).get(leaf or "", {})
    got = delta.get("flushes")
    g = {"leaf": leaf, "leaf_class": lc, "n": n, "gated_arms_run": gated, "required_flushes": need,
         "leaf_flushes_completed": got, "instrument": "/proc/diskstats fields 19-20 (flush requests completed), "
         "stamp_end - stamp_start; a lower bound (other processes' flushes pad it)"}
    if lc == "brd":
        g["outcome"] = "not applicable: brd"
    elif lc == "wt":
        g["outcome"] = "not applicable: no volatile cache: no drive flush"
    elif not gated:  # n x 0 = 0 would pass anything (fresh reviews P-L1, I-M1)
        g["outcome"] = "FAIL"
        g["why"] = "no gated arm ran, so nothing here can show a flush reaching the leaf"
    elif lc != "wb" or need is None or got is None:
        g["outcome"] = "FAIL"
        g["why"] = "cannot determine (leaf class %r, required %r, completed %r)" % (lc, need, got)
    elif got >= need:
        g["outcome"] = "pass"
    else:
        g["outcome"] = "FAIL"
        g["why"] = "%d flushes completed on write-back leaf %s, fewer than n x gated arms = %d" % (got, leaf, need)
    return g


BARE_MIN_FRAC = 0.95  # a bare-flush baseline needs a bare flush request in at least this share of clean's windows


def virt_qualifier(sj, lc):
    """Fourth review L2: the claim carries the VM qualifier on both leaf classes (a virtual drive's flush and its
    "no cache" are both the hypervisor's)."""
    vz = (sj.get("virtualization") or {}).get("virtualized")
    if vz is False:
        return ""
    what = "a virtual drive" if vz is True else "virtualization not ruled out"
    return ("; %s: reach to media unknown" % what) if lc == "wb" else (" (%s: the host's caching is unknown)" % what)


def clean_bare(arms, leaf_names):
    """The clean arm's bare flush requests (REQ_OP_FLUSH, rwbs F) at the leaf: {ops, windows_with_bare_flush}, or
    None when the clean arm did not run. A window counts once whatever else it holds (fourth review L9: mean events
    per op let one window's two flushes stand in for another's none, and FUA or preflush writes are not bare)."""
    c = (arms or {}).get("clean")
    if not c or not c.get("ops"):
        return None
    zero = min([(c.get("devices") or {}).get(x, {}).get("bare_flush_zero_windows", c["ops"]) for x in leaf_names] or [c["ops"]])
    return {"ops": c["ops"], "windows_with_bare_flush": c["ops"] - zero}


def claim_from_counts(sj, clean):
    lc = leaf_class(sj)
    if lc == "brd":
        return "none: a brd floor backs no sentence"
    q = virt_qualifier(sj, lc)
    if lc != "wb":
        return "no drive flush: the leaf reports write-through and receives no flush" + q
    if clean is None:
        return ("no bare-flush baseline: the clean arm did not run or has no device flush record on %s; quote the "
                "per-arm device flush counts only%s" % (sj.get("fstype"), q))
    k, n = clean["windows_with_bare_flush"], clean["ops"]
    if n and k >= BARE_MIN_FRAC * n and sj.get("fstype") in ("ext4", "xfs"):
        return ("clean fsync (a bare flush: a bare flush request at the leaf in %d of %d clean windows) plus the measured "
                "delta, per stack%s" % (k, n, q))
    return ("no bare-flush baseline: a bare flush request at the leaf in %d of %d clean windows on %s; quote the per-arm "
            "device flush counts only%s" % (k, n, sj.get("fstype"), q))


def post(out, cell, sha, mode, verdict_path):
    pj = os.path.join(out, "summary.json")
    probe = os.path.join(out, "summary.probe.json")
    refusals = []
    try:
        if not os.path.exists(probe):
            os.rename(pj, probe)
        sj = load(probe)
    except (OSError, ValueError) as e:
        print("run.sh: REFUSED after the run: no probe summary in %s: %r" % (out, e), file=sys.stderr)
        return 2
    if int(sj.get("mutant_nosync", -1)) != 0:
        refusals.append("mutant_nosync=%r in the summary: a mutant batch never passes run.sh" % sj.get("mutant_nosync"))
    if int(sj.get("trace_clock", -1)) != 0:
        refusals.append("trace_clock=%r in the summary" % sj.get("trace_clock"))
    if sj.get("exe_sha256") != sha:
        refusals.append("exe_sha256: the probe that ran hashed itself as %r, run.sh hashed %s" % (sj.get("exe_sha256"), sha))
    lp = v3cell.layout_problems(cell, sj.get("fstype"), sj.get("mount_source"), sj.get("flush_path"))
    if lp:
        refusals.append("layout: %s" % "; ".join(lp))
    gated = [a for a in gated_list() if a in (sj.get("arms") or {})]
    if not gated:
        refusals.append("no gated arm ran: a batch must time at least one of %s" % ", ".join(gated_list()))
    lc = leaf_class(sj)
    if not isinstance(sj.get("leaf"), dict) or sj["leaf"].get("kind") not in ("drive", "brd"):
        refusals.append("leaf: the summary's leaf record is %r, not a drive or brd" % (sj.get("leaf") or {}).get("kind"))
    lfr = sj.get("leaf") or {}
    if lc == "wt" and not (lfr.get("drive_reports") == lfr.get("write_cache") == sj.get("leaf_write_cache") == "write through"):
        # A16: a write-through leaf's state is the drive's own report cross-checked with the kernel's
        refusals.append("drive report: the write-through leaf's drive reports %r, its queue/write_cache %r (A16: a "
                        "mismatch refuses)" % (lfr.get("drive_reports"), lfr.get("write_cache")))
    if sj.get("plp") not in ("yes", "no"):
        refusals.append("plp: the batch declares %r, not yes or no (run.sh passes V3_PLP)" % (sj.get("plp"),))
    if sj.get("traced") is not False:  # a tracer's stops change every timing (eighth review H2)
        refusals.append("traced: the probe ran under a tracer (or did not record whether it did): %r" % (sj.get("traced"),))
    if os.environ.get("V3_REQUIRE_T3") == "1":  # rental mode: the registered values must exist (A17, MED 5)
        # ... and a real run is on a drive: no loop layer, no brd leaf (A16; eighth review M6)
        if v3cell.is_loop(cell) or len(sj.get("flush_path") or []) != 1 or lc == "brd":
            refusals.append("rental: a real run is on a drive: cell %s, %d layer(s), leaf class %r (A16: ram and loop "
                            "devices are dry-run only)" % (cell, len(sj.get("flush_path") or []), lc))
        tc = str(sj.get("timing_control", ""))
        if not tc.startswith("not applicable") and not sj.get("d0_threshold_ref"):
            refusals.append("registration: no registered d0 threshold for %s (A17: rental mode refuses a provisional "
                            "one)" % sj.get("d0_threshold_key"))
        if not sj.get("frame_arm"):
            refusals.append("registration: no registered frame arm (PREREG section 4: fixed in the Registration annex)")
        elif sj.get("frame_arm") != "ow4k":  # A18 needs the frame arm's fdatasync variant; only ow4k has one (M6)
            refusals.append("registration: the registered frame arm %s has no fdatasync variant arm in this probe (A18 "
                            "needs one; only ow4k has one, fdatasync4k)" % sj.get("frame_arm"))
    if mode == "bound":
        v = {}
        try:
            raw = open(verdict_path, "rb").read() if verdict_path else b"{}"
            v = json.loads(raw)
            if not isinstance(v, dict):
                raise ValueError("not a JSON object")
            # the verdict run.sh bound before the run is the one read now (fourth review L9)
            with open(os.path.join(out, "binary.txt")) as f:
                bt = f.read()
            want = [l.split("=", 1)[1] for l in bt.splitlines() if l.startswith("verdict_sha256=")]
            now = hashlib.sha256(raw).hexdigest()
            if want != [now]:
                refusals.append("verdict: changed during the run (binary.txt names %s, the file now hashes to %s)" % (want, now))
        except (OSError, ValueError) as e:
            refusals.append("verdict: unreadable after the run: %r" % e)
            v = {}
        if lc == "brd":
            refusals.append("leaf brd: fire-check only, never a bound batch")
        if lc != v.get("leaf_class"):
            refusals.append("leaf class: the batch's leaf is %r, the verdict's %r" % (lc, v.get("leaf_class")))
        # the verdict's F3 keeps its flush path as (mount, fstype, source, disk, write_cache) per layer; an ext4loop
        # verdict over a root ext4 must not bind a batch on a loop backed by XFS (fresh review I-L9)
        vfs = [t[1] for t in ((v.get("F3") or {}).get("flush_path") or []) if isinstance(t, (list, tuple)) and len(t) > 1]
        bfs = [l.get("fstype") for l in sj.get("flush_path") or []]
        if bfs != vfs:
            refusals.append("stack: the batch's layer filesystems %s differ from the verdict's %s" % (bfs, vfs))
        # the drive the verdict fire-checked: an NVMe verdict never binds an sd or virtio_blk batch, nor one drive
        # model another (fourth review M3)
        vl = (v.get("F3") or {}).get("leaf") or {}
        bl = sj.get("leaf") or {}
        if (bl.get("driver"), bl.get("model")) != (vl.get("driver"), vl.get("model")):
            refusals.append("leaf drive: the batch's leaf is driver %r model %r, the verdict's driver %r model %r" %
                            (bl.get("driver"), bl.get("model"), vl.get("driver"), vl.get("model")))
        vv = ((v.get("F3") or {}).get("virtualization") or {}).get("virtualized", "absent")
        bv = (sj.get("virtualization") or {}).get("virtualized", "absent")
        if bv != vv or bv == "absent":
            refusals.append("virtualization: the batch's virtualized is %r, the verdict's %r" % (bv, vv))
        vp = (v.get("box") or {}).get("plp")
        if sj.get("plp") != vp:
            refusals.append("plp: the batch declares %r, the verdict's fire-check ran with %r" % (sj.get("plp"), vp))
    st1 = None
    try:
        st1 = load(os.path.join(out, "stamp_end.json"))
    except (OSError, ValueError):
        refusals.append("no stamp_end.json")
    g = flush_gate(sj, st1)
    voids = []
    if g["outcome"] == "FAIL" and str(g.get("why", "")).startswith("cannot determine"):
        refusals.append("flush gate: %s" % g["why"])  # unknown is a refusal, never a computed void (gate-6 LOW 9)
        g["outcome"] = "REFUSED: cannot determine"
    elif g["outcome"] == "FAIL":
        voids.append("flush gate: %s" % g.get("why"))
    if lc == "wt":  # A16: the kernel strips every flush to a write-through drive, so its counter must not move
        got = g.get("leaf_flushes_completed")
        if not isinstance(got, int):
            refusals.append("write-through leaf: its flush counter cannot be read (%r)" % (got,))
        elif got:
            refusals.append("write-through leaf: %s's flush counter rose by %d during the batch (A16: a write-through "
                            "drive receives no flush; a count contradicts the declared state)" % (g.get("leaf"), got))
    rep = None
    try:
        rep = load(os.path.join(out, "blkflush", "report.json"))
    except (OSError, ValueError):
        refusals.append("no blkflush report")
    if rep is not None and rep.get("refused"):
        refusals.append("blkflush refused: %s" % rep.get("refused"))
        rep = None
    merged = dict(sj)
    fp = sj.get("flush_path") or []
    leafinfo = sj.get("leaf") or {}
    leaf_names = ([fp[-1].get("disk")] if fp else []) + [p.get("disk") for p in leafinfo.get("multipath") or []]
    per_layer, per_leaf, bk = {}, {}, {"outcome": "not applicable", "arms": {}}
    if rep is not None:
        arms = (rep.get("windows") or {}).get("arms") or {}
        for l in fp:
            d = l.get("disk")
            per_layer[d] = {a: (arms.get(a, {}).get("devices") or {}).get(d, {}).get("per_op", 0.0) for a in arms}
        for a in arms:
            per_leaf[a] = round(sum((arms[a].get("devices") or {}).get(x, {}).get("events", 0) for x in leaf_names)
                                / max(1, arms[a].get("ops", 1)), 4)
        # every write-back layer of the flush path, the leaf and each loop above it (gate-6 review MED 3): every
        # flush-gated op's window holds a flush-carrying request there (a FUA-only write flushes nothing else)
        wbl = [(k, [l.get("disk")] + ([p.get("disk") for p in leafinfo.get("multipath") or []] if k == len(fp) - 1 else []))
               for k, l in enumerate(fp) if l.get("write_cache") == "write back"
               and not (k == len(fp) - 1 and leafinfo.get("kind") == "brd")]
        if wbl:
            bk["outcome"] = "pass" if gated else "FAIL"
            for a in gated:
                ops = arms.get(a, {}).get("ops", 0)
                rec_a = {}
                for k, names in wbl:
                    zero = min([(arms.get(a, {}).get("devices") or {}).get(x, {}).get("flush_carrying_zero_windows", ops)
                                for x in names] or [ops])
                    rec_a["layer %d (%s)" % (k, names[0])] = zero
                    if zero or not ops:
                        bk["outcome"] = "FAIL"
                        voids.append("flush-carrying: %s's windows without a flush-carrying request on layer %d (%s): %d"
                                     % (a, k, names[0], zero))
                bk["arms"][a] = {"windows_without_a_flush_carrying_request": rec_a}
        # the app's own syncs (A16): every flush-gated op's window holds an fsync or fdatasync by the probe's pid
        sy = (rep.get("syscalls") or {})
        sarms = sy.get("arms")
        if not isinstance(sarms, dict) or sy.get("pid") != sj.get("pid"):
            refusals.append("fsync: cannot determine the per-window sync count (the report has %s for pid %r, the probe "
                            "ran as %r)" % ("no per-arm sync record" if not isinstance(sarms, dict) else "a record",
                                            sy.get("pid"), sj.get("pid")))
        else:
            for a in gated:
                r = sarms.get(a) or {}
                if r.get("ops") != (arms.get(a) or {}).get("ops") or r.get("windows_without_a_sync") != 0:
                    voids.append("fsync: %s's windows without an fsync or fdatasync by the probe: %r of %r" %
                                 (a, r.get("windows_without_a_sync"), r.get("ops")))
                elif r.get("windows_short") != 0:  # attributed by fd: each window holds all its own (tenth review H1)
                    voids.append("fsync: %s's windows short of their own syncs (by fd): %r of %r" %
                                 (a, r.get("windows_short"), r.get("ops")))
            # eleventh review MED 1, V3 review 12 item 6: every sync of the probe's that no overlapping window's arm
            # owns (foreign_fd: nosync25's, wholly inside a window or at its edge) or that names no fd (no_fd) VOIDs
            # the batch; a missing count is not a zero. (The nosync25-only lookup that stood here VOIDed any smoke
            # batch without nosync25 as "None sync(s)"; the "wholly inside" counts stay descriptive.)
            # V3 review 12 item 7: check.py's nosync25 rule, here too: nosync25 syncs nothing by definition, so its
            # windows hold no sync attributed to it (a nosync25 that owns an fd and syncs on it shows here only: every
            # unattributed count is then 0)
            n0 = sarms.get("nosync25")
            if isinstance(n0, dict) and n0.get("syncs") != 0:
                voids.append("fsync: nosync25's windows hold %r sync(s) by the probe (it owns no fd by definition)"
                             % (n0.get("syncs"),))
            ua = sy.get("unattributed") if isinstance(sy.get("unattributed"), dict) else {}
            if ua.get("foreign_fd") != 0 or ua.get("no_fd") != 0:
                voids.append("fsync: %r sync(s) by the probe on an fd no overlapping window's arm owns (foreign_fd), %r "
                             "naming no fd (no_fd)" % (ua.get("foreign_fd"), ua.get("no_fd")))
            # review 11 LOW 2 / review 12 item 7: the probe's own sync_fds against the arm definitions (check.OP), the
            # same half of check.py's sync_fds_problems; post trusted the record attribution reads by
            import check as _ck
            for t in _ck.sync_fds_def_problems(sj, sorted(sj.get("arms") or {}), sj.get("n")):
                if t[1] == "missing or overflowed":
                    voids.append("fsync: %s: missing or overflowed (sync_fds_overflow %r)" % (t[0], t[2]))
                else:
                    voids.append("fsync: %s: %s recorded %r, defined %d fd(s) and %d sync(s)" % t)
            merged["app_syncs_per_op"] = {a: round(r.get("syncs", 0) / max(1, r.get("ops", 1)), 4) for a, r in sarms.items()}
        merged["device_flushes"] = {"instrument": "blkflush.py (tracefs block:block_rq_issue, rwbs with F: flush requests "
                                    "and FUA writes, inside each op's CLOCK_MONOTONIC_RAW window)",
                                    "proves": rep.get("proves"), "leaf_devices": leaf_names, "devices": rep.get("devices"),
                                    "ambiguous": (rep.get("windows") or {}).get("ambiguous"),
                                    "outside": (rep.get("windows") or {}).get("outside"),
                                    "outside_by_device": (rep.get("windows") or {}).get("outside_by_device")}
    else:
        merged["device_flushes"] = None
    merged["device_flushes_per_op"] = per_leaf if rep is not None else None
    merged["layer_device_flushes_per_op"] = per_layer if rep is not None else None
    merged["floor_claim_from_counts"] = claim_from_counts(
        sj, clean_bare((rep.get("windows") or {}).get("arms"), leaf_names) if rep is not None else None)
    g["blkflush_leaf_gate"] = bk
    g["voids"] = voids
    merged["flush_gate"] = g
    void = bool(voids)
    rc = 2 if refusals else 3 if void else 0
    gate = {"mode": mode, "cell": cell, "verdict": verdict_path, "refusals": refusals, "flush_gate": g, "rc": rc,
            "voids": voids, "timing_control": sj.get("timing_control"), "d0_control": sj.get("d0_control")}
    with open(os.path.join(out, "gate.json"), "w") as f:
        json.dump(gate, f, indent=1, sort_keys=True)
    with open(pj, "w") as f:
        json.dump(merged, f, indent=1, sort_keys=True)
    for r in refusals:
        print("run.sh: REFUSED after the run: %s" % r, file=sys.stderr)
    if void and not refusals:
        for v_ in voids:
            print("run.sh: VOID: %s" % v_, file=sys.stderr)
    return rc


V3_DRIFT_US = 60.0  # PREREG section 4: start and end V3 p50 differing by more than this voids the batch


def drift(start_out, end_out):
    """A batch's start and end V3 (two run.sh OUTs): p50 drift on the 25 B arm and the frame arm (gate-6 MED 6)."""
    try:
        a, b = load(os.path.join(start_out, "summary.json")), load(os.path.join(end_out, "summary.json"))
    except (OSError, ValueError) as e:
        print(json.dumps({"outcome": "REFUSED", "why": "unreadable: %r" % e}))
        return 2
    why = []
    if os.path.realpath(start_out) == os.path.realpath(end_out):  # ninth review L11
        why.append("the start and the end are the same OUT (%s)" % os.path.realpath(start_out))
    if a.get("frame_arm") != b.get("frame_arm"):
        why.append("the frame arm differs: %r then %r" % (a.get("frame_arm"), b.get("frame_arm")))
    # the same kind of batch at both ends, each passed by its own gate (eighth review L3)
    for k in ("exe_sha256", "plp", "fstype", "mount_source", "leaf_write_cache", "n"):
        if a.get(k) != b.get(k):
            why.append("%s differs: %r then %r" % (k, a.get(k), b.get(k)))
    if (a.get("leaf") or {}).get("disk") != (b.get("leaf") or {}).get("disk"):
        why.append("the leaf disk differs")
    for side, d in (("start", start_out), ("end", end_out)):
        try:
            g = load(os.path.join(d, "gate.json"))
            bt = open(os.path.join(d, "binary.txt")).read()
        except (OSError, ValueError) as e:
            why.append("%s: no gate.json or binary.txt: %r" % (side, e))
            continue
        if g.get("rc") != 0:
            why.append("%s: its own gate gave rc %r" % (side, g.get("rc")))
        if [l for l in bt.splitlines() if l.startswith("cell=")] != ["cell=%s" % (g.get("cell"),)]:  # exact (ninth review L11)
            why.append("%s: binary.txt does not name exactly the gate's cell %r" % (side, g.get("cell")))
    # the same binding at both ends (ninth review L11): cell, shape, what it is bound to, the verdict, rental mode
    keys = ("cell=", "shape=", "bound=", "verdict_sha256=", "rental=")
    try:
        sa = [l for l in open(os.path.join(start_out, "binary.txt")).read().splitlines() if l.startswith(keys)]
        sb = [l for l in open(os.path.join(end_out, "binary.txt")).read().splitlines() if l.startswith(keys)]
        if sa != sb:
            why.append("cell, shape, binding, verdict or rental mode differs: %r then %r" % (sa, sb))
    except OSError as e:
        why.append("binary.txt unreadable: %r" % e)
    arms = ["append25"] + ([a["frame_arm"]] if a.get("frame_arm") else [])
    rec, void = {}, []
    for arm in arms:
        x, y = (a.get("arms") or {}).get(arm, {}).get("p50_us"), (b.get("arms") or {}).get(arm, {}).get("p50_us")
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            why.append("%s: no p50 in both (%r, %r)" % (arm, x, y))
            continue
        rec[arm] = {"start_p50_us": x, "end_p50_us": y, "drift_us": round(y - x, 1)}
        if abs(y - x) > V3_DRIFT_US:
            void.append("%s drifted %.1f us (> %.0f)" % (arm, y - x, V3_DRIFT_US))
    out = {"rule": "PREREG section 4: start and end V3 p50 differ by > %.0f us on either arm -> void" % V3_DRIFT_US,
           "arms": rec, "frame_arm": a.get("frame_arm")}
    if why:
        out.update(outcome="REFUSED", why=why)
        print(json.dumps(out))
        return 2
    out["outcome"] = "VOID" if void else "pass"
    out["voids"] = void
    print(json.dumps(out))
    return 3 if void else 0


def fixture(out, cell, arch, leaf, sha, fstype, boxspec, mod):
    box = parse_box(boxspec)
    if box_problems(box):
        raise SystemExit("batchgate fixture: BOX %r is not virt=vm|bare,flip=yes|no,plp=yes|no" % boxspec)
    ids = plan(cell, arch, leaf, box)
    checks = [{"id": i, "check": i, "pass": True, "detail": "planted"} for i in ids]
    v = {"cell": cell, "fstype": fstype, "arch": arch, "leaf_class": leaf, "v3floor_sha256": sha, "run_id": "fixture",
         "box": {"virt": box["virt"], "flip": box["flip"], "plp": box["plp"]},
         "pass": len(checks), "total": len(checks), "all_pass": True, "unplanted_refusals": [],
         "harness_sha256": harness(), "checks": checks, "planted": True}
    if mod == "allfail":
        v["all_pass"] = False
    elif mod == "fail-one":
        checks[len(checks) // 2]["pass"] = False
    elif mod == "drop-one":
        del checks[len(checks) // 2]
        v["pass"] = v["total"] = len(checks)
    elif mod.startswith("cell="):
        v["cell"] = mod[5:]
    elif mod == "harness":
        v["harness_sha256"] = dict(v["harness_sha256"], **{"check.py": "0" * 64})
    elif mod and mod not in ("bindfail", "pending"):
        raise SystemExit("batchgate fixture: unknown MOD %r" % mod)
    with open(out, "w") as f:
        json.dump(v, f, indent=1)
    if mod == "pending":  # a pending record for THIS fixture, outside the bind step (fifth review L1)
        with open(out, "rb") as f:
            vs = hashlib.sha256(f.read()).hexdigest()
        with open(bind_path(out)[1], "w") as f:
            json.dump({"verdict_sha256": vs, "planted": True}, f)
    if mod == "bindfail":  # a binding record for THIS fixture whose own binding check failed (fourth review L3)
        with open(out, "rb") as f:
            vs = hashlib.sha256(f.read()).hexdigest()
        with open(bind_path(out)[0], "w") as f:
            json.dump({"verdict_sha256": vs, "all_pass": False, "planted": True,
                       "checks": [{"id": "bind:P_runsh_ok", "pass": False, "detail": "planted"}]}, f)
    return 0


# ---- self-test ----------------------------------------------------------------------------------------------------
# Item 1 red test 1, with the banked cells (testdata/banked/<run>-<cell>/, copied unchanged from artie
# frontier/fastest/linux/v3/runs/): expectations written by hand from the review, never computed by the gate.
BANKED = [
    ("37254957355-v3-ubuntu-24.04-xfs", "pass", 1200, 2821),        # x86 xfs on write-back NVMe
    ("37245757924-v3-ubuntu-24.04-ext4", "pass", 800, 1209),        # x86 ext4 on write-back NVMe
    ("37254957355-v3-ubuntu-24.04-arm-btrfs", "wt", None, 0),
    ("37254957355-v3-ubuntu-24.04-arm-ext4", "wt", None, 0),
    ("37254957355-v3-ubuntu-24.04-arm-ext4loop", "wt", None, 0),
    ("37254957355-v3-ubuntu-24.04-arm-xfs", "wt", None, 0),
    ("37254957355-v3-ubuntu-24.04-btrfs", "wt", None, 0),
    ("37254957355-v3-ubuntu-24.04-ext4", "wt", None, 0),
    ("37254957355-v3-ubuntu-24.04-ext4loop", "wt", None, 0),
]


def _post_batch(d, sha, mod):
    """a minimal run.sh OUT for post(): a write-back NVMe ext4 batch, every op's window holding a bare flush."""
    os.makedirs(os.path.join(d, "blkflush"))
    wt = mod.startswith("wt")
    sj = {"mutant_nosync": 0, "trace_clock": 0, "exe_sha256": sha, "fstype": "ext4", "mount_source": "/dev/nvme0n1",
          "flush_path": [{"fstype": "ext4", "source": "/dev/nvme0n1", "loop_backing": "", "disk": "nvme0n1",
                          "sys": "/sys/block/nvme0n1", "mount": "/d", "write_cache": "write through" if wt else "write back"}],
          "leaf": {"kind": "drive", "driver": "nvme", "model": "M1", "multipath": [],
                   "write_cache": "write through" if wt else "write back",
                   "drive_reports": "write through" if wt else "write back"},
          "leaf_write_cache": "write through" if wt else "write back",
          "virtualization": {"virtualized": False, "evidence": []}, "n": 5, "pid": 4242, "plp": "no",
          "arms": {"append25": {}, "nosync25": {}}, "flush_control_arms": {"append25": {"gated": True}},
          # [V3 review 12 item 7 / review 11 LOW 2: a current probe summary carries its sync_fds, which post now holds
          # to the arm definitions: append25 one fd synced once per op, nosync25 none]
          "sync_fds": {"append25": {"3": 5}, "nosync25": {}}, "sync_fds_overflow": False,
          "timing_control": "not applicable: no volatile cache" if wt else "pass", "d0_threshold_key": "d0_threshold/ext4/wb/bare",
          "d0_threshold_ref": None, "frame_arm": None, "traced": False}
    win = {"events": 5, "zero_windows": 0, "flush_carrying_zero_windows": 0, "bare_flush_zero_windows": 0, "per_op": 1.0}
    rep = {"proves": "planted", "devices": {},
           "windows": {"arms": {"append25": {"ops": 5, "devices": {} if wt else {"nvme0n1": win}},
                                "nosync25": {"ops": 5, "devices": {}}}},
           # [tenth review HIGH 1: a current report carries windows_short; eleventh review MED 1: and the syncs wholly
           # inside each arm's windows on any fd, and the events unattributed inside a window]
           "syscalls": {"pid": 4242, "arms": {"append25": {"ops": 5, "syncs": 5, "windows_without_a_sync": 0, "windows_short": 0,
                                                           "syncs_inside_any_fd": 5, "windows_over": 0},
                                               "nosync25": {"ops": 5, "syncs": 0, "windows_without_a_sync": 5, "windows_short": 0,
                                                            "syncs_inside_any_fd": 0, "windows_over": 0}},
                        "unattributed": {"foreign_fd": 0, "no_fd": 0, "inside_foreign_fd": 0, "inside_no_fd": 0}}}
    if mod == "fuaonly":  # one append25 window holds only a FUA write: a request, but none that flushes
        rep["windows"]["arms"]["append25"]["devices"]["nvme0n1"]["flush_carrying_zero_windows"] = 1
    if mod == "leafkind":
        sj["leaf"]["kind"] = "scsi_debug"
    if mod == "short":  # tenth review HIGH 1: every window synced, one short of its own (by fd)
        rep["syscalls"]["arms"]["append25"].update(windows_short=1)
    if mod == "over":  # V3 review 12 item 8: an append25 window holding an extra sync of its own fd
        rep["syscalls"]["arms"]["append25"].update(windows_over=1)
    # [the three eleventh-review plants AMENDED at V3 review 12 item 6, disclosed: a sync wholly inside a window on an
    # fd its arm does not own is counted in foreign_fd as well, so each plant now carries the foreign_fd/no_fd a real
    # report would, and the gate is foreign_fd/no_fd == 0]
    if mod == "nosync-anyfd":  # eleventh review MED 1: an fsync on fd 7 (no arm's) wholly inside a nosync25 window
        rep["syscalls"]["arms"]["nosync25"].update(syncs_inside_any_fd=1)
        rep["syscalls"]["unattributed"].update(foreign_fd=1, inside_foreign_fd=1)
    if mod == "inside-foreign":  # ... a sync on an fd its window's arm does not own, wholly inside an append25 window
        rep["syscalls"]["arms"]["append25"].update(syncs_inside_any_fd=6)
        rep["syscalls"]["unattributed"].update(foreign_fd=1, inside_foreign_fd=1)
    if mod == "inside-nofd":  # ... a sync naming no fd, wholly inside an append25 window
        rep["syscalls"]["arms"]["append25"].update(syncs_inside_any_fd=6)
        rep["syscalls"]["unattributed"].update(no_fd=1, inside_no_fd=1)
    if mod == "nosync-ownfd":  # V3 review 12 item 7: nosync25 OWNS an fd and syncs on it, every unattributed count 0
        sj["sync_fds"]["nosync25"] = {"9": 5}
        rep["syscalls"]["arms"]["nosync25"].update(syncs=5, windows_without_a_sync=0)
    if mod == "deffd-short":  # review 11 LOW 2: a probe that records no fd for append25 (its sync unrecorded)
        sj["sync_fds"]["append25"] = {}
    if mod == "foreign-edge":  # V3 review 12 item 6: an unowned-fd sync at a nosync25 window's edge: foreign_fd only
        rep["syscalls"]["unattributed"].update(foreign_fd=1)
    if mod == "nofd-only":  # ... a sync naming no fd outside every window's interior: no_fd only
        rep["syscalls"]["unattributed"].update(no_fd=1)
    if mod == "unattr-nokey":  # ... a report whose unattributed lacks foreign_fd (a missing count is not a zero)
        rep["syscalls"]["unattributed"].pop("foreign_fd")
    if mod in ("nosync", "wt-nosync"):  # one append25 window with no fsync by the probe
        rep["syscalls"]["arms"]["append25"].update(syncs=4, windows_without_a_sync=1)
    if mod == "wt-mismatch":
        sj["leaf"]["drive_reports"] = "write back"
    if mod == "loopflush":  # a write-back loop above the leaf, one of whose windows had no flush-carrying request
        sj["flush_path"].insert(0, {"fstype": "ext4", "source": "/dev/loop0", "loop_backing": "/x.img", "disk": "loop0",
                                    "sys": "/sys/block/loop0", "mount": "/l", "write_cache": "write back"})
        sj["mount_source"] = "/dev/loop0"
        rep["windows"]["arms"]["append25"]["devices"]["loop0"] = dict(win, flush_carrying_zero_windows=1)
    if mod == "plp":
        sj["plp"] = "yes"
    if mod == "traced":
        sj["traced"] = True
    if mod == "frame-ow64k":  # a drive cell, a registered threshold, frame arm ow64k: only the variant rule is left
        sj.update(d0_threshold_ref="planted", frame_arm="ow64k")
    if mod == "loopflush-rental":  # a loop cell, everything registered: only the rental-drive rule is left
        sj["flush_path"].insert(0, {"fstype": "ext4", "source": "/dev/loop0", "loop_backing": "/x.img", "disk": "loop0",
                                    "sys": "/sys/block/loop0", "mount": "/l", "write_cache": "write back"})
        sj["mount_source"] = "/dev/loop0"
        rep["windows"]["arms"]["append25"]["devices"]["loop0"] = dict(win)
        sj.update(d0_threshold_ref="planted", frame_arm="ow4k")
    with open(os.path.join(d, "summary.json"), "w") as f:
        json.dump(sj, f)
    with open(os.path.join(d, "stamp_end.json"), "w") as f:
        json.dump({"diskstats_delta": {"nvme0n1": {"flushes": 3 if mod == "wt-counter" else 0 if wt else 50}}}, f)
    with open(os.path.join(d, "blkflush", "report.json"), "w") as f:
        json.dump(rep, f)
    v = {"leaf_class": "wt" if wt else "wb", "box": {"virt": "bare", "flip": "yes", "plp": "no"},
         "F3": {"flush_path": [[l["mount"], l["fstype"], l["source"], l["disk"], l["write_cache"]] for l in sj["flush_path"]],
                "leaf": dict(sj["leaf"], kind="drive"), "virtualization": {"virtualized": False}}}
    if mod == "model":
        v["F3"]["leaf"]["model"] = "M2"
    raw = json.dumps(v).encode()
    vp = d + ".verdict.json"
    with open(vp, "wb") as f:
        f.write(raw)
    with open(os.path.join(d, "binary.txt"), "w") as f:
        f.write("verdict_sha256=%s\n" % hashlib.sha256(raw).hexdigest())
    return vp


def post_selftest(chk):
    import contextlib, io, tempfile
    td = tempfile.mkdtemp(prefix="batchgate-post-")
    sha = "cd" * 32
    for name, mod, mode, want_rc, want, env in (
            ("the control, bound", "", "bound", 0, None, {}),
            ("the control, smoke", "", "smoke", 0, None, {}),
            ("a FUA-only window in a gated arm", "fuaonly", "smoke", 3, "VOID flush-carrying:", {}),
            ("a scsi_debug leaf record", "leafkind", "smoke", 2, "leaf: the summary's leaf record", {}),
            ("another drive model than the verdict's", "model", "bound", 2, "leaf drive:", {}),
            ("A16: a write-through leaf, counter 0, every window synced", "wt", "bound", 0, None, {}),
            ("A16: a write-through leaf whose counter rose", "wt-counter", "smoke", 2, "write-through leaf:", {}),
            ("A16: a write-through leaf whose drive reports write back", "wt-mismatch", "smoke", 2, "drive report:", {}),
            ("A16: a write-through window with no fsync by the probe", "wt-nosync", "smoke", 3, "VOID fsync:", {}),
            ("A16: a write-back window with no fsync by the probe", "nosync", "smoke", 3, "VOID fsync:", {}),
            ("tenth review HIGH 1: a window short of one of its own syncs", "short", "smoke", 3, "VOID fsync:", {}),
            ("V3 review 12 item 8: a window over its own syncs (an extra own-fd sync)", "over", "smoke", 3, "VOID fsync:", {}),
            ("eleventh review MED 1: an fsync on fd 7 wholly inside a nosync25 window", "nosync-anyfd", "smoke", 3,
             "VOID fsync:", {}),
            ("eleventh review MED 1: a sync on another fd wholly inside an append25 window", "inside-foreign", "smoke", 3,
             "VOID fsync:", {}),
            ("eleventh review MED 1: a sync naming no fd wholly inside an append25 window", "inside-nofd", "smoke", 3,
             "VOID fsync:", {}),
            ("V3 review 12 item 6: an unowned-fd sync at a nosync25 window's edge (foreign_fd 1, inside counts 0)",
             "foreign-edge", "smoke", 3, "VOID fsync:", {}),
            ("V3 review 12 item 6: a sync naming no fd (no_fd 1)", "nofd-only", "smoke", 3, "VOID fsync:", {}),
            ("V3 review 12 item 6: a report whose unattributed lacks foreign_fd", "unattr-nokey", "smoke", 3, "VOID fsync:", {}),
            ("MED 3: a write-back loop layer's window without a flush-carrying request", "loopflush", "smoke", 3,
             "VOID flush-carrying:", {}),
            ("A14: a batch declaring PLP bound to a verdict fire-checked without", "plp", "bound", 2, "plp:", {}),
            ("A17: rental mode with no registered threshold or frame arm", "", "smoke", 2, "registration:",
             {"V3_REQUIRE_T3": "1"}),
            ("A16/eighth review M6: rental mode on a loop cell", "loopflush-rental", "smoke", 2, "rental:", {"V3_REQUIRE_T3": "1"}),
            ("ninth review M6 / tenth review LOW 1: rental mode with frame arm ow64k (no fdatasync variant)", "frame-ow64k",
             "smoke", 2, "registration: the registered frame arm", {"V3_REQUIRE_T3": "1"}),
            ("eighth review H2: a batch the probe ran traced", "traced", "smoke", 2, "traced:", {})):
        d = os.path.join(td, (mod or "control") + "-" + mode + ("-t3" if env else ""))
        vp = _post_batch(d, sha, mod)
        old = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                rc = post(d, "ext4loop" if mod.startswith("loopflush") else "ext4", sha, mode, vp if mode == "bound" else None)
        finally:
            for k, x in old.items():
                if x is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = x
        g = load(os.path.join(d, "gate.json"))
        refs = g.get("refusals")
        if want is None:
            ok = rc == want_rc and refs == [] and g.get("voids") == []
        elif want.startswith("VOID "):
            ok = rc == 3 and refs == [] and bool(g.get("voids")) and all(str(x).startswith(want[5:]) for x in g["voids"])
        else:  # a leaf with no class also leaves the flush gate unable to decide: that consequence is a refusal too
            also = ("flush gate: cannot determine",) if mod == "leafkind" else ()
            ok = rc == want_rc and bool(refs) and str(refs[0]).startswith(want) and \
                all(str(r).startswith((want,) + also) for r in refs)
        chk("post() on a planted batch, %s -> rc %d%s" % (name, want_rc, "" if want is None else " (%s)" % want),
            ok, (rc, g))
    # V3 review 12 item 7 and review 11 LOW 2, each with its EXACT VOID text (expected by hand): nosync25 owning and
    # syncing an fd with every unattributed count 0 (the foreign_fd gate cannot see it), and a sync_fds that breaks
    # the arm definitions (post trusted the probe's own record)
    for name, mod, want_voids in (
            ("nosync25 owns fd 9 and syncs on it, unattributed all 0", "nosync-ownfd",
             ["fsync: nosync25's windows hold 5 sync(s) by the probe (it owns no fd by definition)",
              "fsync: sync_fds disagrees with the arm definitions: nosync25 recorded {'9': 5}, defined 0 fd(s) and 0 sync(s)"]),
            ("append25's sync_fds records no fd", "deffd-short",
             ["fsync: sync_fds disagrees with the arm definitions: append25 recorded {}, defined 1 fd(s) and 5 sync(s)"])):
        d = os.path.join(td, "exact-" + mod)
        _post_batch(d, sha, mod)
        with contextlib.redirect_stderr(io.StringIO()):
            rc = post(d, "ext4", sha, "smoke", None)
        g = load(os.path.join(d, "gate.json"))
        chk("post() (review 12 item 7 / review 11 LOW 2): %s -> rc 3 with exactly %s" % (name, want_voids),
            rc == 3 and g.get("refusals") == [] and g.get("voids") == want_voids, (rc, g.get("voids"), g.get("refusals")))
    # gate-6 MED 6: the start-to-end drift
    for name, ea, ok_rc in (("4 us", 1004.0, 0), ("61 us", 1061.0, 3)):
        sd, ed = os.path.join(td, "drift-s-" + name.split()[0]), os.path.join(td, "drift-e-" + name.split()[0])
        for dd, p50 in ((sd, 1000.0), (ed, ea)):
            os.makedirs(dd)
            with open(os.path.join(dd, "summary.json"), "w") as f:
                json.dump({"frame_arm": None, "arms": {"append25": {"p50_us": p50}}, "exe_sha256": "ab", "plp": "no",
                           "n": 10000, "leaf": {"disk": "nvme1n1"}}, f)
            with open(os.path.join(dd, "gate.json"), "w") as f:
                json.dump({"rc": 0, "cell": "xfs"}, f)
            with open(os.path.join(dd, "binary.txt"), "w") as f:
                f.write("cell=xfs\nshape=bound V3: N=10000, append25,fdatasync4k,nosync25\n")
        with contextlib.redirect_stdout(io.StringIO()):
            rc = drift(sd, ed)
        chk("drift: append25 p50 moved %s -> rc %d" % (name, ok_rc), rc == ok_rc, rc)
    # eighth review L3: ends of a different kind, or an end its own gate did not pass, refuse
    for name, mut in (("another binary at the end", lambda d: json.dump(dict(json.load(open(os.path.join(d, "summary.json"))),
                                                                              exe_sha256="cd"), open(os.path.join(d, "summary.json"), "w"))),
                      ("an end whose gate voided it", lambda d: json.dump({"rc": 3, "cell": "xfs"}, open(os.path.join(d, "gate.json"), "w")))):
        sd, ed = os.path.join(td, "drift-s-4"), os.path.join(td, "drift-e-4")
        ed2 = ed + "-" + name[:5].replace(" ", "")
        shutil.copytree(ed, ed2)
        mut(ed2)
        with contextlib.redirect_stdout(io.StringIO()):
            rc = drift(sd, ed2)
        chk("drift: %s -> refused (rc 2)" % name, rc == 2, rc)
    # ninth review L11: drift's identity rules are exact
    def bt(d, text):
        with open(os.path.join(d, "binary.txt"), "w") as f:
            f.write(text)
    base_bt = "cell=xfs\nshape=bound V3: N=10000, append25,fdatasync4k,nosync25\n"
    for name, sm, em in (
            ("binary.txt cell=xfsloop at both ends under a gate for xfs", lambda d: bt(d, base_bt.replace("cell=xfs", "cell=xfsloop")),
             lambda d: bt(d, base_bt.replace("cell=xfs", "cell=xfsloop"))),
            ("a bound end and a smoke end", lambda d: bt(d, base_bt + "bound=fire-checked: /v.json\n"),
             lambda d: bt(d, base_bt + "bound=smoke: V3_SMOKE=1, not bound to a fire-check, never credited\n")),
            ("ends bound to different verdicts", lambda d: bt(d, base_bt + "verdict_sha256=" + "aa" * 32 + "\n"),
             lambda d: bt(d, base_bt + "verdict_sha256=" + "bb" * 32 + "\n")),
            ("a rental end and a dry-run end", lambda d: bt(d, base_bt + "rental=yes\n"), lambda d: bt(d, base_bt + "rental=no\n"))):
        k = name.split()[0] + name.split()[1] + str(len(name))
        s2, e2 = os.path.join(td, "drift-s-" + k), os.path.join(td, "drift-e-" + k)
        shutil.copytree(os.path.join(td, "drift-s-4"), s2)
        shutil.copytree(os.path.join(td, "drift-e-4"), e2)
        sm(s2)
        em(e2)
        with contextlib.redirect_stdout(io.StringIO()):
            rc = drift(s2, e2)
        chk("drift (ninth review L11): %s -> refused (rc 2)" % name, rc == 2, rc)
    with contextlib.redirect_stdout(io.StringIO()):
        rc = drift(os.path.join(td, "drift-s-4"), os.path.join(td, "drift-s-4"))
    chk("drift (ninth review L11): the same OUT as both ends -> refused (rc 2)", rc == 2, rc)
    shutil.rmtree(td)


def self_test(data):
    res = []

    def chk(name, ok, detail=""):
        res.append(bool(ok))
        print(("PASS " if ok else "FAIL ") + name + ("" if ok else ": " + str(detail)[:400]), flush=True)

    # item 1: the leaf flush gate on the banked cells (red at base: every one of these batches was rc 0)
    for cell, want, need, got in BANKED:
        d = os.path.join(data, "banked", cell)
        try:
            sj, st1 = load(os.path.join(d, "summary.json")), load(os.path.join(d, "stamp_end.json"))
            base_rc = open(os.path.join(d, "F3.rc")).read().strip()
        except (OSError, ValueError) as e:
            chk("banked %s readable" % cell, False, e)
            continue
        g = flush_gate(sj, st1)
        if want == "pass":
            chk("item 1 gate: %s passes (%r >= %r)" % (cell, got, need),
                g["outcome"] == "pass" and g["required_flushes"] == need and g["leaf_flushes_completed"] == got, g)
            p = _copy.deepcopy(st1)
            p["diskstats_delta"][g["leaf"]]["flushes"] = need - 1  # planted: one flush short
            gp = flush_gate(sj, p)
            chk("item 1 gate: %s with %d flushes (one short) is VOID" % (cell, need - 1), gp["outcome"] == "FAIL", gp)
            q = _copy.deepcopy(sj)
            for r in q.get("flush_control_arms", {}).values():
                r["gated"] = False  # planted: no gated arm ran
            gq = flush_gate(q, st1)
            chk("item 1 gate: %s with no gated arm is not a pass (n x 0 = 0 would pass anything)" % cell,
                gq["outcome"] == "FAIL" and "no gated arm" in gq.get("why", ""), gq)
        else:
            chk("item 1 gate: %s (write-through, %r flushes) does not pass: labelled 'no volatile cache'" % (cell, got),
                g["outcome"] == "not applicable: no volatile cache: no drive flush" and g["leaf_flushes_completed"] == got, g)
        chk("item 1 red at base: %s's F3 batch was rc 0 (no gate)" % cell, base_rc == "0", base_rc)
    # item 2: the claim the counts allow (run 37476867864: btrfs clean 0.00, ext4/xfs clean 1.00 on write-back); the
    # clean arm is read per window (fourth review L9) and the VM qualifier rides on both classes (L2)
    bm = {"virtualized": False}
    wb = {"leaf_write_cache": "write back", "leaf": {"kind": "drive"}, "virtualization": bm}
    wt = {"leaf_write_cache": "write through", "leaf": {"kind": "drive"}, "virtualization": bm, "fstype": "ext4"}
    every = {"ops": 200, "windows_with_bare_flush": 200}
    chk("item 2 claim: ext4, a bare flush in 200 of 200 clean windows -> a bare flush plus the delta",
        claim_from_counts(dict(wb, fstype="ext4"), every).startswith("clean fsync (a bare flush"))
    chk("item 2 claim: btrfs, a bare flush in 0 of 200 clean windows -> no bare-flush baseline",
        claim_from_counts(dict(wb, fstype="btrfs"), {"ops": 200, "windows_with_bare_flush": 0}).startswith("no bare-flush baseline"))
    chk("item 2 claim: btrfs even with every window -> no bare-flush baseline (ext4/xfs only)",
        claim_from_counts(dict(wb, fstype="btrfs"), every).startswith("no bare-flush baseline"))
    chk("item 2 claim: write-through -> no drive flush",
        claim_from_counts(wt, {"ops": 200, "windows_with_bare_flush": 0}).startswith("no drive flush"))
    c = claim_from_counts(dict(wb, fstype="xfs"), None)
    chk("fourth review L1: clean not run -> 'did not run', never 'issued no flush'",
        c.startswith("no bare-flush baseline") and "did not run" in c and "issued no" not in c, c)
    chk("fourth review L9: 190 of 200 windows is a bare-flush baseline, 189 is not (95%)",
        claim_from_counts(dict(wb, fstype="ext4"), {"ops": 200, "windows_with_bare_flush": 190}).startswith("clean fsync")
        and claim_from_counts(dict(wb, fstype="ext4"), {"ops": 200, "windows_with_bare_flush": 189}).startswith("no bare"))
    two = {"clean": {"ops": 200, "devices": {"nvme0n1": {"events": 200, "by_kind": {"flush": 200},
                                                         "bare_flush_zero_windows": 20}}}}
    cb = clean_bare(two, ["nvme0n1"])
    chk("fourth review L9: 200 bare flushes packed into 180 of 200 windows are 180 windows, not 1.00 per op",
        cb == {"ops": 200, "windows_with_bare_flush": 180} and
        claim_from_counts(dict(wb, fstype="ext4"), cb).startswith("no bare-flush baseline"), cb)
    chk("fourth review L9: clean_bare with no clean arm -> None", clean_bare({"append25": {"ops": 5}}, ["sda"]) is None)
    for vz, want in ((True, "; a virtual drive: reach to media unknown"), (None, "; virtualization not ruled out: reach")):
        cv = claim_from_counts(dict(wb, fstype="ext4", virtualization={"virtualized": vz}), every)
        chk("fourth review L2: write-back, virtualized %r -> the claim says '%s'" % (vz, want), want in cv, cv)
        cw = claim_from_counts(dict(wt, virtualization={"virtualized": vz}), None)
        chk("fourth review L2: write-through, virtualized %r -> the claim carries the qualifier" % vz,
            cw.startswith("no drive flush") and "the host's caching is unknown" in cw, cw)
    chk("fourth review L2: bare metal (virtualized false) -> no qualifier on either class",
        "virtual" not in claim_from_counts(dict(wb, fstype="ext4"), every) and "virtual" not in claim_from_counts(wt, None))
    # the leaf class: only a drive or brd leaf has one (fourth review M2: a scsi_debug leaf is fire-check only)
    chk("leaf class: a scsi_debug leaf has none", leaf_class(dict(wb, leaf={"kind": "scsi_debug"})) is None)
    chk("leaf class: a drive leaf by its write cache; brd as brd; a pre-review-2 summary (no leaf) by its write cache",
        leaf_class(wb) == "wb" and leaf_class(dict(wb, leaf={"kind": "brd"})) == "brd"
        and leaf_class({"leaf_write_cache": "write through"}) == "wt")
    # item 4: the cell layout, explicit (the review's red fixture: xfs on /dev/nvme0n1, one layer, must pass)
    one = [{"fstype": "xfs", "source": "/dev/nvme0n1", "loop_backing": ""}]
    chk("item 4: xfs cell, /dev/nvme0n1, 1 layer -> layout ok", v3cell.layout_problems("xfs", "xfs", "/dev/nvme0n1", one) == [])
    chk("item 4: the same as xfsloop -> refused", v3cell.layout_problems("xfsloop", "xfs", "/dev/nvme0n1", one) != [])
    two = [{"fstype": "xfs", "source": "/dev/loop0", "loop_backing": "/x.img"}, {"fstype": "ext4", "source": "/dev/sda1", "loop_backing": ""}]
    chk("item 4: xfsloop, /dev/loop0, 2 layers -> ok", v3cell.layout_problems("xfsloop", "xfs", "/dev/loop0", two) == [])
    chk("item 4: xfs (block) on /dev/loop0 -> refused", v3cell.layout_problems("xfs", "xfs", "/dev/loop0", two) != [])
    chk("item 4: ext4 on /dev/sda1, 1 layer -> ok",
        v3cell.layout_problems("ext4", "ext4", "/dev/sda1", [{"fstype": "ext4", "source": "/dev/sda1", "loop_backing": ""}]) == [])
    chk("item 4: a block cell whose source is not a /dev path -> refused",
        v3cell.layout_problems("xfs", "xfs", "", one) != [] and v3cell.layout_problems("xfs", "xfs", None, one) != [])
    chk("item 4: ext4loop data on an ext4 cell's loop -> refused (ext4 vs ext4loop no longer bind each other)",
        v3cell.layout_problems("ext4", "ext4", "/dev/loop0", [{"fstype": "ext4", "source": "/dev/loop0", "loop_backing": "/b"},
                                                             {"fstype": "ext4", "source": "/dev/sda1", "loop_backing": ""}]) != [])
    # item 8: verdict shapes, each refused by its OWN rule's reason (fresh review H2)
    sha, cell, arch = "ab" * 32, "xfs", "x86_64"
    good = {"cell": cell, "fstype": "xfs", "arch": arch, "leaf_class": "wb", "v3floor_sha256": sha, "run_id": "1",
            "all_pass": True, "unplanted_refusals": [], "harness_sha256": harness(),
            "box": {"virt": "vm", "flip": "yes", "plp": "no"},
            "checks": [{"id": i, "pass": True} for i in plan(cell, arch, "wb", {"virt": "vm", "flip": "yes", "plp": "no"})]}
    good["pass"] = good["total"] = len(good["checks"])
    chk("item 8: a full-shape passing verdict binds", verdict_problems(good, cell, sha, arch, "xfs") == [],
        verdict_problems(good, cell, sha, arch, "xfs"))
    # seventh review L6: the plan follows the verdict's own box: a bare-metal, PLP, no-flip verdict planned for its box
    # binds; the vm verdict relabelled bare (its ids planned for vm) is refused by the plan rule
    bare = dict(_copy.deepcopy(good), box={"virt": "bare", "flip": "no", "plp": "yes"})
    bare["checks"] = [{"id": i, "pass": True} for i in plan(cell, arch, "wb", bare["box"])]
    bare["pass"] = bare["total"] = len(bare["checks"])
    chk("seventh review L6: a bare/no-flip/PLP verdict planned for its own box binds",
        verdict_problems(bare, cell, sha, arch, "xfs") == [] and len(bare["checks"]) != len(good["checks"]),
        (verdict_problems(bare, cell, sha, arch, "xfs"), len(bare["checks"]), len(good["checks"])))
    rel = dict(_copy.deepcopy(good), box={"virt": "bare", "flip": "no", "plp": "yes"})
    b = verdict_problems(rel, cell, sha, arch, "xfs")
    chk("seventh review L6: the vm verdict relabelled bare/no/PLP is refused by the plan rule", bool(b) and
        all(x.startswith("plan:") for x in b), b)
    old = {"all_pass": True, "v3floor_sha256": sha, "fstype": "xfs", "arch": arch}
    chk("item 8: the old 4-field fixture is refused (no checks)",
        any(b.startswith("checks:") for b in verdict_problems(old, cell, sha, arch, "xfs")))

    def drop(v):
        v["checks"].pop(3)
        v["pass"] = v["total"] = len(v["checks"])

    def other_arch(v):  # an aarch64 verdict, whole and passing for aarch64, on this x86_64 batch
        v["arch"] = "aarch64"
        v["checks"] = [{"id": i, "pass": True} for i in plan(cell, "aarch64", "wb", v["box"])]
        v["pass"] = v["total"] = len(v["checks"])

    for name, mut, prefix in (("planted", lambda v: v.update(planted=True), "planted:"),
                              ("all_pass false", lambda v: v.update(all_pass=False), "all_pass is not true"),
                              ("one check failing", lambda v: v["checks"][3].update({"pass": False}), "pass/total:"),
                              ("one check dropped", drop, "plan:"),
                              ("another cell", lambda v: v.update(cell="xfsloop"), "cell:"),
                              ("another binary", lambda v: v.update(v3floor_sha256="00"), "v3floor_sha256:"),
                              ("another arch", other_arch, "arch:"),
                              ("another fstype", lambda v: v.update(fstype="ext4"), "fstype:"),
                              ("another harness", lambda v: v["harness_sha256"].update({"check.py": "0" * 64}), "harness:"),
                              ("a brd leaf", lambda v: v.update(leaf_class="brd"), "leaf_class brd:"),
                              ("no box", lambda v: v.pop("box"), "box:"),
                              ("a box of unknowns", lambda v: v.update(box={"virt": "unknown", "flip": "yes"}), "box:"),
                              ("no unplanted_refusals", lambda v: v.pop("unplanted_refusals"), "unplanted_refusals:")):
        v = _copy.deepcopy(good)
        mut(v)
        b = verdict_problems(v, cell, sha, arch, "xfs")
        chk("item 8: refused: %s, by its own rule ('%s'), and by no other" % (name, prefix),
            [x for x in b if x.startswith(prefix)] and all(x.startswith(prefix) for x in b), b)
    # fourth review L3: the fire-check's own binding record must have passed for THIS verdict (or be pending for it)
    import tempfile
    td = tempfile.mkdtemp(prefix="batchgate-bind-")
    vp = os.path.join(td, "verdict.json")
    with open(vp, "w") as f:
        f.write("{}")
    vs = hashlib.sha256(b"{}").hexdigest()
    bj, bp = bind_path(vp)
    chk("L3 bind: the record names are verdict.bind.json and verdict.bind.pending",
        (os.path.basename(bj), os.path.basename(bp)) == ("verdict.bind.json", "verdict.bind.pending"), (bj, bp))
    chk("L3 bind: no record -> refused ('bind: no binding record')",
        [x for x in bind_problems(vp, vs) if x.startswith("bind: no binding record")], bind_problems(vp, vs))
    with open(bp, "w") as f:
        json.dump({"verdict_sha256": vs}, f)
    chk("L3 bind: pending for this verdict, from the bind step (its sha named) -> binds",
        bind_problems(vp, vs, pending_sha=vs) == [], bind_problems(vp, vs, pending_sha=vs))
    b = bind_problems(vp, vs, pending_sha="")
    chk("fifth review L1: pending for this verdict outside the bind step -> refused ('bind: pending')",
        bool(b) and all(x.startswith("bind: pending") for x in b), b)
    chk("L3 bind: pending for another verdict -> refused", bind_problems(vp, "00" * 32, pending_sha="00" * 32) != [])
    b = bind_problems(vp, vs, pending_sha="ab" * 32)
    chk("sixth review L1: pending for this verdict while the bind step names ANOTHER verdict -> refused ('bind: pending')",
        bool(b) and all(x.startswith("bind: pending") for x in b), b)
    ok_rec = {"verdict_sha256": vs, "all_pass": True, "checks": [{"id": "bind:P_runsh_ok", "pass": True}]}
    for name, rec, prefix in (("passed for this verdict", ok_rec, None),
                              ("failed", dict(ok_rec, all_pass=False, checks=[{"id": "bind:P_runsh_ok", "pass": False}]),
                               "bind: the fire-check's own binding check failed"),
                              ("all_pass true over a failing check",
                               dict(ok_rec, checks=[{"id": "bind:P_runsh_ok", "pass": False}]),
                               "bind: the fire-check's own binding check failed"),
                              ("for another verdict", dict(ok_rec, verdict_sha256="00" * 32), "bind: the binding record")):
        with open(bj, "w") as f:
            json.dump(rec, f)
        b = bind_problems(vp, vs, pending_sha=vs)  # the pending record is still there: a result, once written, decides
        chk("L3 bind: a record that %s -> %s" % (name, "binds" if prefix is None else "refused by '%s'" % prefix),
            b == [] if prefix is None else bool(b) and all(x.startswith(prefix) for x in b), b)
    shutil.rmtree(td)
    # fifth review L2: a harness file the verdict names that this tree lacks (or the reverse) is a mismatch
    v = _copy.deepcopy(good)
    v["harness_sha256"]["gone.py"] = "0" * 64
    b = verdict_problems(v, cell, sha, arch, "xfs")
    chk("fifth review L2: a harness file only in the verdict -> refused by the harness rule",
        bool(b) and all(x.startswith("harness:") for x in b) and "gone.py" in b[0], b)
    v = _copy.deepcopy(good)
    v["harness_sha256"].pop("run.sh")
    b = verdict_problems(v, cell, sha, arch, "xfs")
    chk("fifth review L2: a harness file only here -> refused by the harness rule",
        bool(b) and all(x.startswith("harness:") for x in b), b)
    # fifth review M3: post() itself on planted batches (a write-back NVMe ext4 batch of append25 + nosync25)
    post_selftest(chk)
    # item 17: the T3 rule on planted states
    chk("item 17: no cpufreq refuses", t3_problems({"governors": {"cpu0": None}, "clocksource": "tsc"}) != [])
    chk("item 17: powersave refuses", t3_problems({"governors": {"cpu0": "performance", "cpu1": "powersave"}, "clocksource": "tsc"}) != [])
    chk("item 17: performance everywhere on tsc passes", t3_problems({"governors": {"cpu0": "performance", "cpu1": "performance"},
                                                                      "clocksource": "tsc"}) == [])
    chk("item 16: hpet refuses", t3_problems({"governors": {"cpu0": "performance"}, "clocksource": "hpet"}) != [])
    ok = all(res) and len(res) > 0
    print("BATCHGATE SELF-TEST %d/%d %s" % (sum(res), len(res), "PASS" if ok else "FAIL"))
    return 0 if ok else 1


def main(a):
    if len(a) == 6 and a[0] == "verdict":
        return cmd_verdict(*a[1:])
    if a == ["t3pre"]:
        return cmd_t3pre()
    if len(a) in (5, 6) and a[0] == "post":
        try:
            return post(a[1], a[2], a[3], a[4], a[5] if len(a) == 6 else None)
        except Exception as e:  # a gate that cannot finish refuses; it never falls through to rc 0 (fresh review I-H1)
            print("run.sh: REFUSED after the run: the batch gate failed: %r" % e, file=sys.stderr)
            return 2
    if len(a) == 3 and a[0] == "drift":
        return drift(a[1], a[2])
    if len(a) == 3 and a[0] == "flushgate":
        print(json.dumps(flush_gate(load(a[1]), load(a[2])), indent=1))
        return 0
    if len(a) == 2 and a[0] == "leafclass":
        try:
            c = leaf_class(load(a[1]))
        except (OSError, ValueError):
            c = None
        if not c:
            return 2
        print(c)
        return 0
    if len(a) in (8, 9) and a[0] == "fixture":
        return fixture(*a[1:8], a[8] if len(a) == 9 else "")
    if len(a) == 2 and a[0] == "self-test":
        return self_test(a[1])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
