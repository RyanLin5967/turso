#!/usr/bin/env python3
"""batchgate.py -- run.sh's binding and post-run gates for one V3 batch (review 2 items 1, 2, 7, 8, 16, 17).

  batchgate.py verdict VERDICT CELL SHA ARCH FSTYPE   may this fire-check verdict bind a batch? (JSON; exit 0 yes, 2 no)
  batchgate.py t3pre                                  the registered T3 preconditions on this box (exit 0 met, 2 not)
  batchgate.py post OUT CELL SHA MODE [VERDICT]       after the probe: refusals, the device flush merge, the flush gates
                                                      and the claim the counts allow (OUT/gate.json; exit 0 ok, 2 refused,
                                                      3 void). MODE is bound or smoke; a bound batch names its verdict
  batchgate.py flushgate SUMMARY STAMP_END            the diskstats leaf flush gate alone (JSON)
  batchgate.py leafclass SUMMARY                      wb | wt | brd for a probe summary (exit 2 if it has none)
  batchgate.py fixture OUT CELL ARCH LEAF SHA FSTYPE [MOD]   a PLANTED full-shape verdict for firecheck.sh's F4 plants,
                                                      always "planted": true (MOD: allfail | fail-one | drop-one |
                                                      cell=<c> | harness)
  batchgate.py self-test DATA                         the gates on banked and planted inputs; exit 0 iff all as expected

Binding (item 8): a verdict binds only if it has check.py's whole shape -- a "checks" list whose ids equal
check.plan(cell, arch, leaf class), every check passing, pass == total, all_pass true -- for this cell, binary sha256,
arch and fstype, with "unplanted_refusals" listed, a leaf class that is not brd, no "planted" key, and the sha256 of
every fire-check harness file equal to this run.sh's own copies (a verdict vouches only for the checker that wrote
it). Each refusal reason starts with its rule's own name. Its sha256 and run id go into binary.txt. Threat model,
stated: this stops a wrong, failed, truncated, foreign, stale or planted verdict; it does not stop a forged one.

Post-run (item 7 and the fresh reviews): refused if the summary carries mutant_nosync or trace_clock != 0, names
another binary, another layout, no gated arm ran, or (bound) a brd leaf, another leaf class or another layer stack
than the verdict's.
Flush gate (item 1): on a write-back leaf, the leaf's FLUSH requests completed in the batch window (diskstats fields
19-20, from stamp.py) must be >= n x the gated arms run, else the batch is VOID (rc 3). It is a lower bound: other
processes' flushes pad it. A write-through leaf is labelled "not applicable: no volatile cache: no drive flush" (the
batch is labelled, not voided, and can never back a drive-flush sentence); a brd leaf, "not applicable: brd". On a
write-back leaf the blkflush record must also show every gated op issuing a flush request to the leaf. The claim the
counts allow is written as floor_claim_from_counts: "clean fsync (a bare flush ...) plus the measured delta" only on
ext4/XFS where the clean arm issued >= 0.95 flush requests per op at the leaf (review 2 item 2); else no bare-flush
baseline.
"""
import copy as _copy, glob, hashlib, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import v3cell  # noqa: E402

GOVERNOR_T3 = "performance"
CLOCKSOURCES = ["tsc", "arch_sys_counter"]


def load(p):
    with open(p) as f:
        return json.load(f)


def plan(cell, arch, leaf):
    import check  # check.py is importable: its work runs only under __main__
    return check.plan(cell, arch, leaf)


def gated_list():
    import check
    return check.GATED


def harness():
    import check
    return check.harness_sha256(HERE)


def leaf_class(sj):
    lf = sj.get("leaf")
    if isinstance(lf, dict) and lf.get("kind") == "brd":
        return "brd"
    wc = sj.get("leaf_write_cache")
    return "wb" if wc == "write back" else "wt" if wc == "write through" else None


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
    diff = sorted(f for f in mine if theirs.get(f) != mine[f] or mine[f] is None)
    if diff:  # a verdict vouches only for the checker that wrote it (fresh review I-M2)
        bad.append("harness: the verdict was made by a different fire-check harness than this run.sh's (%s differ)" %
                   ", ".join(diff[:6]))
    lc = v.get("leaf_class")
    if lc not in ("wb", "wt", "brd"):
        bad.append("leaf_class: %r" % lc)
    elif lc == "brd":
        bad.append("leaf_class brd: a brd fire-check is fire-check only and never binds a batch")
    # the verdict's ids against the plan for this cell and the verdict's own arch and leaf, so that a wrong arch or
    # leaf is refused by its own rule above and a truncated verdict by this one
    if cell in v3cell.CELLS and lc in ("wb", "wt") and checks:
        va = v.get("arch") if isinstance(v.get("arch"), str) else arch
        want = plan(cell, va, lc)
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
    bad = verdict_problems(v, cell, sha, arch, fstype)
    print(json.dumps({"ok": not bad, "reasons": bad, "verdict_sha256": hashlib.sha256(raw).hexdigest(),
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


def claim_from_counts(sj, per_leaf):
    lc = leaf_class(sj)
    if lc == "brd":
        return "none: a brd floor backs no sentence"
    if lc != "wb":
        return "no drive flush: the leaf reports write-through and receives no flush"
    clean = (per_leaf or {}).get("clean")
    if isinstance(clean, (int, float)) and clean >= 0.95 and sj.get("fstype") in ("ext4", "xfs"):
        return ("clean fsync (a bare flush: %.2f flush requests per op at the leaf) plus the measured delta, per stack"
                % clean)
    return ("no bare-flush baseline: the clean arm issued %s flush requests per op at the leaf on %s; quote the per-arm "
            "device flush counts only" % ("no" if clean is None else "%.2f" % clean, sj.get("fstype")))


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
    if mode == "bound":
        v = {}
        try:
            v = load(verdict_path) if verdict_path else {}
        except (OSError, ValueError) as e:
            refusals.append("verdict: unreadable after the run: %r" % e)
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
    st1 = None
    try:
        st1 = load(os.path.join(out, "stamp_end.json"))
    except (OSError, ValueError):
        refusals.append("no stamp_end.json")
    g = flush_gate(sj, st1)
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
        if lc == "wb":
            bk["outcome"] = "pass" if gated else "FAIL"
            for a in gated:
                zero = min([(arms.get(a, {}).get("devices") or {}).get(x, {}).get("zero_windows", arms.get(a, {}).get("ops", 0))
                            for x in leaf_names] or [arms.get(a, {}).get("ops", 0)])
                bk["arms"][a] = {"windows_without_a_leaf_flush_request": zero}
                if zero:
                    bk["outcome"] = "FAIL"
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
    merged["floor_claim_from_counts"] = claim_from_counts(sj, per_leaf if rep is not None else None)
    g["blkflush_leaf_gate"] = bk
    merged["flush_gate"] = g
    void = g["outcome"] == "FAIL" or bk["outcome"] == "FAIL"
    rc = 2 if refusals else 3 if void else 0
    gate = {"mode": mode, "cell": cell, "verdict": verdict_path, "refusals": refusals, "flush_gate": g, "rc": rc}
    with open(os.path.join(out, "gate.json"), "w") as f:
        json.dump(gate, f, indent=1, sort_keys=True)
    with open(pj, "w") as f:
        json.dump(merged, f, indent=1, sort_keys=True)
    for r in refusals:
        print("run.sh: REFUSED after the run: %s" % r, file=sys.stderr)
    if void and not refusals:
        print("run.sh: VOID: %s" % (g.get("why") or "a gated op issued no flush request to the write-back leaf"), file=sys.stderr)
    return rc


def fixture(out, cell, arch, leaf, sha, fstype, mod):
    ids = plan(cell, arch, leaf)
    checks = [{"id": i, "check": i, "pass": True, "detail": "planted"} for i in ids]
    v = {"cell": cell, "fstype": fstype, "arch": arch, "leaf_class": leaf, "v3floor_sha256": sha, "run_id": "fixture",
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
    elif mod:
        raise SystemExit("batchgate fixture: unknown MOD %r" % mod)
    with open(out, "w") as f:
        json.dump(v, f, indent=1)
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
    # item 2: the claim the counts allow (run 37476867864: btrfs clean 0.00, ext4/xfs clean 1.00 on write-back)
    wb = {"leaf_write_cache": "write back", "leaf": {"kind": "drive"}}
    chk("item 2 claim: ext4, clean 1.00 per op -> a bare flush plus the delta",
        claim_from_counts(dict(wb, fstype="ext4"), {"clean": 1.0}).startswith("clean fsync (a bare flush"))
    chk("item 2 claim: btrfs, clean 0.00 per op -> no bare-flush baseline",
        claim_from_counts(dict(wb, fstype="btrfs"), {"clean": 0.0}).startswith("no bare-flush baseline"))
    chk("item 2 claim: xfs with clean not run -> no bare-flush baseline",
        claim_from_counts(dict(wb, fstype="xfs"), {}).startswith("no bare-flush baseline"))
    chk("item 2 claim: write-through -> no drive flush",
        claim_from_counts({"leaf_write_cache": "write through", "leaf": {"kind": "drive"}, "fstype": "ext4"},
                          {"clean": 0.0}).startswith("no drive flush"))
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
            "checks": [{"id": i, "pass": True} for i in plan(cell, arch, "wb")]}
    good["pass"] = good["total"] = len(good["checks"])
    chk("item 8: a full-shape passing verdict binds", verdict_problems(good, cell, sha, arch, "xfs") == [],
        verdict_problems(good, cell, sha, arch, "xfs"))
    old = {"all_pass": True, "v3floor_sha256": sha, "fstype": "xfs", "arch": arch}
    chk("item 8: the old 4-field fixture is refused (no checks)",
        any(b.startswith("checks:") for b in verdict_problems(old, cell, sha, arch, "xfs")))

    def drop(v):
        v["checks"].pop(3)
        v["pass"] = v["total"] = len(v["checks"])

    def other_arch(v):  # an aarch64 verdict, whole and passing for aarch64, on this x86_64 batch
        v["arch"] = "aarch64"
        v["checks"] = [{"id": i, "pass": True} for i in plan(cell, "aarch64", "wb")]
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
                              ("no unplanted_refusals", lambda v: v.pop("unplanted_refusals"), "unplanted_refusals:")):
        v = _copy.deepcopy(good)
        mut(v)
        b = verdict_problems(v, cell, sha, arch, "xfs")
        chk("item 8: refused: %s, by its own rule ('%s'), and by no other" % (name, prefix),
            [x for x in b if x.startswith(prefix)] and all(x.startswith(prefix) for x in b), b)
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
    if len(a) in (7, 8) and a[0] == "fixture":
        return fixture(*a[1:7], a[7] if len(a) == 8 else "")
    if len(a) == 2 and a[0] == "self-test":
        return self_test(a[1])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
