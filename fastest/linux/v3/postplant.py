#!/usr/bin/env python3
"""postplant.py F3DIR OUTDIR CELL SHA PLANT [GATE] -- fire-check helper (fresh review I-M5, fourth review M3, M5, L9):
copy a finished run.sh batch, plant one field, and run `batchgate.py post` on the copy in BOUND mode against a verdict
made from the batch's own leaf class, layer stack, leaf record and virtualization, with binary.txt naming that
verdict's sha256, so that each post-run refusal is shown firing for its own reason and no other rule is tripped by
the copy itself. Exits with post's rc. GATE (default: this directory's batchgate.py) lets the red column run another
revision's gate on the same plant.

  traceclock   the summary says trace_clock=1                          -> "trace_clock=1 in the summary"
  cell         post is told another valid cell (loop <-> block)        -> "layout:"
  leaf         the verdict's leaf class differs from the batch's       -> "leaf class: the batch's leaf is"
  brd          the summary's leaf is brd                               -> "leaf brd:"
  stack        the verdict's layer 0 has another filesystem            -> "stack:"
  driver       the verdict's leaf driver differs from the batch's      -> "leaf drive:"
  virt         the verdict's virtualized differs from the batch's      -> "virtualization:"
  verdictswap  binary.txt names another verdict sha256 than the file's -> "verdict: changed during the run"
  verdictbad   the verdict file is not JSON                            -> "verdict: unreadable after the run"
  nostamp      stamp_end.json removed                                  -> "no stamp_end.json"
  noblk        blkflush/report.json removed                            -> "no blkflush report"
  blkrefused   blkflush/report.json says refused                       -> "blkflush refused:"
  leafkind     the summary's leaf kind is scsi_debug                   -> "leaf: the summary's leaf record"
  model        the verdict's drive model differs from the batch's      -> "leaf drive:"
  nofsync      one append25 window holds no fsync by the probe (A16)   -> VOID "fsync:"
  wtflush      the batch made write-through with its leaf counter > 0  -> "write-through leaf:"
  wtmismatch   the batch made write-through, the drive reporting wb    -> "drive report:"
  layerflush   layer 0 (write-back) lacks a flush-carrying request     -> VOID "flush-carrying:"
  plp          the batch declares PLP, the verdict's fire-check did not -> "plp:"
  unregistered rental mode (V3_REQUIRE_T3=1), nothing registered        -> "registration:"
  none         nothing planted: the control (no refusal at all)        -> refusals == []
"""
import copy, hashlib, json, os, shutil, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
OTHER_CELL = {"ext4": "ext4loop", "ext4loop": "ext4", "xfs": "xfsloop", "xfsloop": "xfs", "btrfs": "btrfsloop",
              "btrfsloop": "btrfs"}
PLANTS = ("traceclock", "cell", "leaf", "brd", "stack", "driver", "virt", "verdictswap", "verdictbad", "nostamp",
          "noblk", "blkrefused", "leafkind", "model", "nofsync", "wtflush", "wtmismatch", "layerflush", "plp",
          "unregistered", "none")
OTHER_FS = {"ext4": "xfs", "xfs": "btrfs", "btrfs": "ext4"}


def main(a):
    if len(a) not in (5, 6) or a[4] not in PLANTS:
        print(__doc__, file=sys.stderr)
        return 2
    f3, out, cell, sha, plant = a[:5]
    gate = a[5] if len(a) == 6 else os.path.join(HERE, "batchgate.py")
    if os.path.exists(out):
        print("postplant: %s exists" % out, file=sys.stderr)
        return 2
    shutil.copytree(f3, out)
    for p in ("summary.json", "gate.json"):
        if os.path.exists(os.path.join(out, p)):
            os.remove(os.path.join(out, p))
    with open(os.path.join(out, "summary.probe.json")) as f:
        sj = json.load(f)
    os.remove(os.path.join(out, "summary.probe.json"))  # post renames summary.json to it again
    lc = "brd" if (sj.get("leaf") or {}).get("kind") == "brd" else \
        "wb" if sj.get("leaf_write_cache") == "write back" else "wt"
    if plant in ("wtflush", "wtmismatch"):  # the batch made write-through first, so the verdict mirrors that class
        for k in ("write_cache", "drive_reports"):
            sj.setdefault("leaf", {})[k] = "write through"
        if sj["leaf"].get("kind") == "brd":  # a brd cell's batch made a write-through drive (run 37808860197)
            sj["leaf"].update(kind="drive", driver="sd", creditable=True)
        sj["leaf_write_cache"] = "write through"
        (sj.get("flush_path") or [{}])[-1]["write_cache"] = "write through"
        lc = "wt"
    v = {"leaf_class": lc, "box": {"plp": sj.get("plp")},
         "F3": {"flush_path": [[l.get("mount"), l.get("fstype"), l.get("source"), l.get("disk"),
                                l.get("write_cache")] for l in sj.get("flush_path") or []],
                "leaf": copy.deepcopy(sj.get("leaf")),
                "virtualization": copy.deepcopy(sj.get("virtualization"))}}
    c = cell
    vraw = None
    if plant == "traceclock":
        sj["trace_clock"] = 1
    elif plant == "cell":
        c = OTHER_CELL[cell]
    elif plant == "leaf":
        v["leaf_class"] = {"wb": "wt", "wt": "wb", "brd": "wt"}[lc]
    elif plant == "brd":
        sj.setdefault("leaf", {})["kind"] = "brd"
        v["leaf_class"] = "brd"
    elif plant == "stack":
        v["F3"]["flush_path"][0][1] = OTHER_FS.get(v["F3"]["flush_path"][0][1], "ext4")
    elif plant == "driver":
        vl = v["F3"]["leaf"] or {}
        vl["driver"] = "nvme" if vl.get("driver") == "virtio_blk" else "virtio_blk"
        v["F3"]["leaf"] = vl
    elif plant == "leafkind":
        sj.setdefault("leaf", {})["kind"] = "scsi_debug"
    elif plant == "model":
        vl = v["F3"]["leaf"] or {}
        vl["model"] = "%s (another model)" % vl.get("model")
        v["F3"]["leaf"] = vl
    elif plant == "virt":
        vz = v["F3"]["virtualization"] or {}
        vz["virtualized"] = False if vz.get("virtualized") is True else True
        v["F3"]["virtualization"] = vz
    elif plant == "verdictbad":
        vraw = b"{ not json"
    elif plant == "nostamp":
        os.remove(os.path.join(out, "stamp_end.json"))
    elif plant == "noblk":
        os.remove(os.path.join(out, "blkflush", "report.json"))
    elif plant == "blkrefused":
        with open(os.path.join(out, "blkflush", "report.json"), "w") as f:
            json.dump({"refused": "planted by postplant.py"}, f)
    elif plant in ("nofsync", "layerflush"):
        rp = os.path.join(out, "blkflush", "report.json")
        with open(rp) as f:
            rj = json.load(f)
        if plant == "nofsync":
            r = rj["syscalls"]["arms"]["append25"]
            r.update(syncs=r["syncs"] - 1, windows_without_a_sync=r.get("windows_without_a_sync", 0) + 1)
        else:
            d0 = sj["flush_path"][0]["disk"]
            dev = rj["windows"]["arms"]["append25"].setdefault("devices", {}).setdefault(d0, {"events": 0})
            dev["flush_carrying_zero_windows"] = dev.get("flush_carrying_zero_windows", 0) + 1
        with open(rp, "w") as f:
            json.dump(rj, f)
    elif plant == "wtflush":
        sp = os.path.join(out, "stamp_end.json")
        with open(sp) as f:
            st = json.load(f)
        leafd = os.path.basename(sj["flush_path"][-1].get("sys", ""))
        st.setdefault("diskstats_delta", {}).setdefault(leafd, {})["flushes"] = 7
        with open(sp, "w") as f:
            json.dump(st, f)
    elif plant == "wtmismatch":
        sj["leaf"]["drive_reports"] = "write back"
    elif plant == "plp":
        v["box"]["plp"] = "no" if sj.get("plp") == "yes" else "yes"
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(sj, f)
    vp = out + ".verdict.json"
    if vraw is None:
        vraw = json.dumps(v).encode()
    with open(vp, "wb") as f:
        f.write(vraw)
    vs = hashlib.sha256(vraw).hexdigest()
    if plant == "verdictswap":
        vs = "0" * 64
    bt = os.path.join(out, "binary.txt")
    lines = [l for l in (open(bt).read().splitlines() if os.path.exists(bt) else []) if not l.startswith("verdict_sha256=")]
    with open(bt, "w") as f:
        f.write("\n".join(lines + ["verdict_sha256=%s" % vs]) + "\n")
    env = dict(os.environ)
    env.pop("V3_REQUIRE_T3", None)
    if plant == "unregistered":
        env["V3_REQUIRE_T3"] = "1"  # its premise, nothing registered, made true in the copy (eighth review H5)
        sj.update(d0_threshold_ref=None, frame_arm=None, frame_arm_ref=None)
        with open(os.path.join(out, "summary.json"), "w") as f:
            json.dump(sj, f)
    r = subprocess.run([sys.executable, "-B", gate, "post", out, c, sha, "bound", vp], timeout=300, env=env)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
