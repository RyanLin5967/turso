#!/usr/bin/env python3
"""postplant.py F3DIR OUTDIR CELL SHA PLANT -- fire-check helper (fresh review I-M5): copy a finished run.sh batch,
plant one field, and run `batchgate.py post` on the copy in BOUND mode against a verdict made from the batch's own
leaf class and layer stack, so each post-run refusal is shown firing for its own reason. Exits with post's rc.

  traceclock  the summary says trace_clock=1                       -> "trace_clock=1 in the summary"
  cell        post is told another valid cell (loop <-> block)     -> "layout:"
  leaf        the verdict's leaf class differs from the batch's    -> "leaf class: the batch's leaf is"
  brd         the summary's leaf is brd                            -> "leaf brd:"
"""
import json, os, shutil, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
OTHER_CELL = {"ext4": "ext4loop", "ext4loop": "ext4", "xfs": "xfsloop", "xfsloop": "xfs", "btrfs": "btrfsloop",
              "btrfsloop": "btrfs"}


def main(a):
    if len(a) != 5 or a[4] not in ("traceclock", "cell", "leaf", "brd"):
        print(__doc__, file=sys.stderr)
        return 2
    f3, out, cell, sha, plant = a
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
    v = {"leaf_class": lc, "F3": {"flush_path": [[l.get("mount"), l.get("fstype"), l.get("source"), l.get("disk"),
                                                  l.get("write_cache")] for l in sj.get("flush_path") or []]}}
    c = cell
    if plant == "traceclock":
        sj["trace_clock"] = 1
    elif plant == "cell":
        c = OTHER_CELL[cell]
    elif plant == "leaf":
        v["leaf_class"] = {"wb": "wt", "wt": "wb", "brd": "wt"}[lc]
    elif plant == "brd":
        sj.setdefault("leaf", {})["kind"] = "brd"
        v["leaf_class"] = "brd"
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(sj, f)
    vp = out + ".verdict.json"
    with open(vp, "w") as f:
        json.dump(v, f)
    r = subprocess.run([sys.executable, "-B", os.path.join(HERE, "batchgate.py"), "post", out, c, sha, "bound", vp],
                       timeout=300)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
