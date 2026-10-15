#!/usr/bin/env python3
"""red.py offline BASE_CHECK_PY BASE_TRACE_GZ WORKDIR CELL OUT.json -- the red column's offline half (review 2 items
4 and 14): the BASE check.py (df4b39e53, fetched by the workflow with git show) run on planted inputs, to show what
it accepted or refused before the fix. Never a verdict on the new code; check.py copies OUT.json into red.json.

  item 4   a fixture fire-check OUT for cell xfs on /dev/nvme0n1 with a 1-layer flush path (the review's red fixture,
           a real T3 XFS data disk): the base check.py, which infers the layout from the fs name, must FAIL both its
           layout checks ("cell: xfs means a loop device" and the F3 flush-path check) -- that failure is the red.
  item 14  the base binary's own --trace-clock trace from this cell (BASE_TRACE_GZ, the base ALL arms at n = 40), with
           an fsync hidden in a split "<unfinished ...>" / "<... resumed>" pair after a nosync25 pwrite: the base
           parser drops both lines silently, so its sequence check ACCEPTS the planted trace -- that acceptance is the red.
"""
import gzip, json, os, re, subprocess, sys, tempfile

BASE_ALL = "append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,fdatasync4k,nosync25"


def load_base(path, out_dir, cell):
    src = open(path).read()
    if not src.rstrip().endswith("sys.exit(main())"):
        raise SystemExit("red.py: the base check.py does not end in sys.exit(main())")
    src = src.rstrip()[: -len("sys.exit(main())")]
    ns = {"__name__": "base_check"}
    argv = sys.argv
    sys.argv = ["check.py", out_dir, cell]
    try:
        exec(compile(src, path, "exec"), ns)
    finally:
        sys.argv = argv
    return ns


def item4(base_check):
    d = tempfile.mkdtemp(prefix="red4-")
    os.makedirs(os.path.join(d, "F3"))
    with open(os.path.join(d, "info.txt"), "w") as f:
        f.write("cell=xfs\nwork=/mnt/t3-xfs/v3fc\nwork_fstype=xfs\nwork_mount=/dev/nvme0n1 /mnt/t3-xfs rw,relatime,attr2,"
                "inode64,logbufs=8,logbsize=32k,noquota\narch=x86_64\npersonality_under_setarch_R=0040000\n")
    sj = {"fstype": "xfs", "mount_source": "/dev/nvme0n1", "n": 200, "mutant_nosync": 0, "trace_clock": 0,
          "refused_arms": {}, "arms": {}, "flush_control_arms": {}, "flush_control": "pass",
          "flush_path": [{"mount": "/mnt/t3-xfs", "fstype": "xfs", "source": "/dev/nvme0n1", "sys": "/sys/block/nvme0n1",
                          "loop_backing": ""}]}
    with open(os.path.join(d, "F3", "summary.json"), "w") as f:
        json.dump(sj, f)
    with open(os.path.join(d, "F3", "raw.tsv"), "w") as f:
        f.write("arm\ti\tns\n")
    r = subprocess.run([sys.executable, "-B", base_check, d, "xfs"], capture_output=True, text=True, timeout=300)
    v = json.load(open(os.path.join(d, "verdict.json")))
    cell = next((c for c in v["checks"] if c["check"].startswith("cell: xfs means")), None)
    f3 = next((c for c in v["checks"] if c["check"].startswith("F3 real run")), None)
    fp_bad = f3 is not None and "flush_path" in json.dumps(f3.get("detail"))
    red = cell is not None and cell["pass"] is False and fp_bad
    return {"item": "4", "claim": "the base check.py fails a real XFS data-disk cell (xfs on /dev/nvme0n1, 1 layer) on "
            "both layout checks", "red": red, "base_cell_check": cell, "base_f3_flush_path_failed": fp_bad,
            "base_rc": r.returncode}


def item14(base_check, trace_gz, work, cell):
    ns = load_base(base_check, tempfile.mkdtemp(prefix="red14-"), cell)
    ns["W"] = work
    text = gzip.open(trace_gz, "rt").read()
    arms, n = BASE_ALL.split(","), 40
    calls, other = ns["parse_trace"](text)
    clean = ns["sequence_problems"](calls, arms, n, False)
    lines = text.splitlines()
    pat = re.compile(r"^(\d+)\s+pwrite64\((\d+)<%s/nosync25>" % re.escape(work))
    hits = [k for k, l in enumerate(lines) if pat.match(l)]
    if len(hits) < 2:
        return {"item": "14", "red": None, "why": "no nosync25 pwrite in the loop of the base trace", "hits": len(hits)}
    j = hits[-1]  # the last one is inside the loop
    pid, fd = pat.match(lines[j]).groups()
    planted = lines[:j + 1] + ["%s fsync(%s<%s/nosync25> <unfinished ...>" % (pid, fd, work),
                               "%s <... fsync resumed>) = 0" % pid] + lines[j + 1:]
    c2, o2 = ns["parse_trace"]("\n".join(planted))
    bad = ns["sequence_problems"](c2, arms, n, False)
    return {"item": "14", "claim": "the base sequence check accepts a trace with an fsync hidden in a split line",
            "red": clean == [] and bad == [], "base_unplanted_problems": clean[:3], "base_planted_problems": bad[:3],
            "base_other_lines": len(o2)}


def main(a):
    if len(a) != 6 or a[0] != "offline":
        print(__doc__, file=sys.stderr)
        return 2
    _, base_check, trace_gz, work, cell, out = a
    res = {"base_check_py": base_check}
    try:
        res["item4"] = item4(base_check)
    except Exception as e:  # recorded: a red that could not be shown is not a red
        res["item4"] = {"item": "4", "red": None, "error": repr(e)}
    try:
        res["item14"] = item14(base_check, trace_gz, work, cell) if os.path.exists(trace_gz) else \
            {"item": "14", "red": None, "why": "no base trace"}
    except Exception as e:
        res["item14"] = {"item": "14", "red": None, "error": repr(e)}
    with open(out, "w") as f:
        json.dump(res, f, indent=1, default=str)
    for k in ("item4", "item14"):
        print("RED offline %s: %s" % (k, {True: "RED (bug shown at base)", False: "NOT RED", None: "n/a"}[res[k].get("red")]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
