#!/usr/bin/env python3
"""regen_f3.py -- regenerate the two banked F3 fixtures (run 37812355435) with THIS tree's tools, by script, never by
hand (V3 review 12 item 3: the fixtures still had 4e8acf0ae's report shape, so check.py --self-test is red by
construction until this runs; it replaces regen_td11.py, which named an absolute lane path and wrote into the live
lane tree). For each fixture: F3/blkflush/report.json from `blkflush.py report --windows raw.tsv --pid P --summary
summary.probe.json` on the fixture's own banked blkflush record (trace, start, stop, stats: run 37812355435's bytes),
then F3/summary.json and F3/gate.json from `batchgate.py post F3 ext4loop <exe_sha256> smoke` on a copy with both
removed. summary.probe.json (with its derived sync_fds, documented in each README) is never touched.

  python3 testdata/regen_f3.py [--check]
Without --check it writes the regenerated files in place and prints, per fixture, post's rc, refusals and voids, and
the sync record's unattributed counts; exit 1 unless post is rc 0 with no refusal and no void and every unattributed
count is 0. With --check it writes nothing and exits 1 when any regenerated file would differ from the committed one
(the fixtures are stale), 0 when they are current. Paths are relative to this file: run it from any directory.
"""
import json, os, shutil, subprocess, sys, tempfile

TD = os.path.dirname(os.path.abspath(__file__))
V3 = os.path.dirname(TD)
FIXTURES = ("f3-37812355435-x86-ext4loop", "f3-37812355435-arm-ext4loop")


def regen(f3, work):
    """the three regenerated files' text for the F3 dir f3, computed in work (a scratch copy) -> {name: text}"""
    w = os.path.join(work, "F3")
    shutil.copytree(f3, w)
    sj = json.load(open(os.path.join(w, "summary.probe.json")))
    r = subprocess.run([sys.executable, "-B", os.path.join(V3, "blkflush.py"), "report", os.path.join(w, "blkflush"),
                        "--windows", os.path.join(w, "raw.tsv"), "--pid", str(sj["pid"]), "--summary",
                        os.path.join(w, "summary.probe.json")], capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        raise SystemExit("blkflush report rc %d: %s" % (r.returncode, (r.stdout + r.stderr)[-400:]))
    open(os.path.join(w, "blkflush", "report.json"), "w").write(r.stdout)
    for f in ("summary.json", "gate.json"):
        os.remove(os.path.join(w, f))
    p = subprocess.run([sys.executable, "-B", os.path.join(V3, "batchgate.py"), "post", w, "ext4loop", sj["exe_sha256"],
                        "smoke"], capture_output=True, text=True, timeout=600)
    out = {n: open(os.path.join(w, *n.split("/"))).read() for n in ("blkflush/report.json", "summary.json", "gate.json")}
    return out, p.returncode


def main(argv):
    check = argv == ["--check"]
    if argv not in ([], ["--check"]):
        print(__doc__, file=sys.stderr)
        return 2
    bad = []
    for fx in FIXTURES:
        f3 = os.path.join(TD, fx, "F3")
        work = tempfile.mkdtemp(prefix="regen_f3-")
        try:
            new, prc = regen(f3, work)
            if check:
                stale = [n for n, t in new.items() if open(os.path.join(f3, *n.split("/"))).read() != t]
                print(fx, "stale:" if stale else "current", ", ".join(stale))
                if stale:
                    bad.append(fx)
                continue
            for n, t in new.items():
                open(os.path.join(f3, *n.split("/")), "w").write(t)
            g = json.loads(new["gate.json"])
            ua = (json.loads(new["blkflush/report.json"]).get("syscalls") or {}).get("unattributed") or {}
            print(fx, "post rc", prc, "refusals", g.get("refusals"), "voids", g.get("voids"), "unattributed", ua)
            if prc != 0 or g.get("refusals") or g.get("voids") or not ua or any(v != 0 for v in ua.values()):
                bad.append(fx)
        finally:
            shutil.rmtree(work)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
