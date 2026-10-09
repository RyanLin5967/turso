# blk-37845193906-arm-xfs

Run 37845193906 (turso f99ff6546), cell arm-xfs, F3: blkflush's own record (trace.txt.gz, start.json, stop.json,
stats.json) and the batch's raw.tsv, copied unchanged from artie frontier/fastest/linux/v3/runs/37845193906/
v3-ubuntu-24.04-arm-xfs/firecheck/F3/. The probe of that run recorded no sync_fds, so sync_fds.json is DERIVED:
from the same cell's F1b real-all strace (strace -y prints each fsync/fdatasync as fd<path>), per arm by check.OP's
definitions (mk_blk_testdata.py in the V3 successor's scratchpad, committed with this directory's message). The
F3 trace's wholly-in-window events carry the same fds per arm on all 12 cells of that run. Input to blkflush.py
self-test (tenth review HIGH 1): report() on it must give nosync25 0 syncs and every gated window its own sync,
and dropping one gated window's own sync, where a neighbour's event overlaps that window, must show.
