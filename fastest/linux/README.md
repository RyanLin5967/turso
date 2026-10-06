# fastest/linux — the Linux side of FASTEST (lane fastest-linux)

This directory and the `.github/workflows/fastest-*.yml` files exist only on the fork's branch
`fastest-branching` (github.com/RyanLin5967/turso). Upstream's own workflows are moved, byte for
byte, to `.github/upstream-workflows-disabled/`, so a push of this branch runs only ours.

Nothing timed here is credited until the lead registers PREREG v1 with its T3 amendment. Until then
every workflow is a smoke run: function, crash gates and flush counts, never a quoted latency.

| Path | What |
|---|---|
| `hw/record.sh` | Runner hardware record, written with every run: CPU, memory, kernel, every block device's model and write-cache mode, the target filesystem and its mount, the Azure VM size, and fio write+fsync / write+fdatasync latency on the target directory. |
| `fs/mkloop.sh` | Makes a loop-backed ext4, XFS (reflink=1) or btrfs filesystem and proves what it made (fstype from the mount table, reflink refused on ext4 and accepted on XFS/btrfs). |
| `v3/` | The V3 device-floor probe (`v3floor.c`) and its batch runner (`run.sh`: explicit `V3_CELL`, an argument allowlist, a verdict binding, `blkflush.py`'s per-op device flush record, the batch gate), and its fire-check (`firecheck.sh` + `check.py`, workflow `fastest-v3.yml`). On the hosted runners' write-through disks a flush count proves only what was issued to the loop above them. |
| `gates/run_gates.sh` | Runs the engine lane's correctness gates from a prebuilt `turso_core` lib test binary with `TMPDIR` on the filesystem under test: the four `branch::` suite arms, C0, the C1 SIGKILL fire-checks and power-loss simulation, and E3. Banks one raw file per step and a verdict. |

Rules this tree keeps: never push `origin` (tursodatabase); never force-push; no secrets; a run
that collected zero tests, zero rows or no hardware record has not passed.
