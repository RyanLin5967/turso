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
| `competitors/` | Lane fastest-linux-comp (workflow `fastest-competitors.yml`): the Mac competitor tools ported to Linux (`bbload.c`, `clonebench.c`, `pg18.sh`, `dolt.sh`, `doltgres.sh`, `common.sh`, committed verbatim first, then ported in `#if` blocks), `build.sh` (pinned HdrHistogram_c and SQLite tarballs, sha256-checked), `fetch_dolt.sh` (Dolt 2.3.5 / Doltgres 1.3.3 release binaries, sha256-checked), `trace.sh` + `stracecount.py` (flush counts from `strace -f -C -y`: the -c table and the per-call lines must agree; blind spots refused, including an O_SYNC/O_DSYNC fd already open at the attach; every flush attributed to a process role through the attach roster, the clone/fork lines and bbload's `backends.tsv`), `firecheck_strace.sh` (12 known-answer probes for that counter, run in every job before it is used), `run_system.sh` (one system on one filesystem, 200 ops per cell at C=1 and C=4: conncheck, idle control, load window, PG deferred checkpoint, functional checks, clone proof with a copy negative control; amendment 14's PG FILE_COPY clone and WAL_LOG at D2 and at defaults, and Dolt/Doltgres variants (a), (b), (c)), `reduce.py` (one table per run; expected jobs from the run's own workflow, expected cells from each job's list; MISSING never skipped). |

Rules this tree keeps: never push `origin` (tursodatabase); never force-push; no secrets; a run
that collected zero tests, zero rows or no hardware record has not passed.
