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
| `gates/run_gates.sh` | Runs the engine lane's correctness gates from a prebuilt `turso_core` lib test binary with `TMPDIR` on the filesystem under test: the four `branch::` suite arms, C0, the C1 SIGKILL fire-checks and power-loss simulation, and E3. Banks one raw file per step and a verdict. |
| `competitors/` | Lane fastest-linux-comp (workflow `fastest-competitors.yml`): the Mac competitor tools ported to Linux (`bbload.c`, `clonebench.c`, `pg18.sh`, `dolt.sh`, `doltgres.sh`, `common.sh`, committed verbatim first, then ported in `#if` blocks), `build.sh` (pinned HdrHistogram_c and SQLite tarballs, sha256-checked), `fetch_dolt.sh` (Dolt 2.3.5 / Doltgres 1.3.3 release binaries, sha256-checked), `trace.sh` + `stracecount.py` (exact flush counts from `strace -f -C -y`, refusing their blind spots), `firecheck_strace.sh` (known-answer probes for that counter, run in every job), `run_system.sh` (one system on one filesystem: conncheck, idle control, load window, PG deferred checkpoint, functional checks and clone proof). |

Rules this tree keeps: never push `origin` (tursodatabase); never force-push; no secrets; a run
that collected zero tests, zero rows or no hardware record has not passed.
