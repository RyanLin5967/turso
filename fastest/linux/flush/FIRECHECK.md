# V1 Linux flush counter: fire-check record

> This copy lives beside the code. The raw output it cites (`runs/<id>/`) is banked in the artie-research repo at
> `frontier/fastest/linux/flush/runs/`; the CI artifacts (with binaries) stay on GitHub Actions for 90 days.

**Verdict: 53/53 registered arms PASS on all four jobs** of GitHub Actions run 37261096153 (fork
RyanLin5967/turso, branch `fastest-linux-flush`, commit `34ade9641`):

| Runner | Kernel | Filesystem under test | Result |
|---|---|---|---|
| ubuntu-24.04 (x86_64) | 6.17.0-1022-azure | ext4 (the runner's own; no reflink) | 53/53 |
| ubuntu-24.04 (x86_64) | 6.17.0-1022-azure | XFS loop, `reflink=1` | 53/53 |
| ubuntu-24.04-arm (aarch64) | 6.17.0-1022-azure | ext4 | 53/53 |
| ubuntu-24.04-arm (aarch64) | 6.17.0-1022-azure | XFS loop, `reflink=1` | 53/53 |

Toolchain on every job: gcc 13.3.0, glibc 2.39 (Ubuntu 2.39-0ubuntu8.9), liburing 2.5-1build1, go1.26.8, strace 6.8.

Raw output is in `runs/37261096153/<job>/`:
- `firecheck/verdict.json`;
- for every probe run: its output, every report (with and without each waiver), its by-mark rows and events, and the strace traces;
- `build/build-info.txt`, which has the sha256 of every binary built. The binaries themselves are not banked.

Earlier rounds are banked beside it: `runs/37224578145` (round 1), `runs/37242502824` (round 2),
`runs/37245712283` (round 3), `runs/37256242992` (round 4) and `runs/37258650057` (round 5, before the hidepid arm
was made to reach its path).

No timing is credited here. This record shows only that the counter counts, and refuses when it cannot.

## What it is

The counter lives in `fastest/linux/flush/` in the turso fork:
- `syncshim.so` is an LD_PRELOAD library for glibc.
- `v1run <run> cmd…` launches a counted run.
- `v1ctl create|report|bymark|events <run>` creates and reads a run.
- `build.sh` builds everything in CI and proves what it built.
- `firecheck.py` is the fire-check.
- `.github/workflows/fastest-flush.yml` runs both on push to `fastest-branching` or `fastest-linux-flush`, and on dispatch.

Counts are kept per process and per marked operation (`v1_set_mark`), in POSIX shared memory. They survive SIGKILL and can be read live. The kinds fall into three classes, which are never summed across:

| Class | Kinds |
|---|---|
| FLUSH | fsync, fdatasync, syncfs, sync, msync(MS_SYNC), and every write of ≥ 1 byte on an O_SYNC or O_DSYNC fd. Those writes go through write, `__write`, pwrite, pwrite64, `__pwrite64`, writev, pwritev, pwritev64, pwritev2, pwritev64v2, sendfile, sendfile64, splice or copy_file_range. pwritev2 with RWF_SYNC or RWF_DSYNC counts even on a plain fd. |
| WRITEBACK | sync_file_range, split by its flags (write+wait_after / write / no write; the event log keeps the exact flags), and msync without MS_SYNC. |
| CLONE | ioctl(FICLONE), ioctl(FICLONERANGE), copy_file_range. |

**`syscall(2)` is watched and never counted.** What it sees goes into the report this way:
- A counted kind made through `syscall(2)` is reported as MISSED.
- An io_uring or Linux AIO setup or submit through it is reported as async I/O. So is a call to liburing.so's `io_uring_queue_init[_params|_mem]` or `io_uring_setup`.
- Its opens, dups, fcntls and closes keep the O_SYNC tracker right.
- A fork-like clone through it gets the child a slot at birth.
- An execve or execveat through it is recorded in the exec table.

**A slot is claimed at load and at birth.** The birth claim covers the child of every fork(), `_Fork()` and fork-like clone, so a process that has counted nothing yet is still visible to the liveness check.

## `v1ctl report` exit codes

The first code that applies wins. Each `--allow-X` waives only its own condition. A report that returns 0 only because of a waiver says `WAIVED: <conditions>` (and JSON `"waived": [...]`), never `ok`.

| rc | Meaning | Waiver |
|---|---|---|
| 2 | Run missing or malformed. `create` also returns 2 when /dev/shm cannot reserve the whole object (posix_fallocate). | — |
| 4 | The root never attached: a static, setuid or non-glibc binary, such as a CGO_ENABLED=0 Go build. | — |
| 3 | No process attached. | — |
| 5 VOID | Slot overflow, exec-table overflow, or a write on an fd beyond the tracker. | none |
| 12 FOREIGN | Slots were claimed in another pid namespace, or the /proc the report reads is another namespace's mount or hides other users (hidepid). A slot /proc does not show is dead only if `kill(pid, 0)` says ESRCH. | none |
| 10 LIVE | A counted process (pid + start time, with any thread not a zombie) is still running, or a slot claim never finished. | `--allow-live` |
| 6 | An exec'd or spawned image never attached. | — |
| 7 | A Go binary attached. Its own calls are raw syscalls. | `--allow-go` |
| 9 UNCOUNTED | MISSED calls through syscall(2), async I/O, or an unresolved interpose. | `--allow-uncounted` |
| 8 ZERO | No attached process made one FLUSH-class call, counted or missed. | `--allow-zero` |
| 11 INFLIGHT | A counted call was entered and never returned (SIGKILL or thread cancel inside it). | `--allow-inflight` |
| 5 INCOMPLETE | Dropped or unfinished events. The counts are exact; per-mark attribution is not. | `--allow-incomplete` |

The shim itself exits 97 before `main` if `SYNCSHIM_RUN` is unset or its run is missing.

## Arms

Every expected count is computed from K alone, with two inputs from outside the counter:
- the kernel's F_SETFL behaviour, measured without the shim (Linux ignores O_DSYNC in F_SETFL: `setfl_truth=0` on all four jobs);
- the filesystem's reflink support, taken from `--fs` and checked against the mount table.

The pids the probes print only say which slot is which.

| Arm | What it holds on all four jobs |
|---|---|
| C-full, K=7 and K=23 | Exact per-process counts, fails and slot numbers for every process. Each `/bin/sh -c 'exec …'` child is one pid with two slots, and each forked exec child has a birth slot plus its image's slot. The processes are:<br>• the root, with all 14 kinds;<br>• a forked child whose marks, including IDLE marks, the parent sets;<br>• one child per exec route: posix_spawn, posix_spawnp, and fork + execve/execv/execvp/execvpe/execl/execle/execlp/fexecve/execveat;<br>• a vfork+execve child;<br>• a `/bin/sh -c 'exec …'` child;<br>• a vfork child that closes the parent's O_DSYNC fd (the parent's later K writes still count);<br>• a child writing on inherited O_DSYNC and O_SYNC fds;<br>• a child whose execve fails (a FAILED record, never "unattached").<br>Also exact: the per-mark rows, and the sync_file_range and msync flags of every event. There are 17 exec records and 28 slots, none Go, live, missed, in flight or unresolved. On ext4 every FICLONE/FICLONERANGE fails and is still counted (fail = K); on XFS none fails. |
| perturb ×6 | The C-full checker is fed the passing K=7 data with one planted change, and must fail for that change's own reason. The changes: root fsync +1; a per-mark row moved with totals unchanged; an exec record unattached; an sfr flag changed; a slot marked Go; a missed call. |
| mutants ×8, plus a v1ctl mutant | Drop one interpose:<br>• pwrite64 → root odsync_write −7;<br>• `__open_2` → root odsync_write −7;<br>• fdatasync → root and forked child fdatasync −7 each.<br>Break one piece of logic:<br>• fcntl through syscall(2) untracked → counted odsync_write −7 in the raw arm;<br>• raw pwritev2's flags read from the wrong argument → missed odsync_write −7;<br>• no slot claimed at fork → forkidle reads rc 0 while the child is alive by /proc, then fsync 8;<br>• a slow slot claim whose waiters claim again without re-checking → the threads child holds 8 slots;<br>• a slot claim that publishes the slot and the pid apart → vforkrace's parent is short and its vfork children hold the thread's fsyncs.<br>Each is caught by exactly its predicted deficit. |
| C-noise | Only calls that are not counted: F_GETFL/F_SETFL, plain writes, zero-length O_DSYNC writes, creat, creat64, `__close`, lseek and FIONREAD. Bad iovec pointers come back EFAULT (writev, pwritev2, `syscall(SYS_writev)`) and iovcnt −1 comes back EINVAL; neither is a SIGSEGV in the shim. Every kind is 0 and the report says ZERO (rc 8); with `--allow-zero` it says `WAIVED: zero`. |
| raw, K=7 and K=23 | **MISSED, as reported by the shim:** fsync K, fdatasync K, syncfs K, msync_SYNC K, sfr_write_wait K, FICLONE K, copy_file_range K, and odsync_write 5K. The odsync_write calls are syscall(2) pwrite64, pwritev2 RWF_DSYNC, copy_file_range into an O_DSYNC fd, sendfile into it, and writev on it. The report says rc 9.<br>**INVISIBLE:** K fsync issued as an inline `syscall` / `svc #0`. strace saw fsync 3K, fdatasync K, syncfs K and msync_sync K. That is exactly counted + missed, plus K inline fsyncs.<br>**COUNTED exactly:** the libc control calls, and libc writes on fds made through syscall(2): openat O_DSYNC, openat2 O_SYNC, dup, fcntl F_DUPFD, dup3, and on x86_64 also dup2 and open. That gives odsync_write 5K on aarch64 and 7K on x86_64, plus osync_write K. |
| uring | io_uring_setup, io_uring_enter and io_setup through syscall(2): async_io = 3, rc 9. |
| liburing, liburing init_mem | A program linked against liburing.so 2.5 does one libc fsync, then 7 IORING_OP_FSYNC through a ring, all completed. The ring is made by `io_uring_queue_init`, or by `io_uring_queue_init_mem` in a 2 MiB huge-page buffer. Either way the shim sees only the libc fsync (1) and the ring's setup (async_io 1), and refuses with rc 9. The ring's fsyncs themselves are invisible. |
| threads | A child is made by a raw-instruction clone, so no wrapper sees it and it has no slot at birth. Its 8 threads race their first counted calls: exactly one slot, fsync 56 and odsync_write 56. |
| Go, CGO_ENABLED=0, K=7 and 23 | Static: rc 4, nothing attached. strace saw every call exactly: fsync 2K, fdatasync K, syncfs K, sync 1, msync_sync K, sync_file_range 2K, msync_nosync K, copy_file_range K, ficlone 2K, rwf_sync_writes K, osync_opens 2. |
| Go, CGO_ENABLED=1, K=7 and 23 | rc 7. With `--allow-go`: 2 slots, both flagged Go, and the waiver is named. The root counts exactly its K cgo fsyncs and none of Go's raw calls; the os/exec child counts 0. strace saw fsync 3K. |
| sqlite | The system libsqlite3, one commit (DELETE journal, synchronous=FULL): shim fdatasync 8 = strace fdatasync 8, and everything else 0 = 0. |
| sigkill and start_ticks | Read live: rc 10, with fsync 7 already visible. The slot's start time is then overwritten in shared memory. Another value means another process, so the slot reads dead and the report gives rc 0. Zero means unknown, so it reads alive and the report gives rc 10. Restored, it reads rc 10. After SIGKILL: fsync 7, rc 0. |
| killmid | SIGKILL lands while the process is blocked inside a counted write; `/proc/<pid>/syscall` read 1 on x86_64 and 64 on aarch64. The report shows inflight 1 and INFLIGHT (rc 11). `--allow-incomplete` does not waive it; `--allow-inflight` does, and says so. |
| live, forkidle ×3, leaderexit | A background process that outlives the root keeps the report LIVE (rc 10) until it exits, then the run reads rc 0 with its 7 fsyncs.<br>A child of fork, `_Fork` or a syscall(2) clone that has counted nothing is LIVE from birth while /proc shows it alive. Once /proc shows it gone, the count is exact.<br>A thread-group leader that called pthread_exit (state Z) while its worker still runs is LIVE. The worker then opens an O_DSYNC file through syscall(2) openat2, which the shim reads on the worker's own thread, and writes 7 times. Afterwards the count is fsync 1 + 7 and odsync_write 7. |
| vforkrace | A thread fsyncs in a loop while 23 vfork children each fsync once. The vfork children share the parent's memory, where the shim publishes the slot the thread reads. The root pid holds exactly the thread's own count (967, 80, 1039 and 84 on the four jobs), every child exactly 1, and the total is the thread's count + 23. |
| hidepid | /proc is mounted with hidepid=invisible in a private mount namespace. The root reader drops CAP_SYS_PTRACE and leaves group 0 (hidepid exempts a reader in its `gid=`, which defaults to 0); the arm first proves it cannot see the probe (`HIDDEN`). That reader's report still reads the probe LIVE (rc 10), because `kill(pid, 0)` is not filtered. A v1ctl mutant without the kill check reads it dead (rc 0), so the arm is red without the fix. A report as the user refuses (rc 12). |
| foreign, foreign /proc | Run under `sudo unshare --pid --fork --mount-proc`, where the root is pid 1 inside: rc 12 while live and after exit, because the slot pidns differs from the reader's.<br>Run and report inside `unshare --pid --fork` without `--mount-proc`: the pidns matches, but the /proc read is the host's, so the report gives rc 12 (`proc_untrusted`). |
| create | A run of 4·10⁹ events is refused at create (rc 2, "cannot reserve"), and nothing is left in /dev/shm. |
| outside | The flushing process runs outside the v1run tree while the tree itself makes 3 FICLONE and 3 copy_file_range. Classes flush 0 / writeback 0 / clone 6, ZERO (rc 8). It is never `ok`. |
| bigfd | 7 writes on fd 1048600 give fd_untracked 7 and VOID (rc 5), which `--allow-incomplete` does not waive. |
| refusals | SYNCSHIM_RUN unset → 97; missing run → 97; nothing attached → rc 3; static root → rc 4. Static and env-stripped children → rc 6 (3 unattached). Event-log overflow → rc 5, waivable (`WAIVED: incomplete`), with exact counts. Slot overflow → rc 5, not waivable, with exact totals. Exec-table overflow → rc 5. A second v1run on one run → 2. |

**What `build.sh` proves before the fire-check runs:**
- The shim exports all 60 interposes.
- Each drop mutant lacks exactly its one interpose.
- Each logic mutant keeps them all and differs from the shim.
- The static probes are static and the dynamic ones dynamic.
- probe_uring links liburing.so.
- libc's own `__write`, `__open`, `__open_2`, …, `_Fork` are default-version exports; they are listed in `build-info.txt`.

## Blind spots (also stated in `syncshim.c`)

- **Raw syscall instructions are not seen, and nothing refuses.** This covers inline asm, static binaries, Rust's rustix `linux_raw` backend, a statically linked liburing ≥ 2.2, and the io-uring crate's `direct-syscall` feature. The raw arm shows K inline fsyncs that the shim does not see and strace does. Before trusting the shim for an engine binary, compare one run of it with the strace counter.
- **Go binaries (Dolt, Doltgres) are raw-syscall programs, with or without cgo.** CGO_ENABLED=0 never loads the shim (rc 4). CGO_ENABLED=1 attaches but sees only cgo calls (rc 7). Count Go with the strace counter.
- **Only processes descended from v1run's root are counted.** A server started by systemd, pg_ctlcluster, ssh or a container runtime, or one already running, is invisible. If the tree made no flush-class call, the run reads ZERO (rc 8). If a counted client and an uncounted server both flushed, the shim cannot know.
- **glibc-internal calls are not interposed.** This covers stdio on an O_SYNC fd, POSIX AIO helper threads, mkostemp's open, and the posix_spawn inside system() and popen().
- **A library dlopen'ed with RTLD_DEEPBIND bypasses the shim, undetected.** dlopen is not wrapped, because a wrapper would become the caller and break the caller's RUNPATH and `$ORIGIN`.
- **io_uring and AIO submissions are invisible as flushes.** Only their setup through syscall(2) or liburing.so refuses (rc 9).
- **Liveness sees a process only from its slot.**
  - A vfork child, a raw-instruction clone, and the child that system() or popen() spawns are unseen until they count something or load the shim.
  - Even a fork() child is unseen between the kernel creating it and its fork handler claiming the slot (a few syscalls).
  - The report must run in the counted processes' pid namespace, reading a /proc mounted for it that does not hide other users (else rc 12).
- **Where process_vm_readv is refused** (Docker's default seccomp profile without CAP_SYS_PTRACE), the shim reads iovec arrays, openat2's open_how and clone3's clone_args directly. A program passing a bad pointer there gets SIGSEGV instead of EFAULT.
- **The shim exports the liburing setup names in every process.** A program that probes for liburing with `dlsym(RTLD_DEFAULT, …)` without linking it finds them; a call returns -ENOSYS and the report refuses (rc 9).
- **O_SYNC tracking misses some fds:** fds from SCM_RIGHTS, raw-instruction opens and mkostemp. fallocate and ftruncate are not counted.
- **The exec guard matches a new image by pid and time.** A pid reused inside one run could hide an unattached image.
- **The mark is one run-wide value.** With concurrent clients or background processes, per-operation attribution is valid per slot only.
- **A vfork child's counted call before its exec gives its parent a second slot.** Per-pid counts stay exact. vforkrace shows it: 23 or 24 root slots for 23 children, depending on timing.
- **pthread_cancel inside a counted call leaves it in flight.** The report refuses (rc 11) even after a clean exit.
- **Capacity:** 65536 slots and 65536 exec records by default, and 2^20 events. Overflow is VOID. The whole object is reserved at create.
- **Limits of the fire-check itself:**
  - Sync writes and clone ops have no kernel-side witness. The strace counter traces neither writes nor their fd flags, and adding a write-tracking parser would make a second strace instrument. Their expectations come from the probe's construction, with every call's return value checked.
  - open_by_handle_at is not exercised; it needs CAP_DAC_READ_SEARCH.
  - creat's and `__close`'s bit clearing cannot be observed by design: a stale bit is re-checked before any write counts.
  - The report's re-read of `slots_used` after its scan (a fork child claimed mid-scan) has no deterministic fixture: it is a timing race.

## How it got here

- **Round 1** (run 37224578145, 25/25 at `2961eb381`): a port of the stopped agent's quarantined tree (`b49c289f8`), re-derived. `v1strace.py` was dropped: it was a second strace instrument. The witness here is lane fastest-linux-comp's `stracecount.py` + `trace.sh`, pinned by sha; since round 2 that is `2fd6a83b9`, whose own fire-check passed 12/12 in all 20 jobs of run 37225919842.
- **Round 2** (run 37242502824, 36/36 at `b63ca12f1`): a fresh-context adversarial review of round 1 found the report said rc 0 where it knew, or could know, it under-counted. The cases were syscall(2) misses, io_uring, a server outside the tree, a live run, a SIGKILL inside a call, and a vfork child's close erasing its parent's tracking. It also found a vacuous checker arm.
- **Round 3** (run 37245712283, 47/47 at `9880cc5a4`): a second fresh-context review, of round 2, found more such rc-0 paths:
  - ZERO was defeated by a clone call;
  - `--allow-incomplete` hid calls left in flight;
  - a zombie leader with live threads read as dead;
  - a foreign pid namespace went unnoticed;
  - a /dev/shm too small for the run could cause a SIGBUS;
  - syscall(2) decoding and the fork-time claim were untested;
  - liburing went undetected;
  - an iovec pointer was trusted.

  Each became a refusal plus an arm that makes it fire, or a stated blind spot. Two CI iterations fixed the probe itself: gcc refused the deliberately bad iovec, and the forkidle child held the launcher's stdout.
- **Round 4** (run 37256242992, 50/50 at `17a687789`): a third fresh-context review, of round 3, found:
  - a report could read another namespace's /proc;
  - `io_uring_queue_init_mem` went undetected;
  - untrusted reads failed unsafe after the leader exited, or under seccomp;
  - the threads arm had become vacuous, because `_Fork` now claims at birth (and the claim order had a real race);
  - the no_fork_claim mutant's evidence also fitted a dead child.

  Each is fixed, and each has an arm or a mutant that fires on the old behaviour.
- **Round 5** (run 37258650057, 53/53 at `9c0ac9fec`): a fourth fresh-context review, of round 4, found two hard-to-reach rc-0 paths:
  - publishing `{slot, pid}` as two words let a parent thread count into its vfork child's slot;
  - a report run as root without CAP_SYS_PTRACE on a hidepid /proc read live processes as dead.

  The pair is now one 64-bit word, published and read atomically, and `kill(pid, 0)` confirms every "dead". vforkrace and hidepid make both fire, and the split_publish mutant shows vforkrace catches a torn publication.
- **Round 5 review** (of `17a687789..9c0ac9fec`) found nothing in classes (a) or (b). It found one vacuous arm (c): hidepid's root reader was in group 0, which hidepid exempts, so the arm never reached the new kill check. Commit `34ade9641` makes that reader blind, proves it is, and adds the no-kill-check v1ctl mutant. Run 37261096153 (53/53): the reader saw `HIDDEN`, real v1ctl gave rc 10, the mutant gave rc 0.
- **Not reviewed in a sixth round:** `34ade9641` changes no shim source. It changes the hidepid fixture (firecheck.py), adds the mutant build (build.sh), and in v1ctl.c only wraps the existing kill check in `#ifndef V1_MUTANT_NO_KILL_CHECK` (2 lines, `git diff 9c0ac9fec 34ade9641`). The default build's logic is the one the round-5 review read. Its binary is not identical, because `-g` line numbers shift; that was not measured.
