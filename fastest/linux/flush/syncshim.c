/* syncshim.c -- V1 flush counter for Linux, an LD_PRELOAD interposition library for glibc (x86_64, aarch64).
 *
 * COUNTS, per process and per marked operation (layout, kind classes and reader API: syncshim.h):
 *   FLUSH      fsync, fdatasync, syncfs, sync, msync(MS_SYNC), and every write of >= 1 byte on an O_SYNC or O_DSYNC
 *              fd through write, __write, pwrite, pwrite64, __pwrite64, writev, pwritev, pwritev64, pwritev2,
 *              pwritev64v2, sendfile, sendfile64, splice and copy_file_range (pwritev2's per-call RWF_SYNC / RWF_DSYNC
 *              count too, even on a plain fd; the stronger of fd mode and call flag wins);
 *   WRITEBACK  sync_file_range split by its flags (write+wait_after / write only / no write; the event log keeps the
 *              exact flags) and msync without MS_SYNC;
 *   CLONE      ioctl(FICLONE), ioctl(FICLONERANGE), copy_file_range.
 * Launch with `v1run <run> cmd...`; read with `v1ctl report|bymark|events <run>`.
 *
 * WATCHES, never counts, the generic syscall(2) entry point: a call of a counted kind made through it is tallied
 * in slot->missed[kind] (the shim missed it, and says so: `v1ctl report` refuses with rc 9), an io_uring or Linux AIO
 * setup/submit through it in slot->async_io (rc 9: flushes may go through the ring), and its opens, dups, fcntls and
 * closes keep the O_SYNC tracker right.
 *
 * REFUSES rather than counting nothing: if SYNCSHIM_RUN is unset, or the run's shm is missing or malformed, the
 * process writes the reason to stderr and _exit(97)s before main. `v1ctl report` lists every refusal (v1ctl.c).
 *
 * BLIND SPOTS (stated here because the shim cannot count them; FIRECHECK.md has the measured arms):
 *  - RAW SYSCALL INSTRUCTIONS ARE NOT SEEN AT ALL: inline asm, a static binary, Rust's rustix linux_raw backend,
 *    liburing >= 2.2 (its own inline syscalls), the io-uring crate's "direct-syscall" feature. Nothing refuses; the
 *    fire-check's raw arm makes K fsync as an inline `syscall` / `svc #0` and shows the shim neither counts nor
 *    reports them while the strace witness sees them. Calls through syscall(2) ARE reported (missed[], rc 9).
 *    Before trusting the shim for an engine binary, compare one run of it with the strace counter.
 *  - GO BINARIES (Dolt, Doltgres) ARE RAW-SYSCALL PROGRAMS: on Linux the Go runtime and the os, syscall and
 *    x/sys/unix packages issue their syscalls directly, WITH OR WITHOUT cgo, so no flush a Go program makes through
 *    Go code passes through libc. CGO_ENABLED=0 binaries are static and never load the shim (report rc 4 as the
 *    root; a child exec'd by Go is not even recorded, because Go's os/exec forks and execs by raw syscall).
 *    CGO_ENABLED=1 binaries load the shim and attach, and the shim then sees ONLY what the program calls through
 *    cgo into C: their slot carries V1_SLOT_GO and `v1ctl report` refuses (rc 7). THE INSTRUMENT FOR A GO BINARY IS
 *    THE STRACE COUNTER (fastest/linux/competitors/stracecount.py with trace.sh). Go is detected by an ELF section
 *    .go.buildinfo / .note.go.buildid or a PT_NOTE owned by "Go"; a Go binary stripped of all three reads as C.
 *  - ONLY PROCESSES DESCENDED FROM v1run's ROOT ARE COUNTED (through fork, exec and spawn with LD_PRELOAD intact). A
 *    server started outside the tree -- by systemd, pg_ctlcluster, ssh, a container runtime, or already running --
 *    is not counted at all. Start servers directly under v1run. A run where every attached process counted nothing
 *    is refused (rc 8) unless --allow-zero; one where a counted client and an uncounted server both flushed is not.
 *  - glibc-internal calls are not interposed: stdio flushing a FILE* on an O_SYNC/O_DSYNC fd, POSIX AIO
 *    (aio_fsync, aio_write: glibc's helper threads call its internal fsync/pwrite), mkostemp's open, and the
 *    posix_spawn that system() and popen() make (the /bin/sh they start attaches on its own if it is dynamic).
 *  - A library dlopen'ed with RTLD_DEEPBIND binds its libc calls in its own scope and bypasses the shim, undetected.
 *    (dlopen is not wrapped: a wrapper would become the "caller", breaking the caller's RUNPATH and $ORIGIN.)
 *  - io_uring and Linux AIO submissions are invisible as flushes. Their setup refuses (rc 9) when it goes through
 *    syscall(2) (the io-uring crate, liburing < 2.2) or through liburing.so's io_uring_queue_init[_params] /
 *    io_uring_setup (a dynamically linked liburing); a statically linked liburing, the io-uring crate's
 *    direct-syscall feature or rustix is undetected (above). The strace counter flags both (verdict INCOMPLETE).
 *  - LIVENESS (rc 10) sees a process from its slot. A child of fork(), _Fork() or a fork-like clone through
 *    syscall(2) has one at birth; a vfork child, a raw-instruction clone, and the child that system() or popen()
 *    spawns have none until they count something or their image loads the shim, so in that window they are unseen.
 *    The report must run in the pid namespace the counted processes ran in (else rc 12).
 *  - A vfork child that makes a counted call before its exec claims a slot in memory it shares with its parent, so
 *    the parent claims a second slot under the same pid; per-pid counts stay exact.
 *  - A thread cancelled (pthread_cancel) inside a counted call leaves it in flight: the report refuses (rc 11) even
 *    after a clean exit, because whether that call reached the disk is unknown.
 *  - Capacity: --slots (default 65536; one per process, two for a fork that execs), --execs (65536) and --events
 *    (2^20). A slot or exec-table overflow is VOID (rc 5); size them for fork-heavy programs. The whole object is
 *    reserved at create (posix_fallocate), so a /dev/shm too small refuses at create, never SIGBUS in the program.
 *  - O_SYNC/O_DSYNC tracking follows open, open64, __open, __open64, openat, openat64, __open_2, __open64_2,
 *    __openat_2, __openat64_2, creat, creat64, open_by_handle_at, dup, dup2, __dup2, dup3, fcntl/fcntl64/__fcntl
 *    (F_DUPFD, F_DUPFD_CLOEXEC, F_SETFL), close/__close, and open/openat/openat2/creat/dup/dup2/dup3/fcntl/close
 *    through syscall(2); fds inherited across exec are seeded at load from /proc/self/fd. An fd made O_SYNC by any
 *    other path (SCM_RIGHTS, a raw-instruction open, mkostemp) is missed. A stale bit is never counted: each
 *    candidate write re-reads F_GETFL first (so close_range/closefrom, which bypass close, are harmless). Only the
 *    process that owns the tracking bitmap clears bits: a vfork child shares its parent's memory, and its close or
 *    dup2 must not erase the parent's tracking (a child made by _Fork or a raw clone never clears either: harmless).
 *    Linux ignores O_SYNC/O_DSYNC in F_SETFL; the fire-check measures that without the shim. Writes on fds
 *    >= V1_FD_BITS cannot be classified; each is tallied in slot->fd_untracked and the report then refuses (rc 5).
 *    fallocate and ftruncate on an O_SYNC fd are not counted.
 *  - The exec guard covers the exec family and posix_spawn/posix_spawnp called through libc. Execs made by raw
 *    syscalls (Go) or inside glibc (system, popen, pidfd_spawn) are not recorded. It matches a new image by pid and
 *    time; a pid reused inside one run could hide an unattached image (pid_max is 4194304 on these kernels).
 *  - A call is recorded after it returns; slot->inflight counts calls entered and not returned, so a process
 *    SIGKILLed inside one leaves it > 0 and the report refuses (rc 11) rather than show the count one short.
 *  - setuid and other secure-exec binaries ignore LD_PRELOAD: they never attach (rc 4 / rc 6, never zero).
 *  - A signal handler that makes a counted call while its own thread holds the one-time slot-claim lock spins
 *    forever. The claim is made once per process (at load, at fork, or at the first counted call after _Fork).
 */
#define _GNU_SOURCE
#include "syncshim.h"
#include <dirent.h>
#include <dlfcn.h>
#include <elf.h>
#include <pthread.h>
#include <sched.h>
#include <spawn.h>
#include <stdarg.h>
#include <sys/ioctl.h>
#include <sys/sendfile.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>

extern char **environ;

/* ---- the real functions (dlsym RTLD_NEXT; resolved eagerly in the constructor, lazily before it) ---- */
#define REAL(ret, name, params) static ret(*real_##name) params
REAL(int, fsync, (int));
REAL(int, fdatasync, (int));
REAL(int, sync_file_range, (int, __off64_t, __off64_t, unsigned int));
REAL(int, syncfs, (int));
REAL(void, sync, (void));
REAL(int, msync, (void *, size_t, int));
REAL(ssize_t, write, (int, const void *, size_t));
REAL(ssize_t, __write, (int, const void *, size_t));
REAL(ssize_t, pwrite, (int, const void *, size_t, __off_t));
REAL(ssize_t, pwrite64, (int, const void *, size_t, __off64_t));
REAL(ssize_t, __pwrite64, (int, const void *, size_t, __off64_t));
REAL(ssize_t, writev, (int, const struct iovec *, int));
REAL(ssize_t, pwritev, (int, const struct iovec *, int, __off_t));
REAL(ssize_t, pwritev64, (int, const struct iovec *, int, __off64_t));
REAL(ssize_t, pwritev2, (int, const struct iovec *, int, __off_t, int));
REAL(ssize_t, pwritev64v2, (int, const struct iovec *, int, __off64_t, int));
REAL(ssize_t, sendfile, (int, int, off_t *, size_t));
REAL(ssize_t, sendfile64, (int, int, __off64_t *, size_t));
REAL(ssize_t, splice, (int, __off64_t *, int, __off64_t *, size_t, unsigned int));
REAL(int, ioctl, (int, unsigned long, ...));
REAL(ssize_t, copy_file_range, (int, __off64_t *, int, __off64_t *, size_t, unsigned int));
REAL(long, syscall, (long, ...));
REAL(int, open, (const char *, int, ...));
REAL(int, open64, (const char *, int, ...));
REAL(int, __open, (const char *, int, ...));
REAL(int, __open64, (const char *, int, ...));
REAL(int, openat, (int, const char *, int, ...));
REAL(int, openat64, (int, const char *, int, ...));
REAL(int, __open_2, (const char *, int));
REAL(int, __open64_2, (const char *, int));
REAL(int, __openat_2, (int, const char *, int));
REAL(int, __openat64_2, (int, const char *, int));
REAL(int, creat, (const char *, mode_t));
REAL(int, creat64, (const char *, mode_t));
REAL(int, open_by_handle_at, (int, struct file_handle *, int));
REAL(int, fcntl, (int, int, ...));
REAL(int, fcntl64, (int, int, ...));
REAL(int, __fcntl, (int, int, ...));
REAL(int, dup, (int));
REAL(int, dup2, (int, int));
REAL(int, __dup2, (int, int));
REAL(int, dup3, (int, int, int));
REAL(int, close, (int));
REAL(int, __close, (int));
REAL(int, execve, (const char *, char *const[], char *const[]));
REAL(int, execv, (const char *, char *const[]));
REAL(int, execvp, (const char *, char *const[]));
REAL(int, execvpe, (const char *, char *const[], char *const[]));
REAL(int, fexecve, (int, char *const[], char *const[]));
REAL(int, execveat, (int, const char *, char *const[], char *const[], int));
REAL(int, posix_spawn, (pid_t *, const char *, const posix_spawn_file_actions_t *, const posix_spawnattr_t *,
                        char *const[], char *const[]));
REAL(int, posix_spawnp, (pid_t *, const char *, const posix_spawn_file_actions_t *, const posix_spawnattr_t *,
                         char *const[], char *const[]));

/* The table the constructor resolves; bit i of slot->unresolved = entry i not found (the report refuses, rc 9). */
#define T(name) {#name, (void *)&real_##name}
static const struct { const char *name; void *ptr; } v1_tab[] = {
    T(fsync), T(fdatasync), T(sync_file_range), T(syncfs), T(sync), T(msync), T(write), T(__write), T(pwrite),
    T(pwrite64), T(__pwrite64), T(writev), T(pwritev), T(pwritev64), T(pwritev2), T(pwritev64v2), T(sendfile),
    T(sendfile64), T(splice), T(ioctl), T(copy_file_range), T(syscall), T(open), T(open64), T(__open), T(__open64),
    T(openat), T(openat64), T(__open_2), T(__open64_2), T(__openat_2), T(__openat64_2), T(creat), T(creat64),
    T(open_by_handle_at), T(fcntl), T(fcntl64), T(__fcntl), T(dup), T(dup2), T(__dup2), T(dup3), T(close),
    T(__close), T(execve), T(execv), T(execvp), T(execvpe), T(fexecve), T(execveat), T(posix_spawn), T(posix_spawnp)};
#undef T
_Static_assert(sizeof v1_tab / sizeof *v1_tab <= 64, "unresolved mask is 64 bits");
static uint64_t g_unresolved;

#define RESOLVE(name)                                                                       \
    do {                                                                                    \
        if (__builtin_expect(real_##name == NULL, 0))                                      \
            real_##name = (__typeof__(real_##name))dlsym(RTLD_NEXT, #name);                \
    } while (0)
/* An alias glibc may not export by that name (or not at all): fall back to the function it aliases. */
#define RESOLVE_ALT(name, alt)                                                              \
    do {                                                                                    \
        RESOLVE(name);                                                                      \
        if (__builtin_expect(real_##name == NULL, 0)) {                                    \
            RESOLVE(alt);                                                                   \
            real_##name = (__typeof__(real_##name))real_##alt;                             \
        }                                                                                   \
    } while (0)
/* A function an older glibc may lack: a caller reaches this wrapper only through dlsym then, so say ENOSYS. */
#define RESOLVE_OR(name, fallback)                                                          \
    do {                                                                                    \
        RESOLVE(name);                                                                      \
        if (real_##name == NULL) { errno = ENOSYS; return (fallback); }                     \
    } while (0)

static void v1_resolve_all(void) {
    uint64_t miss = 0;
    for (size_t i = 0; i < sizeof v1_tab / sizeof *v1_tab; i++) {
        void *p = dlsym(RTLD_NEXT, v1_tab[i].name);
        if (!p) miss |= 1ULL << i;
        else memcpy(v1_tab[i].ptr, &p, sizeof p); /* memcpy: no aliasing of a function-pointer object as void * */
    }
    g_unresolved = miss;
}

/* ---- state ---- */
static v1_hdr *g_hdr;
static v1_slot *g_slot;       /* this process's slot; re-claimed whenever getpid() differs from g_slot_pid */
static int32_t g_slot_pid;
static int32_t g_bitmap_pid;  /* the process whose fd table g_osync describes: only it clears bits */
static int g_claim_lock;
static int g_is_go;
static uint8_t g_osync[V1_FD_BITS / 8]; /* 1 = this fd MAY be O_SYNC/O_DSYNC (re-checked before counting) */
static pthread_once_t g_once = PTHREAD_ONCE_INIT;

static void v1_die(const char *why) {
    char buf[320];
    int n = snprintf(buf, sizeof buf, "syncshim: REFUSING to run uncounted (pid %d): %s\n", (int)getpid(), why);
    RESOLVE(write);
    if (n > 0 && real_write) (void)real_write(2, buf, (size_t)(n < (int)sizeof buf ? n : (int)sizeof buf - 1));
    _exit(97);
}

/* ---- Go detection: an ELF section .go.buildinfo / .note.go.buildid, or a PT_NOTE whose owner is "Go" ---- */
static int v1_pread_all(int fd, void *buf, size_t n, off_t off) {
    return pread(fd, buf, n, off) == (ssize_t)n ? 0 : -1;
}
static int v1_exe_is_go(void) {
    RESOLVE(open);
    RESOLVE(close);
    int fd = real_open("/proc/self/exe", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return 0;
    int go = 0;
    Elf64_Ehdr eh;
    if (v1_pread_all(fd, &eh, sizeof eh, 0) != 0 || memcmp(eh.e_ident, ELFMAG, SELFMAG) != 0 ||
        eh.e_ident[EI_CLASS] != ELFCLASS64)
        goto out;
    if (eh.e_phentsize == sizeof(Elf64_Phdr) && eh.e_phnum > 0 && eh.e_phnum < 512) {
        for (int i = 0; i < eh.e_phnum && !go; i++) {
            Elf64_Phdr ph;
            if (v1_pread_all(fd, &ph, sizeof ph, (off_t)(eh.e_phoff + (uint64_t)i * sizeof ph)) != 0) break;
            if (ph.p_type != PT_NOTE || ph.p_filesz == 0 || ph.p_filesz > 65536) continue;
            unsigned char *b = malloc(ph.p_filesz);
            if (!b) break;
            if (v1_pread_all(fd, b, ph.p_filesz, (off_t)ph.p_offset) == 0) {
                size_t p = 0;
                while (p + 12 <= ph.p_filesz) {
                    uint32_t namesz, descsz;
                    memcpy(&namesz, b + p, 4);
                    memcpy(&descsz, b + p + 4, 4);
                    size_t nm = p + 12, nend = nm + ((namesz + 3u) & ~3u), dend = nend + ((descsz + 3u) & ~3u);
                    if (nend > ph.p_filesz || dend > ph.p_filesz) break;
                    if (namesz == 3 && memcmp(b + nm, "Go\0", 3) == 0) { go = 1; break; }
                    p = dend;
                }
            }
            free(b);
        }
    }
    if (!go && eh.e_shentsize == sizeof(Elf64_Shdr) && eh.e_shnum > 0 && eh.e_shnum < 65280 &&
        eh.e_shstrndx < eh.e_shnum) {
        Elf64_Shdr strsh;
        if (v1_pread_all(fd, &strsh, sizeof strsh, (off_t)(eh.e_shoff + (uint64_t)eh.e_shstrndx * sizeof strsh)) == 0 &&
            strsh.sh_size > 0 && strsh.sh_size < (1u << 20)) {
            char *names = malloc(strsh.sh_size + 1);
            if (names && v1_pread_all(fd, names, strsh.sh_size, (off_t)strsh.sh_offset) == 0) {
                names[strsh.sh_size] = 0;
                for (int i = 0; i < eh.e_shnum && !go; i++) {
                    Elf64_Shdr sh;
                    if (v1_pread_all(fd, &sh, sizeof sh, (off_t)(eh.e_shoff + (uint64_t)i * sizeof sh)) != 0) break;
                    if (sh.sh_name >= strsh.sh_size) continue;
                    const char *nmp = names + sh.sh_name;
                    if (!strcmp(nmp, ".go.buildinfo") || !strcmp(nmp, ".note.go.buildid")) go = 1;
                }
            }
            free(names);
        }
    }
out:
    real_close(fd);
    return go;
}

/* This process's starttime, from /proc/self/stat (async-signal-safe: it runs in the fork child handler too). */
static uint64_t v1_self_start(void) {
    char buf[1024];
    int fd = real_open("/proc/self/stat", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return 0;
    ssize_t n = read(fd, buf, sizeof buf);
    real_close(fd);
    char st;
    uint64_t t = 0;
    if (n <= 0 || v1_parse_stat(buf, (size_t)n, &st, &t) != 0) return 0;
    return t;
}

/* ---- slots ---- */
static void v1_claim_slot(int32_t me) {
    uint64_t i = __atomic_fetch_add(&g_hdr->slots_used, 1, __ATOMIC_SEQ_CST) + 1; /* slot 0 = overflow */
    v1_slot *s;
    if (i >= g_hdr->nslots) {
        __atomic_fetch_add(&g_hdr->slot_overflow, 1, __ATOMIC_SEQ_CST);
        s = &v1_slots(g_hdr)[0];
    } else {
        s = &v1_slots(g_hdr)[i];
        char path[512];
        ssize_t n = readlink("/proc/self/exe", path, sizeof path - 1);
        if (n <= 0) { path[0] = '?'; n = 1; }
        path[n] = 0;
        size_t from = (size_t)n >= sizeof s->exe ? (size_t)n - (sizeof s->exe - 1) : 0, len = (size_t)n - from;
        memcpy(s->exe, path + from, len); /* the tail of the path, which is the informative end */
        s->exe[len] = 0;
        s->ppid = (int32_t)getppid();
        s->attach_ns = v1_now();
        s->start_ticks = v1_self_start();
        struct stat ns;
        s->pidns = stat("/proc/self/ns/pid", &ns) == 0 ? (uint64_t)ns.st_ino : 0;
        s->flags = g_is_go ? V1_SLOT_GO : 0;
        s->unresolved = g_unresolved;
        __atomic_store_n(&s->pid, me, __ATOMIC_RELEASE); /* published last */
    }
    __atomic_store_n(&g_slot_pid, me, __ATOMIC_RELEASE);
    __atomic_store_n(&g_slot, s, __ATOMIC_RELEASE);
}

/* The calling process's slot. getpid() on every call, so a child made by vfork, _Fork or a raw clone (no fork
 * handler) still gets its own slot. Counted calls are flushes, so the syscall is cheap beside them. */
static v1_slot *v1_slot_get(int32_t *pid_out) {
    int32_t me = (int32_t)getpid();
    *pid_out = me;
    v1_slot *s = __atomic_load_n(&g_slot, __ATOMIC_ACQUIRE);
    if (__builtin_expect(s != NULL && __atomic_load_n(&g_slot_pid, __ATOMIC_ACQUIRE) == me, 1)) return s;
    while (__atomic_exchange_n(&g_claim_lock, 1, __ATOMIC_ACQUIRE)) sched_yield();
    s = __atomic_load_n(&g_slot, __ATOMIC_ACQUIRE);
    if (!(s != NULL && __atomic_load_n(&g_slot_pid, __ATOMIC_ACQUIRE) == me)) {
        v1_claim_slot(me);
        s = __atomic_load_n(&g_slot, __ATOMIC_ACQUIRE);
    }
    __atomic_store_n(&g_claim_lock, 0, __ATOMIC_RELEASE);
    return s;
}

/* The child of a fork-like call (fork, _Fork, a clone without CLONE_VM through syscall(2)): it has its own copy of
 * the bitmap, and claims a slot at once, so the report sees it alive before it counts anything. Async-signal-safe
 * (getpid, readlink, open/read/close, stat, atomics): it runs in a fork handler and after _Fork. */
static void v1_child_born(int claim) {
    __atomic_store_n(&g_slot, NULL, __ATOMIC_RELEASE);
    __atomic_store_n(&g_slot_pid, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&g_claim_lock, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&g_bitmap_pid, (int32_t)getpid(), __ATOMIC_RELEASE);
    if (!claim || __atomic_load_n(&g_hdr, __ATOMIC_ACQUIRE) == NULL) return;
    int32_t me;
    (void)v1_slot_get(&me);
}
static void v1_after_fork_child(void) {
#ifdef V1_MUTANT_NO_FORK_CLAIM /* fire-check mutant: the forkidle arm must catch a fork child with no slot */
    v1_child_born(0);
#else
    v1_child_born(1);
#endif
}

/* ---- O_SYNC / O_DSYNC fd tracking ---- */
static inline void osync_on(int fd) {
    if (fd < 0 || (unsigned)fd >= V1_FD_BITS) return;
    __atomic_fetch_or(&g_osync[fd >> 3], (uint8_t)(1u << (fd & 7)), __ATOMIC_RELAXED);
}
/* Clearing is the only update that can lose a sync fd, so only the bitmap's owner does it (a vfork child shares
 * this memory with its suspended parent). Setting is always safe: a stale bit is re-checked before it counts. */
static inline void osync_off(int fd) {
    if (fd < 0 || (unsigned)fd >= V1_FD_BITS) return;
    if (!(__atomic_load_n(&g_osync[fd >> 3], __ATOMIC_RELAXED) & (1u << (fd & 7)))) return;
    if ((int32_t)getpid() != __atomic_load_n(&g_bitmap_pid, __ATOMIC_ACQUIRE)) return;
    __atomic_fetch_and(&g_osync[fd >> 3], (uint8_t)~(1u << (fd & 7)), __ATOMIC_RELAXED);
}
static inline void osync_set(int fd, int on) {
    if (on) osync_on(fd);
    else osync_off(fd);
}
static inline int osync_get(int fd) {
    if (fd < 0 || (unsigned)fd >= V1_FD_BITS) return 0;
    return (__atomic_load_n(&g_osync[fd >> 3], __ATOMIC_RELAXED) >> (fd & 7)) & 1;
}
/* Status flags -> write kind. On Linux O_SYNC == __O_SYNC|O_DSYNC, so test the whole O_SYNC mask first. */
static inline int fl_kind(int fl) {
    if (fl == -1) return -1;
    if ((fl & O_SYNC) == O_SYNC) return V1K_OSYNC_WRITE;
    if (fl & O_DSYNC) return V1K_ODSYNC_WRITE;
    return -1;
}
static inline int flags_maybe_sync(int flags) { return (flags & (O_SYNC | O_DSYNC)) != 0; }

static void v1_ensure_slow(void);
static inline void v1_ensure(void) {
    if (__builtin_expect(__atomic_load_n(&g_hdr, __ATOMIC_ACQUIRE) == NULL, 0)) v1_ensure_slow();
}

/* The write kind for fd now (re-reading F_GETFL), or -1. */
static int osync_candidate(int fd) {
    if (fd < 0) return -1;
    if ((unsigned)fd >= V1_FD_BITS) {
        int32_t me;
        v1_ensure();
        __atomic_fetch_add(&v1_slot_get(&me)->fd_untracked, 1, __ATOMIC_RELAXED);
        return -1;
    }
    if (!osync_get(fd)) return -1;
    int e = errno;
    RESOLVE(fcntl);
    int k = fl_kind(real_fcntl(fd, F_GETFL));
    errno = e;
    if (k < 0) osync_off(fd);
    return k;
}
static inline int rwf_kind(int flags) {
    if (flags & RWF_SYNC) return V1K_OSYNC_WRITE;
    if (flags & RWF_DSYNC) return V1K_ODSYNC_WRITE;
    return -1;
}
static inline int stronger(int a, int b) {
    if (a == V1K_OSYNC_WRITE || b == V1K_OSYNC_WRITE) return V1K_OSYNC_WRITE;
    if (a == V1K_ODSYNC_WRITE || b == V1K_ODSYNC_WRITE) return V1K_ODSYNC_WRITE;
    return -1;
}
static inline int sfr_kind(unsigned int f) {
    if (f & SYNC_FILE_RANGE_WRITE) return (f & SYNC_FILE_RANGE_WAIT_AFTER) ? V1K_SFR_WRITE_WAIT : V1K_SFR_WRITE;
    return V1K_SFR_WAIT;
}
static inline int msync_kind(int f) { return (f & MS_SYNC) ? V1K_MSYNC_SYNC : V1K_MSYNC_OTHER; }
/* Total bytes an iovec array asks for, read without trusting the pointer: the array is copied with
 * process_vm_readv, so a bad pointer gives 0 here (and EFAULT from the real call), never a SIGSEGV in the shim. A
 * count outside 1..IOV_MAX (the kernel says EINVAL) also gives 0. Only called for an fd already known to be sync. */
static size_t iov_len(const struct iovec *v, long n) {
    if (v == NULL || n <= 0 || n > 1024) return 0;
    struct iovec chunk[64];
    size_t s = 0;
    for (long i = 0; i < n; i += 64) {
        long m = n - i < 64 ? n - i : 64;
        struct iovec local = {chunk, (size_t)m * sizeof *chunk};
        struct iovec remote = {(void *)(v + i), (size_t)m * sizeof *chunk};
        int e = errno;
        ssize_t got = process_vm_readv(getpid(), &local, 1, &remote, 1, 0);
        int why = errno;
        errno = e;
        if (got == -1 && why != EFAULT) { /* process_vm_readv refused (seccomp EPERM, ENOSYS): read it directly */
            for (long j = 0; j < m; j++) s += v[i + j].iov_len;
            continue;
        }
        if (got != (ssize_t)local.iov_len) return 0;
        for (long j = 0; j < m; j++) s += chunk[j].iov_len;
    }
    return s;
}

/* fds inherited across exec: read their status flags once, from /proc/self/fd (else fds 0..1023). */
static void v1_seed_fds(void) {
    RESOLVE(fcntl);
    DIR *d = opendir("/proc/self/fd"); /* glibc opens it internally: no wrapper runs */
    if (!d) {
        for (int fd = 0; fd < 1024; fd++) if (fl_kind(real_fcntl(fd, F_GETFL)) >= 0) osync_on(fd);
        return;
    }
    int dfd = dirfd(d);
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] < '0' || de->d_name[0] > '9') continue;
        int fd = atoi(de->d_name);
        if (fd == dfd) continue;
        if (fl_kind(real_fcntl(fd, F_GETFL)) >= 0) osync_on(fd);
    }
    closedir(d);
}

static void v1_init_once(void) {
    v1_resolve_all();
    const char *run = getenv("SYNCSHIM_RUN");
    if (!run || !*run) v1_die("SYNCSHIM_RUN is unset (launch through v1run)");
    const char *why = "?";
    v1_hdr *h = v1_map(run, &why);
    if (!h) v1_die(why);
    g_is_go = v1_exe_is_go();
    __atomic_store_n(&g_bitmap_pid, (int32_t)getpid(), __ATOMIC_RELEASE);
    v1_seed_fds();
    __atomic_store_n(&g_hdr, h, __ATOMIC_RELEASE);
    int32_t me;
    (void)v1_slot_get(&me); /* claim now: an image that never flushes still shows as attached */
    pthread_atfork(NULL, NULL, v1_after_fork_child);
}
static void v1_ensure_slow(void) { pthread_once(&g_once, v1_init_once); }

__attribute__((constructor)) static void v1_ctor(void) { v1_ensure(); }

/* ---- recording ---- */
static void v1_rec(v1_slot *s, int32_t me, int kind, int fd, int64_t aux, uint64_t mark, uint64_t t0, uint64_t t1,
                   int failed, int err) {
    __atomic_fetch_add(&s->count[kind], 1, __ATOMIC_RELAXED);
    if (failed) __atomic_fetch_add(&s->fail[kind], 1, __ATOMIC_RELAXED);
    uint64_t e = __atomic_fetch_add(&g_hdr->ev_next, 1, __ATOMIC_RELAXED);
    if (e >= g_hdr->ev_cap) {
        __atomic_fetch_add(&g_hdr->ev_dropped, 1, __ATOMIC_RELAXED);
        return;
    }
    v1_event *ev = &v1_events(g_hdr)[e];
    ev->t0_ns = t0;
    ev->t1_ns = t1;
    ev->mark = mark;
    ev->aux = aux;
    ev->pid = me;
    ev->fd = fd;
    ev->tid = (uint32_t)gettid();
    ev->slot = (uint32_t)(s - v1_slots(g_hdr));
    ev->ret = failed ? -1 : 0;
    ev->err = (int16_t)(failed ? err : 0);
    __atomic_store_n(&ev->kind, (int16_t)(kind + 1), __ATOMIC_RELEASE); /* kind+1: 0 means "never completed" */
}

/* BEGIN takes the slot and marks the call in flight before the real call; END records it, then clears in-flight.
 * END records only in the process that began the call: a child forked from a signal handler that interrupted a
 * counted call resumes this frame holding its parent's slot, and must not count, or un-count, there. */
#define V1_BEGIN()                                                                 \
    v1_ensure();                                                                   \
    int32_t me_;                                                                   \
    v1_slot *const s_ = v1_slot_get(&me_);                                         \
    __atomic_fetch_add(&s_->inflight, 1, __ATOMIC_SEQ_CST);                        \
    const uint64_t mark_ = __atomic_load_n(&g_hdr->mark, __ATOMIC_SEQ_CST);        \
    const uint64_t t0_ = v1_now()
#define V1_END(kind, fd, aux, failed)                                              \
    do {                                                                           \
        int e_ = errno;                                                            \
        if (__builtin_expect((int32_t)getpid() == me_, 1)) {                       \
            v1_rec(s_, me_, (kind), (fd), (int64_t)(aux), mark_, t0_, v1_now(), (failed), e_); \
            __atomic_fetch_sub(&s_->inflight, 1, __ATOMIC_SEQ_CST);                \
        }                                                                          \
        errno = e_;                                                                \
    } while (0)

/* A call of a counted kind that went through syscall(2): reported, never counted. */
static void v1_missed(int kind) {
    int32_t me;
    v1_ensure();
    __atomic_fetch_add(&v1_slot_get(&me)->missed[kind], 1, __ATOMIC_RELAXED);
}

/* ---- flushes ---- */
int fsync(int fd) {
    RESOLVE(fsync);
    V1_BEGIN();
    int r = real_fsync(fd);
    V1_END(V1K_FSYNC, fd, 0, r == -1);
    return r;
}
#ifndef V1_MUTANT_DROP_FDATASYNC /* fire-check mutant: the checker must catch its absence */
int fdatasync(int fd) {
    RESOLVE(fdatasync);
    V1_BEGIN();
    int r = real_fdatasync(fd);
    V1_END(V1K_FDATASYNC, fd, 0, r == -1);
    return r;
}
#endif
int sync_file_range(int fd, __off64_t off, __off64_t n, unsigned int flags) {
    RESOLVE_OR(sync_file_range, -1);
    V1_BEGIN();
    int r = real_sync_file_range(fd, off, n, flags);
    V1_END(sfr_kind(flags), fd, flags, r == -1);
    return r;
}
int syncfs(int fd) {
    RESOLVE_OR(syncfs, -1);
    V1_BEGIN();
    int r = real_syncfs(fd);
    V1_END(V1K_SYNCFS, fd, 0, r == -1);
    return r;
}
void sync(void) {
    RESOLVE(sync);
    V1_BEGIN();
    real_sync();
    V1_END(V1K_SYNC, -1, 0, 0);
}
int msync(void *a, size_t l, int f) {
    RESOLVE(msync);
    V1_BEGIN();
    int r = real_msync(a, l, f);
    V1_END(msync_kind(f), -1, f, r == -1);
    return r;
}

/* ---- clone ops ---- */
int ioctl(int fd, unsigned long req, ...) {
    va_list ap;
    va_start(ap, req);
    void *arg = va_arg(ap, void *); /* one pointer-sized vararg: what glibc's own ioctl passes on */
    va_end(ap);
    RESOLVE(ioctl);
    if (req != V1_FICLONE && req != V1_FICLONERANGE) return real_ioctl(fd, req, arg);
    V1_BEGIN();
    int r = real_ioctl(fd, req, arg);
    V1_END(req == V1_FICLONE ? V1K_FICLONE : V1K_FICLONERANGE, fd, req == V1_FICLONE ? (int64_t)(intptr_t)arg : 0,
           r == -1);
    return r;
}
ssize_t copy_file_range(int in, __off64_t *pin, int out, __off64_t *pout, size_t len, unsigned int flags) {
    RESOLVE_OR(copy_file_range, -1);
    int k = len ? osync_candidate(out) : -1; /* into an O_SYNC/O_DSYNC fd it is also a sync write */
    V1_BEGIN();
    ssize_t r = real_copy_file_range(in, pin, out, pout, len, flags);
    V1_END(V1K_COPY_FILE_RANGE, out, (int64_t)len, r == -1);
    if (k >= 0 && (int32_t)getpid() == me_) {
        int e = errno;
        v1_rec(s_, me_, k, out, (int64_t)len, mark_, t0_, v1_now(), r == -1, e);
        errno = e;
    }
    return r;
}

/* ---- writes: counted only when >= 1 byte is asked for and the fd's status flags (re-read now) or pwritev2's flags
 * ask for sync. A zero-length write syncs nothing. ---- */
#define V1_WRITE(kindexpr, call, fd, lenexpr)                \
    int k_ = (kindexpr);                                     \
    size_t len_ = k_ >= 0 ? (size_t)(lenexpr) : 0;           \
    if (k_ < 0 || len_ == 0) return call;                    \
    V1_BEGIN();                                              \
    ssize_t r_ = call;                                       \
    V1_END(k_, fd, len_, r_ == -1);                          \
    return r_

ssize_t write(int fd, const void *b, size_t n) {
    RESOLVE(write);
    V1_WRITE(osync_candidate(fd), real_write(fd, b, n), fd, n);
}
ssize_t __write(int fd, const void *b, size_t n) {
    RESOLVE_ALT(__write, write);
    V1_WRITE(osync_candidate(fd), real___write(fd, b, n), fd, n);
}
ssize_t pwrite(int fd, const void *b, size_t n, __off_t o) {
    RESOLVE(pwrite);
    V1_WRITE(osync_candidate(fd), real_pwrite(fd, b, n, o), fd, n);
}
#ifndef V1_MUTANT_DROP_PWRITE64 /* fire-check mutant */
ssize_t pwrite64(int fd, const void *b, size_t n, __off64_t o) {
    RESOLVE_ALT(pwrite64, pwrite);
    V1_WRITE(osync_candidate(fd), real_pwrite64(fd, b, n, o), fd, n);
}
#endif
ssize_t __pwrite64(int fd, const void *b, size_t n, __off64_t o) {
    RESOLVE_ALT(__pwrite64, pwrite);
    V1_WRITE(osync_candidate(fd), real___pwrite64(fd, b, n, o), fd, n);
}
ssize_t writev(int fd, const struct iovec *v, int c) {
    RESOLVE(writev);
    V1_WRITE(osync_candidate(fd), real_writev(fd, v, c), fd, iov_len(v, c));
}
ssize_t pwritev(int fd, const struct iovec *v, int c, __off_t o) {
    RESOLVE(pwritev);
    V1_WRITE(osync_candidate(fd), real_pwritev(fd, v, c, o), fd, iov_len(v, c));
}
ssize_t pwritev64(int fd, const struct iovec *v, int c, __off64_t o) {
    RESOLVE_ALT(pwritev64, pwritev);
    V1_WRITE(osync_candidate(fd), real_pwritev64(fd, v, c, o), fd, iov_len(v, c));
}
ssize_t pwritev2(int fd, const struct iovec *v, int c, __off_t o, int f) {
    RESOLVE_OR(pwritev2, -1);
    V1_WRITE(stronger(osync_candidate(fd), rwf_kind(f)), real_pwritev2(fd, v, c, o, f), fd, iov_len(v, c));
}
ssize_t pwritev64v2(int fd, const struct iovec *v, int c, __off64_t o, int f) {
    RESOLVE_ALT(pwritev64v2, pwritev2);
    if (real_pwritev64v2 == NULL) { errno = ENOSYS; return -1; }
    V1_WRITE(stronger(osync_candidate(fd), rwf_kind(f)), real_pwritev64v2(fd, v, c, o, f), fd, iov_len(v, c));
}
ssize_t sendfile(int out, int in, off_t *off, size_t n) {
    RESOLVE(sendfile);
    V1_WRITE(osync_candidate(out), real_sendfile(out, in, off, n), out, n);
}
ssize_t sendfile64(int out, int in, __off64_t *off, size_t n) {
    RESOLVE_ALT(sendfile64, sendfile);
    V1_WRITE(osync_candidate(out), real_sendfile64(out, in, off, n), out, n);
}
ssize_t splice(int in, __off64_t *pin, int out, __off64_t *pout, size_t n, unsigned int flags) {
    RESOLVE_OR(splice, -1);
    V1_WRITE(osync_candidate(out), real_splice(in, pin, out, pout, n, flags), out, n);
}

/* ---- fd lifecycle (no counting, no shm: these never call v1_ensure, so the constructor may use them) ---- */
static inline int open_needs_mode(int flags) { return (flags & O_CREAT) != 0 || (flags & O_TMPFILE) == O_TMPFILE; }
static inline int after_open(int r, int flags) {
    if (r >= 0) osync_set(r, flags_maybe_sync(flags));
    return r;
}
#define V1_MODE(flags, last)                                \
    mode_t mode = 0;                                        \
    if (open_needs_mode(flags)) {                           \
        va_list ap;                                         \
        va_start(ap, last);                                 \
        mode = (mode_t)va_arg(ap, int);                     \
        va_end(ap);                                         \
    }

int open(const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE(open);
    return after_open(real_open(p, flags, mode), flags);
}
int open64(const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE_ALT(open64, open);
    return after_open(real_open64(p, flags, mode), flags);
}
int __open(const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE_ALT(__open, open);
    return after_open(real___open(p, flags, mode), flags);
}
int __open64(const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE_ALT(__open64, open);
    return after_open(real___open64(p, flags, mode), flags);
}
int openat(int d, const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE(openat);
    return after_open(real_openat(d, p, flags, mode), flags);
}
int openat64(int d, const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE_ALT(openat64, openat);
    return after_open(real_openat64(d, p, flags, mode), flags);
}
/* The _2 forms are fortify's mode-less opens; their fallback is the plain open with no mode. */
#ifndef V1_MUTANT_DROP_OPEN_2 /* fire-check mutant */
int __open_2(const char *p, int flags) {
    RESOLVE(__open_2);
    if (real___open_2 == NULL) { RESOLVE(open); return after_open(real_open(p, flags), flags); }
    return after_open(real___open_2(p, flags), flags);
}
#endif
int __open64_2(const char *p, int flags) {
    RESOLVE(__open64_2);
    if (real___open64_2 == NULL) { RESOLVE(open); return after_open(real_open(p, flags), flags); }
    return after_open(real___open64_2(p, flags), flags);
}
int __openat_2(int d, const char *p, int flags) {
    RESOLVE(__openat_2);
    if (real___openat_2 == NULL) { RESOLVE(openat); return after_open(real_openat(d, p, flags), flags); }
    return after_open(real___openat_2(d, p, flags), flags);
}
int __openat64_2(int d, const char *p, int flags) {
    RESOLVE(__openat64_2);
    if (real___openat64_2 == NULL) { RESOLVE(openat); return after_open(real_openat(d, p, flags), flags); }
    return after_open(real___openat64_2(d, p, flags), flags);
}
int creat(const char *p, mode_t m) {
    RESOLVE(creat);
    return after_open(real_creat(p, m), 0);
}
int creat64(const char *p, mode_t m) {
    RESOLVE_ALT(creat64, creat);
    return after_open(real_creat64(p, m), 0);
}
int open_by_handle_at(int mfd, struct file_handle *h, int flags) {
    RESOLVE_OR(open_by_handle_at, -1);
    return after_open(real_open_by_handle_at(mfd, h, flags), flags);
}

static int fcntl_after(int r, int fd, int cmd) {
    if (r == -1) return r;
    int e = errno;
    if (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC) osync_set(r, osync_get(fd));
    else if (cmd == F_SETFL) {
        RESOLVE(fcntl);
        osync_set(fd, fl_kind(real_fcntl(fd, F_GETFL)) >= 0);
    }
    errno = e;
    return r;
}
/* fcntl is variadic; on x86_64 and aarch64 Linux the third argument travels in a register whatever its type, so
 * one pointer-sized va_arg recovers an int, a long or a pointer alike, as glibc's own fcntl does. */
#define V1_FCNTL_ARG()                     \
    va_list ap;                            \
    va_start(ap, cmd);                     \
    void *arg = va_arg(ap, void *);        \
    va_end(ap)
int fcntl(int fd, int cmd, ...) {
    V1_FCNTL_ARG();
    RESOLVE(fcntl);
    return fcntl_after(real_fcntl(fd, cmd, arg), fd, cmd);
}
int fcntl64(int fd, int cmd, ...) {
    V1_FCNTL_ARG();
    RESOLVE_ALT(fcntl64, fcntl);
    return fcntl_after(real_fcntl64(fd, cmd, arg), fd, cmd);
}
int __fcntl(int fd, int cmd, ...) {
    V1_FCNTL_ARG();
    RESOLVE_ALT(__fcntl, fcntl);
    return fcntl_after(real___fcntl(fd, cmd, arg), fd, cmd);
}
int dup(int fd) {
    RESOLVE(dup);
    int r = real_dup(fd);
    if (r >= 0) osync_set(r, osync_get(fd));
    return r;
}
int dup2(int fd, int fd2) {
    RESOLVE(dup2);
    int r = real_dup2(fd, fd2);
    if (r >= 0) osync_set(r, osync_get(fd));
    return r;
}
int __dup2(int fd, int fd2) {
    RESOLVE_ALT(__dup2, dup2);
    int r = real___dup2(fd, fd2);
    if (r >= 0) osync_set(r, osync_get(fd));
    return r;
}
int dup3(int fd, int fd2, int flags) {
    RESOLVE_OR(dup3, -1);
    int r = real_dup3(fd, fd2, flags);
    if (r >= 0) osync_set(r, osync_get(fd));
    return r;
}
int close(int fd) {
    RESOLVE(close);
    osync_off(fd);
    return real_close(fd);
}
int __close(int fd) {
    RESOLVE_ALT(__close, close);
    osync_off(fd);
    return real___close(fd);
}

/* ---- exec guard: every exec and posix_spawn is recorded; `v1ctl report` refuses (rc 6) when the new image
 * never attached. vfork-safe: no allocation, no lock, only stores into the shared run. ---- */
enum { VIA_EXECVE = 1, VIA_EXECV, VIA_EXECVP, VIA_EXECVPE, VIA_EXECL, VIA_EXECLE, VIA_EXECLP, VIA_FEXECVE,
       VIA_EXECVEAT, VIA_POSIX_SPAWN, VIA_POSIX_SPAWNP };
static int64_t v1_exec_note(int32_t pid, const char *path, int via, uint64_t t, int state) {
    v1_ensure();
    uint64_t i = __atomic_fetch_add(&g_hdr->exec_next, 1, __ATOMIC_SEQ_CST);
    if (i >= g_hdr->exec_cap) return -1;
    v1_exec *x = &v1_execs(g_hdr)[i];
    x->pid = pid;
    x->by_pid = (int32_t)getpid();
    x->t_ns = t;
    x->via = via;
    const char *src = path ? path : "?";
    size_t len = strlen(src);
    if (len >= sizeof x->path) { src += len - (sizeof x->path - 1); len = sizeof x->path - 1; }
    memcpy(x->path, src, len);
    x->path[len] = 0;
    __atomic_store_n(&x->state, state, __ATOMIC_RELEASE);
    return (int64_t)i;
}
static void v1_exec_failed(int64_t i) {
    if (i < 0) return;
    int e = errno;
    __atomic_store_n(&v1_execs(g_hdr)[i].state, V1X_FAILED, __ATOMIC_RELEASE);
    errno = e;
}
#define V1_EXEC(via, path, call)                                                        \
    int64_t x_ = v1_exec_note((int32_t)getpid(), (path), (via), v1_now(), V1X_PENDING); \
    int r_ = call;                                                                      \
    v1_exec_failed(x_);                                                                 \
    return r_

int execve(const char *path, char *const argv[], char *const envp[]) {
    RESOLVE(execve);
    V1_EXEC(VIA_EXECVE, path, real_execve(path, argv, envp));
}
int execv(const char *path, char *const argv[]) {
    RESOLVE(execv);
    V1_EXEC(VIA_EXECV, path, real_execv(path, argv));
}
int execvp(const char *file, char *const argv[]) {
    RESOLVE(execvp);
    V1_EXEC(VIA_EXECVP, file, real_execvp(file, argv));
}
int execvpe(const char *file, char *const argv[], char *const envp[]) {
    RESOLVE_OR(execvpe, -1);
    V1_EXEC(VIA_EXECVPE, file, real_execvpe(file, argv, envp));
}
int fexecve(int fd, char *const argv[], char *const envp[]) {
    char p[32];
    snprintf(p, sizeof p, "fexecve:fd%d", fd);
    RESOLVE_OR(fexecve, -1);
    V1_EXEC(VIA_FEXECVE, p, real_fexecve(fd, argv, envp));
}
int execveat(int dirfd, const char *path, char *const argv[], char *const envp[], int flags) {
    RESOLVE_OR(execveat, -1);
    V1_EXEC(VIA_EXECVEAT, path, real_execveat(dirfd, path, argv, envp, flags));
}
/* execl, execle, execlp: collect the list, then the v-form's REAL function (one record per call). glibc declares
 * arg nonnull, so the list has at least arg. After the second pass reads av[1..n-1], the next vararg is the
 * terminating NULL and, for execle, the one after it is envp. */
#define V1_COLLECT(arg, n, av)                                                     \
    size_t n = 1;                                                                  \
    va_list ap;                                                                    \
    va_start(ap, arg);                                                             \
    while (va_arg(ap, const char *) != NULL) n++;                                  \
    va_end(ap);                                                                    \
    const char *av[n + 1];                                                         \
    av[0] = arg;                                                                   \
    va_start(ap, arg);                                                             \
    for (size_t i_ = 1; i_ < n; i_++) av[i_] = va_arg(ap, const char *);           \
    av[n] = NULL
int execl(const char *path, const char *arg, ...) {
    V1_COLLECT(arg, n, av);
    va_end(ap);
    RESOLVE(execv);
    V1_EXEC(VIA_EXECL, path, real_execv(path, (char *const *)av));
}
int execlp(const char *file, const char *arg, ...) {
    V1_COLLECT(arg, n, av);
    va_end(ap);
    RESOLVE(execvp);
    V1_EXEC(VIA_EXECLP, file, real_execvp(file, (char *const *)av));
}
int execle(const char *path, const char *arg, ...) {
    V1_COLLECT(arg, n, av);
    (void)va_arg(ap, const char *); /* the NULL that ends the list */
    char *const *envp = va_arg(ap, char *const *);
    va_end(ap);
    RESOLVE(execve);
    V1_EXEC(VIA_EXECLE, path, real_execve(path, (char *const *)av, envp));
}

int posix_spawn(pid_t *pid, const char *path, const posix_spawn_file_actions_t *fa, const posix_spawnattr_t *at,
                char *const argv[], char *const envp[]) {
    RESOLVE(posix_spawn);
    uint64_t t = v1_now(); /* before the child exists, so its attach is later */
    pid_t p = 0;
    int r = real_posix_spawn(&p, path, fa, at, argv, envp);
    if (r == 0) {
        int e = errno;
        (void)v1_exec_note((int32_t)p, path, VIA_POSIX_SPAWN, t, V1X_SPAWNED);
        errno = e;
    }
    if (pid) *pid = p;
    return r;
}
int posix_spawnp(pid_t *pid, const char *file, const posix_spawn_file_actions_t *fa, const posix_spawnattr_t *at,
                 char *const argv[], char *const envp[]) {
    RESOLVE(posix_spawnp);
    uint64_t t = v1_now();
    pid_t p = 0;
    int r = real_posix_spawnp(&p, file, fa, at, argv, envp);
    if (r == 0) {
        int e = errno;
        (void)v1_exec_note((int32_t)p, file, VIA_POSIX_SPAWNP, t, V1X_SPAWNED);
        errno = e;
    }
    if (pid) *pid = p;
    return r;
}

/* ---- _Fork (glibc >= 2.34): a fork with no fork handlers, so the child is born here instead. glibc's own fork()
 * calls its internal _Fork, not this symbol, so a fork() child is born once (in the fork handler). ---- */
static pid_t (*real__Fork)(void);
pid_t _Fork(void) {
    RESOLVE_OR(_Fork, -1);
    pid_t r = real__Fork();
    if (r == 0) v1_child_born(1);
    return r;
}

/* ---- liburing's setup entry points (when liburing.so is linked dynamically): async I/O, rc 9. liburing >= 2.2
 * then makes its syscalls as raw instructions, so this is the only sign of a ring the shim can get. Not in v1_tab:
 * a program without liburing never calls these, and dlsym finds liburing only in a program that links it. ---- */
static int (*real_io_uring_queue_init)(unsigned, void *, unsigned);
static int (*real_io_uring_queue_init_params)(unsigned, void *, void *);
static int (*real_io_uring_setup)(unsigned, void *);
static void v1_async(void) {
    int32_t me;
    v1_ensure();
    __atomic_fetch_add(&v1_slot_get(&me)->async_io, 1, __ATOMIC_RELAXED);
}
int io_uring_queue_init(unsigned entries, void *ring, unsigned flags) {
    RESOLVE(io_uring_queue_init);
    v1_async();
    return real_io_uring_queue_init ? real_io_uring_queue_init(entries, ring, flags) : -ENOSYS;
}
int io_uring_queue_init_params(unsigned entries, void *ring, void *p) {
    RESOLVE(io_uring_queue_init_params);
    v1_async();
    return real_io_uring_queue_init_params ? real_io_uring_queue_init_params(entries, ring, p) : -ENOSYS;
}
int io_uring_setup(unsigned entries, void *p) {
    RESOLVE(io_uring_setup);
    v1_async();
    return real_io_uring_setup ? real_io_uring_setup(entries, p) : -ENOSYS;
}

/* ---- syscall(2): watched, never counted. Six pointer-sized varargs are forwarded, as glibc's own syscall() reads
 * six registers whatever the call. Nothing but a switch runs for a syscall this does not watch (futex, getrandom).
 *   counted kinds   -> slot->missed[kind] (and the report refuses, rc 9)
 *   io_uring / AIO  -> slot->async_io (rc 9)
 *   fd lifecycle    -> the O_SYNC tracker (open, creat, openat, openat2, dup, dup2, dup3, fcntl, close)
 *   fork-like clone -> the child is born (slot, bitmap) as after fork(); execve/execveat -> the exec table ---- */
struct v1_open_how { uint64_t flags, mode, resolve; };
static int v1_peek(const void *addr, void *buf, size_t n) { /* copy without trusting addr: 0, or -1 */
    struct iovec local = {buf, n}, remote = {(void *)addr, n};
    int e = errno;
    ssize_t got = process_vm_readv(getpid(), &local, 1, &remote, 1, 0);
    errno = e;
    return got == (ssize_t)n ? 0 : -1;
}
long syscall(long n, ...) {
    va_list ap;
    va_start(ap, n);
    long a0 = va_arg(ap, long), a1 = va_arg(ap, long), a2 = va_arg(ap, long);
    long a3 = va_arg(ap, long), a4 = va_arg(ap, long), a5 = va_arg(ap, long);
    va_end(ap);
    RESOLVE(syscall);
    int miss = -1, miss2 = -1, async = 0, track = 0, forklike = 0;
    int64_t xrec = -1;
    switch (n) {
    case SYS_fsync: miss = V1K_FSYNC; break;
    case SYS_fdatasync: miss = V1K_FDATASYNC; break;
    case SYS_syncfs: miss = V1K_SYNCFS; break;
    case SYS_sync: miss = V1K_SYNC; break;
    case SYS_msync: miss = msync_kind((int)a2); break;
    case SYS_sync_file_range: miss = sfr_kind((unsigned int)a3); break;
    case SYS_ioctl:
        if ((unsigned long)a1 == V1_FICLONE) miss = V1K_FICLONE;
        else if ((unsigned long)a1 == V1_FICLONERANGE) miss = V1K_FICLONERANGE;
        break;
    case SYS_copy_file_range: miss = V1K_COPY_FILE_RANGE; miss2 = a4 ? osync_candidate((int)a2) : -1; break;
    case SYS_write:
    case SYS_pwrite64: miss = a2 ? osync_candidate((int)a0) : -1; break;
    case SYS_writev:
    case SYS_pwritev:
        miss = osync_candidate((int)a0);
        if (miss >= 0 && !iov_len((const struct iovec *)a1, (int)a2)) miss = -1;
        break;
    /* raw pwritev2 is (fd, iov, iovcnt, pos_l, pos_h, flags): the flags are the SIXTH argument */
    case SYS_pwritev2:
#ifdef V1_MUTANT_PWRITEV2_A4 /* fire-check mutant: reads the flags from the wrong argument */
        miss = stronger(osync_candidate((int)a0), rwf_kind((int)a4));
#else
        miss = stronger(osync_candidate((int)a0), rwf_kind((int)a5));
#endif
        if (miss >= 0 && !iov_len((const struct iovec *)a1, (int)a2)) miss = -1;
        break;
    case SYS_sendfile: miss = a3 ? osync_candidate((int)a0) : -1; break;
    case SYS_splice: miss = a4 ? osync_candidate((int)a2) : -1; break;
    case SYS_io_uring_setup:
    case SYS_io_uring_enter:
    case SYS_io_uring_register:
    case SYS_io_setup:
    case SYS_io_submit: async = 1; break;
    /* fork-like only with no new stack (else the child returns into this frame on another stack, as it would in
     * glibc's own syscall()), no shared memory and no shared thread group */
    case SYS_clone: forklike = a1 == 0 && !((unsigned long)a0 & (CLONE_VM | CLONE_VFORK | CLONE_THREAD)); break;
    case SYS_clone3: {
        uint64_t ca[6]; /* struct clone_args: flags, pidfd, child_tid, parent_tid, exit_signal, stack */
        forklike = v1_peek((const void *)a0, ca, sizeof ca) == 0 && ca[5] == 0 &&
                   !(ca[0] & (uint64_t)(CLONE_VM | CLONE_VFORK | CLONE_THREAD));
        break;
    }
#ifdef SYS_fork
    case SYS_fork: forklike = 1; break;
#endif
    case SYS_execve:
        xrec = v1_exec_note((int32_t)getpid(), (const char *)a0, VIA_EXECVE, v1_now(), V1X_PENDING);
        break;
    case SYS_execveat:
        xrec = v1_exec_note((int32_t)getpid(), (const char *)a1, VIA_EXECVEAT, v1_now(), V1X_PENDING);
        break;
#ifdef SYS_open
    case SYS_open:
#endif
#ifdef SYS_creat
    case SYS_creat:
#endif
#ifdef SYS_dup2
    case SYS_dup2:
#endif
    case SYS_openat:
    case SYS_openat2:
    case SYS_dup:
    case SYS_dup3:
#ifndef V1_MUTANT_SYSCALL_NO_FCNTL /* fire-check mutant: fcntl through syscall(2) not tracked */
    case SYS_fcntl:
#endif
        track = 1;
        break;
    case SYS_close: osync_off((int)a0); break;
    default: break;
    }
    long r = real_syscall(n, a0, a1, a2, a3, a4, a5);
    if (miss < 0 && miss2 < 0 && !async && !(track && r >= 0) && !forklike && xrec < 0) return r;
    int e = errno;
    if (forklike && r == 0) { v1_child_born(1); errno = e; return r; } /* the child: born, nothing else to do */
    if (xrec >= 0) v1_exec_failed(xrec); /* execve returned: it failed */
    if (miss >= 0) v1_missed(miss);
    if (miss2 >= 0) v1_missed(miss2);
    if (async) v1_async();
    if (track && r >= 0) {
        switch (n) {
#ifdef SYS_open
        case SYS_open: osync_set((int)r, flags_maybe_sync((int)a1)); break;
#endif
#ifdef SYS_creat
        case SYS_creat: osync_off((int)r); break;
#endif
#ifdef SYS_dup2
        case SYS_dup2: osync_set((int)r, osync_get((int)a0)); break;
#endif
        case SYS_openat: osync_set((int)r, flags_maybe_sync((int)a2)); break;
        case SYS_openat2: {
            struct v1_open_how h;
            osync_set((int)r, v1_peek((const void *)a2, &h, sizeof h) == 0 &&
                                  (h.flags & (uint64_t)(O_SYNC | O_DSYNC)) != 0);
            break;
        }
        case SYS_dup:
        case SYS_dup3: osync_set((int)r, osync_get((int)a0)); break;
        case SYS_fcntl: (void)fcntl_after((int)r, (int)a0, (int)a1); break;
        default: break;
        }
    }
    errno = e;
    return r;
}
