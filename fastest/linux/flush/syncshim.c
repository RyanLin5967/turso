/* syncshim.c -- V1 flush counter for Linux, an LD_PRELOAD interposition library for glibc (x86_64, arm64).
 *
 * Counts, per process and per marked operation (layout and reader API: syncshim.h):
 *   flushes    fsync, fdatasync, sync_file_range (split: write+wait_after / write only / wait only), syncfs, sync,
 *              msync (MS_SYNC vs the rest), and writes on an O_SYNC or O_DSYNC fd through write, __write, pwrite,
 *              pwrite64, __pwrite64, writev, pwritev, pwritev64, pwritev2, pwritev64v2 (pwritev2's per-call
 *              RWF_SYNC / RWF_DSYNC count too, even on a plain fd);
 *   clone ops  ioctl(FICLONE), ioctl(FICLONERANGE), copy_file_range -- separate kinds, never summed with flushes;
 *   and the same calls made through the generic syscall(2) entry point (counted in their kind, tallied in
 *   slot->via_syscall).
 * Launch with `v1run <run> cmd...`; read with `v1ctl report|bymark|events <run>`.
 *
 * REFUSES rather than counting nothing: if SYNCSHIM_RUN is unset, or the run's shm is missing or malformed, the
 * process writes the reason to stderr and _exit(97)s before main. `v1ctl report` refuses when the root pid never
 * attached (rc 4), when an image the shim saw being exec'd or posix_spawned never attached (rc 6), and when a Go
 * binary attached (rc 7).
 *
 * BLIND SPOTS (stated here because the shim cannot count them):
 *  - GO BINARIES (Dolt, Doltgres): the Go runtime makes raw syscalls on Linux, with or without cgo, so NO flush
 *    of a Go program passes through libc. CGO_ENABLED=0 binaries are static and never load the shim (report rc 4
 *    as the root, rc 6 as a child exec'd by a counted process). CGO_ENABLED=1 binaries load it and attach, but
 *    count nothing: their slot carries V1_SLOT_GO and `v1ctl report` refuses (rc 7). THE INSTRUMENT FOR A GO
 *    BINARY IS THE STRACE COUNTER (v1strace.py). Go is detected by an ELF section .go.buildinfo /
 *    .note.go.buildid or a PT_NOTE owned by "Go"; a Go binary with neither would read as C.
 *  - Raw syscall instructions (inline asm, rustix's linux_raw backend, liburing, any static binary) are not seen.
 *    `v1strace.py cross` runs the shim and strace on the same run and reports every call the shim missed, per pid
 *    and kind; run it once per engine binary before trusting the shim for that binary.
 *  - glibc-internal calls are not interposed: stdio flushing a FILE* on an O_SYNC/O_DSYNC fd, POSIX AIO
 *    (aio_fsync, aio_write: helper threads call glibc's internal fsync/pwrite), and the /bin/sh that system() and
 *    popen() spawn (sh attaches on its own; a static /bin/sh would be unseen and unverified).
 *  - io_uring (IORING_OP_FSYNC, IORING_OP_SYNC_FILE_RANGE, writes on O_DSYNC fds) and Linux AIO (io_submit,
 *    IOCB_CMD_FSYNC) submissions are invisible. The strace counter refuses (rc 8) when it sees either.
 *  - O_SYNC/O_DSYNC tracking follows open, open64, __open, __open64, openat, openat64, __open_2, __open64_2,
 *    __openat_2, __openat64_2, creat, creat64, open_by_handle_at, dup, dup2, __dup2, dup3, fcntl/fcntl64/__fcntl
 *    (F_DUPFD, F_DUPFD_CLOEXEC, F_SETFL) and close/__close, plus open/openat/openat2/dup*/fcntl/close through
 *    syscall(2); fds inherited across exec are seeded at load from /proc/self/fd. An fd made O_SYNC by another
 *    path (SCM_RIGHTS, a raw openat2) is missed. A stale bit is never counted: each candidate write re-reads
 *    F_GETFL first. (Linux ignores O_SYNC/O_DSYNC in F_SETFL; FIRECHECK measures that.) fds >= V1_FD_BITS are
 *    not tracked; each write on one is tallied in slot->fd_untracked.
 *  - The exec guard covers the exec family and posix_spawn/posix_spawnp called through libc. Execs made by raw
 *    syscalls (Go) or inside glibc (system, popen) are not recorded.
 *  - A call is recorded after it returns: a process SIGKILLed inside a counted call does not count that call
 *    (strace counts it at entry).
 *  - setuid and other secure-exec binaries ignore LD_PRELOAD: they never attach (rc 4 / rc 6, never zero).
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
#include <stdlib.h>
#include <sys/types.h>

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

#define RESOLVE(name)                                                                       \
    do {                                                                                    \
        if (__builtin_expect(real_##name == NULL, 0))                                      \
            real_##name = (__typeof__(real_##name))dlsym(RTLD_NEXT, #name);                \
    } while (0)
/* For names an older glibc may lack: a caller can only reach this wrapper through dlsym, so say ENOSYS. */
#define RESOLVE_OR(name, fallback)                                                          \
    do {                                                                                    \
        RESOLVE(name);                                                                      \
        if (real_##name == NULL) { errno = ENOSYS; return (fallback); }                     \
    } while (0)

static void v1_resolve_all(void) {
    RESOLVE(fsync); RESOLVE(fdatasync); RESOLVE(sync_file_range); RESOLVE(syncfs); RESOLVE(sync); RESOLVE(msync);
    RESOLVE(write); RESOLVE(__write); RESOLVE(pwrite); RESOLVE(pwrite64); RESOLVE(__pwrite64); RESOLVE(writev);
    RESOLVE(pwritev); RESOLVE(pwritev64); RESOLVE(pwritev2); RESOLVE(pwritev64v2); RESOLVE(ioctl);
    RESOLVE(copy_file_range); RESOLVE(syscall); RESOLVE(open); RESOLVE(open64); RESOLVE(__open); RESOLVE(__open64);
    RESOLVE(openat); RESOLVE(openat64); RESOLVE(__open_2); RESOLVE(__open64_2); RESOLVE(__openat_2);
    RESOLVE(__openat64_2); RESOLVE(creat); RESOLVE(creat64); RESOLVE(open_by_handle_at); RESOLVE(fcntl);
    RESOLVE(fcntl64); RESOLVE(__fcntl); RESOLVE(dup); RESOLVE(dup2); RESOLVE(__dup2); RESOLVE(dup3); RESOLVE(close);
    RESOLVE(__close); RESOLVE(execve); RESOLVE(execv); RESOLVE(execvp); RESOLVE(execvpe); RESOLVE(fexecve);
    RESOLVE(execveat); RESOLVE(posix_spawn); RESOLVE(posix_spawnp);
}

/* ---- state ---- */
static v1_hdr *g_hdr;
static v1_slot *g_slot;       /* this process's slot; re-claimed whenever getpid() differs from g_slot_pid */
static int32_t g_slot_pid;
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
    /* program headers: PT_NOTE segments, notes owned by "Go" */
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
    /* section headers (absent only in a binary stripped of them) */
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
        const char *tail = path;
        if ((size_t)n >= sizeof s->exe) tail += (size_t)n - (sizeof s->exe - 1);
        memcpy(s->exe, tail, strlen(tail) + 1);
        s->ppid = (int32_t)getppid();
        s->attach_ns = v1_now();
        s->flags = g_is_go ? V1_SLOT_GO : 0;
        __atomic_store_n(&s->pid, me, __ATOMIC_RELEASE); /* published last */
    }
    __atomic_store_n(&g_slot_pid, me, __ATOMIC_RELEASE);
    __atomic_store_n(&g_slot, s, __ATOMIC_RELEASE);
}

/* The calling process's slot. getpid() on every call, so a child made by fork, vfork, _Fork or a raw clone gets
 * its own slot even where no atfork handler runs. Counted calls are flushes, so the syscall is cheap beside them. */
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

static void v1_after_fork_child(void) {
    __atomic_store_n(&g_slot, NULL, __ATOMIC_RELEASE);
    __atomic_store_n(&g_slot_pid, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&g_claim_lock, 0, __ATOMIC_RELEASE);
}

/* ---- O_SYNC / O_DSYNC fd tracking ---- */
static inline void osync_set(int fd, int on) {
    if (fd < 0 || (unsigned)fd >= V1_FD_BITS) return;
    uint8_t bit = (uint8_t)(1u << (fd & 7));
    if (on) __atomic_fetch_or(&g_osync[fd >> 3], bit, __ATOMIC_RELAXED);
    else __atomic_fetch_and(&g_osync[fd >> 3], (uint8_t)~bit, __ATOMIC_RELAXED);
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
    if (k < 0) osync_set(fd, 0);
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

/* fds inherited across exec: read their status flags once, from /proc/self/fd (else fds 0..1023). */
static void v1_seed_fds(void) {
    RESOLVE(fcntl);
    DIR *d = opendir("/proc/self/fd"); /* glibc opens it internally: no wrapper runs */
    if (!d) {
        for (int fd = 0; fd < 1024; fd++) osync_set(fd, fl_kind(real_fcntl(fd, F_GETFL)) >= 0);
        return;
    }
    int dfd = dirfd(d);
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] < '0' || de->d_name[0] > '9') continue;
        int fd = atoi(de->d_name);
        if (fd == dfd) continue;
        osync_set(fd, fl_kind(real_fcntl(fd, F_GETFL)) >= 0);
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
    v1_seed_fds();
    __atomic_store_n(&g_hdr, h, __ATOMIC_RELEASE);
    int32_t me;
    (void)v1_slot_get(&me); /* claim now: an image that never flushes still shows it attached */
    pthread_atfork(NULL, NULL, v1_after_fork_child);
}
static void v1_ensure_slow(void) { pthread_once(&g_once, v1_init_once); }

__attribute__((constructor)) static void v1_ctor(void) { v1_ensure(); }

/* ---- recording ---- */
static void v1_rec(int kind, int fd, int64_t aux, uint64_t mark, uint64_t t0, uint64_t t1, int failed, int err,
                   int via) {
    int32_t me;
    v1_slot *s = v1_slot_get(&me);
    __atomic_fetch_add(&s->count[kind], 1, __ATOMIC_RELAXED);
    if (failed) __atomic_fetch_add(&s->fail[kind], 1, __ATOMIC_RELAXED);
    if (via) __atomic_fetch_add(&s->via_syscall, 1, __ATOMIC_RELAXED);
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
    ev->via_syscall = (uint8_t)(via ? 1 : 0);
    __atomic_store_n(&ev->kind, (int16_t)(kind + 1), __ATOMIC_RELEASE); /* kind+1: 0 means "never completed" */
}

#define V1_BEGIN()                                                                 \
    v1_ensure();                                                                   \
    const uint64_t mark_ = __atomic_load_n(&g_hdr->mark, __ATOMIC_SEQ_CST);        \
    const uint64_t t0_ = v1_now()
#define V1_END(kind, fd, aux, failed, via)                                         \
    do {                                                                           \
        int e_ = errno;                                                            \
        v1_rec((kind), (fd), (int64_t)(aux), mark_, t0_, v1_now(), (failed), e_, (via)); \
        errno = e_;                                                                \
    } while (0)

/* ---- flushes ---- */
int fsync(int fd) {
    RESOLVE(fsync);
    V1_BEGIN();
    int r = real_fsync(fd);
    V1_END(V1K_FSYNC, fd, 0, r == -1, 0);
    return r;
}
int fdatasync(int fd) {
    RESOLVE(fdatasync);
    V1_BEGIN();
    int r = real_fdatasync(fd);
    V1_END(V1K_FDATASYNC, fd, 0, r == -1, 0);
    return r;
}
int sync_file_range(int fd, __off64_t off, __off64_t n, unsigned int flags) {
    RESOLVE_OR(sync_file_range, -1);
    V1_BEGIN();
    int r = real_sync_file_range(fd, off, n, flags);
    V1_END(sfr_kind(flags), fd, flags, r == -1, 0);
    return r;
}
int syncfs(int fd) {
    RESOLVE_OR(syncfs, -1);
    V1_BEGIN();
    int r = real_syncfs(fd);
    V1_END(V1K_SYNCFS, fd, 0, r == -1, 0);
    return r;
}
void sync(void) {
    RESOLVE(sync);
    V1_BEGIN();
    real_sync();
    V1_END(V1K_SYNC, -1, 0, 0, 0);
}
int msync(void *a, size_t l, int f) {
    RESOLVE(msync);
    V1_BEGIN();
    int r = real_msync(a, l, f);
    V1_END(msync_kind(f), -1, f, r == -1, 0);
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
           r == -1, 0);
    return r;
}
ssize_t copy_file_range(int in, __off64_t *pin, int out, __off64_t *pout, size_t len, unsigned int flags) {
    RESOLVE_OR(copy_file_range, -1);
    V1_BEGIN();
    ssize_t r = real_copy_file_range(in, pin, out, pout, len, flags);
    V1_END(V1K_COPY_FILE_RANGE, out, (int64_t)len, r == -1, 0);
    return r;
}

/* ---- writes: counted only when the fd's status flags (re-read now) or pwritev2's flags ask for sync ---- */
static size_t iov_len(const struct iovec *v, int n) {
    size_t s = 0;
    for (int i = 0; v && i < n; i++) s += v[i].iov_len;
    return s;
}
#define V1_WRITE(kindexpr, call, fd, len)       \
    int k_ = (kindexpr);                        \
    if (k_ < 0) return call;                    \
    V1_BEGIN();                                 \
    ssize_t r_ = call;                          \
    V1_END(k_, fd, len, r_ == -1, 0);           \
    return r_

ssize_t write(int fd, const void *b, size_t n) {
    RESOLVE(write);
    V1_WRITE(osync_candidate(fd), real_write(fd, b, n), fd, n);
}
ssize_t __write(int fd, const void *b, size_t n) {
    RESOLVE_OR(__write, -1);
    V1_WRITE(osync_candidate(fd), real___write(fd, b, n), fd, n);
}
ssize_t pwrite(int fd, const void *b, size_t n, __off_t o) {
    RESOLVE(pwrite);
    V1_WRITE(osync_candidate(fd), real_pwrite(fd, b, n, o), fd, n);
}
#ifndef V1_MUTANT_DROP_PWRITE64 /* fire-check mutant: firecheck.py must catch its absence */
ssize_t pwrite64(int fd, const void *b, size_t n, __off64_t o) {
    RESOLVE_OR(pwrite64, -1);
    V1_WRITE(osync_candidate(fd), real_pwrite64(fd, b, n, o), fd, n);
}
#endif
ssize_t __pwrite64(int fd, const void *b, size_t n, __off64_t o) {
    RESOLVE_OR(__pwrite64, -1);
    V1_WRITE(osync_candidate(fd), real___pwrite64(fd, b, n, o), fd, n);
}
ssize_t writev(int fd, const struct iovec *v, int c) {
    RESOLVE(writev);
    V1_WRITE(osync_candidate(fd), real_writev(fd, v, c), fd, iov_len(v, c));
}
ssize_t pwritev(int fd, const struct iovec *v, int c, __off_t o) {
    RESOLVE_OR(pwritev, -1);
    V1_WRITE(osync_candidate(fd), real_pwritev(fd, v, c, o), fd, iov_len(v, c));
}
ssize_t pwritev64(int fd, const struct iovec *v, int c, __off64_t o) {
    RESOLVE_OR(pwritev64, -1);
    V1_WRITE(osync_candidate(fd), real_pwritev64(fd, v, c, o), fd, iov_len(v, c));
}
ssize_t pwritev2(int fd, const struct iovec *v, int c, __off_t o, int f) {
    RESOLVE_OR(pwritev2, -1);
    V1_WRITE(stronger(osync_candidate(fd), rwf_kind(f)), real_pwritev2(fd, v, c, o, f), fd, iov_len(v, c));
}
#ifndef V1_MUTANT_DROP_PWRITEV64V2 /* fire-check mutant */
ssize_t pwritev64v2(int fd, const struct iovec *v, int c, __off64_t o, int f) {
    RESOLVE_OR(pwritev64v2, -1);
    V1_WRITE(stronger(osync_candidate(fd), rwf_kind(f)), real_pwritev64v2(fd, v, c, o, f), fd, iov_len(v, c));
}
#endif

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
    RESOLVE_OR(open64, -1);
    return after_open(real_open64(p, flags, mode), flags);
}
int __open(const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE_OR(__open, -1);
    return after_open(real___open(p, flags, mode), flags);
}
int __open64(const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE_OR(__open64, -1);
    return after_open(real___open64(p, flags, mode), flags);
}
int openat(int d, const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE(openat);
    return after_open(real_openat(d, p, flags, mode), flags);
}
int openat64(int d, const char *p, int flags, ...) {
    V1_MODE(flags, flags);
    RESOLVE_OR(openat64, -1);
    return after_open(real_openat64(d, p, flags, mode), flags);
}
#ifndef V1_MUTANT_DROP_OPEN_2 /* fire-check mutant */
int __open_2(const char *p, int flags) {
    RESOLVE_OR(__open_2, -1);
    return after_open(real___open_2(p, flags), flags);
}
#endif
int __open64_2(const char *p, int flags) {
    RESOLVE_OR(__open64_2, -1);
    return after_open(real___open64_2(p, flags), flags);
}
int __openat_2(int d, const char *p, int flags) {
    RESOLVE_OR(__openat_2, -1);
    return after_open(real___openat_2(d, p, flags), flags);
}
int __openat64_2(int d, const char *p, int flags) {
    RESOLVE_OR(__openat64_2, -1);
    return after_open(real___openat64_2(d, p, flags), flags);
}
int creat(const char *p, mode_t m) {
    RESOLVE(creat);
    return after_open(real_creat(p, m), 0);
}
int creat64(const char *p, mode_t m) {
    RESOLVE_OR(creat64, -1);
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
    RESOLVE_OR(fcntl64, -1);
    return fcntl_after(real_fcntl64(fd, cmd, arg), fd, cmd);
}
int __fcntl(int fd, int cmd, ...) {
    V1_FCNTL_ARG();
    RESOLVE_OR(__fcntl, -1);
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
    RESOLVE_OR(__dup2, -1);
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
    osync_set(fd, 0);
    return real_close(fd);
}
int __close(int fd) {
    RESOLVE_OR(__close, -1);
    osync_set(fd, 0);
    return real___close(fd);
}

/* ---- syscall(2): the generic entry point. Flushes and clone ops through it are counted in their kind (and
 * tallied in via_syscall); fd lifecycle through it is tracked. Six pointer-sized varargs are forwarded, as glibc's
 * own syscall() reads six registers whatever the call. ---- */
struct v1_open_how { uint64_t flags, mode, resolve; };
static void v1_syscall_track(long n, long a0, long a1, long a2, long r) {
    if (r < 0) return;
    switch (n) {
#ifdef SYS_open
    case SYS_open: osync_set((int)r, flags_maybe_sync((int)a1)); break;
#endif
#ifdef SYS_creat
    case SYS_creat: osync_set((int)r, 0); break;
#endif
    case SYS_openat: osync_set((int)r, flags_maybe_sync((int)a2)); break;
#ifdef SYS_openat2
    case SYS_openat2: {
        const struct v1_open_how *h = (const struct v1_open_how *)a2;
        osync_set((int)r, h != NULL && (h->flags & (uint64_t)(O_SYNC | O_DSYNC)) != 0);
        break;
    }
#endif
    case SYS_dup:
#ifdef SYS_dup2
    case SYS_dup2:
#endif
    case SYS_dup3: osync_set((int)r, osync_get((int)a0)); break;
    case SYS_fcntl:
        if ((int)a1 == F_DUPFD || (int)a1 == F_DUPFD_CLOEXEC) osync_set((int)r, osync_get((int)a0));
        break;
    default: break;
    }
}
long syscall(long n, ...) {
    va_list ap;
    va_start(ap, n);
    long a0 = va_arg(ap, long);
    long a1 = va_arg(ap, long);
    long a2 = va_arg(ap, long);
    long a3 = va_arg(ap, long);
    long a4 = va_arg(ap, long);
    long a5 = va_arg(ap, long);
    va_end(ap);
    RESOLVE(syscall);
    int kind = -1, fd = -1;
    int64_t aux = 0;
    switch (n) {
    case SYS_fsync: kind = V1K_FSYNC; fd = (int)a0; break;
    case SYS_fdatasync: kind = V1K_FDATASYNC; fd = (int)a0; break;
    case SYS_sync_file_range: kind = sfr_kind((unsigned int)a3); fd = (int)a0; aux = (unsigned int)a3; break;
    case SYS_syncfs: kind = V1K_SYNCFS; fd = (int)a0; break;
    case SYS_sync: kind = V1K_SYNC; break;
    case SYS_msync: kind = msync_kind((int)a2); aux = (int)a2; break;
    case SYS_copy_file_range: kind = V1K_COPY_FILE_RANGE; fd = (int)a2; aux = a4; break;
    case SYS_ioctl:
        if ((unsigned long)a1 == V1_FICLONE) { kind = V1K_FICLONE; aux = a2; }
        else if ((unsigned long)a1 == V1_FICLONERANGE) kind = V1K_FICLONERANGE;
        fd = (int)a0;
        break;
    case SYS_write:
    case SYS_pwrite64: kind = osync_candidate((int)a0); fd = (int)a0; aux = a2; break;
    case SYS_writev:
    case SYS_pwritev: kind = osync_candidate((int)a0); fd = (int)a0; break;
    /* raw pwritev2 is (fd, iov, iovcnt, pos_l, pos_h, flags): the flags are the SIXTH argument */
    case SYS_pwritev2: kind = stronger(osync_candidate((int)a0), rwf_kind((int)a5)); fd = (int)a0; break;
    case SYS_close: osync_set((int)a0, 0); break;
    default: break;
    }
    if (kind < 0) {
        long r = real_syscall(n, a0, a1, a2, a3, a4, a5);
        int e = errno;
        v1_syscall_track(n, a0, a1, a2, r);
        errno = e;
        return r;
    }
    V1_BEGIN();
    long r = real_syscall(n, a0, a1, a2, a3, a4, a5);
    V1_END(kind, fd, aux, r == -1, 1);
    return r;
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
    if (len >= sizeof x->path) src += len - (sizeof x->path - 1);
    memcpy(x->path, src, strlen(src) + 1);
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
