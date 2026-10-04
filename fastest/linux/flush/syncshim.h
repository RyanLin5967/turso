/* syncshim.h -- V1 flush counter for Linux (glibc): shared-memory layout and the client mark API.
 *
 * Port of the macOS DYLD counter (artie-research frontier/fastest/tools/v1). One RUN = one POSIX shm object
 * "/v1.<run>" (in /dev/shm) made by `v1ctl create <run>`. Every process that loads syncshim.so (LD_PRELOAD, set
 * by `v1run`) claims a SLOT and counts its own calls there: at load, and again in the child of every fork() (so a
 * forked process is visible, and its liveness checkable, before it counts anything); a process whose pid changes
 * without a fork handler (vfork, _Fork, a raw clone) claims at its first counted call. Counts live in shared memory,
 * so they survive SIGKILL of the counted process and can be read live by another process. Each call is also
 * appended to an EVENT log with CLOCK_MONOTONIC timestamps and the run's current MARK, which a client (the load
 * generator) sets around each operation with v1_set_mark.
 *
 * ONE MARK PER RUN: the mark is a single run-wide value, so an event's mark says which operation was in flight when
 * the call began, not which process asked for it. With concurrent clients or background processes (a checkpointer,
 * a WAL writer) per-operation attribution is only valid per slot (per process); read `v1ctl bymark` with that in mind.
 *
 * Every exec and posix_spawn the shim sees is written to an EXEC table; `v1ctl report` refuses when an exec'd
 * image never attached (a static binary, a CGO_ENABLED=0 Go build, a setuid binary, an envp without LD_PRELOAD):
 * that process was NOT counted, and a missing count must never read as zero.
 *
 * Clock: clock_gettime(CLOCK_MONOTONIC), system-wide; a load generator that wants to line events up with its own
 * timestamps must use the same clock.
 */
#ifndef SYNCSHIM_H
#define SYNCSHIM_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define V1_MAGIC 0x31584e4c4e595356ULL /* "VSYNLNX1" little-endian */
#define V1_VERSION 3u

/* Kinds, in three classes that are never summed across. Keep in step with V1_KIND_NAMES and firecheck.py KINDS.
 *   FLUSH      a request that data reach stable storage (the class stracecount.py calls a flush, plus sync writes)
 *   WRITEBACK  starts or waits on page-cache writeback, or is a no-op: makes nothing durable by itself
 *   CLONE      a clone or copy op (the FASTEST branch primitives), counted apart from both */
enum {
    V1K_FSYNC = 0,            /* FLUSH: fsync() */
    V1K_FDATASYNC = 1,        /* FLUSH: fdatasync() */
    V1K_SYNCFS = 2,           /* FLUSH: syncfs() */
    V1K_SYNC = 3,             /* FLUSH: sync() */
    V1K_MSYNC_SYNC = 4,       /* FLUSH: msync() with MS_SYNC */
    V1K_OSYNC_WRITE = 5,      /* FLUSH: a write of >= 1 byte on an O_SYNC fd, or pwritev2(RWF_SYNC) */
    V1K_ODSYNC_WRITE = 6,     /* FLUSH: a write of >= 1 byte on an O_DSYNC (not O_SYNC) fd, or pwritev2(RWF_DSYNC) */
    V1K_SFR_WRITE_WAIT = 7,   /* WRITEBACK: sync_file_range() with WRITE and WAIT_AFTER (no device cache flush) */
    V1K_SFR_WRITE = 8,        /* WRITEBACK: sync_file_range() with WRITE, no WAIT_AFTER: starts writeback only */
    V1K_SFR_WAIT = 9,         /* WRITEBACK: sync_file_range() without WRITE: waits on writeback in flight, or no-op */
    V1K_MSYNC_OTHER = 10,     /* WRITEBACK: msync() without MS_SYNC (MS_ASYNC is a no-op on Linux; MS_INVALIDATE) */
    V1K_FICLONE = 11,         /* CLONE: ioctl(FICLONE) */
    V1K_FICLONERANGE = 12,    /* CLONE: ioctl(FICLONERANGE) */
    V1K_COPY_FILE_RANGE = 13, /* CLONE: copy_file_range() (into an O_SYNC/O_DSYNC fd it is also a sync write) */
    V1K_NKINDS = 14
};
#define V1K_FLUSH_END 7      /* kinds [0, 7) are FLUSH */
#define V1K_WRITEBACK_END 11 /* kinds [7, 11) are WRITEBACK, [11, 14) CLONE */
#define V1_KIND_SLOTS 16
static const char *const V1_KIND_NAMES[V1K_NKINDS] = {
    "fsync", "fdatasync", "syncfs", "sync", "msync_SYNC", "osync_write", "odsync_write", "sfr_write_wait",
    "sfr_write", "sfr_wait", "msync_other", "FICLONE", "FICLONERANGE", "copy_file_range"};

/* ioctl numbers, spelled out so this header never includes <linux/fs.h> (whose RWF_* clash with glibc's). */
#define V1_FICLONE 0x40049409UL      /* _IOW(0x94, 9, int) */
#define V1_FICLONERANGE 0x4020940dUL /* _IOW(0x94, 13, struct file_clone_range), 32 bytes */

/* Marks: the client stores op ids here. V1_MARK_IDLE is OR-ed in after an ack, so calls that land between
 * operations (background work) are attributed to "after op k", not to op k. */
#define V1_MARK_IDLE ((uint64_t)1 << 63)

/* Slot flags. */
#define V1_SLOT_GO 1u /* the image is a Go binary: its syscalls are raw, this slot's counts are NOT its flushes */

typedef struct {
    uint64_t t0_ns, t1_ns;   /* CLOCK_MONOTONIC around the real call */
    uint64_t mark;           /* run mark at t0 */
    int64_t aux;             /* sync_file_range / msync: the flags; FICLONE: the source fd; writes, sendfile,
                                splice, copy_file_range: the byte count asked for; else 0 */
    int32_t pid;
    int32_t fd;
    uint32_t tid;            /* gettid() */
    uint32_t slot;
    int32_t ret;             /* 0, or -1 on failure */
    int16_t kind;            /* V1K_* + 1, stored last with release order; 0 = claimed, never completed */
    int16_t err;             /* errno when ret == -1, else 0 */
    uint8_t pad[8];
} v1_event; /* 64 bytes */

typedef struct {
    int32_t pid, ppid;        /* pid 0 = free slot */
    uint64_t attach_ns;
    uint64_t start_ticks;     /* /proc/<pid>/stat field 22 at claim: (pid, start_ticks) names the process for the
                                 liveness check (an exec keeps both) */
    uint64_t count[V1_KIND_SLOTS];
    uint64_t fail[V1_KIND_SLOTS];
    uint64_t missed[V1_KIND_SLOTS]; /* calls of a counted kind seen going through syscall(2): NOT in count[] */
    uint64_t fd_untracked;    /* writes on fds >= V1_FD_BITS (the O_SYNC tracker cannot see them) */
    uint64_t unresolved;      /* bit i: the shim's real-function table entry i was not found by dlsym */
    uint64_t async_io;        /* io_uring_setup/enter/register, io_setup, io_submit seen through syscall(2) */
    uint64_t inflight;        /* counted calls entered and not yet returned (a SIGKILL inside one leaves it > 0) */
    uint32_t flags;           /* V1_SLOT_* */
    uint32_t pad0;
    char exe[128];
    uint8_t pad[1024 - 8 - 8 - 8 - 3 * 8 * V1_KIND_SLOTS - 4 * 8 - 8 - 128];
} v1_slot; /* 1024 bytes */

/* Exec records: one per exec-family call or successful posix_spawn the shim saw. */
enum { V1X_PENDING = 1, V1X_FAILED = 2, V1X_SPAWNED = 3 };
typedef struct {
    int32_t pid;              /* the pid that runs the new image (exec keeps the pid; posix_spawn: the child) */
    int32_t by_pid;           /* the caller */
    uint64_t t_ns;            /* CLOCK_MONOTONIC before the call: the new image attaches after this */
    int32_t state;            /* V1X_* (stored last with release order); 0 = claimed, never written */
    int32_t via;              /* index into V1_EXEC_VIA */
    char path[104];
} v1_exec; /* 128 bytes */
static const char *const V1_EXEC_VIA[] = {"?", "execve", "execv", "execvp", "execvpe", "execl", "execle", "execlp",
                                          "fexecve", "execveat", "posix_spawn", "posix_spawnp"};
#define V1_NVIA ((int)(sizeof V1_EXEC_VIA / sizeof *V1_EXEC_VIA))

typedef struct {
    uint64_t magic;
    uint32_t version, hdr_size;
    uint64_t total_size;
    uint32_t nslots, ev_cap;
    uint32_t exec_cap, pad0;
    uint64_t created_ns;
    int32_t root_pid;                 /* set by v1run just before exec: the pid that MUST attach */
    int32_t pad1;
    volatile uint64_t mark;
    volatile uint64_t slots_used;     /* claimed slots; slot index = fetch_add + 1 (slot 0 = overflow) */
    volatile uint64_t slot_overflow;  /* claims refused for lack of a slot (their counts go to slot 0) */
    volatile uint64_t ev_next;        /* events claimed */
    volatile uint64_t ev_dropped;     /* events not stored because the log was full */
    volatile uint64_t exec_next;      /* exec records claimed (> exec_cap means some were dropped) */
    uint8_t pad[4096 - 7 * 8 - 6 * 8];
} v1_hdr; /* 4096 bytes; then slots, then exec records, then events */

_Static_assert(sizeof(v1_event) == 64, "v1_event");
_Static_assert(sizeof(v1_slot) == 1024, "v1_slot");
_Static_assert(sizeof(v1_exec) == 128, "v1_exec");
_Static_assert(sizeof(v1_hdr) == 4096, "v1_hdr");

#define V1_FD_BITS (1u << 20)

static inline size_t v1_total_size(uint32_t nslots, uint32_t exec_cap, uint32_t ev_cap) {
    return sizeof(v1_hdr) + (size_t)nslots * sizeof(v1_slot) + (size_t)exec_cap * sizeof(v1_exec) +
           (size_t)ev_cap * sizeof(v1_event);
}
static inline v1_slot *v1_slots(v1_hdr *h) { return (v1_slot *)((char *)h + sizeof(v1_hdr)); }
static inline v1_exec *v1_execs(v1_hdr *h) {
    return (v1_exec *)((char *)h + sizeof(v1_hdr) + (size_t)h->nslots * sizeof(v1_slot));
}
static inline v1_event *v1_events(v1_hdr *h) {
    return (v1_event *)((char *)h + sizeof(v1_hdr) + (size_t)h->nslots * sizeof(v1_slot) +
                        (size_t)h->exec_cap * sizeof(v1_exec));
}

static inline uint64_t v1_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Parse /proc/<pid>/stat text: the state letter (field 3) and starttime (field 22), read after the LAST ')' so a
 * command name holding spaces or parentheses cannot shift the fields. Returns 0, or -1 if the text is malformed. */
static inline int v1_parse_stat(const char *buf, size_t n, char *state, uint64_t *start_ticks) {
    const char *p = NULL;
    for (size_t i = 0; i < n; i++)
        if (buf[i] == ')') p = buf + i;
    if (!p || (size_t)(p - buf) + 3 >= n || p[1] != ' ') return -1;
    const char *end = buf + n;
    p += 2;
    *state = *p;
    for (int field = 3; field < 22; field++) { /* skip to field 22 */
        while (p < end && *p != ' ') p++;
        if (p >= end) return -1;
        p++;
    }
    uint64_t v = 0;
    int digits = 0;
    while (p < end && *p >= '0' && *p <= '9') { v = v * 10 + (uint64_t)(*p - '0'); p++; digits++; }
    if (!digits) return -1;
    *start_ticks = v;
    return 0;
}

/* "/v1.<run>". Returns 0, or -1 if the run name is empty, longer than 26 or has '/'. */
static inline int v1_shm_name(const char *run, char *out, size_t outsz) {
    size_t n = run ? strlen(run) : 0;
    if (n == 0 || n > 26 || strchr(run, '/')) return -1;
    snprintf(out, outsz, "/v1.%s", run);
    return 0;
}

/* Map an existing run read-write. Returns NULL (and sets *why) on any mismatch: never a silent half-attach. */
static inline v1_hdr *v1_map(const char *run, const char **why) {
    char name[40];
    if (v1_shm_name(run, name, sizeof name) != 0) { *why = "bad run name (1-26 chars, no '/')"; return NULL; }
    int fd = shm_open(name, O_RDWR | O_CLOEXEC, 0);
    if (fd < 0) { *why = "shm_open failed: run does not exist (v1ctl create first)"; return NULL; }
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)sizeof(v1_hdr)) { close(fd); *why = "shm too small"; return NULL; }
    void *p = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { *why = "mmap failed"; return NULL; }
    v1_hdr *h = (v1_hdr *)p;
    if (__atomic_load_n(&h->magic, __ATOMIC_ACQUIRE) != V1_MAGIC || h->version != V1_VERSION ||
        h->total_size != (uint64_t)st.st_size || v1_total_size(h->nslots, h->exec_cap, h->ev_cap) != h->total_size) {
        munmap(p, (size_t)st.st_size);
        *why = "bad magic/version/size";
        return NULL;
    }
    return h;
}

/* Client API. v1_client() maps the run named by SYNCSHIM_RUN (set by v1run), or returns NULL when the variable is
 * unset; a client that was launched under v1run and gets NULL here must refuse, not run unmarked. */
static inline v1_hdr *v1_client(const char **why) {
    const char *run = getenv("SYNCSHIM_RUN");
    if (!run || !*run) { *why = "SYNCSHIM_RUN is unset"; return NULL; }
    return v1_map(run, why);
}
static inline void v1_set_mark(v1_hdr *h, uint64_t m) { __atomic_store_n(&h->mark, m, __ATOMIC_SEQ_CST); }

#endif
