/* syncshim.h -- V1 flush counter for Linux (glibc): shared-memory layout and the client mark API.
 *
 * Port of the macOS DYLD counter (artie-research frontier/fastest/tools/v1). One RUN = one POSIX shm object
 * "/v1.<run>" (in /dev/shm) made by `v1ctl create <run>`. Every process that loads syncshim.so (LD_PRELOAD, set
 * by `v1run`) claims a SLOT and counts its own flush calls there. A process whose pid changes (fork, vfork, a raw
 * clone) claims a new slot at its first counted call. Counts live in shared memory, so they survive SIGKILL of
 * the counted process and can be read live by another process. Each call is also appended to an EVENT log with
 * CLOCK_MONOTONIC timestamps and the run's current MARK, which a client (the load generator) sets around each
 * operation with v1_set_mark, so calls can be attributed per marked operation across processes.
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
#define V1_VERSION 2u

/* Kinds. Keep in step with V1_KIND_NAMES and with KINDS in firecheck.py. */
enum {
    V1K_FSYNC = 0,           /* fsync() */
    V1K_FDATASYNC = 1,       /* fdatasync() */
    V1K_SFR_WRITE_WAIT = 2,  /* sync_file_range() with SYNC_FILE_RANGE_WRITE and SYNC_FILE_RANGE_WAIT_AFTER */
    V1K_SFR_WRITE = 3,       /* sync_file_range() with WRITE but not WAIT_AFTER: starts writeback, does not wait */
    V1K_SFR_WAIT = 4,        /* sync_file_range() without WRITE: waits on writeback already in flight, or no-op */
    V1K_SYNCFS = 5,          /* syncfs() */
    V1K_SYNC = 6,            /* sync() */
    V1K_MSYNC_SYNC = 7,      /* msync() with MS_SYNC */
    V1K_MSYNC_OTHER = 8,     /* msync() without MS_SYNC (MS_ASYNC / MS_INVALIDATE) */
    V1K_OSYNC_WRITE = 9,     /* a write on an O_SYNC fd, or pwritev2(RWF_SYNC) */
    V1K_ODSYNC_WRITE = 10,   /* a write on an O_DSYNC (not O_SYNC) fd, or pwritev2(RWF_DSYNC) */
    V1K_FICLONE = 11,        /* clone op: ioctl(FICLONE) */
    V1K_FICLONERANGE = 12,   /* clone op: ioctl(FICLONERANGE) */
    V1K_COPY_FILE_RANGE = 13,/* clone op: copy_file_range() */
    V1K_NKINDS = 14
};
#define V1K_NFLUSH 11 /* kinds below this are flush requests; the rest are clone ops, never summed with flushes */
#define V1_KIND_SLOTS 16
static const char *const V1_KIND_NAMES[V1K_NKINDS] = {
    "fsync", "fdatasync", "sfr_write_wait", "sfr_write", "sfr_wait", "syncfs", "sync", "msync_SYNC",
    "msync_other", "osync_write", "odsync_write", "FICLONE", "FICLONERANGE", "copy_file_range"};

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
    int64_t aux;             /* flags (sync_file_range, msync, pwritev2) / write length / FICLONE source fd */
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
    uint64_t count[V1_KIND_SLOTS];
    uint64_t fail[V1_KIND_SLOTS];
    uint64_t fd_untracked;    /* writes on fds >= V1_FD_BITS (the O_SYNC tracker cannot see them) */
    uint64_t unresolved;      /* bit i: the shim's real-function table entry i was not found by dlsym */
    uint32_t flags;           /* V1_SLOT_* */
    uint32_t pad0;
    char exe[128];
    uint8_t pad[512 - 8 - 8 - 2 * 8 * V1_KIND_SLOTS - 8 - 8 - 8 - 128];
} v1_slot; /* 512 bytes */

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
_Static_assert(sizeof(v1_slot) == 512, "v1_slot");
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
