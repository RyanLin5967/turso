/* clonebench.c -- the "just copy the file" baselines (PREREG §6 B0 and B1), embedded.
 *
 *   clonebench extents FILE
 *   clonebench run --mode b1|b0 --op m1c|m1 --parent DB --dir BRANCHDIR --clients C --out OUT
 *       [--max-ops N | --duration-s S [--min-ops N]] [--warmup-ops W] [--hold-us U] [--sync d2|d0] [--rows R]
 *       [--warmup OPS:S:MAX_S] [--max-window-s S]
 *       [--drop] [--seed S] [--v1-run NAME [--v1-mark-base B]] [--mutant-early-ack]
 *   clonebench --warmup-replay OPS:S:MAX_S < TRACE
 *       the warm-up decision (warm_ends, the one each claim makes) replayed on claim times, for the shared
 *       cross-driver test fastest/linux/gates/warmup_conformance.py (PREREG annex A23)
 *   clonebench par --src FILE --dir D --procs P --n N --out OUT
 *       D0 clonefile throughput with P processes (M0 exit 3; the decider's clone_par.py, compiled).
 *
 * One create = [checkpoint the parent if its WAL is not empty, exclusively] + clonefile(parent, branch) (clones run
 *   concurrently with each other: a read lock against the checkpoint)
 *   + the barrier. B1: each create issues its own F_FULLFSYNC of BRANCHDIR (the decider's one-barrier recipe,
 *   2.96 ms p50 at C=1). B0: ONE flusher thread, one flight in the air: a create takes a ticket after its clonefile
 *   returns and waits until a flight that SNAPSHOTTED the tickets before issuing its F_FULLFSYNC has returned.
 *   Adaptive hold (DESIGN.md §3): with C > 1, the next flight starts when as many creates wait as rode the last
 *   flight, or after --hold-us (default 300 = 10% of a 3 ms device flush), whichever is first.
 *   --sync d0: no barrier at all (report only).
 * m1c = create + open the branch (sqlite3_open_v2, synchronous=FULL, fullfsync=1, read sqlite_schema).
 * m1  = m1c + UPDATE t SET v = v + 1 WHERE id = random (autocommit; SQLite's own WAL commit and its own barriers;
 *       PREREG §6 gives B0 no first-write grouping). V1 at C=1 (2026-10-04): the first commit into a fresh WAL is
 *       2 F_FULLFSYNC (WAL header, then the frame) + 1 directory fsync, so M1 = 3 barriers for B0/B1.
 *       Branch connections close with SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, and by default open in
 *       locking_mode=EXCLUSIVE: the server owns each branch connection, so the WAL index lives in heap memory and no
 *       -shm file is created in the branch directory (at C=64 the stock open, which creates -shm beside 64 landing
 *       clones and the flusher's directory F_FULLFSYNC, held the clients away from the flights: smoke 2026-10-04,
 *       open p50 92 ms, 2.0 creates per flight). --branch-shared restores the stock open. --drop unlinks each branch.
 * --mutant-early-ack: B0 acknowledges a create before its flight (fire-check: the V2 ordering check must fail).
 * --mutant-branch-nosync: branch connections run synchronous=OFF, so an acknowledged first write is not durable.
 * Under c1brun (C1B_RUN set) every create and first write records OPSTART/ACK ("create:<branch path>",
 * "write:<branch path>") for the C1b crash-state enumerator (tools/c1b).
 *
 * Output (OUT must not exist): raw.tsv (one row per op; clone_done_ns and flight are the V2 evidence),
 * flights.tsv (b0), summary.json, hdr_total.txt. Clock: CLOCK_UPTIME_RAW (the V1 shim's).
 * Every directory must be on APFS with a *.noindex component, or the command refuses (rc 2).
 * Exit: 0 ok | 2 usage/refused | 3 no measured op succeeded, or any op failed.
 *
 * LINUX PORT (lane fastest-linux-comp; source artie-research frontier/fastest/tools/baselines/clonebench.c
 * @10c484e4a). Every change is an #if block; the macOS branches are the original lines. On Linux:
 *   - clonefile(src, dst) = open dst O_CREAT|O_EXCL + ioctl(FICLONE) of the whole source; directories must be XFS
 *     or btrfs (FICLONE-capable) with a *.noindex component (kept so paths stay interchangeable with the Mac's).
 *   - B1's per-create flush (lane brief): fsync(clone fd), then fsync(branch dir fd). --sync d0: neither.
 *     On Linux fsync IS the device flush (no F_FULLFSYNC split), so B1 = 2 flushes per create by construction.
 *   - --mode b0 REFUSES (rc 2): its flight design rests on F_FULLFSYNC being separate from fsync, and Linux needs
 *     its own registered recipe before a B0 number means anything.
 *   - extents: FIEMAP (FS_IOC_FIEMAP), which also reports FIEMAP_EXTENT_SHARED extents (the clone proof).
 *   - clock CLOCK_MONOTONIC; V1/C1b hooks only with BB_HOOKS=1 (as bbload.c): --v1-run and C1B_RUN refuse without.
 *   - --drop is a durable delete: the unlinks, then fsync(branch dir) under --sync d2 (op err 4 if it fails),
 *     untimed (after_ns). With no warm-up at all the run starts in the measured window, as bbload's does.
 *   - no mkparent (lead review 62430d8bf..b49fb656a LOW 27): the Mac's own-generator parent is replaced on Linux by
 *     gen_seed.py's stream, loaded by fixture.py sqlite (gate-6 review, t3run item 4: one parent for every system).
 *   - the warm-up (PREREG annex A23 / A23-AM1): --warmup OPS:S:MAX_S is the macOS bbload's claim_op rule, decided AT
 *     EACH CLAIM of an op by warm_ends (claim_warm): the warm-up ends at the first claim with OPS warm-up ops claimed
 *     before it and S seconds since t0 (done), or with MAX_S seconds since t0 (capped), in integer ns with seconds
 *     truncated as (uint64_t)(S * 1e9); the ending claim is the first measured op. MAX_S is a bound, never "no limit":
 *     MAX_S 0 ends the warm-up at the first claim. --warmup-ops alone takes S 0 and MAX_S 0.1 x --max-window-s (as the
 *     macOS bbload takes 0.1 x its run cap), and is refused without a window bound. summary.json records warmup_ops,
 *     warmup_end_ns (the ending claim, ns since t0), warmup_capped, warmup_last_claim_ns (null with no warm-up op) and
 *     warmup_backsteps (claims judged with an older time than the one before them: 0, each claim's time being read
 *     under the claim lock).
 */
#ifndef BB_HOOKS
#ifdef __APPLE__
#define BB_HOOKS 1
#else
#define BB_HOOKS 0
#endif
#endif
#if BB_HOOKS
#ifndef __APPLE__
#error "BB_HOOKS=1 needs the Linux V1/C1b shim (lane fastest-linux-flush); build with -DBB_HOOKS=0 until it lands"
#endif
#include "../v1/syncshim.h"
#include "../c1b/c1btrace.h"
#else
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
/* Hooks off: the names the call sites use, never reached (V1 stays NULL and g_have_c1b 0; the inputs refuse). */
typedef struct v1_hdr v1_hdr;
typedef struct { int unused; } c1b_client;
#define V1_MARK_IDLE (1ULL << 63)
static inline void v1_set_mark(v1_hdr *h, uint64_t m) { (void)h; (void)m; }
static inline void c1b_opstart(c1b_client *c, uint64_t op, const char *l) { (void)c; (void)op; (void)l; }
static inline void c1b_ack(c1b_client *c, uint64_t op, const char *l) { (void)c; (void)op; (void)l; }
#endif
#include <errno.h>
#include <hdr/hdr_histogram.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdlib.h>
#ifdef __APPLE__
#include <sys/clonefile.h>
#include <sys/mount.h>
#else
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <linux/magic.h>
#include <sys/ioctl.h>
#include <sys/vfs.h>
#ifndef XFS_SUPER_MAGIC
#define XFS_SUPER_MAGIC 0x58465342
#endif
#ifndef BTRFS_SUPER_MAGIC
#define BTRFS_SUPER_MAGIC 0x9123683E
#endif
#endif
#include <math.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>

#define MIB (1u << 20)
#ifdef __APPLE__
#define BB_CLOCK_NAME "CLOCK_UPTIME_RAW"
#define B1_BARRIER "F_FULLFSYNC(dir)"
static uint64_t now_ns(void) { return clock_gettime_nsec_np(CLOCK_UPTIME_RAW); }
#else
#define BB_CLOCK_NAME "CLOCK_MONOTONIC"
#define B1_BARRIER "fsync(clone)+fsync(dir)"
static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}
/* clonefile(src, dst, 0) on Linux: a NEW file (O_EXCL: clonefile refuses an existing dst) that shares every extent
 * of src (FICLONE). clone_open returns the open dst fd (B1 fsyncs it), or -1 with errno and no dst left behind. */
static int clone_open(const char *src, const char *dst) {
    int s = open(src, O_RDONLY | O_CLOEXEC);
    if (s < 0) return -1;
    int d = open(dst, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (d < 0) { int e = errno; close(s); errno = e; return -1; }
    if (ioctl(d, FICLONE, s) != 0) { int e = errno; close(d); close(s); unlink(dst); errno = e; return -1; }
    close(s);
    return d;
}
static int clonefile(const char *src, const char *dst, int flags) {
    (void)flags;
    int d = clone_open(src, dst);
    if (d < 0) return -1;
    close(d);
    return 0;
}
#endif
/* Its own TracerPid (lead review 62430d8bf..b49fb656a MED 4), read at the measured window's start and end into
 * summary.json tracerpid_tm0/tm1: -1 when it cannot be read (no /proc: not Linux), which timedrun.py refuses. */
static int self_tracerpid(void) {
#ifdef __linux__
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return -1;
    char ln[256];
    int tp = -1;
    while (fgets(ln, sizeof ln, f))
        if (sscanf(ln, "TracerPid: %d", &tp) == 1) break;
    fclose(f);
    return tp;
#else
    return -1;
#endif
}
static void die(const char *w) { fprintf(stderr, "clonebench: %s: %s\n", w, strerror(errno)); exit(2); }
static uint64_t xs(uint64_t *s) { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; return *s; }

static int guard_dir(const char *d) {
    struct statfs sf;
    if (statfs(d, &sf) != 0) { fprintf(stderr, "clonebench: REFUSED: %s: %s\n", d, strerror(errno)); return -1; }
#ifdef __APPLE__
    if (strcmp(sf.f_fstypename, "apfs")) { fprintf(stderr, "clonebench: REFUSED: %s is %s, not apfs\n", d, sf.f_fstypename); return -1; }
#else
    uint32_t ft = (uint32_t)sf.f_type;
    if (ft != (uint32_t)XFS_SUPER_MAGIC && ft != (uint32_t)BTRFS_SUPER_MAGIC) {
        fprintf(stderr, "clonebench: REFUSED: %s is filesystem type 0x%x, not xfs or btrfs (FICLONE)\n", d, ft);
        return -1;
    }
#endif
    if (!strstr(d, ".noindex/") && !(strlen(d) >= 8 && !strcmp(d + strlen(d) - 8, ".noindex"))) {
        fprintf(stderr, "clonebench: REFUSED: %s has no *.noindex component\n", d);
        return -1;
    }
    return 0;
}
static int guard_file_dir(const char *f) {
    char d[2048];
    snprintf(d, sizeof d, "%s", f);
    char *sl = strrchr(d, '/');
    if (!sl) return guard_dir(".");
    *sl = 0;
    return guard_dir(d);
}

/* ---------- extents ---------- */
#ifdef __APPLE__
static long extents(const char *path, off_t *size_out) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    fstat(fd, &st);
    off_t off = 0;
    long n = 0;
    while (off < st.st_size) {
        struct log2phys l = {0};
        l.l2p_contigbytes = st.st_size - off;
        l.l2p_devoffset = off;
        if (fcntl(fd, F_LOG2PHYS_EXT, &l) == -1 || l.l2p_contigbytes <= 0) { n = -1; break; }
        off += l.l2p_contigbytes;
        n++;
    }
    close(fd);
    if (size_out) *size_out = st.st_size;
    return n;
}
#else
static long g_ext_shared; /* FIEMAP_EXTENT_SHARED extents found by the last extents() call */
static long extents(const char *path, off_t *size_out) {
    enum { NEXT = 256 };
    uint64_t buf[(sizeof(struct fiemap) + NEXT * sizeof(struct fiemap_extent)) / sizeof(uint64_t) + 1];
    struct fiemap *fm = (struct fiemap *)buf;
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    fstat(fd, &st);
    if (size_out) *size_out = st.st_size;
    long n = 0;
    uint64_t start = 0;
    g_ext_shared = 0;
    for (;;) {
        memset(fm, 0, sizeof *fm);
        fm->fm_start = start;
        fm->fm_length = FIEMAP_MAX_OFFSET - start;
        fm->fm_flags = FIEMAP_FLAG_SYNC;
        fm->fm_extent_count = NEXT;
        if (ioctl(fd, FS_IOC_FIEMAP, fm) != 0) { n = -1; break; }
        if (fm->fm_mapped_extents == 0) break;
        int last = 0;
        for (unsigned i = 0; i < fm->fm_mapped_extents; i++) {
            struct fiemap_extent *e = &fm->fm_extents[i];
            n++;
            if (e->fe_flags & FIEMAP_EXTENT_SHARED) g_ext_shared++;
            if (e->fe_flags & FIEMAP_EXTENT_LAST) last = 1;
            start = e->fe_logical + e->fe_length;
        }
        if (last) break;
    }
    close(fd);
    return n;
}
#endif

/* ---------- sqlite helpers ---------- */
static void sq_exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) { fprintf(stderr, "clonebench: sqlite: %s (%s)\n", err, sql); exit(2); }
}
static sqlite3 *sq_open(const char *path, int create) {
    sqlite3 *db;
    int fl = SQLITE_OPEN_READWRITE | (create ? SQLITE_OPEN_CREATE : 0) | SQLITE_OPEN_NOMUTEX;
    if (sqlite3_open_v2(path, &db, fl, NULL) != SQLITE_OK) { fprintf(stderr, "clonebench: open %s: %s\n", path, sqlite3_errmsg(db)); return NULL; }
    return db;
}

/* ---------- run ---------- */
enum { PH_INIT, PH_WARM, PH_MEAS, PH_DRAIN };
typedef struct {
    uint32_t client, seq;
    uint8_t phase, ok, nsteps, pad;
    int16_t err;
    uint64_t start, end, after_end, clone_done, ticket, flight;
    uint64_t step_end[3];
} rec_t;
typedef struct { uint64_t t0, t1, riders, last_ticket; } flight_t;
typedef struct { int id; pthread_t th; uint64_t rng; rec_t *rec; size_t n, cap; } client_t;

static int MODE_B0, OP_M1, SYNC_D0, DROP, MUTANT_EARLY, BRANCH_SHARED, MUTANT_BNOSYNC, C = 1;
static c1b_client g_c1b;
static int g_have_c1b;
static const char *PARENT, *BDIR;
static long ROWS;
static uint64_t MAX_OPS, MIN_OPS, WARM_OPS, HOLD_US = 300, MARKB, SEED = 1;
static double DUR_S;
/* --warmup OPS:S:MAX_S (gate-6 review, t3run item 3, as bbload; PREREG annex A23): decided at each claim by
 * warm_ends, see the header. PREREG :210 = 1000:10:<10% of the cap>. WARM_S_NS and WARM_MAX_NS are its seconds
 * truncated to ns once, as (uint64_t)(S * 1e9). */
static double WARM_S, WARM_MAX_S;
static uint64_t WARM_S_NS, WARM_MAX_NS;
static char WARM_RULE[64];
/* A23: the claim state, under g_claim_mu (claim_warm), and the window's start as the ending claim opened it */
static pthread_mutex_t g_claim_mu = PTHREAD_MUTEX_INITIALIZER;
static volatile uint64_t g_t0, g_tm0; /* the run's t0 (the warm-up clock's zero), the window's start (0: not yet) */
static int g_warm_capped, g_warm_any;
static uint64_t g_warm_end_ns, g_warm_last_ns;
static struct rusage g_ru0;
static uint64_t g_fl0;
static int g_tp_tm0 = -2; /* MED 4: own TracerPid at the window's start (-2: never reached) */
/* --max-window-s S (gate-6 review, t3run item 16): the registered per-run cap as a bound on the measured window (0: none). */
static double MAXWIN_S;
static int CAPPED;
static int g_dirfd;
static sqlite3 *g_parent;
static char g_parent_wal[2100];
/* Clonefiles of the idle parent run concurrently (read side); only a parent checkpoint is exclusive (write side). */
static pthread_rwlock_t g_parent_rw = PTHREAD_RWLOCK_INITIALIZER;
static uint64_t g_checkpoints;
static volatile int g_phase = PH_INIT;
static volatile uint64_t g_warm, g_meas, g_done;
static v1_hdr *V1;
static char RUN_TAG[32];
/* B0 flusher */
static pthread_mutex_t f_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t f_work = PTHREAD_COND_INITIALIZER, f_done = PTHREAD_COND_INITIALIZER;
static uint64_t f_submitted, f_durable, f_prev_riders = 1, f_nflights, f_cap;
static int f_stop;
static flight_t *f_log;
static int f_err;

static void *flusher(void *arg) {
    (void)arg;
    pthread_mutex_lock(&f_mu);
    for (;;) {
        while (f_submitted == f_durable && !f_stop) pthread_cond_wait(&f_work, &f_mu);
        if (f_submitted == f_durable && f_stop) break;
        if (C > 1 && HOLD_US > 0) {
            uint64_t deadline = now_ns() + HOLD_US * 1000;
            while (f_submitted - f_durable < f_prev_riders) {
                uint64_t n = now_ns();
                if (n >= deadline) break;
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                uint64_t ns = (uint64_t)ts.tv_nsec + (deadline - n);
                ts.tv_sec += (time_t)(ns / 1000000000ULL);
                ts.tv_nsec = (long)(ns % 1000000000ULL);
                pthread_cond_timedwait(&f_work, &f_mu, &ts);
            }
        }
        uint64_t snap = f_submitted; /* every ticket <= snap had its clonefile return before this line */
        pthread_mutex_unlock(&f_mu);
        uint64_t t0 = now_ns();
#ifdef __APPLE__
        int rc = SYNC_D0 ? 0 : fcntl(g_dirfd, F_FULLFSYNC);
#else
        int rc = -1; /* unreachable: --mode b0 is refused off macOS */
        errno = ENOTSUP;
#endif
        uint64_t t1 = now_ns();
        pthread_mutex_lock(&f_mu);
        if (rc == -1) f_err = errno;
        if (f_nflights == f_cap) { f_cap = f_cap ? f_cap * 2 : 4096; f_log = realloc(f_log, f_cap * sizeof *f_log); }
        f_log[f_nflights++] = (flight_t){t0, t1, snap - f_durable, snap};
        f_prev_riders = snap - f_durable;
        f_durable = snap;
        pthread_cond_broadcast(&f_done);
    }
    pthread_mutex_unlock(&f_mu);
    return NULL;
}

static int create_branch(const char *dst, rec_t *r) {
    struct stat st;
    pthread_rwlock_rdlock(&g_parent_rw);
    if (stat(g_parent_wal, &st) == 0 && st.st_size > 0) {
        pthread_rwlock_unlock(&g_parent_rw);
        pthread_rwlock_wrlock(&g_parent_rw);
        if (stat(g_parent_wal, &st) == 0 && st.st_size > 0) {
            if (sqlite3_wal_checkpoint_v2(g_parent, NULL, SQLITE_CHECKPOINT_TRUNCATE, NULL, NULL) != SQLITE_OK) {
                pthread_rwlock_unlock(&g_parent_rw);
                return -1;
            }
            g_checkpoints++;
        }
        pthread_rwlock_unlock(&g_parent_rw);
        pthread_rwlock_rdlock(&g_parent_rw);
    }
#ifdef __APPLE__
    int rc = clonefile(PARENT, dst, 0);
    pthread_rwlock_unlock(&g_parent_rw);
    if (rc != 0) return -1;
    r->clone_done = now_ns();
    if (SYNC_D0 && !MODE_B0) return 0;
    if (!MODE_B0) return fcntl(g_dirfd, F_FULLFSYNC) == -1 ? -1 : 0;
#else
    int cfd = clone_open(PARENT, dst);
    pthread_rwlock_unlock(&g_parent_rw);
    if (cfd < 0) return -1;
    r->clone_done = now_ns();
    /* B1 (Linux): fsync the clone (its extent map and inode), then fsync the directory (its entry). D0: neither. */
    int brc = 0;
    if (!SYNC_D0) brc = (fsync(cfd) != 0 || fsync(g_dirfd) != 0) ? -1 : 0;
    close(cfd);
    return brc; /* b0 is refused off macOS, so nothing below runs here */
#endif
    pthread_mutex_lock(&f_mu);
    uint64_t t = ++f_submitted;
    r->ticket = t;
    pthread_cond_signal(&f_work);
    if (!MUTANT_EARLY)
        while (f_durable < t) pthread_cond_wait(&f_done, &f_mu);
    uint64_t j = f_nflights; /* the flight that covered ticket t: the first whose snapshot reached t */
    while (!MUTANT_EARLY && j > 1 && f_log[j - 2].last_ticket >= t) j--;
    r->flight = MUTANT_EARLY ? 0 : j;
    int err = f_err;
    pthread_mutex_unlock(&f_mu);
    return err ? -1 : 0;
}

/* The warm-up rule, the definition every load driver matches (the lead's ruling on fastest-linux's fourth lane review
 * LOW 14; PREREG annex A23): at a claim, with CLAIMED warm-up ops claimed before it and EL ns since t0, the warm-up
 * ends at this claim when CLAIMED >= OPS and EL >= S_NS (done) or EL >= MAX_NS (capped). 0: go on, this claim is a
 * warm-up op; 1: it ends done; 2: it ends capped. Verbatim from the macOS bbload (artie 4010ff3b06), whose claim_op
 * is the validated implementation (A23-AM1); claim_warm decides with it, and --warmup-replay replays it. */
static int warm_ends(uint64_t claimed, uint64_t el, uint64_t ops, uint64_t s_ns, uint64_t max_ns) {
    int done = claimed >= ops && el >= s_ns;
    return done ? 1 : el >= max_ns ? 2 : 0;
}

/* --warmup-replay OPS:S:MAX_S < TRACE: warm_ends on claim times read from stdin (integer ns since t0, one per line,
 * non-decreasing). Prints "stop_at=I warm_ops=N capped=0|1", or "stop_at=none warm_ops=N capped=none" when the trace
 * ends first; seconds become ns as claim_warm makes them, (uint64_t)(S * 1e9). The macOS bbload's (artie 4010ff3b06)
 * with clonebench's name in its messages, so the shared harness feeds every driver the same claims. */
static int warmup_replay(const char *rule) {
    unsigned long long ops;
    double s, m;
    char tail;
    if (rule[0] < '0' || rule[0] > '9' || sscanf(rule, "%llu:%lf:%lf%c", &ops, &s, &m, &tail) != 3 || !isfinite(s) ||
        !isfinite(m) || !(s >= 0) || !(m > 0)) {
        fprintf(stderr, "clonebench: --warmup-replay OPS:S:MAX_S (OPS an integer, S and MAX_S finite seconds, MAX_S > 0): %s\n", rule);
        return 2;
    }
    uint64_t claimed = 0, prev = 0;
    char line[64];
    while (fgets(line, sizeof line, stdin)) {
        if (line[0] == '\n') continue;
        char *e;
        errno = 0;
        unsigned long long el = strtoull(line, &e, 10);
        if (line[0] < '0' || line[0] > '9' || (*e != '\n' && *e != '\0') || errno || el < prev) {
            fprintf(stderr, "clonebench: --warmup-replay: bad trace line: %s", line);
            return 2;
        }
        prev = el;
        int end = warm_ends(claimed, el, ops, (uint64_t)(s * 1e9), (uint64_t)(m * 1e9));
        if (end) {
            printf("stop_at=%llu warm_ops=%llu capped=%d\n", (unsigned long long)claimed, (unsigned long long)claimed,
                   end == 2);
            return 0;
        }
        claimed++;
    }
    printf("stop_at=none warm_ops=%llu capped=none\n", (unsigned long long)claimed);
    return 0;
}

/* claim_warm -- the warm-up decision of one claim, serialized under g_claim_mu as the macOS bbload's claim_op
 * serializes under g_claim (A23; this port used to decide on a 1 ms main-thread poll of the claimed count, comparing
 * seconds as doubles, with MAX_S 0 as no limit). The claim's time T is read UNDER the lock, as claim_op reads now_ns()
 * after taking g_claim, so claims are judged in lock order with non-decreasing times (fastest-linux's LOW on
 * f6c2dbb2f: it was read before the lock, so a slower claimant could be judged later with an older time). Returns
 * PH_WARM when this claim is a warm-up op, else the phase now in force: the claim that ends the warm-up opens the
 * window at T and is its first measured op. g_warm_backsteps counts claims judged with an older time than the one
 * judged before them: 0 by construction here, recorded so timedrun.py can refuse a run where it is not. */
static uint64_t g_claim_prev_el, g_warm_backsteps;
static int claim_warm(void) {
    pthread_mutex_lock(&g_claim_mu);
    int ph = __atomic_load_n(&g_phase, __ATOMIC_ACQUIRE);
    if (ph == PH_WARM) {
        uint64_t t = now_ns();
        uint64_t el = t > g_t0 ? t - g_t0 : 0;
        if (el < g_claim_prev_el) g_warm_backsteps++;
        g_claim_prev_el = el;
        int end = warm_ends(g_warm, el, WARM_OPS, WARM_S_NS, WARM_MAX_NS);
        if (end) {
            g_warm_capped = end == 2;
            g_warm_end_ns = el;
            getrusage(RUSAGE_SELF, &g_ru0);
            pthread_mutex_lock(&f_mu); /* lock order g_claim_mu -> f_mu; nothing holding f_mu takes g_claim_mu */
            g_fl0 = f_nflights;
            pthread_mutex_unlock(&f_mu);
            __atomic_store_n(&g_tm0, t, __ATOMIC_RELEASE);
            __atomic_store_n(&g_phase, PH_MEAS, __ATOMIC_RELEASE);
            if (V1 && C > 1) v1_set_mark(V1, MARKB + 2);
            g_tp_tm0 = self_tracerpid();
            ph = PH_MEAS;
        } else {
            g_warm_last_ns = el;
            g_warm_any = 1;
            g_warm++;
        }
    }
    pthread_mutex_unlock(&g_claim_mu);
    return ph;
}

static void *client_main(void *arg) {
    client_t *c = arg;
    while (__atomic_load_n(&g_phase, __ATOMIC_ACQUIRE) == PH_INIT) { struct timespec ts = {0, 100000}; nanosleep(&ts, NULL); }
    for (uint32_t seq = 0;; seq++) {
        int ph = __atomic_load_n(&g_phase, __ATOMIC_ACQUIRE);
        if (ph == PH_WARM) ph = claim_warm(); /* A23: decided at this claim, its time read under g_claim_mu */
        if (ph >= PH_DRAIN) break;
        if (ph == PH_MEAS) {
            uint64_t k = __atomic_fetch_add(&g_meas, 1, __ATOMIC_RELAXED);
            if (MAX_OPS && k >= MAX_OPS) break;
        }
        rec_t r;
        memset(&r, 0, sizeof r);
        r.client = (uint32_t)c->id;
        r.seq = seq;
        r.phase = (uint8_t)ph;
        r.ok = 1;
        r.err = -1;
        char dst[2100];
        snprintf(dst, sizeof dst, "%s/b_%s_%d_%u.db", BDIR, RUN_TAG, c->id, seq);
        if (V1 && C == 1) v1_set_mark(V1, MARKB + seq + 1);
        uint64_t opc = ((uint64_t)c->id << 33) | ((uint64_t)seq << 1), opw = opc | 1;
        char lc[2200], lw[2200];
        snprintf(lc, sizeof lc, "create:%s", dst);
        snprintf(lw, sizeof lw, "write:%s", dst);
        if (g_have_c1b) c1b_opstart(&g_c1b, opc, lc);
        r.start = now_ns();
        sqlite3 *b = NULL;
        if (create_branch(dst, &r) != 0) { r.ok = 0; r.err = 1; }
        else if (g_have_c1b) c1b_ack(&g_c1b, opc, lc);
        r.step_end[0] = now_ns();
        r.nsteps = 1;
        if (r.ok) {
            b = sq_open(dst, 0);
            /* No checkpoint at close: the branch's commits are already durable in its WAL, and a close-time
             * checkpoint (2 F_FULLFSYNC, V1-measured) is work an honest file-copy server would not do per op. */
            if (b) sqlite3_db_config(b, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, (int *)0);
            char *e = NULL;
            const char *osql = BRANCH_SHARED
                ? "PRAGMA synchronous=FULL; PRAGMA fullfsync=1; SELECT count(*) FROM sqlite_schema"
                : "PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=FULL; PRAGMA fullfsync=1; SELECT count(*) FROM sqlite_schema";
            if (MUTANT_BNOSYNC) osql = "PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=OFF; SELECT count(*) FROM sqlite_schema";
            if (!b || sqlite3_exec(b, osql, NULL, NULL, &e) != SQLITE_OK) { r.ok = 0; r.err = 2; }
            sqlite3_free(e);
            r.step_end[1] = now_ns();
            r.nsteps = 2;
        }
        if (r.ok && OP_M1) {
            char sql[128];
            snprintf(sql, sizeof sql, "UPDATE t SET v = v + 1 WHERE id = %ld", 1 + (long)(xs(&c->rng) % (uint64_t)ROWS));
            if (g_have_c1b) c1b_opstart(&g_c1b, opw, lw);
            if (sqlite3_exec(b, sql, NULL, NULL, NULL) != SQLITE_OK || sqlite3_changes(b) != 1) { r.ok = 0; r.err = 3; }
            else if (g_have_c1b) c1b_ack(&g_c1b, opw, lw);
            r.step_end[2] = now_ns();
            r.nsteps = 3;
        }
        r.end = now_ns();
        if (V1 && C == 1) v1_set_mark(V1, V1_MARK_IDLE | (MARKB + seq + 1));
        if (b) sqlite3_close(b);
        if (DROP) {
            char p[2200];
            unlink(dst);
            snprintf(p, sizeof p, "%s-wal", dst); unlink(p);
            snprintf(p, sizeof p, "%s-shm", dst); unlink(p);
#ifndef __APPLE__
            /* Linux port: a DURABLE delete (PREREG §7, "each measured create is followed by a durable delete"; lead
             * review 62430d8bf..b49fb656a HIGH 1): the unlinks reach the device by an fsync of the branch directory,
             * under --sync d2; d0 takes none, like its creates. Untimed: after r.end, recorded in after_ns. */
            if (!SYNC_D0 && fsync(g_dirfd) != 0 && r.ok) { r.ok = 0; r.err = 4; }
#endif
        }
        r.after_end = now_ns();
        if (c->n == c->cap) { c->cap = c->cap ? c->cap * 2 : 4096; c->rec = realloc(c->rec, c->cap * sizeof *c->rec); }
        c->rec[c->n++] = r;
        __atomic_fetch_add(&g_done, 1, __ATOMIC_RELEASE);
    }
    return NULL;
}

static int cmp_rec(const void *a, const void *b) {
    const rec_t *x = a, *y = b;
    return x->start < y->start ? -1 : x->start > y->start;
}

static int cmd_run(int argc, char **argv) {
    const char *out = NULL, *v1run = NULL;
    int warm_rule_flag = 0, warm_legacy = 0; /* MED 7: --warmup and --warmup-ops may not be mixed */
    MODE_B0 = -1;
    OP_M1 = -1;
    for (int i = 0; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!strcmp(a, "--mode") && v) { MODE_B0 = !strcmp(v, "b0") ? 1 : !strcmp(v, "b1") ? 0 : -2; i++; }
        else if (!strcmp(a, "--op") && v) { OP_M1 = !strcmp(v, "m1") ? 1 : !strcmp(v, "m1c") ? 0 : -2; i++; }
        else if (!strcmp(a, "--sync") && v) { SYNC_D0 = !strcmp(v, "d0") ? 1 : !strcmp(v, "d2") ? 0 : -1; i++; }
        else if (!strcmp(a, "--parent") && v) PARENT = argv[++i];
        else if (!strcmp(a, "--dir") && v) BDIR = argv[++i];
        else if (!strcmp(a, "--clients") && v) C = atoi(argv[++i]);
        else if (!strcmp(a, "--out") && v) out = argv[++i];
        else if (!strcmp(a, "--max-ops") && v) MAX_OPS = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(a, "--min-ops") && v) MIN_OPS = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(a, "--duration-s") && v) DUR_S = atof(argv[++i]);
        else if (!strcmp(a, "--warmup-ops") && v) { WARM_OPS = strtoull(argv[++i], NULL, 10); warm_legacy = 1; }
        else if (!strcmp(a, "--max-window-s") && v) MAXWIN_S = atof(argv[++i]);
        else if (!strcmp(a, "--warmup") && v) {  /* PREREG annex A23 */
            unsigned long long wo; double ws, wm; char extra;
            /* S and MAX_S finite (A23-AM1: MAX_S is a bound, 0 ends the warm-up at the first claim) and at most 1e7 s,
             * so (uint64_t)(x * 1e9) is defined; OPS starts with a digit (%llu would wrap "-1") */
            if (v[0] < '0' || v[0] > '9' || sscanf(v, "%llu:%lf:%lf%c", &wo, &ws, &wm, &extra) != 3 || !isfinite(ws) ||
                !isfinite(wm) || !(ws >= 0) || !(wm >= 0) || ws > 1e7 || wm > 1e7) {
                fprintf(stderr, "clonebench run: --warmup OPS:S:MAX_S (OPS an integer, S and MAX_S finite seconds in "
                                "[0, 1e7]) (got %s)\n", v);
                return 2;
            }
            WARM_OPS = wo; WARM_S = ws; WARM_MAX_S = wm;
            warm_rule_flag = 1;
            i++;
        }
        else if (!strcmp(a, "--hold-us") && v) HOLD_US = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(a, "--rows") && v) ROWS = atol(argv[++i]);
        else if (!strcmp(a, "--seed") && v) SEED = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(a, "--v1-run") && v) v1run = argv[++i];
        else if (!strcmp(a, "--v1-mark-base") && v) MARKB = strtoull(argv[++i], NULL, 0);
        else if (!strcmp(a, "--drop")) DROP = 1;
        else if (!strcmp(a, "--branch-shared")) BRANCH_SHARED = 1;
        else if (!strcmp(a, "--mutant-branch-nosync")) MUTANT_BNOSYNC = 1;
        else if (!strcmp(a, "--mutant-early-ack")) MUTANT_EARLY = 1;
        else { fprintf(stderr, "clonebench run: bad argument %s\n", a); return 2; }
    }
    if (MODE_B0 < 0 || OP_M1 < 0 || SYNC_D0 < 0 || !PARENT || !BDIR || !out || C < 1 || (!MAX_OPS && DUR_S <= 0 && !MIN_OPS)) {
        fprintf(stderr, "usage: clonebench run --mode b1|b0 --op m1c|m1 --parent DB --dir D --clients C --out O (--max-ops N | --duration-s S)\n");
        return 2;
    }
    if (warm_rule_flag && warm_legacy) {  /* lead review 62430d8bf..b49fb656a MED 7 */
        fprintf(stderr, "clonebench: REFUSED: --warmup OPS:S:MAX_S together with --warmup-ops (the effective warm-up and "
                        "the recorded rule would differ)\n");
        return 2;
    }
    if (warm_legacy) {
        /* PREREG annex A23-AM1: MAX_S is a bound, never "no limit". --warmup-ops alone takes S 0 and MAX_S = 0.1 x the
         * window bound, as the macOS bbload takes 0.1 x its run cap; without a window bound it has none, so refuse. */
        if (!isfinite(MAXWIN_S) || !(MAXWIN_S > 0) || MAXWIN_S > 1e8) {
            fprintf(stderr, "clonebench: REFUSED: --warmup-ops needs a window bound (--max-window-s > 0), whose tenth is "
                            "its MAX_S (A23-AM1)\n");
            return 2;
        }
        WARM_MAX_S = 0.1 * MAXWIN_S;
    }
    WARM_S_NS = (uint64_t)(WARM_S * 1e9);
    WARM_MAX_NS = (uint64_t)(WARM_MAX_S * 1e9);
    if (OP_M1 && ROWS < 1) { fprintf(stderr, "clonebench: --op m1 needs --rows (the parent's row count)\n"); return 2; }
    if (MUTANT_EARLY && !MODE_B0) { fprintf(stderr, "clonebench: --mutant-early-ack is a b0 mutant\n"); return 2; }
#ifndef __APPLE__
    if (MODE_B0) {
        fprintf(stderr, "clonebench: REFUSED: --mode b0 is not ported to Linux (its flights assume F_FULLFSYNC apart from fsync)\n");
        return 2;
    }
#endif
#if !BB_HOOKS
    if (v1run) { fprintf(stderr, "clonebench: REFUSED: --v1-run, but this build has no V1 hooks (BB_HOOKS=0)\n"); return 2; }
    if (getenv("C1B_RUN") && *getenv("C1B_RUN")) {
        fprintf(stderr, "clonebench: REFUSED: C1B_RUN is set, but this build has no C1b hooks (BB_HOOKS=0)\n");
        return 2;
    }
#endif
    if (guard_dir(BDIR) || guard_file_dir(PARENT)) return 2;
    if (!(g_parent = sq_open(PARENT, 0))) return 2;
    sq_exec(g_parent, "PRAGMA synchronous=FULL; PRAGMA fullfsync=1; PRAGMA checkpoint_fullfsync=1; SELECT count(*) FROM t");
    snprintf(g_parent_wal, sizeof g_parent_wal, "%s-wal", PARENT);
    if ((g_dirfd = open(BDIR, O_RDONLY)) < 0) die("open branch dir");
    if (mkdir(out, 0755) != 0) { fprintf(stderr, "clonebench: REFUSED: out dir %s must not exist: %s\n", out, strerror(errno)); return 2; }
#if BB_HOOKS
    if (v1run) {
        const char *why = "?";
        if (!(V1 = v1_map(v1run, &why))) { fprintf(stderr, "clonebench: V1 run %s: %s\n", v1run, why); return 2; }
        /* Embedded: THIS process makes the syncs, so it must be the one counted. Marks set by an unattached
         * process attribute nothing, and the run's report cannot tell (its root may be another process). */
        int self = 0;
        uint64_t used = V1->slots_used < V1->nslots ? V1->slots_used : V1->nslots - 1;
        for (uint64_t i = 1; i <= used && !self; i++) self = v1_slots(V1)[i].pid == getpid();
        if (!self) { fprintf(stderr, "clonebench: REFUSED: --v1-run %s but this process is not attached (launch it with v1run)\n", v1run); return 2; }
        if (!MARKB) MARKB = ((uint64_t)time(NULL) & 0x7fffff) << 32;
    }
#endif
    snprintf(RUN_TAG, sizeof RUN_TAG, "r%llx", (unsigned long long)(now_ns() & 0xffffffffff));
#if BB_HOOKS
    const char *c1run = getenv("C1B_RUN");
    if (c1run && *c1run) {
        const char *why = "?";
        if (c1b_client_open(&g_c1b, c1run, &why) != 0) { fprintf(stderr, "clonebench: C1B client: %s\n", why); return 2; }
        g_have_c1b = 1;
    }
#endif
    pthread_t fth;
    if (MODE_B0) pthread_create(&fth, NULL, flusher, NULL);
    client_t *cl = calloc((size_t)C, sizeof *cl);
    for (int i = 0; i < C; i++) {
        cl[i].id = i;
        cl[i].rng = (SEED * 0x9E3779B97F4A7C15ULL) ^ ((uint64_t)(i + 1) * 0xD1B54A32D192ED03ULL);
        if (!cl[i].rng) cl[i].rng = 1;
        pthread_create(&cl[i].th, NULL, client_main, &cl[i]);
    }
    uint64_t t_start = now_ns(), tm1 = 0;
    struct rusage ru1;
    uint64_t fl1 = 0;
    int tp_tm1 = -2; /* MED 4: own TracerPid at the window's end (-2: never reached; g_tp_tm0 at its start) */
    g_t0 = t_start;
    /* No warm-up asked for (OPS, S and MAX_S all 0): start in the measured window, so --max-ops N makes exactly N ops
     * (the B1 prebranch; lead review 62430d8bf..b49fb656a, MED 3 / HIGH 1), as bbload does; A23 gives 0:0:0 the same
     * 0 warm-up ops. Otherwise every claim decides the warm-up (claim_warm, PREREG annex A23). */
    if (WARM_OPS == 0 && WARM_S == 0 && WARM_MAX_S == 0) {
        getrusage(RUSAGE_SELF, &g_ru0);
        g_tm0 = t_start;
        g_tp_tm0 = self_tracerpid();
        __atomic_store_n(&g_phase, PH_MEAS, __ATOMIC_RELEASE);
    } else __atomic_store_n(&g_phase, PH_WARM, __ATOMIC_RELEASE);
    if (V1 && C > 1) v1_set_mark(V1, MARKB + (g_phase == PH_MEAS ? 2 : 1));
    for (;;) {
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
        /* The phase first, then the clock: the claim that ended the warm-up stored g_tm0 before PH_MEAS (release), so
         * a clock read after this acquire is not before it (the guard keeps a cross-CPU read from going negative). */
        int ph = __atomic_load_n(&g_phase, __ATOMIC_ACQUIRE);
        uint64_t n = now_ns();
        if (ph == PH_MEAS) {
            uint64_t tm0 = __atomic_load_n(&g_tm0, __ATOMIC_ACQUIRE);
            double el = n > tm0 ? (n - tm0) / 1e9 : 0;
            int timed = DUR_S > 0 || MIN_OPS > 0;
            int cap = MAXWIN_S > 0 && el > MAXWIN_S && !((timed && el >= DUR_S && g_meas >= MIN_OPS) || (MAX_OPS && g_meas >= MAX_OPS));
            if (cap) CAPPED = 1;
            if (cap || (timed && el >= DUR_S && g_meas >= MIN_OPS) || (MAX_OPS && g_meas >= MAX_OPS)) {
                getrusage(RUSAGE_SELF, &ru1);
                pthread_mutex_lock(&f_mu); fl1 = f_nflights; pthread_mutex_unlock(&f_mu);
                tm1 = n;
                __atomic_store_n(&g_phase, PH_DRAIN, __ATOMIC_RELEASE);
                if (V1 && C > 1) v1_set_mark(V1, MARKB + 3);
                tp_tm1 = self_tracerpid();
                break;
            }
        }
    }
    for (int i = 0; i < C; i++) pthread_join(cl[i].th, NULL);
    if (MODE_B0) {
        pthread_mutex_lock(&f_mu);
        f_stop = 1;
        pthread_cond_signal(&f_work);
        pthread_mutex_unlock(&f_mu);
        pthread_join(fth, NULL);
    }
    if (V1) v1_set_mark(V1, 0);

    size_t total = 0;
    for (int i = 0; i < C; i++) total += cl[i].n;
    rec_t *all = malloc((total ? total : 1) * sizeof *all);
    size_t k = 0;
    for (int i = 0; i < C; i++) { memcpy(all + k, cl[i].rec, cl[i].n * sizeof *all); k += cl[i].n; }
    qsort(all, total, sizeof *all, cmp_rec);
    char p[2200];
    snprintf(p, sizeof p, "%s/raw.tsv", out);
    FILE *f = fopen(p, "w");
    if (!f) die("raw.tsv");
    fprintf(f, "client\tseq\tphase\tok\tstart_ns\tclone_done_ns\tend_ns\tlat_ns\tcreate_ns\topen_ns\twrite_ns\tafter_ns\tticket\tflight\terr\n");
    for (size_t i = 0; i < total; i++) {
        rec_t *o = &all[i];
        uint64_t s1 = o->nsteps >= 1 ? o->step_end[0] - o->start : 0, s2 = o->nsteps >= 2 ? o->step_end[1] - o->step_end[0] : 0,
                 s3 = o->nsteps >= 3 ? o->step_end[2] - o->step_end[1] : 0;
        fprintf(f, "%u\t%u\t%s\t%u\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%d\n", o->client, o->seq,
                o->phase == PH_MEAS ? "measure" : o->phase == PH_WARM ? "warmup" : "drain", o->ok, (unsigned long long)o->start,
                (unsigned long long)o->clone_done, (unsigned long long)o->end, (unsigned long long)(o->end - o->start),
                (unsigned long long)s1, (unsigned long long)s2, (unsigned long long)s3, (unsigned long long)(o->after_end - o->end),
                (unsigned long long)o->ticket, (unsigned long long)o->flight, o->err);
    }
    if (fclose(f) != 0) die("raw.tsv close");
    if (MODE_B0) {
        snprintf(p, sizeof p, "%s/flights.tsv", out);
        f = fopen(p, "w");
        fprintf(f, "flight\tt0_ns\tt1_ns\triders\tlast_ticket\n");
        for (uint64_t i = 0; i < f_nflights; i++)
            fprintf(f, "%llu\t%llu\t%llu\t%llu\t%llu\n", (unsigned long long)(i + 1), (unsigned long long)f_log[i].t0,
                    (unsigned long long)f_log[i].t1, (unsigned long long)f_log[i].riders, (unsigned long long)f_log[i].last_ticket);
        fclose(f);
    }
    struct hdr_histogram *h;
    hdr_init(1, INT64_C(3600000000000), 3, &h);
    uint64_t meas = 0, ok = 0, bad = 0;
    for (size_t i = 0; i < total; i++) {
        if (all[i].phase != PH_MEAS) { if (!all[i].ok) bad++; continue; }
        meas++;
        if (!all[i].ok) { bad++; continue; }
        ok++;
        hdr_record_value(h, (int64_t)(all[i].end - all[i].start));
    }
    snprintf(p, sizeof p, "%s/hdr_total.txt", out);
    f = fopen(p, "w");
    hdr_percentiles_print(h, f, 5, 1000.0, CLASSIC);
    fclose(f);
    uint64_t tm0 = g_tm0, fl0 = g_fl0; /* the window's start as the ending claim (or t0, with no warm-up) set it */
    double win = (tm1 - tm0) / 1e9;
    double cpu = (ru1.ru_utime.tv_sec - g_ru0.ru_utime.tv_sec) + (ru1.ru_utime.tv_usec - g_ru0.ru_utime.tv_usec) / 1e6 +
                 (ru1.ru_stime.tv_sec - g_ru0.ru_stime.tv_sec) + (ru1.ru_stime.tv_usec - g_ru0.ru_stime.tv_usec) / 1e6;
    int rc = (ok == 0 || bad) ? 3 : 0;
    /* gate-6 review, t3run item 16; lead review 62430d8bf..b49fb656a MED 5: a run the registered cap (--max-window-s)
     * ended is reported, not judged: rc 0, verdict "capped", capped: true and its counts; timedrun.py alone applies
     * PREREG's tiers (>= 1000 ok complete, 100-999 p50 only, fewer failed with cause 'cap'). */
    snprintf(p, sizeof p, "%s/summary.json", out);
    f = fopen(p, "w");
    fprintf(f, "{\"clock\":\"%s\",\"b1_barrier\":\"%s\",", BB_CLOCK_NAME, SYNC_D0 ? "none" : B1_BARRIER); /* Linux port */
    fprintf(f, "\"capped\":%s,\"max_window_s\":%.3f,", CAPPED ? "true" : "false", MAXWIN_S);
    {   /* MED 4: the measured window on CLOCK_REALTIME (the tracer sweeps' clock), through one offset read now, and the
         * process's own TracerPid at its start and end */
        struct timespec rt;
        clock_gettime(CLOCK_REALTIME, &rt);
        double off = (double)rt.tv_sec + rt.tv_nsec / 1e9 - now_ns() / 1e9;
        fprintf(f, "\"tm0_realtime_s\":%.6f,\"tm1_realtime_s\":%.6f,\"tracerpid_tm0\":%d,\"tracerpid_tm1\":%d,",
                tm0 / 1e9 + off, tm1 / 1e9 + off, g_tp_tm0, tp_tm1);
    }
    /* MED 7: the recorded rule is formatted from the EFFECTIVE values, never copied from the command line */
    snprintf(WARM_RULE, sizeof WARM_RULE, "%llu:%g:%g", (unsigned long long)WARM_OPS, WARM_S, WARM_MAX_S);
    fprintf(f, "\"warmup_rule\":\"%s\",\"warmup_ops\":%llu,\"warmup_s\":%.6f,", WARM_RULE, (unsigned long long)g_warm,
            tm0 > t_start ? (tm0 - t_start) / 1e9 : 0.0);
    /* PREREG annex A23: the warm-up as claim_warm decided it -- the ending claim (ns since t0; 0 with no warm-up),
     * whether it ended capped, and the last warm-up claim (null when none was claimed) -- for timedrun.py's exact check */
    fprintf(f, "\"warmup_capped\":%s,\"warmup_end_ns\":", g_warm_capped ? "true" : "false");
    if (tm0) fprintf(f, "%llu,", (unsigned long long)g_warm_end_ns); else fprintf(f, "null,");
    fprintf(f, "\"warmup_last_claim_ns\":");
    if (g_warm_any) fprintf(f, "%llu,", (unsigned long long)g_warm_last_ns); else fprintf(f, "null,");
    fprintf(f, "\"warmup_backsteps\":%llu,", (unsigned long long)g_warm_backsteps);
    fprintf(f, "\"verdict\":\"%s\",\"rc\":%d,\"mode\":\"%s\",\"op\":\"%s\",\"sync\":\"%s\",\"clients\":%d,\"hold_us\":%llu,"
               "\"mutant_early_ack\":%d,\"drop\":%d,\"branch_locking\":\"%s\",\"parent\":\"%s\",\"window_s\":%.6f,\"measured_ops\":%llu,\"measured_ok\":%llu,"
               "\"failed_ops\":%llu,\"total_ops\":%zu,\"tput_per_s\":%.3f,\"parent_checkpoints\":%llu,\"flights_total\":%llu,"
               "\"flights_in_window\":%llu,\"creates_per_flight_in_window\":%.3f,\"cpu_s\":%.3f,\"cpu_cores\":%.3f,"
               "\"v1_run\":\"%s\",\"v1_mark_base\":%llu,\"sqlite_version\":\"%s\",\"lat_us\":{\"p50\":%.1f,\"p99\":%.1f,\"max\":%.1f}}\n",
            rc ? (ok == 0 ? "REFUSED: no measured operation succeeded" : "REFUSED: some operations failed") : CAPPED ? "capped" : "ok", rc,
            MODE_B0 ? "b0" : "b1", OP_M1 ? "m1" : "m1c", SYNC_D0 ? "d0" : "d2", C, (unsigned long long)HOLD_US, MUTANT_EARLY,
            DROP, BRANCH_SHARED ? "shared" : "exclusive", PARENT, win, (unsigned long long)meas, (unsigned long long)ok, (unsigned long long)bad, total,
            win > 0 ? ok / win : 0, (unsigned long long)g_checkpoints, (unsigned long long)f_nflights,
            (unsigned long long)(fl1 - fl0), (fl1 > fl0) ? (double)ok / (double)(fl1 - fl0) : 0.0, cpu, win > 0 ? cpu / win : 0,
            v1run ? v1run : "", (unsigned long long)MARKB, sqlite3_libversion(), hdr_value_at_percentile(h, 50) / 1e3, hdr_value_at_percentile(h, 99) / 1e3,
            hdr_max(h) / 1e3);
    fclose(f);
    printf("clonebench %s %s %s C=%d: measured %llu ok %llu failed %llu window %.2fs flights(window) %llu -> %s (rc %d)\n",
           MODE_B0 ? "b0" : "b1", OP_M1 ? "m1" : "m1c", SYNC_D0 ? "d0" : "d2", C, (unsigned long long)meas,
           (unsigned long long)ok, (unsigned long long)bad, win, (unsigned long long)(fl1 - fl0), out, rc);
    return rc;
}

/* ---------- par: D0 clonefile throughput across processes ---------- */
static int cmd_par(int argc, char **argv) {
    const char *src = NULL, *dir = NULL, *out = NULL;
    int procs = 0;
    long n = 0;
    for (int i = 0; i < argc; i++) {
        if (!strcmp(argv[i], "--src") && i + 1 < argc) src = argv[++i];
        else if (!strcmp(argv[i], "--dir") && i + 1 < argc) dir = argv[++i];
        else if (!strcmp(argv[i], "--out") && i + 1 < argc) out = argv[++i];
        else if (!strcmp(argv[i], "--procs") && i + 1 < argc) procs = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--n") && i + 1 < argc) n = atol(argv[++i]);
        else { fprintf(stderr, "clonebench par: bad argument %s\n", argv[i]); return 2; }
    }
    if (!src || !dir || !out || procs < 1 || n < procs) { fprintf(stderr, "usage: clonebench par --src F --dir D --procs P --n N --out O\n"); return 2; }
    if (guard_dir(dir) || guard_file_dir(src)) return 2;
    if (mkdir(out, 0755) != 0) { fprintf(stderr, "clonebench: REFUSED: out dir %s must not exist\n", out); return 2; }
    long per = n / procs;
    uint64_t *times = mmap(NULL, (size_t)procs * 2 * sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    volatile uint64_t *go = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    pid_t *kids = calloc((size_t)procs, sizeof *kids);
    for (int w = 0; w < procs; w++) {
        pid_t pid = fork();
        if (pid < 0) die("fork");
        if (pid == 0) {
            char wd[2100], dst[2200];
            snprintf(wd, sizeof wd, "%s/w%d", dir, w);
            if (mkdir(wd, 0755) != 0 && errno != EEXIST) _exit(3);
            while (!*go) { }
            times[2 * w] = now_ns();
            for (long i = 0; i < per; i++) {
                snprintf(dst, sizeof dst, "%s/c%ld", wd, i);
                if (clonefile(src, dst, 0) != 0) _exit(4);
            }
            times[2 * w + 1] = now_ns();
            _exit(0);
        }
        kids[w] = pid;
    }
    struct timespec ts = {0, 200000000};
    nanosleep(&ts, NULL);
    *go = 1;
    int bad = 0;
    for (int w = 0; w < procs; w++) {
        int st;
        waitpid(kids[w], &st, 0);
        if (!WIFEXITED(st) || WEXITSTATUS(st)) bad++;
    }
    uint64_t t0 = UINT64_MAX, t1 = 0;
    for (int w = 0; w < procs; w++) { if (times[2 * w] < t0) t0 = times[2 * w]; if (times[2 * w + 1] > t1) t1 = times[2 * w + 1]; }
    char p[2200];
    for (int w = 0; w < procs; w++)
        for (long i = 0; i < per; i++) { snprintf(p, sizeof p, "%s/w%d/c%ld", dir, w, i); unlink(p); }
    for (int w = 0; w < procs; w++) { snprintf(p, sizeof p, "%s/w%d", dir, w); rmdir(p); }
    off_t sz;
    long ext = extents(src, &sz);
    snprintf(p, sizeof p, "%s/summary.json", out);
    FILE *f = fopen(p, "w");
    double wall = (t1 - t0) / 1e9;
    fprintf(f, "{\"src\":\"%s\",\"src_bytes\":%lld,\"src_extents\":%ld,\"procs\":%d,\"n\":%ld,\"failed_procs\":%d,\"wall_s\":%.6f,"
               "\"clones_per_s\":%.1f}\n", src, (long long)sz, ext, procs, per * procs, bad, wall, wall > 0 ? per * procs / wall : 0);
    fclose(f);
    printf("clonebench par P=%d n=%ld wall %.3fs -> %.0f clones/s (src extents %ld)%s\n", procs, per * procs, wall,
           wall > 0 ? per * procs / wall : 0, ext, bad ? " FAILED" : "");
    return bad ? 3 : 0;
}

int main(int argc, char **argv) {
    if (argc == 3 && !strcmp(argv[1], "--warmup-replay")) return warmup_replay(argv[2]); /* PREREG annex A23 */
    if (argc < 2) { fprintf(stderr, "usage: clonebench extents|run|par|--warmup-replay ...\n"); return 2; }
    if (!strcmp(argv[1], "run")) return cmd_run(argc - 2, argv + 2);
    if (!strcmp(argv[1], "par")) return cmd_par(argc - 2, argv + 2);
    if (!strcmp(argv[1], "extents") && argc == 3) {
        off_t sz;
        long e = extents(argv[2], &sz);
#ifdef __APPLE__
        printf("{\"file\":\"%s\",\"bytes\":%lld,\"extents\":%ld}\n", argv[2], (long long)sz, e);
#else
        printf("{\"file\":\"%s\",\"bytes\":%lld,\"extents\":%ld,\"shared_extents\":%ld}\n", argv[2], (long long)sz, e, g_ext_shared);
#endif
        return e < 0 ? 2 : 0;
    }
    fprintf(stderr, "clonebench: unknown command %s\n", argv[1]);
    return 2;
}
