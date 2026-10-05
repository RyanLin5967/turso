/* v3floor.c -- V3 device-floor probe, Linux port of frontier/fastest/tools/v3/floor.c (PREREG §4 V3, §11 M0 exit 1).
 *
 *   v3floor --dir D --out O --n N [--arms a,b,...] [--seed S] [--mutant-nosync] [--trace-clock]
 *
 * Arms (default: the M0 set plus the D0 control, append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,nosync25):
 *   append25     pwrite 25 B at EOF (the file grows) + fsync                        (the V3 floor; one fork frame)
 *   ow4k         overwrite 4 KiB in a preallocated, durable region + fsync
 *   ow64k        overwrite 64 KiB, same
 *   ow1m         overwrite 1 MiB, same
 *   clone1b      create a file, ioctl(FICLONE) it from a 1 MiB durable source, close it, fsync the directory
 *                only                                                                   (one barrier)
 *   clone2b      the same, plus an fsync of the clone before its close                   (two barriers)
 *   clean        fsync of a file with nothing dirty (the dirty/clean control; never mutated)
 *   nosync25     the append25 write with no flush (D0): the flush control's reference
 *   Report-only extra: fdatasync4k (ow4k with fdatasync in place of fsync).
 * The barrier is fsync(2): it is what the engine's FullFsync class issues on Linux (core/branch/journal.rs
 * fsync_file). Apple's F_FULLFSYNC and F_BARRIERFSYNC have no Linux counterpart, so the Mac's fsync4k and
 * barrier4k extras are not ported (fsync4k IS ow4k here).
 *
 * Syscalls of op i, by definition (fastest/linux/v3/firecheck.sh checks counts with strace -f -c, and the exact
 * sequence, files, sizes and offsets inside each timed window with strace -f -y under --trace-clock):
 *   append25, nosync25   pwrite64(<arm>, 25, 25 + 25 i); append25 then fsync(<arm>)
 *   ow4k, ow64k, ow1m    pwrite64(<arm>, rec, (i mod cap/rec) x rec); fsync(<arm>)    cap 16 MiB (ow1m: 128 MiB)
 *   fdatasync4k          pwrite64 as ow4k; fdatasync(<arm>)
 *   clean                fsync(<arm>)
 *   clone1b              openat(<arm>.clones/c<i>, O_WRONLY|O_CREAT|O_EXCL), ioctl(c<i>, FICLONE, <arm>.src), close,
 *                        fsync(<arm>.clones)
 *   clone2b              the same with fsync(c<i>) before the close
 *   Nothing runs between ops. The clone arms' teardown unlinks one clone per op, outside the timed window.
 * Setup per arm, before the loop: append25/nosync25 write 25 B + fsync; ow*, fdatasync4k and clean preallocate +
 *   fsync; clone arms preallocate the source + fsync, mkdir the clones' directory + fsync it. Teardown ends with one
 *   fsync of D, so the unlinks are durable before the next batch. Setup and teardown syncs are never mutated.
 *
 * Arms are interleaved round-robin, each round in a fresh seeded shuffle, so every arm shares the moment.
 * Latency = CLOCK_MONOTONIC_RAW around the op (write + barrier(s)), in ns, read through the vDSO.
 * --trace-clock reads the clock with a real clock_gettime syscall instead, so strace sees each timed window's
 *   edges; it is for the fire-check only, and summary.json records it ("trace_clock":1).
 *
 * Output (raw first, then the summary computed from it): O/raw.tsv (arm, i, ns), O/summary.json.
 * Exit: 0 ok | 2 usage or refused setup | 1 an operation failed | 3 VOID: the flush control failed.
 * Flush control (D0, per arm, tools review 1 item 6): for EVERY selected M0 flushed arm (append25, ow4k, ow64k, ow1m,
 *   clone1b, clone2b), p50(arm) / p50(nosync25) must be > 10, else the run is void (rc 3). The fdatasync4k and
 *   clean ratios are reported, never gated. flush_d0_p50_ratio keeps the Mac's headline (append25 / nosync25).
 *   What the control can and cannot see: run 37243798049's no-flush mutant read <= 5.9 for append25, ow4k and
 *   fdatasync4k on every cell, but 2.8-14.7 for ow64k, 28-237 for ow1m and 10-24 for the clones, so for those arms a
 *   missing flush can pass it. It shows that a flush cost something, never that it reached the device: it passed at
 *   46-106x on hosted disks that report write-through, where the block layer sends the device no flush at all. The
 *   per-arm flush identity is firecheck.sh's strace check, which must pass on the same binary (run.sh enforces it).
 * Reported, NOT a gate: the drafted M0 exit-1 ratio p50(append25) / p50(clean) > 10, and clean_fast_frac, the share
 *   of clean samples under 100 us (the Mac record found that ratio does not discriminate: tools/v3/FIRECHECK.md).
 *
 * Refusals (rc 2, nothing left behind):
 *   - D's filesystem is not ext4, xfs or btrfs (the mount table's fstype for D's longest mount prefix and statfs's
 *     magic must agree, else "cannot determine" also refuses);
 *   - the flush path has a barrier switched off: D's mount, or, through each loop device, the mount holding its
 *     backing file (up to 4 layers), carries nobarrier or barrier=0, so an fsync's cache flush never reaches the
 *     device (the GitHub runners' root ext4 is mounted nobarrier); a layer whose device cannot be found in /sys, or
 *     whose options cannot be read whole, refuses too. A leaf device that reports write-through is recorded, not
 *     refused: it claims no volatile cache, so the block layer correctly sends it no flush;
 *   - nice != 0, a scheduling policy other than SCHED_OTHER, or an I/O priority other than the default (class
 *     none, or best-effort level 4, the nice-0 default);
 *   - D's path is too long, the out dir exists, n is not a positive integer, an unknown or repeated arm;
 *   - a flushed arm (append25, ow*, clone*, fdatasync4k) selected without nosync25, whose control would otherwise
 *     silently not run (tools review 1 item 6), or every selected flushed arm refused (see below);
 *   - a clone arm's <arm>.clones directory left in D by an earlier run.
 * The clone arms run on XFS and btrfs only. On ext4 every selected clone arm is REFUSED, never skipped silently: it
 *   does not run, and the reason (the filesystem and the errno a trial FICLONE returned there) is written to
 *   summary.json "refused_arms" and to stderr; the remaining arms run. On xfs or btrfs a failed FICLONE is an error
 *   (rc 1), and an ext4 that accepts the trial FICLONE is an error too (the refusal rule would be wrong).
 * --mutant-nosync skips every flush in the flushed arms: a fire-check that the control above fails it.
 *
 * Blind spots, stated: foreign I/O on the device and cgroup I/O throttling are not observed here (run.sh's stamps
 *   record /proc/diskstats and PSI around the batch); device-mapper, md and network block layers below D are not
 *   followed (only loop devices are), so a barrier switched off beneath one of them is not seen. On Linux an fsync
 *   is per inode plus the filesystem's journal, not a device-wide cache flush as F_FULLFSYNC is on Apple, so what
 *   clone1b's one barrier makes durable is a question for a crash test, not for this probe (unverified).
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <linux/fs.h>
#include <sched.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

#ifndef FICLONE
#define FICLONE _IOW(0x94, 9, int)
#endif
#define MAGIC_EXT4 0xEF53u
#define MAGIC_XFS 0x58465342u
#define MAGIC_BTRFS 0x9123683Eu
#define IOPRIO_WHO_PROC 1
#define IOPRIO_SHIFT 13
#define MAXLAYERS 4
#define OPTS 4096

enum { APPEND25, OW4K, OW64K, OW1M, CLONE1B, CLONE2B, CLEAN, FDATASYNC4K, NOSYNC25, NARMS };
static const char *NAMES[NARMS] = {"append25", "ow4k", "ow64k", "ow1m", "clone1b", "clone2b", "clean",
                                   "fdatasync4k", "nosync25"};
#define MIB (1u << 20)
static int gated(int a) { return a <= CLONE2B; }                     /* M0 flushed arms: the control gates them */
static int flushed(int a) { return a <= CLONE2B || a == FDATASYNC4K; } /* a flush the mutant removes */
static int is_clone(int a) { return a == CLONE1B || a == CLONE2B; }

static const char *DIR_;
static int MUTANT, TRACE_CLOCK;
static char buf[MIB];

typedef struct {
    int fd;
    off_t off, cap;
    size_t rec;
    int dfd;   /* clone arms: the clones' directory */
    int srcfd; /* clone arms: the durable source */
    char cdir[PATH_MAX];
    char src[PATH_MAX];
} armst;

static void die(const char *what) { fprintf(stderr, "v3floor: %s: %s\n", what, strerror(errno)); exit(1); }
static void barrier(int fd) { if (!MUTANT && fsync(fd) == -1) die("fsync"); }
static void setup_sync(int fd, const char *what) { if (fsync(fd) == -1) die(what); } /* never mutated */
static uint64_t now(void) {
    struct timespec ts;
    int r = TRACE_CLOCK ? (int)syscall(SYS_clock_gettime, CLOCK_MONOTONIC_RAW, &ts) : clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    if (r != 0) die("clock_gettime CLOCK_MONOTONIC_RAW");
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* every path is built here: a path that does not fit is an error, never a silently truncated name */
static void pathf(char *dst, size_t cap, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
static void pathf(char *dst, size_t cap, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = vsnprintf(dst, cap, fmt, ap);
    va_end(ap);
    if (r < 0 || (size_t)r >= cap) { fprintf(stderr, "v3floor: path too long (%s...)\n", dst); exit(1); }
}

static void jstr(FILE *f, const char *s) { /* a JSON string */
    fputc('"', f);
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if (*p == '"' || *p == '\\') fprintf(f, "\\%c", *p);
        else if (*p < 0x20) fprintf(f, "\\u%04x", *p);
        else fputc(*p, f);
    }
    fputc('"', f);
}

static int parse_u64(const char *s, uint64_t *out) { /* a whole decimal number, nothing else */
    if (!s || !*s || *s < '0' || *s > '9') return -1;
    errno = 0;
    char *end;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno || *end) return -1;
    *out = v;
    return 0;
}

static int mkfile(const char *name) {
    char p[PATH_MAX];
    pathf(p, sizeof p, "%s/%s", DIR_, name);
    int fd = open(p, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) die(p);
    return fd;
}
static void prealloc(int fd, off_t bytes) {
    for (off_t o = 0; o < bytes; o += MIB) if (pwrite(fd, buf, MIB, o) != (ssize_t)MIB) die("prealloc");
    setup_sync(fd, "prealloc fsync");
}

static void setup(int a, armst *s) {
    memset(s, 0, sizeof *s);
    s->fd = s->dfd = s->srcfd = -1;
    switch (a) {
    case APPEND25: case NOSYNC25:
        s->fd = mkfile(NAMES[a]);
        s->rec = 25;
        if (pwrite(s->fd, buf, 25, 0) != 25) die("append init");
        setup_sync(s->fd, "append init fsync");
        s->off = 25;
        break;
    case OW4K: case OW64K: case OW1M: case FDATASYNC4K:
        s->fd = mkfile(NAMES[a]);
        s->rec = a == OW64K ? 65536 : a == OW1M ? MIB : 4096;
        s->cap = a == OW1M ? 128 * (off_t)MIB : 16 * (off_t)MIB;
        prealloc(s->fd, s->cap);
        break;
    case CLONE1B: case CLONE2B:
        pathf(s->src, sizeof s->src, "%s/%s.src", DIR_, NAMES[a]);
        s->srcfd = open(s->src, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (s->srcfd < 0) die("clone src");
        prealloc(s->srcfd, MIB);
        pathf(s->cdir, sizeof s->cdir, "%s/%s.clones", DIR_, NAMES[a]);
        if (mkdir(s->cdir, 0755) != 0) die("clone dir (must not exist)");
        s->dfd = open(s->cdir, O_RDONLY | O_DIRECTORY);
        if (s->dfd < 0) die("clone dir open");
        setup_sync(s->dfd, "clone dir fsync");
        break;
    case CLEAN:
        s->fd = mkfile(NAMES[a]);
        prealloc(s->fd, MIB);
        break;
    }
}

static void op(int a, armst *s, uint64_t i) {
    buf[i % 4096] ^= 1; /* every write differs */
    switch (a) {
    case APPEND25: case NOSYNC25:
        if (pwrite(s->fd, buf, 25, s->off) != 25) die("append");
        s->off += 25;
        if (a == APPEND25) barrier(s->fd);
        break;
    case OW4K: case OW64K: case OW1M: case FDATASYNC4K:
        if (s->off + (off_t)s->rec > s->cap) s->off = 0;
        if (pwrite(s->fd, buf, s->rec, s->off) != (ssize_t)s->rec) die("overwrite");
        s->off += s->rec;
        if (a == FDATASYNC4K) { if (!MUTANT && fdatasync(s->fd) == -1) die("fdatasync"); }
        else barrier(s->fd);
        break;
    case CLONE1B: case CLONE2B: {
        char dst[PATH_MAX];
        pathf(dst, sizeof dst, "%s/c%llu", s->cdir, (unsigned long long)i);
        int fd = open(dst, O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (fd < 0) die("clone create");
        if (ioctl(fd, FICLONE, s->srcfd) != 0) die("ioctl FICLONE");
        if (a == CLONE2B) barrier(fd);
        if (close(fd) != 0) die("clone close");
        barrier(s->dfd);
        break;
    }
    case CLEAN:
        if (fsync(s->fd) == -1) die("clean fsync"); /* the control is never mutated */
        break;
    }
}

static void teardown(int a, armst *s, uint64_t n) {
    char p[PATH_MAX];
    if (s->fd >= 0) {
        if (close(s->fd) != 0) die("teardown close");
        pathf(p, sizeof p, "%s/%s", DIR_, NAMES[a]);
        if (unlinkat(AT_FDCWD, p, 0) != 0) die("teardown unlink");
    }
    if (s->dfd >= 0) {
        for (uint64_t i = 0; i < n; i++) {
            pathf(p, sizeof p, "c%llu", (unsigned long long)i);
            if (unlinkat(s->dfd, p, 0) != 0) die("teardown unlink clone");
        }
        if (close(s->dfd) != 0) die("teardown close dir");
        if (unlinkat(AT_FDCWD, s->cdir, AT_REMOVEDIR) != 0) die("teardown rmdir clones");
    }
    if (s->srcfd >= 0) {
        if (close(s->srcfd) != 0) die("teardown close src");
        if (unlinkat(AT_FDCWD, s->src, 0) != 0) die("teardown unlink src");
    }
}

/* ---- mounts: the longest mount-table prefix of a path ---- */
typedef struct {
    char real[PATH_MAX], mnt[PATH_MAX], fstype[64], source[PATH_MAX], mopts[OPTS], sopts[OPTS], dev[32];
    int truncated;
} mrec;

static void unescape(char *s) { /* mountinfo escapes space, tab, newline and backslash as \ooo */
    char *w = s;
    for (char *r = s; *r; r++) {
        if (r[0] == '\\' && r[1] >= '0' && r[1] <= '7' && r[2] >= '0' && r[2] <= '7' && r[3] >= '0' && r[3] <= '7') {
            *w++ = (char)((r[1] - '0') * 64 + (r[2] - '0') * 8 + (r[3] - '0'));
            r += 3;
        } else *w++ = *r;
    }
    *w = 0;
}

static int copy(char *dst, size_t cap, const char *src) { /* 1 when it did not fit */
    size_t l = strlen(src);
    if (l >= cap) { memcpy(dst, src, cap - 1); dst[cap - 1] = 0; return 1; }
    memcpy(dst, src, l + 1);
    return 0;
}

static const char *mount_of(const char *path, mrec *m) { /* NULL when found, else why not */
    memset(m, 0, sizeof *m);
    if (!realpath(path, m->real)) return "realpath failed";
    FILE *f = fopen("/proc/self/mountinfo", "r");
    if (!f) return "cannot read /proc/self/mountinfo";
    char *line = NULL;
    size_t cap = 0, best = 0;
    int found = 0;
    while (getline(&line, &cap, f) > 0) {
        line[strcspn(line, "\n")] = 0;
        char *fl[64];
        int nf = 0;
        for (char *t = strtok(line, " "); t && nf < 64; t = strtok(NULL, " ")) fl[nf++] = t;
        int dash = -1;
        for (int k = 6; k < nf; k++) if (!strcmp(fl[k], "-")) { dash = k; break; }
        if (dash < 0 || dash + 2 >= nf) continue; /* need the fstype and the source after "-" */
        char mp[PATH_MAX];
        if (copy(mp, sizeof mp, fl[4])) continue;
        unescape(mp);
        size_t l = strlen(mp);
        int under = !strcmp(mp, "/") || (!strncmp(m->real, mp, l) && (m->real[l] == 0 || m->real[l] == '/'));
        if (!under) continue;
        size_t key = !strcmp(mp, "/") ? 1 : l + 1;
        if (key < best) continue; /* equal length: the later mount shadows the earlier one */
        best = key;
        found = 1;
        m->truncated = 0;
        m->truncated |= copy(m->mnt, sizeof m->mnt, mp);
        m->truncated |= copy(m->dev, sizeof m->dev, fl[2]);
        m->truncated |= copy(m->mopts, sizeof m->mopts, fl[5]);
        m->truncated |= copy(m->fstype, sizeof m->fstype, fl[dash + 1]);
        m->truncated |= copy(m->source, sizeof m->source, fl[dash + 2]);
        m->truncated |= copy(m->sopts, sizeof m->sopts, dash + 3 < nf ? fl[dash + 3] : "");
        unescape(m->source);
    }
    free(line);
    fclose(f);
    return found ? NULL : "no mount in /proc/self/mountinfo covers it";
}

static int has_opt(const char *opts, const char *o) { /* o is one whole comma-separated token of opts */
    size_t l = strlen(o);
    for (const char *p = opts; (p = strstr(p, o)); p += l)
        if ((p == opts || p[-1] == ',') && (p[l] == 0 || p[l] == ',')) return 1;
    return 0;
}
static int barrier_off(const mrec *m) {
    return has_opt(m->mopts, "nobarrier") || has_opt(m->sopts, "nobarrier") || has_opt(m->mopts, "barrier=0") ||
           has_opt(m->sopts, "barrier=0");
}

static int read_line(const char *p, char *out, size_t cap) { /* 0 on success */
    FILE *f = fopen(p, "r");
    if (!f) return -1;
    int ok = fgets(out, (int)cap, f) != NULL;
    fclose(f);
    if (!ok) return -1;
    out[strcspn(out, "\n")] = 0;
    return 0;
}

/* ---- the flush path: D's mount, then through each loop device the mount holding its backing file ---- */
typedef struct {
    mrec m;
    char disk[PATH_MAX];    /* the whole disk's /sys dir (a partition's parent) */
    char backing[PATH_MAX]; /* a loop device's backing file, else "" */
} layer;
static layer L[MAXLAYERS];
static int NL;
static char WHY[3 * PATH_MAX + 512];

static const char *flush_path(const char *dir) { /* NULL when every layer was read, else why not */
    char cur[PATH_MAX];
    pathf(cur, sizeof cur, "%s", dir);
    for (NL = 0; NL < MAXLAYERS; NL++) {
        layer *l = &L[NL];
        const char *why = mount_of(cur, &l->m);
        if (why) { snprintf(WHY, sizeof WHY, "layer %d (%s): %s", NL, cur, why); return WHY; }
        if (l->m.truncated) { snprintf(WHY, sizeof WHY, "layer %d (%s): its mount record is too long to read whole", NL, l->m.mnt); return WHY; }
        char sys[PATH_MAX], p[PATH_MAX];
        pathf(p, sizeof p, "/sys/dev/block/%s", l->m.dev);
        if (!realpath(p, sys)) {
            const char *b = strrchr(l->m.source, '/');
            pathf(p, sizeof p, "/sys/class/block/%s", b ? b + 1 : l->m.source);
            if (strncmp(l->m.source, "/dev/", 5) || !realpath(p, sys)) {
                snprintf(WHY, sizeof WHY, "layer %d (%s): cannot find the device of %s (dev %s) in /sys", NL, l->m.mnt,
                         l->m.source, l->m.dev);
                return WHY;
            }
        }
        pathf(p, sizeof p, "%s/partition", sys);
        if (access(p, F_OK) == 0) { char *d = dirname(sys); pathf(l->disk, sizeof l->disk, "%s", d); }
        else pathf(l->disk, sizeof l->disk, "%s", sys);
        pathf(p, sizeof p, "%s/loop/backing_file", l->disk);
        if (access(p, F_OK) != 0) { NL++; return NULL; } /* not a loop: the leaf */
        if (read_line(p, l->backing, sizeof l->backing) != 0 || !l->backing[0] || strstr(l->backing, " (deleted)")) {
            snprintf(WHY, sizeof WHY, "layer %d (%s): cannot read a live backing file for loop %s", NL, l->m.mnt, l->disk);
            return WHY;
        }
        pathf(cur, sizeof cur, "%s", l->backing);
    }
    return "more than 4 loop layers";
}

/* ---- ext4: a trial FICLONE, so a refused clone arm records what the filesystem actually said ---- */
static void ext4_clone_reason(char *out, size_t cap, const mrec *m) {
    char a[PATH_MAX], b[PATH_MAX];
    pathf(a, sizeof a, "%s/.v3floor-ficlone-trial-%d.src", DIR_, (int)getpid());
    pathf(b, sizeof b, "%s/.v3floor-ficlone-trial-%d.dst", DIR_, (int)getpid());
    int sfd = open(a, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (sfd < 0) die("FICLONE trial src");
    if (pwrite(sfd, buf, 4096, 0) != 4096) die("FICLONE trial write");
    int dfd = open(b, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dfd < 0) die("FICLONE trial dst");
    int r = ioctl(dfd, FICLONE, sfd), e = errno;
    if (close(dfd) != 0 || close(sfd) != 0 || unlinkat(AT_FDCWD, a, 0) != 0 || unlinkat(AT_FDCWD, b, 0) != 0)
        die("FICLONE trial cleanup");
    if (r == 0) {
        fprintf(stderr, "v3floor: ext4 at %s ACCEPTED a trial FICLONE: the ext4 clone refusal is wrong here\n", m->mnt);
        exit(1);
    }
    snprintf(out, cap, "%s at %s has no reflink: a trial ioctl(FICLONE) returned %s (%s); clone arms run on xfs and btrfs only",
             m->fstype, m->mnt, e == EOPNOTSUPP ? "EOPNOTSUPP" : e == EXDEV ? "EXDEV" : e == EINVAL ? "EINVAL" : "errno",
             strerror(e));
}

static int cmp_u64(const void *x, const void *y) {
    uint64_t a = *(const uint64_t *)x, b = *(const uint64_t *)y;
    return a < b ? -1 : a > b;
}
static uint64_t pct(const uint64_t *v, uint64_t n, double p) {
    uint64_t k = (uint64_t)(p * (double)n);
    return v[k >= n ? n - 1 : k];
}

static uint64_t rng = 88172645463325252ULL;
static uint64_t xs(void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

int main(int argc, char **argv) {
    const char *out = NULL, *arms = "append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,nosync25";
    uint64_t n = 0, seed = 0;
    int have_n = 0, have_seed = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--dir") && i + 1 < argc) DIR_ = argv[++i];
        else if (!strcmp(argv[i], "--out") && i + 1 < argc) out = argv[++i];
        else if (!strcmp(argv[i], "--n") && i + 1 < argc) {
            if (parse_u64(argv[++i], &n) != 0) { fprintf(stderr, "v3floor: REFUSED: --n %s is not a whole number\n", argv[i]); return 2; }
            have_n = 1;
        } else if (!strcmp(argv[i], "--arms") && i + 1 < argc) arms = argv[++i];
        else if (!strcmp(argv[i], "--seed") && i + 1 < argc) {
            if (parse_u64(argv[++i], &seed) != 0) { fprintf(stderr, "v3floor: REFUSED: --seed %s is not a whole number\n", argv[i]); return 2; }
            have_seed = 1;
        } else if (!strcmp(argv[i], "--mutant-nosync")) MUTANT = 1;
        else if (!strcmp(argv[i], "--trace-clock")) TRACE_CLOCK = 1;
        else { fprintf(stderr, "v3floor: bad argument %s\n", argv[i]); return 2; }
    }
    if (!DIR_ || !out || !have_n || n == 0) {
        fprintf(stderr, "usage: v3floor --dir D --out O --n N (N >= 1) [--arms ...] [--seed S] [--mutant-nosync] [--trace-clock]\n");
        return 2;
    }
    if (strlen(DIR_) > PATH_MAX - 128 || strlen(out) > PATH_MAX - 128) {
        fprintf(stderr, "v3floor: REFUSED: the dir or out path is too long (over %d bytes)\n", PATH_MAX - 128);
        return 2;
    }
    if (have_seed) rng = seed | 1;
    const uint64_t seed_used = rng;

    /* the filesystem under D (its type first: a tmpfs has no device to follow), then the flush path below it */
    static mrec top0;
    const char *why = mount_of(DIR_, &top0);
    if (why) { fprintf(stderr, "v3floor: REFUSED: cannot determine the filesystem under %s: %s\n", DIR_, why); return 2; }
    unsigned long want = !strcmp(top0.fstype, "ext4") ? MAGIC_EXT4 : !strcmp(top0.fstype, "xfs") ? MAGIC_XFS
                       : !strcmp(top0.fstype, "btrfs") ? MAGIC_BTRFS : 0;
    if (!want) {
        fprintf(stderr, "v3floor: REFUSED: %s is on %s (mount %s), not ext4, xfs or btrfs\n", DIR_, top0.fstype, top0.mnt);
        return 2;
    }
    why = flush_path(DIR_);
    if (why) { fprintf(stderr, "v3floor: REFUSED: cannot determine the flush path under %s: %s\n", DIR_, why); return 2; }
    const mrec *top = &L[0].m;
    struct statfs sf;
    if (statfs(DIR_, &sf) != 0) die("statfs");
    unsigned long magic = (unsigned long)(uint32_t)sf.f_type;
    unsigned fsid0, fsid1;
    memcpy(&fsid0, &sf.f_fsid, sizeof fsid0);
    memcpy(&fsid1, (const char *)&sf.f_fsid + sizeof fsid0, sizeof fsid1);
    if (magic != want) {
        fprintf(stderr, "v3floor: REFUSED: cannot determine the filesystem under %s: the mount table says %s but statfs "
                "magic is 0x%lx\n", DIR_, top->fstype, magic);
        return 2;
    }
    for (int k = 0; k < NL; k++)
        if (barrier_off(&L[k].m)) {
            fprintf(stderr, "v3floor: REFUSED: the flush path has nobarrier: layer %d, %s (%s on %s, options %s,%s), so an "
                    "fsync's cache flush never reaches the device\n", k, L[k].m.mnt, L[k].m.fstype, L[k].m.source,
                    L[k].m.mopts, L[k].m.sopts);
            return 2;
        }
    char wc[64] = "", fua[16] = "", p[PATH_MAX];
    pathf(p, sizeof p, "%s/queue/write_cache", L[NL - 1].disk);
    if (read_line(p, wc, sizeof wc) != 0) snprintf(wc, sizeof wc, "unknown");
    pathf(p, sizeof p, "%s/queue/fua", L[NL - 1].disk);
    if (read_line(p, fua, sizeof fua) != 0) snprintf(fua, sizeof fua, "unknown");

    errno = 0;
    int nice_v = getpriority(PRIO_PROCESS, 0);
    if (errno) die("getpriority");
    if (nice_v != 0) { fprintf(stderr, "v3floor: REFUSED: nice is %d, not 0\n", nice_v); return 2; }
    int pol = sched_getscheduler(0);
    if (pol < 0) die("sched_getscheduler");
    if (pol != SCHED_OTHER) { fprintf(stderr, "v3floor: REFUSED: scheduling policy is %d, not SCHED_OTHER\n", pol); return 2; }
    long iop = syscall(SYS_ioprio_get, IOPRIO_WHO_PROC, 0);
    if (iop < 0) die("ioprio_get");
    int ioclass = (int)(iop >> IOPRIO_SHIFT), iolevel = (int)(iop & ((1 << IOPRIO_SHIFT) - 1));
    if (!(ioclass == 0 || (ioclass == 2 && iolevel == 4))) {
        fprintf(stderr, "v3floor: REFUSED: I/O priority is class %d level %d, not the default (class 0, or best-effort "
                "level 4)\n", ioclass, iolevel);
        return 2;
    }

    int req[NARMS], nreq = 0;
    char *list = strdup(arms);
    if (!list) die("strdup");
    for (char *t = strtok(list, ","); t; t = strtok(NULL, ",")) {
        int f = -1;
        for (int a = 0; a < NARMS; a++) if (!strcmp(t, NAMES[a])) f = a;
        if (f < 0) { fprintf(stderr, "v3floor: unknown arm %s\n", t); return 2; }
        for (int j = 0; j < nreq; j++) if (req[j] == f) { fprintf(stderr, "v3floor: arm %s twice\n", t); return 2; }
        req[nreq++] = f;
    }
    if (nreq == 0) { fprintf(stderr, "v3floor: REFUSED: no arm selected\n"); return 2; }
    int has_d0 = 0, first_flushed = -1;
    for (int j = 0; j < nreq; j++) {
        if (req[j] == NOSYNC25) has_d0 = 1;
        if (flushed(req[j]) && first_flushed < 0) first_flushed = req[j];
    }
    if (first_flushed >= 0 && !has_d0) {
        fprintf(stderr, "v3floor: REFUSED: flushed arm %s selected without nosync25, so its flush control could not run\n",
                NAMES[first_flushed]);
        return 2;
    }
    for (size_t i = 0; i < sizeof buf; i++) buf[i] = (char)xs();

    /* the clone arms on ext4: refused per arm, with the reason recorded */
    int sel[NARMS], na = 0, refused[NARMS] = {0}, nref = 0, nflushed = 0;
    char reason[PATH_MAX + 1024] = "";
    for (int j = 0; j < nreq; j++) {
        if (is_clone(req[j]) && want == MAGIC_EXT4) {
            if (!reason[0]) ext4_clone_reason(reason, sizeof reason, top);
            refused[req[j]] = 1;
            nref++;
            fprintf(stderr, "v3floor: REFUSED arm %s: %s\n", NAMES[req[j]], reason);
        } else {
            sel[na++] = req[j];
            nflushed += flushed(req[j]);
        }
    }
    if (first_flushed >= 0 && nflushed == 0) {
        fprintf(stderr, "v3floor: REFUSED: every flushed arm selected was refused, so the run would measure no flush\n");
        return 2;
    }
    for (int j = 0; j < na; j++)
        if (is_clone(sel[j])) {
            struct stat sb;
            pathf(p, sizeof p, "%s/%s.clones", DIR_, NAMES[sel[j]]);
            if (lstat(p, &sb) == 0) {
                fprintf(stderr, "v3floor: REFUSED: %s is left over from an earlier run; remove it\n", p);
                return 2;
            }
        }
    if (mkdir(out, 0755) != 0) { fprintf(stderr, "v3floor: REFUSED: out dir %s must not exist: %s\n", out, strerror(errno)); return 2; }

    armst st[NARMS];
    uint64_t *lat[NARMS];
    for (int j = 0; j < na; j++) {
        setup(sel[j], &st[j]);
        lat[j] = calloc(n, sizeof(uint64_t));
        if (!lat[j]) die("calloc");
    }
    int order[NARMS];
    for (uint64_t i = 0; i < n; i++) {
        for (int j = 0; j < na; j++) order[j] = j;
        for (int j = na - 1; j > 0; j--) { int k = (int)(xs() % (uint64_t)(j + 1)), t = order[j]; order[j] = order[k]; order[k] = t; }
        for (int q = 0; q < na; q++) {
            int j = order[q];
            uint64_t t0 = now();
            op(sel[j], &st[j], i);
            lat[j][i] = now() - t0;
        }
    }
    for (int j = 0; j < na; j++) teardown(sel[j], &st[j], n);
    int dd = open(DIR_, O_RDONLY | O_DIRECTORY);
    if (dd < 0 || fsync(dd) != 0 || close(dd) != 0) die("teardown fsync of the dir");

    /* Each output file gets one buffer big enough for all of it, so it costs a constant number of write(2)s
     * whatever n is: the fire-check's per-op syscall slope then sees only the timed ops. */
    pathf(p, sizeof p, "%s/raw.tsv", out);
    FILE *f = fopen(p, "w");
    if (!f) die("raw.tsv");
    size_t rawcap = (size_t)n * (size_t)na * 64 + 64;
    char *rawbuf = malloc(rawcap);
    if (!rawbuf || setvbuf(f, rawbuf, _IOFBF, rawcap) != 0) die("raw.tsv buffer");
    fprintf(f, "arm\ti\tns\n");
    for (int j = 0; j < na; j++)
        for (uint64_t i = 0; i < n; i++) fprintf(f, "%s\t%llu\t%llu\n", NAMES[sel[j]], (unsigned long long)i, (unsigned long long)lat[j][i]);
    if (fclose(f) != 0) die("raw.tsv close");
    free(rawbuf);

    pathf(p, sizeof p, "%s/summary.json", out);
    f = fopen(p, "w");
    if (!f) die("summary.json");
    static char sumbuf[1 << 17];
    if (setvbuf(f, sumbuf, _IOFBF, sizeof sumbuf) != 0) die("summary.json buffer");
    struct utsname u;
    if (uname(&u) != 0) die("uname");
    fprintf(f, "{\"probe\":\"v3floor-linux\",\"n\":%llu,\"seed\":%llu,\"mutant_nosync\":%d,\"trace_clock\":%d,"
            "\"clock\":\"CLOCK_MONOTONIC_RAW\",\"barrier\":\"fsync(2)\",\"dir\":", (unsigned long long)n,
            (unsigned long long)seed_used, MUTANT, TRACE_CLOCK);
    jstr(f, DIR_);
    fprintf(f, ",\"realpath\":"); jstr(f, top->real);
    fprintf(f, ",\"fstype\":"); jstr(f, top->fstype);
    fprintf(f, ",\"mount_point\":"); jstr(f, top->mnt);
    fprintf(f, ",\"mount_source\":"); jstr(f, top->source);
    fprintf(f, ",\"mount_opts\":"); jstr(f, top->mopts);
    fprintf(f, ",\"super_opts\":"); jstr(f, top->sopts);
    fprintf(f, ",\"dev\":"); jstr(f, top->dev);
    fprintf(f, ",\"statfs_magic\":\"0x%lx\",\"fsid\":\"%08x%08x\",\"flush_path\":[", magic, fsid0, fsid1);
    for (int k = 0; k < NL; k++) {
        fprintf(f, "%s{\"mount\":", k ? "," : ""); jstr(f, L[k].m.mnt);
        fprintf(f, ",\"fstype\":"); jstr(f, L[k].m.fstype);
        fprintf(f, ",\"source\":"); jstr(f, L[k].m.source);
        fprintf(f, ",\"options\":"); jstr(f, L[k].m.mopts);
        fprintf(f, ",\"super_options\":"); jstr(f, L[k].m.sopts);
        fprintf(f, ",\"sys\":"); jstr(f, L[k].disk);
        fprintf(f, ",\"loop_backing\":"); jstr(f, L[k].backing);
        fprintf(f, "}");
    }
    fprintf(f, "],\"leaf_write_cache\":"); jstr(f, wc);
    fprintf(f, ",\"leaf_fua\":"); jstr(f, fua);
    fprintf(f, ",\"flush_sent_to_device\":\"%s\"", !strcmp(wc, "write back") ? "yes: the leaf device reports a write-back cache"
            : !strcmp(wc, "write through") ? "no: the leaf device reports write-through, so the block layer sends it no flush"
            : "unknown");
    fprintf(f, ",\"uname\":");
    char un[600];
    snprintf(un, sizeof un, "%s %s %s", u.sysname, u.release, u.machine);
    jstr(f, un);
    fprintf(f, ",\"nice\":%d,\"sched_policy\":%d,\"ioprio_class\":%d,\"ioprio_level\":%d,\"arms_requested\":", nice_v, pol,
            ioclass, iolevel);
    jstr(f, arms);
    fprintf(f, ",\"refused_arms\":{");
    for (int a = 0, k = 0; a < NARMS; a++)
        if (refused[a]) { fprintf(f, "%s\"%s\":", k++ ? "," : "", NAMES[a]); jstr(f, reason); }
    fprintf(f, "},\"arms\":{");
    uint64_t p50[NARMS] = {0};
    int have_app = -1, have_clean = -1, have_d0 = -1;
    for (int j = 0; j < na; j++) {
        qsort(lat[j], n, sizeof(uint64_t), cmp_u64);
        double mean = 0;
        for (uint64_t i = 0; i < n; i++) mean += (double)lat[j][i];
        mean /= (double)n;
        p50[j] = pct(lat[j], n, .50);
        if (sel[j] == APPEND25) have_app = j;
        if (sel[j] == CLEAN) have_clean = j;
        if (sel[j] == NOSYNC25) have_d0 = j;
        fprintf(f, "%s\"%s\":{\"min_us\":%.1f,\"p1_us\":%.1f,\"p10_us\":%.1f,\"p50_us\":%.1f,\"p90_us\":%.1f,\"p99_us\":%.1f,",
                j ? "," : "", NAMES[sel[j]], lat[j][0] / 1e3, pct(lat[j], n, .01) / 1e3, pct(lat[j], n, .10) / 1e3,
                p50[j] / 1e3, pct(lat[j], n, .90) / 1e3, pct(lat[j], n, .99) / 1e3);
        if (n >= 10000) fprintf(f, "\"p999_us\":%.1f,", pct(lat[j], n, .999) / 1e3);
        fprintf(f, "\"max_us\":%.1f,\"mean_us\":%.1f}", lat[j][n - 1] / 1e3, mean / 1e3);
    }
    fprintf(f, "}");
    int rc = 0;
    if (have_app >= 0 && have_clean >= 0) {
        double ratio = p50[have_clean] ? (double)p50[have_app] / (double)p50[have_clean] : 1e18;
        uint64_t fast = 0;
        for (uint64_t i = 0; i < n; i++) fast += lat[have_clean][i] < 100000;
        fprintf(f, ",\"dirty_clean_p50_ratio\":%.1f,\"dirty_clean_m0\":\"%s\",\"clean_fast_frac\":%.4f", ratio,
                ratio > 10.0 ? "met" : "not met (report only)", (double)fast / (double)n);
    } else {
        fprintf(f, ",\"dirty_clean_m0\":\"not run (needs append25 and clean)\"");
    }
    if (have_d0 >= 0) {
        if (have_app >= 0)
            fprintf(f, ",\"flush_d0_p50_ratio\":%.1f",
                    p50[have_d0] ? (double)p50[have_app] / (double)p50[have_d0] : 1e18);
        fprintf(f, ",\"flush_control_arms\":{");
        int ngated = 0, nfail = 0, k = 0;
        char failed[256] = "";
        for (int j = 0; j < na; j++) {
            if (sel[j] == NOSYNC25) continue;
            double ratio = p50[have_d0] ? (double)p50[j] / (double)p50[have_d0] : 1e18;
            int g = gated(sel[j]), ok = ratio > 10.0;
            fprintf(f, "%s\"%s\":{\"ratio\":%.1f,\"gated\":%s,\"pass\":%s}", k++ ? "," : "", NAMES[sel[j]], ratio,
                    g ? "true" : "false", ok ? "true" : "false");
            if (g) {
                ngated++;
                if (!ok) {
                    nfail++;
                    size_t l = strlen(failed);
                    snprintf(failed + l, sizeof failed - l, "%s%s", l ? "," : "", NAMES[sel[j]]);
                }
            }
        }
        fprintf(f, "}");
        if (ngated == 0) fprintf(f, ",\"flush_control\":\"not applicable (no gated arm ran)\"");
        else if (nfail) { fprintf(f, ",\"flush_control\":\"FAIL: run void (%s)\"", failed); rc = 3; }
        else fprintf(f, ",\"flush_control\":\"pass\"");
    } else {
        fprintf(f, ",\"flush_control\":\"not applicable (no flushed arm selected)\"");
    }
    fprintf(f, "}\n");
    if (fclose(f) != 0) die("summary.json close");
    printf("v3floor n=%llu arms=%s refused=%d -> %s (rc %d)\n", (unsigned long long)n, arms, nref, out, rc);
    return rc;
}
