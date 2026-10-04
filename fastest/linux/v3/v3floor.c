/* v3floor.c -- V3 device-floor probe, Linux port of frontier/fastest/tools/v3/floor.c (PREREG §4 V3, §11 M0 exit 1).
 *
 *   v3floor --dir D --out O --n N [--arms a,b,...] [--seed S] [--mutant-nosync]
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
 * Syscalls per op, by definition (fastest/linux/v3/firecheck.sh checks them with strace -f -c):
 *   append25, ow4k, ow64k, ow1m   pwrite64, fsync          fdatasync4k  pwrite64, fdatasync
 *   clean                         fsync                    nosync25     pwrite64
 *   clone1b   openat, ioctl(FICLONE), close, fsync(dir)    clone2b      openat, ioctl(FICLONE), fsync, close, fsync(dir)
 *   (the clone arms' teardown adds one unlinkat per clone, outside the timed window)
 * Setup per arm, before the loop: append25/nosync25 write 25 B + fsync; ow*/fdatasync4k/clean preallocate + fsync;
 *   clone arms preallocate the source + fsync, mkdir the clones' directory + fsync it. Setup syncs are never mutated.
 *
 * Arms are interleaved round-robin, each round in a fresh seeded shuffle, so every arm shares the moment.
 * Latency = CLOCK_MONOTONIC_RAW around the op (write + barrier(s)), in ns.
 *
 * Output (raw first, then the summary computed from it): O/raw.tsv (arm, i, ns), O/summary.json.
 * Exit: 0 ok | 2 usage or refused setup | 1 an operation failed | 3 VOID: the flush control failed.
 * Flush control (D0, per arm, tools review 1 item 6): for EVERY selected M0 flushed arm (append25, ow4k, ow64k, ow1m,
 *   clone1b, clone2b), p50(arm) / p50(nosync25) must be > 10, else the run is void (rc 3). The fdatasync4k and
 *   clean ratios are reported, never gated. flush_d0_p50_ratio keeps the Mac's headline (append25 / nosync25).
 * Reported, NOT a gate: the drafted M0 exit-1 ratio p50(append25) / p50(clean) > 10, and clean_fast_frac, the share
 *   of clean samples under 100 us (the Mac record found that ratio does not discriminate: tools/v3/FIRECHECK.md).
 *
 * Refusals (rc 2, nothing left behind): D's filesystem is not ext4, xfs or btrfs (the mount table's fstype for D's
 *   longest mount prefix and statfs's magic must agree, else "cannot determine" also refuses); nice != 0, or an I/O
 *   priority other than the default (class none, or best-effort level 4, the nice-0 default); the out dir exists;
 *   n is not a positive integer; an unknown or repeated arm; a flushed arm (append25, ow*, clone*, fdatasync4k)
 *   selected without nosync25, whose control would otherwise silently not run (tools review 1 item 6).
 * The clone arms run on XFS and btrfs only. On ext4 every selected clone arm is REFUSED, never skipped silently: it
 *   does not run, and the reason (the filesystem and the errno a trial FICLONE returned there) is written to
 *   summary.json "refused_arms" and to stderr; the remaining arms run. On xfs or btrfs a failed FICLONE is an error
 *   (rc 1), and an ext4 that accepts the trial FICLONE is an error too (the refusal rule would be wrong).
 * --mutant-nosync skips every flush in the flushed arms: a fire-check that the control above fails it.
 *
 * Blind spots, stated: foreign I/O on the device and cgroup I/O throttling are not observed here (run.sh's stamps
 *   record /proc/diskstats and PSI around the batch). The timing control cannot tell an arm whose write alone costs
 *   more than 10x nosync25 (ow1m, the clones) from the same arm with its flush; the exact flush identity is the
 *   strace count in firecheck.sh, which must run on the same binary (sha256) as any credited batch. On Linux an fsync
 *   is per inode plus the filesystem's journal, not a device-wide cache flush as F_FULLFSYNC is on Apple, so what
 *   clone1b's one barrier makes durable is a question for a crash test, not for this probe (unverified).
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/fs.h>
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

enum { APPEND25, OW4K, OW64K, OW1M, CLONE1B, CLONE2B, CLEAN, FDATASYNC4K, NOSYNC25, NARMS };
static const char *NAMES[NARMS] = {"append25", "ow4k", "ow64k", "ow1m", "clone1b", "clone2b", "clean",
                                   "fdatasync4k", "nosync25"};
#define MIB (1u << 20)
static int gated(int a) { return a <= CLONE2B; }                     /* M0 flushed arms: the control gates them */
static int flushed(int a) { return a <= CLONE2B || a == FDATASYNC4K; } /* a flush the mutant removes */
static int is_clone(int a) { return a == CLONE1B || a == CLONE2B; }

static const char *DIR_;
static int MUTANT;
static char buf[MIB];

typedef struct {
    int fd;
    off_t off, cap;
    size_t rec;
    int dfd;   /* clone arms: the clones' directory */
    int srcfd; /* clone arms: the durable source */
    char cdir[1024];
    char src[1024];
} armst;

static void die(const char *what) { fprintf(stderr, "v3floor: %s: %s\n", what, strerror(errno)); exit(1); }
static void barrier(int fd) { if (!MUTANT && fsync(fd) == -1) die("fsync"); }
static void setup_sync(int fd, const char *what) { if (fsync(fd) == -1) die(what); } /* never mutated */
static uint64_t now(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) die("clock_gettime CLOCK_MONOTONIC_RAW");
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
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
    char p[1100];
    snprintf(p, sizeof p, "%s/%s", DIR_, name);
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
        snprintf(s->src, sizeof s->src, "%s/%s.src", DIR_, NAMES[a]);
        s->srcfd = open(s->src, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (s->srcfd < 0) die("clone src");
        prealloc(s->srcfd, MIB);
        snprintf(s->cdir, sizeof s->cdir, "%s/%s.clones", DIR_, NAMES[a]);
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
        char dst[1100];
        snprintf(dst, sizeof dst, "%s/c%llu", s->cdir, (unsigned long long)i);
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
    char p[1100];
    if (s->fd >= 0) {
        close(s->fd);
        snprintf(p, sizeof p, "%s/%s", DIR_, NAMES[a]);
        unlinkat(AT_FDCWD, p, 0);
    }
    if (s->dfd >= 0) {
        for (uint64_t i = 0; i < n; i++) {
            snprintf(p, sizeof p, "c%llu", (unsigned long long)i);
            unlinkat(s->dfd, p, 0);
        }
        close(s->dfd);
        unlinkat(AT_FDCWD, s->cdir, AT_REMOVEDIR);
    }
    if (s->srcfd >= 0) {
        close(s->srcfd);
        unlinkat(AT_FDCWD, s->src, 0);
    }
}

/* ---- the filesystem under D, from the mount table and statfs, which must agree ---- */
typedef struct {
    char real[PATH_MAX], fstype[64], mnt[PATH_MAX], source[512], mopts[1024], sopts[1024], dev[32];
    unsigned long magic;
    unsigned fsid0, fsid1;
} fsinfo;

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

static const char *fs_of(const char *dir, fsinfo *fi) { /* NULL when known, else why it cannot be determined */
    memset(fi, 0, sizeof *fi);
    if (!realpath(dir, fi->real)) return "realpath failed";
    FILE *m = fopen("/proc/self/mountinfo", "r");
    if (!m) return "cannot read /proc/self/mountinfo";
    char *line = NULL;
    size_t cap = 0;
    size_t best = 0;
    int found = 0;
    while (getline(&line, &cap, m) > 0) {
        line[strcspn(line, "\n")] = 0;
        char *f[64];
        int nf = 0;
        for (char *t = strtok(line, " "); t && nf < 64; t = strtok(NULL, " ")) f[nf++] = t;
        int dash = -1;
        for (int k = 6; k < nf; k++) if (!strcmp(f[k], "-")) { dash = k; break; }
        if (dash < 0 || dash + 2 >= nf) continue; /* need the fstype and the source after "-" */
        char mp[PATH_MAX];
        snprintf(mp, sizeof mp, "%s", f[4]);
        unescape(mp);
        size_t l = strlen(mp);
        int under = !strcmp(mp, "/") || (!strncmp(fi->real, mp, l) && (fi->real[l] == 0 || fi->real[l] == '/'));
        if (!under) continue;
        size_t key = !strcmp(mp, "/") ? 1 : l + 1;
        if (key < best) continue; /* equal length: the later mount shadows the earlier one */
        best = key;
        found = 1;
        snprintf(fi->mnt, sizeof fi->mnt, "%s", mp);
        snprintf(fi->dev, sizeof fi->dev, "%s", f[2]);
        snprintf(fi->mopts, sizeof fi->mopts, "%s", f[5]);
        snprintf(fi->fstype, sizeof fi->fstype, "%s", f[dash + 1]);
        snprintf(fi->source, sizeof fi->source, "%s", f[dash + 2]);
        snprintf(fi->sopts, sizeof fi->sopts, "%s", dash + 3 < nf ? f[dash + 3] : "");
        unescape(fi->source);
    }
    free(line);
    fclose(m);
    if (!found) return "no mount in /proc/self/mountinfo covers it";
    struct statfs sf;
    if (statfs(dir, &sf) != 0) return "statfs failed";
    fi->magic = (unsigned long)(uint32_t)sf.f_type;
    memcpy(&fi->fsid0, &sf.f_fsid, sizeof fi->fsid0);
    memcpy(&fi->fsid1, (const char *)&sf.f_fsid + sizeof fi->fsid0, sizeof fi->fsid1);
    return NULL;
}

/* ---- ext4: a trial FICLONE, so a refused clone arm records what the filesystem actually said ---- */
static void ext4_clone_reason(char *out, size_t cap, const fsinfo *fi) {
    char a[1100], b[1100];
    snprintf(a, sizeof a, "%s/.v3floor-ficlone-trial-%d.src", DIR_, (int)getpid());
    snprintf(b, sizeof b, "%s/.v3floor-ficlone-trial-%d.dst", DIR_, (int)getpid());
    int sfd = open(a, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (sfd < 0) die("FICLONE trial src");
    if (pwrite(sfd, buf, 4096, 0) != 4096) die("FICLONE trial write");
    int dfd = open(b, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dfd < 0) die("FICLONE trial dst");
    int r = ioctl(dfd, FICLONE, sfd), e = errno;
    close(dfd);
    close(sfd);
    unlinkat(AT_FDCWD, a, 0);
    unlinkat(AT_FDCWD, b, 0);
    if (r == 0) {
        fprintf(stderr, "v3floor: ext4 at %s ACCEPTED a trial FICLONE: the ext4 clone refusal is wrong here\n", fi->mnt);
        exit(1);
    }
    snprintf(out, cap, "%s at %s has no reflink: a trial ioctl(FICLONE) returned %s (%s); clone arms run on xfs and btrfs only",
             fi->fstype, fi->mnt, e == EOPNOTSUPP ? "EOPNOTSUPP" : e == EXDEV ? "EXDEV" : e == EINVAL ? "EINVAL" : "errno",
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
        else { fprintf(stderr, "v3floor: bad argument %s\n", argv[i]); return 2; }
    }
    if (!DIR_ || !out || !have_n || n == 0) {
        fprintf(stderr, "usage: v3floor --dir D --out O --n N (N >= 1) [--arms ...] [--seed S] [--mutant-nosync]\n");
        return 2;
    }
    if (have_seed) rng = seed | 1;
    const uint64_t seed_used = rng;

    fsinfo fi;
    const char *why = fs_of(DIR_, &fi);
    if (why) { fprintf(stderr, "v3floor: REFUSED: cannot determine the filesystem under %s: %s\n", DIR_, why); return 2; }
    unsigned long want = !strcmp(fi.fstype, "ext4") ? MAGIC_EXT4 : !strcmp(fi.fstype, "xfs") ? MAGIC_XFS
                       : !strcmp(fi.fstype, "btrfs") ? MAGIC_BTRFS : 0;
    if (!want) {
        fprintf(stderr, "v3floor: REFUSED: %s is on %s (mount %s), not ext4, xfs or btrfs\n", DIR_, fi.fstype, fi.mnt);
        return 2;
    }
    if (fi.magic != want) {
        fprintf(stderr, "v3floor: REFUSED: cannot determine the filesystem under %s: the mount table says %s but statfs "
                "magic is 0x%lx\n", DIR_, fi.fstype, fi.magic);
        return 2;
    }
    errno = 0;
    int nice_v = getpriority(PRIO_PROCESS, 0);
    if (errno) die("getpriority");
    if (nice_v != 0) { fprintf(stderr, "v3floor: REFUSED: nice is %d, not 0\n", nice_v); return 2; }
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
    int sel[NARMS], na = 0, refused[NARMS] = {0}, nref = 0;
    char reason[600] = "";
    for (int j = 0; j < nreq; j++) {
        if (is_clone(req[j]) && want == MAGIC_EXT4) {
            if (!reason[0]) ext4_clone_reason(reason, sizeof reason, &fi);
            refused[req[j]] = 1;
            nref++;
            fprintf(stderr, "v3floor: REFUSED arm %s: %s\n", NAMES[req[j]], reason);
        } else sel[na++] = req[j];
    }
    if (na == 0) { fprintf(stderr, "v3floor: REFUSED: every selected arm was refused\n"); return 2; }
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

    /* Each output file gets one buffer big enough for all of it, so it costs a constant number of write(2)s
     * whatever n is: the fire-check's per-op syscall slope then sees only the timed ops. */
    char p[1100];
    snprintf(p, sizeof p, "%s/raw.tsv", out);
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

    snprintf(p, sizeof p, "%s/summary.json", out);
    f = fopen(p, "w");
    if (!f) die("summary.json");
    static char sumbuf[1 << 16];
    if (setvbuf(f, sumbuf, _IOFBF, sizeof sumbuf) != 0) die("summary.json buffer");
    struct utsname u;
    if (uname(&u) != 0) die("uname");
    fprintf(f, "{\"probe\":\"v3floor-linux\",\"n\":%llu,\"seed\":%llu,\"mutant_nosync\":%d,\"clock\":\"CLOCK_MONOTONIC_RAW\","
            "\"barrier\":\"fsync(2)\",\"dir\":", (unsigned long long)n, (unsigned long long)seed_used, MUTANT);
    jstr(f, DIR_);
    fprintf(f, ",\"realpath\":"); jstr(f, fi.real);
    fprintf(f, ",\"fstype\":"); jstr(f, fi.fstype);
    fprintf(f, ",\"mount_point\":"); jstr(f, fi.mnt);
    fprintf(f, ",\"mount_source\":"); jstr(f, fi.source);
    fprintf(f, ",\"mount_opts\":"); jstr(f, fi.mopts);
    fprintf(f, ",\"super_opts\":"); jstr(f, fi.sopts);
    fprintf(f, ",\"dev\":"); jstr(f, fi.dev);
    fprintf(f, ",\"statfs_magic\":\"0x%lx\",\"fsid\":\"%08x%08x\",\"uname\":", fi.magic, fi.fsid0, fi.fsid1);
    char un[600];
    snprintf(un, sizeof un, "%s %s %s", u.sysname, u.release, u.machine);
    jstr(f, un);
    fprintf(f, ",\"nice\":%d,\"ioprio_class\":%d,\"ioprio_level\":%d,\"arms_requested\":", nice_v, ioclass, iolevel);
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
