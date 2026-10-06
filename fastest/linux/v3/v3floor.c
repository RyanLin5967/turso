/* v3floor.c -- V3 device-floor probe, Linux port of frontier/fastest/tools/v3/floor.c (PREREG §4 V3, §11 M0 exit 1).
 *
 *   v3floor --dir D --out O --n N [--arms a,b,...] [--seed S]
 *   v3floor ... --mutant-nosync | --trace-clock                  (fire-check only, see "Fire-check flags")
 *   v3floor --crash-op ARM --dir D --out O [--crash-aim] [--mutant-nosync]   (fire-check only: one op, no teardown)
 *
 * Arms (default: append25,append64,ow4k,ow64k,ow1m,clone1b,clone2b,cfr2b,clean,nosync25):
 *   append25     pwrite 25 B at EOF (the file grows) + fsync                           (the V3 floor, PREREG's 25 B arm)
 *   append64     pwrite 64 B at EOF + fsync: the frame arm (below)
 *   ow4k         overwrite 4 KiB in a preallocated, durable region + fsync
 *   ow64k        overwrite 64 KiB, same
 *   ow1m         overwrite 1 MiB, same
 *   clone2b      create a file, ioctl(FICLONE) it from a 1 MiB durable source, fsync the clone, close it, fsync the
 *                directory: the durable-clone arm (B1's and PG18's file-clone shape)
 *   cfr2b        the same with copy_file_range(2) of the 1 MiB source in place of FICLONE (PG18's copy call)
 *   clone1b      clone2b without the clone's own fsync. REPORT-ONLY, never gated: on Linux a directory fsync does not
 *                guarantee a FICLONE durable (PREREG crash model; review 2 item 3; crash.sh: lost on btrfs, and on XFS
 *                when the log was forced between the create and the FICLONE; it survives plain XFS by batching)
 *   clean        fsync of a file with nothing dirty (the dirty/clean control; never mutated)
 *   nosync25     the append25 write with no flush (D0): the flush control's reference
 *   Report-only extra: fdatasync4k (ow4k with fdatasync in place of fsync).
 * Gated arms (the flush control voids the run on them): append25, append64, ow4k, ow64k, ow1m, clone2b, cfr2b.
 * Frame arm: PREREG §4 picks, among the M0 append and overwrite arms, the smallest bytes per flush >= the M1 build's
 *   median create frame. Review 2 item 11 puts a named create's flight at about 56-60 B (unverified here), so
 *   without append64 the rule would pick ow4k, an overwrite; append64 is that append. summary.json names it.
 * The barrier is fsync(2): it is what the engine's FullFsync class issues on Linux (core/branch/journal.rs fsync_file).
 *
 * Syscalls of op i, by definition (firecheck.sh checks counts with strace -f -c, and the exact sequence, files,
 * sizes and offsets inside each timed window with strace -f -y under --trace-clock):
 *   append25, nosync25   pwrite64(<arm>, 25, 4096 + 25 i); append25 then fsync(<arm>)
 *   append64             pwrite64(<arm>, 64, 4096 + 64 i); fsync(<arm>)
 *   ow4k, ow64k, ow1m    pwrite64(<arm>, rec, (i mod cap/rec) x rec); fsync(<arm>)    cap 16 MiB (ow1m: 128 MiB)
 *   fdatasync4k          pwrite64 as ow4k; fdatasync(<arm>)
 *   clean                fsync(<arm>)
 *   clone1b              openat(<arm>.clones, "c<i>", O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW), ioctl(c<i>, FICLONE,
 *                        <arm>.src), close, fsync(<arm>.clones)
 *   clone2b              the same with fsync(c<i>) before the close
 *   cfr2b                openat as clone1b, copy_file_range(<arm>.src, [0], c<i>, NULL, 1 MiB, 0), fsync(c<i>), close,
 *                        fsync(<arm>.clones)
 *   Nothing runs between ops. The copy arms' teardown unlinks one clone per op, outside the timed window.
 * Setup per arm, before the loop: append/nosync arms write one 4 KiB block + fsync (so no timed append crosses
 *   btrfs's 2 KiB inline-data limit, and every append is the same kind of write all run); ow*, fdatasync4k and clean
 *   preallocate + fsync; copy arms preallocate the source + fsync, mkdir the clones' directory + fsync it. Every arm
 *   file is created with O_CREAT|O_EXCL|O_NOFOLLOW and fstat'd: it must be a new regular file on D's device. Teardown
 *   ends with one fsync of D, so the unlinks are durable before the next batch. Setup and teardown syncs are never
 *   mutated.
 *
 * Arms are interleaved round-robin, each round in a fresh seeded shuffle, so every arm shares the moment.
 * Latency = CLOCK_MONOTONIC_RAW around the op (write + barrier(s)), in ns, read through the vDSO; raw.tsv also keeps
 *   each op's start (t0_ns), so blkflush.py can place every device flush request in its op's window (tracefs runs
 *   on trace_clock mono_raw, the same clock).
 *
 * Output (raw first, then the summary computed from it): O/raw.tsv (arm, i, ns, t0_ns), O/summary.json.
 * Exit: 0 ok | 2 usage or refused setup | 1 an operation failed | 3 VOID: the flush control failed.
 * Flush control (D0, per arm, tools review 1 item 6): for EVERY selected gated arm, p50(arm) / p50(nosync25) must be
 *   > 10, else the run is void (rc 3). The report-only ratios are reported, never gated. flush_d0_p50_ratio keeps the
 *   Mac's headline (append25 / nosync25). The threshold 10 is provisional for T3: check.py records the separation of
 *   the mutant (F2b) and real (F3) ratios on every cell, and the first T3 fire-check's record re-derives it.
 *   What the control can and cannot see (run 37245757924, 8 cells): the no-flush mutant read <= 5.1 for append25,
 *   ow4k and fdatasync4k on every cell, but 2.7-12.2 for ow64k, 25-197 for ow1m and 8.8-24.3 for the clones, so for
 *   those arms a missing flush can pass it. It shows that a flush cost something, never that it reached the device.
 *   The per-arm flush-syscall identity is firecheck.sh's strace check, and the per-arm DEVICE flush count is run.sh's
 *   blkflush.py record (tracefs block:block_rq_issue), both outside this probe.
 * Reported, NOT a gate: the drafted M0 exit-1 ratio p50(append25) / p50(clean) > 10, and clean_fast_frac.
 *
 * What a flush reaches (summary.json "flush_sent_to_device", "floor_kind"): the leaf is the device under the last
 *   layer. A leaf whose kernel queue/write_cache reads "write back" gets a flush per fsync; one reading "write through"
 *   gets none (the block layer strips REQ_PREFLUSH and REQ_FUA before a request exists), so its floor is labelled
 *   "no volatile cache: no drive flush" and can never back a drive-flush sentence. The kernel's view must agree with
 *   the drive's own report (NVMe Identify Controller VWC bit 0, read through NVME_IOCTL_ADMIN_CMD on the controller's
 *   character device; SCSI MODE SENSE(10) caching page WCE through SG_IO -- not sd's cache_type, which is the kernel's
 *   own copy; virtio_blk cache_type, from the device config), else the run is refused. On a VM (cpuinfo hypervisor
 *   flag, /sys/hypervisor, DMI) a write-back leaf's floor is "virtual drive flush: reach to media unknown".
 *
 * Refusals (rc 2, nothing left behind):
 *   - the clocksource is not tsc or arch_sys_counter;
 *   - D's filesystem is not ext4, xfs or btrfs: D's mount is found by statx(STATX_MNT_ID) matched to mountinfo field
 *     1 (never by path prefix, which a hidden mount can win), and the mount table's fstype and statfs's magic must
 *     agree, else "cannot determine" also refuses;
 *   - the flush path: D's mount, then through each loop device the mount holding its backing file (up to 3 nested
 *     loops, 4 layers; more refuses). At every layer: the fstype is ext4, xfs or btrfs; a barrier switched off
 *     (nobarrier, barrier=0) refuses, as does any mount option outside a per-fstype allowlist of known-safe options
 *     (an external log, journal or realtime device among them: logdev=, rtdev=, journal_dev=, journal_path=); an
 *     ext4 layer must have an internal journal (/proc/fs/jbd2/<dev>-N) and records data=, commit= and
 *     journal_async_commit from /proc/fs/ext4/<dev>/options; a btrfs layer must have exactly one device; each loop's
 *     backing path must name the inode LOOP_GET_STATUS64 reports (a lazy unmount or a replaced file refuses), its
 *     backing file must be live, and a loop that reads write-through refuses (no flush reaches its backing file);
 *     a layer whose device, queue/write_cache or options cannot be read refuses too;
 *   - the leaf's driver is not nvme, sd or virtio_blk: brd, zram, nbd, dm, md and anything unknown refuse. brd is
 *     accepted only with V3FLOOR_BRD=1 (set by firecheck.sh's brd cells, refused by run.sh in a bound batch), and its
 *     summary says "fire-check only, never credited";
 *   - the kernel's write_cache disagrees with the drive's own report, or that report cannot be read;
 *   - nice != 0, a scheduling policy other than SCHED_OTHER, or an I/O priority other than the default;
 *   - D's path is too long, the out dir exists, n is not a positive integer, an unknown or repeated arm;
 *   - a flushed arm selected without nosync25, or every selected flushed arm refused (see below);
 *   - a file or directory an arm would create already exists in D (a leftover, or a planted symlink).
 * The FICLONE arms run on XFS and btrfs only. On ext4 every selected FICLONE arm is REFUSED, never skipped silently:
 *   it does not run, and the reason (the filesystem and the errno a trial FICLONE returned there) is written to
 *   summary.json "refused_arms" and to stderr; the remaining arms run (cfr2b runs on ext4: copy_file_range copies).
 *   On xfs or btrfs a failed FICLONE is an error (rc 1), and an ext4 that accepts the trial FICLONE is an error too.
 * Fire-check flags (refused unless V3FLOOR_FIRECHECK=1, which only firecheck.sh sets; run.sh refuses it):
 *   --mutant-nosync skips every flush in the flushed arms: a fire-check that the control above fails it;
 *   --trace-clock reads the clock with a real clock_gettime syscall, so strace sees each timed window's edges;
 *   --crash-op ARM sets up one copy arm, runs op 0 and exits without teardown, for crash.sh (--crash-aim also appends
 *     1 B to and fsyncs an unrelated file between the create and the copy). It checks only the filesystem type.
 *
 * Blind spots, stated: foreign I/O on the device and cgroup I/O throttling are not observed here (run.sh's stamps
 *   record /proc/diskstats and PSI around the batch); device-mapper, md and network block layers below D are not
 *   followed (only loop devices are; such a leaf refuses on its driver), but network disks that present as SCSI
 *   (iSCSI, FC, SRP) use sd and are accepted. NVMe VWC says a cache is present, not enabled (Get Features 06h needs
 *   CAP_SYS_ADMIN). The VM test can miss a hypervisor that hides itself. On Linux an fsync is per inode plus the
 *   filesystem's journal, not a device-wide cache flush as F_FULLFSYNC is on Apple.
 * Also refused: LD_PRELOAD, LD_AUDIT or LD_LIBRARY_PATH set (outside the fire-check), and inode flags on D or an arm
 *   file outside {extents, directory index} (chattr +S/+D/+j/+C/+c/+x change what an fsync does).
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <linux/loop.h>
#include <linux/nvme_ioctl.h>
#include <scsi/sg.h>
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
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

#ifndef FICLONE
#define FICLONE _IOW(0x94, 9, int)
#endif
#ifndef STATX_MNT_ID
#define STATX_MNT_ID 0x00001000U
#endif
#define MAGIC_EXT4 0xEF53u
#define MAGIC_XFS 0x58465342u
#define MAGIC_BTRFS 0x9123683Eu
#define IOPRIO_WHO_PROC 1
#define IOPRIO_SHIFT 13
#define MAXLAYERS 4 /* 3 nested loops and the leaf */
#define OPTS 4096
#define MAXCTRL 8
#define APPEND_BASE 4096 /* the append arms' files start one block long */

enum { APPEND25, APPEND64, OW4K, OW64K, OW1M, CLONE2B, CFR2B, CLONE1B, FDATASYNC4K, CLEAN, NOSYNC25, NARMS };
static const char *NAMES[NARMS] = {"append25", "append64", "ow4k", "ow64k", "ow1m", "clone2b", "cfr2b", "clone1b",
                                   "fdatasync4k", "clean", "nosync25"};
#define MIB (1u << 20)
static int gated(int a) { return a <= CFR2B; }                       /* the control gates them */
static int flushed(int a) { return a <= FDATASYNC4K; }               /* a flush the mutant removes */
static int is_ficlone(int a) { return a == CLONE1B || a == CLONE2B; } /* refused on ext4 */
static int is_copy(int a) { return a == CLONE1B || a == CLONE2B || a == CFR2B; }
static int is_append(int a) { return a == APPEND25 || a == APPEND64 || a == NOSYNC25; }
static size_t append_rec(int a) { return a == APPEND64 ? 64 : 25; }
static const char *report_only_why(int a) {
    return a == CLONE1B ? "report-only: on Linux a directory fsync does not guarantee a FICLONE durable (PREREG crash "
                          "model; review 2 item 3; crash.sh loses it on btrfs and on XFS-aimed); clone2b is the clone arm "
                          "that survived every crash case"
         : a == FDATASYNC4K ? "report-only extra: ow4k with fdatasync in place of fsync"
         : a == CLEAN ? "the dirty/clean control, never mutated"
         : NULL;
}

static const char *DIR_;
static dev_t DIR_DEV;
static int MUTANT, TRACE_CLOCK, CRASH_AIM;
static char buf[MIB];

typedef struct {
    int fd;
    off_t off, cap;
    size_t rec;
    int dfd;   /* copy arms: the clones' directory */
    int srcfd; /* copy arms: the durable source */
    int aimfd; /* crash mode, --crash-aim: the unrelated file */
    char cdir[PATH_MAX];
    char src[PATH_MAX];
} armst;

static void die(const char *what) { fprintf(stderr, "v3floor: %s: %s\n", what, strerror(errno)); exit(1); }
static void refuse(const char *fmt, ...) __attribute__((format(printf, 1, 2), noreturn));
static void refuse(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fputs("v3floor: REFUSED: ", stderr);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
    exit(2);
}
static char WHY[3 * PATH_MAX + 2048];
static const char *whyf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static const char *whyf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(WHY, sizeof WHY, fmt, ap);
    va_end(ap);
    return WHY;
}
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
        else if (*p < 0x20 || *p >= 0x7f) fprintf(f, "\\u%04x", *p); /* bytes, not code points: valid JSON always */
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

static int read_line(const char *p, char *out, size_t cap) { /* 0 on success */
    FILE *f = fopen(p, "r");
    if (!f) return -1;
    int ok = fgets(out, (int)cap, f) != NULL;
    fclose(f);
    if (!ok) return -1;
    out[strcspn(out, "\n")] = 0;
    return 0;
}
static int read_all(const char *p, char *out, size_t cap) { /* 0 on success; the whole file must fit */
    FILE *f = fopen(p, "r");
    if (!f) return -1;
    size_t n = fread(out, 1, cap - 1, f);
    int more = fgetc(f) != EOF, err = ferror(f);
    fclose(f);
    if (err || more) return -1;
    out[n] = 0;
    return 0;
}
static int link_base(const char *p, char *out, size_t cap) { /* basename of a symlink's target; 0 on success */
    char t[PATH_MAX];
    ssize_t r = readlink(p, t, sizeof t - 1);
    if (r < 0) return -1;
    t[r] = 0;
    const char *b = strrchr(t, '/');
    b = b ? b + 1 : t;
    if (strlen(b) >= cap) return -1;
    memcpy(out, b, strlen(b) + 1);
    return 0;
}
static int copy(char *dst, size_t cap, const char *src) { /* 1 when it did not fit */
    size_t l = strlen(src);
    if (l >= cap) { memcpy(dst, src, cap - 1); dst[cap - 1] = 0; return 1; }
    memcpy(dst, src, l + 1);
    return 0;
}
static int starts(const char *s, const char *p) { return !strncmp(s, p, strlen(p)); }
static int all_digits(const char *s) {
    if (!*s) return 0;
    for (; *s; s++) if (*s < '0' || *s > '9') return 0;
    return 1;
}

/* ---- SHA-256 of /proc/self/exe (FIPS 180-4), so summary.json names the binary that ran ---- */
static const uint32_t K256[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};
#define ROR32(x, n) (((x) >> (n)) | ((x) << (32 - (n))))
static void sha256_block(uint32_t h[8], const unsigned char *p) {
    uint32_t w[64];
    for (int i = 0; i < 16; i++)
        w[i] = (uint32_t)p[4 * i] << 24 | (uint32_t)p[4 * i + 1] << 16 | (uint32_t)p[4 * i + 2] << 8 | (uint32_t)p[4 * i + 3];
    for (int i = 16; i < 64; i++) {
        uint32_t s0 = ROR32(w[i - 15], 7) ^ ROR32(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = ROR32(w[i - 2], 17) ^ ROR32(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], k = h[7];
    for (int i = 0; i < 64; i++) {
        uint32_t S1 = ROR32(e, 6) ^ ROR32(e, 11) ^ ROR32(e, 25), ch = (e & f) ^ (~e & g);
        uint32_t t1 = k + S1 + ch + K256[i] + w[i];
        uint32_t S0 = ROR32(a, 2) ^ ROR32(a, 13) ^ ROR32(a, 22), mj = (a & b) ^ (a & c) ^ (b & c);
        uint32_t t2 = S0 + mj;
        k = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
    }
    h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += k;
}
static void sha256_file(const char *path, char hex[65]) {
    uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) die("open /proc/self/exe");
    static unsigned char blk[1 << 16];
    unsigned char tail[128];
    uint64_t total = 0;
    size_t have = 0; /* bytes of a partial block carried in tail */
    for (;;) {
        ssize_t r = read(fd, blk, sizeof blk);
        if (r < 0) die("read /proc/self/exe");
        if (r == 0) break;
        total += (uint64_t)r;
        size_t off = 0;
        if (have) {
            size_t take = 64 - have < (size_t)r ? 64 - have : (size_t)r;
            memcpy(tail + have, blk, take);
            have += take;
            off = take;
            if (have == 64) { sha256_block(h, tail); have = 0; }
        }
        for (; off + 64 <= (size_t)r; off += 64) sha256_block(h, blk + off);
        if (off < (size_t)r) { memcpy(tail + have, blk + off, (size_t)r - off); have += (size_t)r - off; }
    }
    if (close(fd) != 0) die("close /proc/self/exe");
    tail[have++] = 0x80;
    size_t padto = have <= 56 ? 56 : 120;
    memset(tail + have, 0, padto - have);
    uint64_t bits = total * 8;
    for (int i = 0; i < 8; i++) tail[padto + (size_t)i] = (unsigned char)(bits >> (56 - 8 * i));
    sha256_block(h, tail);
    if (padto == 120) sha256_block(h, tail + 64);
    for (int i = 0; i < 8; i++) snprintf(hex + 8 * i, 9, "%08x", h[i]);
}

/* ---- arm files ---- */
/* Inode flags an arm file or D may carry (fresh review M4): extents and a directory's hashed index. Anything else
 * (per-file sync S, dirsync D, data journaling j, no-COW C, compression c, DAX x, ...) changes what an fsync does
 * without showing in any mount option, so it refuses. */
#define FLAGS_OK ((unsigned)FS_EXTENT_FL | (unsigned)FS_INDEX_FL)
static const char *inode_flags_problem(int fd, const char *what) {
    int fl = 0;
    if (ioctl(fd, FS_IOC_GETFLAGS, &fl) != 0) return whyf("cannot read the inode flags of %s: %s", what, strerror(errno));
    unsigned bad = (unsigned)fl & ~FLAGS_OK;
    if (bad)
        return whyf("%s carries inode flags 0x%x (all 0x%x) outside the allowlist (extents, directory index): per-file sync, "
                    "dirsync, data journaling, no-COW, compression or DAX change what an fsync does", what, bad, (unsigned)fl);
    return NULL;
}
static uint64_t DIR_MNT;
static void dir_identity(void) { /* D's device and mount id, from one statx */
    struct statx sx;
    if (statx(AT_FDCWD, DIR_, 0, STATX_BASIC_STATS | STATX_MNT_ID, &sx) != 0 || !(sx.stx_mask & STATX_MNT_ID)) die("statx D");
    DIR_DEV = makedev(sx.stx_dev_major, sx.stx_dev_minor);
    DIR_MNT = sx.stx_mnt_id;
}
static void check_new_fd(int fd, const char *p, int want_dir) { /* a new file or directory on D's device and mount */
    struct statx sx;
    if (statx(fd, "", AT_EMPTY_PATH, STATX_BASIC_STATS | STATX_MNT_ID, &sx) != 0) die(p);
    dev_t dev = makedev(sx.stx_dev_major, sx.stx_dev_minor);
    int kind_ok = want_dir ? S_ISDIR(sx.stx_mode) : S_ISREG(sx.stx_mode);
    if (dev != DIR_DEV || !(sx.stx_mask & STATX_MNT_ID) || sx.stx_mnt_id != DIR_MNT || !kind_ok) {
        fprintf(stderr, "v3floor: %s is not a new %s on D's filesystem and mount (dev %llx mount %llu, D's %llx %llu)\n", p,
                want_dir ? "directory" : "file", (unsigned long long)dev, (unsigned long long)sx.stx_mnt_id,
                (unsigned long long)DIR_DEV, (unsigned long long)DIR_MNT);
        exit(1);
    }
    const char *why = inode_flags_problem(fd, p);
    if (why) { fprintf(stderr, "v3floor: %s\n", why); exit(1); }
}
static int mkfile(const char *name) {
    char p[PATH_MAX];
    pathf(p, sizeof p, "%s/%s", DIR_, name);
    int fd = open(p, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, 0644);
    if (fd < 0) die(p);
    check_new_fd(fd, p, 0);
    return fd;
}
static void prealloc(int fd, off_t bytes) {
    for (off_t o = 0; o < bytes; o += MIB) if (pwrite(fd, buf, MIB, o) != (ssize_t)MIB) die("prealloc");
    setup_sync(fd, "prealloc fsync");
}

static void setup(int a, armst *s, int crash) {
    memset(s, 0, sizeof *s);
    s->fd = s->dfd = s->srcfd = s->aimfd = -1;
    char nm[64];
    if (is_append(a)) {
        s->fd = mkfile(NAMES[a]);
        s->rec = append_rec(a);
        /* one whole 4 KiB block first: the timed appends then never cross btrfs's 2 KiB inline-data limit, which
         * stepped append25 and append64 up mid-run on every btrfs cell of run 37476867864 (fresh review M3) */
        if (pwrite(s->fd, buf, APPEND_BASE, 0) != APPEND_BASE) die("append init");
        setup_sync(s->fd, "append init fsync");
        s->off = APPEND_BASE;
    } else if (a == OW4K || a == OW64K || a == OW1M || a == FDATASYNC4K) {
        s->fd = mkfile(NAMES[a]);
        s->rec = a == OW64K ? 65536 : a == OW1M ? MIB : 4096;
        s->cap = a == OW1M ? 128 * (off_t)MIB : 16 * (off_t)MIB;
        prealloc(s->fd, s->cap);
    } else if (is_copy(a)) {
        snprintf(nm, sizeof nm, "%s.src", NAMES[a]);
        pathf(s->src, sizeof s->src, "%s/%s", DIR_, nm);
        s->srcfd = mkfile(nm);
        prealloc(s->srcfd, MIB);
        pathf(s->cdir, sizeof s->cdir, "%s/%s.clones", DIR_, NAMES[a]);
        if (mkdir(s->cdir, 0755) != 0) die("clone dir (must not exist)");
        s->dfd = open(s->cdir, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
        if (s->dfd < 0) die("clone dir open");
        check_new_fd(s->dfd, s->cdir, 1);
        setup_sync(s->dfd, "clone dir fsync");
        if (crash && CRASH_AIM) {
            snprintf(nm, sizeof nm, "%s.aim", NAMES[a]);
            s->aimfd = mkfile(nm);
            if (pwrite(s->aimfd, buf, 4096, 0) != 4096) die("aim init");
            setup_sync(s->aimfd, "aim init fsync");
        }
    } else if (a == CLEAN) {
        s->fd = mkfile(NAMES[a]);
        prealloc(s->fd, MIB);
    }
}

static void op(int a, armst *s, uint64_t i) {
    buf[i % 4096] ^= 1; /* every write differs */
    if (is_append(a)) {
        if (pwrite(s->fd, buf, s->rec, s->off) != (ssize_t)s->rec) die("append");
        s->off += (off_t)s->rec;
        if (a != NOSYNC25) barrier(s->fd);
    } else if (a == OW4K || a == OW64K || a == OW1M || a == FDATASYNC4K) {
        if (s->off + (off_t)s->rec > s->cap) s->off = 0;
        if (pwrite(s->fd, buf, s->rec, s->off) != (ssize_t)s->rec) die("overwrite");
        s->off += (off_t)s->rec;
        if (a == FDATASYNC4K) { if (!MUTANT && fdatasync(s->fd) == -1) die("fdatasync"); }
        else barrier(s->fd);
    } else if (is_copy(a)) {
        char nm[32];
        snprintf(nm, sizeof nm, "c%llu", (unsigned long long)i);
        int fd = openat(s->dfd, nm, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0644);
        if (fd < 0) die("clone create");
        if (s->aimfd >= 0) { /* crash mode only: force the log past the create, before the copy. An APPEND, so the
                              * aim inode's size changes and its fsync must force the log (run 37475543956: an
                              * overwrite inside one coarse timestamp left the inode clean and forced nothing) */
            if (pwrite(s->aimfd, buf, 1, 4096) != 1 || fsync(s->aimfd) != 0) die("aim fsync");
        }
        if (a == CFR2B) {
            off64_t oin = 0;
            ssize_t r = copy_file_range(s->srcfd, &oin, fd, NULL, MIB, 0);
            if (r != (ssize_t)MIB) { if (r >= 0) errno = EIO; die("copy_file_range (failed or short)"); }
        } else if (ioctl(fd, FICLONE, s->srcfd) != 0) die("ioctl FICLONE");
        if (a != CLONE1B) barrier(fd);
        if (close(fd) != 0) die("clone close");
        barrier(s->dfd);
    } else if (a == CLEAN) {
        if (fsync(s->fd) == -1) die("clean fsync"); /* the control is never mutated */
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

/* ---- mounts: a path's own mount, by statx(STATX_MNT_ID) and mountinfo field 1 ---- */
typedef struct {
    uint64_t id;
    char real[PATH_MAX], mnt[PATH_MAX], root[PATH_MAX], fstype[64], source[PATH_MAX], mopts[OPTS], sopts[OPTS], dev[32];
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

static const char *mount_of(const char *path, mrec *m) { /* NULL when found, else why not */
    memset(m, 0, sizeof *m);
    if (!realpath(path, m->real)) return whyf("realpath(%s): %s", path, strerror(errno));
    struct statx sx;
    if (statx(AT_FDCWD, path, 0, STATX_MNT_ID, &sx) != 0) return whyf("statx(%s): %s", path, strerror(errno));
    if (!(sx.stx_mask & STATX_MNT_ID)) return whyf("statx(%s) returned no mount id", path);
    m->id = sx.stx_mnt_id;
    FILE *f = fopen("/proc/self/mountinfo", "r");
    if (!f) return "cannot read /proc/self/mountinfo";
    char *line = NULL;
    size_t cap = 0;
    int found = 0, trunc = 0;
    while (getline(&line, &cap, f) > 0) {
        line[strcspn(line, "\n")] = 0;
        char *fl[64];
        int nf = 0;
        for (char *t = strtok(line, " "); t && nf < 64; t = strtok(NULL, " ")) fl[nf++] = t;
        uint64_t id;
        if (nf < 1 || parse_u64(fl[0], &id) != 0 || id != m->id) continue;
        int dash = -1;
        for (int k = 6; k < nf; k++) if (!strcmp(fl[k], "-")) { dash = k; break; }
        if (dash < 0 || dash + 2 >= nf) { found = -1; break; }
        trunc |= copy(m->dev, sizeof m->dev, fl[2]);
        trunc |= copy(m->root, sizeof m->root, fl[3]);
        trunc |= copy(m->mnt, sizeof m->mnt, fl[4]);
        trunc |= copy(m->mopts, sizeof m->mopts, fl[5]);
        trunc |= copy(m->fstype, sizeof m->fstype, fl[dash + 1]);
        trunc |= copy(m->source, sizeof m->source, fl[dash + 2]);
        trunc |= copy(m->sopts, sizeof m->sopts, dash + 3 < nf ? fl[dash + 3] : "");
        unescape(m->root);
        unescape(m->mnt);
        unescape(m->source);
        found = 1;
        break;
    }
    free(line);
    fclose(f);
    if (found < 0) return whyf("mount id %llu's mountinfo line is malformed", (unsigned long long)m->id);
    if (!found) return whyf("mount id %llu (statx of %s) is not in /proc/self/mountinfo", (unsigned long long)m->id, path);
    if (trunc) return whyf("mount id %llu's record is too long to read whole", (unsigned long long)m->id);
    return NULL;
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

/* Known-safe mount options, an allowlist per fstype (review 2 item 15): an entry ending in '=' takes any value.
 * Anything else refuses, so an option nobody reviewed is never assumed to keep the flush. */
static const char *GENERIC_OPTS[] = {"rw", "relatime", "noatime", "nodiratime", "strictatime", "lazytime", "nosuid",
                                     "nodev", "noexec", "nosymfollow", "seclabel", NULL};
static const char *EXT4_OPTS[] = {
    "errors=", "commit=", "data=ordered", "data=writeback", "journal_async_commit", "journal_checksum", "discard",
    "nodiscard", "delalloc", "nodelalloc", "dioread_nolock", "dioread_lock", "auto_da_alloc", "noauto_da_alloc",
    "user_xattr", "nouser_xattr", "acl", "noacl", "noquota", "quota", "usrquota", "grpquota", "stripe=",
    "inode_readahead_blks=", "init_itable=", "noinit_itable", "max_batch_time=", "min_batch_time=", "i_version",
    "prefetch_block_bitmaps", "no_prefetch_block_bitmaps", "block_validity", "noblock_validity", "bsddf", "minixdf",
    "grpid", "nogrpid", "bsdgroups", "sysvgroups", "resuid=", "resgid=", "barrier", "barrier=1", "journal_ioprio=",
    "data_err=abort", "nowarn_on_error", "warn_on_error", "max_dir_size_kb=", "mb_optimize_scan=", "dax=never",
    "nombcache", NULL};
static const char *XFS_OPTS[] = {"attr2", "inode64", "inode32", "logbufs=", "logbsize=", "noquota", "quota", "usrquota",
                                 "uquota", "grpquota", "gquota", "prjquota", "pquota", "sunit=", "swidth=", "largeio",
                                 "nolargeio", "allocsize=", "nouuid", "discard", "nodiscard", "ikeep", "noikeep",
                                 "filestreams", "grpid", "nogrpid", "bsdgroups", "sysvgroups", "noalign", NULL};
static const char *BTRFS_OPTS[] = {"ssd", "nossd", "ssd_spread", "nossd_spread", "discard", "discard=async",
                                   "discard=sync", "nodiscard", "space_cache", "space_cache=v1", "space_cache=v2",
                                   "nospace_cache", "subvolid=", "subvol=", "compress=", "compress-force=", "commit=",
                                   "autodefrag", "noautodefrag", "datacow", "datasum", "user_subvol_rm_allowed", "acl",
                                   "noacl", "treelog", "flushoncommit", "noflushoncommit", "max_inline=",
                                   "thread_pool=", "skip_balance", "enospc_debug", "noenospc_debug", "barrier", NULL};
static int opt_in(const char *tok, const char **list) {
    for (int i = 0; list[i]; i++) {
        size_t l = strlen(list[i]);
        if (list[i][l - 1] == '=' ? !strncmp(tok, list[i], l) : !strcmp(tok, list[i])) return 1;
    }
    return 0;
}

/* ---- the flush path: D's mount, then through each loop device the mount holding its backing file ---- */
typedef struct {
    mrec m;
    char path[PATH_MAX];    /* the path looked up at this layer: D, or the previous loop's backing file */
    char sys[PATH_MAX];     /* /sys dir of the mount's block device (a partition or a whole disk) */
    char devname[64];       /* its basename (sda1, loop0, nvme0n1p1) */
    char disk[PATH_MAX];    /* the whole disk's /sys dir */
    char diskname[64];
    char diskdev[32];       /* major:minor of the whole disk */
    char wc[32], fua[16];
    char backing[PATH_MAX]; /* a loop device's backing file, else "" */
    char loopdio[8];
    unsigned long long lo_dev, lo_ino;
    char ext4_data[32], ext4_commit[16], jbd2[96];
    int ext4_async, btrfs_devices;
} layer;
static layer L[MAXLAYERS];
static int NL;

static const char *find_sys(layer *l, int k) {
    char p[PATH_MAX];
    pathf(p, sizeof p, "/sys/dev/block/%s", l->m.dev);
    if (!realpath(p, l->sys)) { /* btrfs reports an anonymous 0:N: fall back to the source's name */
        const char *b = strrchr(l->m.source, '/');
        pathf(p, sizeof p, "/sys/class/block/%s", b ? b + 1 : l->m.source);
        if (!starts(l->m.source, "/dev/") || !realpath(p, l->sys))
            return whyf("layer %d (%s): cannot find the device of %s (dev %s) in /sys", k, l->m.mnt, l->m.source, l->m.dev);
    }
    const char *b = strrchr(l->sys, '/');
    if (copy(l->devname, sizeof l->devname, b ? b + 1 : l->sys)) return whyf("layer %d: device name too long", k);
    pathf(p, sizeof p, "%s/partition", l->sys);
    if (access(p, F_OK) == 0) {
        char t[PATH_MAX];
        pathf(t, sizeof t, "%s", l->sys);
        pathf(l->disk, sizeof l->disk, "%s", dirname(t));
    } else pathf(l->disk, sizeof l->disk, "%s", l->sys);
    b = strrchr(l->disk, '/');
    if (copy(l->diskname, sizeof l->diskname, b ? b + 1 : l->disk)) return whyf("layer %d: disk name too long", k);
    pathf(p, sizeof p, "%s/dev", l->disk);
    if (read_line(p, l->diskdev, sizeof l->diskdev) != 0) return whyf("layer %d: cannot read %s", k, p);
    pathf(p, sizeof p, "%s/queue/write_cache", l->disk);
    if (read_line(p, l->wc, sizeof l->wc) != 0) return whyf("layer %d (%s): cannot read %s", k, l->diskname, p);
    pathf(p, sizeof p, "%s/queue/fua", l->disk);
    if (read_line(p, l->fua, sizeof l->fua) != 0) return whyf("layer %d (%s): cannot read %s", k, l->diskname, p);
    return NULL;
}

static const char *loop_verify(layer *l, int k) {
    char node[96];
    unsigned maj = 0, mnr = 0;
    if (sscanf(l->diskdev, "%u:%u", &maj, &mnr) != 2) return whyf("layer %d: bad dev %s", k, l->diskdev);
    snprintf(node, sizeof node, "/dev/%s", l->diskname);
    struct stat sb;
    if (stat(node, &sb) != 0 || !S_ISBLK(sb.st_mode) || major(sb.st_rdev) != maj || minor(sb.st_rdev) != mnr)
        return whyf("layer %d: %s is not block device %u:%u", k, node, maj, mnr);
    int fd = open(node, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return whyf("layer %d: cannot open %s to verify its backing inode with LOOP_GET_STATUS64: %s (grant read access, "
                    "e.g. setfacl -m u:$USER:r %s)", k, node, strerror(errno), node);
    struct loop_info64 li;
    memset(&li, 0, sizeof li);
    int r = ioctl(fd, LOOP_GET_STATUS64, &li), e = errno;
    close(fd);
    if (r != 0) return whyf("layer %d: LOOP_GET_STATUS64 on %s: %s", k, node, strerror(e));
    struct stat bs;
    if (stat(l->backing, &bs) != 0)
        return whyf("layer %d: loop %s's backing path %s cannot be stat'd: %s (a lazily unmounted or moved backing "
                    "filesystem)", k, l->diskname, l->backing, strerror(errno));
    if ((unsigned long long)bs.st_dev != (unsigned long long)li.lo_device ||
        (unsigned long long)bs.st_ino != (unsigned long long)li.lo_inode)
        return whyf("layer %d: %s is not the loop's backing inode: the path names dev %llx inode %llu, %s holds dev %llx "
                    "inode %llu (a lazy unmount, or a replaced file)", k, l->backing, (unsigned long long)bs.st_dev,
                    (unsigned long long)bs.st_ino, l->diskname, (unsigned long long)li.lo_device,
                    (unsigned long long)li.lo_inode);
    l->lo_dev = li.lo_device;
    l->lo_ino = li.lo_inode;
    return NULL;
}

static const char *flush_path(const char *dir) { /* NULL when every layer was read, else why not */
    char cur[PATH_MAX];
    pathf(cur, sizeof cur, "%s", dir);
    for (NL = 0; NL < MAXLAYERS; NL++) {
        layer *l = &L[NL];
        memset(l, 0, sizeof *l);
        pathf(l->path, sizeof l->path, "%s", cur);
        const char *why = mount_of(cur, &l->m);
        if (why) { /* why points into WHY: copy it before formatting into WHY again */
            static char t[sizeof WHY];
            copy(t, sizeof t, why);
            return whyf("layer %d (%s): %s", NL, cur, t);
        }
        if (strcmp(l->m.fstype, "ext4") && strcmp(l->m.fstype, "xfs") && strcmp(l->m.fstype, "btrfs"))
            return whyf("layer %d (%s) is on %s (mount %s), not ext4, xfs or btrfs", NL, cur, l->m.fstype, l->m.mnt);
        if ((why = find_sys(l, NL))) return why;
        char p[PATH_MAX];
        pathf(p, sizeof p, "%s/loop/backing_file", l->disk);
        if (access(p, F_OK) != 0) { NL++; return NULL; } /* not a loop: the leaf */
        if (read_line(p, l->backing, sizeof l->backing) != 0 || !l->backing[0] || strstr(l->backing, " (deleted)"))
            return whyf("layer %d (%s): cannot read a live backing file for loop %s (%s)", NL, l->m.mnt, l->diskname,
                        l->backing);
        pathf(p, sizeof p, "%s/loop/dio", l->disk);
        if (read_line(p, l->loopdio, sizeof l->loopdio) != 0) snprintf(l->loopdio, sizeof l->loopdio, "?");
        if ((why = loop_verify(l, NL))) return why;
        pathf(cur, sizeof cur, "%s", l->backing);
    }
    return whyf("more than %d nested loop devices (%d layers): the flush path is followed through at most %d",
                MAXLAYERS - 1, MAXLAYERS, MAXLAYERS - 1);
}

static const char *layer_fs_checks(layer *l, int k) { /* options allowlist, ext4 journal, btrfs device count */
    const char **fsl = !strcmp(l->m.fstype, "ext4") ? EXT4_OPTS : !strcmp(l->m.fstype, "xfs") ? XFS_OPTS : BTRFS_OPTS;
    for (int pass = 0; pass < 2; pass++) {
        char tmp[OPTS], *sv = NULL;
        copy(tmp, sizeof tmp, pass ? l->m.sopts : l->m.mopts);
        for (char *t = strtok_r(tmp, ",", &sv); t; t = strtok_r(NULL, ",", &sv)) {
            if (!strcmp(t, "nobarrier") || !strcmp(t, "barrier=0")) continue; /* the barrier check names these */
            if (starts(t, "logdev=") || starts(t, "rtdev=") || starts(t, "journal_dev=") || starts(t, "journal_path="))
                return whyf("layer %d (%s on %s at %s): '%s' is an external log, journal or realtime device; the flush "
                            "path follows only the data device", k, l->m.fstype, l->m.source, l->m.mnt, t);
            if (opt_in(t, GENERIC_OPTS) || (pass && opt_in(t, fsl))) continue;
            return whyf("layer %d (%s on %s at %s): mount option '%s' is not in the known-safe list for %s (an allowlist: "
                        "an option it does not know is refused, never assumed to keep the flush)", k, l->m.fstype,
                        l->m.source, l->m.mnt, t, l->m.fstype);
        }
    }
    char p[PATH_MAX];
    if (!strcmp(l->m.fstype, "ext4")) {
        static char o[16384];
        pathf(p, sizeof p, "/proc/fs/ext4/%s/options", l->devname);
        if (read_all(p, o, sizeof o) != 0) return whyf("layer %d: cannot read %s whole", k, p);
        snprintf(l->ext4_data, sizeof l->ext4_data, "?");
        snprintf(l->ext4_commit, sizeof l->ext4_commit, "?");
        for (char *sv = NULL, *t = strtok_r(o, "\n", &sv); t; t = strtok_r(NULL, "\n", &sv)) {
            if (starts(t, "data=")) copy(l->ext4_data, sizeof l->ext4_data, t + 5);
            else if (starts(t, "commit=")) copy(l->ext4_commit, sizeof l->ext4_commit, t + 7);
            else if (!strcmp(t, "journal_async_commit")) l->ext4_async = 1;
            else if (!strcmp(t, "nobarrier"))
                return whyf("the flush path has nobarrier: layer %d, %s (%s says nobarrier)", k, l->m.mnt, p);
            /* the same allowlist over every effective option, defaults included: mountinfo omits options that match
             * the superblock's defaults (tune2fs -o / -E mount_opts), this file does not (fresh review L3) */
            if (!opt_in(t, GENERIC_OPTS) && !opt_in(t, EXT4_OPTS) && strcmp(t, "nobarrier"))
                return whyf("layer %d (ext4 on %s at %s): effective option '%s' (%s) is not in the known-safe list for "
                            "ext4", k, l->m.source, l->m.mnt, t, p);
        }
        DIR *d = opendir("/proc/fs/jbd2");
        if (!d) return whyf("layer %d: cannot list /proc/fs/jbd2", k);
        size_t dl = strlen(l->devname);
        for (struct dirent *e; (e = readdir(d));)
            if (!strncmp(e->d_name, l->devname, dl) && e->d_name[dl] == '-' && all_digits(e->d_name + dl + 1))
                copy(l->jbd2, sizeof l->jbd2, e->d_name);
        closedir(d);
        if (!l->jbd2[0])
            return whyf("layer %d: ext4 on %s has no internal journal (/proc/fs/jbd2 has no %s-N): an external "
                        "journal_dev, or no journal", k, l->devname, l->devname);
    } else if (!strcmp(l->m.fstype, "btrfs")) {
        const char *b = strrchr(l->m.source, '/');
        const char *name = b ? b + 1 : l->m.source;
        DIR *d = opendir("/sys/fs/btrfs");
        if (!d) return whyf("layer %d: cannot list /sys/fs/btrfs", k);
        int found = 0;
        for (struct dirent *e; (e = readdir(d)) && !found;) {
            if (e->d_name[0] == '.') continue;
            char dp[PATH_MAX];
            pathf(dp, sizeof dp, "/sys/fs/btrfs/%s/devices/%s", e->d_name, name);
            if (access(dp, F_OK) != 0) continue;
            found = 1;
            pathf(dp, sizeof dp, "/sys/fs/btrfs/%s/devices", e->d_name);
            DIR *dd = opendir(dp);
            if (!dd) return whyf("layer %d: cannot list %s", k, dp);
            for (struct dirent *x; (x = readdir(dd));) l->btrfs_devices += x->d_name[0] != '.';
            closedir(dd);
        }
        closedir(d);
        if (!found) return whyf("layer %d: cannot find %s in /sys/fs/btrfs/*/devices", k, name);
        if (l->btrfs_devices != 1)
            return whyf("layer %d: multi-device btrfs (%d devices): the flush path follows one device", k, l->btrfs_devices);
    }
    return NULL;
}

/* ---- the leaf: its driver (an allowlist) and the drive's own cache report ---- */
typedef struct {
    char driver[64], kind[16]; /* kind: drive | brd */
    char report[32], report_source[PATH_MAX + 128], cache_type[64], model[64];
    char ctrls[MAXCTRL][32];
    int nctrl;
    char paths[MAXCTRL][64], pathdevs[MAXCTRL][32];
    int npath;
} leafinfo;
static leafinfo LEAF;

static void nvme_ctrls(const char *disk, leafinfo *li) {
    char p[PATH_MAX], r[PATH_MAX];
    pathf(p, sizeof p, "%s/device", disk);
    if (!realpath(p, r)) return;
    const char *b = strrchr(r, '/');
    b = b ? b + 1 : r;
    if (starts(b, "nvme") && all_digits(b + 4)) { copy(li->ctrls[li->nctrl++], sizeof li->ctrls[0], b); return; }
    if (!starts(b, "nvme-subsys")) return;
    DIR *d = opendir(r);
    if (!d) return;
    for (struct dirent *e; (e = readdir(d)) && li->nctrl < MAXCTRL;)
        if (starts(e->d_name, "nvme") && all_digits(e->d_name + 4)) copy(li->ctrls[li->nctrl++], sizeof li->ctrls[0], e->d_name);
    closedir(d);
}

static const char *nvme_vwc(const char *ctrl, int *vwc, char *model, size_t mcap) {
    char node[64], p[PATH_MAX], dv[32];
    snprintf(node, sizeof node, "/dev/%s", ctrl);
    pathf(p, sizeof p, "/sys/class/nvme/%s/dev", ctrl);
    unsigned maj = 0, mnr = 0;
    struct stat sb;
    if (read_line(p, dv, sizeof dv) != 0 || sscanf(dv, "%u:%u", &maj, &mnr) != 2 || stat(node, &sb) != 0 ||
        !S_ISCHR(sb.st_mode) || major(sb.st_rdev) != maj || minor(sb.st_rdev) != mnr)
        return whyf("%s is not the character device of controller %s (%s)", node, ctrl, dv);
    int fd = open(node, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return whyf("cannot read the drive's own cache report: open %s: %s (Identify Controller is an unprivileged "
                    "admin command once the node is readable: grant read access, e.g. setfacl -m u:$USER:r %s)", node,
                    strerror(errno), node);
    static unsigned char id[4096] __attribute__((aligned(4096)));
    struct nvme_admin_cmd c;
    memset(&c, 0, sizeof c);
    memset(id, 0, sizeof id);
    c.opcode = 0x06; /* Identify */
    c.addr = (uint64_t)(uintptr_t)id;
    c.data_len = sizeof id;
    c.cdw10 = 1; /* CNS 01h: the controller */
    int r = ioctl(fd, NVME_IOCTL_ADMIN_CMD, &c), e = errno;
    close(fd);
    if (r != 0) return whyf("cannot read the drive's own cache report: Identify Controller on %s: %s", node, r < 0 ? strerror(e) : "NVMe status");
    *vwc = id[525] & 1; /* VWC bit 0: a volatile write cache is PRESENT (whether it is enabled is Get Features 06h,
                         * which needs CAP_SYS_ADMIN: a stated blind spot) */
    size_t m = 0; /* MN, bytes 24-63, space padded */
    for (int k = 24; k < 64 && m + 1 < mcap; k++) model[m++] = (char)(id[k] >= 0x20 && id[k] < 0x7f ? id[k] : '?');
    while (m && model[m - 1] == ' ') m--;
    model[m] = 0;
    return NULL;
}

/* SCSI: the drive's own caching mode page (MODE SENSE(10), page 08h, current values; WCE is byte 2 bit 2), read
 * through SG_IO on a read-only fd -- a read-safe command the kernel allows any opener. sd's cache_type is NOT the
 * drive's report: it and queue/write_cache both derive from sd's cached WCE bit, so "temporary write back|through"
 * moves both together (fresh review H2). */
static const char *scsi_wce(const layer *l, int *wce) {
    char node[96];
    unsigned maj = 0, mnr = 0;
    struct stat sb;
    snprintf(node, sizeof node, "/dev/%s", l->diskname);
    if (sscanf(l->diskdev, "%u:%u", &maj, &mnr) != 2 || stat(node, &sb) != 0 || !S_ISBLK(sb.st_mode) ||
        major(sb.st_rdev) != maj || minor(sb.st_rdev) != mnr)
        return whyf("%s is not block device %s", node, l->diskdev);
    int fd = open(node, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0)
        return whyf("cannot read the drive's own cache report: open %s: %s (MODE SENSE needs read access, e.g. setfacl "
                    "-m u:$USER:r %s)", node, strerror(errno), node);
    unsigned char cdb[10] = {0x5A, 0x08, 0x08, 0, 0, 0, 0, 0, 0xFC, 0}; /* DBD=1, PC=current, page 08h, 252 B */
    unsigned char resp[252], sense[32];
    memset(resp, 0, sizeof resp);
    sg_io_hdr_t io;
    memset(&io, 0, sizeof io);
    io.interface_id = 'S';
    io.cmdp = cdb;
    io.cmd_len = sizeof cdb;
    io.dxferp = resp;
    io.dxfer_len = sizeof resp;
    io.dxfer_direction = SG_DXFER_FROM_DEV;
    io.sbp = sense;
    io.mx_sb_len = sizeof sense;
    io.timeout = 10000;
    int r = ioctl(fd, SG_IO, &io), e = errno;
    close(fd);
    if (r != 0) return whyf("cannot read the drive's own cache report: SG_IO MODE SENSE(10) on %s: %s", node, strerror(e));
    if ((io.info & SG_INFO_OK_MASK) != SG_INFO_OK || io.status || io.host_status || io.driver_status)
        return whyf("cannot read the drive's own cache report: MODE SENSE(10) on %s failed (status 0x%x host 0x%x driver "
                    "0x%x)", node, io.status, io.host_status, io.driver_status);
    unsigned len = ((unsigned)resp[0] << 8 | resp[1]) + 2, bdl = (unsigned)resp[6] << 8 | resp[7], off = 8 + bdl;
    if (off + 3 > len || off + 3 > sizeof resp || (resp[off] & 0x3F) != 0x08)
        return whyf("cannot read the drive's own cache report: no caching mode page in %s's MODE SENSE reply", node);
    *wce = (resp[off + 2] >> 2) & 1;
    return NULL;
}

static void virt_record(int *vm, int *flag, char *hyp, char *vendor, char *product, size_t cap) {
    static char ci[1 << 20];
    *flag = 0;
    if (read_all("/proc/cpuinfo", ci, sizeof ci) == 0 && (strstr(ci, " hypervisor ") || strstr(ci, " hypervisor\n"))) *flag = 1;
    if (read_line("/sys/hypervisor/type", hyp, cap) != 0) hyp[0] = 0;
    if (read_line("/sys/class/dmi/id/sys_vendor", vendor, cap) != 0) vendor[0] = 0;
    if (read_line("/sys/class/dmi/id/product_name", product, cap) != 0) product[0] = 0;
    *vm = *flag || hyp[0] || strstr(product, "Virtual") || strstr(product, "KVM") || strstr(product, "VMware") ||
          strstr(vendor, "QEMU") || strstr(vendor, "Amazon EC2") || strstr(vendor, "Google") || strstr(vendor, "Xen");
}

static const char *leaf_checks(const layer *l) {
    leafinfo *li = &LEAF;
    memset(li, 0, sizeof *li);
    char p[PATH_MAX];
    pathf(p, sizeof p, "%s/device/driver", l->disk);
    if (link_base(p, li->driver, sizeof li->driver) != 0) {
        pathf(p, sizeof p, "%s/device/device/driver", l->disk);
        if (link_base(p, li->driver, sizeof li->driver) != 0) li->driver[0] = 0;
    }
    nvme_ctrls(l->disk, li);
    if (!li->driver[0] && li->nctrl) {
        pathf(p, sizeof p, "/sys/class/nvme/%s/device/driver", li->ctrls[0]);
        if (link_base(p, li->driver, sizeof li->driver) != 0) li->driver[0] = 0;
    }
    pathf(p, sizeof p, "%s/multipath", l->disk);
    DIR *d = opendir(p);
    if (d) {
        for (struct dirent *e; (e = readdir(d)) && li->npath < MAXCTRL;) {
            if (e->d_name[0] == '.') continue;
            char q[PATH_MAX];
            copy(li->paths[li->npath], sizeof li->paths[0], e->d_name);
            pathf(q, sizeof q, "%s/%s/dev", p, e->d_name);
            if (read_line(q, li->pathdevs[li->npath], sizeof li->pathdevs[0]) != 0) snprintf(li->pathdevs[li->npath], sizeof li->pathdevs[0], "?");
            li->npath++;
        }
        closedir(d);
    }
    unsigned maj = 0, mnr = 0;
    sscanf(l->diskdev, "%u:%u", &maj, &mnr);
    if (!li->driver[0] && maj == 1 && starts(l->diskname, "ram")) {
        snprintf(li->driver, sizeof li->driver, "brd");
        const char *env = getenv("V3FLOOR_BRD");
        if (!env || strcmp(env, "1"))
            return whyf("the leaf %s is brd (a RAM disk, no drive): brd is fire-check only (V3FLOOR_BRD=1, set by "
                        "firecheck.sh's brd cells), never a measured device", l->diskname);
        snprintf(li->kind, sizeof li->kind, "brd");
        snprintf(li->report, sizeof li->report, "none (RAM)");
        snprintf(li->report_source, sizeof li->report_source, "brd has no cache and no report");
        if (strcmp(l->wc, "write through"))
            return whyf("the leaf %s is brd but its queue/write_cache reads '%s'", l->diskname, l->wc);
        return NULL;
    }
    if (strcmp(li->driver, "nvme") && strcmp(li->driver, "sd") && strcmp(li->driver, "virtio_blk"))
        return whyf("the leaf %s (dev %s, %s) has driver '%s', not in the leaf allowlist (nvme, sd, virtio_blk): brd, "
                    "zram, nbd, dm, md and unknown devices are refused", l->diskname, l->diskdev, l->disk,
                    li->driver[0] ? li->driver : "none");
    snprintf(li->kind, sizeof li->kind, "drive");
    if (!strcmp(li->driver, "sd")) {
        pathf(p, sizeof p, "%s/device/scsi_disk", l->disk);
        DIR *sd = opendir(p);
        if (!sd) return whyf("cannot read the drive's own cache report: no %s", p);
        int nfound = 0;
        char ct[64] = "";
        for (struct dirent *e; (e = readdir(sd));) {
            if (e->d_name[0] == '.') continue;
            char q[PATH_MAX];
            pathf(q, sizeof q, "%s/%s/cache_type", p, e->d_name);
            if (read_line(q, ct, sizeof ct) == 0) { nfound++; pathf(li->report_source, sizeof li->report_source, "%s", q); }
        }
        closedir(sd);
        if (nfound != 1) return whyf("cannot read sd's cache_type: %d cache_type files under %s", nfound, p);
        copy(li->cache_type, sizeof li->cache_type, ct);
        int wce = -1;
        const char *why = scsi_wce(l, &wce);
        if (why) return why;
        snprintf(li->report, sizeof li->report, "%s", wce ? "write back" : "write through");
        snprintf(li->report_source, sizeof li->report_source, "SCSI MODE SENSE(10) caching page WCE=%d via SG_IO on /dev/%s "
                 "(sd's cache_type, the kernel's copy, reads '%s')", wce, l->diskname, ct);
        pathf(p, sizeof p, "%s/device/model", l->disk);
        if (read_line(p, li->model, sizeof li->model) != 0) li->model[0] = 0;
    } else if (!strcmp(li->driver, "virtio_blk")) {
        char ct[32];
        pathf(li->report_source, sizeof li->report_source, "%s/cache_type", l->disk);
        if (read_line(li->report_source, ct, sizeof ct) != 0)
            return whyf("cannot read the drive's own cache report: %s", li->report_source);
        if (!strcmp(ct, "write back") || !strcmp(ct, "write through")) snprintf(li->report, sizeof li->report, "%s", ct);
        else return whyf("cannot parse the drive's cache report '%s' (%s)", ct, li->report_source);
    } else { /* nvme */
        if (!li->nctrl) return whyf("cannot map the leaf %s to an NVMe controller", l->diskname);
        int first = -1;
        for (int c = 0; c < li->nctrl; c++) {
            int vwc = 0;
            const char *why = nvme_vwc(li->ctrls[c], &vwc, li->model, sizeof li->model);
            if (why) return why;
            if (first >= 0 && vwc != first) return whyf("the controllers of %s disagree on VWC", l->diskname);
            first = vwc;
        }
        snprintf(li->report, sizeof li->report, "%s", first ? "write back" : "write through");
        snprintf(li->report_source, sizeof li->report_source, "NVMe Identify Controller VWC bit 0 via /dev/%s%s",
                 li->ctrls[0], li->nctrl > 1 ? " (and every other controller of the subsystem)" : "");
    }
    if (strcmp(l->wc, li->report))
        return whyf("the leaf %s's kernel queue/write_cache says '%s' but the drive reports '%s' (%s): one of them was "
                    "overridden, so whether a flush reaches the drive cannot be determined", l->diskname, l->wc,
                    li->report, li->report_source);
    return NULL;
}

/* ---- ext4: a trial FICLONE, so a refused clone arm records what the filesystem actually said ---- */
static void ext4_clone_reason(char *out, size_t cap, const mrec *m) {
    char a[PATH_MAX], b[PATH_MAX];
    pathf(a, sizeof a, "%s/.v3floor-ficlone-trial-%d.src", DIR_, (int)getpid());
    pathf(b, sizeof b, "%s/.v3floor-ficlone-trial-%d.dst", DIR_, (int)getpid());
    int sfd = open(a, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, 0644);
    if (sfd < 0) die("FICLONE trial src");
    if (pwrite(sfd, buf, 4096, 0) != 4096) die("FICLONE trial write");
    int dfd = open(b, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0644);
    if (dfd < 0) die("FICLONE trial dst");
    int r = ioctl(dfd, FICLONE, sfd), e = errno;
    if (close(dfd) != 0 || close(sfd) != 0 || unlinkat(AT_FDCWD, a, 0) != 0 || unlinkat(AT_FDCWD, b, 0) != 0)
        die("FICLONE trial cleanup");
    if (r == 0) {
        fprintf(stderr, "v3floor: ext4 at %s ACCEPTED a trial FICLONE: the ext4 clone refusal is wrong here\n", m->mnt);
        exit(1);
    }
    snprintf(out, cap, "%s at %s has no reflink: a trial ioctl(FICLONE) returned %s (%s); FICLONE arms run on xfs and btrfs only",
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

static int firecheck_env(void) { const char *e = getenv("V3FLOOR_FIRECHECK"); return e && !strcmp(e, "1"); }

/* after the loop, before teardown: does a copy arm's c0 share its extents with the source (a reflink) or not (a
 * byte copy)? cfr2b is a reflink on xfs/btrfs and a copy on ext4 (fresh review L6). FIEMAP without SYNC. */
static const char *shared_extents(int dfd) {
    int fd = openat(dfd, "c0", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) return "unknown (cannot open c0)";
    struct fiemap *fm = calloc(1, sizeof *fm + 8 * sizeof(struct fiemap_extent));
    if (!fm) die("calloc");
    fm->fm_start = 0;
    fm->fm_length = ~0ULL;
    fm->fm_extent_count = 8;
    int r = ioctl(fd, FS_IOC_FIEMAP, fm);
    close(fd);
    const char *out = "not shared (a byte copy)";
    if (r != 0) out = "unknown (FIEMAP failed)";
    else if (!fm->fm_mapped_extents) out = "no mapped extents";
    else
        for (unsigned k = 0; k < fm->fm_mapped_extents && k < 8; k++)
            if (fm->fm_extents[k].fe_flags & FIEMAP_EXTENT_SHARED) out = "shared (a reflink)";
    free(fm);
    return out;
}

static void leftover_check(int a, int crash) { /* every name an arm would create must be absent (lstat: a symlink counts) */
    const char *suffix[4] = {"", ".src", ".clones", ".aim"};
    for (int k = 0; k < 4; k++) {
        if (k == 0 && is_copy(a)) continue;
        if (k > 0 && !is_copy(a)) continue;
        if (k == 3 && !(crash && CRASH_AIM)) continue;
        char p[PATH_MAX];
        struct stat sb;
        pathf(p, sizeof p, "%s/%s%s", DIR_, NAMES[a], suffix[k]);
        if (lstat(p, &sb) == 0)
            refuse("%s is left over from an earlier run (or planted: %s); remove it", p, S_ISLNK(sb.st_mode) ? "a symlink" : "it exists");
    }
}

static int crash_main(int arm, const char *out, const char *exe_sha) {
    struct statfs sf;
    if (statfs(DIR_, &sf) != 0) die("statfs");
    unsigned long magic = (unsigned long)(uint32_t)sf.f_type;
    if (magic != MAGIC_EXT4 && magic != MAGIC_XFS && magic != MAGIC_BTRFS) refuse("crash mode: %s is not ext4, xfs or btrfs", DIR_);
    if (!is_copy(arm)) refuse("--crash-op takes a copy arm (clone1b, clone2b, cfr2b), not %s", NAMES[arm]);
    if (is_ficlone(arm) && magic == MAGIC_EXT4) refuse("crash mode: %s needs FICLONE, which ext4 has not", NAMES[arm]);
    dir_identity();
    leftover_check(arm, 1);
    if (mkdir(out, 0755) != 0) refuse("out dir %s must not exist: %s", out, strerror(errno));
    for (size_t i = 0; i < sizeof buf; i++) buf[i] = (char)xs();
    armst s;
    setup(arm, &s, 1);
    op(arm, &s, 0);
    char p[PATH_MAX];
    pathf(p, sizeof p, "%s/crash.json", out);
    FILE *f = fopen(p, "w");
    if (!f) die("crash.json");
    fprintf(f, "{\"probe\":\"v3floor-linux\",\"mode\":\"crash-op\",\"arm\":\"%s\",\"aim\":%d,\"mutant_nosync\":%d,\"dir\":",
            NAMES[arm], CRASH_AIM, MUTANT);
    jstr(f, DIR_);
    fprintf(f, ",\"src\":"); jstr(f, s.src);
    fprintf(f, ",\"clone\":"); pathf(p, sizeof p, "%s/c0", s.cdir); jstr(f, p);
    fprintf(f, ",\"exe_sha256\":\"%s\",\"statfs_magic\":\"0x%lx\"}\n", exe_sha, magic);
    if (fclose(f) != 0) die("crash.json close");
    printf("v3floor crash-op %s aim=%d mutant=%d: op 0 done, no teardown -> %s\n", NAMES[arm], CRASH_AIM, MUTANT, out);
    return 0;
}

int main(int argc, char **argv) {
    const char *out = NULL, *arms = "append25,append64,ow4k,ow64k,ow1m,clone1b,clone2b,cfr2b,clean,nosync25";
    const char *crash_arm = NULL;
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
        else if (!strcmp(argv[i], "--crash-op") && i + 1 < argc) crash_arm = argv[++i];
        else if (!strcmp(argv[i], "--crash-aim")) CRASH_AIM = 1;
        else { fprintf(stderr, "v3floor: bad argument %s\n", argv[i]); return 2; }
    }
    if ((MUTANT || TRACE_CLOCK || crash_arm || CRASH_AIM) && !firecheck_env())
        refuse("%s is for the fire-check only (V3FLOOR_FIRECHECK=1, which only firecheck.sh sets, is unset)",
               crash_arm ? "--crash-op" : CRASH_AIM ? "--crash-aim" : MUTANT ? "--mutant-nosync" : "--trace-clock");
    if (CRASH_AIM && !crash_arm) refuse("--crash-aim needs --crash-op");
    if (!firecheck_env()) {
        static const char *LDV[] = {"LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH", NULL};
        for (int k = 0; LDV[k]; k++)
            if (getenv(LDV[k]) && getenv(LDV[k])[0])
                refuse("%s is set: an interposed library could change what the probe does under an unchanged exe_sha256",
                       LDV[k]);
    }
    if (!DIR_ || !out || (!crash_arm && (!have_n || n == 0))) {
        fprintf(stderr, "usage: v3floor --dir D --out O --n N (N >= 1) [--arms ...] [--seed S] | --crash-op ARM --dir D --out O\n");
        return 2;
    }
    if (strlen(DIR_) > PATH_MAX - 128 || strlen(out) > PATH_MAX - 128)
        refuse("the dir or out path is too long (over %d bytes)", PATH_MAX - 128);
    if (have_seed) rng = seed | 1;
    const uint64_t seed_used = rng;
    char exe_sha[65];
    sha256_file("/proc/self/exe", exe_sha);
    if (crash_arm) {
        int a = -1;
        for (int k = 0; k < NARMS; k++) if (!strcmp(crash_arm, NAMES[k])) a = k;
        if (a < 0) { fprintf(stderr, "v3floor: unknown arm %s\n", crash_arm); return 2; }
        return crash_main(a, out, exe_sha);
    }

    char clocksrc[64];
    if (read_line("/sys/devices/system/clocksource/clocksource0/current_clocksource", clocksrc, sizeof clocksrc) != 0)
        refuse("cannot read the current clocksource");
    if (strcmp(clocksrc, "tsc") && strcmp(clocksrc, "arch_sys_counter"))
        refuse("the clocksource is %s, not tsc or arch_sys_counter (review 2 item 16's rule for a credited batch: the "
               "counter every latency here is read from)", clocksrc);

    /* the filesystem under D (its own mount, by mount id), then the flush path below it */
    static mrec top0;
    const char *why = mount_of(DIR_, &top0);
    if (why) refuse("cannot determine the filesystem under %s: %s", DIR_, why);
    unsigned long want = !strcmp(top0.fstype, "ext4") ? MAGIC_EXT4 : !strcmp(top0.fstype, "xfs") ? MAGIC_XFS
                       : !strcmp(top0.fstype, "btrfs") ? MAGIC_BTRFS : 0;
    if (!want) refuse("%s is on %s (mount %s), not ext4, xfs or btrfs", DIR_, top0.fstype, top0.mnt);
    struct statfs sf;
    if (statfs(DIR_, &sf) != 0) die("statfs");
    unsigned long magic = (unsigned long)(uint32_t)sf.f_type;
    unsigned fsid0, fsid1;
    memcpy(&fsid0, &sf.f_fsid, sizeof fsid0);
    memcpy(&fsid1, (const char *)&sf.f_fsid + sizeof fsid0, sizeof fsid1);
    if (magic != want)
        refuse("cannot determine the filesystem under %s: the mount table says %s but statfs magic is 0x%lx", DIR_,
               top0.fstype, magic);
    why = flush_path(DIR_);
    if (why) refuse("cannot determine the flush path under %s: %s", DIR_, why);
    const mrec *top = &L[0].m;
    for (int k = 0; k < NL; k++)
        if (barrier_off(&L[k].m))
            refuse("the flush path has nobarrier: layer %d, %s (%s on %s, options %s,%s), so an fsync's cache flush never "
                   "reaches the device", k, L[k].m.mnt, L[k].m.fstype, L[k].m.source, L[k].m.mopts, L[k].m.sopts);
    for (int k = 0; k < NL; k++)
        if ((why = layer_fs_checks(&L[k], k))) refuse("%s", why);
    for (int k = 0; k < NL - 1; k++)
        if (strcmp(L[k].wc, "write back"))
            refuse("layer %d, loop %s, reports '%s' above the leaf: the block layer drops every flush before the loop "
                   "driver, so no flush reaches its backing file", k, L[k].diskname, L[k].wc);
    const layer *leaf = &L[NL - 1];
    if ((why = leaf_checks(leaf))) refuse("%s", why);
    dir_identity();
    if (DIR_MNT != top->id) refuse("D's mount changed between the lookup (mount id %llu) and now (%llu)",
                                   (unsigned long long)top->id, (unsigned long long)DIR_MNT);
    int dfd0 = open(DIR_, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd0 < 0) die("open D");
    if ((why = inode_flags_problem(dfd0, DIR_))) refuse("%s", why);
    char dflags[16];
    {
        int fl = 0;
        if (ioctl(dfd0, FS_IOC_GETFLAGS, &fl) != 0) die("FS_IOC_GETFLAGS D");
        snprintf(dflags, sizeof dflags, "0x%x", (unsigned)fl);
    }
    close(dfd0);

    errno = 0;
    int nice_v = getpriority(PRIO_PROCESS, 0);
    if (errno) die("getpriority");
    if (nice_v != 0) refuse("nice is %d, not 0", nice_v);
    int pol = sched_getscheduler(0);
    if (pol < 0) die("sched_getscheduler");
    if (pol != SCHED_OTHER) refuse("scheduling policy is %d, not SCHED_OTHER", pol);
    long iop = syscall(SYS_ioprio_get, IOPRIO_WHO_PROC, 0);
    if (iop < 0) die("ioprio_get");
    int ioclass = (int)(iop >> IOPRIO_SHIFT), iolevel = (int)(iop & ((1 << IOPRIO_SHIFT) - 1));
    if (!(ioclass == 0 || (ioclass == 2 && iolevel == 4)))
        refuse("I/O priority is class %d level %d, not the default (class 0, or best-effort level 4)", ioclass, iolevel);

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
    if (nreq == 0) refuse("no arm selected");
    int has_d0 = 0, first_flushed = -1;
    for (int j = 0; j < nreq; j++) {
        if (req[j] == NOSYNC25) has_d0 = 1;
        if (flushed(req[j]) && first_flushed < 0) first_flushed = req[j];
    }
    if (first_flushed >= 0 && !has_d0)
        refuse("flushed arm %s selected without nosync25, so its flush control could not run", NAMES[first_flushed]);
    for (size_t i = 0; i < sizeof buf; i++) buf[i] = (char)xs();

    /* the FICLONE arms on ext4: refused per arm, with the reason recorded */
    int sel[NARMS], na = 0, refused[NARMS] = {0}, nref = 0, nflushed = 0;
    char reason[PATH_MAX + 1024] = "";
    for (int j = 0; j < nreq; j++) {
        if (is_ficlone(req[j]) && want == MAGIC_EXT4) {
            if (!reason[0]) ext4_clone_reason(reason, sizeof reason, top);
            refused[req[j]] = 1;
            nref++;
            fprintf(stderr, "v3floor: REFUSED arm %s: %s\n", NAMES[req[j]], reason);
        } else {
            sel[na++] = req[j];
            nflushed += flushed(req[j]);
        }
    }
    if (first_flushed >= 0 && nflushed == 0) refuse("every flushed arm selected was refused, so the run would measure no flush");
    for (int j = 0; j < na; j++) leftover_check(sel[j], 0);
    if (mkdir(out, 0755) != 0) refuse("out dir %s must not exist: %s", out, strerror(errno));

    armst st[NARMS];
    uint64_t *lat[NARMS], *t0s[NARMS];
    for (int j = 0; j < na; j++) {
        setup(sel[j], &st[j], 0);
        lat[j] = calloc(n, sizeof(uint64_t));
        t0s[j] = calloc(n, sizeof(uint64_t));
        if (!lat[j] || !t0s[j]) die("calloc");
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
            t0s[j][i] = t0;
        }
    }
    /* stationarity, recorded: p50 of the first and the last quarter of each arm's ops, in op order (fresh review M3) */
    uint64_t q1[NARMS] = {0}, q4[NARMS] = {0};
    uint64_t qn = n / 4;
    if (qn >= 2) {
        uint64_t *tmp = calloc(qn, sizeof(uint64_t));
        if (!tmp) die("calloc");
        for (int j = 0; j < na; j++) {
            memcpy(tmp, lat[j], qn * sizeof(uint64_t));
            qsort(tmp, qn, sizeof(uint64_t), cmp_u64);
            q1[j] = pct(tmp, qn, .5);
            memcpy(tmp, lat[j] + (n - qn), qn * sizeof(uint64_t));
            qsort(tmp, qn, sizeof(uint64_t), cmp_u64);
            q4[j] = pct(tmp, qn, .5);
        }
        free(tmp);
    }
    const char *shared[NARMS] = {0};
    for (int j = 0; j < na; j++) if (is_copy(sel[j])) shared[j] = shared_extents(st[j].dfd);
    for (int j = 0; j < na; j++) teardown(sel[j], &st[j], n);
    int dd = open(DIR_, O_RDONLY | O_DIRECTORY);
    if (dd < 0 || fsync(dd) != 0 || close(dd) != 0) die("teardown fsync of the dir");

    /* Each output file gets one buffer big enough for all of it, so it costs a constant number of write(2)s
     * whatever n is: the fire-check's per-op syscall slope then sees only the timed ops. */
    char p[PATH_MAX];
    pathf(p, sizeof p, "%s/raw.tsv", out);
    FILE *f = fopen(p, "w");
    if (!f) die("raw.tsv");
    size_t rawcap = (size_t)n * (size_t)na * 96 + 64;
    char *rawbuf = malloc(rawcap);
    if (!rawbuf || setvbuf(f, rawbuf, _IOFBF, rawcap) != 0) die("raw.tsv buffer");
    fprintf(f, "arm\ti\tns\tt0_ns\n");
    for (int j = 0; j < na; j++)
        for (uint64_t i = 0; i < n; i++)
            fprintf(f, "%s\t%llu\t%llu\t%llu\n", NAMES[sel[j]], (unsigned long long)i, (unsigned long long)lat[j][i],
                    (unsigned long long)t0s[j][i]);
    if (fclose(f) != 0) die("raw.tsv close");
    free(rawbuf);

    pathf(p, sizeof p, "%s/summary.json", out);
    f = fopen(p, "w");
    if (!f) die("summary.json");
    static char sumbuf[1 << 18];
    if (setvbuf(f, sumbuf, _IOFBF, sizeof sumbuf) != 0) die("summary.json buffer");
    struct utsname u;
    if (uname(&u) != 0) die("uname");
    const char *wc = leaf->wc;
    int brd = !strcmp(LEAF.kind, "brd");
    fprintf(f, "{\"probe\":\"v3floor-linux\",\"probe_rev\":\"review2\",\"n\":%llu,\"seed\":%llu,\"mutant_nosync\":%d,"
            "\"trace_clock\":%d,\"clock\":\"CLOCK_MONOTONIC_RAW\",\"clocksource\":", (unsigned long long)n,
            (unsigned long long)seed_used, MUTANT, TRACE_CLOCK);
    jstr(f, clocksrc);
    if (have_seed) fprintf(f, ",\"seed_arg\":%llu", (unsigned long long)seed);
    else fprintf(f, ",\"seed_arg\":null");
    fprintf(f, ",\"ld_env\":\"none (LD_PRELOAD, LD_AUDIT, LD_LIBRARY_PATH refused outside the fire-check)\",\"dir_flags\":");
    jstr(f, dflags);
    fprintf(f, ",\"barrier\":\"fsync(2)\",\"exe_sha256\":\"%s\",\"dir\":", exe_sha);
    jstr(f, DIR_);
    fprintf(f, ",\"realpath\":"); jstr(f, top->real);
    fprintf(f, ",\"fstype\":"); jstr(f, top->fstype);
    fprintf(f, ",\"mount_point\":"); jstr(f, top->mnt);
    fprintf(f, ",\"mount_id\":%llu", (unsigned long long)top->id);
    fprintf(f, ",\"mount_source\":"); jstr(f, top->source);
    fprintf(f, ",\"mount_opts\":"); jstr(f, top->mopts);
    fprintf(f, ",\"super_opts\":"); jstr(f, top->sopts);
    fprintf(f, ",\"dev\":"); jstr(f, top->dev);
    fprintf(f, ",\"statfs_magic\":\"0x%lx\",\"fsid\":\"%08x%08x\",\"flush_path\":[", magic, fsid0, fsid1);
    for (int k = 0; k < NL; k++) {
        const layer *l = &L[k];
        fprintf(f, "%s{\"path\":", k ? "," : ""); jstr(f, l->path);
        fprintf(f, ",\"mount\":"); jstr(f, l->m.mnt);
        fprintf(f, ",\"mount_id\":%llu,\"fstype\":", (unsigned long long)l->m.id); jstr(f, l->m.fstype);
        fprintf(f, ",\"source\":"); jstr(f, l->m.source);
        fprintf(f, ",\"options\":"); jstr(f, l->m.mopts);
        fprintf(f, ",\"super_options\":"); jstr(f, l->m.sopts);
        fprintf(f, ",\"device\":"); jstr(f, l->devname);
        fprintf(f, ",\"sys\":"); jstr(f, l->disk);
        fprintf(f, ",\"disk\":"); jstr(f, l->diskname);
        fprintf(f, ",\"disk_dev\":"); jstr(f, l->diskdev);
        fprintf(f, ",\"write_cache\":"); jstr(f, l->wc);
        fprintf(f, ",\"fua\":"); jstr(f, l->fua);
        fprintf(f, ",\"loop_backing\":"); jstr(f, l->backing);
        if (l->backing[0]) {
            fprintf(f, ",\"loop_dio\":"); jstr(f, l->loopdio);
            fprintf(f, ",\"loop_backing_dev_ino\":\"%llx:%llu (LOOP_GET_STATUS64 == stat)\"", l->lo_dev, l->lo_ino);
        }
        if (!strcmp(l->m.fstype, "ext4")) {
            fprintf(f, ",\"ext4\":{\"data\":"); jstr(f, l->ext4_data);
            fprintf(f, ",\"commit_s\":"); jstr(f, l->ext4_commit);
            fprintf(f, ",\"journal_async_commit\":%s,\"journal\":", l->ext4_async ? "true" : "false");
            jstr(f, l->jbd2);
            char lab[128];
            snprintf(lab, sizeof lab, "data=%s%s, commit=%s", l->ext4_data, l->ext4_async ? ", async commit" : "", l->ext4_commit);
            fprintf(f, ",\"label\":");
            jstr(f, lab);
            fprintf(f, "}");
        }
        if (!strcmp(l->m.fstype, "btrfs")) fprintf(f, ",\"btrfs_devices\":%d", l->btrfs_devices);
        fprintf(f, "}");
    }
    fprintf(f, "],\"layers\":%d,\"loop_layers\":%d,\"leaf\":{\"disk\":", NL, NL - 1);
    jstr(f, leaf->diskname);
    fprintf(f, ",\"dev\":"); jstr(f, leaf->diskdev);
    fprintf(f, ",\"driver\":"); jstr(f, LEAF.driver);
    fprintf(f, ",\"kind\":"); jstr(f, LEAF.kind);
    fprintf(f, ",\"write_cache\":"); jstr(f, wc);
    fprintf(f, ",\"fua\":"); jstr(f, leaf->fua);
    fprintf(f, ",\"drive_reports\":"); jstr(f, LEAF.report);
    fprintf(f, ",\"drive_report_source\":"); jstr(f, LEAF.report_source);
    fprintf(f, ",\"model\":"); jstr(f, LEAF.model);
    fprintf(f, ",\"sd_cache_type\":"); jstr(f, LEAF.cache_type);
    if (!strcmp(LEAF.driver, "nvme"))
        fprintf(f, ",\"nvme_vwc_enabled\":\"not read: Get Features 06h needs CAP_SYS_ADMIN (VWC bit 0 says a cache is "
                "present, not that it is on)\"");
    fprintf(f, ",\"nvme_controllers\":[");
    for (int c = 0; c < LEAF.nctrl; c++) { fprintf(f, "%s", c ? "," : ""); jstr(f, LEAF.ctrls[c]); }
    fprintf(f, "],\"multipath\":[");
    for (int c = 0; c < LEAF.npath; c++) {
        fprintf(f, "%s{\"disk\":", c ? "," : ""); jstr(f, LEAF.paths[c]);
        fprintf(f, ",\"dev\":"); jstr(f, LEAF.pathdevs[c]);
        fprintf(f, "}");
    }
    fprintf(f, "],\"creditable\":%s}", brd ? "false" : "true");
    fprintf(f, ",\"leaf_write_cache\":"); jstr(f, wc);
    fprintf(f, ",\"leaf_fua\":"); jstr(f, leaf->fua);
    int wb = !strcmp(wc, "write back");
    int vm = 0, vflag = 0;
    char hyp[64], vendor[128], product[128];
    virt_record(&vm, &vflag, hyp, vendor, product, sizeof vendor < sizeof hyp ? sizeof vendor : sizeof hyp);
    fprintf(f, ",\"virtualization\":{\"virtualized\":%s,\"cpuinfo_hypervisor_flag\":%s,\"sys_hypervisor_type\":",
            vm ? "true" : "false", vflag ? "true" : "false");
    jstr(f, hyp);
    fprintf(f, ",\"dmi_sys_vendor\":"); jstr(f, vendor);
    fprintf(f, ",\"dmi_product_name\":"); jstr(f, product);
    fprintf(f, "}");
    fprintf(f, ",\"flush_sent_to_device\":\"%s\"",
            brd ? "no: the leaf is brd (RAM, fire-check only), which has no cache"
            : wb ? "yes: the leaf reports a volatile write cache and the drive agrees, so a flush the filesystem issues "
                   "reaches it; how many each arm's op issues is the device flush record (device_flushes_per_op)"
                 : "no: the leaf reports write-through and the drive agrees, so the block layer sends it no flush");
    /* on a VM the "drive" is the hypervisor's: whether its flush reaches media is the host's business (fresh review M1) */
    fprintf(f, ",\"floor_kind\":\"%s\"", brd ? "brd: no drive (fire-check only, never credited)"
            : wb ? (vm ? "virtual drive flush: reach to media unknown" : "drive flush") : "no volatile cache: no drive flush");
    /* the review's sentence is for ext4/XFS; btrfs's clean fsync issues no flush at all (run 37476867864), so it gets
     * no bare-flush baseline; batchgate.py writes floor_claim_from_counts from the batch's own device flush counts */
    fprintf(f, ",\"floor_claim\":\"%s\"",
            brd ? "none: a brd floor backs no sentence"
            : !wb ? "per stack: the cost of an fsync on a drive that receives no flush; neither 'one drive flush' nor "
                    "'above a same-batch drive flush' may be written"
            : strcmp(top->fstype, "btrfs") ? "per stack: a clean fsync (a bare flush) plus the measured delta, only where "
                                             "the device flush record shows the clean arm issuing a flush per op "
                                             "(floor_claim_from_counts); never 'one drive flush' without that count"
                                           : "per stack: the measured per-arm device flush counts only; on btrfs a clean "
                                             "fsync can issue no flush, so no bare-flush baseline is claimed");
    fprintf(f, ",\"uname\":");
    char un[600];
    snprintf(un, sizeof un, "%s %s %s", u.sysname, u.release, u.machine);
    jstr(f, un);
    fprintf(f, ",\"nice\":%d,\"sched_policy\":%d,\"ioprio_class\":%d,\"ioprio_level\":%d,\"arms_requested\":", nice_v, pol,
            ioclass, iolevel);
    jstr(f, arms);
    fprintf(f, ",\"arms_gated\":[");
    for (int a = 0, k = 0; a < NARMS; a++) if (gated(a)) fprintf(f, "%s\"%s\"", k++ ? "," : "", NAMES[a]);
    fprintf(f, "],\"arms_gated_run\":[");
    for (int j = 0, k = 0; j < na; j++) if (gated(sel[j])) fprintf(f, "%s\"%s\"", k++ ? "," : "", NAMES[sel[j]]);
    fprintf(f, "],\"copy_extents\":{");
    for (int j = 0, k = 0; j < na; j++)
        if (shared[j]) { fprintf(f, "%s\"%s\":", k++ ? "," : "", NAMES[sel[j]]); jstr(f, shared[j]); }
    fprintf(f, "},\"arms_report_only\":{");
    for (int a = 0, k = 0; a < NARMS; a++)
        if (report_only_why(a)) { fprintf(f, "%s\"%s\":", k++ ? "," : "", NAMES[a]); jstr(f, report_only_why(a)); }
    fprintf(f, "},\"durability\":{\"clone2b\":\"device durability unverified on Linux by this probe; the fire-check's "
            "crash.sh shows it surviving a filesystem-level crash on loops, which cannot see a missing device flush\","
            "\"cfr2b\":\"device durability unverified on Linux by this probe; the fire-check's crash.sh shows it surviving "
            "a filesystem-level crash on loops, which cannot see a missing device flush\",\"clone1b\":\"not guaranteed on "
            "Linux (report-only): crash.sh loses it on btrfs and on XFS when the log was forced between the create and the "
            "FICLONE; it survived plain XFS by checkpoint batching\"}");
    int frame_ran = 0;
    for (int j = 0; j < na; j++) frame_ran |= sel[j] == APPEND64;
    if (frame_ran)
        fprintf(f, ",\"frame_arm\":\"append64\",\"frame_bytes\":64,\"frame_rule\":\"PREREG section 4: the smallest bytes "
                "per flush >= the M1 build's median create frame; review 2 item 11 puts a named create's flight at about "
                "56-60 B (unverified here)\"");
    else fprintf(f, ",\"frame_arm\":null");
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
        if (qn >= 2) fprintf(f, "\"p50_q1_us\":%.1f,\"p50_q4_us\":%.1f,", q1[j] / 1e3, q4[j] / 1e3);
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
            fprintf(f, ",\"flush_d0_p50_ratio\":%.1f", p50[have_d0] ? (double)p50[have_app] / (double)p50[have_d0] : 1e18);
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
    printf("v3floor n=%llu arms=%s refused=%d leaf=%s(%s,%s) -> %s (rc %d)\n", (unsigned long long)n, arms, nref,
           leaf->diskname, LEAF.driver, wc, out, rc);
    return rc;
}
