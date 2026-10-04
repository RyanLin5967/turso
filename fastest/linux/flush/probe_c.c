/* probe_c.c -- V1 Linux fire-check probe: issues an exact, K-determined number of each flush kind.
 *
 *   probe_c <mode> <K> <dir> [static_probe]
 *     full        every root phase below, then a forked child (cross-process marks), a posix_spawned child, a
 *                 system-libsqlite3 child, a child exec'd through /bin/sh, a vfork+execve child, a fork+execv child
 *     noise       only non-flush calls: must count zero
 *     spawned     K fsync
 *     sqlite      one commit through the system libsqlite3.so.0 (journal_mode=DELETE, synchronous=FULL)
 *     raw         K fsync through syscall(2), then K fsync, K fdatasync and K pwrite64 on an O_DSYNC fd as RAW
 *                 syscall instructions (the shim cannot see these; strace can)
 *     clone       K FICLONE, K FICLONERANGE, K copy_file_range in <dir>
 *     killme      K fsync, print "ready pid=N", then wait to be SIGKILLed
 *     execstatic  posix_spawn, then fork+execv, the STATIC probe (argv[4]) in "spawned" mode: the exec guard fires
 *     fanout      posix_spawn three "spawned" children (the slot- and exec-table overflow arms)
 *     asyncio     one io_uring_setup through syscall(2): the strace counter must refuse
 *     setfl-truth (run WITHOUT the shim) prints 1 if F_SETFL O_DSYNC sticks on this kernel, else 0
 *
 * Each phase sets the run mark (v1_set_mark: shm when a run is mapped, and the strace marker lseek always), so both
 * instruments can be checked per phase. Expected counts are derived from K by firecheck.py, never read back from
 * this program or from either instrument.
 */
#define _GNU_SOURCE
#include "syncshim.h"
#include <dlfcn.h>
#include <spawn.h>
#include <stdlib.h>
#include <sys/wait.h>

extern char **environ;
extern int __open_2(const char *, int);
extern int __open64_2(const char *, int);
extern int __openat_2(int, const char *, int);
extern int __openat64_2(int, const char *, int);
extern int fcntl64(int, int, ...);

#ifndef SYS_openat2
#error "SYS_openat2 missing from <sys/syscall.h>"
#endif
struct probe_open_how { uint64_t flags, mode, resolve; };
struct probe_fcr { int64_t src_fd; uint64_t src_offset, src_length, dest_offset; };

static v1_hdr *H;
static const char *DIR_;
static char SELF[4096];

static void die(const char *what) { fprintf(stderr, "probe_c: %s: %s\n", what, strerror(errno)); exit(1); }
static void mark(uint64_t m) { v1_set_mark(H, m); }
static void pathof(const char *name, char *out, size_t n) { snprintf(out, n, "%s/%s", DIR_, name); }
static int openf(const char *name, int flags) {
    char p[4096];
    pathof(name, p, sizeof p);
    int fd = open(p, flags | O_CREAT, 0644);
    if (fd < 0) die(p);
    return fd;
}
static void ck(long r, const char *what) { if (r == -1) die(what); }

#if defined(__x86_64__)
static long rawsys(long n, long a, long b, long c, long d) {
    long r;
    register long r10 __asm__("r10") = d;
    __asm__ volatile("syscall" : "=a"(r) : "a"(n), "D"(a), "S"(b), "d"(c), "r"(r10) : "rcx", "r11", "memory");
    return r;
}
#elif defined(__aarch64__)
static long rawsys(long n, long a, long b, long c, long d) {
    register long x8 __asm__("x8") = n;
    register long x0 __asm__("x0") = a;
    register long x1 __asm__("x1") = b;
    register long x2 __asm__("x2") = c;
    register long x3 __asm__("x3") = d;
    __asm__ volatile("svc #0" : "+r"(x0) : "r"(x8), "r"(x1), "r"(x2), "r"(x3) : "memory");
    return x0;
}
#else
#error "probe_c raw mode: x86_64 or aarch64 only"
#endif
static void ckraw(long r, const char *what) {
    if (r < 0) { errno = (int)-r; die(what); }
}

static void wait_ok(pid_t pid, const char *what) {
    int st;
    if (waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "probe_c: child %s (pid %d) exited badly (status 0x%x)\n", what, (int)pid, st);
        exit(1);
    }
}

/* posix_spawn <exe> <mode> K DIR, directly or through /bin/sh -c 'exec ...'. */
static pid_t spawn_mode(const char *exe, const char *mode, int K, int via_sh) {
    char kbuf[32], cmd[8192];
    snprintf(kbuf, sizeof kbuf, "%d", K);
    pid_t pid;
    int r;
    if (via_sh) {
        snprintf(cmd, sizeof cmd, "exec '%s' %s %d '%s'", exe, mode, K, DIR_);
        char *av[] = {"/bin/sh", "-c", cmd, NULL};
        r = posix_spawn(&pid, "/bin/sh", NULL, NULL, av, environ);
    } else {
        char *av[] = {(char *)exe, (char *)mode, kbuf, (char *)DIR_, NULL};
        r = posix_spawn(&pid, exe, NULL, NULL, av, environ);
    }
    if (r != 0) { errno = r; die("posix_spawn"); }
    wait_ok(pid, mode);
    return pid;
}
static pid_t vfork_execve(char *const av[]) {
    pid_t p = vfork();
    if (p == 0) {
        execve(av[0], av, environ);
        _exit(127);
    }
    return p;
}
static pid_t fork_execv(char *const av[]) {
    pid_t p = fork();
    if (p == 0) {
        execv(av[0], av);
        _exit(127);
    }
    return p;
}

static void noise_phases(int K, uint64_t m) {
    char buf[4096];
    memset(buf, 'n', sizeof buf);
    struct iovec iv[2] = {{buf, 100}, {buf, 412}};
    int fd = openf("noise", O_RDWR | O_TRUNC);
    mark(m);
    for (int i = 0; i < 3 * K; i++) ck(fcntl(fd, F_GETFL), "F_GETFL");
    for (int i = 0; i < K; i++) ck(fcntl(fd, F_SETFL, (i & 1) ? O_NONBLOCK : 0), "F_SETFL");
    for (int i = 0; i < K; i++) ck(write(fd, buf, 512), "write");
    for (int i = 0; i < K; i++) ck(pwrite(fd, buf, 512, 8192), "pwrite");
    for (int i = 0; i < K; i++) ck(pwrite64(fd, buf, 512, 8192), "pwrite64");
    for (int i = 0; i < K; i++) ck(writev(fd, iv, 2), "writev");
    for (int i = 0; i < K; i++) ck(pwritev2(fd, iv, 2, 0, 0), "pwritev2 0");
    for (int i = 0; i < K; i++) ck(lseek(fd, 0, SEEK_SET), "lseek");
    for (int i = 0; i < K; i++) { int n = 0; ck(ioctl(fd, FIONREAD, &n), "FIONREAD"); }
    close(fd);
}

static int child_loop(int rfd, int wfd, int fd) {
    unsigned char c[2];
    for (;;) {
        if (read(rfd, c, 2) != 2) return 1;
        if (c[0] == 0) return 0;
        if (c[0] == 1) for (int i = 0; i < (c[1] % 3) + 1; i++) ck(fsync(fd), "child fsync");
        if (c[0] == 2) ck(fdatasync(fd), "child fdatasync");
        if (write(wfd, c, 1) != 1) return 1;
    }
}

static void clone_phase(int K) {
    char buf[65536];
    memset(buf, 'c', sizeof buf);
    int src = openf("clsrc", O_RDWR | O_TRUNC);
    ck(write(src, buf, sizeof buf), "clsrc");
    int dst = openf("cldst", O_RDWR | O_TRUNC);
    struct probe_fcr r = {src, 0, 4096, 0};
    int clone_fail = 0, range_fail = 0;
    for (int i = 0; i < K; i++) if (ioctl(dst, V1_FICLONE, src) == -1) clone_fail++;
    for (int i = 0; i < K; i++) if (ioctl(dst, V1_FICLONERANGE, &r) == -1) range_fail++;
    for (int i = 0; i < K; i++) {
        __off64_t in = 0, out = 0;
        ssize_t n = copy_file_range(src, &in, dst, &out, 4096, 0);
        if (n != 4096) die("copy_file_range");
    }
    close(src);
    close(dst);
    printf("clone_fail=%d range_fail=%d ", clone_fail, range_fail);
}

static int run_full(int K) {
    char buf[65536], p[4096];
    memset(buf, 'x', sizeof buf);
    struct iovec iv[2] = {{buf, 100}, {buf, 412}};
    int fd = openf("a", O_RDWR | O_TRUNC);
    ck(write(fd, buf, sizeof buf), "write a");
    mark(1); for (int i = 0; i < K; i++) ck(fsync(fd), "fsync");
    mark(2); for (int i = 0; i < K; i++) ck(fdatasync(fd), "fdatasync");
    mark(3);
    for (int i = 0; i < K; i++)
        ck(sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER), "sfr wait");
    mark(4); for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WRITE), "sfr write");
    mark(5);
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WAIT_BEFORE), "sfr wait_before");
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WAIT_AFTER), "sfr wait_after");
    mark(6); for (int i = 0; i < K; i++) ck(syncfs(fd), "syncfs");
    char *m = mmap(NULL, sizeof buf, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (m == MAP_FAILED) die("mmap");
    mark(7); for (int i = 0; i < K; i++) { m[i % 4096] ^= 1; ck(msync(m, sizeof buf, MS_SYNC), "msync SYNC"); }
    mark(8);
    for (int i = 0; i < K; i++) { m[i % 4096] ^= 1; ck(msync(m, sizeof buf, MS_ASYNC), "msync ASYNC"); }
    for (int i = 0; i < K; i++) ck(msync(m, sizeof buf, MS_INVALIDATE), "msync INVALIDATE");
    munmap(m, sizeof buf);
    noise_phases(K, 9);

    mark(10); /* O_DSYNC fd: every write entry point */
    int d = openf("dsync", O_WRONLY | O_TRUNC | O_DSYNC);
    for (int i = 0; i < K; i++) ck(write(d, buf, 512), "dsync write");
    for (int i = 0; i < K; i++) ck(pwrite(d, buf, 512, 4096), "dsync pwrite");
    for (int i = 0; i < K; i++) ck(pwrite64(d, buf, 512, 4096), "dsync pwrite64");
    int d2 = dup(d);
    for (int i = 0; i < K; i++) ck(writev(d2, iv, 2), "dsync writev via dup");
    close(d2);
    for (int i = 0; i < K; i++) ck(pwritev(d, iv, 2, 0), "dsync pwritev");
    for (int i = 0; i < K; i++) ck(pwritev64(d, iv, 2, 0), "dsync pwritev64");
    for (int i = 0; i < K; i++) ck(pwritev2(d, iv, 2, 0, 0), "dsync pwritev2");
    for (int i = 0; i < K; i++) ck(pwritev64v2(d, iv, 2, 0, 0), "dsync pwritev64v2");
    close(d);

    mark(11); /* plain fds that reuse the numbers just closed */
    int r1 = openf("plain1", O_WRONLY | O_TRUNC);
    int r2 = openf("plain2", O_WRONLY | O_TRUNC);
    for (int i = 0; i < K; i++) { ck(write(r1, buf, 512), "plain write"); ck(write(r2, buf, 512), "plain write2"); }
    close(r1);
    close(r2);

    mark(12); /* O_SYNC fd */
    int s = openf("osync", O_WRONLY | O_TRUNC | O_SYNC);
    for (int i = 0; i < K; i++) ck(write(s, buf, 512), "osync write");
    for (int i = 0; i < K; i++) ck(pwritev(s, iv, 2, 0), "osync pwritev");
    close(s);

    mark(13); /* F_SETFL O_DSYNC on a plain fd: counts only if the kernel keeps it (setfl-truth) */
    int f = openf("setfl", O_WRONLY | O_TRUNC);
    ck(fcntl(f, F_SETFL, O_DSYNC), "F_SETFL O_DSYNC");
    for (int i = 0; i < K; i++) ck(write(f, buf, 512), "setfl write");
    close(f);

    mark(14); /* per-call RWF_* on a plain fd, and RWF_SYNC on an O_DSYNC fd (the stronger wins) */
    int pv = openf("plainv2", O_WRONLY | O_TRUNC);
    for (int i = 0; i < K; i++) ck(pwritev2(pv, iv, 2, 0, RWF_DSYNC), "pwritev2 RWF_DSYNC");
    for (int i = 0; i < K; i++) ck(pwritev2(pv, iv, 2, 0, RWF_SYNC), "pwritev2 RWF_SYNC");
    for (int i = 0; i < K; i++) ck(pwritev64v2(pv, iv, 2, 0, RWF_DSYNC), "pwritev64v2 RWF_DSYNC");
    close(pv);
    int d3 = openf("dsync2", O_WRONLY | O_TRUNC | O_DSYNC);
    for (int i = 0; i < K; i++) ck(pwritev2(d3, iv, 2, 0, RWF_SYNC), "dsync pwritev2 RWF_SYNC");
    close(d3);

    mark(15); /* every open/dup variant hands on O_DSYNC */
    pathof("dsync", p, sizeof p);
    const int fl = O_WRONLY | O_DSYNC;
    int v[12];
    v[0] = open64(p, fl);
    v[1] = openat(AT_FDCWD, p, fl);
    v[2] = openat64(AT_FDCWD, p, fl);
    v[3] = __open_2(p, fl);
    v[4] = __open64_2(p, fl);
    v[5] = __openat_2(AT_FDCWD, p, fl);
    v[6] = __openat64_2(AT_FDCWD, p, fl);
    v[7] = fcntl(v[0], F_DUPFD, 100);
    v[8] = fcntl(v[0], F_DUPFD_CLOEXEC, 100);
    v[9] = fcntl64(v[0], F_DUPFD, 100);
    v[10] = dup2(v[0], 200);
    v[11] = dup3(v[0], 201, O_CLOEXEC);
    for (int j = 0; j < 12; j++) {
        if (v[j] < 0) { fprintf(stderr, "probe_c: open/dup variant %d failed: %s\n", j, strerror(errno)); return 1; }
        for (int i = 0; i < K; i++) ck(write(v[j], buf, 512), "variant write");
    }
    for (int j = 0; j < 12; j++) close(v[j]);

    mark(16);
    sync();

    mark(17);
    clone_phase(K);

    mark(18); /* the generic syscall(2) entry point */
    for (int i = 0; i < K; i++) ck(syscall(SYS_fsync, fd), "syscall fsync");
    pathof("osync2", p, sizeof p);
    struct probe_open_how how = {O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, 0644, 0};
    int o = (int)syscall(SYS_openat2, AT_FDCWD, p, &how, sizeof how);
    if (o < 0) die("syscall openat2");
    for (int i = 0; i < K; i++) ck(write(o, buf, 512), "openat2 O_SYNC write");
    close(o);
    int d4 = openf("dsync3", O_WRONLY | O_TRUNC | O_DSYNC);
    for (int i = 0; i < K; i++) ck(syscall(SYS_pwrite64, d4, buf, 512, 0), "syscall pwrite64");
    close(d4);
    close(fd);

    /* 1000+i: cross-process marks. The parent sets the mark; a forked child does the flushes. */
    int p2c[2], c2p[2];
    if (pipe(p2c) || pipe(c2p)) die("pipe");
    int cf = openf("child", O_RDWR | O_TRUNC);
    ck(write(cf, buf, 4096), "child file");
    pid_t pid = fork();
    if (pid < 0) die("fork");
    if (pid == 0) {
        close(p2c[1]);
        close(c2p[0]);
        _exit(child_loop(p2c[0], c2p[1], cf));
    }
    close(p2c[0]);
    close(c2p[1]);
    for (int i = 1; i <= K; i++) {
        unsigned char c[2] = {1, (unsigned char)i}, a;
        mark(1000 + (uint64_t)i);
        if (write(p2c[1], c, 2) != 2 || read(c2p[0], &a, 1) != 1) die("child rpc");
        mark(V1_MARK_IDLE | (1000 + (uint64_t)i));
        c[0] = 2;
        if (write(p2c[1], c, 2) != 2 || read(c2p[0], &a, 1) != 1) die("child rpc idle");
    }
    unsigned char z[2] = {0, 0};
    if (write(p2c[1], z, 2) != 2) die("child stop");
    wait_ok(pid, "fork child");
    close(cf);
    close(p2c[1]);
    close(c2p[0]);

    char kbuf[32];
    snprintf(kbuf, sizeof kbuf, "%d", K);
    char *av[] = {SELF, "spawned", kbuf, (char *)DIR_, NULL};
    mark(21);
    pid_t sp = spawn_mode(SELF, "spawned", K, 0);
    mark(22);
    pid_t sq = spawn_mode(SELF, "sqlite", K, 0);
    mark(23);
    pid_t sh = spawn_mode(SELF, "spawned", K, 1);
    mark(24);
    pid_t vf = vfork_execve(av);
    if (vf < 0) die("vfork");
    wait_ok(vf, "vfork+execve");
    mark(25);
    pid_t fx = fork_execv(av);
    if (fx < 0) die("fork");
    wait_ok(fx, "fork+execv");
    mark(0);
    printf("probe_c full K=%d root=%d fork_child=%d spawned=%d sqlite=%d via_sh=%d vfork=%d forkexec=%d\n", K,
           (int)getpid(), (int)pid, (int)sp, (int)sq, (int)sh, (int)vf, (int)fx);
    return 0;
}

static int run_raw(int K) {
    char buf[4096];
    memset(buf, 'r', sizeof buf);
    int fd = openf("raw", O_RDWR | O_TRUNC);
    ck(write(fd, buf, sizeof buf), "write raw");
    mark(1); for (int i = 0; i < K; i++) ck(syscall(SYS_fsync, fd), "syscall(2) fsync");
    mark(2); for (int i = 0; i < K; i++) ckraw(rawsys(SYS_fsync, fd, 0, 0, 0), "raw fsync");
    mark(3); for (int i = 0; i < K; i++) ckraw(rawsys(SYS_fdatasync, fd, 0, 0, 0), "raw fdatasync");
    int d = openf("rawdsync", O_WRONLY | O_TRUNC | O_DSYNC);
    mark(4); for (int i = 0; i < K; i++) ckraw(rawsys(SYS_pwrite64, d, (long)buf, 512, 0), "raw pwrite64");
    close(d);
    close(fd);
    mark(0);
    printf("probe_c raw K=%d root=%d\n", K, (int)getpid());
    return 0;
}

static int run_sqlite(void) {
    void *lib = dlopen("libsqlite3.so.0", RTLD_NOW);
    if (!lib) { fprintf(stderr, "probe_c: dlopen libsqlite3.so.0: %s\n", dlerror()); return 1; }
    int (*op)(const char *, void **) = (int (*)(const char *, void **))dlsym(lib, "sqlite3_open");
    int (*ex)(void *, const char *, void *, void *, char **) =
        (int (*)(void *, const char *, void *, void *, char **))dlsym(lib, "sqlite3_exec");
    int (*cl)(void *) = (int (*)(void *))dlsym(lib, "sqlite3_close");
    if (!op || !ex || !cl) { fprintf(stderr, "probe_c: sqlite3 symbols missing\n"); return 1; }
    char p[4096];
    pathof("sys.sqlite", p, sizeof p);
    unlink(p);
    void *db;
    if (op(p, &db) != 0) return 1;
    char *err = NULL;
    const char *sql = "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;"
                      "CREATE TABLE t(x); BEGIN; INSERT INTO t VALUES(1); COMMIT;";
    if (ex(db, sql, NULL, NULL, &err) != 0) { fprintf(stderr, "probe_c: sqlite: %s\n", err ? err : "?"); return 1; }
    cl(db);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: probe_c full|noise|spawned|sqlite|raw|clone|killme|execstatic|fanout|asyncio|setfl-truth K dir [static]\n");
        return 2;
    }
    const char *mode = argv[1];
    int K = atoi(argv[2]);
    DIR_ = argv[3];
    if (K < 1) { fprintf(stderr, "probe_c: K must be >= 1\n"); return 2; }
    ssize_t sn = readlink("/proc/self/exe", SELF, sizeof SELF - 1);
    if (sn <= 0) die("readlink /proc/self/exe");
    SELF[sn] = 0;
    if (!strcmp(mode, "setfl-truth")) {
        int f = openf("setfl_truth", O_WRONLY | O_TRUNC);
        ck(fcntl(f, F_SETFL, O_DSYNC), "F_SETFL");
        int fl = fcntl(f, F_GETFL);
        printf("%d\n", (fl != -1 && (fl & O_DSYNC)) ? 1 : 0);
        return 0;
    }
    const char *run = getenv("SYNCSHIM_RUN"), *why = "?";
    if (run && *run) {
        H = v1_map(run, &why);
        if (!H) { fprintf(stderr, "probe_c: map %s: %s\n", run, why); return 1; }
    }
    if (!strcmp(mode, "full")) return run_full(K);
    if (!strcmp(mode, "noise")) {
        noise_phases(K, 9);
        mark(0);
        return 0;
    }
    if (!strcmp(mode, "spawned")) {
        int fd = openf("spawned", O_RDWR | O_TRUNC);
        char b[512] = {0};
        ck(write(fd, b, sizeof b), "write");
        for (int i = 0; i < K; i++) ck(fsync(fd), "spawned fsync");
        close(fd);
        return 0;
    }
    if (!strcmp(mode, "sqlite")) return run_sqlite();
    if (!strcmp(mode, "raw")) return run_raw(K);
    if (!strcmp(mode, "clone")) {
        mark(17);
        clone_phase(K);
        mark(0);
        printf("probe_c clone K=%d root=%d\n", K, (int)getpid());
        return 0;
    }
    if (!strcmp(mode, "killme")) {
        int fd = openf("killme", O_RDWR | O_TRUNC);
        char b[512] = {0};
        ck(write(fd, b, sizeof b), "write");
        mark(1);
        for (int i = 0; i < K; i++) ck(fsync(fd), "killme fsync");
        printf("ready pid=%d\n", (int)getpid());
        fflush(stdout);
        for (;;) pause();
    }
    if (!strcmp(mode, "execstatic")) {
        if (argc < 5) { fprintf(stderr, "probe_c: execstatic needs the static probe path\n"); return 2; }
        char kbuf[32];
        snprintf(kbuf, sizeof kbuf, "%d", K);
        char *av[] = {argv[4], "spawned", kbuf, (char *)DIR_, NULL};
        mark(1);
        pid_t a = spawn_mode(argv[4], "spawned", K, 0);
        mark(2);
        pid_t b = fork_execv(av);
        if (b < 0) die("fork");
        wait_ok(b, "fork+execv static");
        mark(0);
        printf("probe_c execstatic K=%d root=%d spawned_static=%d forkexec_static=%d\n", K, (int)getpid(), (int)a, (int)b);
        return 0;
    }
    if (!strcmp(mode, "fanout")) {
        pid_t c[3];
        for (int i = 0; i < 3; i++) c[i] = spawn_mode(SELF, "spawned", K, 0);
        printf("probe_c fanout K=%d root=%d c1=%d c2=%d c3=%d\n", K, (int)getpid(), (int)c[0], (int)c[1], (int)c[2]);
        return 0;
    }
    if (!strcmp(mode, "asyncio")) {
#ifdef SYS_io_uring_setup
        unsigned char params[256];
        memset(params, 0, sizeof params);
        long r = syscall(SYS_io_uring_setup, 4, params);
        int e = errno;
        if (r >= 0) close((int)r);
        printf("probe_c asyncio io_uring_setup=%ld errno=%d root=%d\n", r, r < 0 ? e : 0, (int)getpid());
        return 0;
#else
        fprintf(stderr, "probe_c: SYS_io_uring_setup missing\n");
        return 2;
#endif
    }
    fprintf(stderr, "probe_c: unknown mode %s\n", mode);
    return 2;
}
