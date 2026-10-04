/* probe_c.c -- V1 Linux fire-check probe: issues an exact, K-determined number of each counted call.
 *
 *   probe_c <mode> <K> <dir> [static_probe]
 *     full        every root phase (marks 1-17), a forked child driven by the parent's marks (1000+i), then one
 *                 child per exec route (marks 21-31: posix_spawn, posix_spawnp, and fork + execve, execv, execvp,
 *                 execvpe, execl, execle, execlp, fexecve, execveat), a vfork+execve child (32) and a child
 *                 exec'd through /bin/sh -c 'exec ...' (33); each child runs "spawned"
 *     noise       only calls that are not counted: must count zero
 *     spawned     K fsync
 *     raw         K fsync and K fdatasync through syscall(2), K fsync as an inline syscall instruction, K pwrite64
 *                 on an O_DSYNC fd through syscall(2) -- the shim must miss all of them -- then K fsync and K
 *                 pwrite64 through libc, which it must count (the control that it was loaded and working)
 *     sqlite      one commit through the system libsqlite3.so.0 (journal_mode=DELETE, synchronous=FULL)
 *     killme      K fsync, print "ready pid=N", then wait to be SIGKILLed
 *     execstatic  three children that never attach: posix_spawn and fork+execv of the STATIC probe (argv[4]),
 *                 and fork+execve of this probe with an environment that has no LD_PRELOAD
 *     fanout      posix_spawn three "spawned" children (the slot- and exec-table overflow arms)
 *     setfl-truth (run WITHOUT the shim) prints 1 if F_SETFL O_DSYNC sticks on this kernel, else 0
 *
 * Each phase sets the run mark first (v1_set_mark). Expected counts are derived from K by firecheck.py, never read
 * back from this program or from the counter; the pids printed here only say which slot is which process.
 */
#define _GNU_SOURCE
#include "syncshim.h"
#include <dlfcn.h>
#include <spawn.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sys/wait.h>

extern char **environ;
static const char *DIR_;
static void die(const char *what) { fprintf(stderr, "probe_c: %s: %s\n", what, strerror(errno)); exit(1); }
static void pathof(const char *name, char *out, size_t n) { snprintf(out, n, "%s/%s", DIR_, name); }
static int openf(const char *name, int flags) {
    char p[4096];
    pathof(name, p, sizeof p);
    int fd = open(p, flags | O_CREAT, 0644);
    if (fd < 0) die(p);
    return fd;
}
static void ck(long r, const char *what) { if (r == -1) die(what); }
static void spawned(int K) {
    int fd = openf("spawned", O_RDWR | O_TRUNC);
    char b[512] = {0};
    ck(write(fd, b, sizeof b), "write");
    for (int i = 0; i < K; i++) ck(fsync(fd), "spawned fsync");
    close(fd);
}

#ifdef PROBE_STATIC
/* The static build (-static): only "spawned", as a child or root that can never load the shim. */
int main(int argc, char **argv) {
    if (argc < 4 || strcmp(argv[1], "spawned") != 0) { fprintf(stderr, "probe_c_static: spawned K dir\n"); return 2; }
    int K = atoi(argv[2]);
    DIR_ = argv[3];
    if (K < 1 || K > 250) return 2;
    spawned(K);
    return 0;
}
#else
/* glibc exports these but declares them only under _FORTIFY_SOURCE, or not at all. */
extern int __open(const char *, int, ...);
extern int __open64(const char *, int, ...);
extern int __open_2(const char *, int);
extern int __open64_2(const char *, int);
extern int __openat_2(int, const char *, int);
extern int __openat64_2(int, const char *, int);
extern int __fcntl(int, int, ...);
extern int __dup2(int, int);
extern ssize_t __write(int, const void *, size_t);
extern ssize_t __pwrite64(int, const void *, size_t, __off64_t);
extern int fcntl64(int, int, ...);                                         /* glibc >= 2.28 */
extern int execveat(int, const char *, char *const[], char *const[], int); /* glibc >= 2.34 */

struct probe_fcr { int64_t src_fd; uint64_t src_offset, src_length, dest_offset; };

static char SELF[4096];
static v1_hdr *H;
static void mark(uint64_t m) {
    if (!H) { fprintf(stderr, "probe_c: this mode needs a run (SYNCSHIM_RUN)\n"); exit(1); }
    v1_set_mark(H, m);
}

#if defined(__x86_64__)
static long rawsys1(long n, long a) {
    long r;
    __asm__ volatile("syscall" : "=a"(r) : "a"(n), "D"(a) : "rcx", "r11", "memory");
    return r;
}
#elif defined(__aarch64__)
static long rawsys1(long n, long a) {
    register long x8 __asm__("x8") = n;
    register long x0 __asm__("x0") = a;
    __asm__ volatile("svc #0" : "+r"(x0) : "r"(x8) : "memory");
    return x0;
}
#else
#error "probe_c raw mode: x86_64 or aarch64 only"
#endif

static void wait_ok(pid_t pid, const char *what) {
    int st;
    if (waitpid(pid, &st, 0) != pid || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "probe_c: child %s (pid %d) exited badly (status 0x%x)\n", what, (int)pid, st);
        exit(1);
    }
}

static void noise_phases(int K) {
    char buf[4096];
    memset(buf, 'n', sizeof buf);
    struct iovec iv[2] = {{buf, 100}, {buf, 412}};
    int fd = openf("noise", O_RDWR | O_TRUNC);
    for (int i = 0; i < 3 * K; i++) ck(fcntl(fd, F_GETFL), "F_GETFL");
    for (int i = 0; i < K; i++) ck(fcntl(fd, F_SETFL, (i & 1) ? O_NONBLOCK : 0), "F_SETFL");
    for (int i = 0; i < K; i++) ck(write(fd, buf, 512), "write");
    for (int i = 0; i < K; i++) ck(__write(fd, buf, 512), "__write");
    for (int i = 0; i < K; i++) ck(pwrite(fd, buf, 512, 8192), "pwrite");
    for (int i = 0; i < K; i++) ck(pwrite64(fd, buf, 512, 8192), "pwrite64");
    for (int i = 0; i < K; i++) ck(writev(fd, iv, 2), "writev");
    for (int i = 0; i < K; i++) ck(pwritev2(fd, iv, 2, 0, 0), "pwritev2 0");
    for (int i = 0; i < K; i++) ck(pwritev64v2(fd, iv, 2, 0, 0), "pwritev64v2 0");
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
    mark(17);
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

/* One child per exec route, each running "spawned" K. via names match V1_EXEC_VIA in syncshim.h. */
static const char *const ROUTES[] = {"posix_spawn", "posix_spawnp", "execve", "execv", "execvp", "execvpe",
                                     "execl", "execle", "execlp", "fexecve", "execveat"};
#define NROUTES ((int)(sizeof ROUTES / sizeof *ROUTES))
static pid_t run_route(int r, int K) {
    char kbuf[32];
    snprintf(kbuf, sizeof kbuf, "%d", K);
    char *av[] = {SELF, "spawned", kbuf, (char *)DIR_, NULL};
    pid_t pid;
    if (r == 0 || r == 1) {
        int e = r == 0 ? posix_spawn(&pid, SELF, NULL, NULL, av, environ)
                       : posix_spawnp(&pid, SELF, NULL, NULL, av, environ);
        if (e != 0) { errno = e; die(ROUTES[r]); }
        return pid;
    }
    pid = fork();
    if (pid < 0) die("fork");
    if (pid > 0) return pid;
    switch (r) {
    case 2: execve(SELF, av, environ); break;
    case 3: execv(SELF, av); break;
    case 4: execvp(SELF, av); break;
    case 5: execvpe(SELF, av, environ); break;
    case 6: execl(SELF, SELF, "spawned", kbuf, DIR_, (char *)NULL); break;
    case 7: execle(SELF, SELF, "spawned", kbuf, DIR_, (char *)NULL, environ); break;
    case 8: execlp(SELF, SELF, "spawned", kbuf, DIR_, (char *)NULL); break;
    case 9: {
        int fd = open(SELF, O_RDONLY | O_CLOEXEC);
        if (fd >= 0) fexecve(fd, av, environ);
        break;
    }
    case 10: execveat(AT_FDCWD, SELF, av, environ, 0); break;
    }
    fprintf(stderr, "probe_c: route %s failed: %s\n", ROUTES[r], strerror(errno));
    _exit(127);
}

/* Its own function, so no local of run_full is live across the vfork (-Wclobbered). */
static pid_t vfork_execve(char *const av[]) {
    pid_t p = vfork();
    if (p == 0) {
        execve(av[0], av, environ);
        _exit(127);
    }
    return p;
}

static int run_full(int K) {
    char buf[65536], p[4096];
    memset(buf, 'x', sizeof buf);
    struct iovec iv[2] = {{buf, 100}, {buf, 412}};
    int fd = openf("a", O_RDWR | O_TRUNC);
    ck(write(fd, buf, sizeof buf), "write a");
    mark(1); for (int i = 0; i < K; i++) ck(fsync(fd), "fsync");
    mark(2); for (int i = 0; i < K; i++) ck(fdatasync(fd), "fdatasync");
    const unsigned WB = SYNC_FILE_RANGE_WAIT_BEFORE, W = SYNC_FILE_RANGE_WRITE, WA = SYNC_FILE_RANGE_WAIT_AFTER;
    mark(3);
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, WB | W | WA), "sfr WB|W|WA");
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, W | WA), "sfr W|WA");
    mark(4);
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, W), "sfr W");
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, WB | W), "sfr WB|W");
    mark(5);
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, WB), "sfr WB");
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, WA), "sfr WA");
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, WB | WA), "sfr WB|WA");
    for (int i = 0; i < K; i++) ck(sync_file_range(fd, 0, 0, 0), "sfr 0");
    mark(6); for (int i = 0; i < K; i++) ck(syncfs(fd), "syncfs");
    char *m = mmap(NULL, sizeof buf, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (m == MAP_FAILED) die("mmap");
    mark(7); for (int i = 0; i < K; i++) { m[i % 4096] ^= 1; ck(msync(m, sizeof buf, MS_SYNC), "msync SYNC"); }
    mark(8);
    for (int i = 0; i < K; i++) { m[i % 4096] ^= 1; ck(msync(m, sizeof buf, MS_ASYNC), "msync ASYNC"); }
    for (int i = 0; i < K; i++) ck(msync(m, sizeof buf, MS_INVALIDATE), "msync INVALIDATE");
    munmap(m, sizeof buf);
    mark(9);
    noise_phases(K);

    mark(10); /* O_DSYNC fd: every write entry point (10) */
    int d = openf("dsync", O_WRONLY | O_TRUNC | O_DSYNC);
    for (int i = 0; i < K; i++) ck(write(d, buf, 512), "dsync write");
    for (int i = 0; i < K; i++) ck(__write(d, buf, 512), "dsync __write");
    for (int i = 0; i < K; i++) ck(pwrite(d, buf, 512, 4096), "dsync pwrite");
    for (int i = 0; i < K; i++) ck(pwrite64(d, buf, 512, 4096), "dsync pwrite64");
    for (int i = 0; i < K; i++) ck(__pwrite64(d, buf, 512, 4096), "dsync __pwrite64");
    int d2 = dup(d);
    for (int i = 0; i < K; i++) ck(writev(d2, iv, 2), "dsync writev via dup");
    close(d2);
    for (int i = 0; i < K; i++) ck(pwritev(d, iv, 2, 0), "dsync pwritev");
    for (int i = 0; i < K; i++) ck(pwritev64(d, iv, 2, 0), "dsync pwritev64");
    for (int i = 0; i < K; i++) ck(pwritev2(d, iv, 2, 0, 0), "dsync pwritev2");
    for (int i = 0; i < K; i++) ck(pwritev64v2(d, iv, 2, 0, 0), "dsync pwritev64v2");
    close(d);

    mark(11); /* plain fds that reuse the numbers just closed: 0 */
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

    mark(14); /* per-call RWF_*; the stronger of fd mode and call flag wins */
    int pv = openf("plainv2", O_WRONLY | O_TRUNC);
    for (int i = 0; i < K; i++) ck(pwritev2(pv, iv, 2, 0, RWF_DSYNC), "pwritev2 RWF_DSYNC");
    for (int i = 0; i < K; i++) ck(pwritev2(pv, iv, 2, 0, RWF_SYNC), "pwritev2 RWF_SYNC");
    for (int i = 0; i < K; i++) ck(pwritev64v2(pv, iv, 2, 0, RWF_DSYNC), "pwritev64v2 RWF_DSYNC");
    for (int i = 0; i < K; i++) ck(pwritev64v2(pv, iv, 2, 0, RWF_SYNC), "pwritev64v2 RWF_SYNC");
    close(pv);
    int d3 = openf("dsync2", O_WRONLY | O_TRUNC | O_DSYNC);
    for (int i = 0; i < K; i++) ck(pwritev2(d3, iv, 2, 0, RWF_SYNC), "dsync pwritev2 RWF_SYNC");
    for (int i = 0; i < K; i++) ck(pwritev2(d3, iv, 2, 0, RWF_DSYNC), "dsync pwritev2 RWF_DSYNC");
    close(d3);
    int s3 = openf("osync3", O_WRONLY | O_TRUNC | O_SYNC);
    for (int i = 0; i < K; i++) ck(pwritev2(s3, iv, 2, 0, RWF_DSYNC), "osync pwritev2 RWF_DSYNC");
    close(s3);

    mark(15); /* every open and dup variant hands on O_DSYNC (18 variants) */
    pathof("dsync", p, sizeof p);
    const int fl = O_WRONLY | O_DSYNC;
    int v[18];
    v[0] = open(p, fl);
    v[1] = open64(p, fl);
    v[2] = openat(AT_FDCWD, p, fl);
    v[3] = openat64(AT_FDCWD, p, fl);
    v[4] = __open(p, fl);
    v[5] = __open64(p, fl);
    v[6] = __open_2(p, fl);
    v[7] = __open64_2(p, fl);
    v[8] = __openat_2(AT_FDCWD, p, fl);
    v[9] = __openat64_2(AT_FDCWD, p, fl);
    v[10] = fcntl(v[0], F_DUPFD, 100);
    v[11] = fcntl(v[0], F_DUPFD_CLOEXEC, 100);
    v[12] = fcntl64(v[0], F_DUPFD, 100);
    v[13] = __fcntl(v[0], F_DUPFD, 100);
    v[14] = dup(v[0]);
    v[15] = dup2(v[0], 200);
    v[16] = __dup2(v[0], 201);
    v[17] = dup3(v[0], 202, O_CLOEXEC);
    for (int j = 0; j < 18; j++) {
        if (v[j] < 0) { fprintf(stderr, "probe_c: open/dup variant %d failed: %s\n", j, strerror(errno)); return 1; }
        for (int i = 0; i < K; i++) ck(write(v[j], buf, 512), "variant write");
    }
    for (int j = 0; j < 18; j++) close(v[j]);

    mark(16);
    sync();

    clone_phase(K); /* sets mark 17 itself, after its setup fsync */
    close(fd);

    /* 1000+i: cross-process marks. The parent sets the mark; a forked child does the flushes. */
    mark(0);
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

    pid_t rp[NROUTES];
    for (int r = 0; r < NROUTES; r++) {
        mark(21 + (uint64_t)r);
        rp[r] = run_route(r, K);
        wait_ok(rp[r], ROUTES[r]);
    }
    char kbuf[32];
    snprintf(kbuf, sizeof kbuf, "%d", K);
    char *av[] = {SELF, "spawned", kbuf, (char *)DIR_, NULL};
    mark(32);
    pid_t vf = vfork_execve(av);
    if (vf < 0) die("vfork");
    wait_ok(vf, "vfork+execve");
    mark(33);
    char cmd[8192];
    snprintf(cmd, sizeof cmd, "exec '%s' spawned %d '%s'", SELF, K, DIR_);
    char *shv[] = {"/bin/sh", "-c", cmd, NULL};
    pid_t sh;
    int e = posix_spawn(&sh, "/bin/sh", NULL, NULL, shv, environ);
    if (e != 0) { errno = e; die("posix_spawn /bin/sh"); }
    wait_ok(sh, "/bin/sh -c exec");
    mark(0);
    printf("probe_c full K=%d root=%d fork_child=%d vfork=%d via_sh=%d", K, (int)getpid(), (int)pid, (int)vf, (int)sh);
    for (int r = 0; r < NROUTES; r++) printf(" route_%s=%d", ROUTES[r], (int)rp[r]);
    printf("\n");
    return 0;
}

static int run_raw(int K) {
    char buf[4096];
    memset(buf, 'r', sizeof buf);
    int fd = openf("raw", O_RDWR | O_TRUNC);
    ck(write(fd, buf, sizeof buf), "write raw");
    int d = openf("rawdsync", O_WRONLY | O_TRUNC | O_DSYNC);
    mark(1); for (int i = 0; i < K; i++) ck(syscall(SYS_fsync, fd), "syscall(2) fsync");
    mark(2); for (int i = 0; i < K; i++) ck(syscall(SYS_fdatasync, fd), "syscall(2) fdatasync");
    mark(3);
    for (int i = 0; i < K; i++) {
        long r = rawsys1(SYS_fsync, fd);
        if (r < 0) { errno = (int)-r; die("raw fsync"); }
    }
    mark(4); for (int i = 0; i < K; i++) ck(syscall(SYS_pwrite64, d, buf, 512, 0), "syscall(2) pwrite64 O_DSYNC");
    mark(5); for (int i = 0; i < K; i++) ck(fsync(fd), "libc fsync (control)");
    mark(6); for (int i = 0; i < K; i++) ck(pwrite64(d, buf, 512, 0), "libc pwrite64 O_DSYNC (control)");
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
    printf("probe_c sqlite root=%d\n", (int)getpid());
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: probe_c full|noise|spawned|raw|sqlite|killme|execstatic|fanout|setfl-truth K dir [static]\n");
        return 2;
    }
    const char *mode = argv[1];
    int K = atoi(argv[2]);
    DIR_ = argv[3];
    if (K < 1 || K > 250) { fprintf(stderr, "probe_c: K must be 1..250\n"); return 2; }
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
    if (!strcmp(mode, "spawned")) { spawned(K); return 0; }
    const char *why = "?";
    if (getenv("SYNCSHIM_RUN")) {
        H = v1_client(&why);
        if (!H) { fprintf(stderr, "probe_c: map run: %s\n", why); return 1; }
    }
    if (!strcmp(mode, "full")) return run_full(K);
    if (!strcmp(mode, "noise")) {
        mark(9);
        noise_phases(K);
        mark(0);
        printf("probe_c noise K=%d root=%d\n", K, (int)getpid());
        return 0;
    }
    if (!strcmp(mode, "raw")) return run_raw(K);
    if (!strcmp(mode, "sqlite")) return run_sqlite();
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
        char *sav[] = {argv[4], "spawned", kbuf, (char *)DIR_, NULL};
        char *dav[] = {SELF, "spawned", kbuf, (char *)DIR_, NULL};
        char *noenv[] = {"PATH=/usr/bin:/bin", NULL};
        mark(1);
        pid_t a;
        int e = posix_spawn(&a, argv[4], NULL, NULL, sav, environ);
        if (e != 0) { errno = e; die("posix_spawn static"); }
        wait_ok(a, "posix_spawn static");
        mark(2);
        pid_t b = fork();
        if (b < 0) die("fork");
        if (b == 0) { execv(argv[4], sav); _exit(127); }
        wait_ok(b, "fork+execv static");
        mark(3);
        pid_t c = fork();
        if (c < 0) die("fork");
        if (c == 0) { execve(SELF, dav, noenv); _exit(127); }
        wait_ok(c, "fork+execve without LD_PRELOAD");
        mark(0);
        printf("probe_c execstatic K=%d root=%d spawned_static=%d forkexec_static=%d noenv=%d\n", K, (int)getpid(),
               (int)a, (int)b, (int)c);
        return 0;
    }
    if (!strcmp(mode, "fanout")) {
        char kbuf[32];
        snprintf(kbuf, sizeof kbuf, "%d", K);
        char *av[] = {SELF, "spawned", kbuf, (char *)DIR_, NULL};
        pid_t c[3];
        for (int i = 0; i < 3; i++) {
            int e = posix_spawn(&c[i], SELF, NULL, NULL, av, environ);
            if (e != 0) { errno = e; die("posix_spawn"); }
            wait_ok(c[i], "fanout child");
        }
        printf("probe_c fanout K=%d root=%d c1=%d c2=%d c3=%d\n", K, (int)getpid(), (int)c[0], (int)c[1], (int)c[2]);
        return 0;
    }
    fprintf(stderr, "probe_c: unknown mode %s\n", mode);
    return 2;
}
#endif /* !PROBE_STATIC */
