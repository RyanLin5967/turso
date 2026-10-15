#!/usr/bin/env python3
"""C17: compile the actual conn_close_sync function with test-only driver stubs.

The plant is #define dup c17_dup around the extracted production function.
It forces EMFILE; driver close deliberately overwrites errno. The control uses
a real socketpair and real dup/EOF. PG, MySQL and already-dead connections are
all covered. No test hook or env lookup is compiled into bbload.
UNRUN under QUIET. Run: python3 -B <this file>
"""
from pathlib import Path
import os
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
source = (HERE / "bbload.c").read_text()
start = source.index("static int conn_close_sync(")
end = source.index("\nstatic void conn_close(", start)
function = source[start:end]
prefix = r"""
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
typedef struct { int fd; } driver_t;
typedef struct { driver_t *pg, *my; } conn_t;
static double CLOSE_TIMEOUT_S = 0.1;
static int planted, finished, peer = -1;
static uint64_t now_ns(void) {
    struct timespec t;
    assert(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
    return (uint64_t)t.tv_sec * 1000000000 + (uint64_t)t.tv_nsec;
}
static int PQsocket(driver_t *d) { return d->fd; }
static int mysql_get_socket(driver_t *d) { return d->fd; }
static void finish(driver_t *d) {
    if (d->fd >= 0) assert(close(d->fd) == 0);
    if (peer >= 0) { assert(close(peer) == 0); peer = -1; }
    finished++;
    errno = EINVAL; /* cleanup must not replace the dup failure's errno */
}
static void PQfinish(driver_t *d) { finish(d); }
static void mysql_close(driver_t *d) { finish(d); }
static int c17_dup(int fd) {
    if (planted) { errno = EMFILE; return -1; }
    return dup(fd);
}
#define dup c17_dup
"""
suffix = r"""
#undef dup
int main(void) {
    for (int my = 0; my <= 1; my++) {
        for (int mode = 0; mode < 3; mode++) {
            driver_t d = {-1};
            planted = mode == 1;
            finished = 0;
            if (mode != 2) {
                int fds[2];
                assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
                d.fd = fds[0]; peer = fds[1];
            }
            conn_t c = {my ? NULL : &d, my ? &d : NULL};
            char why[256] = "", expected[256];
            int rc = conn_close_sync(&c, why, sizeof why);
            assert(finished == 1 && c.pg == NULL && c.my == NULL);
            if (planted) {
                snprintf(expected, sizeof expected, "close: dup: %s", strerror(EMFILE));
                if (rc != -1 || strcmp(why, expected)) {
                    fprintf(stderr, "CLAIM %s dup EMFILE: rc=%d why=%s; expected -1 / %s\n",
                            my ? "mysql" : "pg", rc, why, expected);
                    return 1;
                }
            } else {
                if (rc != 0 || why[0]) {
                    fprintf(stderr, "CLAIM %s control mode=%d: rc=%d why=%s\n",
                            my ? "mysql" : "pg", mode, rc, why);
                    return 1;
                }
            }
        }
    }
    puts("C17 PASS: PG and MySQL dup failure, EOF control, no-fd control");
    return 0;
}
"""
with tempfile.TemporaryDirectory(prefix="C17-close-") as tmp:
    c = Path(tmp) / "close.c"
    binary = Path(tmp) / "close"
    c.write_text(prefix + function + suffix)
    subprocess.run([os.environ.get("CC", "clang"), "-std=c11", "-Wall", "-Wextra", "-Werror",
                    str(c), "-o", str(binary)], check=True, timeout=60)
    subprocess.run([str(binary)], check=True, timeout=5)
