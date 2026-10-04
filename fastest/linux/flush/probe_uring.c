/* probe_uring.c -- V1 Linux fire-check probe for a program that links liburing dynamically (as a C/C++ engine
 * using io_uring would): one libc fsync, then K IORING_OP_FSYNC through a ring set up with io_uring_queue_init.
 *
 *   probe_uring K dir
 *
 * liburing >= 2.2 makes its syscalls as raw instructions, so the shim cannot see the ring's fsyncs; it sees the
 * io_uring_queue_init call (liburing.so's exported entry point) and the report must refuse (rc 9, async I/O). The
 * expected result is derived by firecheck.py from this program's construction, never from the counter.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 3) { fprintf(stderr, "usage: probe_uring K dir\n"); return 2; }
    int K = atoi(argv[1]);
    if (K < 1 || K > 250) return 2;
    char p[4096];
    snprintf(p, sizeof p, "%s/uring_lib", argv[2]);
    int fd = open(p, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror("open"); return 1; }
    char b[512] = {0};
    if (write(fd, b, sizeof b) != (ssize_t)sizeof b || fsync(fd) != 0) { perror("write/fsync"); return 1; }
    struct io_uring ring;
    int r = io_uring_queue_init(8, &ring, 0);
    if (r < 0) { fprintf(stderr, "probe_uring: io_uring_queue_init: %s\n", strerror(-r)); return 1; }
    int done = 0;
    for (int i = 0; i < K; i++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) return 1;
        io_uring_prep_fsync(sqe, fd, 0);
        if (io_uring_submit(&ring) != 1) return 1;
        struct io_uring_cqe *cqe;
        if (io_uring_wait_cqe(&ring, &cqe) != 0) return 1;
        if (cqe->res == 0) done++;
        io_uring_cqe_seen(&ring, cqe);
    }
    io_uring_queue_exit(&ring);
    close(fd);
    printf("probe_uring K=%d root=%d ring_fsyncs_done=%d\n", K, (int)getpid(), done);
    return done == K ? 0 : 1;
}
