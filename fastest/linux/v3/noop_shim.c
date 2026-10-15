/* noop_shim.c -- the V3 fire-check's /etc/ld.so.preload plant (fourth review M4): a library that changes nothing and
 * only notes, when V3_NOOP_MARK names a file, which process loaded it. firecheck.sh names it in /etc/ld.so.preload
 * for two runs: the dynamic build of the probe must load it (its mark) and refuse it (it is mapped), and the static
 * build must never load it (no mark) and run. Fire-check only; built by the workflow:
 *   gcc -O2 -shared -fPIC -o noop_shim.so noop_shim.c
 */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

__attribute__((constructor)) static void v3_noop_mark(void) {
    const char *m = getenv("V3_NOOP_MARK");
    if (!m || !*m) return;
    char comm[64] = "?";
    int fd = open("/proc/self/comm", O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        ssize_t r = read(fd, comm, sizeof comm - 1);
        comm[r > 0 ? r : 0] = 0;
        comm[strcspn(comm, "\n")] = 0;
        close(fd);
    }
    fd = open(m, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return;
    char line[160];
    int n = snprintf(line, sizeof line, "loaded in %s pid %d\n", comm, (int)getpid());
    if (n > 0) {
        ssize_t w = write(fd, line, (size_t)n);
        (void)w;
    }
    close(fd);
}
