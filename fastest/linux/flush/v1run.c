/* v1run.c -- exec a command with the V1 flush counter injected (Linux).
 *
 *   v1run <run> <cmd> [args...]
 *
 * Records its own pid as the run's ROOT pid (exec keeps the pid), prepends syncshim.so (next to this binary, or
 * $V1_SHIM) to LD_PRELOAD, sets SYNCSHIM_RUN=<run>, then execvp's <cmd>. `v1ctl report` refuses (rc 4) if the
 * root never attached -- a static binary (a CGO_ENABLED=0 Go build), a setuid binary, a non-glibc binary. There is
 * deliberately no pre-check of <cmd> here: the report's outcome check is the one guard, and it stays testable.
 * One root per run: refuses if the run already has one.
 */
#include "syncshim.h"
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: v1run <run> <cmd> [args...]\n"); return 2; }
    const char *run = argv[1];
    const char *why = "?";
    v1_hdr *h = v1_map(run, &why);
    if (!h) { fprintf(stderr, "v1run: run %s: %s\n", run, why); return 2; }
    char shim[PATH_MAX];
    const char *env_shim = getenv("V1_SHIM");
    if (env_shim && *env_shim) {
        if (!realpath(env_shim, shim)) { fprintf(stderr, "v1run: V1_SHIM %s: %s\n", env_shim, strerror(errno)); return 2; }
    } else {
        char self[PATH_MAX];
        ssize_t n = readlink("/proc/self/exe", self, sizeof self - 1);
        if (n <= 0) { fprintf(stderr, "v1run: cannot find self\n"); return 2; }
        self[n] = 0;
        snprintf(shim, sizeof shim, "%s/syncshim.so", dirname(self));
    }
    if (access(shim, R_OK) != 0) { fprintf(stderr, "v1run: shim %s: %s\n", shim, strerror(errno)); return 2; }
    if (strchr(shim, ' ') || strchr(shim, ':')) { fprintf(stderr, "v1run: shim path %s has a space or ':'\n", shim); return 2; }
    int32_t expect = 0;
    if (!__atomic_compare_exchange_n(&h->root_pid, &expect, (int32_t)getpid(), 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        fprintf(stderr, "v1run: run %s already has root pid %d; one v1run per run\n", run, expect);
        return 2;
    }
    const char *old = getenv("LD_PRELOAD");
    char pre[2 * PATH_MAX + 2];
    if (old && *old) snprintf(pre, sizeof pre, "%s:%s", shim, old);
    else snprintf(pre, sizeof pre, "%s", shim);
    setenv("LD_PRELOAD", pre, 1);
    setenv("SYNCSHIM_RUN", run, 1);
    execvp(argv[2], argv + 2);
    fprintf(stderr, "v1run: exec %s: %s\n", argv[2], strerror(errno));
    return 127;
}
