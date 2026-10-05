/* v1ctl.c -- create, read and remove V1 flush-counter runs on Linux (see syncshim.h).
 *
 *   v1ctl create <run> [--slots N] [--execs N] [--events N]   make the shm (refuses if it exists)
 *   v1ctl report <run> [--json] [--allow-incomplete|-go|-live|-uncounted|-zero|-inflight]
 *   v1ctl events <run>                            every completed event, TSV
 *   v1ctl bymark <run>                            completed events grouped by (slot, mark, kind), TSV
 *   v1ctl mark <run> <u64>                        set the run mark (scripts; the load generator uses the API)
 *   v1ctl rm <run>
 *
 * report exit codes -- a run that counted nothing has not passed, and a process that was not counted is never 0.
 * The first that applies wins; each --allow-X waives only its own condition, and a report that is 0 only because of
 * a waiver says "WAIVED: <conditions>" (JSON "waived": [...]), never "ok". Counts are printed either way:
 *   2  run missing or malformed (create: also when /dev/shm cannot reserve the whole object)
 *   4  the root pid recorded by v1run never attached (a static binary such as a CGO_ENABLED=0 Go build, a setuid
 *      binary, a non-glibc binary): NO COUNT for it
 *   3  no process attached
 *   5  VOID: slot overflow, exec-table overflow, or writes on fds the O_SYNC tracker cannot see (fd_untracked).
 *      Per-process counts or the exec guard below cannot be trusted. Not waivable.
 *   12 FOREIGN: a slot was claimed in another pid namespace, where its pid and liveness mean something else.
 *      Not waivable: report from inside that namespace.
 *   10 LIVE: a counted process (pid + start time, any thread not a zombie) is still running, or a slot claim never
 *      finished; the counts can still grow (--allow-live: a live snapshot)
 *   6  an image the shim saw being exec'd or posix_spawned never attached: NO COUNT for that process
 *   7  a Go binary attached: Go makes raw syscalls, so its counts are NOT its flushes (--allow-go for a cross-check)
 *   9  UNCOUNTED: calls of a counted kind went through syscall(2) (missed[], not in the counts), io_uring or Linux
 *      AIO was set up or submitted, or the shim could not resolve one of its real functions (--allow-uncounted)
 *   8  ZERO: no attached process made one FLUSH-class call (counted or missed) -- the wrong scope (a server outside
 *      the v1run tree) reads exactly like this, even when the tree made clone or writeback calls (--allow-zero)
 *   11 INFLIGHT: counted calls were entered and never returned (a SIGKILL or a thread cancel inside one): whether
 *      they reached the disk is unknown (--allow-inflight)
 *   5  INCOMPLETE: dropped or unfinished events. The counts are exact; per-mark attribution is not
 *      (--allow-incomplete)
 */
#include "syncshim.h"
#include <dirent.h>
#include <inttypes.h>
#include <signal.h>

static int usage(void) {
    fprintf(stderr, "usage: v1ctl create|report|events|bymark|mark|rm <run> ...\n");
    return 2;
}

static int cmd_create(const char *run, int argc, char **argv) {
    uint32_t nslots = 65536, exec_cap = 65536, ev_cap = 1u << 20;
    for (int i = 0; i < argc; i++) {
        if (!strcmp(argv[i], "--slots") && i + 1 < argc) nslots = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--execs") && i + 1 < argc) exec_cap = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--events") && i + 1 < argc) ev_cap = (uint32_t)strtoul(argv[++i], NULL, 10);
        else return usage();
    }
    if (nslots < 2 || exec_cap < 1 || ev_cap < 1) {
        fprintf(stderr, "v1ctl: --slots >= 2, --execs >= 1, --events >= 1\n");
        return 2;
    }
    char name[40];
    if (v1_shm_name(run, name, sizeof name) != 0) { fprintf(stderr, "v1ctl: bad run name\n"); return 2; }
    int fd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, 0600);
    if (fd < 0) { fprintf(stderr, "v1ctl: create %s: %s\n", name, strerror(errno)); return 2; }
    size_t sz = v1_total_size(nslots, exec_cap, ev_cap);
    if (ftruncate(fd, (off_t)sz) != 0) {
        fprintf(stderr, "v1ctl: ftruncate: %s\n", strerror(errno));
        close(fd);
        shm_unlink(name);
        return 2;
    }
    /* Reserve every page now: a sparse object on a too-small /dev/shm would otherwise SIGBUS the measured program
     * at its first touch of a fresh slot or event, long after this create said yes. */
    int fe = posix_fallocate(fd, 0, (off_t)sz);
    if (fe != 0) {
        fprintf(stderr, "v1ctl: cannot reserve %zu bytes for %s in /dev/shm: %s (smaller --slots/--execs/--events, or "
                        "a larger /dev/shm)\n", sz, name, strerror(fe));
        close(fd);
        shm_unlink(name);
        return 2;
    }
    v1_hdr *h = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (h == MAP_FAILED) { shm_unlink(name); return 2; }
    /* tmpfs pages arrive zeroed; set the header, magic last of all. */
    h->version = V1_VERSION;
    h->hdr_size = sizeof(v1_hdr);
    h->total_size = sz;
    h->nslots = nslots;
    h->exec_cap = exec_cap;
    h->ev_cap = ev_cap;
    h->created_ns = v1_now();
    __atomic_store_n(&h->magic, V1_MAGIC, __ATOMIC_SEQ_CST);
    printf("created %s slots=%u execs=%u events=%u bytes=%zu\n", name, nslots, exec_cap, ev_cap, sz);
    return 0;
}

static v1_hdr *open_run(const char *run) {
    const char *why = "?";
    v1_hdr *h = v1_map(run, &why);
    if (!h) fprintf(stderr, "v1ctl: run %s: %s\n", run, why);
    return h;
}

static uint64_t used_slots(v1_hdr *h) {
    uint64_t u = __atomic_load_n(&h->slots_used, __ATOMIC_SEQ_CST);
    return u + 1 > h->nslots ? h->nslots - 1 : u; /* real slots are 1..u */
}
static uint64_t stored_events(v1_hdr *h) {
    uint64_t n = __atomic_load_n(&h->ev_next, __ATOMIC_SEQ_CST);
    return n > h->ev_cap ? h->ev_cap : n;
}
static uint64_t stored_execs(v1_hdr *h) {
    uint64_t n = __atomic_load_n(&h->exec_next, __ATOMIC_SEQ_CST);
    return n > h->exec_cap ? h->exec_cap : n;
}

/* Read /proc/<path>/stat: 1 = read (state, start filled), 0 = the process does not exist, -1 = unknown. */
static int read_stat(const char *path, char *st, uint64_t *start) {
    char buf[1024];
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return (errno == ENOENT || errno == ESRCH) ? 0 : -1;
    ssize_t n = read(fd, buf, sizeof buf);
    int e = errno;
    close(fd);
    if (n < 0) return e == ESRCH ? 0 : -1; /* the process went away between open and read */
    return (n > 0 && v1_parse_stat(buf, (size_t)n, st, start) == 0) ? 1 : -1;
}
/* Is the process a slot names still running? The same pid with the same start time, and not every one of its
 * threads a zombie: a thread-group leader that called pthread_exit is a zombie while its other threads run.
 * Unknown is never "finished": a stat that cannot be read, or a slot with no start time, counts as alive. */
static int slot_alive(const v1_slot *s) {
    char path[96];
    snprintf(path, sizeof path, "/proc/%d/stat", s->pid);
    char st = '?';
    uint64_t t = 0;
    int got = read_stat(path, &st, &t);
    if (got == 0) {
        /* /proc says no such process -- but hidepid hides other users' processes from a reader without
         * CAP_SYS_PTRACE (even euid 0), and kill(pid, 0) is not filtered by it: dead only if kill says ESRCH. */
        if (kill(s->pid, 0) == 0 || errno == EPERM) return 1;
        return 0;
    }
    if (got < 0) return 1;
    if (s->start_ticks != 0 && t != s->start_ticks) return 0; /* the pid now names another process */
    if (st != 'Z' && st != 'X' && st != 'x') return 1;
    snprintf(path, sizeof path, "/proc/%d/task", s->pid);
    DIR *d = opendir(path);
    if (!d) return errno == ENOENT ? 0 : 1;
    int alive = 0;
    struct dirent *de;
    while (!alive && (de = readdir(d)) != NULL) {
        if (de->d_name[0] < '0' || de->d_name[0] > '9') continue;
        char tp[320], ts = '?';
        uint64_t tt = 0;
        snprintf(tp, sizeof tp, "/proc/%d/task/%s/stat", s->pid, de->d_name);
        int g = read_stat(tp, &ts, &tt);
        if (g < 0 || (g > 0 && ts != 'Z' && ts != 'X' && ts != 'x')) alive = 1;
    }
    closedir(d);
    return alive;
}
static uint64_t self_pidns(void) {
    struct stat st;
    return stat("/proc/self/ns/pid", &st) == 0 ? (uint64_t)st.st_ino : 0;
}
/* Can the liveness check trust /proc? It reads pids through the /proc MOUNT, which belongs to the namespace that
 * mounted it, not necessarily to this process's (nsenter -p without -m, unshare --pid without --mount-proc), and
 * which can hide other users' processes (hidepid). Returns NULL if it can, else why not. */
static const char *proc_untrusted(void) {
    char buf[32];
    ssize_t n = readlink("/proc/self", buf, sizeof buf - 1);
    if (n <= 0) return "/proc/self is unreadable";
    buf[n] = 0;
    if (strtol(buf, NULL, 10) != (long)getpid()) return "/proc is another pid namespace's mount";
    if (geteuid() == 0) return NULL; /* root sees every process whatever hidepid says */
    FILE *f = fopen("/proc/self/mountinfo", "re");
    if (!f) return "/proc/self/mountinfo is unreadable";
    char line[4096];
    const char *why = NULL;
    while (!why && fgets(line, sizeof line, f)) {
        /* "id parent maj:min root mountpoint opts ... - fstype source superopts" */
        char mp[1024];
        if (sscanf(line, "%*s %*s %*s %*s %1023s", mp) != 1 || strcmp(mp, "/proc") != 0) continue;
        const char *h = strstr(line, "hidepid=");
        if (h && strncmp(h, "hidepid=0", 9) != 0 && strncmp(h, "hidepid=off", 11) != 0)
            why = "/proc hides other users' processes (hidepid)";
    }
    fclose(f);
    return why;
}

static void json_str(const char *s) {
    putchar('"');
    for (; *s; s++) {
        if (*s == '"' || *s == '\\') { putchar('\\'); putchar(*s); }
        else if ((unsigned char)*s < 0x20) printf("\\u%04x", *s);
        else putchar(*s);
    }
    putchar('"');
}
static void json_counts(const uint64_t *c) {
    putchar('{');
    for (int k = 0; k < V1K_NKINDS; k++) printf("%s\"%s\":%" PRIu64, k ? "," : "", V1_KIND_NAMES[k], c[k]);
    putchar('}');
}
static uint64_t sum_range(const uint64_t *c, int lo, int hi) {
    uint64_t s = 0;
    for (int k = lo; k < hi; k++) s += c[k];
    return s;
}

/* Did the image an exec record names attach? A slot with that pid, attached at or after the record's time. */
static int exec_attached(v1_hdr *h, const v1_exec *x, uint64_t nslot) {
    v1_slot *sl = v1_slots(h);
    for (uint64_t i = 1; i <= nslot; i++)
        if (__atomic_load_n(&sl[i].pid, __ATOMIC_ACQUIRE) == x->pid && sl[i].attach_ns >= x->t_ns) return 1;
    return 0;
}
static const char *exec_state(int st) {
    return st == V1X_PENDING ? "exec" : st == V1X_FAILED ? "failed" : st == V1X_SPAWNED ? "spawned" : "unwritten";
}

static int cmd_report(const char *run, int argc, char **argv) {
    int json = 0, allow_inc = 0, allow_go = 0, allow_live = 0, allow_unc = 0, allow_zero = 0, allow_infl = 0;
    for (int i = 0; i < argc; i++) {
        if (!strcmp(argv[i], "--json")) json = 1;
        else if (!strcmp(argv[i], "--allow-incomplete")) allow_inc = 1;
        else if (!strcmp(argv[i], "--allow-go")) allow_go = 1;
        else if (!strcmp(argv[i], "--allow-live")) allow_live = 1;
        else if (!strcmp(argv[i], "--allow-uncounted")) allow_unc = 1;
        else if (!strcmp(argv[i], "--allow-zero")) allow_zero = 1;
        else if (!strcmp(argv[i], "--allow-inflight")) allow_infl = 1;
        else return usage();
    }
    v1_hdr *h = open_run(run);
    if (!h) return 2;
    v1_slot *sl = v1_slots(h);
    v1_event *ev = v1_events(h);
    v1_exec *xs = v1_execs(h);
    uint64_t nslot = used_slots(h), nev = stored_events(h), nx = stored_execs(h), incomplete = 0;
    uint64_t exec_next = h->exec_next, exec_dropped = exec_next > h->exec_cap ? exec_next - h->exec_cap : 0;
    for (uint64_t i = 0; i < nev; i++)
        if (__atomic_load_n(&ev[i].kind, __ATOMIC_ACQUIRE) == 0) incomplete++;
    uint64_t tot[V1K_NKINDS] = {0}, miss[V1K_NKINDS] = {0}, fd_untracked = 0, unresolved = 0, async_io = 0;
    uint64_t inflight = 0, my_ns = self_pidns();
    int attached = 0, root_seen = 0, go_slots = 0, live = 0, claiming = 0, foreign = 0;
    unsigned char *alive = calloc(nslot + 1, 1);
    if (!alive) return 2;
    for (uint64_t i = 0; i <= nslot; i++) {
        if (i > 0 && __atomic_load_n(&sl[i].pid, __ATOMIC_ACQUIRE) == 0) {
            claiming++; /* claimed, never published: a process mid-claim now, or one that died in its claim */
            continue;
        }
        if (i > 0) {
            attached++;
            if (h->root_pid && sl[i].pid == h->root_pid) root_seen = 1;
            if (sl[i].flags & V1_SLOT_GO) go_slots++;
            if (sl[i].pidns && sl[i].pidns != my_ns) foreign++;
            alive[i] = (unsigned char)slot_alive(&sl[i]);
            live += alive[i];
        }
        for (int k = 0; k < V1K_NKINDS; k++) { tot[k] += sl[i].count[k]; miss[k] += sl[i].missed[k]; }
        fd_untracked += sl[i].fd_untracked;
        unresolved |= sl[i].unresolved;
        async_io += sl[i].async_io;
        inflight += sl[i].inflight;
    }
    /* a slot claimed while the scan ran (a fork child of a process just read as dead) makes the snapshot stale */
    if (used_slots(h) != nslot) claiming++;
    const char *proc_bad = proc_untrusted();
    uint64_t unattached = 0, unwritten = 0;
    for (uint64_t i = 0; i < nx; i++) {
        int st = __atomic_load_n(&xs[i].state, __ATOMIC_ACQUIRE);
        if (st == 0) unwritten++;
        else if ((st == V1X_PENDING || st == V1X_SPAWNED) && !exec_attached(h, &xs[i], nslot)) unattached++;
    }
    uint64_t overflow = h->slot_overflow, dropped = h->ev_dropped;
    uint64_t missed = sum_range(miss, 0, V1K_NKINDS);
    uint64_t flush_seen = sum_range(tot, 0, V1K_FLUSH_END) + sum_range(miss, 0, V1K_FLUSH_END);
    /* Each check below either refuses (and ends the walk) or, when its --allow-X is given, is recorded as waived. */
    const char *waived[8];
    int nw = 0, rc = 0;
    const char *verdict = "ok";
#define V1_CHECK(cond, waive, code, text, name)                       \
    if (rc == 0 && (cond)) {                                            \
        if (waive) waived[nw++] = (name);                               \
        else { rc = (code); verdict = (text); }                         \
    }
    V1_CHECK(h->root_pid && !root_seen, 0, 4,
             "REFUSED: root pid never attached (static, setuid or non-glibc binary): NO COUNT", "")
    V1_CHECK(attached == 0, 0, 3, "REFUSED: no process attached (nothing was counted)", "")
    V1_CHECK(overflow || exec_dropped || fd_untracked, 0, 5,
             "VOID: slot overflow, exec-table overflow or untracked fds; per-process counts and the exec guard cannot "
             "be trusted (totals printed, not waivable)", "")
    V1_CHECK(foreign, 0, 12,
             "FOREIGN: slots from another pid namespace; their pids and liveness mean nothing here (report from inside "
             "that namespace)", "")
    V1_CHECK(proc_bad != NULL, 0, 12,
             "FOREIGN: the /proc this report reads cannot show the counted processes (another pid namespace's mount, "
             "or hidepid); liveness would be read from the wrong processes", "")
    V1_CHECK(live || claiming, allow_live, 10,
             "LIVE: a counted process is still running (or a slot claim never finished); the counts can still grow",
             "live")
    V1_CHECK(unattached || unwritten, 0, 6,
             "REFUSED: an exec'd or spawned image never attached (static/Go/setuid binary or LD_PRELOAD dropped from its "
             "environment): NO COUNT for it", "")
    V1_CHECK(go_slots, allow_go, 7,
             "REFUSED: a Go binary attached; Go makes raw syscalls, so its counts are not its flushes (count it with the "
             "strace counter)", "go")
    V1_CHECK(missed || async_io || unresolved, allow_unc, 9,
             "UNCOUNTED: calls of a counted kind went through syscall(2), or io_uring / Linux AIO was used, or a real "
             "function was unresolved: the counts are short by at least the missed calls", "uncounted")
    V1_CHECK(flush_seen == 0, allow_zero, 8,
             "ZERO: no attached process made a single flush-class call; a server outside the v1run tree reads like this",
             "zero")
    V1_CHECK(inflight, allow_infl, 11,
             "INFLIGHT: counted calls were entered and never returned (the process died, or the thread was cancelled, "
             "inside one): whether they reached the disk is unknown", "inflight")
    V1_CHECK(dropped || incomplete, allow_inc, 5,
             "INCOMPLETE: dropped or unfinished events (counts exact, per-mark attribution not)", "incomplete")
#undef V1_CHECK
    char wv[160] = "";
    for (int i = 0; i < nw; i++) {
        size_t l = strlen(wv);
        snprintf(wv + l, sizeof wv - l, "%s%s", i ? ", " : "", waived[i]);
    }
    char vbuf[256];
    if (rc == 0 && nw) {
        snprintf(vbuf, sizeof vbuf, "WAIVED: %s (the counts are printed; each waived condition still holds)", wv);
        verdict = vbuf;
    }
    if (json) {
        printf("{\"instrument\":\"syncshim\",\"run\":");
        json_str(run);
        printf(",\"verdict\":");
        json_str(verdict);
        printf(",\"waived\":[");
        for (int i = 0; i < nw; i++) { printf("%s", i ? "," : ""); json_str(waived[i]); }
        printf("],\"rc\":%d,\"root_pid\":%d,\"root_attached\":%s,\"attached\":%d,\"live\":%d,\"claiming\":%d"
               ",\"foreign_ns\":%d,\"proc_untrusted\":%s,\"go_slots\":%d"
               ",\"slot_overflow\":%" PRIu64 ",\"events_claimed\":%" PRIu64 ",\"events_dropped\":%" PRIu64
               ",\"events_incomplete\":%" PRIu64 ",\"inflight\":%" PRIu64 ",\"execs_claimed\":%" PRIu64
               ",\"execs_dropped\":%" PRIu64 ",\"execs_unwritten\":%" PRIu64 ",\"execs_unattached\":%" PRIu64
               ",\"fd_untracked\":%" PRIu64 ",\"unresolved_mask\":%" PRIu64 ",\"async_io\":%" PRIu64
               ",\"mark\":%" PRIu64,
               rc, h->root_pid, root_seen ? "true" : "false", attached, live, claiming, foreign,
               proc_bad ? "true" : "false", go_slots, overflow,
               h->ev_next, dropped, incomplete, inflight, exec_next, exec_dropped, unwritten, unattached, fd_untracked,
               unresolved, async_io, h->mark);
        printf(",\"classes\":{\"flush\":%" PRIu64 ",\"writeback\":%" PRIu64 ",\"clone\":%" PRIu64 "}",
               sum_range(tot, 0, V1K_FLUSH_END), sum_range(tot, V1K_FLUSH_END, V1K_WRITEBACK_END),
               sum_range(tot, V1K_WRITEBACK_END, V1K_NKINDS));
        printf(",\"totals\":");
        json_counts(tot);
        printf(",\"missed\":");
        json_counts(miss);
        printf(",\"slots\":[");
        int first = 1;
        for (uint64_t i = 0; i <= nslot; i++) {
            if (i == 0 && !overflow) continue;
            if (i > 0 && sl[i].pid == 0) continue;
            printf("%s{\"idx\":%" PRIu64 ",\"pid\":%d,\"ppid\":%d,\"go\":%s,\"alive\":%s,\"attach_ns\":%" PRIu64
                   ",\"start_ticks\":%" PRIu64 ",\"pidns\":%" PRIu64 ",\"exe\":",
                   first ? "" : ",", i, sl[i].pid, sl[i].ppid, (sl[i].flags & V1_SLOT_GO) ? "true" : "false",
                   alive[i] ? "true" : "false", sl[i].attach_ns, sl[i].start_ticks, sl[i].pidns);
            json_str(i == 0 ? "(overflow)" : sl[i].exe);
            printf(",\"fd_untracked\":%" PRIu64 ",\"unresolved_mask\":%" PRIu64 ",\"async_io\":%" PRIu64
                   ",\"inflight\":%" PRIu64 ",\"counts\":",
                   sl[i].fd_untracked, sl[i].unresolved, sl[i].async_io, sl[i].inflight);
            json_counts(sl[i].count);
            printf(",\"fails\":");
            json_counts(sl[i].fail);
            printf(",\"missed\":");
            json_counts(sl[i].missed);
            putchar('}');
            first = 0;
        }
        printf("],\"execs\":[");
        for (uint64_t i = 0; i < nx; i++) {
            int st = __atomic_load_n(&xs[i].state, __ATOMIC_ACQUIRE);
            int via = xs[i].via;
            printf("%s{\"pid\":%d,\"by_pid\":%d,\"state\":\"%s\",\"via\":\"%s\",\"attached\":%s,\"path\":", i ? "," : "",
                   xs[i].pid, xs[i].by_pid, exec_state(st), (via > 0 && via < V1_NVIA) ? V1_EXEC_VIA[via] : "?",
                   (st == V1X_PENDING || st == V1X_SPAWNED) && exec_attached(h, &xs[i], nslot) ? "true" : "false");
            char p[sizeof xs[i].path + 1];
            memcpy(p, xs[i].path, sizeof xs[i].path);
            p[sizeof xs[i].path] = 0;
            json_str(p);
            putchar('}');
        }
        printf("]}\n");
    } else {
        printf("run %s: %s\n", run, verdict);
        printf("root_pid %d attached=%s procs=%d live=%d claiming=%d foreign_ns=%d go=%d slot_overflow=%" PRIu64
               " events=%" PRIu64 " dropped=%" PRIu64 " unfinished=%" PRIu64 " inflight=%" PRIu64 " execs=%" PRIu64
               " unattached=%" PRIu64 " fd_untracked=%" PRIu64 " async_io=%" PRIu64 " missed=%" PRIu64 "\n",
               h->root_pid, root_seen ? "yes" : "no", attached, live, claiming, foreign, go_slots, overflow, h->ev_next,
               dropped, incomplete, inflight, exec_next, unattached, fd_untracked, async_io, missed);
        printf("classes: flush=%" PRIu64 " writeback=%" PRIu64 " clone=%" PRIu64 "\n", sum_range(tot, 0, V1K_FLUSH_END),
               sum_range(tot, V1K_FLUSH_END, V1K_WRITEBACK_END), sum_range(tot, V1K_WRITEBACK_END, V1K_NKINDS));
        printf("%-6s %-7s %-7s", "slot", "pid", "ppid");
        for (int k = 0; k < V1K_NKINDS; k++) printf(" %s", V1_KIND_NAMES[k]);
        printf("  exe\n");
        for (uint64_t i = 0; i <= nslot; i++) {
            if (i == 0 && !overflow) continue;
            if (i > 0 && sl[i].pid == 0) continue;
            printf("%-6" PRIu64 " %-7d %-7d", i, sl[i].pid, sl[i].ppid);
            for (int k = 0; k < V1K_NKINDS; k++) printf(" %*" PRIu64, (int)strlen(V1_KIND_NAMES[k]), sl[i].count[k]);
            const char *b = strrchr(sl[i].exe, '/');
            printf("  %s%s%s\n", i == 0 ? "(overflow)" : (b ? b + 1 : sl[i].exe), (sl[i].flags & V1_SLOT_GO) ? " [GO]" : "",
                   alive[i] ? " [LIVE]" : "");
        }
        printf("%-22s", "TOTAL");
        for (int k = 0; k < V1K_NKINDS; k++) printf(" %*" PRIu64, (int)strlen(V1_KIND_NAMES[k]), tot[k]);
        printf("\n%-22s", "MISSED (syscall(2))");
        for (int k = 0; k < V1K_NKINDS; k++) printf(" %*" PRIu64, (int)strlen(V1_KIND_NAMES[k]), miss[k]);
        printf("\n");
    }
    free(alive);
    return rc;
}

static int cmd_events(const char *run) {
    v1_hdr *h = open_run(run);
    if (!h) return 2;
    v1_event *ev = v1_events(h);
    uint64_t n = stored_events(h);
    printf("idx\tt0_ns\tt1_ns\tmark\tpid\ttid\tslot\tfd\tkind\taux\tret\terr\n");
    for (uint64_t i = 0; i < n; i++) {
        int k = __atomic_load_n(&ev[i].kind, __ATOMIC_ACQUIRE);
        if (k == 0) continue;
        printf("%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%d\t%u\t%u\t%d\t%s\t%" PRId64 "\t%d\t%d\n", i,
               ev[i].t0_ns, ev[i].t1_ns, ev[i].mark, ev[i].pid, ev[i].tid, ev[i].slot, ev[i].fd,
               (k - 1) < V1K_NKINDS ? V1_KIND_NAMES[k - 1] : "?", ev[i].aux, ev[i].ret, ev[i].err);
    }
    return h->ev_dropped ? 5 : 0;
}

typedef struct { uint32_t slot; uint64_t mark; int kind; } bm_row;
static int bm_cmp(const void *a, const void *b) {
    const bm_row *x = a, *y = b;
    if (x->slot != y->slot) return x->slot < y->slot ? -1 : 1;
    if (x->mark != y->mark) return x->mark < y->mark ? -1 : 1;
    return x->kind - y->kind;
}
static int cmd_bymark(const char *run) {
    v1_hdr *h = open_run(run);
    if (!h) return 2;
    v1_event *ev = v1_events(h);
    uint64_t n = stored_events(h), m = 0;
    bm_row *rows = calloc(n ? n : 1, sizeof *rows);
    if (!rows) return 2;
    for (uint64_t i = 0; i < n; i++) {
        int k = __atomic_load_n(&ev[i].kind, __ATOMIC_ACQUIRE);
        if (k == 0 || k - 1 >= V1K_NKINDS) continue;
        rows[m++] = (bm_row){ev[i].slot, ev[i].mark, k - 1};
    }
    qsort(rows, m, sizeof *rows, bm_cmp);
    printf("slot\tpid\tmark_idle\tmark\tkind\tcount\n");
    v1_slot *sl = v1_slots(h);
    for (uint64_t i = 0; i < m;) {
        uint64_t j = i;
        while (j < m && bm_cmp(&rows[i], &rows[j]) == 0) j++;
        printf("%u\t%d\t%d\t%" PRIu64 "\t%s\t%" PRIu64 "\n", rows[i].slot, sl[rows[i].slot].pid,
               (rows[i].mark & V1_MARK_IDLE) ? 1 : 0, rows[i].mark & ~V1_MARK_IDLE, V1_KIND_NAMES[rows[i].kind], j - i);
        i = j;
    }
    return h->ev_dropped ? 5 : 0;
}

int main(int argc, char **argv) {
    if (argc < 3) return usage();
    const char *cmd = argv[1], *run = argv[2];
    if (!strcmp(cmd, "create")) return cmd_create(run, argc - 3, argv + 3);
    if (!strcmp(cmd, "report")) return cmd_report(run, argc - 3, argv + 3);
    if (!strcmp(cmd, "events")) return cmd_events(run);
    if (!strcmp(cmd, "bymark")) return cmd_bymark(run);
    if (!strcmp(cmd, "mark")) {
        if (argc != 4) return usage();
        v1_hdr *h = open_run(run);
        if (!h) return 2;
        v1_set_mark(h, strtoull(argv[3], NULL, 0));
        return 0;
    }
    if (!strcmp(cmd, "rm")) {
        char name[40];
        if (v1_shm_name(run, name, sizeof name) != 0) return 2;
        if (shm_unlink(name) != 0) { fprintf(stderr, "v1ctl: rm %s: %s\n", name, strerror(errno)); return 2; }
        return 0;
    }
    return usage();
}
