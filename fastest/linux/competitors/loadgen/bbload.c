/* bbload.c -- compiled branch-benchmark load generator (PREREG §5 "compiled libpq load generator").
 *
 *   bbload --spec FILE --out DIR --clients C
 *          [--mode closed|open] [--rate R]          open loop: Poisson arrivals, R ops/s in total
 *          [--warmup-s S] [--warmup-ops N]           warm-up ends when both are reached (default 0, 0)
 *          [--warmup OPS:S:MAX_S]                    the same, ending at MAX_S at the latest (0: no limit); PREREG
 *                                                    :210 is min(max(1000 ops, 10 s), 10% of the cap), e.g. 1000:10:180
 *                                                    at an 1800 s cap. summary.json warmup_rule is formatted from
 *                                                    the effective values (%llu:%g:%g); mixing it with --warmup-ops
 *                                                    or --warmup-s is refused (rc 2; MED 7)
 *          [--duration-s S] [--min-ops N]            the measured window ends when both are reached
 *          [--max-ops N]                             closed loop: end the window after exactly N measured ops
 *          [--max-window-s S]                        refuse (exit 3) if min-ops is not reached by then (default 3600);
 *                                                    with --max-ops it is the registered per-run cap: a run it ends
 *                                                    exits 0 with verdict "capped" (capped: true) and its counts;
 *                                                    timedrun.py applies PREREG's tiers (MED 5)
 *          [--run-tag T] [--seed S] [--set k=v]... [--v1-run NAME [--v1-mark-base B]] [--c1b-run NAME]
 *          [--stall-s S] [--allow-errors] [--skip-after]       --skip-after: run no after-step (summary.json skip_after)
 *   With no warm-up at all (OPS, S and MAX_S all 0, the default) the run starts in the measured window, so a
 *   --max-ops N run makes exactly N ops (there is no warm phase to add ops to).
 *
 * One OS thread and one connection per client (C up to 1024+), blocking libpq or MariaDB-connector calls, so each
 * thread timestamps its own operation. Clock: CLOCK_UPTIME_RAW, the clock the V1 shim stamps its events with.
 *
 * Latency is measured from the INTENDED send time. Closed loop: intended = actual send, think time 0. Open loop:
 * each client follows its own Poisson schedule at R/C; a late op still counts from its intended time, and every op
 * intended inside the window is run even after the window closes, so queueing is never hidden (AG9).
 * An operation is the spec's steps run in order; no step is sent before the previous one is acknowledged.
 *
 * Spec file (one directive per line, '#' comments; templates: {c} client, {i} op number of that client,
 * {run} run tag, {rand:a:b} uniform integer in [a,b] where a and b are numbers or var/--set names,
 * {name} any var or --set key):
 *   protocol pg|mysql
 *   connect <conninfo>            per-client home connection (libpq conninfo; mysql: host= port= user= password= db=)
 *   var <name> = <template>       expanded once per op, in order
 *   setup <sql>                   once per connection after connect, untimed
 *   step sql <sql>                timed; must succeed
 *   step write <sql>              timed; must succeed AND affect >= 1 row
 *   step connect <overrides>      timed; open a new connection (home conninfo + overrides) that becomes current
 *   step close                    timed; close the current non-home connection
 *   after sql|write|connect|close untimed, after every op (also after a failed one); recorded as after_ns
 *   after sql-serial <sql>        as after sql, but no two clients run a sql-serial statement at the same time (Dolt's
 *                                 branch deletes: 2.4.1 panics on concurrent DOLT_BRANCH('-d'))
 *   teardown <sql>                once per connection at the end, untimed
 *   c1b <step> <label>            with --c1b-run RUN: an OPSTART "<label>" before the op's first byte and an ACK
 *                                 "<label>" right after step <step> (1-based) is acknowledged, into the C1b trace
 *                                 (tools/c1b); label = "<phase>:<detail>", templates allowed
 * A non-home connection still open at the end of an op is closed, untimed.
 *
 * Output (OUT must not exist): spec.txt, raw.tsv (one row per op), hdr_total.txt and hdr_step<k>.txt (HDR
 * percentile distributions of the measured ops), errors.txt, summary.json.
 * V1 marks (--v1-run): a server stays attached to one V1 run for its whole life, so each invocation marks in its
 * own namespace B (default: (epoch seconds & 0x7fffff) << 32, written to summary.json as v1_mark_base). C=1: op seq
 * k is marked B+k+1 from just before its first byte to its acknowledgement, then V1_MARK_IDLE|(B+k+1) until the
 * next op. C>1: B+1 warm-up, B+2 measured window, B+3 drain. v1_per_op.py reduces them.
 * Exit: 0 ok | 2 usage, spec or connect failure | 3 no measured op succeeded, min-ops not reached, or errors
 * without --allow-errors | 4 stall (no op completed for --stall-s, default 120).
 *
 * LINUX PORT (lane fastest-linux-comp; source artie-research frontier/fastest/tools/loadgen/bbload.c @648ce2929).
 * Every change is an #if block; the macOS branches are the original lines. On Linux:
 *   - clock: CLOCK_MONOTONIC (clock_gettime / clock_nanosleep TIMER_ABSTIME), recorded in summary.json as "clock".
 *     It is the clock bpf_ktime_get_ns() reads, so an eBPF flush counter can share it; a shim on another clock
 *     must rebuild with -DBB_CLOCK=<clock> -DBB_CLOCK_NAME=\"<clock>\" (clock_nanosleep rejects CLOCK_MONOTONIC_RAW).
 *   - the V1/C1b hooks are compiled only with BB_HOOKS=1 (default: 1 on macOS, 0 elsewhere). With them off,
 *     --v1-run and --c1b-run REFUSE (rc 2) instead of running unmarked. BB_HOOKS=1 off macOS is a build error
 *     until the Linux shim lands from lane fastest-linux-flush.
 */
#ifndef BB_HOOKS
#ifdef __APPLE__
#define BB_HOOKS 1
#else
#define BB_HOOKS 0
#endif
#endif
#if BB_HOOKS
#ifndef __APPLE__
#error "BB_HOOKS=1 needs the Linux V1/C1b shim (lane fastest-linux-flush); build with -DBB_HOOKS=0 until it lands"
#endif
#include "../v1/syncshim.h"
#include "../c1b/c1btrace.h"
#else
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
/* Hooks off: the names the call sites use, never reached (V1 stays NULL and HAVE_C1B 0; the flags refuse). */
typedef struct v1_hdr v1_hdr;
typedef struct { int unused; } c1b_client;
#define V1_MARK_IDLE (1ULL << 63)
static inline void v1_set_mark(v1_hdr *h, uint64_t m) { (void)h; (void)m; }
static inline void c1b_opstart(c1b_client *c, uint64_t op, const char *l) { (void)c; (void)op; (void)l; }
static inline void c1b_ack(c1b_client *c, uint64_t op, const char *l) { (void)c; (void)op; (void)l; }
#endif
#include <errno.h>
#include <hdr/hdr_histogram.h>
#include <libpq-fe.h>
#ifdef __APPLE__
#include <mach/mach_time.h>
#endif
#include <math.h>
#include <mysql.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <time.h>

#define MAXSTEPS 8
#define MAXAFTER 8
#define MAXLIST 32
#define MAXERR 64

enum { P_PG = 0, P_MYSQL = 1 };
enum { K_SQL, K_WRITE, K_CONNECT, K_CLOSE };
enum { PH_INIT = 0, PH_WARM = 1, PH_MEAS = 2, PH_DRAIN = 3, PH_ABORT = 4 };

typedef struct { int k; char *t; int serial; } step_t;
typedef struct {
    int proto;
    char *connect;
    int nvar; char *vname[MAXLIST]; char *vtmpl[MAXLIST];
    int nsetup; char *setup[MAXLIST];
    int nstep; step_t step[MAXSTEPS];
    int nafter; step_t after[MAXAFTER];
    int nteardown; char *teardown[MAXLIST];
    char *c1blab[MAXSTEPS];
} spec_t;

typedef struct { int proto; PGconn *pg; MYSQL *my; } conn_t;

typedef struct {
    uint32_t client, seq;
    uint8_t phase, ok, nsteps, pad;
    int16_t err;
    uint64_t intended, start, end, after_end;
    uint64_t step_end[MAXSTEPS];
} oprec;

typedef struct {
    int id;
    pthread_t th;
    conn_t home;
    uint64_t rng;
    oprec *rec;
    size_t nrec, cap;
    int state; /* 0 connecting, 1 ready, -1 failed */
    char err[512];
    /* Linux port addition (not in the Mac original): the server-side id of every connection this client opened --
     * PG: the backend's pid (PQbackendPID), MySQL protocol: the connection id (mysql_thread_id) -- written to
     * backends.tsv, so a flush counter can attribute a backend process's syscalls to this load generator. */
    struct { uint32_t seq; uint8_t kind; long id; } *bk; /* kind 0 = home (seq unused), 1 = a step connect of op seq */
    size_t nbk, capbk;
} client_t;

static spec_t S;
static int C = 1, OPEN_LOOP = 0, ALLOW_ERR = 0;
static int SKIP_AFTER = 0; /* --skip-after: run no after-step (the driver's designated branches, which must stay) */
static double RATE = 0, WARM_S = 0, WARM_MAX_S = 0, DUR_S = 0, MAXWIN_S = 3600, STALL_S = 120;
static char WARM_RULE[64] = "";
static uint64_t WARM_OPS = 0, MIN_OPS = 0, MAX_OPS = 0, SEED = 1, MARKB = 0;
static char RUNTAG[64];
static int NSET; static char *SETK[MAXLIST], *SETV[MAXLIST];
static v1_hdr *V1;
static client_t *CL;
static c1b_client C1B;
static int HAVE_C1B;

static volatile int g_phase = PH_INIT;
static volatile uint64_t g_t0, g_tm0, g_tm1; /* run start, window start, window end (0 = not yet) */
static volatile uint64_t g_warm_claimed, g_meas_claimed, g_completed;
static int g_nready, g_nfailed;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_serial_mu = PTHREAD_MUTEX_INITIALIZER; /* one sql-serial statement at a time, all clients */
static pthread_cond_t g_cv = PTHREAD_COND_INITIALIZER;
static char *g_errs[MAXERR];
static uint64_t g_errn[MAXERR];
static int g_nerr;
static uint64_t g_reconnects;

#ifdef __APPLE__
#define BB_CLOCK_NAME "CLOCK_UPTIME_RAW"
static uint64_t now_ns(void) { return clock_gettime_nsec_np(CLOCK_UPTIME_RAW); }
static mach_timebase_info_data_t TB;
static void sleep_until(uint64_t t_ns) {
    uint64_t n = now_ns();
    if (t_ns <= n) return;
    mach_wait_until(mach_absolute_time() + (t_ns - n) * TB.denom / TB.numer);
}
#else
/* The name is spelled out, never stringified from BB_CLOCK: glibc defines CLOCK_MONOTONIC as 1, so #BB_CLOCK after
 * expansion recorded "clock":"1" (review finding, run 37175962413). An override must name its clock too. */
#ifndef BB_CLOCK
#define BB_CLOCK CLOCK_MONOTONIC
#define BB_CLOCK_NAME "CLOCK_MONOTONIC"
#endif
#ifndef BB_CLOCK_NAME
#error "-DBB_CLOCK=<clock> needs -DBB_CLOCK_NAME=\"<its name>\""
#endif
static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(BB_CLOCK, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}
static void sleep_until(uint64_t t_ns) {
    struct timespec ts = {(time_t)(t_ns / 1000000000ULL), (long)(t_ns % 1000000000ULL)};
    while (clock_nanosleep(BB_CLOCK, TIMER_ABSTIME, &ts, NULL) == EINTR) { }
}
#endif
/* The load generator's own TracerPid (lead review 62430d8bf..b49fb656a MED 4), read at the measured window's start and
 * end into summary.json tracerpid_tm0/tm1: -1 when it cannot be read (no /proc: not Linux), which timedrun.py refuses. */
static int self_tracerpid(void) {
#ifdef __linux__
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return -1;
    char ln[256];
    int tp = -1;
    while (fgets(ln, sizeof ln, f))
        if (sscanf(ln, "TracerPid: %d", &tp) == 1) break;
    fclose(f);
    return tp;
#else
    return -1;
#endif
}
static int g_tp_tm0 = -2, g_tp_tm1 = -2; /* -2: the window never opened / closed */
static uint64_t xs(uint64_t *s) { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; return *s; }
static double unif(uint64_t *s) { return ((xs(s) >> 11) + 0.5) / 9007199254740992.0; }

static int err_index(const char *msg) {
    pthread_mutex_lock(&g_mu);
    int i;
    for (i = 0; i < g_nerr; i++) if (!strcmp(g_errs[i], msg)) break;
    if (i == g_nerr && g_nerr < MAXERR) g_errs[g_nerr++] = strdup(msg);
    if (i < MAXERR) g_errn[i]++;
    pthread_mutex_unlock(&g_mu);
    return i < MAXERR ? i : MAXERR - 1;
}

/* ---------- spec ---------- */
static char *trim(char *s) {
    while (*s == ' ' || *s == '\t') s++;
    char *e = s + strlen(s);
    while (e > s && (e[-1] == ' ' || e[-1] == '\t' || e[-1] == '\n' || e[-1] == '\r')) *--e = 0;
    return s;
}
static int parse_step(char *rest, step_t *st, char *why) {
    char *kw = rest, *arg = rest;
    while (*arg && *arg != ' ' && *arg != '\t') arg++;
    if (*arg) *arg++ = 0;
    arg = trim(arg);
    st->serial = 0;
    if (!strcmp(kw, "sql")) st->k = K_SQL;
    else if (!strcmp(kw, "sql-serial")) { st->k = K_SQL; st->serial = 1; }
    else if (!strcmp(kw, "write")) st->k = K_WRITE;
    else if (!strcmp(kw, "connect")) st->k = K_CONNECT;
    else if (!strcmp(kw, "close")) st->k = K_CLOSE;
    else { sprintf(why, "unknown step kind '%s'", kw); return -1; }
    if (st->k != K_CLOSE && !*arg) { sprintf(why, "step %s needs an argument", kw); return -1; }
    st->t = strdup(arg);
    return 0;
}
static int load_spec(const char *path, char **text) {
    FILE *f = fopen(path, "r");
    if (!f) { fprintf(stderr, "bbload: spec %s: %s\n", path, strerror(errno)); return -1; }
    size_t cap = 1 << 16, len = 0;
    *text = malloc(cap);
    char line[8192], why[256];
    int ln = 0, have_proto = 0;
    S.proto = -1;
    while (fgets(line, sizeof line, f)) {
        ln++;
        size_t l = strlen(line);
        if (len + l + 1 > cap) { cap *= 2; *text = realloc(*text, cap); }
        memcpy(*text + len, line, l + 1);
        len += l;
        char *s = trim(line);
        if (!*s || *s == '#') continue;
        char *kw = s, *rest = s;
        while (*rest && *rest != ' ' && *rest != '\t') rest++;
        if (*rest) *rest++ = 0;
        rest = trim(rest);
        why[0] = 0;
        if (!strcmp(kw, "protocol")) {
            if (!strcmp(rest, "pg")) S.proto = P_PG;
            else if (!strcmp(rest, "mysql")) S.proto = P_MYSQL;
            else sprintf(why, "protocol must be pg or mysql");
            have_proto = 1;
        } else if (!strcmp(kw, "connect")) S.connect = strdup(rest);
        else if (!strcmp(kw, "var")) {
            char *eq = strchr(rest, '=');
            if (!eq || S.nvar >= MAXLIST) sprintf(why, "var needs 'name = template'");
            else { *eq = 0; S.vname[S.nvar] = strdup(trim(rest)); S.vtmpl[S.nvar++] = strdup(trim(eq + 1)); }
        } else if (!strcmp(kw, "setup") && S.nsetup < MAXLIST) S.setup[S.nsetup++] = strdup(rest);
        else if (!strcmp(kw, "teardown") && S.nteardown < MAXLIST) S.teardown[S.nteardown++] = strdup(rest);
        else if (!strcmp(kw, "step")) {
            if (S.nstep >= MAXSTEPS) sprintf(why, "more than %d steps", MAXSTEPS);
            else if (parse_step(rest, &S.step[S.nstep], why) == 0) S.nstep++;
        } else if (!strcmp(kw, "c1b")) {
            char *sp = rest;
            while (*sp && *sp != ' ' && *sp != '\t') sp++;
            if (*sp) *sp++ = 0;
            int k = atoi(rest);
            sp = trim(sp);
            if (k < 1 || k > MAXSTEPS || !*sp) sprintf(why, "c1b needs '<step 1..%d> <label>'", MAXSTEPS);
            else S.c1blab[k - 1] = strdup(sp);
        } else if (!strcmp(kw, "after")) {
            if (S.nafter >= MAXAFTER) sprintf(why, "more than %d after-steps", MAXAFTER);
            else if (parse_step(rest, &S.after[S.nafter], why) == 0) S.nafter++;
        } else sprintf(why, "unknown directive '%s'", kw);
        if (why[0]) { fprintf(stderr, "bbload: %s:%d: %s\n", path, ln, why); fclose(f); return -1; }
    }
    fclose(f);
    if (!have_proto || S.proto < 0 || !S.connect || S.nstep == 0) {
        fprintf(stderr, "bbload: spec needs protocol, connect and at least one step\n");
        return -1;
    }
    return 0;
}

/* Expand {c} {i} {run} {rand:a:b} {var}. Returns -1 on an unknown or malformed placeholder. */
typedef struct { int c; uint32_t i; uint64_t *rng; char *vval[MAXLIST]; int nv; } ctx_t;
static const char *lookup(const char *key, const ctx_t *x) {
    for (int v = 0; v < x->nv; v++) if (!strcmp(S.vname[v], key)) return x->vval[v];
    for (int v = 0; v < NSET; v++) if (!strcmp(SETK[v], key)) return SETV[v];
    return NULL;
}
/* A rand bound: a decimal integer, or the name of a var or --set key holding one. */
static int bound(const char *tok, const ctx_t *x, long long *out) {
    const char *v = tok;
    char *end;
    if (!(*tok >= '0' && *tok <= '9') && *tok != '-') { v = lookup(tok, x); if (!v) return -1; }
    *out = strtoll(v, &end, 10);
    return (*v && !*end) ? 0 : -1;
}
static int expand(const char *t, const ctx_t *x, char *out, size_t sz, char *why) {
    size_t o = 0;
    for (const char *p = t; *p;) {
        if (*p != '{') { if (o + 1 >= sz) goto big; out[o++] = *p++; continue; }
        const char *e = strchr(p, '}');
        if (!e) { sprintf(why, "unclosed '{' in: %.80s", t); return -1; }
        char key[128];
        size_t kl = (size_t)(e - p - 1);
        if (kl >= sizeof key) { sprintf(why, "placeholder too long"); return -1; }
        memcpy(key, p + 1, kl);
        key[kl] = 0;
        char val[1024];
        val[0] = 0;
        if (!strcmp(key, "c")) snprintf(val, sizeof val, "%d", x->c);
        else if (!strcmp(key, "i")) snprintf(val, sizeof val, "%u", x->i);
        else if (!strcmp(key, "run")) snprintf(val, sizeof val, "%s", RUNTAG);
        else if (!strncmp(key, "rand:", 5)) {
            char ta[128], tb[128];
            long long a, b;
            const char *c2 = strchr(key + 5, ':');
            if (!c2 || (size_t)(c2 - key - 5) >= sizeof ta || strlen(c2 + 1) >= sizeof tb) { sprintf(why, "malformed {%s}", key); return -1; }
            memcpy(ta, key + 5, (size_t)(c2 - key - 5));
            ta[c2 - key - 5] = 0;
            strcpy(tb, c2 + 1);
            if (bound(ta, x, &a) || bound(tb, x, &b) || b < a) { sprintf(why, "bad bounds in {%s}", key); return -1; }
            snprintf(val, sizeof val, "%lld", a + (long long)(xs(x->rng) % (uint64_t)(b - a + 1)));
        } else {
            const char *v = lookup(key, x);
            if (!v) { sprintf(why, "unknown placeholder {%s}", key); return -1; }
            snprintf(val, sizeof val, "%s", v);
        }
        size_t vl = strlen(val);
        if (o + vl >= sz) goto big;
        memcpy(out + o, val, vl);
        o += vl;
        p = e + 1;
    }
    out[o] = 0;
    return 0;
big:
    sprintf(why, "expanded text too long");
    return -1;
}

/* ---------- connections ---------- */
typedef struct { char k[64][64]; char v[64][256]; int n; } kv_t;
static int kv_parse(const char *s, kv_t *kv, char *why) {
    char buf[4096], *save = NULL;
    snprintf(buf, sizeof buf, "%s", s);
    /* strtok_r: every client thread parses its own conninfo at once (strtok's hidden state corrupted them). */
    for (char *t = strtok_r(buf, " \t", &save); t; t = strtok_r(NULL, " \t", &save)) {
        char *eq = strchr(t, '=');
        if (!eq) { sprintf(why, "conninfo token '%s' has no '='", t); return -1; }
        *eq = 0;
        int i;
        for (i = 0; i < kv->n; i++) if (!strcmp(kv->k[i], t)) break;
        if (i == kv->n) { if (kv->n == 64) { sprintf(why, "too many keys"); return -1; } kv->n++; }
        snprintf(kv->k[i], 64, "%s", t);
        snprintf(kv->v[i], 256, "%s", eq + 1);
    }
    return 0;
}

static int conn_open(conn_t *x, const char *base, const char *over, char *why, size_t wsz) {
    memset(x, 0, sizeof *x);
    x->proto = S.proto;
    kv_t kv = {.n = 0};
    char w2[256] = "";
    if (S.proto == P_PG) {
        /* Merge with libpq's own parser so an override key replaces the base key. */
        const char *srcs[2] = {base, over};
        for (int s = 0; s < 2; s++) {
            if (!srcs[s] || !*srcs[s]) continue;
            char *perr = NULL;
            PQconninfoOption *o = PQconninfoParse(srcs[s], &perr);
            if (!o) { snprintf(why, wsz, "conninfo: %s", perr ? perr : "?"); PQfreemem(perr); return -1; }
            for (PQconninfoOption *p = o; p->keyword; p++) {
                if (!p->val) continue;
                int i;
                for (i = 0; i < kv.n; i++) if (!strcmp(kv.k[i], p->keyword)) break;
                if (i == kv.n) kv.n++;
                snprintf(kv.k[i], 64, "%s", p->keyword);
                snprintf(kv.v[i], 256, "%s", p->val);
            }
            PQconninfoFree(o);
        }
        const char *keys[65], *vals[65];
        for (int i = 0; i < kv.n; i++) { keys[i] = kv.k[i]; vals[i] = kv.v[i]; }
        keys[kv.n] = vals[kv.n] = NULL;
        x->pg = PQconnectdbParams(keys, vals, 0);
        if (PQstatus(x->pg) != CONNECTION_OK) {
            snprintf(why, wsz, "connect: %s", PQerrorMessage(x->pg));
            PQfinish(x->pg);
            x->pg = NULL;
            return -1;
        }
        return 0;
    }
    if (kv_parse(base, &kv, w2) || (over && kv_parse(over, &kv, w2))) { snprintf(why, wsz, "%s", w2); return -1; }
    const char *host = "127.0.0.1", *user = "root", *pass = "", *db = NULL;
    unsigned port = 3306;
    for (int i = 0; i < kv.n; i++) {
        if (!strcmp(kv.k[i], "host")) host = kv.v[i];
        else if (!strcmp(kv.k[i], "port")) port = (unsigned)atoi(kv.v[i]);
        else if (!strcmp(kv.k[i], "user")) user = kv.v[i];
        else if (!strcmp(kv.k[i], "password")) pass = kv.v[i];
        else if (!strcmp(kv.k[i], "db") || !strcmp(kv.k[i], "dbname")) db = kv.v[i];
        else { snprintf(why, wsz, "mysql conninfo: unknown key %s", kv.k[i]); return -1; }
    }
    x->my = mysql_init(NULL);
    if (!mysql_real_connect(x->my, host, user, pass, db, port, NULL, CLIENT_MULTI_RESULTS | CLIENT_MULTI_STATEMENTS)) {
        snprintf(why, wsz, "connect: %s", mysql_error(x->my));
        mysql_close(x->my);
        x->my = NULL;
        return -1;
    }
    return 0;
}

static void conn_close(conn_t *x) {
    if (x->pg) PQfinish(x->pg);
    if (x->my) mysql_close(x->my);
    x->pg = NULL;
    x->my = NULL;
}
static int conn_alive(conn_t *x) {
    if (x->proto == P_PG) return x->pg && PQstatus(x->pg) == CONNECTION_OK;
    return x->my != NULL;
}

/* Run one statement; need_rows: it must affect >= 1 row. */
static int conn_exec(conn_t *x, const char *sql, int need_rows, char *why, size_t wsz) {
    if (x->proto == P_PG) {
        PGresult *r = PQexec(x->pg, sql);
        ExecStatusType st = PQresultStatus(r);
        int rc = 0;
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            snprintf(why, wsz, "%s", PQresultErrorMessage(r));
            rc = -1;
        } else if (need_rows) {
            const char *n = PQcmdTuples(r);
            if (!n || atol(n) < 1) { snprintf(why, wsz, "write affected 0 rows: %.120s", sql); rc = -1; }
        }
        PQclear(r);
        return rc;
    }
    if (mysql_real_query(x->my, sql, (unsigned long)strlen(sql)) != 0) { snprintf(why, wsz, "%s", mysql_error(x->my)); return -1; }
    my_ulonglong affected = 0;
    int status;
    do {
        MYSQL_RES *res = mysql_store_result(x->my);
        if (res) mysql_free_result(res);
        else if (mysql_field_count(x->my) == 0) affected += mysql_affected_rows(x->my);
        else { snprintf(why, wsz, "%s", mysql_error(x->my)); return -1; }
        status = mysql_next_result(x->my);
        if (status > 0) { snprintf(why, wsz, "%s", mysql_error(x->my)); return -1; }
    } while (status == 0);
    if (need_rows && affected < 1) { snprintf(why, wsz, "write affected 0 rows: %.120s", sql); return -1; }
    return 0;
}

/* Linux port addition: remember the server-side id of a connection just opened (see client_t.bk). */
static void note_backend(client_t *c, uint32_t seq, uint8_t kind, conn_t *x) {
    if (c->nbk == c->capbk) {
        c->capbk = c->capbk ? c->capbk * 2 : 256;
        c->bk = realloc(c->bk, c->capbk * sizeof *c->bk);
        if (!c->bk) { fprintf(stderr, "bbload: out of memory\n"); _exit(2); }
    }
    c->bk[c->nbk].seq = seq;
    c->bk[c->nbk].kind = kind;
    c->bk[c->nbk].id = x->proto == P_PG ? (long)PQbackendPID(x->pg) : (long)mysql_thread_id(x->my);
    c->nbk++;
}

/* ---------- one operation ---------- */
static int run_step(client_t *c, conn_t *cur, int *have_branch, const step_t *st, ctx_t *x, char *why, size_t wsz) {
    char text[8192], w[256];
    if (st->k == K_CLOSE) {
        if (*have_branch) { conn_close(cur); *have_branch = 0; }
        return 0;
    }
    if (expand(st->t, x, text, sizeof text, w) != 0) { snprintf(why, wsz, "%s", w); return -1; }
    if (st->k == K_CONNECT) {
        if (*have_branch) conn_close(cur);
        *have_branch = 0;
        char ci[4096];
        if (expand(S.connect, x, ci, sizeof ci, w) != 0) { snprintf(why, wsz, "%s", w); return -1; }
        if (conn_open(cur, ci, text, why, wsz) != 0) return -1;
        note_backend(c, x->i, 1, cur);
        *have_branch = 1;
        return 0;
    }
    conn_t *target = *have_branch ? cur : &c->home;
    if (!st->serial) return conn_exec(target, text, st->k == K_WRITE, why, wsz);
    /* sql-serial: no two clients run one at the same time (Dolt 2.4.1's DOLT_BRANCH('-d') from concurrent sessions
     * panics the server; lead review 62430d8bf..b49fb656a HIGH 1). Used in untimed after-steps only. */
    pthread_mutex_lock(&g_serial_mu);
    int rc = conn_exec(target, text, 0, why, wsz);
    pthread_mutex_unlock(&g_serial_mu);
    return rc;
}

static void record(client_t *c, const oprec *r) {
    if (c->nrec == c->cap) {
        c->cap = c->cap ? c->cap * 2 : 4096;
        c->rec = realloc(c->rec, c->cap * sizeof *c->rec);
        if (!c->rec) { fprintf(stderr, "bbload: out of memory\n"); _exit(2); }
    }
    c->rec[c->nrec++] = *r;
}

static void reconnect_home(client_t *c) {
    if (conn_alive(&c->home)) return;
    conn_close(&c->home);
    char ci[4096], w[256];
    ctx_t x = {.c = c->id, .i = 0, .rng = &c->rng};
    if (expand(S.connect, &x, ci, sizeof ci, w) == 0 && conn_open(&c->home, ci, NULL, w, sizeof w) == 0) {
        note_backend(c, 0, 0, &c->home);
        for (int k = 0; k < S.nsetup; k++) {
            char t[8192];
            if (expand(S.setup[k], &x, t, sizeof t, w) == 0) conn_exec(&c->home, t, 0, w, sizeof w);
        }
    }
    __atomic_fetch_add(&g_reconnects, 1, __ATOMIC_RELAXED);
}

static void *client_main(void *arg) {
    client_t *c = arg;
    if (S.proto == P_MYSQL) mysql_thread_init();
    char ci[4096], w[512];
    ctx_t x0 = {.c = c->id, .i = 0, .rng = &c->rng};
    int fail = expand(S.connect, &x0, ci, sizeof ci, w) != 0 || conn_open(&c->home, ci, NULL, w, sizeof w) != 0;
    if (!fail) note_backend(c, 0, 0, &c->home);
    for (int k = 0; !fail && k < S.nsetup; k++) {
        char t[8192];
        fail = expand(S.setup[k], &x0, t, sizeof t, w) != 0 || conn_exec(&c->home, t, 0, w, sizeof w) != 0;
    }
    pthread_mutex_lock(&g_mu);
    if (fail) { snprintf(c->err, sizeof c->err, "%s", w); c->state = -1; g_nfailed++; }
    else { c->state = 1; g_nready++; }
    pthread_cond_broadcast(&g_cv);
    while (__atomic_load_n(&g_phase, __ATOMIC_ACQUIRE) == PH_INIT) pthread_cond_wait(&g_cv, &g_mu);
    pthread_mutex_unlock(&g_mu);
    if (fail || g_phase == PH_ABORT) return NULL;

    double mean_gap_ns = OPEN_LOOP ? 1e9 * C / RATE : 0;
    uint64_t intended = g_t0;
    for (uint32_t seq = 0;; seq++) {
        oprec r;
        memset(&r, 0, sizeof r);
        int ph;
        if (OPEN_LOOP) {
            intended += (uint64_t)(-log(unif(&c->rng)) * mean_gap_ns);
            for (;;) { /* wait for the intended time in slices, so the window end is noticed */
                uint64_t tm1 = __atomic_load_n(&g_tm1, __ATOMIC_ACQUIRE);
                if ((tm1 && intended >= tm1) || g_phase == PH_ABORT) goto done;
                uint64_t n = now_ns();
                if (n >= intended) break;
                sleep_until(intended - n > 10000000 ? n + 10000000 : intended);
            }
            uint64_t tm0 = __atomic_load_n(&g_tm0, __ATOMIC_ACQUIRE);
            ph = (!tm0 || intended < tm0) ? PH_WARM : PH_MEAS;
            if (ph == PH_WARM) __atomic_fetch_add(&g_warm_claimed, 1, __ATOMIC_RELAXED);
            else __atomic_fetch_add(&g_meas_claimed, 1, __ATOMIC_RELAXED);
        } else {
            ph = __atomic_load_n(&g_phase, __ATOMIC_ACQUIRE);
            if (ph >= PH_DRAIN) goto done;
            if (ph == PH_MEAS) {
                uint64_t k = __atomic_fetch_add(&g_meas_claimed, 1, __ATOMIC_RELAXED);
                if (MAX_OPS && k >= MAX_OPS) goto done;
            } else __atomic_fetch_add(&g_warm_claimed, 1, __ATOMIC_RELAXED);
        }
        ctx_t x = {.c = c->id, .i = seq, .rng = &c->rng, .nv = S.nvar};
        char vbuf[MAXLIST][1024];
        int bad = 0;
        for (int v = 0; v < S.nvar && !bad; v++) {
            x.vval[v] = vbuf[v];
            x.nv = v; /* a var may use only earlier vars */
            bad = expand(S.vtmpl[v], &x, vbuf[v], sizeof vbuf[v], w) != 0;
        }
        x.nv = S.nvar;
        conn_t cur;
        memset(&cur, 0, sizeof cur);
        int have_branch = 0;
        r.client = (uint32_t)c->id;
        r.seq = seq;
        r.phase = (uint8_t)ph;
        r.ok = 1;
        r.err = -1;
        if (V1 && C == 1) v1_set_mark(V1, MARKB + seq + 1);
        char c1bl[MAXSTEPS][512];
        for (int k = 0; HAVE_C1B && k < S.nstep; k++) {
            if (!S.c1blab[k]) continue;
            if (bad || expand(S.c1blab[k], &x, c1bl[k], sizeof c1bl[k], w) != 0) { c1bl[k][0] = 0; continue; }
            c1b_opstart(&C1B, ((uint64_t)c->id << 40) | ((uint64_t)seq << 8) | (uint64_t)k, c1bl[k]);
        }
        r.start = now_ns();
        r.intended = OPEN_LOOP ? intended : r.start;
        if (bad) { r.ok = 0; r.err = (int16_t)err_index(w); }
        for (int k = 0; k < S.nstep && r.ok; k++) {
            if (run_step(c, &cur, &have_branch, &S.step[k], &x, w, sizeof w) != 0) { r.ok = 0; r.err = (int16_t)err_index(w); }
            else if (HAVE_C1B && S.c1blab[k] && c1bl[k][0]) c1b_ack(&C1B, ((uint64_t)c->id << 40) | ((uint64_t)seq << 8) | (uint64_t)k, c1bl[k]);
            r.step_end[k] = now_ns();
            r.nsteps = (uint8_t)(k + 1);
        }
        r.end = now_ns();
        if (V1 && C == 1) v1_set_mark(V1, V1_MARK_IDLE | (MARKB + seq + 1));
        for (int k = 0; !SKIP_AFTER && k < S.nafter; k++)
            if (run_step(c, &cur, &have_branch, &S.after[k], &x, w, sizeof w) != 0) {
                int e = err_index(w);
                if (r.ok) { r.ok = 0; r.err = (int16_t)(1000 + e); } /* the op succeeded; its after-step did not */
            }
        if (have_branch) conn_close(&cur);
        r.after_end = now_ns();
        if (!conn_alive(&c->home)) reconnect_home(c);
        record(c, &r);
        __atomic_fetch_add(&g_completed, 1, __ATOMIC_RELEASE);
    }
done:
    for (int k = 0; k < S.nteardown; k++) {
        char t[8192];
        if (expand(S.teardown[k], &x0, t, sizeof t, w) == 0) conn_exec(&c->home, t, 0, w, sizeof w);
    }
    conn_close(&c->home);
    if (S.proto == P_MYSQL) mysql_thread_end();
    return NULL;
}

/* ---------- output ---------- */
static int cmp_rec(const void *a, const void *b) {
    const oprec *x = a, *y = b;
    return x->start < y->start ? -1 : x->start > y->start;
}

static void hdr_out(struct hdr_histogram *h, const char *dir, const char *name) {
    char p[2048];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    FILE *f = fopen(p, "w");
    if (!f) return;
    hdr_percentiles_print(h, f, 5, 1000.0, CLASSIC); /* values in us */
    fclose(f);
}

int main(int argc, char **argv) {
    const char *specp = NULL, *out = NULL, *v1run = NULL;
    int warm_rule_flag = 0, warm_legacy = 0; /* MED 7: --warmup and --warmup-ops/--warmup-s may not be mixed */
#ifdef __APPLE__
    mach_timebase_info(&TB);
#endif
    snprintf(RUNTAG, sizeof RUNTAG, "r%llx", (unsigned long long)(time(NULL) & 0xffffffff));
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!strcmp(a, "--spec") && v) specp = argv[++i];
        else if (!strcmp(a, "--out") && v) out = argv[++i];
        else if (!strcmp(a, "--clients") && v) C = atoi(argv[++i]);
        else if (!strcmp(a, "--mode") && v) { OPEN_LOOP = !strcmp(v, "open"); if (strcmp(v, "open") && strcmp(v, "closed")) { fprintf(stderr, "bbload: --mode closed|open\n"); return 2; } i++; }
        else if (!strcmp(a, "--rate") && v) RATE = atof(argv[++i]);
        else if (!strcmp(a, "--warmup-s") && v) { WARM_S = atof(argv[++i]); warm_legacy = 1; }
        else if (!strcmp(a, "--warmup-ops") && v) { WARM_OPS = strtoull(argv[++i], NULL, 10); warm_legacy = 1; }
        else if (!strcmp(a, "--warmup") && v) {  /* gate-6 review, t3run item 3: OPS:S:MAX_S */
            unsigned long long wo; double ws, wm; char extra;
            if (sscanf(v, "%llu:%lf:%lf%c", &wo, &ws, &wm, &extra) != 3 || ws < 0 || wm < 0) {
                fprintf(stderr, "bbload: --warmup OPS:S:MAX_S (got %s)\n", v); return 2;
            }
            WARM_OPS = wo; WARM_S = ws; WARM_MAX_S = wm;
            warm_rule_flag = 1;
            i++;
        }
        else if (!strcmp(a, "--duration-s") && v) DUR_S = atof(argv[++i]);
        else if (!strcmp(a, "--min-ops") && v) MIN_OPS = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(a, "--max-ops") && v) MAX_OPS = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(a, "--max-window-s") && v) MAXWIN_S = atof(argv[++i]);
        else if (!strcmp(a, "--stall-s") && v) STALL_S = atof(argv[++i]);
        else if (!strcmp(a, "--run-tag") && v) snprintf(RUNTAG, sizeof RUNTAG, "%s", argv[++i]);
        else if (!strcmp(a, "--seed") && v) SEED = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(a, "--v1-run") && v) {
            v1run = argv[++i];
#if !BB_HOOKS
            fprintf(stderr, "bbload: REFUSED: --v1-run, but this build has no V1 hooks (BB_HOOKS=0)\n");
            return 2;
#endif
        }
        else if (!strcmp(a, "--v1-mark-base") && v) MARKB = strtoull(argv[++i], NULL, 0);
        else if (!strcmp(a, "--c1b-run") && v) {
#if BB_HOOKS
            const char *why = "?";
            if (c1b_client_open(&C1B, argv[++i], &why) != 0) { fprintf(stderr, "bbload: --c1b-run: %s\n", why); return 2; }
            HAVE_C1B = 1;
#else
            fprintf(stderr, "bbload: REFUSED: --c1b-run, but this build has no C1b hooks (BB_HOOKS=0)\n");
            return 2;
#endif
        }
        else if (!strcmp(a, "--allow-errors")) ALLOW_ERR = 1;
        else if (!strcmp(a, "--skip-after")) SKIP_AFTER = 1;
        else if (!strcmp(a, "--set") && v) {
            char *eq = strchr(argv[++i], '=');
            if (!eq || NSET >= MAXLIST) { fprintf(stderr, "bbload: --set k=v\n"); return 2; }
            *eq = 0;
            SETK[NSET] = argv[i];
            SETV[NSET++] = eq + 1;
        } else { fprintf(stderr, "bbload: bad argument %s (see the header of bbload.c)\n", a); return 2; }
    }
    if (!specp || !out || C < 1) { fprintf(stderr, "usage: bbload --spec FILE --out DIR --clients C ...\n"); return 2; }
    if (warm_rule_flag && warm_legacy) {  /* lead review 62430d8bf..b49fb656a MED 7 */
        fprintf(stderr, "bbload: REFUSED: --warmup OPS:S:MAX_S together with --warmup-ops/--warmup-s (the effective "
                        "warm-up and the recorded rule would differ)\n");
        return 2;
    }
    if (OPEN_LOOP && RATE <= 0) { fprintf(stderr, "bbload: open loop needs --rate > 0\n"); return 2; }
    if (OPEN_LOOP && MAX_OPS) { fprintf(stderr, "bbload: --max-ops is closed-loop only\n"); return 2; }
    if (DUR_S <= 0 && MIN_OPS == 0 && MAX_OPS == 0) { fprintf(stderr, "bbload: set --duration-s, --min-ops or --max-ops\n"); return 2; }
    char *spectext = NULL;
    if (load_spec(specp, &spectext) != 0) return 2;
    if (mkdir(out, 0755) != 0) { fprintf(stderr, "bbload: REFUSED: out dir %s must not exist: %s\n", out, strerror(errno)); return 2; }
    if (v1run) {
#if BB_HOOKS
        const char *why = "?";
        if (!(V1 = v1_map(v1run, &why))) { fprintf(stderr, "bbload: V1 run %s: %s\n", v1run, why); return 2; }
        if (!MARKB) MARKB = ((uint64_t)time(NULL) & 0x7fffff) << 32;
#endif
    }
    char p[2048];
    snprintf(p, sizeof p, "%s/spec.txt", out);
    FILE *f = fopen(p, "w");
    if (f) { fputs(spectext, f); fclose(f); }

    /* The connector must be initialised once before any thread calls mysql_init. */
    if (S.proto == P_MYSQL && mysql_library_init(0, NULL, NULL)) { fprintf(stderr, "bbload: mysql_library_init failed\n"); return 2; }
    CL = calloc((size_t)C, sizeof *CL);
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setstacksize(&at, 512 * 1024);
    for (int i = 0; i < C; i++) {
        CL[i].id = i;
        CL[i].rng = (SEED * 0x9E3779B97F4A7C15ULL) ^ ((uint64_t)(i + 1) * 0xD1B54A32D192ED03ULL);
        if (!CL[i].rng) CL[i].rng = 1;
        if (pthread_create(&CL[i].th, &at, client_main, &CL[i]) != 0) { fprintf(stderr, "bbload: pthread_create %d failed\n", i); _exit(2); }
    }
    pthread_mutex_lock(&g_mu);
    while (g_nready + g_nfailed < C) pthread_cond_wait(&g_cv, &g_mu);
    int failed = g_nfailed;
    /* No warm-up asked for (all of OPS, S and MAX_S zero): the run starts in the measured phase, so --max-ops N makes
     * exactly N ops. With a warm phase the first 1 ms tick let every client claim a warm op first, so an untimed
     * N-op run (the prebranch) made about N + C (lead review 62430d8bf..b49fb656a, MED 3 / HIGH 1). */
    int nowarm = WARM_OPS == 0 && WARM_S == 0 && WARM_MAX_S == 0;
    g_t0 = now_ns();
    if (nowarm && !failed) __atomic_store_n(&g_tm0, g_t0, __ATOMIC_RELEASE);
    __atomic_store_n(&g_phase, failed ? PH_ABORT : (nowarm ? PH_MEAS : PH_WARM), __ATOMIC_RELEASE);
    pthread_cond_broadcast(&g_cv);
    pthread_mutex_unlock(&g_mu);
    if (failed) {
        for (int i = 0; i < C; i++) pthread_join(CL[i].th, NULL);
        for (int i = 0; i < C; i++) if (CL[i].state < 0) { fprintf(stderr, "bbload: client %d: %s\n", i, CL[i].err); break; }
        fprintf(stderr, "bbload: %d of %d clients failed to connect or set up\n", failed, C);
        return 2;
    }
    if (V1 && C > 1) v1_set_mark(V1, MARKB + (nowarm ? PH_MEAS : PH_WARM));
    struct rusage ru0, ru1;
    memset(&ru0, 0, sizeof ru0);
    if (nowarm) { getrusage(RUSAGE_SELF, &ru0); g_tp_tm0 = self_tracerpid(); }
    uint64_t last_done = 0, last_progress = now_ns();
    int stalled = 0, short_window = 0;
    for (;;) {
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
        uint64_t n = now_ns(), done = __atomic_load_n(&g_completed, __ATOMIC_ACQUIRE);
        if (done != last_done) { last_done = done; last_progress = n; }
        else if ((n - last_progress) / 1e9 > STALL_S) { stalled = 1; break; }
        int ph = g_phase;
        double wel = (n - g_t0) / 1e9;
        if (ph == PH_WARM && ((g_warm_claimed >= WARM_OPS && wel >= WARM_S) || (WARM_MAX_S > 0 && wel >= WARM_MAX_S))) {
            getrusage(RUSAGE_SELF, &ru0);
            __atomic_store_n(&g_tm0, n, __ATOMIC_RELEASE);
            __atomic_store_n(&g_phase, PH_MEAS, __ATOMIC_RELEASE);
            if (V1 && C > 1) v1_set_mark(V1, MARKB + PH_MEAS);
            g_tp_tm0 = self_tracerpid();
        } else if (ph == PH_MEAS) {
            double el = (n - g_tm0) / 1e9;
            /* The duration/min-ops clause applies only when one of them was asked for: with --max-ops alone,
             * "el >= 0 && claimed >= 0" held at once and closed the window before any op was measured. */
            int timed = DUR_S > 0 || MIN_OPS > 0;
            int end = (timed && el >= DUR_S && g_meas_claimed >= MIN_OPS) || (MAX_OPS && g_meas_claimed >= MAX_OPS);
            if (!end && el > MAXWIN_S) { end = 1; short_window = 1; }
            if (end) {
                getrusage(RUSAGE_SELF, &ru1);
                __atomic_store_n(&g_tm1, n, __ATOMIC_RELEASE);
                __atomic_store_n(&g_phase, PH_DRAIN, __ATOMIC_RELEASE);
                if (V1 && C > 1) v1_set_mark(V1, MARKB + PH_DRAIN);
                g_tp_tm1 = self_tracerpid();
                break;
            }
        }
    }
    if (stalled) {
        fprintf(stderr, "bbload: STALL: no operation completed for %.0f s (phase %d, completed %llu); exiting without draining\n",
                STALL_S, g_phase, (unsigned long long)g_completed);
        _exit(4);
    }
    for (int i = 0; i < C; i++) pthread_join(CL[i].th, NULL);
    if (V1) v1_set_mark(V1, 0);

    /* Raw first. */
    size_t total = 0;
    for (int i = 0; i < C; i++) total += CL[i].nrec;
    oprec *all = malloc((total ? total : 1) * sizeof *all);
    size_t k = 0;
    for (int i = 0; i < C; i++) { memcpy(all + k, CL[i].rec, CL[i].nrec * sizeof *all); k += CL[i].nrec; }
    qsort(all, total, sizeof *all, cmp_rec);
    snprintf(p, sizeof p, "%s/raw.tsv", out);
    f = fopen(p, "w");
    if (!f) { fprintf(stderr, "bbload: raw.tsv: %s\n", strerror(errno)); return 2; }
    fprintf(f, "client\tseq\tphase\tok\tintended_ns\tstart_ns\tend_ns\tlat_ns");
    for (int s = 0; s < S.nstep; s++) fprintf(f, "\tstep%d_ns", s + 1);
    fprintf(f, "\tafter_ns\terr\n");
    for (size_t r = 0; r < total; r++) {
        oprec *o = &all[r];
        fprintf(f, "%u\t%u\t%s\t%u\t%llu\t%llu\t%llu\t%llu", o->client, o->seq,
                o->phase == PH_MEAS ? "measure" : o->phase == PH_WARM ? "warmup" : "drain", o->ok,
                (unsigned long long)o->intended, (unsigned long long)o->start, (unsigned long long)o->end,
                (unsigned long long)(o->end - o->intended));
        uint64_t prev = o->start;
        for (int s = 0; s < S.nstep; s++) {
            if (s < o->nsteps) { fprintf(f, "\t%llu", (unsigned long long)(o->step_end[s] - prev)); prev = o->step_end[s]; }
            else fprintf(f, "\t");
        }
        fprintf(f, "\t%llu\t%d\n", (unsigned long long)(o->after_end - o->end), o->err);
    }
    if (fclose(f) != 0) { fprintf(stderr, "bbload: raw.tsv close failed\n"); return 2; }
    snprintf(p, sizeof p, "%s/errors.txt", out);
    f = fopen(p, "w");
    for (int e = 0; f && e < g_nerr; e++) fprintf(f, "%d\t%llu\t%s\n", e, (unsigned long long)g_errn[e], g_errs[e]);
    if (f) fclose(f);
    /* Linux port addition: every connection's server-side id (PG backend pid / MySQL connection id). */
    snprintf(p, sizeof p, "%s/backends.tsv", out);
    f = fopen(p, "w");
    if (!f) { fprintf(stderr, "bbload: backends.tsv: %s\n", strerror(errno)); return 2; }
    fprintf(f, "client\tseq\tkind\t%s\n", S.proto == P_PG ? "backend_pid" : "connection_id");
    for (int i = 0; i < C; i++)
        for (size_t b = 0; b < CL[i].nbk; b++) {
            if (CL[i].bk[b].kind == 0) fprintf(f, "%d\t-\thome\t%ld\n", i, CL[i].bk[b].id);
            else fprintf(f, "%d\t%u\tstep\t%ld\n", i, CL[i].bk[b].seq, CL[i].bk[b].id);
        }
    if (fclose(f) != 0) { fprintf(stderr, "bbload: backends.tsv close failed\n"); return 2; }

    /* Summary from the raw records. */
    struct hdr_histogram *ht, *hs[MAXSTEPS];
    hdr_init(1, INT64_C(3600000000000), 3, &ht);
    for (int s = 0; s < S.nstep; s++) hdr_init(1, INT64_C(3600000000000), 3, &hs[s]);
    uint64_t meas = 0, meas_ok = 0, meas_err = 0, in_window_done = 0;
    double sum = 0;
    for (size_t r = 0; r < total; r++) {
        oprec *o = &all[r];
        if (o->end >= g_tm0 && o->end <= g_tm1 && o->ok) in_window_done++;
        if (o->phase != PH_MEAS) continue;
        meas++;
        if (!o->ok) { meas_err++; continue; }
        meas_ok++;
        uint64_t lat = o->end - o->intended;
        sum += (double)lat;
        hdr_record_value(ht, (int64_t)lat);
        uint64_t prev = o->start;
        for (int s = 0; s < o->nsteps; s++) { hdr_record_value(hs[s], (int64_t)(o->step_end[s] - prev)); prev = o->step_end[s]; }
    }
    hdr_out(ht, out, "hdr_total.txt");
    for (int s = 0; s < S.nstep; s++) { char nm[32]; snprintf(nm, sizeof nm, "hdr_step%d.txt", s + 1); hdr_out(hs[s], out, nm); }
    double win = (g_tm1 - g_tm0) / 1e9;
    double cpu = (ru1.ru_utime.tv_sec - ru0.ru_utime.tv_sec) + (ru1.ru_utime.tv_usec - ru0.ru_utime.tv_usec) / 1e6 +
                 (ru1.ru_stime.tv_sec - ru0.ru_stime.tv_sec) + (ru1.ru_stime.tv_usec - ru0.ru_stime.tv_usec) / 1e6;
    int rc = 0;
    const char *verdict = "ok";
    if (meas_ok == 0) { rc = 3; verdict = "REFUSED: no measured operation succeeded"; }
    /* gate-6 review, t3run item 16; lead review 62430d8bf..b49fb656a MED 5: with --max-ops, the window cap is the
     * registered per-run cap, and a run it ended is reported, not judged: rc 0, verdict "capped", capped: true and its
     * counts; timedrun.py alone applies PREREG's tiers (>= 1000 ok complete, 100-999 p50 only, fewer failed). */
    else if (meas_err && !ALLOW_ERR) { rc = 3; verdict = "REFUSED: measured operations failed (see errors.txt; --allow-errors to accept)"; }
    else if (short_window && MAX_OPS) verdict = "capped";
    else if (short_window && !MAX_OPS) { rc = 3; verdict = "REFUSED: --min-ops not reached within --max-window-s"; }
    snprintf(p, sizeof p, "%s/summary.json", out);
    f = fopen(p, "w");
    fprintf(f, "{\"verdict\":\"%s\",\"rc\":%d,\"spec\":\"%s\",\"run_tag\":\"%s\",\"clients\":%d,\"mode\":\"%s\",\"rate\":%.3f,"
               "\"protocol\":\"%s\",\"window_s\":%.6f,\"measured_ops\":%llu,\"measured_ok\":%llu,\"measured_err\":%llu,"
               "\"warmup_ops\":%llu,\"total_ops\":%zu,\"reconnects\":%llu,"
               "\"tput_ok_started_per_s\":%.3f,\"tput_completed_in_window_per_s\":%.3f,"
               "\"client_cpu_s\":%.3f,\"client_cpu_cores\":%.3f,\"v1_run\":\"%s\",\"v1_mark_base\":%llu,",
            verdict, rc, specp, RUNTAG, C, OPEN_LOOP ? "open" : "closed", RATE, S.proto == P_PG ? "pg" : "mysql", win,
            (unsigned long long)meas, (unsigned long long)meas_ok, (unsigned long long)meas_err,
            (unsigned long long)g_warm_claimed, total, (unsigned long long)g_reconnects,
            win > 0 ? meas_ok / win : 0, win > 0 ? in_window_done / win : 0, cpu, win > 0 ? cpu / win : 0,
            v1run ? v1run : "", (unsigned long long)MARKB);
    fprintf(f, "\"clock\":\"%s\",\"hooks\":%d,", BB_CLOCK_NAME, BB_HOOKS); /* Linux port: which clock stamped the ops */
    fprintf(f, "\"capped\":%s,\"max_window_s\":%.3f,", short_window ? "true" : "false", MAXWIN_S);
    fprintf(f, "\"after_steps\":%d,\"skip_after\":%s,", S.nafter, SKIP_AFTER ? "true" : "false");
    {   /* MED 4: the measured window on CLOCK_REALTIME (the tracer sweeps' clock), mapped through one offset read now,
         * and the load generator's own TracerPid at its start and end */
        struct timespec rt;
        clock_gettime(CLOCK_REALTIME, &rt);
        double off = (double)rt.tv_sec + rt.tv_nsec / 1e9 - now_ns() / 1e9;
        fprintf(f, "\"tm0_realtime_s\":%.6f,\"tm1_realtime_s\":%.6f,\"tracerpid_tm0\":%d,\"tracerpid_tm1\":%d,",
                g_tm0 / 1e9 + off, g_tm1 / 1e9 + off, g_tp_tm0, g_tp_tm1);
    }
    /* MED 7: the recorded rule is formatted from the EFFECTIVE values, never copied from the command line */
    snprintf(WARM_RULE, sizeof WARM_RULE, "%llu:%g:%g", (unsigned long long)WARM_OPS, WARM_S, WARM_MAX_S);
    fprintf(f, "\"warmup_rule\":\"%s\",\"warmup_s\":%.6f,", WARM_RULE, g_tm0 > g_t0 ? (g_tm0 - g_t0) / 1e9 : 0.0);
    fprintf(f, "\"lat_us\":{\"p50\":%.1f,\"p90\":%.1f,\"p99\":%.1f,\"p999\":%s%.1f%s,\"max\":%.1f,\"mean\":%.1f},",
            hdr_value_at_percentile(ht, 50) / 1e3, hdr_value_at_percentile(ht, 90) / 1e3, hdr_value_at_percentile(ht, 99) / 1e3,
            meas_ok >= 10000 ? "" : "null,\"p999_unlicensed\":", hdr_value_at_percentile(ht, 99.9) / 1e3, "",
            hdr_max(ht) / 1e3, meas_ok ? sum / meas_ok / 1e3 : 0);
    fprintf(f, "\"steps_us\":[");
    for (int s = 0; s < S.nstep; s++)
        fprintf(f, "%s{\"step\":%d,\"kind\":\"%s\",\"p50\":%.1f,\"p99\":%.1f}", s ? "," : "", s + 1,
                S.step[s].k == K_SQL ? "sql" : S.step[s].k == K_WRITE ? "write" : S.step[s].k == K_CONNECT ? "connect" : "close",
                hdr_value_at_percentile(hs[s], 50) / 1e3, hdr_value_at_percentile(hs[s], 99) / 1e3);
    fprintf(f, "],\"errors\":%d}\n", g_nerr);
    fclose(f);
    printf("bbload %s: C=%d %s window %.2fs measured %llu ok %llu err %llu  tput %.1f/s  p50 %.1f us p99 %.1f us  client cpu %.2f cores -> %s\n",
           verdict, C, OPEN_LOOP ? "open" : "closed", win, (unsigned long long)meas, (unsigned long long)meas_ok,
           (unsigned long long)meas_err, win > 0 ? meas_ok / win : 0, hdr_value_at_percentile(ht, 50) / 1e3,
           hdr_value_at_percentile(ht, 99) / 1e3, win > 0 ? cpu / win : 0, out);
    return rc;
}
