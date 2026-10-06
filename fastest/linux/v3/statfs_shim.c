/* statfs_shim.c -- LD_PRELOAD plant for the V3 fire-check (review 2 item 12(d)): statfs(2) reports the wrong magic
 * (XFS for anything that is not XFS, ext4 for XFS), so the probe must refuse with "the mount table says ... but
 * statfs magic is ...". Fire-check only; built by the workflow:
 *   gcc -O2 -shared -fPIC -o statfs_shim.so statfs_shim.c -ldl
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <sys/statfs.h>

#define MAGIC_EXT4 0xEF53
#define MAGIC_XFS 0x58465342

static void flip(__fsword_t *t) { *t = *t == MAGIC_XFS ? MAGIC_EXT4 : MAGIC_XFS; }

int statfs(const char *path, struct statfs *buf) {
    static int (*real)(const char *, struct statfs *);
    if (!real) real = (int (*)(const char *, struct statfs *))dlsym(RTLD_NEXT, "statfs");
    int r = real(path, buf);
    if (r == 0) flip(&buf->f_type);
    return r;
}

int statfs64(const char *path, struct statfs64 *buf) {
    static int (*real)(const char *, struct statfs64 *);
    if (!real) real = (int (*)(const char *, struct statfs64 *))dlsym(RTLD_NEXT, "statfs64");
    int r = real(path, buf);
    if (r == 0) flip(&buf->f_type);
    return r;
}
