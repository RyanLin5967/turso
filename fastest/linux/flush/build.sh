#!/usr/bin/env bash
# build.sh OUT -- build the V1 Linux flush counter, its mutants and its probes into OUT, then prove what was built.
# Linux (glibc, x86_64 or aarch64) only, and only in CI: nothing here is compiled on the Mac. Needs liburing-dev.
#
#   syncshim.so                  the LD_PRELOAD counter
#   syncshim_mut_<name>.so       fire-check mutants: three each missing one interpose (pwrite64, __open_2, fdatasync)
#                                and three each breaking one piece of logic (no_fork_claim: a fork child gets no slot
#                                at birth; syscall_no_fcntl: fcntl through syscall(2) is not tracked; pwritev2_a4: raw
#                                pwritev2's flags are read from the wrong argument)
#   v1ctl, v1run                 run control and launcher
#   probe_c, probe_c_static      the C probe, and a static build of its "spawned" mode (never loads the shim)
#   probe_uring                  a C probe linked against liburing.so (io_uring_queue_init + IORING_OP_FSYNC)
#   probe_go_nocgo, probe_go_cgo the Go probe built CGO_ENABLED=0 (static) and =1 (dynamic, like Dolt)
#   build-info.txt               toolchain versions, file types, the shim's exported interposes, sha256 of each
# Exits non-zero, naming the check, if any proof below fails: a build that is not what it claims is not a build.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT=${1:?usage: build.sh OUT}
[ "$(uname -s)" = Linux ] || { echo "build.sh: Linux only (CI)" >&2; exit 2; }
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
CC=${CC:-gcc}
# No fortify: its inline open() wrappers would collide with the shim's own definitions, and the probe must call
# exactly the entry points it names. No _FILE_OFFSET_BITS: glibc would rename open/fcntl/pwrite to their 64-bit
# symbols, and the shim would define one symbol twice.
CF=(-O2 -g -Wall -Wextra -Werror -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -U_FILE_OFFSET_BITS)
DROP_MUTANTS="pwrite64 open_2 fdatasync"
LOGIC_MUTANTS="no_fork_claim syscall_no_fcntl pwritev2_a4"

# Every unit is attempted, so one run reports every compile error; any failure fails the build at the end.
broken=()
unit() { local name=$1; shift; "$@" || broken+=("$name"); }
unit syncshim.so $CC "${CF[@]}" -fPIC -shared -o "$OUT/syncshim.so" "$HERE/syncshim.c" -ldl -lpthread
for m in $DROP_MUTANTS; do
  M=$(echo "$m" | tr '[:lower:]' '[:upper:]')
  unit "syncshim_mut_$m.so" $CC "${CF[@]}" -fPIC -shared "-DV1_MUTANT_DROP_$M" -o "$OUT/syncshim_mut_$m.so" \
    "$HERE/syncshim.c" -ldl -lpthread
done
for m in $LOGIC_MUTANTS; do
  M=$(echo "$m" | tr '[:lower:]' '[:upper:]')
  unit "syncshim_mut_$m.so" $CC "${CF[@]}" -fPIC -shared "-DV1_MUTANT_$M" -o "$OUT/syncshim_mut_$m.so" \
    "$HERE/syncshim.c" -ldl -lpthread
done
unit v1ctl $CC "${CF[@]}" -o "$OUT/v1ctl" "$HERE/v1ctl.c" -lrt
unit v1run $CC "${CF[@]}" -o "$OUT/v1run" "$HERE/v1run.c" -lrt
unit probe_c $CC "${CF[@]}" -o "$OUT/probe_c" "$HERE/probe_c.c" -ldl -lrt -lpthread
unit probe_c_static $CC "${CF[@]}" -static -DPROBE_STATIC -o "$OUT/probe_c_static" "$HERE/probe_c.c"
unit probe_uring $CC "${CF[@]}" -o "$OUT/probe_uring" "$HERE/probe_uring.c" -luring
unit probe_go_nocgo env -C "$HERE/probe_go" CGO_ENABLED=0 timeout 600 go build -trimpath -o "$OUT/probe_go_nocgo" .
unit probe_go_cgo env -C "$HERE/probe_go" CGO_ENABLED=1 timeout 600 go build -trimpath -o "$OUT/probe_go_cgo" .
[ ${#broken[@]} -eq 0 ] || { echo "build.sh: FAILED to build: ${broken[*]}" >&2; exit 1; }

fail() { echo "build.sh: PROOF FAILED: $*" >&2; exit 1; }
# The interposes the shim must export (every wrapper in syncshim.c), and the one each drop mutant must lack.
WRAPPERS="fsync fdatasync sync_file_range syncfs sync msync ioctl copy_file_range write __write pwrite pwrite64
  __pwrite64 writev pwritev pwritev64 pwritev2 pwritev64v2 sendfile sendfile64 splice syscall open open64 __open
  __open64 openat openat64 __open_2 __open64_2 __openat_2 __openat64_2 creat creat64 open_by_handle_at fcntl fcntl64
  __fcntl dup dup2 __dup2 dup3 close __close execve execv execvp execvpe execl execle execlp fexecve execveat
  posix_spawn posix_spawnp _Fork io_uring_queue_init io_uring_queue_init_params io_uring_setup"
exports() { nm -D --defined-only "$1" | awk '$2 == "T" || $2 == "W" {print $3}' | sort -u; }
shim_exp=$(exports "$OUT/syncshim.so")
for w in $WRAPPERS; do grep -qx -- "$w" <<<"$shim_exp" || fail "syncshim.so does not export $w"; done
nw=$(wc -w <<<"$WRAPPERS")
for m in $DROP_MUTANTS; do
  sym=$m; [ "$m" = open_2 ] && sym=__open_2
  mexp=$(exports "$OUT/syncshim_mut_$m.so")
  grep -qx -- "$sym" <<<"$mexp" && fail "mutant $m still exports $sym"
  for w in $WRAPPERS; do
    [ "$w" = "$sym" ] && continue
    grep -qx -- "$w" <<<"$mexp" || fail "mutant $m lost $w as well"
  done
done
shim_sum=$(sha256sum <"$OUT/syncshim.so")
for m in $LOGIC_MUTANTS; do  # same exports, different code: the -D took effect
  mexp=$(exports "$OUT/syncshim_mut_$m.so")
  for w in $WRAPPERS; do grep -qx -- "$w" <<<"$mexp" || fail "logic mutant $m lost $w"; done
  [ "$(sha256sum <"$OUT/syncshim_mut_$m.so")" != "$shim_sum" ] || fail "logic mutant $m is byte-identical to the shim"
done
file -L "$OUT/probe_c_static" | grep -Eq "statically linked|static-pie linked" || fail "probe_c_static is not static"
file -L "$OUT/probe_go_nocgo" | grep -q 'statically linked' || fail "probe_go_nocgo is not static"
file -L "$OUT/probe_go_cgo" | grep -q 'dynamically linked' || fail "probe_go_cgo is not dynamic"
file -L "$OUT/probe_c" | grep -q 'dynamically linked' || fail "probe_c is not dynamic"
ldd "$OUT/probe_uring" | grep -q 'liburing\.so' || fail "probe_uring does not link liburing.so dynamically"
LIBC=$(ldd "$OUT/probe_c" | awk '$1 ~ /^libc\.so/ {print $3}')
[ -n "$LIBC" ] && [ -e "$LIBC" ] || fail "cannot find the libc probe_c links"
BINS="syncshim.so syncshim_mut_*.so v1ctl v1run probe_c probe_c_static probe_uring probe_go_nocgo probe_go_cgo"
{
  echo "built_utc=$(date -u +%FT%TZ) uname=$(uname -srm)"
  echo "cc=$($CC --version | head -1)"
  echo "libc=$LIBC $(ldd --version | head -1)"
  echo "liburing=$(ldd "$OUT/probe_uring" | awk '/liburing/ {print $3}') $(dpkg-query -W -f='${Version}' liburing2 2>/dev/null || echo '?')"
  echo "go=$(go version)"
  echo "cflags=${CF[*]}"
  echo "shim_wrappers_exported=$nw (every one checked; each drop mutant lacks exactly its one; each logic mutant keeps all)"
  echo "## libc's own definitions of the double-underscore names the shim wraps (dlsym finds only default versions)"
  nm -D --defined-only "$LIBC" | grep -E ' (__write|__pwrite64|__open|__open64|__open_2|__open64_2|__openat_2|__openat64_2|__fcntl|__dup2|__close|fcntl64|pwritev64v2|execveat|_Fork)(@|$)' || true
  echo "## file types"
  (cd "$OUT" && file -L $BINS)
  echo "## sha256"
  (cd "$OUT" && sha256sum $BINS)
} >"$OUT/build-info.txt"
cat "$OUT/build-info.txt"
