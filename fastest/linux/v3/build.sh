#!/usr/bin/env bash
# build.sh DIST -- the one build of the V3 probe and its fire-check binaries (fastest-v3.yml and t3run.sh both call
# it, so they cannot drift). Writes:
#   DIST/v3floor         the probe, linked static (fourth review M4: exe_sha256 then covers the libc whose fsync it
#                        calls, and no preload reaches it) -- firecheck.sh's V3FLOOR and run.sh's binary
#   DIST/v3floor.dyn     the same source linked dynamic, for the preload plants only (firecheck.sh's V3_DYN)
#   DIST/statfs_shim.so  the statfs-magic plant library (V3_SHIM)
#   DIST/noop_shim.so    the /etc/ld.so.preload plant library (V3_NOOP)
#   DIST/build.txt       the compiler, the commands, and the sha256 of every output
# and prints each output's sha256. Every output is removed first and every command's status is checked on its own
# (seventh review L1: a failed compile must never leave an older binary in place under a zero exit). Exit 0 when all
# four were built and v3floor is statically linked, else 1. Needs gcc and the glibc static archive (libc6-dev).
set -uo pipefail
[ $# -eq 1 ] || { echo "usage: build.sh DIST" >&2; exit 2; }
DIST=$1
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$DIST" || exit 1
OUTS=("$DIST/v3floor" "$DIST/v3floor.dyn" "$DIST/statfs_shim.so" "$DIST/noop_shim.so")
rm -f "${OUTS[@]}" "$DIST/build.txt"
LOG=$DIST/build.txt
CF=(-O2 -std=gnu11 -Wall -Wextra -Werror)
step() { # cmd... : logged, and the build stops at the first failure
  echo "+ $*" >> "$LOG"
  "$@" >> "$LOG" 2>&1 || { cat "$LOG" >&2; echo "build.sh: FAILED: $*" >&2; exit 1; }
}
gcc --version | head -1 > "$LOG"
step gcc "${CF[@]}" -static -o "$DIST/v3floor" "$HERE/v3floor.c"
step gcc "${CF[@]}" -o "$DIST/v3floor.dyn" "$HERE/v3floor.c"
step gcc -O2 -shared -fPIC -o "$DIST/statfs_shim.so" "$HERE/statfs_shim.c" -ldl
step gcc -O2 -Wall -Wextra -Werror -shared -fPIC -o "$DIST/noop_shim.so" "$HERE/noop_shim.c"
step file "$DIST/v3floor" "$DIST/v3floor.dyn"
file "$DIST/v3floor" | grep -q 'statically linked' || { echo "build.sh: $DIST/v3floor is not statically linked" >&2; exit 1; }
for o in "${OUTS[@]}"; do [ -s "$o" ] || { echo "build.sh: $o was not written" >&2; exit 1; }; done
sha256sum "${OUTS[@]}" | tee -a "$LOG"
