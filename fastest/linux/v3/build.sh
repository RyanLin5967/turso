#!/usr/bin/env bash
# build.sh DIST -- the one build of the V3 probe and its fire-check binaries (fastest-v3.yml and t3run.sh both call
# it, so they cannot drift). Writes:
#   DIST/v3floor         the probe, linked static (fourth review M4: exe_sha256 then covers the libc whose fsync it
#                        calls, and no preload reaches it) -- firecheck.sh's V3FLOOR and run.sh's binary
#   DIST/v3floor.dyn     the same source linked dynamic, for the preload plants only (firecheck.sh's V3_DYN)
#   DIST/statfs_shim.so  the statfs-magic plant library (V3_SHIM)
#   DIST/noop_shim.so    the /etc/ld.so.preload plant library (V3_NOOP)
#   DIST/build.txt       the compiler, the commands, and the sha256 of every output
# and prints each output's sha256. Exit 0 when all four were built and the static build maps no other file's code
# (checked with `file` naming it statically linked), else 1. Needs gcc and the glibc static archive (libc6-dev).
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: build.sh DIST" >&2; exit 2; }
DIST=$1
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$DIST"
CF=(-O2 -std=gnu11 -Wall -Wextra -Werror)
{
  gcc --version | head -1
  set -x
  gcc "${CF[@]}" -static -o "$DIST/v3floor" "$HERE/v3floor.c"
  gcc "${CF[@]}" -o "$DIST/v3floor.dyn" "$HERE/v3floor.c"
  gcc -O2 -shared -fPIC -o "$DIST/statfs_shim.so" "$HERE/statfs_shim.c" -ldl
  gcc -O2 -Wall -Wextra -Werror -shared -fPIC -o "$DIST/noop_shim.so" "$HERE/noop_shim.c"
  set +x
  file "$DIST/v3floor" "$DIST/v3floor.dyn"
  sha256sum "$DIST/v3floor" "$DIST/v3floor.dyn" "$DIST/statfs_shim.so" "$DIST/noop_shim.so"
} > "$DIST/build.txt" 2>&1 || { cat "$DIST/build.txt" >&2; echo "build.sh: the build failed" >&2; exit 1; }
file "$DIST/v3floor" | grep -q 'statically linked' || { echo "build.sh: $DIST/v3floor is not statically linked" >&2; exit 1; }
sha256sum "$DIST/v3floor" "$DIST/v3floor.dyn" "$DIST/statfs_shim.so" "$DIST/noop_shim.so"
