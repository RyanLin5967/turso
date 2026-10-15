#!/usr/bin/env bash
# build.sh OUT -- the Linux port of tools/loadgen/build.sh and tools/baselines/build.sh: bbload against PGDG
# libpq 18 and Ubuntu's MariaDB connector, clonebench against stock SQLite 3.53.4 (the Mac's Homebrew version,
# built from sqlite.org's autoconf tarball, configure defaults), both with HdrHistogram_c 0.11.10 (the Mac's).
# Same flags as the Mac builds (clang -O2 -Wall -Wextra -Wno-unused-parameter -Werror), BB_HOOKS=0 (no V1/C1b shim
# on Linux yet). Each tarball is checked against the sha256 Homebrew's formula pins for it; a mismatch refuses.
# Writes OUT/{bbload,clonebench,sqlite3,build-info.txt}. Runs on a runner only: nothing compiles on the Mac.
set -euo pipefail
OUT=${1:?usage: build.sh OUT}
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
W="$OUT/.work"
rm -rf "$W"
mkdir -p "$W"

fetch() { # fetch URL SHA256 FILE
  curl -fsSL --retry 5 -o "$3" "$1"
  local got
  got=$(sha256sum "$3" | cut -d' ' -f1)
  [ "$got" = "$2" ] || { echo "REFUSED: $1 sha256 $got, pinned $2" >&2; exit 2; }
  echo "fetched $1 sha256=$got"
}

# HdrHistogram_c 0.11.10 (Homebrew hdrhistogram_c.rb pin), static, without the zlib log component (bbload and
# clonebench use only hdr_histogram.c).
fetch https://github.com/HdrHistogram/HdrHistogram_c/archive/refs/tags/0.11.10.tar.gz \
  c3b06d077e680d112abf9f027d8a558f1176ee4a55a7c523577833391d8c2249 "$W/hdr.tar.gz"
tar -xzf "$W/hdr.tar.gz" -C "$W"
cmake -S "$W/HdrHistogram_c-0.11.10" -B "$W/hdr-build" -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang \
  -DHDR_LOG_REQUIRED=DISABLED -DHDR_HISTOGRAM_BUILD_PROGRAMS=OFF -DHDR_HISTOGRAM_BUILD_SHARED=OFF \
  -DCMAKE_INSTALL_PREFIX="$W/hdr" >"$W/hdr-cmake.log"
cmake --build "$W/hdr-build" -j "$(nproc)" >"$W/hdr-build.log"
cmake --install "$W/hdr-build" >"$W/hdr-install.log"
HDRLIB=$(find "$W/hdr" -name 'libhdr_histogram_static.a' | head -1)
[ -f "$HDRLIB" ] || { echo "REFUSED: no libhdr_histogram_static.a under $W/hdr" >&2; exit 2; }

# SQLite 3.53.4 (Homebrew sqlite.rb pin), stock: the autoconf build's own configure defaults, static library and CLI.
fetch https://www.sqlite.org/2026/sqlite-autoconf-3530400.tar.gz \
  0e9483900e92cd5de8fd48d16bf9200145a61f7fd5be542a5ac81d8a9516eb9c "$W/sqlite.tar.gz"
tar -xzf "$W/sqlite.tar.gz" -C "$W"
( cd "$W/sqlite-autoconf-3530400" && CC=clang ./configure --prefix="$W/sqlite" >"$W/sqlite-configure.log" 2>&1 &&
  make -j "$(nproc)" >"$W/sqlite-make.log" 2>&1 && make install >"$W/sqlite-install.log" 2>&1 ) ||
  { tail -40 "$W"/sqlite-*.log >&2; exit 1; }
[ -f "$W/sqlite/lib/libsqlite3.a" ] || { echo "REFUSED: no static libsqlite3.a" >&2; exit 2; }

CF=(-O2 -Wall -Wextra -Wno-unused-parameter -Werror -DBB_HOOKS=0)
clang "${CF[@]}" -I"$W/hdr/include" $(pkg-config --cflags libpq) $(mariadb_config --cflags) \
  -o "$OUT/bbload.tmp.$$" "$HERE/loadgen/bbload.c" "$HDRLIB" \
  $(pkg-config --libs libpq) $(mariadb_config --libs) -lpthread -lm
mv -f "$OUT/bbload.tmp.$$" "$OUT/bbload"
clang "${CF[@]}" -I"$W/hdr/include" -I"$W/sqlite/include" \
  -o "$OUT/clonebench.tmp.$$" "$HERE/baselines/clonebench.c" "$W/sqlite/lib/libsqlite3.a" "$HDRLIB" -lpthread -ldl -lm
mv -f "$OUT/clonebench.tmp.$$" "$OUT/clonebench"
install -m 0755 "$W/sqlite/bin/sqlite3" "$OUT/sqlite3"
# A binary that resolved SQLite from the system (Ubuntu's libsqlite3) is not the pinned build: refuse.
if ldd "$OUT/clonebench" | grep -q libsqlite3; then echo "REFUSED: clonebench links a shared libsqlite3" >&2; exit 2; fi
if ldd "$OUT/sqlite3" | grep -q libsqlite3; then echo "REFUSED: the sqlite3 CLI links a shared libsqlite3" >&2; exit 2; fi
"$OUT/sqlite3" --version | grep -q '^3\.53\.4 ' || { echo "REFUSED: sqlite3 CLI is not 3.53.4" >&2; exit 2; }

{
  echo "git_sha=${GITHUB_SHA:-?} runner=${RUNNER_NAME:-?} image=${ImageOS:-?} ${ImageVersion:-?} arch=$(uname -m)"
  clang --version | head -1
  echo "libpq=$(pkg-config --modversion libpq) ($(pkg-config --libs libpq))"
  echo "mariadb_connector=$(mariadb_config --cc_version 2>/dev/null || mariadb_config --version)"
  echo "sqlite_cli=$("$OUT/sqlite3" --version)"
  echo "hdrhistogram_c=0.11.10 static=$HDRLIB"
  echo "## ldd bbload"; ldd "$OUT/bbload"
  echo "## ldd clonebench"; ldd "$OUT/clonebench"
  (cd "$OUT" && sha256sum bbload clonebench sqlite3)
} | tee "$OUT/build-info.txt"
rm -rf "$W"
