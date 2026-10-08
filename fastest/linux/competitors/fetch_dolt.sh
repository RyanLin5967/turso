#!/usr/bin/env bash
# fetch_dolt.sh BIN_DIR OUT_TXT -- Dolt 2.3.5 and Doltgres 1.3.3 release binaries (the Mac smoke's versions) for
# this machine's arch, into BIN_DIR/dolt and BIN_DIR/doltgres.
#
# Each tarball is checked against the sha256 GitHub reports for that release asset (`gh release view --json assets`,
# field digest, read 2026-10-04); a mismatch or an unknown arch refuses (exit 2). OUT_TXT records the URL, the
# tarball sha256, each binary's sha256 and its own version output. The version commands run with a throwaway
# DOLT_ROOT_PATH whose global config disables metrics and the version check (common.sh's dolt_quiet_root), and with
# DOLT_DISABLE_EVENT_FLUSH=1, so nothing is reported to eventsapi.dolthub.com from here either.
set -euo pipefail
BIN=${1:?usage: fetch_dolt.sh BIN_DIR OUT_TXT}
OUT=${2:?usage: fetch_dolt.sh BIN_DIR OUT_TXT}
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
case "$(uname -m)" in
  x86_64) arch=amd64
    dolt_sha=c49d4c3e004cf1581ba0d4a00c5023a26f84eb2ec15d5fe876eed36d5343f463
    dg_sha=4873959e06190ac43c5da20561034cb8598885d0ca4bcc7a2bb8f7210965f8c0 ;;
  aarch64) arch=arm64
    dolt_sha=9ce70fc81e50139e97758ef7f4dc57e9583e4e5ef05ad75d7535c30caa161387
    dg_sha=97dfbf2436413fa8bc25554eb9cf2361cdf5b71f53c856505d511fcda20f58f9 ;;
  *) die "REFUSED: no pinned Dolt/Doltgres digest for arch $(uname -m)" ;;
esac
mkdir -p "$BIN"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

fetch() { # fetch NAME URL SHA256 BINNAME
  local name=$1 url=$2 sha=$3 binname=$4 got found
  curl -fsSL --retry 5 -o "$work/$name.tar.gz" "$url"
  got=$(sha256sum "$work/$name.tar.gz" | cut -d' ' -f1)
  [ "$got" = "$sha" ] || die "REFUSED: $url sha256 $got, pinned $sha"
  mkdir -p "$work/$name"
  tar -xzf "$work/$name.tar.gz" -C "$work/$name"
  found=$(find "$work/$name" -type f -name "$binname" -perm -u+x)
  [ "$(printf '%s\n' "$found" | grep -c .)" = 1 ] || die "REFUSED: expected one $binname in $url, found: [$found]"
  install -m 0755 "$found" "$BIN/$binname"
  { echo "$binname url=$url"; echo "$binname tarball_sha256=$got (pinned $sha)";
    echo "$binname binary_sha256=$(sha256sum "$BIN/$binname" | cut -d' ' -f1) path=$BIN/$binname"; } >>"$OUT"
}

: >"$OUT"
fetch dolt "https://github.com/dolthub/dolt/releases/download/v2.3.5/dolt-linux-$arch.tar.gz" "$dolt_sha" dolt
fetch doltgres "https://github.com/dolthub/doltgresql/releases/download/v1.3.3/doltgresql-linux-$arch.tar.gz" "$dg_sha" doltgres
dolt_quiet_root "$work/root"
{ echo "dolt version: $("$BIN/dolt" version 2>&1 | head -3 | tr '\n' ' ')";
  echo "doltgres version: $(cd "$work" && "$BIN/doltgres" -version 2>&1 | head -3 | tr '\n' ' ')";
  echo "telemetry: DOLT_ROOT_PATH config $(cat "$work/root/.dolt/config_global.json") DOLT_DISABLE_EVENT_FLUSH=$DOLT_DISABLE_EVENT_FLUSH"; } >>"$OUT"
grep -q 'dolt version: .*2\.3\.5' "$OUT" || die "REFUSED: dolt does not report 2.3.5: $(grep 'dolt version' "$OUT")"
grep -q 'doltgres version: .*1\.3\.3' "$OUT" || die "REFUSED: doltgres does not report 1.3.3: $(grep 'doltgres version' "$OUT")"
cat "$OUT"
