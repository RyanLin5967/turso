#!/usr/bin/env bash
# fetch_dolt.sh BIN_DIR OUT_TXT -- the REGISTERED Dolt and Doltgres release binaries (PREREG §6(6): Dolt 2.4.1,
# Doltgres 1.4.0; gate-6 review, t3run item 13) for this machine's arch, into BIN_DIR/dolt and BIN_DIR/doltgres.
#
# Versions and tarball sha256s come from versions.tsv (pins.py; GitHub's release-asset digests), the one table the
# driver checks against too; a mismatch, a missing row or an unknown arch refuses (exit 2). OUT_TXT records the URL,
# the tarball sha256, each binary's sha256 and its own version output, which must name the registered version. The
# version commands run with a throwaway DOLT_ROOT_PATH whose global config disables metrics and the version check
# (common.sh's dolt_quiet_root), and with DOLT_DISABLE_EVENT_FLUSH=1, so nothing is reported to
# eventsapi.dolthub.com from here either.
set -euo pipefail
BIN=${1:?usage: fetch_dolt.sh BIN_DIR OUT_TXT}
OUT=${2:?usage: fetch_dolt.sh BIN_DIR OUT_TXT}
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/common.sh"
PINS="$HERE/pins.py"
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64) arch=arm64 ;;
  *) die "REFUSED: no pinned Dolt/Doltgres digest for arch $(uname -m)" ;;
esac
dolt_v=$(python3 -B "$PINS" version dolt) || die "REFUSED: no Dolt version in versions.tsv"
dg_v=$(python3 -B "$PINS" version doltgres) || die "REFUSED: no Doltgres version in versions.tsv"
dolt_sha=$(python3 -B "$PINS" get dolt "$arch" tarball_sha256) || die "REFUSED: no Dolt $arch digest in versions.tsv"
dg_sha=$(python3 -B "$PINS" get doltgres "$arch" tarball_sha256) || die "REFUSED: no Doltgres $arch digest in versions.tsv"
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
fetch dolt "https://github.com/dolthub/dolt/releases/download/v$dolt_v/dolt-linux-$arch.tar.gz" "$dolt_sha" dolt
fetch doltgres "https://github.com/dolthub/doltgresql/releases/download/v$dg_v/doltgresql-linux-$arch.tar.gz" "$dg_sha" doltgres
dolt_quiet_root "$work/root"
# Each version command's own output, unprefixed, is what check-version reads: since LOW 19 it takes the version from
# the FIRST line in the command's own form ("dolt version X", "Doltgres version X"). This used to grep the prefixed,
# space-joined OUT lines ("dolt version: dolt version 2.4.1 ..."), which that first-line rule refuses, and its
# `grep -v doltgres` never removed anything (LOW 27). The rc is recorded; the output is what is judged.
dv_rc=0 dgv_rc=0
"$BIN/dolt" version >"$work/dv" 2>&1 || dv_rc=$?
(cd "$work" && "$BIN/doltgres" -version) >"$work/dgv" 2>&1 || dgv_rc=$?
{ echo "dolt version (rc=$dv_rc): $(head -3 "$work/dv" | tr '\n' ' ')";
  echo "doltgres version (rc=$dgv_rc): $(head -3 "$work/dgv" | tr '\n' ' ')";
  echo "telemetry: DOLT_ROOT_PATH config $(cat "$work/root/.dolt/config_global.json") DOLT_DISABLE_EVENT_FLUSH=$DOLT_DISABLE_EVENT_FLUSH"; } >>"$OUT"
python3 -B "$PINS" check-version dolt "$work/dv" || die "REFUSED: dolt does not report $dolt_v: $(head -3 "$work/dv")"
python3 -B "$PINS" check-version doltgres "$work/dgv" || die "REFUSED: doltgres does not report $dg_v: $(head -3 "$work/dgv")"
cat "$OUT"
