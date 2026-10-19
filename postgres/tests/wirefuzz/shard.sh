#!/bin/bash
# shard.sh SHA SEEDS [STATEMENTS] -- one wirefuzz shard in a uwrun run dir that holds src/ (git archive of SHA) and
# this directory as wirefuzz/: builds tursopg at SHA (refusing a binary older than the build), runs the two
# controls (PostgreSQL against itself must agree; the plant must be reported), then the seed range over both
# protocols. Results in out/. PG 18.6 from $PGBIN (default: the host's source build).
set -u
SHA=${1:?sha}; SEEDS=${2:?seeds}; N=${3:-60}; H=$PWD; O=$H/out; mkdir -p "$O"
PGBIN=${PGBIN:-/var/tmp/r226lin-wire/pg18/inst/bin}
for t in 13 12 11; do [ -f /opt/rh/gcc-toolset-$t/enable ] && { source /opt/rh/gcc-toolset-$t/enable; break; }; done
if [ -d /usr/lib64/llvm17/lib ]; then export LIBCLANG_PATH=/usr/lib64/llvm17/lib; else export LIBCLANG_PATH=/usr/lib/llvm-18/lib; fi
export BINDGEN_EXTRA_CLANG_ARGS="-I$(gcc -print-file-name=include)" PATH="$HOME/.cargo/bin:$PATH" CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR=${WF_TARGET:-/var/tmp/r226lin-wire/target}
PY="uv run -p 3.12 --no-project python -B"
if [ -z "${WF_TURSOPG:-}" ]; then
  STARTED=$(date +%s)
  (cd src && find . -name '*.rs' -exec touch {} + && timeout 5400 cargo build -p tursopg --bin tursopg > "$O/build.txt" 2>&1) \
    || { echo "BUILD FAILED"; tail -20 "$O/build.txt"; exit 2; }
  [ "$(stat -c %Y "$CARGO_TARGET_DIR/debug/tursopg")" -ge "$STARTED" ] || { echo "REFUSED: stale tursopg"; exit 2; }
  mkdir -p bin && cp "$CARGO_TARGET_DIR/debug/tursopg" bin/tursopg && echo "$SHA" > bin/tursopg.sha
  WF_TURSOPG=$H/bin/tursopg
fi
echo "tursopg $(cat "$WF_TURSOPG.sha" 2>/dev/null) sha256 $(sha256sum "$WF_TURSOPG" | cut -c1-16) pg $($PGBIN/postgres --version)" | tee "$O/binary.txt"
rc=0
$PY wirefuzz/wirefuzz.py run --pg-bin "$PGBIN" --control pg --seeds 1-20 --statements "$N" --out "$O/control-pg" > "$O/control-pg.txt" 2>&1
echo "control pg rc=$? (want 0)" | tee -a "$O/controls.txt"
$PY wirefuzz/wirefuzz.py run --pg-bin "$PGBIN" --control pg --plant --no-shrink --seeds 1-20 --statements "$N" --out "$O/plant" > "$O/plant.txt" 2>&1
echo "plant rc=$? (want 0: reported, nothing else)" | tee -a "$O/controls.txt"
$PY wirefuzz/wirefuzz.py run --pg-bin "$PGBIN" --tursopg "$WF_TURSOPG" --seeds "$SEEDS" --statements "$N" --out "$O/seeds" > "$O/seeds.txt" 2>&1
rc=$?; echo "seeds $SEEDS rc=$rc" | tee -a "$O/controls.txt"; tail -1 "$O/seeds.txt"
exit $rc
