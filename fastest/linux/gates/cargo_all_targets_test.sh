#!/bin/bash
# cargo_all_targets_test.sh -- the platform class in Rust (the lead's ruling, 2026-10-18): every target of turso_core
# must compile on Linux, not only the ones a lane happened to build there. iccad7 found core/examples/branch_githost
# and branch_restart calling macOS-only libc proc_pid_rusage / rusage_info_v4 with no cfg, so `cargo check
# --all-targets` failed on Linux while every lane's own command passed. run_everywhere runs this on Linux only
# (gates/platforms.tsv); on the Mac it is N/A, never a pass, and never a local build. Exit 0 iff the check passes.
set -uo pipefail
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2> /dev/null) || ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
[ -f "$ROOT/core/Cargo.toml" ] || { echo "REFUSED: no core/Cargo.toml under $ROOT"; exit 2; }
command -v cargo > /dev/null || { echo "REFUSED: cargo is not on PATH"; exit 2; }
cd "$ROOT" || exit 2
# linux-rev-uw2 LOW 8: --all-targets SKIPS a target whose required-features are not enabled, silently. Every feature
# any [[bench]], [[example]] or [[test]] of core/Cargo.toml requires must be in FEATURES, or this refuses, naming it
# The features checked: conn_raw_api (the lanes' own builds), bench and fts (the [[bench]] targets' required-features)
# and io_uring (the Linux io_uring backend). linux-rev-uw2 LOW 8 ran this set clean (E6c). THE GAP, named: code behind
# any other feature (simulator, fuzz, experimental_win_iocp, aristo-instr, pure-rust-crypto, ...) is not compiled here.
FEATURES=conn_raw_api,bench,fts,io_uring
req=$(awk '/^\[\[(bench|example|test)\]\]/{t=1} /^\[/ && !/^\[\[(bench|example|test)\]\]/{t=0} t && /^required-features/' core/Cargo.toml |
      grep -oE '"[A-Za-z0-9_-]+"' | tr -d '"' | sort -u)
miss=""
for f in $req; do [[ ",$FEATURES," == *",$f,"* ]] || miss="$miss $f"; done
[ -z "$miss" ] || { echo "CARGO-ALL-TARGETS FAIL: targets need features not checked here (cargo skips them):$miss"
  echo "TALLY ran=1 pass=0 na=0 fail=1"; exit 1; }
out=$(timeout 3600 cargo check -p turso_core --all-targets --features "$FEATURES" 2>&1); rc=$?
printf '%s\n' "$out" | grep -E '^error|could not compile' | head -20
# one case: the check over every target (gates/tally.py)
[ $rc = 0 ] && { echo "TALLY ran=1 pass=1 na=0 fail=0"; echo "CARGO-ALL-TARGETS PASS: cargo check -p turso_core --all-targets --features $FEATURES"; exit 0; }
echo "TALLY ran=1 pass=0 na=0 fail=1"; echo "CARGO-ALL-TARGETS FAIL: rc $rc"
exit 1
