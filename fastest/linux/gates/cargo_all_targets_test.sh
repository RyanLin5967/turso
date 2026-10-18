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
# linux-rev-uw4 LOW 2: no tomllib is a REFUSAL of the host (rc 2, no tally), probed once here, never a failure of
# the parser cases below (which would print a tally blaming the parser and exit 1)
python3 -B -c 'import tomllib' 2> /dev/null || { echo "CARGO-ALL-TARGETS REFUSED: python3 has no tomllib (3.11+): the manifest cannot be parsed"; exit 2; }
# linux-rev-uw2 LOW 8: --all-targets SKIPS a target whose required-features are not enabled, silently. Every feature
# any [[bench]], [[example]] or [[test]] of core/Cargo.toml requires must be in FEATURES, or this refuses, naming it
# The features checked: conn_raw_api (the lanes' own builds), bench and fts (the [[bench]] targets' required-features)
# and io_uring (the Linux io_uring backend). linux-rev-uw2 LOW 8 ran this set clean (E6c). THE GAP, named: code behind
# any other feature (simulator, fuzz, experimental_win_iocp, aristo-instr, pure-rust-crypto, ...) is not compiled here.
FEATURES=conn_raw_api,bench,fts,io_uring
# required_features TOML: every feature a [[bench]], [[example]] or [[test]] of TOML requires, one per line
# parsed as TOML (python3's tomllib, 3.11+), as cargo reads it, so a multi-line array is read whole (linux-rev-uw3
# LOW 5: the line-grep missed one). No tomllib refuses (rc 2, no tally), never falls back to a line match
required_features() {
  python3 -B -c 'import sys
try:
    import tomllib
except ImportError:
    sys.exit("REFUSED: python3 has no tomllib (3.11+): the manifest cannot be parsed")
m = tomllib.load(open(sys.argv[1], "rb"))
fs = sorted({f for k in ("bench", "example", "test") for t in m.get(k) or [] for f in t.get("required-features") or []})
print("\n".join(fs))' "$1" || exit 2
}
# linux-rev-uw3 LOW 5: the parser's own cases, before the build, on planted manifests: a multi-line required-features
# array (which cargo reads, and the line-grep missed: a planted bench needing 'simulator' passed) must be read whole
pp=0 pf=0
pcase() { if eval "$2"; then pp=$((pp + 1)); echo "CARGO-ALL-TARGETS self-test PASS: $1"
          else pf=$((pf + 1)); echo "CARGO-ALL-TARGETS self-test FAIL: $1"; fi; }
pd=$(mktemp -d) || exit 2
trap 'rm -rf "$pd"' EXIT
printf '[package]\nname = "x"\n\n[[bench]]\nname = "one"\nrequired-features = ["simulator"]\n' > "$pd/one.toml"
printf '[package]\nname = "x"\n\n[[bench]]\nname = "multi"\nharness = false\nrequired-features = [\n    "simulator",\n    "fuzz",\n]\n' > "$pd/multi.toml"
# linux-rev-uw4 LOW 3 (FLAGGED TEST EDIT, a strengthening): the old none.toml expected EMPTY output, which a parser
# printing nothing also gave; the same non-target key now sits beside an [[example]] and a [[test]] whose
# requirements must be read, so the case fails both a parser that reads nothing and one that reads [[bench]] only
printf '[package]\nname = "x"\n\n[features]\nrequired-features = ["notatarget"]\n\n[[example]]\nname = "e"\nrequired-features = ["fts"]\n\n[[test]]\nname = "t"\nrequired-features = ["simulator"]\n' > "$pd/none.toml"
pcase "a one-line required-features array is read" '[ "$(required_features "$pd/one.toml" | tr "\n" " ")" = "simulator " ]'
pcase "uw3 LOW 5: a multi-line required-features array is read whole" '[ "$(required_features "$pd/multi.toml" | tr "\n" " ")" = "fuzz simulator " ]'
pcase "uw4 LOW 3: a key outside [[bench]]/[[example]]/[[test]] is not a target's requirement; [[example]] and [[test]] requirements are" '[ "$(required_features "$pd/none.toml" | tr "\n" " ")" = "fts simulator " ]'
[ $pf = 0 ] || { echo "TALLY ran=$((pp + pf + 1)) pass=$pp na=0 fail=$((pf + 1))"; echo "CARGO-ALL-TARGETS FAIL: its parser"; exit 1; }
req=$(required_features core/Cargo.toml) || { echo "CARGO-ALL-TARGETS REFUSED: core/Cargo.toml could not be parsed"; exit 2; }
miss=""
for f in $req; do [[ ",$FEATURES," == *",$f,"* ]] || miss="$miss $f"; done
[ -z "$miss" ] || { echo "CARGO-ALL-TARGETS FAIL: targets need features not checked here (cargo skips them):$miss"
  echo "TALLY ran=$((pp + 1)) pass=$pp na=0 fail=1"; exit 1; }
out=$(timeout 3600 cargo check -p turso_core --all-targets --features "$FEATURES" 2>&1); rc=$?
printf '%s\n' "$out" | grep -E '^error|could not compile' | head -20
# one case: the check over every target (gates/tally.py)
[ $rc = 0 ] && { echo "TALLY ran=$((pp + 1)) pass=$((pp + 1)) na=0 fail=0"; echo "CARGO-ALL-TARGETS PASS: cargo check -p turso_core --all-targets --features $FEATURES"; exit 0; }
echo "TALLY ran=$((pp + 1)) pass=$pp na=0 fail=1"; echo "CARGO-ALL-TARGETS FAIL: rc $rc"
exit 1
