#!/usr/bin/env bash
# cargo_all_targets_notoml_test.sh -- linux-rev-uw4 LOW 2: cargo_all_targets_test.sh on a python3 with NO tomllib
# (RHEL 8's /usr/bin/python3 is 3.6) must REFUSE (rc 2, no tally), not blame its own parser (rc 1 with a tally).
# tomllib is blocked by a PYTHONPATH module that raises ImportError; a fake cargo proves the build never starts.
HERE=$(cd "$(dirname "$0")" && pwd)
t=$(mktemp -d) || exit 2
trap 'rm -rf "$t"' EXIT
mkdir -p "$t/py" "$t/bin"
printf 'raise ImportError("tomllib blocked by cargo_all_targets_notoml_test")\n' > "$t/py/tomllib.py"
printf '#!/bin/sh\necho "fake cargo: must never run" >&2\nexit 99\n' > "$t/bin/cargo"
chmod +x "$t/bin/cargo"
out=$(PATH="$t/bin:$PATH" PYTHONPATH="$t/py" bash "$HERE/cargo_all_targets_test.sh" 2>&1)
rc=$?
pass=0 fail=0
c() { if eval "$2"; then pass=$((pass + 1)); echo "CARGO-NOTOML self-test PASS: $1"
      else fail=$((fail + 1)); echo "CARGO-NOTOML self-test FAIL: $1 (rc $rc)"; fi; }
c "uw4 LOW 2: no tomllib refuses with rc 2" '[ $rc = 2 ]'
c "uw4 LOW 2: no tomllib prints no TALLY (a refusal is not a parser failure)" '! printf "%s\n" "$out" | grep -q "^TALLY ran"'
c "the refusal names tomllib" 'printf "%s\n" "$out" | grep -q "REFUSED.*tomllib"'
c "the fake cargo never ran" '! printf "%s\n" "$out" | grep -q "fake cargo"'
echo "TALLY ran=$((pass + fail)) pass=$pass na=0 fail=$fail"  # gates/tally.py
[ $fail = 0 ] && { echo "CARGO-NOTOML SELF-TEST $pass/$((pass + fail)) PASS"; exit 0; }
printf '%s\n' "$out" | tail -5 | sed 's/^/    | /'
echo "CARGO-NOTOML SELF-TEST $pass/$((pass + fail)) FAIL"
exit 1
