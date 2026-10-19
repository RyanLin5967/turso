#!/bin/bash
# run_selftest.sh CMD... -- run one self-test and judge it by its TALLY line and rc (gates/tally.py, linux-rev-uw2 MED 1),
# never by a truncated view of its output. CI steps used `CMD | tail -1`, which shows the summary line and hides the
# TALLY (it sits just before the summary), so CI judged by rc alone (comp a81f9 and the lead, 2026-10-18). This keeps
# the WHOLE output: it prints it in full and hands it to tally.py, with --require-all, since a CI runner runs every
# case or the step fails. Exit 0 only when the tally says every case ran and passed and agrees with the rc.
set -uo pipefail
[ $# -ge 1 ] || { echo "usage: run_selftest.sh CMD..." >&2; exit 2; }
HERE=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp) || exit 2
trap 'rm -f "$out"' EXIT
rc=0
"$@" > "$out" 2>&1 || rc=$?
cat "$out"
python3 -B "$HERE/tally.py" check "$out" "$rc" --require-all
