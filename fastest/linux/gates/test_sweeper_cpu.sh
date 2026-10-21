#!/usr/bin/env bash
# gates/test_sweeper_cpu.sh -- comp-rev-89ec-b3c3 LOW 19: sweeper.sh's sweeper_cpu must count the CPU of the
# children the sweeper reaps (without the FIFO, every sleep between sweeps is a sleep(1) child), not only its own
# utime+stime: a reaped child that burns about 1 s of CPU must add at least 0.2 s (CLK_TCK/5 ticks; the bar leaves room
# for a loaded host) to SWEEPER_CPU. Prints PASS/FAIL and a TALLY line (gates/tally.py); exit 0 only on PASS.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=sweeper.sh
source "$HERE/sweeper.sh"
clk=$(getconf CLK_TCK 2>/dev/null)
[[ $clk =~ ^[1-9][0-9]*$ ]] || { echo "REFUSED: CLK_TCK [$clk]"; exit 2; }
sweeper_cpu
before=$SWEEPER_CPU
timeout 1 bash -c 'while :; do :; done'  # a child (timeout, then its bash), both reaped before the next read
sweeper_cpu
after=$SWEEPER_CPU
[[ $before =~ ^[0-9]+$ && $after =~ ^[0-9]+$ ]] || { echo "REFUSED: sweeper_cpu unreadable [$before] [$after]"; exit 2; }
d=$((after - before)) want=$((clk / 5))
if [ "$d" -ge "$want" ]; then
  echo "PASS a reaped child's CPU is counted: $d ticks (at least $want of about $clk)"
  echo "TALLY ran=1 pass=1 na=0 fail=0"
  exit 0
fi
echo "FAIL a reaped child's CPU is not counted: $d ticks, want at least $want (about $clk burned)"
echo "TALLY ran=1 pass=0 na=0 fail=1"
exit 1
