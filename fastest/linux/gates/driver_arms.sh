#!/bin/bash
# driver_arms.sh BIN OUT -- fastest_profile's own contract, run on a built driver: the arms the fastest-gates release job
# runs (fourth lane review MED 6, LOW 14; review 5 LOW 15, 16), in one place so a local run and the job run the same
# arms. Results, not exit codes: driver_contract.py checks each summary.json against the asked T, C and rule, with rules
# whose outcome is forced (1000000:0:2 must run to MAX_S; 10:0:30 must stop on its ops); each refusal must exit 1 with
# NOT A RESULT before any op; warmup_conformance.py replays the shared cases (PARTIAL, rc 3: one driver given), and
# (review 5 MED 8, the driver half) replays each run's own live warm-up record through the same driver (rc 0).
# Every arm runs whatever an earlier one did, its full output in OUT/contract-<arm>.txt; exit 1 naming each failed arm.
set -uo pipefail
BIN=${1:?BIN} OUT=${2:?OUT}
HERE=$(cd "$(dirname "$0")" && pwd)
C=$HERE/driver_contract.py W=$HERE/warmup_conformance.py
mkdir -p "$OUT"
d=$(mktemp -d)
trap 'rm -rf "$d"' EXIT
FAILED=""
arm() { local n=$1 want=$2 rc=0; shift 2; "$@" > "$OUT/contract-$n.txt" 2>&1 || rc=$?
  tail -1 "$OUT/contract-$n.txt"
  [ $rc -eq "$want" ] || { echo "arm $n: rc $rc, want $want ($OUT/contract-$n.txt)"; FAILED="$FAILED $n"; }; }
# judged by its TALLY and rc (gates/run_selftest.sh), not by the tail of its output
arm selftest 0 bash "$HERE/run_selftest.sh" python3 -B "$C" self-test
# each run's record directory starts empty, so a rerun into the same OUT never replays a stale warmup-live.json (fresh
# read of 7a8c3a817..0802dd739, finding 3)
rm -rf "$OUT"/contract-k1 "$OUT"/contract-k2 "$OUT"/contract-k3
arm k1 0 "$BIN" --dir "$d/k1" --class full --clients 4 --ops-total 33 --warmup 1000000:0:2 --out "$OUT/contract-k1"
arm k1-check 0 python3 -B "$C" check "$OUT/contract-k1/summary.json" 33 4 1000000:0:2 9,8,8,8
arm k1-live 0 python3 -B "$W" live "$BIN" "$OUT/contract-k1/warmup-live.json"
arm k1-tie 0 python3 -B "$C" tie "$OUT/contract-k1/summary.json" "$OUT/contract-k1/warmup-live.json"
arm k2 0 "$BIN" --dir "$d/k2" --class full --clients 16 --ops-total 20 --warmup 10:0:30 --mode cycle --out "$OUT/contract-k2"
arm k2-check 0 python3 -B "$C" check "$OUT/contract-k2/summary.json" 20 16 10:0:30 2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1
arm k2-live 0 python3 -B "$W" live "$BIN" "$OUT/contract-k2/warmup-live.json"
arm k2-tie 0 python3 -B "$C" tie "$OUT/contract-k2/summary.json" "$OUT/contract-k2/warmup-live.json"
# S > 0 (review 5 LOW 15): it must stop at a claim at or after 1 s with 10 ops claimed, not capped
arm k3 0 "$BIN" --dir "$d/k3" --class full --clients 4 --ops-total 33 --warmup 10:1:30 --out "$OUT/contract-k3"
arm k3-check 0 python3 -B "$C" check "$OUT/contract-k3/summary.json" 33 4 10:1:30 9,8,8,8
arm k3-live 0 python3 -B "$W" live "$BIN" "$OUT/contract-k3/warmup-live.json"
arm k3-tie 0 python3 -B "$C" tie "$OUT/contract-k3/summary.json" "$OUT/contract-k3/warmup-live.json"
# the ONE gen_seed parent (the owed ours fixture record, 2026-10-16): the driver built on comp's gen_seed streams for
# (2000 rows, 50 aging updates) with 3 live branches; its own record checked against what was asked, and its dump of
# the trunk read back equal to gen_seed's facts() (ours_readback.py, the read-back fixture.py write compares)
G=$HERE/../competitors/gen_seed.py
rm -rf "$OUT/contract-fx"
python3 -B "$G" sql --rows 2000 > "$d/fx-seed.sql" && python3 -B "$G" age --rows 2000 --updates 50 > "$d/fx-age.sql"
arm fx 0 "$BIN" --dir "$d/fx" --class full --clients 2 --ops-total 20 --warmup 2 --rows 2000 --seed-sql "$d/fx-seed.sql" \
  --age-sql "$d/fx-age.sql" --live 3 --dump-trunk "$d/fx-trunk.tsv" --out "$OUT/contract-fx"
arm fx-check 0 python3 -B "$C" fixture "$OUT/contract-fx/summary.json" 2000 50 3
arm fx-readback 0 python3 -B -c 'import subprocess, sys
sys.path.insert(0, sys.argv[1]); import gen_seed
f = gen_seed.facts(2000, 50, 1)
got = subprocess.run([sys.executable, "-B", sys.argv[2], sys.argv[3]], capture_output=True, text=True).stdout.strip()
want = "%d|%d|%d" % (f["count"], f["sum"], f["row_hash"])
print("readback", got, "generator", want); sys.exit(got != want)' "$HERE/../competitors" "$HERE/../t3/ours_readback.py" "$d/fx-trunk.tsv"
refuse() { local rc=0; "$BIN" --dir "$d/r$1" "${@:2}" > "$OUT/contract-refuse-$1.txt" 2>&1 || rc=$?
  [ $rc -eq 1 ] && grep -q '^NOT A RESULT' "$OUT/contract-refuse-$1.txt" ||
    { echo "refusal $1 (${*:2}): rc=$rc"; cat "$OUT/contract-refuse-$1.txt"; FAILED="$FAILED refuse-$1"; }; }
refuse t-below-c --class full --clients 4 --ops-total 3
refuse both-ops --class full --ops 5 --ops-total 5
refuse prereg-form --class full --warmup prereg:1800
refuse two-fields --class full --warmup 1:2
refuse inf --class full --warmup 1000:inf:inf
refuse zero-max --class full --warmup 10:0:0
# one warm-up rule for every driver (fourth lane review LOW 14): with fastest_profile alone the verdict can only be
# PARTIAL (rc 3: every case matches, not all three drivers given); the three-driver verdict is t3run's envchecks
# judged by its TALLY and rc too (linux-rev-uw3 MED 2: this arm was still rc-only)
arm conformance-selftest 0 bash "$HERE/run_selftest.sh" python3 -B "$W" self-test
arm conformance 3 python3 -B "$W" run --fastest-profile "$BIN"
cat "$OUT/contract-conformance.txt"
[ -z "$FAILED" ] || { echo "driver contract: FAILED arms:$FAILED"; exit 1; }
echo "driver contract: 3 records kept, their live warm-ups replayed and tied to the records, 6 refusals refused, conformance PARTIAL with every case matching"
