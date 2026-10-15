#!/bin/bash
# fastest-linux profiling: run one fastest_profile binary through every instrument, one fresh
# database per run, and bank the raw output for analyze.py. Nothing here is credited.
#
# usage: profile.sh <fastest_profile binary> <side: head|base> <raw-dir> <work-dir>
# Arms (PROFILE_ARMS, default below): <class>-<store>-c<C>, class full|async|off, store snap|cat.
# Per arm, into <raw-dir>/<side>/<arm>/:
#   plain/          the driver alone: per-op latency and the engine's sync counter per window
#   strace.txt      strace -f -y of a second run with FASTEST_PHASE markers (+ strace-run/)
#   perfstat-W.csv  perf stat over window W only (perf --control), one run per window
#   flame-create.svg, perf-create.data.txt  perf record of the create window (class full only)
#   cg-kK-n.total, cg-kK-2n.total, cg-n.ops  callgrind total Ir of runs doing the first K phases at N
#                   and 2N ops (C=1 only), plus cg-k4-n.annotate.txt for reading
#   status          "NOT AVAILABLE: ..." when the driver refuses the class (rc 4); every other
#                   failure is recorded in <raw-dir>/<side>/runs.tsv and fails the script.
# Sizes: PROFILE_OPS_C1 (200), PROFILE_OPS_CN (20 per client), PROFILE_CG_OPS (100).
set -u
drv=$(readlink -f "${1:?driver}")
side=${2:?side}
raw=${3:?raw dir}/$side
work=${4:?work dir}/$side
arms=${PROFILE_ARMS:-full-snap-c1 full-snap-c64 full-cat-c1 async-snap-c1 async-snap-c64}
ops1=${PROFILE_OPS_C1:-200}
opsn=${PROFILE_OPS_CN:-20}
cgops=${PROFILE_CG_OPS:-100}
flame=${FLAMEGRAPH_DIR:-}
mkdir -p "$raw" "$work" || exit 1
# the sizes this side ran, part of the identity a baseline artifact must match (analyze.py, review 5 MED 12)
echo "$ops1:$opsn:$cgops" > "$raw/sizes.txt"
runs="$raw/runs.tsv"
: > "$runs"
k=0
bad=0
# fresh: a new database directory in $DB. Never call it as $(fresh): a command substitution is a subshell,
# so k would never advance and every run would reuse db-1 (run 37254757721: every run after the first refused).
fresh() { k=$((k + 1)); DB="$work/db-$k"; }

# record <arm> <what> <rc>: rc 0 is ok; anything else fails the script (rc 4 is handled by the caller)
record() {
  printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$runs"
  [ "$3" = 0 ] || { echo "profile: $1 $2 rc=$3" >&2; bad=1; }
}

for arm in $arms; do
  class=${arm%%-*}
  rest=${arm#*-}
  store=${rest%%-*}
  c=${arm##*-c}
  d="$raw/$arm"
  mkdir -p "$d"
  args=(--class "$class" --clients "$c")
  [ "$store" = cat ] && args+=(--catalog)
  if [ "$c" = 1 ]; then args+=(--ops "$ops1" --warmup 20); else args+=(--ops "$opsn" --warmup 2); fi

  # 1. plain
  fresh
  timeout 1800 "$drv" --dir "$DB" "${args[@]}" --out "$d/plain" > "$d/plain.stdout" 2>&1
  rc=$?
  if [ $rc = 4 ] && grep -q '^NOT AVAILABLE' "$d/plain.stdout"; then
    head -1 "$d/plain.stdout" > "$d/status"
    printf '%s\tall\tNOT-AVAILABLE\n' "$arm" >> "$runs"
    continue
  fi
  record "$arm" plain $rc

  # 2. strace, every thread, fds shown as paths
  fresh
  timeout 3600 strace -f -qq -y -o "$d/strace.txt" "$drv" --dir "$DB" "${args[@]}" --mark \
    --out "$d/strace-run" > "$d/strace.stdout" 2>&1
  record "$arm" strace $?

  # 3. perf stat, one run per window, counting only inside it
  for w in create connect write delete; do
    ctl="$work/ctl-$k.fifo" ack="$work/ack-$k.fifo"
    rm -f "$ctl" "$ack"
    mkfifo "$ctl" "$ack"
    fresh
    timeout 1800 perf stat -D -1 --control "fifo:$ctl,$ack" -x, -o "$d/perfstat-$w.csv" \
      -e task-clock,context-switches,cpu-migrations,page-faults,instructions:u,cycles:u,instructions:k,cycles:k \
      -- "$drv" --dir "$DB" "${args[@]}" --perf-ctl "$ctl,$ack" --perf-only "$w" > "$d/perfstat-$w.stdout" 2>&1
    record "$arm" "perfstat-$w" $?
    rm -f "$ctl" "$ack"
  done

  # 4. perf record of the create window -> flame graph (D2 only)
  if [ "$class" = full ]; then
    # The flame graph needs on-CPU samples: a create is ~15k instructions and mostly waits in fsync,
    # so 200 creates at 2 kHz gave none (run 37256052378). Its own run: creates only (--phases 1, no
    # connections held), 25x the ops, 15 kHz.
    ctl="$work/ctl-r-$k.fifo" ack="$work/ack-r-$k.fifo"
    rm -f "$ctl" "$ack"
    mkfifo "$ctl" "$ack"
    fresh
    fargs=(--class "$class" --clients "$c" --phases 1)
    [ "$store" = cat ] && fargs+=(--catalog)
    if [ "$c" = 1 ]; then fargs+=(--ops $((25 * ops1)) --warmup 20); else fargs+=(--ops $((25 * opsn)) --warmup 2); fi
    timeout 1800 perf record -F 15000 -g --call-graph dwarf,16384 -D -1 --control "fifo:$ctl,$ack" \
      -o "$work/perf-$arm.data" -- "$drv" --dir "$DB" "${fargs[@]}" --perf-ctl "$ctl,$ack" \
      --perf-only create > "$d/perfrecord.stdout" 2>&1
    record "$arm" perf-record $?
    rm -f "$ctl" "$ack"
    timeout 1800 perf script -i "$work/perf-$arm.data" > "$work/perf-$arm.script" 2> "$d/perfscript.stderr"
    record "$arm" perf-script $?
    if [ -n "$flame" ]; then
      "$flame/stackcollapse-perf.pl" "$work/perf-$arm.script" > "$d/perf-create.folded" 2>/dev/null
      if [ -s "$d/perf-create.folded" ]; then
        "$flame/flamegraph.pl" --title "fastest_profile $side $arm: create window (on-CPU)" \
          "$d/perf-create.folded" > "$d/flame-create.svg"
        record "$arm" flamegraph $?
      else
        # No sample is a fact about the window (nothing on CPU long enough), recorded, never a pass.
        printf '%s\t%s\t%s\n' "$arm" flamegraph NO-SAMPLES >> "$runs"
      fi
    fi
  fi

  # 5. callgrind, C=1: whole-program Ir of runs doing only the first K phases (K = 1..4) at N and 2N ops;
  # a phase's Ir per op is a difference of differences, so the setup cancels and no per-function
  # attribution is trusted (callgrind reported false recursion and >100% inclusive costs on arm64,
  # run 37255309860).
  if [ "$c" = 1 ]; then
    echo "$cgops" > "$d/cg-n.ops"
    for kph in 1 2 3 4; do
      for n in "$cgops" $((2 * cgops)); do
        tag=n; [ "$n" != "$cgops" ] && tag=2n
        cargs=(--class "$class" --clients 1 --ops "$n" --warmup 0 --phases "$kph")
        [ "$store" = cat ] && cargs+=(--catalog)
        fresh
        timeout 3600 valgrind --tool=callgrind --fair-sched=yes --callgrind-out-file="$work/cg-$arm-k$kph-$tag.out" \
          "$drv" --dir "$DB" "${cargs[@]}" > "$d/cg-k$kph-$tag.stdout" 2> "$d/cg-k$kph-$tag.stderr"
        record "$arm" "callgrind-k$kph-$tag" $?
        # the run's total Ir: the 'totals:' (or older 'summary:') line of the callgrind output file
        sed -nE 's/^(totals|summary): *([0-9]+).*/\2/p' "$work/cg-$arm-k$kph-$tag.out" | tail -1 > "$d/cg-k$kph-$tag.total"
        [ -s "$d/cg-k$kph-$tag.total" ]
        record "$arm" "callgrind-total-k$kph-$tag" $?
      done
    done
    callgrind_annotate --inclusive=yes --threshold=99 "$work/cg-$arm-k4-n.out" > "$d/cg-k4-n.annotate.txt" 2>&1
  fi
  gzip -f "$d/strace.txt"
done
echo "profile: $side done, $(grep -c . "$runs") runs, bad=$bad"
exit $bad
