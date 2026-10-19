#!/usr/bin/env bash
# clonebench_schema_check.sh CLONEBENCH SQLITE3 SCRATCH -- the VM --dry-block defect 2 check on a real clonebench: a
# fresh 2000-row parent, one closed-loop m1c d0 run at C=4 (warm-up 50:0:60, 400 ops, --drop) and one at C=1, and on
# each raw.tsv: raw_schema.py header (the one schema) and timedrun.py warm-check 50:0:60 (what the B1 timed cells and
# the warm-up plants hit). Exit 0 only when every check reads ok.
set -uo pipefail
CB=$1 SQ3=$2 S=$3
HERE="$(cd "$(dirname "$0")" && pwd)"
[ -e "$S" ] && { echo "REFUSED: $S exists" >&2; exit 2; }
# clonebench's own directory rule (baselines/clonebench.c), checked here first so a wrong SCRATCH is refused naming
# why, not reported as "clonebench rc 2" (2026-10-16: a brief sent this to a VM's ext4 /tmp): a *.noindex path
# component, and a clone-capable filesystem: APFS on macOS (clonefile), xfs or btrfs on Linux (FICLONE)
case "/$S/" in */*.noindex/*) ;; *)
  echo "REFUSED: SCRATCH $S has no *.noindex path component: clonebench needs FICLONE on xfs/btrfs (APFS clonefile on macOS) under *.noindex" >&2
  exit 2 ;; esac
P=$(dirname "$S")
case $(uname -s) in
  Linux) fs=$(stat -f -c %T "$P" 2>/dev/null)
         case $fs in xfs|btrfs) ;; *)
           echo "REFUSED: SCRATCH's filesystem is ${fs:-unreadable} ($P): clonebench needs FICLONE on xfs/btrfs under *.noindex" >&2
           exit 2 ;; esac ;;
  Darwin) fs=$(diskutil info "$(df "$P" | awk 'NR == 2 { print $1 }')" 2>/dev/null | awk -F: '/Type \(Bundle\)/ { gsub(/ /, "", $2); print $2 }')
          [ "$fs" = apfs ] || {
            echo "REFUSED: SCRATCH's filesystem is ${fs:-unreadable} ($P): clonebench needs APFS clonefile on macOS under *.noindex" >&2
            exit 2; } ;;
  *) echo "REFUSED: $(uname -s): clonebench runs on Linux (xfs/btrfs) or macOS (APFS) only" >&2; exit 2 ;;
esac
mkdir -p "$S/branches" || exit 2
python3 -B "$HERE/fixture.py" sqlite "$S/parent.db" --rows 2000 --age 0 --sqlite3 "$SQ3" >"$S/parent.json" || exit 2
bad=0
for c in 4 1; do
  o="$S/run-c$c"
  timeout 300 "$CB" run --mode b1 --op m1c --sync d0 --parent "$S/parent.db" --dir "$S/branches" --clients "$c" \
    --max-ops 400 --rows 2000 --warmup 50:0:60 --max-window-s 60 --drop --out "$o" >"$o.txt" 2>&1 || { echo "C=$c: clonebench rc $?"; bad=$((bad + 1)); continue; }
  h=$(python3 -B "$HERE/raw_schema.py" header "$o/raw.tsv")
  w=$(python3 -B "$HERE/timedrun.py" warm-check "$o" 50:0:60 | tail -1)
  echo "C=$c: header [$(head -1 "$o/raw.tsv" | cut -f1-6 | tr '\t' ' ') ...] raw_schema [$h] warm-check [$w]"
  [ "$h" = ok ] && [ "$w" = ok ] || bad=$((bad + 1))
done
[ $bad = 0 ]
