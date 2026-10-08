#!/bin/bash
# nsfake.sh PREMISE UID GID SRC:DST... -- CMD... -- the V3 fire-check's view-planting helper (fourth review M1; fifth
# review M1, M2, L6). Run it as root inside a private mount namespace:
#   sudo unshare -m --propagation private bash nsfake.sh PREMISE UID GID SRC:DST... -- CMD...
# Each DST (a file under /proc or /sys the probe reads) is bind-mounted over by SRC in that namespace only, so nothing
# outside it sees the change and it ends with CMD. The premise is then read back INSIDE the namespace into PREMISE,
# one line per DST, "<basename of DST>=<its first line>", plus "hypervisor_flags=<count of the word hypervisor in
# /proc/cpuinfo>", before CMD starts as UID:GID (setpriv, supplementary groups from the user database).
# Fire-check only. Exit: 2 usage, 97 a bind failed, else CMD's.
set -u
[ $# -ge 5 ] || { echo "nsfake: usage: PREMISE UID GID SRC:DST... -- CMD..." >&2; exit 2; }
prem=$1 uid=$2 gid=$3
shift 3
binds=()
while [ $# -gt 0 ] && [ "$1" != -- ]; do binds+=("$1"); shift; done
[ "${1:-}" = -- ] && [ $# -ge 2 ] || { echo "nsfake: usage: PREMISE UID GID SRC:DST... -- CMD..." >&2; exit 2; }
shift
for b in "${binds[@]}"; do
  mount --bind "${b%%:*}" "${b#*:}" || { echo "nsfake: bind of ${b%%:*} over ${b#*:} failed" >&2; exit 97; }
done
{
  for b in "${binds[@]}"; do d=${b#*:}; echo "$(basename "$d")=$(head -1 "$d")"; done
  echo "hypervisor_flags=$(grep -c -w hypervisor /proc/cpuinfo)"
} > "$prem"
exec setpriv --reuid="$uid" --regid="$gid" --init-groups "$@"
