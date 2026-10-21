#!/usr/bin/env bash
# nestloops.sh -- THE nest-chain loop matcher (V3 review 12 item 1), sourced by mkfixtures.sh and firecheck.sh. Run it
# directly with --self-test to check it on canned listings (expectations written here, by hand).
#
#   loop_list          print `losetup --list -n -O NAME,BACK-FILE`; return 2 when losetup fails, its own stderr
#                      passing straight through. A failing losetup is an error, never "nothing attached" (the copies
#                      before this discarded its errors into awk).
#   nest_match BASE    a filter: reads "DEV BACKFILE" lines, prints DEV for each whose backing file is under
#                      BASE/n1/ .. BASE/n4/ (a level of the chain, anchored on the level's digit and its slash: nbx's
#                      BASE/nb/x.img is NOT the chain, nor is BASE/n10/), or is an image named v3fx-n1.img (the chain's
#                      first image, on whatever directory, " (deleted)" or not).
#   nest_loops BASE    loop_list | nest_match BASE; returns 2 when loop_list fails or the filter does (V3 review 13
#                      item 10: a filter that failed returned its own status, and an empty output reads as nothing).
#   nest_level_match DIR  a filter like nest_match: prints DEV for each backing file under DIR/ (anchored on DIR and its
#                      slash, a trailing slash on DIR the same: /mnt/v3fx/n1 does not match n10/, n1x/ or a file named
#                      n1 itself). teardown_nest's per-level sweep (V3 review 13 item 20: it kept its own copy).
# BLIND SPOT, stated: a backing path holding runs of spaces is compared with single spaces (awk rebuilds the field).
cell_nest_dir() {
  local mnt src backing
  mnt=$(readlink -m "${1:?cell_nest_dir MNT}") || return 2
  src=$(findmnt -n -o SOURCE -T "$mnt") || return 2
  case $src in
    /dev/loop*)
      backing=$(losetup -n -O BACK-FILE "$src") || return 2
      case $backing in /*) ;; *) echo "cell_nest_dir: no absolute loop backing file" >&2; return 2 ;; esac
      readlink -m "$(dirname "$backing")" ;;
    /*) printf '%s\n' "$mnt" ;;
    *) echo "cell_nest_dir: no mount source" >&2; return 2 ;;
  esac
}

loop_list() {
  local out rc
  out=$(losetup --list -n -O NAME,BACK-FILE); rc=$?  # losetup's stderr is ours (V3 review 13 item 10)
  if [ "$rc" -ne 0 ]; then
    echo "loop_list: losetup --list failed (rc $rc)" >&2
    return 2
  fi
  [ -z "$out" ] || printf '%s\n' "$out"
}
nest_match() {
  awk -v b="$1" '{ d = $1; $1 = ""; sub(/^ +/, ""); p = $0
    lvl = (index(p, b "/n") == 1 && substr(p, length(b) + 3, 1) ~ /^[1-4]$/ && substr(p, length(b) + 4, 1) == "/")
    if (lvl || p ~ /\/v3fx-n1\.img( \(deleted\))?$/) print d }'
}
nest_level_match() {
  awk -v b="${1%/}/" '{ d = $1; $1 = ""; sub(/^ +/, ""); if (index($0, b) == 1) print d }'
}
nest_loops() {
  local l
  l=$(loop_list) || return 2
  [ -z "$l" ] && return 0
  printf '%s\n' "$l" | nest_match "$1" || return 2  # the filter's status is its own (the pipeline's last command)
}

nestloops_self_test() {
  local pass=0 fail=0 td rc
  t() {  # t NAME GOT WANT
    if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "PASS $1"; else fail=$((fail + 1)); echo "FAIL $1: got '$2', want '$3'"; fi
  }
  local canned='/dev/loop0 /mnt/v3fx/nb/x.img
/dev/loop1 /v3fx-n1.img
/dev/loop2 /mnt/v3fx/n1/x.img
/dev/loop3 /mnt/v3fx/n2/x.img
/dev/loop4 /mnt/v3fx/n3/x.img
/dev/loop5 /mnt/v3fx/n10/x.img
/dev/loop6 /mnt/v3fx/nbx.img
/dev/loop7 /data/v3fx-n1.img (deleted)
/dev/loop8 /mnt/v3fx/n1/stray.img
/dev/loop9 /mnt/v3fxn1/x.img
/dev/loop10 /mnt/v3fx/n4/x.img
/dev/loop11 /mnt/v3fx/nb/teardown-plant.img
/dev/loop12 /mnt/v3fx/n5/x.img
/dev/loop13 /srv/v3fx-n1.img.bak'
  t "canned: the chain's levels n1..n4 and v3fx-n1.img (deleted or not) only; nb/, nbx, n10, n5, v3fxn1 and a .bak are not" \
    "$(printf '%s\n' "$canned" | nest_match /mnt/v3fx | xargs)" \
    "/dev/loop1 /dev/loop2 /dev/loop3 /dev/loop4 /dev/loop7 /dev/loop8 /dev/loop10"
  t "canned: nbx's backing /mnt/v3fx/nb/x.img alone matches nothing" \
    "$(printf '/dev/loop0 /mnt/v3fx/nb/x.img\n' | nest_match /mnt/v3fx | xargs)" ""
  t "canned: an empty listing matches nothing" "$(printf '' | nest_match /mnt/v3fx | xargs)" ""
  # V3 review 13 item 20: teardown_nest's per-level sweep kept its own prefix matcher; it is nest_level_match now.
  # Under a level's directory, anchored on DIR/ (n10/, n1x/ and v3fxn1/ are not n1; a trailing slash on DIR is the same)
  local lvl='/dev/loop2 /mnt/v3fx/n1/x.img
/dev/loop5 /mnt/v3fx/n10/x.img
/dev/loop8 /mnt/v3fx/n1/stray.img
/dev/loop9 /mnt/v3fxn1/x.img
/dev/loop14 /mnt/v3fx/n1x/a.img
/dev/loop15 /mnt/v3fx/n1
/dev/loop0 /mnt/v3fx/nb/x.img
/dev/loop6 /mnt/v3fx/nbx.img'
  t "level: nest_level_match /mnt/v3fx/n1 -> the files under n1/ only (not n10/, n1x/, v3fxn1/, nor n1 itself)" \
    "$(printf '%s\n' "$lvl" | nest_level_match /mnt/v3fx/n1 | xargs)" "/dev/loop2 /dev/loop8"
  t "level: a trailing slash on DIR is the same level" \
    "$(printf '%s\n' "$lvl" | nest_level_match /mnt/v3fx/n1/ | xargs)" "/dev/loop2 /dev/loop8"
  t "level: nest_level_match /mnt/v3fx/nb -> nb/x.img, not nbx.img" \
    "$(printf '%s\n' "$lvl" | nest_level_match /mnt/v3fx/nb | xargs)" "/dev/loop0"
  t "root ext4 keeps the supplied cell directory" \
    "$(findmnt() { echo /dev/sda1; }; cell_nest_dir /mnt/fastest-v3-ext4)" /mnt/fastest-v3-ext4
  t "loop cell uses its backing directory" \
    "$(findmnt() { echo /dev/loop0; }; losetup() { echo /var/tmp/cell.img; }; cell_nest_dir /mnt/cell)" /var/tmp
  td=$(mktemp -d) || { echo "FAIL mktemp"; return 1; }
  printf '#!/bin/sh\necho "losetup: cannot open /dev/loop-control" >&2\nexit 1\n' > "$td/losetup"
  chmod +x "$td/losetup"
  PATH="$td:$PATH" nest_loops /mnt/v3fx > /dev/null 2>&1; rc=$?
  t "a losetup that fails -> nest_loops returns 2 (never 'nothing attached')" "$rc" 2
  printf '#!/bin/sh\nprintf "%%s\\n" "/dev/loop0 /mnt/v3fx/nb/x.img" "/dev/loop2 /mnt/v3fx/n1/x.img"\n' > "$td/losetup"
  t "a working losetup: nest_loops prints the chain's loop only" "$(PATH="$td:$PATH" nest_loops /mnt/v3fx | xargs)" "/dev/loop2"
  printf '#!/bin/sh\nexit 0\n' > "$td/losetup"
  PATH="$td:$PATH" nest_loops /mnt/v3fx > /dev/null; rc=$?
  t "no loop attached: nest_loops prints nothing and returns 0" "$rc" 0
  # V3 review 13 item 10: a filter (awk) that fails is a failure, returned as 2 like a listing failure, never an empty
  # "nothing attached"; and loop_list passes losetup's own stderr straight through
  mkdir -p "$td/badawk"
  printf '#!/bin/sh\nprintf "%%s\\n" "/dev/loop2 /mnt/v3fx/n1/x.img"\n' > "$td/losetup"
  printf '#!/bin/sh\nexit 3\n' > "$td/badawk/awk"
  chmod +x "$td/badawk/awk"
  PATH="$td/badawk:$td:$PATH" nest_loops /mnt/v3fx > /dev/null 2>&1; rc=$?
  t "a filter (awk) that fails -> nest_loops returns 2 (never 'nothing attached')" "$rc" 2
  printf '#!/bin/sh\necho "losetup: cannot open /dev/loop-control" >&2\nexit 1\n' > "$td/losetup"
  t "a failing losetup's own stderr reaches loop_list's stderr" \
    "$(PATH="$td:$PATH" loop_list 2>&1 > /dev/null | grep -q 'losetup: cannot open /dev/loop-control' && echo seen)" seen
  rm -rf "$td"
  echo "NESTLOOPS SELF-TEST $pass/$((pass + fail)) $([ "$fail" = 0 ] && [ "$pass" -gt 0 ] && echo PASS || echo FAIL)"
  [ "$fail" = 0 ] && [ "$pass" -gt 0 ]
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  [ "${1:-}" = --self-test ] || { echo "usage: nestloops.sh --self-test (otherwise source it)" >&2; exit 2; }
  nestloops_self_test
  exit $?
fi
