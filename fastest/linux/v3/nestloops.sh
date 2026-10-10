#!/usr/bin/env bash
# nestloops.sh -- THE nest-chain loop matcher (V3 review 12 item 1), sourced by mkfixtures.sh and firecheck.sh. Run it
# directly with --self-test to check it on canned listings (expectations written here, by hand).
#
#   loop_list          print `losetup --list -n -O NAME,BACK-FILE`; return 2 when losetup fails, its stderr kept on
#                      ours. A failing losetup is an error, never "nothing attached" (the copies before this
#                      discarded its errors into awk).
#   nest_match BASE    a filter: reads "DEV BACKFILE" lines, prints DEV for each whose backing file is under
#                      BASE/n1/ .. BASE/n4/ (a level of the chain, anchored on the level's digit and its slash: nbx's
#                      BASE/nb/x.img is NOT the chain, nor is BASE/n10/), or is an image named v3fx-n1.img (the chain's
#                      first image, on whatever directory, " (deleted)" or not).
#   nest_loops BASE    loop_list | nest_match BASE; returns 2 when loop_list fails.
# BLIND SPOT, stated: a backing path holding runs of spaces is compared with single spaces (awk rebuilds the field).
loop_list() {
  local err out rc
  err=$(mktemp) || return 2
  out=$(losetup --list -n -O NAME,BACK-FILE 2> "$err"); rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "loop_list: losetup --list failed (rc $rc): $(cat "$err")" >&2
    rm -f "$err"
    return 2
  fi
  rm -f "$err"
  [ -z "$out" ] || printf '%s\n' "$out"
}
nest_match() {
  awk -v b="$1" '{ d = $1; $1 = ""; sub(/^ +/, ""); p = $0
    lvl = (index(p, b "/n") == 1 && substr(p, length(b) + 3, 1) ~ /^[1-4]$/ && substr(p, length(b) + 4, 1) == "/")
    if (lvl || p ~ /\/v3fx-n1\.img( \(deleted\))?$/) print d }'
}
nest_loops() {
  local l
  l=$(loop_list) || return 2
  [ -z "$l" ] || printf '%s\n' "$l" | nest_match "$1"
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
  rm -rf "$td"
  echo "NESTLOOPS SELF-TEST $pass/$((pass + fail)) $([ "$fail" = 0 ] && [ "$pass" -gt 0 ] && echo PASS || echo FAIL)"
  [ "$fail" = 0 ] && [ "$pass" -gt 0 ]
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  [ "${1:-}" = --self-test ] || { echo "usage: nestloops.sh --self-test (otherwise source it)" >&2; exit 2; }
  nestloops_self_test
  exit $?
fi
