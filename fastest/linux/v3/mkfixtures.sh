#!/bin/bash
# mkfixtures.sh BASE -- the V3 fire-check's flush-path refusal fixtures (uses sudo; for CI or a scratch box):
#   BASE/nb/w    a directory on an ext4 loop mounted -o nobarrier                     (barrier off at the top layer)
#   BASE/nbx/w   a directory on an XFS loop whose backing file sits on BASE/nb's ext4 (barrier off one layer down)
# The backing image of BASE/nb sits on the root filesystem. Proves what it made before returning 0 (else exit 1):
# BASE/nb's mount options carry nobarrier, BASE/nbx's do not, and BASE/nbx's loop is backed by a file under BASE/nb.
set -euo pipefail
base=${1:?usage: mkfixtures.sh BASE}
me="$(id -u):$(id -g)"
sudo mkdir -p "$base/nb" "$base/nbx"
img=/v3fx-nb.img
sudo rm -f "$img"
sudo truncate -s 3G "$img"
d1=$(sudo losetup --find --show "$img")
sudo mkfs.ext4 -F -q "$d1"
sudo mount -o nobarrier "$d1" "$base/nb"
sudo chown "$me" "$base/nb"
mkdir -p "$base/nb/w"
truncate -s 1G "$base/nb/x.img"
d2=$(sudo losetup --find --show "$base/nb/x.img")
sudo mkfs.xfs -f -q "$d2"
sudo mount "$d2" "$base/nbx"
sudo chown "$me" "$base/nbx"
mkdir -p "$base/nbx/w"
o1=$(findmnt -n -o OPTIONS -T "$base/nb/w")
o2=$(findmnt -n -o OPTIONS -T "$base/nbx/w")
echo "fixture nb:  $(findmnt -n -o SOURCE,FSTYPE -T "$base/nb/w") $o1"
echo "fixture nbx: $(findmnt -n -o SOURCE,FSTYPE -T "$base/nbx/w") $o2 backing=$(losetup -n -O BACK-FILE "$d2")"
case ",$o1," in *,nobarrier,*) ;; *) echo "mkfixtures: $base/nb is not mounted nobarrier" >&2; exit 1 ;; esac
case ",$o2," in *,nobarrier,*) echo "mkfixtures: $base/nbx should not itself be nobarrier" >&2; exit 1 ;; esac
case "$(losetup -n -O BACK-FILE "$d2" | xargs)" in "$base/nb/"*) ;; *) echo "mkfixtures: $d2 is not backed under $base/nb" >&2; exit 1 ;; esac
