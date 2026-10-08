#!/bin/bash
# T3 hardware identification: the machine, every NVMe controller and namespace (incl. the volatile
# write cache and anything that names power-loss protection), and every filesystem the run will use.
#
# usage: hwid.sh <out-dir> [target-dir...]
# Writes <out-dir>/hwid.txt (every command, its output and rc) and <out-dir>/hwid.json:
#   nvme[]: controller, model, firmware, vwc_present (id-ctrl VWC bit 0), vwc_enabled (feature 0x06),
#           plp_text (any line of smartctl/id-ctrl output that mentions power loss), oncs/fna raw
#   targets[]: dir, source, fstype, options, barrier (false when nobarrier or barrier=0), backing device's
#           queue/write_cache and queue/fua (through a loop device to its backing file's device)
# PLP is NOT a standard NVMe field: the record carries model + firmware + whatever text the drive
# exposes; whether a model has PLP is read from its datasheet, never inferred here.
# Exit 1 if a target's mount cannot be read or `nvme`/`lsblk` is missing; 0 otherwise (no NVMe is a
# recorded fact, not a failure: a hosted runner has none).
set -uo pipefail
out=${1:?usage: hwid.sh <out-dir> [target-dir...]}
shift
mkdir -p "$out" || exit 1
run() { echo "\$ $*"; "$@" 2>&1; echo "[rc=$?]"; }
{
  echo "# fastest-linux t3 hwid $(date -u +%FT%TZ) host=$(hostname)"
  run uname -a
  run sudo -n dmidecode -t system -t baseboard -t processor -t memory
  run lscpu
  run lsblk -o NAME,KNAME,TYPE,SIZE,MODEL,SERIAL,REV,TRAN,ROTA,LOG-SEC,PHY-SEC,FSTYPE,MOUNTPOINTS
  run sudo -n nvme list
  for c in /dev/nvme[0-9]; do
    [ -e "$c" ] || continue
    run sudo -n nvme id-ctrl -H "$c"
    run sudo -n nvme get-feature -f 0x06 -H "$c"
    run sudo -n smartctl -x "$c"
  done
  for n in /dev/nvme[0-9]n[0-9]; do
    [ -e "$n" ] && run sudo -n nvme id-ns -H "$n"
  done
  run findmnt -o TARGET,SOURCE,FSTYPE,OPTIONS
  run losetup -l -O NAME,BACK-FILE,DIO
} > "$out/hwid.txt" 2>&1

python3 - "$out" "$@" <<'PY'
import json, os, re, subprocess, sys
out, targets = sys.argv[1], sys.argv[2:]
def sh(*a):
    try:
        return subprocess.run(a, capture_output=True, text=True, timeout=60).stdout
    except Exception as e:
        return f"ERROR {e}"
bad = []
nvme = []
for c in sorted(p for p in os.listdir("/dev") if re.fullmatch(r"nvme\d+", p)):
    idc = sh("sudo", "-n", "nvme", "id-ctrl", f"/dev/{c}")
    feat = sh("sudo", "-n", "nvme", "get-feature", "-f", "0x06", f"/dev/{c}")
    smart = sh("sudo", "-n", "smartctl", "-x", f"/dev/{c}")
    f = dict(re.findall(r"^(\w+)\s*:\s*(.*?)\s*$", idc, re.M))
    vwc = f.get("vwc")
    m = re.search(r"Current value:\s*(0x[0-9a-fA-F]+)", feat)
    nvme.append({"controller": c, "model": f.get("mn"), "firmware": f.get("fr"), "serial_present": bool(f.get("sn")),
                 "vwc_raw": vwc, "vwc_present": (int(vwc, 0) & 1 == 1) if vwc else None,
                 "vwc_enabled": (int(m.group(1), 16) & 1 == 1) if m else None,
                 "oncs_raw": f.get("oncs"), "fna_raw": f.get("fna"),
                 "plp_text": [l.strip() for l in (smart + idc).splitlines() if re.search(r"power.?loss|PLP", l, re.I)][:10]})
tg = []
for d in targets:
    r = sh("findmnt", "-n", "-T", d, "-o", "TARGET,SOURCE,FSTYPE,OPTIONS").split()
    if len(r) != 4:
        bad.append(f"no mount for {d}")
        continue
    target, source, fstype, opts = r
    o = opts.split(",")
    barrier = not ("nobarrier" in o or "barrier=0" in o)
    dev = os.path.basename(source)
    chain = [source]
    if dev.startswith("loop"):
        back = sh("losetup", "-n", "-O", "BACK-FILE", source).strip()
        bsrc = sh("findmnt", "-n", "-T", back, "-o", "SOURCE,OPTIONS").split()
        if len(bsrc) == 2:
            chain.append(bsrc[0])
            bo = bsrc[1].split(",")
            barrier = barrier and not ("nobarrier" in bo or "barrier=0" in bo)
            dev = os.path.basename(bsrc[0])
    disk = dev
    if os.path.exists(f"/sys/class/block/{dev}/partition"):
        disk = os.path.basename(os.path.dirname(os.path.realpath(f"/sys/class/block/{dev}")))
    def rd(p):
        try:
            return open(f"/sys/block/{disk}/queue/{p}").read().strip()
        except OSError:
            return None
    tg.append({"dir": d, "mount": target, "source": source, "fstype": fstype, "options": opts,
               "flush_path": chain, "disk": disk, "barrier": barrier,
               "write_cache": rd("write_cache"), "fua": rd("fua")})
json.dump({"nvme": nvme, "targets": tg, "problems": bad}, open(f"{out}/hwid.json", "w"), indent=1)
print(json.dumps({"nvme": [(n["model"], n["vwc_present"], n["vwc_enabled"]) for n in nvme],
                  "targets": [(t["dir"], t["fstype"], t["barrier"], t["write_cache"]) for t in tg], "problems": bad}))
sys.exit(1 if bad else 0)
PY
