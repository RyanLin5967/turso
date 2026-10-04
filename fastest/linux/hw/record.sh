#!/bin/bash
# Runner hardware record. Hosted runners vary from job to job (PREREG T3), so every run carries its
# own: CPU, memory, kernel, the Azure VM size, every block device's model and write-cache mode, the
# filesystem and mount under the target directory, and fio small-write sync latency measured IN the
# target directory (the device floor the run sat on).
#
# usage: record.sh <out-dir> <target-dir> [fio seconds per job, default 15]
# Writes <out-dir>/hw.txt (every command, its output and rc), <out-dir>/fio-<job>.json (raw) and
# <out-dir>/hw.json (summary). Exits 1 if a REQUIRED part is missing: lscpu, the target's mount, or
# a parsed fio sync latency for either job. Optional parts (Azure metadata, nvme-cli) are recorded as
# absent, never filled in.
set -uo pipefail
out=${1:?usage: record.sh <out-dir> <target-dir> [secs]}
target=${2:?usage: record.sh <out-dir> <target-dir> [secs]}
secs=${3:-15}
mkdir -p "$out" "$target" || exit 1
fail=0

run() { # print the command, its output and its rc; return the rc
  echo "\$ $*"
  "$@" 2>&1
  local rc=$?
  echo "[rc=$rc]"
  return $rc
}

{
  echo "# fastest-linux hw record start=$(date -u +%FT%TZ) host=$(hostname) target=$target"
  echo; echo "## os"; run uname -a; run cat /etc/os-release
  echo; echo "## cpu"; run lscpu || echo "REQUIRED-MISSING lscpu"; run nproc
  echo; echo "## memory"; run free -m
  echo; echo "## azure instance metadata (optional)"
  run timeout 5 curl -s -H Metadata:true \
    "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01&format=json"
  echo; echo "## block devices"
  run lsblk -o NAME,KNAME,TYPE,SIZE,MODEL,VENDOR,ROTA,DISC-GRAN,LOG-SEC,PHY-SEC,FSTYPE,MOUNTPOINTS
  for d in /sys/block/*; do
    n=${d##*/}
    printf '%s model=[%s] vendor=[%s] write_cache=[%s] fua=[%s] rotational=[%s] scheduler=[%s]\n' "$n" \
      "$(cat "$d/device/model" 2>/dev/null | xargs)" "$(cat "$d/device/vendor" 2>/dev/null | xargs)" \
      "$(cat "$d/queue/write_cache" 2>/dev/null)" "$(cat "$d/queue/fua" 2>/dev/null)" \
      "$(cat "$d/queue/rotational" 2>/dev/null)" "$(cat "$d/queue/scheduler" 2>/dev/null)"
  done
  echo; echo "## loop devices"; run losetup -l -O NAME,BACK-FILE,DIO,LOG-SEC
  echo; echo "## nvme (optional)"; command -v nvme >/dev/null && run sudo nvme list
  echo; echo "## target"
  run findmnt -T "$target" -o TARGET,SOURCE,FSTYPE,OPTIONS || echo "REQUIRED-MISSING findmnt"
  run df -h "$target"
  echo; echo "## fio $(fio --version 2>&1) (${secs}s per job, 4 KiB writes, ioengine=sync)"
} > "$out/hw.txt" 2>&1

grep -q '^REQUIRED-MISSING' "$out/hw.txt" && fail=1

# Two jobs, each a fresh file in the target directory:
# - fdatasync-overwrite: the file is laid out first (overwrite=1), so each 4 KiB write + fdatasync
#   is data only: the closest fio gets to the device flush floor.
# - fsync-append: a fresh file grown by each write (no layout), so each fsync also commits the
#   allocation metadata, as a log append does.
fio_job() { # fio_job <name> <fio args...>
  local name=$1; shift
  rm -f "$target/fio-$name.dat"
  timeout $((secs + 120)) fio --name="$name" --filename="$target/fio-$name.dat" --rw=write --bs=4k \
    --ioengine=sync --time_based --runtime="$secs" --output-format=json "$@" \
    > "$out/fio-$name.json" 2> "$out/fio-$name.stderr"
  local rc=$?
  rm -f "$target/fio-$name.dat"
  echo "fio $name rc=$rc" >> "$out/hw.txt"
}
fio_job fdatasync-overwrite --size=256m --overwrite=1 --fdatasync=1
fio_job fsync-append --size=1g --overwrite=0 --fsync=1

python3 - "$out" "$target" <<'PY' || fail=1
import json, os, subprocess, sys
out, target = sys.argv[1], sys.argv[2]
def sh(*cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception as e:
        return f"ERROR {e}"
summary = {"target": target, "missing": []}
mnt = sh("findmnt", "-n", "-T", target, "-o", "TARGET,SOURCE,FSTYPE,OPTIONS").split()
summary["mount"] = dict(zip(["target", "source", "fstype", "options"], mnt)) if len(mnt) == 4 else None
if summary["mount"] is None:
    summary["missing"].append("mount")
cpu = {}
for line in sh("lscpu").splitlines():
    k, _, v = line.partition(":")
    cpu[k.strip()] = v.strip()
summary["cpu_model"] = cpu.get("Model name")
summary["arch"] = cpu.get("Architecture")
summary["nproc"] = sh("nproc")
summary["kernel"] = sh("uname", "-r")
if not summary["cpu_model"]:
    summary["missing"].append("lscpu")
try:
    meta = json.loads(sh("timeout", "5", "curl", "-s", "-H", "Metadata:true",
        "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01&format=json"))
    summary["azure_vm_size"] = meta.get("vmSize")
    summary["azure_location"] = meta.get("location")
except Exception:
    summary["azure_vm_size"] = None  # optional: absent, not invented
disks = []
for n in sorted(os.listdir("/sys/block")):
    def rd(p):
        try:
            return open(f"/sys/block/{n}/{p}").read().strip()
        except OSError:
            return None
    disks.append({"name": n, "model": rd("device/model"), "vendor": rd("device/vendor"),
                  "write_cache": rd("queue/write_cache"), "fua": rd("queue/fua"),
                  "rotational": rd("queue/rotational")})
summary["block_devices"] = disks
summary["fio"] = {}
for job in ("fdatasync-overwrite", "fsync-append"):
    try:
        j = json.load(open(f"{out}/fio-{job}.json"))["jobs"][0]
        s = j["sync"]["lat_ns"]
        w = j["write"]["lat_ns"]
        pct = s["percentile"]
        summary["fio"][job] = {
            "ops": j["write"]["total_ios"],
            "sync_n": s["N"],
            "sync_mean_us": round(s["mean"] / 1e3, 2),
            "sync_p50_us": round(pct["50.000000"] / 1e3, 2),
            "sync_p99_us": round(pct["99.000000"] / 1e3, 2),
            "write_mean_us": round(w["mean"] / 1e3, 2),
        }
        if not s["N"]:
            summary["missing"].append(f"fio:{job}:zero-syncs")
    except Exception as e:
        summary["fio"][job] = {"error": repr(e)}
        summary["missing"].append(f"fio:{job}")
json.dump(summary, open(f"{out}/hw.json", "w"), indent=1)
print(json.dumps({"mount": summary["mount"], "cpu": summary["cpu_model"], "vm": summary["azure_vm_size"],
                  "fio": summary["fio"], "missing": summary["missing"]}))
sys.exit(1 if summary["missing"] else 0)
PY
[ $fail -eq 0 ] || { echo "hw record INCOMPLETE: see $out/hw.txt and $out/hw.json" >&2; exit 1; }
