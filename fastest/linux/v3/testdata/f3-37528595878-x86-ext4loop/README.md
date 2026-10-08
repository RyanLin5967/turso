# f3-37528595878-x86-ext4loop

A banked F3 batch for `check.py --self-test` (`real_selftest`). It comes from run 37528595878 at turso 9f218af4a, the
x86_64 ext4loop cell (write-back MSFT NVMe, a VM), with raw in artie
`frontier/fastest/linux/v3/runs/37528595878/v3-ubuntu-24.04-ext4loop/firecheck/F3/`.

**Upgraded, interim.** The probe it came from predates the gate-6 record format: annex A14/A16/A17/A18 and the
sixth review's box. The fields that probe never wrote were synthesized from the batch's own raw.tsv and records:

| Fields | Where |
|---|---|
| `pid`, `plp` ("no") | summary |
| `registered_file`, `d0_threshold*` (provisional 10, key d0_threshold/ext4/wb/vm) | summary |
| `timing_control` and `flush_control` (from the raw append25/nosync25 p50s), `timing_gated_arms` | summary |
| `d0_control` | summary |
| `timing_gated` per arm | summary |
| `frame_*` (none registered) | summary |
| `floor_reference` (A18: min p50 of append25 and fdatasync4k) | summary |
| `app_syncs_per_op` | merged summary |
| `syscalls` (per arm: one sync per op, two for clone2b/cfr2b, none for nosync25) | blkflush report |
| `voids` | gate and flush gate |
| `plp`, `shape` | binary.txt |
| `plp`, `leafdisk`, `box` | info.txt |

The script that made the upgrade is lane-local, in the lane's scratchpad `v3/upgrade_td.py`. Every other byte is the
banked run's.

The next CI run's real batches replace it: a write-back cell and a write-through cell, so the write-through rules
also run through check_real. When that happens, this README is replaced by a plain provenance line.
