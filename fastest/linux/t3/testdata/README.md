# t3 testdata

`v3-37812355435-{x86,arm}-ext4loop/`: two real V3 batch records (summary.json, binary.txt, rc) in the probe's
c225124ae format, for `blockgate.py self-test`. Copied byte for byte from turso fork/fastest-linux-v3 94183d127
`fastest/linux/v3/testdata/f3-37812355435-*-ext4loop/F3/`, which is run 37812355435's F3 batch with the four fields
that README names (arms_gated, flush_control_arms.fdatasync4k.gated, traced, floor_frame_variant) set as the
eighth-review probe writes them. x86 is a write-back NVMe leaf (MSFT NVMe Accelerator), arm a write-through sda
leaf (Virtual Disk); both are smoke (unbound) batches, rc 0.

`v3l-37812992594-brd-xfs-b0/v3l.json`: a real V3L record from a brd block (ram0, write through, drive report
`none (RAM)`, VALID), for `v3l.py self-test` (fourth lane review HIGH 3: plants() must run and fire on it). Copied byte
for byte from artie-research c43ca05e0b `frontier/fastest/linux/t3-runs/37812992594/t3-dryrun-ubuntu-24.04-brd/t3-out/fs-xfs/v3l-b0/v3l.json`,
which is dry run 37812992594's brd job, xfs, measurement b0.
