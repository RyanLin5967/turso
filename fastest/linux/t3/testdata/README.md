# t3 testdata

`v3-37812355435-{x86,arm}-ext4loop/`: two real V3 batch records (summary.json, binary.txt, rc) in the probe's
c225124ae format, for `blockgate.py self-test`. Copied byte for byte from turso fork/fastest-linux-v3 94183d127
`fastest/linux/v3/testdata/f3-37812355435-*-ext4loop/F3/`, which is run 37812355435's F3 batch with the four fields
that README names (arms_gated, flush_control_arms.fdatasync4k.gated, traced, floor_frame_variant) set as the
eighth-review probe writes them. x86 is a write-back NVMe leaf (MSFT NVMe Accelerator), arm a write-through sda
leaf (Virtual Disk); both are smoke (unbound) batches, rc 0.
