# t3 testdata

`v3-37812355435-{x86,arm}-ext4loop/`: two real V3 batch records (summary.json, binary.txt, rc) in the probe's
c225124ae format, for `blockgate.py self-test`. They are run 37812355435's F3 batch bytes AS RUN, copied from artie
`frontier/fastest/linux/v3/runs/37812355435/v3-ubuntu-24.04[-arm]-ext4loop/firecheck/F3/` (fourth lane review
LOW 19 and 26: not v3/testdata's copy, whose summary.json carries four fields edited for the next probe, including
fdatasync4k marked flush-gated, so its required_flushes 1200 no longer equals n x the gated arms, 200 x 7). x86 is a
write-back NVMe leaf (MSFT NVMe Accelerator), arm a write-through sda leaf (Virtual Disk); both are smoke (unbound)
batches of N=200 with rc 0 (read from rc).
