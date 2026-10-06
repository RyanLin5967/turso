Banked F3 batches, copied unchanged from artie-research frontier/fastest/linux/v3/runs/<run>/<cell>/firecheck/
(F3/summary.json, F3/stamp_end.json, F3.rc). Input to `batchgate.py self-test` (review 2 item 1, red test 1):
the x86 xfs and ext4 cells sat on write-back NVMe (2821 >= 1200 and 1209 >= 800 leaf flushes); the other seven on
write-through sda (0 flushes). Every one of these batches was rc 0 at base: there was no gate.
