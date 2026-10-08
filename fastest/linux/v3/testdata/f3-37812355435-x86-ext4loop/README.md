# f3-37812355435-x86-ext4loop

A banked F3 batch for `check.py --self-test` (`real_selftest`): run 37812355435 at turso c225124ae, raw in artie
`frontier/fastest/linux/v3/runs/37812355435/v3-ubuntu-24.04-ext4loop/firecheck/F3/`. Everything here is that run's bytes (the blkflush report
included: report() ran on CI), except four fields in summary.probe.json and summary.json that the next probe
(eighth review) adds or changes: `arms_gated` (+ fdatasync4k, M1), `flush_control_arms.fdatasync4k.gated` (true),
`traced` (false: run.sh's batch runs untraced, H2), `floor_frame_variant` (no frame arm registered, M2). The next
green run's own batch replaces this with no edit at all.
