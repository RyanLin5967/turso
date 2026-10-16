# Our recorded replay answers

These 94 files record our replies at `a8f4eab84d350aa31fd2daa5bbe9d7ed1dddeef8`, the lead-ruled C55 base. They preserve the PostgreSQL command text and use the replay harness's existing reply formatter. `manifest.json` pins both our answers and the unchanged PostgreSQL recordings by SHA-256.

`capture.patch` is the observation-only change used for this run: it copies each reply after sending the command and does not alter comparison or server behavior. Run through the bounded buildslot with `cargo test -p tursopg --test wire_sessions replays_as_postgresql_answered -- --test-threads=1 --nocapture`, jobs 2 and `RUST_MIN_STACK=8388608`. All 94 generated replay tests passed. The broad filter also selected the existing `the_float_recording_replays_as_postgresql_answered_it`, which failed its max-float expectation; that test does not produce one of these files.

The register compares each registered command with its exact reply here. A changed answer must be investigated; these fixtures are not automatically rewritten. A command that starts agreeing with PostgreSQL must leave the divergence register. Only `pg18-random.out` #2 compares shape instead of its sampled row value. These are records of our differing behavior, not replacements for PostgreSQL expectations.
