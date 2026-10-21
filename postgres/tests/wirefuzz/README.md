# wirefuzz: a wire-level differential fuzzer against PostgreSQL 18.6

`wirefuzz.py` generates random SQL sequences (DDL, DML, transactions and savepoints, COPY FROM STDIN, statements with
`$n` parameters over Parse/Bind/Execute, expected errors, and the branch functions, which run on tursopg only and are
never compared), runs each on a fresh `tursopg --server` and a fresh PostgreSQL 18.6 database over the simple and the
extended protocol, and compares SQLSTATEs, CommandComplete tags, RowDescription type OIDs, ReadyForQuery status and
rows. See the module docstring for the rules: explicit seeds, an exact-tuple allowlist (`allowlist.json`), a
delta-debugging shrinker, and the two controls (PostgreSQL against itself must agree; a planted divergence must be
reported). `shard.sh` runs one shard on a UW host (build at a sha, both controls, one seed range). Stdlib Python only.

`numfuzz.py` is its numeric-class sibling (the lead ruling on the numeric class): the same targets, client and
comparison, a generator of numbers in every spelling, source, nesting, aggregate and write path, and an oracle that
passes a statement only when the answers are the same or ours is a loud refusal (0A000); no allowlist. `numshard.sh`
runs one shard as `shard.sh` does.
