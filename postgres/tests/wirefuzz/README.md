# wirefuzz: a wire-level differential fuzzer against PostgreSQL 18.6

`wirefuzz.py` generates random SQL sequences (DDL, DML, transactions and savepoints, COPY FROM STDIN, statements with
`$n` parameters over Parse/Bind/Execute, expected errors, and the branch functions, which run on tursopg only and are
never compared), runs each on a fresh `tursopg --server` and a fresh PostgreSQL 18.6 database over the simple and the
extended protocol, and compares SQLSTATEs, CommandComplete tags, RowDescription type OIDs, ReadyForQuery status and
rows. See the module docstring for the rules: explicit seeds, an exact-tuple allowlist (`allowlist.json`), a
delta-debugging shrinker, and the two controls (PostgreSQL against itself must agree; a planted divergence must be
reported). `shard.sh` runs one shard on a UW host (build at a sha, both controls, one seed range). Stdlib Python only.
