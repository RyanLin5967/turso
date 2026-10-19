# PostgreSQL 18.6 recordings that are not replays

`tests/pg18/` holds pgproto.py transcripts, each replayed by its `replays_postgresql!` test
(`every_pg18_recording_has_one_replay` holds every file there to one). A recording here holds what
that transcript format cannot carry, so no replay can read it; the test that cites it asserts it.

- `pg18-typmod.out`: each RowDescription field's type size (typlen) and type modifier (atttypmod),
  read by a raw v3 client (`name:oid/typlen/typmod`). pgproto.py prints a RowDescription as its
  type OIDs alone (`T oids=[...]`). Asserted by `row_descriptions_carry_postgresqls_type_sizes_and_modifiers`.
