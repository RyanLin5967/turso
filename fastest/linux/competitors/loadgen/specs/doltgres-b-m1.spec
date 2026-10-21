# Doltgres M1 (create-to-first-write), variant (b): doltgres-b-m1c.spec (dolt_branch, then PREREG N7's
# USE "postgres/name"; comp-rev-89ec-b3c3 MED 1), then one autocommit UPDATE of a random existing row of t on the new
# branch. STATEMENTS.md row doltgres b m1-switch.
# The lead's ruling (DECISIONS 2026-10-19): Doltgres 1.4.0 poisons a connection that wrote on a branch once that
# branch is deleted, wherever the delete runs (every later statement: "branch not found"; tools/doltgres_use_probe.sh,
# artie 44b1e4dd86), so each cycle opens a FRESH home connection outside the timed window (home per-cycle; its cost is
# raw.tsv home_connect_ns, untimed) and closes it before the delete, which runs on the client's admin connection.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
home per-cycle
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step sql USE "postgres/{branch}"
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency); the switch-back
# runs on the cycle's home connection, which is then closed, and the delete on the admin connection
after sql USE postgres
after admin-sql SELECT dolt_branch('-d', '{branch}')
