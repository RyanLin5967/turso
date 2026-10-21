# Dolt sql-server M1 (create-to-first-write), variant (b): dolt-b-m1c.spec (DOLT_BRANCH, then PREREG N7's
# USE `bench/name`; comp-rev-89ec-b3c3 MED 1), then one autocommit UPDATE of a random existing row of t on the new
# branch. STATEMENTS.md row dolt b m1-switch.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step sql USE `bench/{branch}`
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency); the switch-back
# comes first, so the delete never runs on the branch it deletes
after sql USE bench
after sql-serial CALL DOLT_BRANCH('-d', '{branch}')
