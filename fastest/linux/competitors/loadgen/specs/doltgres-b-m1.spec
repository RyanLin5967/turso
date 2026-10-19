# Doltgres M1 (create-to-first-write), variant (b): doltgres-b-m1c.spec (dolt_branch, then PREREG N7's
# USE "postgres/name"; comp-rev-89ec-b3c3 MED 1), then one autocommit UPDATE of a random existing row of t on the new
# branch. STATEMENTS.md row doltgres b m1-switch.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step sql USE "postgres/{branch}"
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency); the switch-back
# comes first, so the delete never runs on the branch it deletes
after sql USE postgres
after sql SELECT dolt_branch('-d', '{branch}')
