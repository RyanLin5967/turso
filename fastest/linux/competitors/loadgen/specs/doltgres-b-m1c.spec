# Doltgres M1c, amendment 14 variant (b): dolt_branch(name, parent), then USE "postgres/name" (M1c-switch), back with
# USE postgres. The switch is PREREG N7's (DECISIONS 2026-10-15T11:32:50Z: 136.1 us against SELECT dolt_checkout's
# 519.7 us median p50 on the Mac probe), as STATEMENTS.md row doltgres b m1c-switch carries it (comp-rev-89ec-b3c3
# MED 1: this spec had timed SELECT dolt_checkout). The parent is named, so the session's current branch never matters.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step sql USE "postgres/{branch}"
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency); the switch-back
# comes first, so the delete never runs on the branch it deletes
after sql USE postgres
after sql SELECT dolt_branch('-d', '{branch}')
