# Dolt sql-server M1c, amendment 14 variant (b): DOLT_BRANCH(name, parent), then USE `bench/name` (M1c-switch), back
# with USE bench. The switch is PREREG N7's (DECISIONS 2026-10-15T11:04:04Z: the cheapest documented switch, 113.9 us
# against CALL DOLT_CHECKOUT's 206.2 us median p50 on the Mac probe), as STATEMENTS.md row dolt b m1c-switch carries
# it (comp-rev-89ec-b3c3 MED 1: this spec had timed CALL DOLT_CHECKOUT). The parent is named, so the session's current
# branch never matters.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step sql USE `bench/{branch}`
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency); the switch-back
# comes first, so the delete never runs on the branch it deletes
after sql USE bench
after sql-serial CALL DOLT_BRANCH('-d', '{branch}')
