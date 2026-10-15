# Dolt sql-server M1c-create for variants (b) and (c): DOLT_BRANCH(name, parent) alone, so the switch's and the
# connect's own flushes are the difference to dolt-b-m1c / dolt-c-m1c.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql-serial CALL DOLT_BRANCH('-d', '{branch}')
