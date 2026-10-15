# Dolt sql-server M1c, amendment 14 variant (b): DOLT_BRANCH(name, parent), then DOLT_CHECKOUT(name) (M1c-switch).
# The parent is named, so the session's current branch (the previous op's) never matters.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step sql CALL DOLT_CHECKOUT('{branch}')
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql CALL DOLT_CHECKOUT('main')
after sql-serial CALL DOLT_BRANCH('-d', '{branch}')
