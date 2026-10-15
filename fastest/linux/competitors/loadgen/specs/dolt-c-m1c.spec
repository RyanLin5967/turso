# Dolt sql-server M1c, amendment 14 variant (c): DOLT_BRANCH(name, parent), then a NEW connection to the revision
# database 'bench/<name>' completing SELECT 1 (M1c-connect). The home connection stays on main.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step connect db=bench/{branch}
step sql SELECT 1
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after close
after sql-serial CALL DOLT_BRANCH('-d', '{branch}')
