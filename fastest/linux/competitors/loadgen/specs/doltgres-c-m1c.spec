# Doltgres M1c, amendment 14 variant (c): dolt_branch(name, parent), then a NEW connection to the revision database
# 'postgres/<name>' completing SELECT 1 (M1c-connect). The home connection stays on main.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step connect dbname=postgres/{branch}
step sql SELECT 1
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after close
after sql-serial SELECT dolt_branch('-d', '{branch}')
