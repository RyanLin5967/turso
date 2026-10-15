# Doltgres M1 (create-to-first-write), variant (c): dolt_branch(name, parent), a new connection to
# 'postgres/<name>', then one autocommit UPDATE of a random existing row of t on it.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step connect dbname=postgres/{branch}
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after close
after sql-serial SELECT dolt_branch('-d', '{branch}')
