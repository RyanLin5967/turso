# Dolt sql-server M1 (create-to-first-write), variant (c): DOLT_BRANCH(name, parent), a new connection to
# 'bench/<name>', then one autocommit UPDATE of a random existing row of t on it.
# The registered m1-connect form (STATEMENTS.md) probes the new connection with SELECT 1 before the write; this spec
# omitted it (comp review 1 MED 6's spec lint).
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step connect db=bench/{branch}
step sql SELECT 1
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after close
after sql-serial CALL DOLT_BRANCH('-d', '{branch}')
