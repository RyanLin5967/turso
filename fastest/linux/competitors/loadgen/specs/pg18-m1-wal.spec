# PG18 M1 (create-to-first-write), STRATEGY = WAL_LOG (lane fastest-linux-comp): pg18-m1c-wal.spec, then one
# autocommit UPDATE of a random existing row of t on the branch.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = WAL_LOG
step connect dbname={branch}
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after close
after sql DROP DATABASE {branch}
