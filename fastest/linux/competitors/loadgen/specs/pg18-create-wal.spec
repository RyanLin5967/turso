# PG18 M1c-create, STRATEGY = WAL_LOG (amendment 14's other registered PG variant; lane fastest-linux-comp): the
# template is copied block by block through the buffer manager and WAL-logged, so the create is durable through the
# WAL and the new database's data files are flushed by a LATER checkpoint (the cell's deferred window), not the create.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = WAL_LOG
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql DROP DATABASE {branch}
