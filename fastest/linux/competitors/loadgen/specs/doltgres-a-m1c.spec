# Doltgres M1c, amendment 14 variant (a): dolt_checkout(parent), then dolt_checkout('-b', name) -- the create from
# the parent, which also switches the session to the new branch (M1c-switch). Both statements are timed.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_checkout('main')
step sql SELECT dolt_checkout('-b', '{branch}')
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql SELECT dolt_checkout('main')
after sql-serial SELECT dolt_branch('-d', '{branch}')
