# Doltgres M1c, amendment 14 variant (a): dolt_checkout('-b', name) on a session at the parent -- the create from
# the parent, which also switches the session to the new branch (M1c-switch).
# Comp review 1 MED 6: only the create is timed, as the registered statements time it (STATEMENTS.md, variant (a)):
# the session is on main before it -- at the start of the run, or after the previous op's untimed after-step
# -- so the old timed step 1, a checkout of main, was a no-op inside lat_ns.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_checkout('-b', '{branch}')
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql SELECT dolt_checkout('main')
after sql-serial SELECT dolt_branch('-d', '{branch}')
