# Dolt sql-server M1 (create-to-first-write), variant (a): dolt-a-m1c.spec, then one autocommit UPDATE of a random
# existing row of t on the new branch (the session is on it after DOLT_CHECKOUT('-b')).
# Comp review 1 MED 6: only the create is timed, as the registered statements time it (STATEMENTS.md, variant (a)):
# the session is on main before it -- at the start of the run, or after the previous op's untimed after-step
# -- so the old timed step 1, a checkout of main, was a no-op inside lat_ns.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_CHECKOUT('-b', '{branch}')
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql CALL DOLT_CHECKOUT('main')
after sql-serial CALL DOLT_BRANCH('-d', '{branch}')
