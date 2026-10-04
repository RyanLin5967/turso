# Dolt M1c, BranchBench's create: DOLT_CHECKOUT('-b') creates from the session's branch and switches to it;
# the untimed after-step returns to main (fan-out).
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_CHECKOUT('-b', '{branch}')
after sql CALL DOLT_CHECKOUT('main')
