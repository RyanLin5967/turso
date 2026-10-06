# Dolt sql-server M1c-create for variants (b) and (c): DOLT_BRANCH(name, parent) alone, so the switch's and the
# connect's own flushes are the difference to dolt-b-m1c / dolt-c-m1c.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
