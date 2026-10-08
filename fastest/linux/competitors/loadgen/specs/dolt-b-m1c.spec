# Dolt sql-server M1c, amendment 14 variant (b): DOLT_BRANCH(name, parent), then DOLT_CHECKOUT(name) (M1c-switch).
# The parent is named, so the session's current branch (the previous op's) never matters.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step sql CALL DOLT_CHECKOUT('{branch}')
