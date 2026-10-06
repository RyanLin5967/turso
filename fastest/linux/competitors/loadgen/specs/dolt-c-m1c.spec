# Dolt sql-server M1c, amendment 14 variant (c): DOLT_BRANCH(name, parent), then a NEW connection to the revision
# database 'bench/<name>' completing SELECT 1 (M1c-connect). The home connection stays on main.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step connect db=bench/{branch}
step sql SELECT 1
