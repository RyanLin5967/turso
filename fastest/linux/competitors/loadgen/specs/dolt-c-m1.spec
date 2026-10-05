# Dolt sql-server M1 (create-to-first-write), variant (c): DOLT_BRANCH(name, parent), a new connection to
# 'bench/<name>', then one autocommit UPDATE of a random existing row of t on it.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}', 'main')
step connect db=bench/{branch}
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
