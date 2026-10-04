# Doltgres M1 (create-to-first-write), variant (c): dolt_branch(name, parent), a new connection to
# 'postgres/<name>', then one autocommit UPDATE of a random existing row of t on it.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step connect dbname=postgres/{branch}
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
