# Doltgres M1 (create-to-first-write), variant (b): doltgres-b-m1c.spec, then one autocommit UPDATE of a random
# existing row of t on the new branch.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step sql SELECT dolt_checkout('{branch}')
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
