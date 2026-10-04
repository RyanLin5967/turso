# Doltgres M1 (create-to-first-write): M1c, then one autocommit UPDATE of a random existing row of t on the branch.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_checkout('-b', '{branch}')
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
after sql SELECT dolt_checkout('main')
