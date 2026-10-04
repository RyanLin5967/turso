# Dolt M1 (create-to-first-write): M1c, then one autocommit UPDATE of a random existing row of t on the branch.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_CHECKOUT('-b', '{branch}')
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
after sql CALL DOLT_CHECKOUT('main')
