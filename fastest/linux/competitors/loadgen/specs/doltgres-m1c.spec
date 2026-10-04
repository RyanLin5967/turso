# Doltgres M1c, BranchBench's create: dolt_checkout('-b') creates the branch from the session's current branch AND
# switches the session to it. Fan-out: the untimed after-step switches back to main, so every branch forks main.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_checkout('-b', '{branch}')
after sql SELECT dolt_checkout('main')
