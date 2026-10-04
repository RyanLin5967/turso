# Doltgres M1c, DOLT_BRANCH variant (PREREG §6 C1): create with dolt_branch(name), then switch with dolt_checkout.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}')
step sql SELECT dolt_checkout('{branch}')
after sql SELECT dolt_checkout('main')
