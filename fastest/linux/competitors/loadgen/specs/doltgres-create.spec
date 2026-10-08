# Doltgres M1c-create for variants (b) and (c): dolt_branch(name, parent) alone, so the switch's and the connect's
# own flushes are the difference to doltgres-b-m1c / doltgres-c-m1c.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
