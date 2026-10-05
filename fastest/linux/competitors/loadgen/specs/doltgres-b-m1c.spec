# Doltgres M1c, amendment 14 variant (b): dolt_branch(name, parent), then dolt_checkout(name) (M1c-switch). The
# parent is named, so the session's current branch (the previous op's) never matters.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_branch('{branch}', 'main')
step sql SELECT dolt_checkout('{branch}')
