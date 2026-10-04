# Doltgres M1c, amendment 14 variant (a): dolt_checkout(parent), then dolt_checkout('-b', name) -- the create from
# the parent, which also switches the session to the new branch (M1c-switch). Both statements are timed.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
var branch = b_{run}_{c}_{i}
step sql SELECT dolt_checkout('main')
step sql SELECT dolt_checkout('-b', '{branch}')
