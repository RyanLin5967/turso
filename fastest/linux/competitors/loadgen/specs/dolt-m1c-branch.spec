# Dolt M1c, DOLT_BRANCH variant: create with DOLT_BRANCH(name), then switch with DOLT_CHECKOUT.
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
var branch = b_{run}_{c}_{i}
step sql CALL DOLT_BRANCH('{branch}')
step sql CALL DOLT_CHECKOUT('{branch}')
after sql CALL DOLT_CHECKOUT('main')
