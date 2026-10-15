# Cap plant (lead review 62430d8bf..b49fb656a MED 5): an op of about 5 ms, so a 1 s capped window holds 100-999 ops
# (the p50-only tier). Never a measured cell.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
step sql SELECT pg_sleep(0.005)
