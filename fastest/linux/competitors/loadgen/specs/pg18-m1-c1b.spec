# PG18 M1 with C1b acknowledgement labels (bbload --c1b-run): the create is acknowledged when step 1 returns, the
# first write when step 3 returns. Phases "create" and "write" for c1bgen.py.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = FILE_COPY
step connect dbname={branch}
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
c1b 1 create:{branch}
c1b 3 write:{branch}
