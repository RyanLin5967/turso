# PG18 M1 holding N fixed: as pg18-m1.spec, then (untimed) close the branch connection and DROP the branch.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = FILE_COPY
step connect dbname={branch}
step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}
after close
after sql DROP DATABASE {branch}
