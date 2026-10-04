# PG18 M1c, STRATEGY = WAL_LOG (amendment 14's other registered PG variant; lane fastest-linux-comp): as
# pg18-m1c.spec but the template is copied block by block through the buffer manager and WAL-logged, so the create
# is durable through the WAL and the new database's data files are flushed by a LATER checkpoint, not the create.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = WAL_LOG
step connect dbname={branch}
