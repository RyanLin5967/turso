# PG18 M1c-create, STRATEGY = WAL_LOG (amendment 14's other registered PG variant; lane fastest-linux-comp): the
# template is copied block by block through the buffer manager and WAL-logged, so the create is durable through the
# WAL and the new database's data files are flushed by a LATER checkpoint (the cell's deferred window), not the create.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = WAL_LOG
