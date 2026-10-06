# PG18 M1c: create a branch database by FILE_COPY clone of the idle template p, then connect to it
# (PG cannot switch databases on a connection, so the switch is BranchBench's BRANCH_CONNECT: a new connection).
# Linux port (lane fastest-linux-comp): amendment 14's M1c-connect ends when the new connection has completed
# SELECT 1, so that step is added (the Mac copy ended at the connect).
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = FILE_COPY
step connect dbname={branch}
step sql SELECT 1
