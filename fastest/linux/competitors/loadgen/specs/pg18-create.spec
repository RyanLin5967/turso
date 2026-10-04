# PG18 M1c-create (amendment 14): CREATE DATABASE ... TEMPLATE p STRATEGY = FILE_COPY alone, from its first byte to
# its acknowledgement; the server's file_copy_method decides clone or copy (lane fastest-linux-comp).
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = FILE_COPY
