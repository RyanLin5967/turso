# PG18 M1c-create (amendment 14): CREATE DATABASE ... TEMPLATE p STRATEGY = FILE_COPY alone, from its first byte to
# its acknowledgement; the server's file_copy_method decides clone or copy (lane fastest-linux-comp).
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = FILE_COPY
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql DROP DATABASE {branch}
