# PG18 M1c-create with file_copy_method = copy for this session only (SET on the home connection, untimed): the
# NEGATIVE CONTROL of the clone proof -- its branch must share no block with the template, and its window must hold
# no copy_file_range. PG's own default is copy; the server under test is configured for clone (lane
# fastest-linux-comp). A server that refuses the SET fails the cell's setup, loudly.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
setup SET file_copy_method = copy
var branch = b_{run}_{c}_{i}
step sql CREATE DATABASE {branch} TEMPLATE p STRATEGY = FILE_COPY
# HIGH 1 (lead review 62430d8bf..b49fb656a): N is held fixed -- every op, warm-up included, is followed by an
# untimed durable delete of its branch (after-steps: recorded as after_ns, outside the op's latency)
after sql DROP DATABASE {branch}
