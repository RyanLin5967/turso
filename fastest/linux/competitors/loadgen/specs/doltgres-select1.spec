# The harness floor (PREREG §5) on Doltgres.
protocol pg
connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres
step sql SELECT 1
