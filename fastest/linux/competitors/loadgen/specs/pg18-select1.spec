# The harness floor (PREREG §5): one SELECT 1 round trip on the home connection.
protocol pg
connect host=127.0.0.1 port={port} user=postgres dbname=postgres
step sql SELECT 1
