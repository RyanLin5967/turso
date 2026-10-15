# The harness floor (PREREG §5) on Dolt sql-server (MySQL protocol).
protocol mysql
connect host=127.0.0.1 port={port} user=root db=bench
step sql SELECT 1
