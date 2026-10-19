set -u
P=/var/tmp/r226lin-wire/pg18/inst/bin; W=/var/tmp/r226lin-wire-copy; D=$W/pgdata; PORT=55931
mkdir -p $W
rm -rf $D; timeout 120 $P/initdb -D $D -U postgres -A trust > $W/initdb.txt 2>&1 || { echo initdb failed; tail $W/initdb.txt; exit 1; }
timeout 60 $P/pg_ctl -D $D -o "-p $PORT -k $W -c listen_addresses=127.0.0.1" -l $W/pg.log -w start > /dev/null || { echo start failed; exit 1; }
$P/postgres --version
timeout 120 python3 $W/copyrec.py 127.0.0.1 $PORT; echo rc=$?
# A role without pg_read_server_files: the file grant against a missing table (the check order).
timeout 30 $P/psql -h 127.0.0.1 -p $PORT -U postgres -qAt -c "CREATE ROLE nofiles LOGIN" postgres
for sql in "COPY nosuch FROM '/x'" "COPY cs (zz) FROM '/x'" "COPY cs FROM '/x' (NOSUCH 1)"; do
  printf '%-14s %s -> ' nofiles "$sql"
  timeout 30 $P/psql -h 127.0.0.1 -p $PORT -U nofiles -qAt -v VERBOSITY=verbose -c "$sql" postgres 2>&1 | head -1
done
timeout 60 $P/pg_ctl -D $D -m fast stop > /dev/null; rm -rf $D
