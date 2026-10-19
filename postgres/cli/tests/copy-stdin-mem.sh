set -u
# mem.sh BIN "MODE MIB ROWBYTES"...: each case on a fresh server and database.
BIN=$1; shift
for case in "$@"; do
  rm -rf m.db*; ($BIN m.db --server 127.0.0.1:${PORT:-56961} > srv.out 2> srv.err &); sleep 3
  TP=$(pgrep -u $USER -f "$BIN m.db --server" | head -1)
  timeout 3600 python3 copy-stdin-mem.py ${PORT:-56961} $TP $case; echo "rc=$?"
  kill $TP; sleep 2
done
rm -rf m.db*
