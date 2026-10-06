#!/opt/homebrew/bin/python3 -B
"""gen_seed.py ROWS > seed.sql -- the same parent table for every system (PG, Doltgres, Dolt):
t(id INT PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL), ids 1..ROWS, v = 0, a deterministic 100-char pad.
Plain SQL accepted by PostgreSQL, Doltgres and Dolt (MySQL dialect); INSERTs in batches of 1000 rows."""
import sys

rows = int(sys.argv[1])
if rows < 1:
    sys.exit("gen_seed.py: ROWS must be >= 1")
out = sys.stdout
out.write("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, pad VARCHAR(120) NOT NULL);\n")
x = 2463534242
for start in range(1, rows + 1, 1000):
    vals = []
    for i in range(start, min(start + 1000, rows + 1)):
        chars = []
        for _ in range(100):
            x ^= (x << 13) & 0xFFFFFFFF
            x ^= x >> 17
            x ^= (x << 5) & 0xFFFFFFFF
            chars.append("abcdefghijklmnopqrstuvwxyz0123456789"[x % 36])
        vals.append("(%d,0,'%s')" % (i, "".join(chars)))
    out.write("INSERT INTO t (id, v, pad) VALUES " + ",".join(vals) + ";\n")
