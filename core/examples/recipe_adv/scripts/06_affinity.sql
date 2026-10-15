# Affinity and storage-class corners: the recipe applies the target affinity to a register; the
# eager UPDATE applies it and then round-trips the value through the record.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, r REAL, n NUMERIC, s TEXT, d DECIMAL(10,2), v VARCHAR(4), bl BLOB, u)
INSERT INTO t(a) VALUES (1), (2)
UPDATE t SET r = -0.0
UPDATE t SET n = ' 12 '
UPDATE t SET a = '0x10'
UPDATE t SET s = 1.0
UPDATE t SET d = '3.0'
UPDATE t SET v = 2.50
UPDATE t SET bl = '12'
UPDATE t SET u = 1e999
SELECT id, a, typeof(a), r, typeof(r), n, typeof(n), s, typeof(s), d, typeof(d), v, typeof(v), bl, typeof(bl), u, typeof(u) FROM t ORDER BY id
SELECT id, printf('%f', r), 1 / r, CAST(r AS TEXT), r = 0 FROM t ORDER BY id
UPDATE t SET u = 9007199254740993
UPDATE t SET r = 9007199254740993
SELECT id, u, typeof(u), r FROM t ORDER BY id
UPDATE t SET r = 3
UPDATE t SET u = typeof(r) || ':' || r
SELECT id, r, u FROM t ORDER BY id
UPDATE t SET n = 1e20
UPDATE t SET u = typeof(n) || ':' || n
SELECT id, n, u FROM t ORDER BY id
UPDATE t SET n = '1.0e+3'
UPDATE t SET s = n
SELECT id, n, typeof(n), s, typeof(s) FROM t ORDER BY id
UPDATE t SET r = '  7.25e1'
SELECT id, r, typeof(r) FROM t ORDER BY id
UPDATE t SET u = zeroblob(3)
SELECT id, length(u), typeof(u), hex(u) FROM t ORDER BY id
UPDATE t SET s = char(97, 0, 98)
SELECT id, length(s), hex(s) FROM t ORDER BY id
