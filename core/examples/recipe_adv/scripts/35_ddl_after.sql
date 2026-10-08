# k1-adv2. After recipes: ANALYZE (before and after indexes on targets), a UNIQUE index on a
# target, integrity_check / quick_check, INSERT ... SELECT * into another table, views over the
# recipe table, an eager UPDATE of an indexed target, ANALYZE of one table.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)
CREATE TABLE dst(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)
INSERT INTO t(a, b, c) VALUES (1, 'x', 1), (2, 'y', 1), (3, 'z', 2), (4, 'x', 2)
CREATE INDEX ia ON t(a)
UPDATE t SET b = 'k' || a
UPDATE t SET c = a % 2
ANALYZE
SELECT * FROM sqlite_stat1 ORDER BY tbl, idx
CREATE UNIQUE INDEX ub ON t(b)
CREATE INDEX ic ON t(c)
ANALYZE
SELECT * FROM sqlite_stat1 ORDER BY tbl, idx
SELECT b FROM t WHERE b = 'k3'
SELECT id FROM t WHERE c = 1 ORDER BY id
INSERT INTO t(a, b) VALUES (9, 'k1')
PRAGMA integrity_check
PRAGMA quick_check
INSERT INTO dst SELECT * FROM t
INSERT INTO dst(a, b, c) SELECT a, b, c FROM t WHERE c = 0
SELECT * FROM dst ORDER BY id
CREATE VIEW v AS SELECT b, c, a * 2 AS a2 FROM t WHERE c = 1
SELECT * FROM v ORDER BY b
UPDATE t SET c = c + 10
SELECT * FROM v ORDER BY b
SELECT * FROM t ORDER BY id
ANALYZE t
SELECT * FROM sqlite_stat1 ORDER BY tbl, idx
SELECT * FROM t INDEXED BY ic WHERE c > 0 ORDER BY id
SELECT * FROM t NOT INDEXED WHERE b = 'k2'
