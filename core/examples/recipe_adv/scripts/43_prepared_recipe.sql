# k1-adv2. One prepared recipe UPDATE executed repeatedly (each run is a schema change, so the
# compiled generation must not be reused), a prepared recipe across ROLLBACK, a multi-target swap,
# and two recipe UPDATEs prepared before either ran.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c TEXT)
INSERT INTO t(a, b, c) VALUES (1, 'x', 'p'), (2, 'y', 'q')
@prep u UPDATE t SET b = b || '+'
@bind u
@bind u
@bind u
SELECT * FROM t ORDER BY id
BEGIN
@bind u
ROLLBACK
@bind u
SELECT * FROM t ORDER BY id
SELECT count(*) FROM sqlite_schema WHERE type = 'recipe'
UPDATE t SET b = c, c = b
SELECT * FROM t ORDER BY id
UPDATE t SET b = c, c = b WHERE a = 1
SELECT * FROM t ORDER BY id
@prep p1 UPDATE t SET a = a * 2
@prep p2 UPDATE t SET b = a || b
@bind p2
@bind p1
@bind p2
@bind p1
SELECT * FROM t ORDER BY id
@prep p3 UPDATE t SET c = c || a WHERE b LIKE '%+%'
BEGIN
@bind p3
SAVEPOINT s
@bind p3
ROLLBACK TO s
@bind p3
COMMIT
SELECT * FROM t ORDER BY id
@reopen
SELECT * FROM t ORDER BY id
