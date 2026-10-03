# auto_vacuum = FULL: dropping an earlier table relocates later root pages (sqlite_schema rootpage
# is rewritten). The recipe table's in-memory root, and its recipes, must follow.
PRAGMA auto_vacuum = FULL
CREATE TABLE a0(x)
INSERT INTO a0 VALUES (1)
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
UPDATE t SET b = 'r' || a
SELECT name, rootpage FROM sqlite_schema WHERE type = 'table' ORDER BY name
DROP TABLE a0
SELECT name, rootpage FROM sqlite_schema WHERE type = 'table' ORDER BY name
SELECT * FROM t ORDER BY id
UPDATE t SET a = a + 1 WHERE id = 1
SELECT * FROM t ORDER BY id
@conn c2
SELECT * FROM t ORDER BY id
