# DROP TABLE of a recipe table, then a new table of the same name.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x')
UPDATE t SET b = 'old'
DROP TABLE t
SELECT type, name, tbl_name FROM sqlite_schema ORDER BY name
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (7, 'n')
SELECT * FROM t
UPDATE t SET b = 'new'
SELECT type, name, tbl_name FROM sqlite_schema ORDER BY name
SELECT * FROM t
@conn c2
SELECT * FROM t
