# A column FK with no parent column list, on a column the recipe does not touch.
PRAGMA foreign_keys = ON
CREATE TABLE p(id INTEGER PRIMARY KEY)
INSERT INTO p VALUES (1)
CREATE TABLE t(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p, b INTEGER)
INSERT INTO t(pid, b) VALUES (1, 1)
UPDATE t SET b = b + 100
SELECT sql FROM sqlite_schema WHERE name = 't'
@reopen
PRAGMA foreign_keys = ON
SELECT * FROM t ORDER BY id
INSERT INTO t(pid, b) VALUES (2, 2)
