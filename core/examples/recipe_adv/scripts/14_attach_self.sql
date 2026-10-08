# ATTACH the database's own file under a second name: the attached schema is parsed from disk
# inside the live process, with no reopen.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
UPDATE t SET b = 'upd' || a
SELECT * FROM t ORDER BY id
ATTACH '$SELF' AS o
SELECT * FROM o.t ORDER BY id
