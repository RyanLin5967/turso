# INTEGER PRIMARY KEY DESC is not a rowid alias (SQLite quirk); to_sql drops DESC.
CREATE TABLE t(id INTEGER PRIMARY KEY DESC, b INTEGER)
INSERT INTO t VALUES (10, 1), (20, 2)
UPDATE t SET b = b + 100
SELECT sql FROM sqlite_schema WHERE name = 't'
SELECT rowid, id, b FROM t ORDER BY id
@reopen
SELECT rowid, id, b FROM t ORDER BY id
INSERT INTO t VALUES ('text-key', 3)
SELECT rowid, id, b FROM t ORDER BY b
