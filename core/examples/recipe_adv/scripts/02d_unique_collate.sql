# Table-level UNIQUE (u COLLATE NOCASE): to_sql emits UNIQUE ("u"), losing the collation.
CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT, b INTEGER, UNIQUE (u COLLATE NOCASE))
INSERT INTO t(u, b) VALUES ('abc', 1)
UPDATE t SET b = b + 100
SELECT sql FROM sqlite_schema WHERE name = 't'
@reopen
INSERT INTO t(u, b) VALUES ('ABC', 2)
SELECT * FROM t ORDER BY id
