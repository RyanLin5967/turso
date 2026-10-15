# The first recipe rewrites the table's sqlite_schema SQL with BTreeTable::to_sql, which drops
# column conflict clauses. Eager never rewrites it. After a reopen (schema parsed from disk) the
# recipe database enforces a different constraint.
CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE ON CONFLICT REPLACE, b INTEGER)
INSERT INTO t(u, b) VALUES ('k', 1), ('m', 2)
UPDATE t SET b = b + 100
SELECT sql FROM sqlite_schema WHERE name = 't'
INSERT INTO t(u, b) VALUES ('k', 9)
SELECT * FROM t ORDER BY id
@reopen
@expect-diff after reopen the conflict clause is gone in the recipe arm
INSERT INTO t(u, b) VALUES ('m', 99)
SELECT * FROM t ORDER BY id
