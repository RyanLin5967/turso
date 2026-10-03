# Sanity: a recipe installs and reads agree.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, NULL)
UPDATE t SET b = a * 10
SELECT * FROM t ORDER BY id
SELECT __turso_gen FROM t
SELECT type, name, sql FROM sqlite_schema
# Negative control: a statement whose result legitimately differs must print DIFF.
SELECT count(*) FROM sqlite_schema
