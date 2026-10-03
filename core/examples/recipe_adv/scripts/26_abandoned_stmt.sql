# A statement abandoned after its count row (PRAGMA count_changes): the RecipeInstall has already
# run when the row is returned, the Halt (autocommit) has not.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
PRAGMA count_changes = 1
@prep u UPDATE t SET b = 'abandoned'
@open u
@next u
@drop u
SELECT * FROM t ORDER BY id
SELECT count(*) FROM sqlite_schema WHERE type = 'recipe'
@conn c2
SELECT * FROM t ORDER BY id
@conn main
@prep u2 UPDATE t SET b = 'reset'
@open u2
@next u2
@open u2
SELECT * FROM t ORDER BY id
@conn c2
SELECT * FROM t ORDER BY id
