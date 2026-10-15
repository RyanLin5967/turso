# Miscellany: connection counters, schema_version, overflow-sized values, invalid UTF-8 text,
# deferred seeks through an index on a recipe input, a user index whose name collides with a
# recipe row's name, and autovacuum root relocation.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c BLOB)
CREATE INDEX ia ON t(a)
INSERT INTO t(a, b, c) VALUES (1, 'x', x'00'), (2, 'y', x'01'), (3, 'z', x'02')
SELECT last_insert_rowid(), total_changes()
PRAGMA schema_version
UPDATE t SET b = 'r' || a
SELECT last_insert_rowid(), total_changes(), changes()
PRAGMA schema_version
SELECT b FROM t WHERE a = 2
SELECT id, b FROM t WHERE a > 1 ORDER BY a DESC
UPDATE t SET c = zeroblob(20000)
UPDATE t SET b = b || hex(substr(c, 19990, 4)) || length(c)
SELECT id, b, length(c) FROM t ORDER BY id
UPDATE t SET b = CAST(x'ff00fe41' AS TEXT)
SELECT id, hex(b), length(b), typeof(b) FROM t ORDER BY id
UPDATE t SET c = CAST(x'c3' AS TEXT) || 'a'
SELECT id, hex(c), length(c), typeof(c) FROM t ORDER BY id
CREATE INDEX __turso_recipe_t_9 ON t(a, id)
UPDATE t SET b = 'nine'
SELECT type, name FROM sqlite_schema WHERE name LIKE '__turso_recipe%' ORDER BY type, name
DROP INDEX __turso_recipe_t_9
SELECT type, name FROM sqlite_schema WHERE name LIKE '__turso_recipe%' ORDER BY type, name
SELECT id, b FROM t ORDER BY id
@conn c2
SELECT id, b FROM t ORDER BY id
