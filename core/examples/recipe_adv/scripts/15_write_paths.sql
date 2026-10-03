# Write paths after a recipe: DEFAULT VALUES, REPLACE, OR IGNORE, DO NOTHING, rowid change,
# an eager UPDATE through an index created after the recipe, a recipe reading the rowid alias.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER DEFAULT 5, b INTEGER, c TEXT DEFAULT 'd')
INSERT INTO t(a, b, c) VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 3, 'z')
UPDATE t SET b = id * 100 + a
UPDATE t SET c = c || b
INSERT INTO t DEFAULT VALUES
SELECT * FROM t ORDER BY id
REPLACE INTO t(id, a, b) VALUES (2, 20, NULL)
INSERT OR IGNORE INTO t(id, a) VALUES (1, 99)
INSERT INTO t(id, a) VALUES (3, 99) ON CONFLICT DO NOTHING
SELECT * FROM t ORDER BY id
UPDATE t SET id = id + 100 WHERE a > 0
SELECT * FROM t ORDER BY id
CREATE INDEX ib ON t(b)
UPDATE t SET b = b + 1 WHERE b > 0
SELECT * FROM t ORDER BY id
UPDATE t SET c = 'all'
UPDATE t SET b = b * 2 WHERE b IS NOT NULL
SELECT * FROM t ORDER BY id
SELECT b, id FROM t WHERE b IS NOT NULL ORDER BY b
PRAGMA integrity_check
