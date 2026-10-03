# Indexes created after a recipe (expression, partial, covering), REINDEX, ADD COLUMN after a
# recipe, AUTOINCREMENT, a SET b = DEFAULT, and a user column that is named __turso_gen.
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, a INTEGER, b INTEGER, c TEXT)
INSERT INTO t(a, b, c) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')
UPDATE t SET b = b + 1000
UPDATE t SET c = c || b
CREATE INDEX ie ON t(b + 1)
CREATE INDEX ip ON t(a) WHERE b > 1015
CREATE INDEX icov ON t(c, b)
SELECT id FROM t WHERE b + 1 = 1011
SELECT a FROM t WHERE b > 1015 ORDER BY a
SELECT c, b FROM t WHERE c > 'x' ORDER BY c
UPDATE t SET b = 5 WHERE id = 2
SELECT a FROM t WHERE b > 1015 ORDER BY a
SELECT id FROM t WHERE b + 1 = 6
REINDEX
PRAGMA integrity_check
ALTER TABLE t ADD COLUMN x INTEGER DEFAULT 7
SELECT * FROM t ORDER BY id
INSERT INTO t(a, b, c) VALUES (4, 40, 'w')
DELETE FROM t WHERE id = 4
INSERT INTO t(a, b, c) VALUES (5, 50, 'v')
SELECT * FROM t ORDER BY id
SELECT name, seq FROM sqlite_sequence
CREATE TABLE d(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER DEFAULT 9)
INSERT INTO d(a, b) VALUES (1, 1), (2, 2)
UPDATE d SET b = DEFAULT
SELECT * FROM d ORDER BY id
CREATE TABLE g(id INTEGER PRIMARY KEY, a INTEGER, __turso_gen INTEGER)
INSERT INTO g(a, __turso_gen) VALUES (1, 100), (2, 200)
UPDATE g SET a = a + 1
SELECT id, a, __turso_gen FROM g ORDER BY id
SELECT * FROM g ORDER BY id
INSERT INTO g(a, __turso_gen) VALUES (3, 300)
SELECT * FROM g ORDER BY id
UPDATE g SET a = a * 10
SELECT * FROM g ORDER BY id
