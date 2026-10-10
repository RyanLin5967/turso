# k1-adv2. Durable reopen of shapes the faithful-rewrite allowlist admits: AUTOINCREMENT, a
# column-level UNIQUE before a later column-level PRIMARY KEY, NOT NULL / DEFAULT columns that are
# not targets, a view and ANALYZE stats over the table, ADD COLUMN after the first recipe.
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, a INTEGER NOT NULL DEFAULT 0, b TEXT DEFAULT 'd', c REAL)
INSERT INTO t(a, b, c) VALUES (1, 'x', 1.0), (2, 'y', 2.0), (3, 'z', 3.0)
DELETE FROM t WHERE id = 3
UPDATE t SET c = c * 1.5
CREATE VIEW v AS SELECT id, b, c * 2 AS c2 FROM t
CREATE INDEX ia ON t(a)
ANALYZE
ALTER TABLE t ADD COLUMN e TEXT DEFAULT 'e0'
UPDATE t SET e = e || b
@reopen
SELECT * FROM t ORDER BY id
SELECT * FROM v ORDER BY id
INSERT INTO t(a) VALUES (9)
SELECT * FROM t ORDER BY id
SELECT name, seq FROM sqlite_sequence
SELECT * FROM sqlite_stat1 ORDER BY tbl, idx
PRAGMA integrity_check
CREATE TABLE w(a TEXT UNIQUE, k TEXT PRIMARY KEY, n INTEGER)
INSERT INTO w VALUES ('a1', 'k1', 1), ('a2', 'k2', 2)
UPDATE w SET n = n + 1
@reopen
SELECT * FROM w WHERE k = 'k2'
SELECT * FROM w WHERE a = 'a1'
PRAGMA integrity_check
INSERT INTO w VALUES ('a1', 'k9', 0)
INSERT INTO w VALUES ('a9', 'k1', 0)
SELECT * FROM w ORDER BY rowid
