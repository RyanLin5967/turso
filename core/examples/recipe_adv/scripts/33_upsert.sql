# k1-adv2. UPSERT after recipes: excluded.* next to governed columns of the existing row, DO
# UPDATE WHERE on a governed column, multi-row INSERT ... SELECT upserts, RETURNING.
CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE, a INTEGER, b TEXT, c TEXT)
INSERT INTO t(k, a, b, c) VALUES ('p', 1, 'x', 'c1'), ('q', 2, 'y', 'c2'), ('r', 3, 'z', 'c3')
UPDATE t SET b = 'r' || a
UPDATE t SET c = b || '|' || c
INSERT INTO t(k, a) VALUES ('p', 100) ON CONFLICT(k) DO UPDATE SET a = excluded.a + a, c = excluded.c
INSERT INTO t(k, a, b) VALUES ('q', 200, 'newb') ON CONFLICT(k) DO UPDATE SET c = excluded.b || b || c
INSERT INTO t(id, k, a) VALUES (3, 'zz', 300) ON CONFLICT(id) DO UPDATE SET k = excluded.k, b = excluded.b WHERE b = 'r3'
INSERT INTO t(k, a) VALUES ('p', 1) ON CONFLICT(k) DO NOTHING
INSERT INTO t(k, a, b, c) VALUES ('p', 7, 'B', 'C') ON CONFLICT DO UPDATE SET (a, b) = (excluded.a, excluded.b)
SELECT * FROM t ORDER BY id
INSERT INTO t(k, a) VALUES ('q', 5) ON CONFLICT(k) DO UPDATE SET a = excluded.a RETURNING id, k, a, b, c
REPLACE INTO t(id, k, a) VALUES (2, 'q2', 9)
SELECT * FROM t ORDER BY id
UPDATE t SET b = coalesce(b, 'nb')
INSERT INTO t(k, a) SELECT k || 'x', a FROM t WHERE true ON CONFLICT(k) DO UPDATE SET b = excluded.b
SELECT * FROM t ORDER BY id
INSERT INTO t(k, a) SELECT k, a + 1 FROM t WHERE true ON CONFLICT(k) DO UPDATE SET a = excluded.a, c = c || '!'
SELECT * FROM t ORDER BY id
UPDATE t SET c = 'reset'
INSERT INTO t(k, a, c) VALUES ('p', 0, 'ex') ON CONFLICT(k) DO UPDATE SET c = excluded.c || c, b = b || excluded.b
SELECT * FROM t ORDER BY id
PRAGMA integrity_check
