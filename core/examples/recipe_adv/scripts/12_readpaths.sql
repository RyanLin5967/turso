# Read paths other than a plain scan: joins, self-joins, subqueries, CTEs, windows, sorters,
# compound selects, views, triggers, RETURNING on later statements, upsert, INSERT ... SELECT with
# a column list, covering indexes created after a recipe, DISTINCT, GROUP BY, min/max shortcuts.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT, g TEXT)
CREATE TABLE u(id INTEGER PRIMARY KEY, tid INTEGER, v INTEGER)
CREATE TABLE log(id INTEGER PRIMARY KEY, what TEXT, val)
INSERT INTO t(a, b, c, g) VALUES (1, 10, 'p', 'k1'), (2, 20, 'q', 'k1'), (3, 30, 'r', 'k2'), (4, 40, NULL, 'k2'), (5, 50, 's', NULL)
INSERT INTO u(tid, v) VALUES (1, 100), (2, 200), (3, 300), (9, 900)
UPDATE t SET b = a * 1000 + b
UPDATE t SET c = c || ':' || b
UPDATE t SET g = coalesce(g, 'k3') WHERE a > 2
SELECT t.id, t.b, u.v FROM t JOIN u ON u.tid = t.id ORDER BY t.id
SELECT t.id, t.b, u.v FROM u LEFT JOIN t ON u.tid = t.id ORDER BY u.id
SELECT x.id, y.id, x.b, y.b FROM t x JOIN t y ON x.b < y.b AND y.a = x.a + 1 ORDER BY x.id
SELECT id, b, (SELECT max(b) FROM t t2 WHERE t2.g = t.g) FROM t ORDER BY id
SELECT id FROM t WHERE b IN (SELECT b FROM t WHERE a % 2 = 1) ORDER BY id
SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id AND u.v * 10 < t.b) ORDER BY id
WITH w AS (SELECT id, b, c FROM t WHERE b > 2000) SELECT * FROM w ORDER BY b DESC
WITH RECURSIVE r(n, s) AS (SELECT 1, (SELECT b FROM t WHERE id = 1) UNION ALL SELECT n + 1, s + (SELECT b FROM t WHERE id = n + 1) FROM r WHERE n < 5) SELECT * FROM r
SELECT id, b, sum(b) OVER (PARTITION BY g ORDER BY id), row_number() OVER (ORDER BY b DESC), lag(c) OVER (ORDER BY id) FROM t ORDER BY id
SELECT g, count(*), sum(b), group_concat(c, ';'), max(c) FROM t GROUP BY g ORDER BY g
SELECT DISTINCT g FROM t ORDER BY 1
SELECT max(b), min(b), avg(b), total(b) FROM t
SELECT b FROM t UNION SELECT v FROM u ORDER BY 1
SELECT c FROM t EXCEPT SELECT c FROM t WHERE a = 1 ORDER BY 1
SELECT id, b FROM t ORDER BY b DESC LIMIT 2 OFFSET 1
SELECT * FROM t ORDER BY c COLLATE NOCASE, id
CREATE VIEW vt AS SELECT id, b * 2 AS b2, upper(c) AS uc FROM t
SELECT * FROM vt ORDER BY id
CREATE TRIGGER trd AFTER DELETE ON t BEGIN INSERT INTO log(what, val) VALUES ('del-b', old.b); INSERT INTO log(what, val) VALUES ('del-c', old.c); END
CREATE TRIGGER tru AFTER UPDATE ON t BEGIN INSERT INTO log(what, val) VALUES ('upd-old-b', old.b); INSERT INTO log(what, val) VALUES ('upd-new-c', new.c); END
DELETE FROM t WHERE id = 5
UPDATE t SET a = a + 1 WHERE id = 4
SELECT * FROM log ORDER BY id
DELETE FROM t WHERE id = 3 RETURNING id, b, c, g
UPDATE t SET a = a WHERE id = 2 RETURNING id, a, b, c
INSERT INTO t(id, a) VALUES (1, 77) ON CONFLICT(id) DO UPDATE SET a = excluded.a + b RETURNING id, a, b, c
INSERT INTO u(tid, v) SELECT id, b FROM t WHERE id <= 2
SELECT * FROM u ORDER BY id
CREATE INDEX ib ON t(b)
CREATE INDEX igc ON t(g, c)
SELECT b FROM t WHERE b > 1000 ORDER BY b
SELECT g, c FROM t WHERE g = 'k1' ORDER BY c
SELECT count(*) FROM t WHERE c LIKE '%:%'
PRAGMA integrity_check
SELECT * FROM t ORDER BY id
UPDATE t SET a = a + 1
SELECT * FROM t ORDER BY id
SELECT * FROM log ORDER BY id
