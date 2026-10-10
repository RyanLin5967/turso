# k1-adv2. Cursor-opening paths other than OpenRead/OpenWrite that can be typed as the recipe
# table: materialized CTEs referenced twice (OpenDup), automatic indexes (OpenAutoindex), the
# self-INSERT buffer, two-pass UPDATEs (rowid change, subquery on the target), window sorters,
# IN-subquery ephemeral indexes, recursive CTEs, coroutines, and a trigger reading the table.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)
CREATE TABLE u(tid INTEGER, w INTEGER)
INSERT INTO t(a, b, c) VALUES (1, 'x', 0), (2, 'y', 0), (3, 'z', 0), (4, 'w', 0)
INSERT INTO u VALUES (1, 10), (2, 20), (3, 30), (3, 31)
UPDATE t SET b = 'r' || a
UPDATE t SET c = a * a
WITH m AS MATERIALIZED (SELECT * FROM t WHERE c > 1) SELECT x.id, y.b, x.c FROM m AS x JOIN m AS y ON y.id = x.id ORDER BY x.id
WITH m AS (SELECT id, b, c FROM t) SELECT (SELECT count(*) FROM m), (SELECT max(b) FROM m), (SELECT sum(c) FROM m)
SELECT t.id, t.b, u.w FROM u JOIN t ON t.c = u.tid * u.tid ORDER BY u.w
SELECT t.id, t.b FROM t WHERE t.c IN (SELECT tid * tid FROM u) ORDER BY t.id
SELECT u.w, (SELECT b FROM t WHERE t.c = u.tid * u.tid) FROM u ORDER BY u.w
INSERT INTO t(a, b, c) SELECT a + 100, b || '+', c FROM t WHERE a < 3
SELECT * FROM t ORDER BY id
UPDATE t SET id = id + 1000 WHERE c < 5
SELECT * FROM t ORDER BY id
UPDATE t SET a = a + 1 WHERE b IN (SELECT b FROM t WHERE c > 4)
SELECT * FROM t ORDER BY id
UPDATE t SET a = (SELECT max(c) FROM t AS z WHERE z.id < t.id)
SELECT * FROM t ORDER BY id
SELECT id, b, c, sum(c) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), first_value(b) OVER (PARTITION BY c > 4 ORDER BY id) FROM t ORDER BY id
WITH RECURSIVE r(i, s) AS (SELECT min(id), (SELECT b FROM t ORDER BY id LIMIT 1) FROM t UNION ALL SELECT i + 1, s || (SELECT coalesce(max(b), '') FROM t WHERE id = i + 1) FROM r WHERE i < 1010) SELECT max(i), max(length(s)) FROM r
SELECT b FROM t INTERSECT SELECT b FROM t WHERE c > 0 ORDER BY 1
SELECT DISTINCT c % 2, b FROM t ORDER BY 1, 2
SELECT * FROM (SELECT id, b FROM t ORDER BY c DESC LIMIT 3) ORDER BY id
UPDATE t SET b = b || '#'
CREATE TABLE log(v)
CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO log SELECT b || ':' || c FROM t WHERE c > new.w; END
INSERT INTO u VALUES (0, 5)
SELECT * FROM log ORDER BY v
DELETE FROM t WHERE c IN (SELECT c FROM t ORDER BY c LIMIT 1)
SELECT * FROM t ORDER BY id
