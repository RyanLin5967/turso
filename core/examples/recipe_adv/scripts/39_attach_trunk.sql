# k1-adv2. Tables created on the TRUNK before the branch forks, so the same root page is reachable
# twice from one branch statement: as main.t (the branch, with recipes) and, through ATTACH of the
# database file, as o.t (the trunk's copy, no recipes). Joins and copies between the two.
@trunk CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
@trunk INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')
@newmain
SELECT * FROM t ORDER BY id
UPDATE t SET b = 'br' || a
SELECT * FROM t ORDER BY id
ATTACH '$SELF' AS o
SELECT * FROM o.t ORDER BY id
SELECT m.id, m.b, x.b FROM main.t AS m JOIN o.t AS x ON x.id = m.id ORDER BY m.id
SELECT x.id, x.b, m.b FROM o.t AS x JOIN main.t AS m ON m.id = x.id ORDER BY x.id
SELECT id, b FROM main.t UNION SELECT id, b FROM o.t ORDER BY 1, 2
SELECT id, b FROM main.t EXCEPT SELECT id, b FROM o.t ORDER BY 1
SELECT (SELECT b FROM o.t WHERE id = m.id), b FROM main.t AS m ORDER BY id
CREATE TEMP TABLE tt AS SELECT * FROM main.t
SELECT * FROM tt ORDER BY id
INSERT INTO main.t(a, b) SELECT a + 10, b FROM o.t
SELECT * FROM main.t ORDER BY id
UPDATE main.t SET a = (SELECT a FROM o.t WHERE o.t.id = main.t.id) WHERE id <= 3
SELECT * FROM main.t ORDER BY id
DETACH o
SELECT * FROM t ORDER BY id
@trunk SELECT * FROM t ORDER BY id
