# NULL, three-valued WHERE, affinity of comparisons against recipe outputs, collated targets, and a
# chain of recipes whose WHEREs read columns that later recipes change.
CREATE TABLE t(id INTEGER PRIMARY KEY, i INTEGER, r REAL, s TEXT, n NUMERIC, u, cs TEXT COLLATE NOCASE)
INSERT INTO t(i, r, s, n, u, cs) VALUES (NULL, NULL, NULL, NULL, NULL, 'Abc'), (1, 1.0, '1', '1', 1, 'abc'), (2, 2.5, '02', ' 2 ', '2', 'ABD'), (3, NULL, 'x', 'x', x'00', NULL)
UPDATE t SET u = i * 2 WHERE r IS NULL OR i > 1
UPDATE t SET s = u WHERE NOT (u = 2)
UPDATE t SET i = NULL WHERE s > 5
UPDATE t SET r = s WHERE i IS NULL
UPDATE t SET n = r + 0 WHERE NOT (n IN (1, 2))
UPDATE t SET cs = 'ABC' WHERE s IS NOT NULL
UPDATE t SET u = NULLIF(u, 6)
SELECT id, i, typeof(i), r, typeof(r), s, typeof(s), n, typeof(n), u, typeof(u), cs FROM t ORDER BY id
SELECT id FROM t WHERE cs = 'abc' ORDER BY id
SELECT id FROM t WHERE s = 6 ORDER BY id
SELECT id FROM t WHERE s > 5 ORDER BY id
SELECT id FROM t WHERE r = '1' ORDER BY id
SELECT cs, count(*) FROM t GROUP BY cs ORDER BY cs
SELECT DISTINCT cs FROM t ORDER BY cs
SELECT id, i IS NULL, s IS NULL, r IS NULL FROM t ORDER BY id
