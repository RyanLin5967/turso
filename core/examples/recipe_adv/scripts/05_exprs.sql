# Exotic SET expressions: the recipe stores Display(expr) and re-parses it; the round-trip check
# compares targets only. Each UPDATE targets its own column so each installs on its own.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, r REAL, n NUMERIC, x, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30)
INSERT INTO t(a, b, r, n, x) VALUES (1, 'Abc', 1.5, '12', x'00ff'), (-3, 'x%y', -0.5, 'abc', NULL), (NULL, NULL, NULL, NULL, 'q')
UPDATE t SET c1 = - -a
UPDATE t SET c2 = a - -1
UPDATE t SET c3 = NOT NOT a
UPDATE t SET c4 = 'it''s' || b
UPDATE t SET c5 = x'41' || b
UPDATE t SET c6 = b COLLATE NOCASE = 'abc'
UPDATE t SET c7 = a IS NOT NULL
UPDATE t SET c8 = a NOTNULL
UPDATE t SET c9 = a IS NOT DISTINCT FROM 1
UPDATE t SET c10 = a NOT BETWEEN 0 AND 2
UPDATE t SET c11 = a NOT IN (1, 2)
UPDATE t SET c12 = b LIKE 'x!%%' ESCAPE '!'
UPDATE t SET c13 = b NOT GLOB 'A*'
UPDATE t SET c14 = CASE a WHEN 1 THEN 'one' WHEN -3 THEN 'mthree' END
UPDATE t SET c15 = ~a + (a << 2) - (a >> 1) + (a & 3) + (a | 4) + (a % 3)
UPDATE t SET c16 = CAST(n AS INTEGER)
UPDATE t SET c17 = TRUE + FALSE
UPDATE t SET c18 = 9223372036854775807 + a
UPDATE t SET c19 = -9223372036854775808
UPDATE t SET c20 = 1e3 + .5 + 0x10
UPDATE t SET c21 = r * -0.0
UPDATE t SET c22 = "a" + [a] + `a`
UPDATE t SET c23 = printf('%5.2f|%s', r, b)
UPDATE t SET c24 = iif(a > 0, 'pos', 'neg')
UPDATE t SET c25 = '{"k":[1,2]}' -> '$.k'
UPDATE t SET c26 = '{"k":[1,2]}' ->> '$.k[1]'
UPDATE t SET c27 = a IN ()
UPDATE t SET c28 = b COLLATE RTRIM = 'Abc   '
UPDATE t SET c29 = typeof(x) || hex(x) || quote(x)
UPDATE t SET c30 = - 9223372036854775808
SELECT * FROM t ORDER BY id
SELECT type, sql FROM sqlite_schema WHERE type = 'recipe'
