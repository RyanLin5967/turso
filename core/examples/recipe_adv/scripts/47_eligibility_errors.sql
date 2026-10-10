# k1-adv2. Bulk UPDATEs the eager path rejects at prepare or run time, run where the recipe path
# would plan them: an unknown collation, RAISE outside a trigger, row values, a non-boolean
# vector WHERE, query_only, and errors raised only on some rows.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c TEXT)
INSERT INTO t(a, b, c) VALUES (1, 'x', 'p'), (2, 'y', 'q'), (-9223372036854775808, 'z', 'r')
UPDATE t SET c = b COLLATE nosuch
UPDATE t SET c = (b COLLATE nosuch) = 'x'
UPDATE t SET c = 'k' WHERE b COLLATE nosuch = 'x'
UPDATE t SET c = RAISE(IGNORE)
UPDATE t SET c = CASE WHEN a > 1 THEN RAISE(ABORT, 'no') ELSE c END
UPDATE t SET c = (a, b)
UPDATE t SET c = 'rv' WHERE (a, b) = (1, 'x')
UPDATE t SET c = (a, b) = (1, 'x')
UPDATE t SET c = 'rv2' WHERE (a, b) IN (VALUES (2, 'y'))
UPDATE t SET c = abs(a)
UPDATE t SET c = abs(a) WHERE a > 0
UPDATE t SET c = a * 2
SELECT * FROM t ORDER BY id
UPDATE t SET c = CAST(a AS nosuchtype)
UPDATE t SET c = a COLLATE NOCASE
UPDATE t SET c = coalesce()
UPDATE t SET c = substr(b)
UPDATE t SET c = max(a)
UPDATE t SET c = "c" || "nosuchcol"
SELECT * FROM t ORDER BY id
PRAGMA query_only = 1
UPDATE t SET c = 'ro'
PRAGMA query_only = 0
SELECT * FROM t ORDER BY id
