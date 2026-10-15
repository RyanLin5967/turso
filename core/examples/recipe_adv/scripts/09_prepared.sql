# Prepared statements reused across an install, on the installing connection and a second one.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')
@prep sel SELECT id, a, b FROM t ORDER BY id
@prep ins INSERT INTO t(a, b) VALUES (?1, ?2)
@prep upd UPDATE t SET a = a + ?1 WHERE id = ?2
@bind sel
UPDATE t SET b = 'r' || a
@bind sel
@bind ins 4 'new'
@bind sel
@bind upd 100 1
@bind sel
UPDATE t SET a = a * 10
@bind ins 5 'new5'
@bind sel
@conn c2
@prep sel2 SELECT id, a, b FROM t ORDER BY id
@prep ins2 INSERT INTO t(a, b) VALUES (?1, ?2)
@bind sel2
@conn main
UPDATE t SET b = b || '+'
@conn c2
@bind sel2
@bind ins2 6 'c2'
@bind sel2
@conn main
@bind sel
# A statement stepped halfway, an install on the same connection, then the rest of the steps.
@open sel
@next sel
@next sel
UPDATE t SET a = -a
@next sel
@next sel
@next sel
@next sel
@next sel
SELECT * FROM t ORDER BY id
