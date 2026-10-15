# k1-adv2. Durable reopen through Database::branch of EVERY branch after a recipe chain with an
# ADD COLUMN in the middle, an index on a target, a fork child with its own recipe, and a reaped
# parent whose child is reopened.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)
INSERT INTO t(a, b, c) VALUES (1, 'x', 1.5), (2, 'y', 2.5), (3, NULL, NULL)
UPDATE t SET b = coalesce(b, 'nil') || a
UPDATE t SET c = c * 2 WHERE a > 1
ALTER TABLE t ADD COLUMN d INTEGER DEFAULT 7
UPDATE t SET d = d + a
INSERT INTO t(a, b, c) VALUES (4, 'w', 4.0)
CREATE INDEX ib ON t(b)
UPDATE t SET a = a * 10 WHERE id = 2
@fork kid
UPDATE t SET d = d * 100
SELECT * FROM t ORDER BY id
@conn main
SELECT * FROM t ORDER BY id
@reopenall
SELECT * FROM t ORDER BY id
SELECT b FROM t WHERE b > 'n' ORDER BY b
PRAGMA integrity_check
INSERT INTO t(a, b) VALUES (5, 'v')
SELECT * FROM t ORDER BY id
@conn kid
SELECT * FROM t ORDER BY id
UPDATE t SET c = -c
@hfork grandkid
UPDATE t SET b = upper(b)
SELECT * FROM t ORDER BY id
@reap kid
@conn grandkid
SELECT * FROM t ORDER BY id
@reopenall
@conn grandkid
SELECT * FROM t ORDER BY id
UPDATE t SET a = a + 1 WHERE id = 1
SELECT * FROM t ORDER BY id
@conn main
SELECT * FROM t ORDER BY id
@detach __main
@attach __main
SELECT * FROM t ORDER BY id
