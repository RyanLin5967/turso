# Branches forked mid-chain, recipes on both sides of a fork, a reaped parent, reconnects.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)
INSERT INTO t(a, b, c) VALUES (1, 0, 'p'), (2, 0, 'q'), (3, 0, 'r')
UPDATE t SET b = a * 10
@fork b1
SELECT * FROM t ORDER BY id
UPDATE t SET c = c || b
INSERT INTO t(a, b, c) VALUES (4, 4, 's')
@fork b2
UPDATE t SET b = b + 1
UPDATE t SET a = 0 WHERE id = 2
SELECT * FROM t ORDER BY id
@conn main
UPDATE t SET b = -b
UPDATE t SET c = 'trunk' || a
SELECT * FROM t ORDER BY id
@conn b1
SELECT * FROM t ORDER BY id
UPDATE t SET b = b * 2
SELECT * FROM t ORDER BY id
@reap b1
@conn b2
SELECT * FROM t ORDER BY id
@reconnect b2
SELECT * FROM t ORDER BY id
UPDATE t SET c = c || '!'
@fork b3
SELECT * FROM t ORDER BY id
@reap b2
@conn b3
SELECT * FROM t ORDER BY id
@reconnect b3
SELECT * FROM t ORDER BY id
BEGIN
UPDATE t SET a = a + 100
ROLLBACK
SELECT * FROM t ORDER BY id
@conn main
SELECT * FROM t ORDER BY id
@reconnect main
SELECT * FROM t ORDER BY id
