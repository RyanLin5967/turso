# k1-adv2. Forks through the Branch HANDLE while the branch connection has a transaction open:
# a write transaction holding an uncommitted recipe, then a read transaction with the recipe
# committed before it and another installed after the fork.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
BEGIN
UPDATE t SET b = 'in-txn'
@hfork kid
SELECT * FROM t ORDER BY id
@fork kidc
@conn main
COMMIT
SELECT * FROM t ORDER BY id
BEGIN
SELECT * FROM t ORDER BY id
@hfork kid2
SELECT * FROM t ORDER BY id
@conn main
UPDATE t SET b = 'after-fork'
SELECT * FROM t ORDER BY id
COMMIT
@conn kid2
SELECT * FROM t ORDER BY id
UPDATE t SET a = a + 100
SELECT * FROM t ORDER BY id
@conn main
SELECT * FROM t ORDER BY id
BEGIN
UPDATE t SET a = -a
SAVEPOINT q
UPDATE t SET b = b || '?'
@hfork kid3
ROLLBACK TO q
@hfork kid4
COMMIT
@hfork kid5
SELECT * FROM t ORDER BY id
