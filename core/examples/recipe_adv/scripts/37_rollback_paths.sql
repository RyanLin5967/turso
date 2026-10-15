# k1-adv2. Every path that discards a recipe's transaction or savepoint, with a cached statement
# held across it: nested ROLLBACK TO, cookie ABA after ROLLBACK TO, a failing statement after the
# recipe, INSERT OR ROLLBACK, a deferred-FK COMMIT failure, FK actions in the same transaction.
PRAGMA foreign_keys = ON
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT)
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
INSERT INTO p VALUES (1, 'p1'), (2, 'p2')
INSERT INTO c(pid) VALUES (1), (2)
@prep held SELECT id, a, b FROM t ORDER BY id
BEGIN
SAVEPOINT a
UPDATE t SET b = 'sp-a'
@bind held
SAVEPOINT b
UPDATE t SET a = a * 100
@bind held
ROLLBACK TO b
@bind held
UPDATE t SET a = -a
@bind held
ROLLBACK TO a
@bind held
UPDATE t SET b = b || '!'
RELEASE a
COMMIT
@bind held
SELECT * FROM t ORDER BY id
BEGIN
SAVEPOINT s
UPDATE t SET b = 'gone'
@prep held2 SELECT id, a, b FROM t ORDER BY id
@bind held2
ROLLBACK TO s
CREATE TABLE other(x)
@bind held2
COMMIT
@bind held2
BEGIN
UPDATE t SET b = 'kept'
INSERT INTO c(pid) VALUES (99)
SELECT * FROM t ORDER BY id
DELETE FROM p WHERE id = 1
UPDATE t SET a = a + 5
COMMIT
SELECT * FROM t ORDER BY id
SELECT count(*) FROM c
CREATE TABLE uq(k INTEGER UNIQUE)
INSERT INTO uq VALUES (1)
@bind held
BEGIN
UPDATE t SET b = 'or-rollback'
@bind held
INSERT OR ROLLBACK INTO uq VALUES (1)
SELECT * FROM t ORDER BY id
@bind held
ROLLBACK
SELECT * FROM t ORDER BY id
CREATE TABLE dc(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)
@bind held
BEGIN
UPDATE t SET b = 'deferred'
@bind held
INSERT INTO dc(pid) VALUES (77)
COMMIT
SELECT * FROM t ORDER BY id
@bind held
ROLLBACK
SELECT * FROM t ORDER BY id
@bind held
UPDATE t SET b = 'final'
@bind held
