# Schema-cookie ABA. A statement prepared inside a transaction that installed a recipe carries the
# recipe and schema version v+1. ROLLBACK restores version v; an unrelated DDL then brings the
# cookie back to v+1, so the held statement is not re-prepared and still applies the rolled-back
# recipe. In the eager arm the UPDATE was never DDL, so the cookie never moved.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
PRAGMA schema_version
BEGIN
UPDATE t SET b = 'rolled-back'
@prep held SELECT id, a, b FROM t ORDER BY id
@prep heldw UPDATE t SET a = a + 1 WHERE id = 1
@bind held
ROLLBACK
SELECT * FROM t ORDER BY id
PRAGMA schema_version
CREATE TABLE other(x)
PRAGMA schema_version
@expect-diff the held statement still applies the rolled-back recipe
@bind held
@bind heldw
SELECT * FROM t ORDER BY id
@bind held
