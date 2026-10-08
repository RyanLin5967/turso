# k1-adv2. A statement in flight across a ROLLBACK (and a ROLLBACK TO) that discards a recipe.
# The in-flight program carries the TableRecipes it was compiled with.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')
BEGIN
UPDATE t SET b = 'rb'
@prep s SELECT id, a, b FROM t ORDER BY id
@open s
@next s
ROLLBACK
@next s
@next s
@next s
SELECT * FROM t ORDER BY id
BEGIN
SAVEPOINT sp
UPDATE t SET b = 'sp'
@prep s2 SELECT id, a, b FROM t ORDER BY id
@open s2
@next s2
ROLLBACK TO sp
@next s2
@next s2
@next s2
COMMIT
SELECT * FROM t ORDER BY id
