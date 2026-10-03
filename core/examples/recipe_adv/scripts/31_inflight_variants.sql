# k1-adv2. Variants of a read statement in flight across a recipe install on the same (branch)
# connection: compiled before the table's FIRST recipe (no gen column), across an install plus a
# point UPDATE that freezes a row the reader has not reached, and an aggregate opened but unstepped.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z'), (4, 'w')
@prep s SELECT id, a, b FROM t ORDER BY id
@open s
@next s
UPDATE t SET b = 'first'
@next s
@next s
@next s
@next s
@prep s3 SELECT id, b FROM t ORDER BY id
@open s3
@next s3
UPDATE t SET b = b || '-2'
UPDATE t SET a = a + 100 WHERE id = 4
@next s3
@next s3
@next s3
@next s3
@prep agg SELECT count(*), group_concat(b, ',') FROM t WHERE id > ?1
@open agg 0
UPDATE t SET b = 'q'
@next agg
SELECT * FROM t ORDER BY id
