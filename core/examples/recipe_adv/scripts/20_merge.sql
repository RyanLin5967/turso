# Branch merge (Merger, BaseRead validation) around recipes.
# (a) The trunk has a recipe from before the fork; the branch changes one row with a point UPDATE.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')
UPDATE t SET b = 'r' || a
@fork b1
UPDATE t SET a = 100 WHERE id = 1
@merge b1
SELECT * FROM t ORDER BY id
# (b) A bulk UPDATE on the branch (a recipe in the recipe arm), rows the trunk never touched.
@fork b2
UPDATE t SET a = a + 1000
@merge b2
SELECT * FROM t ORDER BY id
# (c) A bulk UPDATE on the trunk after the fork that touches rows the branch did not change.
@fork b3
UPDATE t SET b = 'branch' WHERE id = 3
@conn main
UPDATE t SET b = b || '+' WHERE a < 3
@merge b3
SELECT * FROM t ORDER BY id
