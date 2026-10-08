# A write through an attached alias of the recipe database: the eager UPDATE (an attached target is
# refused as a recipe) reads unchanged columns through cursors on the attached pager, which the read
# path does not recognise as the table's own b-tree, so it copies the stale physical values, and the
# generation column of a short record reads its in-memory DEFAULT (= G): the row is frozen stale.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
UPDATE t SET b = 'upd' || a
SELECT * FROM t ORDER BY id
ATTACH '$SELF' AS o
@expect-diff the attached write freezes row 1 with its pre-recipe b
UPDATE o.t SET a = a + 10 WHERE id = 1
DETACH o
SELECT * FROM t ORDER BY id
@reconnect main
SELECT * FROM t ORDER BY id
