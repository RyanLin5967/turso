# A write statement compiled before an install keeps running after it on the same connection
# (UPDATE ... RETURNING yields per row, and is refused as a recipe, so it runs eagerly). Its
# program has the pre-install table: no gen column, no recipe. Rows it writes after the install
# carry no generation, read as 0, so the new recipe is applied on top of a row written after it.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)
INSERT INTO t(a, b) VALUES (1, 0), (2, 0), (3, 0)
@prep ret UPDATE t SET a = a + 100 RETURNING id, a, b
@open ret
@next ret
@expect-diff the recipe (b = a) is applied to rows the in-flight statement writes after it
UPDATE t SET b = a
@next ret
@next ret
@next ret
SELECT * FROM t ORDER BY id
SELECT id, a, b FROM t WHERE b = a ORDER BY id
