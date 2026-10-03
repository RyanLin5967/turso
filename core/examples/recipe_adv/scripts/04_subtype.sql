# A recipe's output register goes straight to the reader, so a value's subtype (JSON) survives,
# where the eager UPDATE's record write drops it.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, c TEXT)
INSERT INTO t(a) VALUES (1), (2)
UPDATE t SET c = json_object('k', a)
SELECT id, c FROM t ORDER BY id
@expect-diff subtype leaks through the recipe path
SELECT id, json_array(c) FROM t ORDER BY id
SELECT id, json_object('x', c) FROM t ORDER BY id
SELECT id, subtype(c) FROM t ORDER BY id
SELECT id, json_insert('{}', '$.v', c) FROM t ORDER BY id
SELECT json_group_array(c) FROM t
