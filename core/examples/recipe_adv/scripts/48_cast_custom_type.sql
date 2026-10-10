# k1-adv2. Run with RECIPE_ADV_TYPES=1 (custom types on, both arms). CAST(x AS name) resolves
# `name` against the schema's custom types when the expression is COMPILED; a recipe is compiled
# at read time (CompiledRecipe::compile, the connection's current schema), so a type created or
# dropped after the recipe changes what the recipe computes. The eager UPDATE stored the value the
# CAST produced when it ran.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, c, d)
INSERT INTO t(a) VALUES (1), (2)
SELECT CAST(7 AS cents)
UPDATE t SET c = CAST(a AS cents)
SELECT * FROM t ORDER BY id
CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100
SELECT CAST(7 AS cents)
SELECT * FROM t ORDER BY id
UPDATE t SET d = CAST(a AS cents)
SELECT * FROM t ORDER BY id
DROP TYPE cents
SELECT * FROM t ORDER BY id
@reopen
SELECT * FROM t ORDER BY id
CREATE TYPE cents BASE integer ENCODE value * 1000 DECODE value / 1000
SELECT * FROM t ORDER BY id
UPDATE t SET a = a + 1 WHERE id = 1
SELECT * FROM t ORDER BY id
