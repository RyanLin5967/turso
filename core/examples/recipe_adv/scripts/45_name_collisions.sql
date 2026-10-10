# k1-adv2. The recipe-row name prefix __turso_recipe_ is not a reserved table prefix, and
# parse_recipe_name takes the generation from the row's NAME (rsplit on '_'). A user table whose
# name equals a recipe row's name is renamed together with it by ALTER TABLE ... RENAME (the rename
# function rewrites every sqlite_schema row whose name equals the old name), so the recipe row's
# generation changes at the next schema parse. DDL on unrelated tables is checked alongside.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
CREATE TABLE u(id INTEGER PRIMARY KEY, x INTEGER, y TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
INSERT INTO u(x, y) VALUES (1, 'p')
UPDATE t SET b = b || '+'
UPDATE t SET a = a * 10 WHERE id = 1
SELECT * FROM t ORDER BY id
ALTER TABLE u RENAME TO u2
ALTER TABLE u2 RENAME COLUMN x TO xx
ALTER TABLE u2 ADD COLUMN z INTEGER
ALTER TABLE u2 DROP COLUMN y
SELECT * FROM u2
CREATE TABLE __turso_recipe_t_1(v)
SELECT type, name, tbl_name FROM sqlite_schema WHERE name LIKE '%recipe%' ORDER BY type, name
ALTER TABLE __turso_recipe_t_1 RENAME TO __turso_recipe_t_7
SELECT type, name, tbl_name FROM sqlite_schema WHERE name LIKE '%recipe%' ORDER BY type, name
SELECT * FROM t ORDER BY id
@reopen
SELECT * FROM t ORDER BY id
INSERT INTO t(a, b) VALUES (3, 'z')
SELECT * FROM t ORDER BY id
