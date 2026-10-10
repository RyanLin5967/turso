# k1-adv2. 45 with DROP TABLE of the colliding user table, then a rename to a name that no longer
# parses as a recipe generation.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
UPDATE t SET b = b || '+'
CREATE TABLE __turso_recipe_t_1(v)
DROP TABLE __turso_recipe_t_1
SELECT type, name, tbl_name FROM sqlite_schema ORDER BY type, name
SELECT * FROM t ORDER BY id
@reopen
SELECT * FROM t ORDER BY id
CREATE TABLE __turso_recipe_t_1(v)
ALTER TABLE __turso_recipe_t_1 RENAME TO plain
SELECT type, name, tbl_name FROM sqlite_schema ORDER BY type, name
@reopen
SELECT * FROM t ORDER BY id
