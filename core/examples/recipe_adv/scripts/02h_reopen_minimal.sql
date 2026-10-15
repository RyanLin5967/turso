# The minimal reopen: one recipe, close, open. create_table (the sqlite_schema parse) never sets a
# column's hidden flag, so the reparsed __turso_gen is not is_gen_column() and attach_recipe fails.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x')
UPDATE t SET b = 'y'
@reopen
SELECT * FROM t
