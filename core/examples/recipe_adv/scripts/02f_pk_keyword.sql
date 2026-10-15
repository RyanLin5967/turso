# A composite PRIMARY KEY over a column whose name needs quoting: to_sql emits it unquoted.
CREATE TABLE t("order" INTEGER, "x y" INTEGER, b INTEGER, PRIMARY KEY ("order", "x y"))
INSERT INTO t VALUES (1, 1, 1)
UPDATE t SET b = b + 100
SELECT sql FROM sqlite_schema WHERE name = 't'
@reopen
SELECT * FROM t
