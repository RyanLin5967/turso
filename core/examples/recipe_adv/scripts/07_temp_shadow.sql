# A TEMP table shadows the main table of the same name for an unqualified UPDATE. plan_recipe
# looks the name up in the main schema only.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'main1'), (2, 'main2')
CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO temp.t(a, b) VALUES (1, 'temp1'), (2, 'temp2')
@expect-diff the recipe lands on main.t, eager writes temp.t
UPDATE t SET b = 'updated'
SELECT * FROM main.t ORDER BY id
SELECT * FROM temp.t ORDER BY id
SELECT * FROM t ORDER BY id
