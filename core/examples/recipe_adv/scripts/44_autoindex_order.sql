# k1-adv2. The first recipe rewrites CREATE TABLE through BTreeTable::to_sql, which puts a
# single-column table-level PRIMARY KEY inline on its column. create_table orders unique sets
# column-level first, then table constraints, and populate_indices pairs them with the
# sqlite_autoindex_<t>_N rows in that order. Moving the PRIMARY KEY inline ahead of an earlier
# column-level UNIQUE swaps which autoindex b-tree each constraint gets at the next schema parse.
# The plan's re-parse check compares only unique_sets.len(), so it does not see the swap.
CREATE TABLE t(k TEXT, a TEXT UNIQUE, b INTEGER, PRIMARY KEY(k))
INSERT INTO t VALUES ('k1', 'a1', 1), ('k2', 'a2', 2), ('k3', 'a3', 3)
SELECT type, name, tbl_name FROM sqlite_schema WHERE tbl_name = 't' ORDER BY name
UPDATE t SET b = b * 10
SELECT sql FROM sqlite_schema WHERE name = 't'
SELECT k FROM t ORDER BY k
SELECT * FROM t WHERE k = 'k2'
@reopen
SELECT * FROM t ORDER BY rowid
SELECT k FROM t ORDER BY k
SELECT a FROM t ORDER BY a
SELECT * FROM t WHERE k = 'k2'
SELECT * FROM t WHERE a = 'a2'
SELECT count(*) FROM t WHERE k >= 'k'
PRAGMA integrity_check
INSERT INTO t VALUES ('k1', 'a9', 9)
INSERT INTO t VALUES ('k9', 'a1', 9)
SELECT * FROM t ORDER BY rowid
SELECT k, a FROM t ORDER BY k
