# k1-adv2. 44's swap with a COMPOSITE table-level PRIMARY KEY: to_sql emits ", PRIMARY KEY (..)"
# right after the columns, ahead of a table-level UNIQUE the original text declared before it.
CREATE TABLE t(a TEXT, b TEXT, c TEXT, v INTEGER, UNIQUE(c), PRIMARY KEY(a, b))
INSERT INTO t VALUES ('a1', 'b1', 'c1', 1), ('a2', 'b2', 'c2', 2)
UPDATE t SET v = v + 100
SELECT sql FROM sqlite_schema WHERE name = 't'
@reopen
SELECT a, b FROM t WHERE a = 'a1' AND b = 'b1'
SELECT c FROM t WHERE c = 'c2'
PRAGMA integrity_check
INSERT INTO t VALUES ('a1', 'b1', 'c9', 9)
INSERT INTO t VALUES ('a9', 'b9', 'c1', 9)
SELECT * FROM t ORDER BY rowid
