# k1-adv2. Catalog and storage metadata a recipe changes that an eager UPDATE does not: the
# column count PRAGMA table_list reports (it counts the hidden column), table_xinfo, the schema
# cookie, sqlite_schema rows, and page counts after a bulk UPDATE that grows rows.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
PRAGMA table_list(t)
UPDATE t SET b = 'r' || a
PRAGMA table_list(t)
SELECT name, ncol FROM pragma_table_list WHERE name = 't'
SELECT name, hidden FROM pragma_table_xinfo('t')
PRAGMA schema_version
CREATE TABLE big(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO big(a) SELECT value FROM generate_series(1, 3000)
PRAGMA page_count
UPDATE big SET b = substr('0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' || a, -200)
PRAGMA page_count
PRAGMA freelist_count
SELECT count(*), sum(length(b)) FROM big
SELECT rootpage > 0 FROM sqlite_schema WHERE name = 'big'
CREATE TABLE later(x)
SELECT name, rootpage FROM sqlite_schema WHERE name = 'later'
