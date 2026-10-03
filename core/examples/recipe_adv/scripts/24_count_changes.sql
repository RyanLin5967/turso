# PRAGMA count_changes appends a ChangeCount + ResultRow after the translated program.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')
PRAGMA count_changes = 1
UPDATE t SET b = 'r' || a
UPDATE t SET b = 'q' WHERE a > 1
UPDATE t SET b = 'none' WHERE a > 100
SELECT * FROM t ORDER BY id
