# Minimal form of 10: plain SQL only. A reader holding a snapshot from before another
# connection's install, with a statement prepared before it (a statement cache), re-prepares after
# any setter that bumps the prepare context (PRAGMA cache_size here) and adopts the newer shared
# schema inside its open transaction; from then on every statement fails until COMMIT.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
@conn c2
@prep cached SELECT id, a, b FROM t ORDER BY id
BEGIN
@bind cached
@conn main
UPDATE t SET b = 'installed'
@conn c2
PRAGMA cache_size = 100
@bind cached
SELECT count(*) FROM t
COMMIT
SELECT * FROM t ORDER BY id
