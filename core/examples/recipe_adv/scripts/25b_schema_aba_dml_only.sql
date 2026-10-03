# 25 with no DDL at all: only UPDATEs and one cached SELECT. The second bulk UPDATE re-uses the
# schema version the rolled-back one had, so the cached statement keeps the rolled-back recipe and
# never learns of the committed one.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c TEXT)
INSERT INTO t(a, b, c) VALUES (1, 'x', 'p'), (2, 'y', 'q')
@prep cached SELECT id, a, b, c FROM t ORDER BY id
BEGIN
UPDATE t SET b = 'rolled-back'
@bind cached
ROLLBACK
UPDATE t SET c = 'committed'
@expect-diff the cached statement shows b from the rolled-back recipe and misses the committed one
@bind cached
SELECT id, a, b, c FROM t ORDER BY id
