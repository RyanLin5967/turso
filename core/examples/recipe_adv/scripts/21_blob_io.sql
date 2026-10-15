# Incremental blob I/O (sqlite3_blob_open family) addresses the record's bytes directly, not
# through op_column. A read of a recipe target returns the stored (pre-recipe) bytes; a write to a
# recipe INPUT changes a stale row's stored field without freezing it, so the recipe re-reads it.
CREATE TABLE t(id INTEGER PRIMARY KEY, x BLOB, y BLOB, n INTEGER)
INSERT INTO t(x, y, n) VALUES (x'00112233', x'aabbccdd', 0), (x'44556677', x'eeff0011', 0)
UPDATE t SET x = x'99999999'
SELECT id, hex(x) FROM t ORDER BY id
@expect-diff the blob handle reads the stored bytes, not the recipe's value
@blobread t x 1
UPDATE t SET n = length(y) + unicode(CAST(y AS TEXT))
SELECT id, n, hex(y) FROM t ORDER BY id
@expect-diff the write to y changes the input a stale row's recipe re-reads
@blobwrite t y 2 01020304
SELECT id, n, hex(y) FROM t ORDER BY id
