# k1-adv2. Statements an eager database accepts that a recipe table refuses (listed as refusals,
# not wrong answers): every ALTER other than a plain ADD COLUMN, VACUUM, blob handles.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c BLOB)
INSERT INTO t(a, b, c) VALUES (1, 'x', x'00'), (2, 'y', x'01')
UPDATE t SET b = 'r' || a
ALTER TABLE t RENAME COLUMN a TO aa
ALTER TABLE t RENAME TO t2
ALTER TABLE t DROP COLUMN c
ALTER TABLE t ADD COLUMN g INTEGER GENERATED ALWAYS AS (a * 2)
ALTER TABLE t ADD COLUMN ok INTEGER DEFAULT 3
ALTER TABLE t ADD COLUMN ck INTEGER CHECK (ck IS NULL OR ck > 0)
ALTER TABLE t ADD COLUMN fk INTEGER REFERENCES t(id)
VACUUM
SELECT * FROM t ORDER BY id
@blobread t c 1
CREATE TABLE s(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO s(a, b) VALUES (1, 'q')
UPDATE s SET b = 'r'
ALTER TABLE s RENAME TO s2
SELECT * FROM s ORDER BY id
