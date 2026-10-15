# table_columns_json_array reads the connection's CURRENT schema when evaluated, and is marked
# deterministic, so it passes validate_generated_expr and a recipe re-evaluates it at read time.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, c TEXT)
INSERT INTO t(a) VALUES (1), (2)
@expect-diff the gen column already shows in the recipe arm
UPDATE t SET c = table_columns_json_array('t')
SELECT id, c FROM t ORDER BY id
ALTER TABLE t ADD COLUMN later INTEGER
SELECT id, c FROM t ORDER BY id
