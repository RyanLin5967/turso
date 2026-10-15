# Change data capture turned on after a recipe: the before-images of later changes to stale rows.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
UPDATE t SET b = 'recipe' || a
PRAGMA capture_data_changes_conn('full')
UPDATE t SET a = a + 10 WHERE id = 1
DELETE FROM t WHERE id = 2
SELECT change_type, table_name, id, bin_record_json_object(table_columns_json_array('t'), before), bin_record_json_object(table_columns_json_array('t'), after) FROM turso_cdc ORDER BY change_id
SELECT change_type, table_name, id, hex(before), hex(after) FROM turso_cdc ORDER BY change_id
