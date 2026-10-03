# Subprograms that read or write the recipe table with their own cursors: a trigger body on
# another table, and a foreign-key ON UPDATE CASCADE into a recipe table.
PRAGMA foreign_keys = ON
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
CREATE TABLE u(x INTEGER)
CREATE TABLE log(v)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')
UPDATE t SET b = 'upd' || a
CREATE TRIGGER tr AFTER INSERT ON u BEGIN UPDATE t SET a = a + 1 WHERE id = NEW.x; INSERT INTO log SELECT b FROM t WHERE id = NEW.x + 1; END
INSERT INTO u VALUES (1)
SELECT * FROM t ORDER BY id
SELECT * FROM log
CREATE TABLE p(id INTEGER PRIMARY KEY)
INSERT INTO p VALUES (1), (2)
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON UPDATE CASCADE ON DELETE SET NULL, v TEXT)
INSERT INTO c(pid, v) VALUES (1, 'a'), (2, 'b'), (1, 'c')
UPDATE c SET v = 'recipe-' || v
SELECT * FROM c ORDER BY id
UPDATE p SET id = 10 WHERE id = 1
SELECT * FROM c ORDER BY id
DELETE FROM p WHERE id = 2
SELECT * FROM c ORDER BY id
