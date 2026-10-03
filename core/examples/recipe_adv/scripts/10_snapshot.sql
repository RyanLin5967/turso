# A second trunk connection holding a read snapshot from before the install.
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)
INSERT INTO t(a, b) VALUES (1, 'x'), (2, 'y')
@conn c2
@prep held SELECT id, a, b FROM t ORDER BY id
BEGIN
SELECT * FROM t ORDER BY id
@conn main
UPDATE t SET b = 'installed'
SELECT * FROM t ORDER BY id
@conn c2
SELECT * FROM t ORDER BY id
@bind held
# set_recipe_backfill bumps the prepare-context generation, so a held statement re-prepares.
@recipe off
@bind held
SELECT * FROM t ORDER BY id
@recipe on
SELECT * FROM t ORDER BY id
PRAGMA cache_size = 100
@bind held
SELECT * FROM t ORDER BY id
COMMIT
SELECT * FROM t ORDER BY id
@bind held
