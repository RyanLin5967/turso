\set VERBOSITY verbose
SELECT version();
CREATE TABLE c52_alias_t(a integer);
INSERT INTO c52_alias_t VALUES(2);
CREATE VIEW c52_alias_implicit AS SELECT a,true AS flag FROM c52_alias_t;
CREATE VIEW c52_alias_correlated(n) AS SELECT (SELECT a WHERE c52_alias_t.a > 0) FROM c52_alias_t;
ALTER TABLE c52_alias_t RENAME COLUMN a TO b;
SELECT a,flag FROM c52_alias_implicit;
SELECT n FROM c52_alias_correlated;
