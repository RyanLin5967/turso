\set ON_ERROR_STOP on
SELECT version();
CREATE TABLE c52_derived_t(a integer,b boolean);
INSERT INTO c52_derived_t VALUES(1,true);
CREATE VIEW c52_derived_v AS SELECT d.b FROM (SELECT * FROM c52_derived_t) d(x);
SELECT b,pg_typeof(b)::text,pg_typeof(b)::oid FROM c52_derived_v;
ALTER TABLE c52_derived_t RENAME COLUMN b TO z;
SELECT b,pg_typeof(b)::text,pg_typeof(b)::oid FROM c52_derived_v;
SELECT pg_get_viewdef('c52_derived_v'::regclass,true);
