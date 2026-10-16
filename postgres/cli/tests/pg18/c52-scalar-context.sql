\set VERBOSITY verbose
SELECT version();
PREPARE c52_scalar_left AS SELECT $1::int4 = (SELECT $1 WHERE $1=1);
PREPARE c52_scalar_right AS SELECT (SELECT $1 WHERE $1=1) = $1::int4;
PREPARE c52_scalar_compare AS SELECT $1 = (SELECT 1);
PREPARE c52_scalar_cte AS SELECT $1 = (WITH q AS (SELECT 1 AS n) SELECT n FROM q);
PREPARE c52_scalar_order AS SELECT (SELECT $1), $1::int4;
SELECT name,parameter_types FROM pg_prepared_statements WHERE name LIKE 'c52_scalar_%' ORDER BY name;
SELECT pg_typeof(abs((SELECT 1::numeric UNION SELECT 1::numeric)))::text;
SELECT pg_typeof((WITH q AS (SELECT 1::numeric AS n) SELECT n FROM q))::text;
