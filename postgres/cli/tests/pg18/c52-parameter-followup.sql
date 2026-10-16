\set VERBOSITY verbose
SELECT version();
PREPARE c52_target AS SELECT $1 WHERE $1=1;
PREPARE c52_abs AS SELECT $1 WHERE $1=abs($2);
PREPARE c52_abs_only AS SELECT abs($1);
PREPARE c52_eq AS SELECT $1=$2;
SELECT name,parameter_types FROM pg_prepared_statements ORDER BY name;
SELECT 16777217 - 16777216::float4;
SELECT 1e100::numeric * 0::float4;
SELECT '1e-100'::numeric * 'Infinity'::float4;
