\set VERBOSITY verbose
SELECT version();
PREPARE c52_scalar_union_param AS SELECT $1 UNION SELECT 1;
PREPARE c52_scalar_window_order AS SELECT row_number() OVER (ORDER BY (SELECT $1::int4));
PREPARE c52_scalar_aggregate_order AS SELECT array_agg(1 ORDER BY (SELECT $1::int4));
PREPARE c52_scalar_frame AS SELECT sum(1) OVER (ORDER BY 1 ROWS (SELECT $1::int8) PRECEDING);
PREPARE c52_scalar_index AS SELECT (ARRAY[1,2])[(SELECT $1::int4)];
PREPARE c52_scalar_common AS SELECT COALESCE((SELECT $1),1);
PREPARE c52_scalar_array_common AS SELECT ARRAY[(SELECT $1),1];
PREPARE c52_scalar_case_order AS SELECT CASE WHEN true THEN $1::int4 WHEN (SELECT $1 WHERE $1=1)=1 THEN 2 ELSE 3 END;
SELECT name,parameter_types FROM pg_prepared_statements WHERE name LIKE 'c52_scalar_%' ORDER BY name;
