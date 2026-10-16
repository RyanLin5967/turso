\set VERBOSITY verbose
SELECT version();
SELECT pg_typeof(-2147483648), pg_typeof(-9223372036854775808);
SELECT -2147483648 - 1;
SELECT -9223372036854775808 - 1;
SELECT pg_typeof(2147483648), pg_typeof(9223372036854775808);
PREPARE c52_abs_chain(unknown,unknown,int4) AS SELECT $1 WHERE $1=abs($2) AND $2=$3;
PREPARE c52_plain_chain(unknown,unknown,int4) AS SELECT $1 WHERE $1=$2 AND $2=$3;
PREPARE c52_abs_chain_seed AS SELECT $1 WHERE $1=abs($2) AND $2=$3::int4;
PREPARE c52_plain_chain_seed AS SELECT $1 WHERE $1=$2 AND $2=$3::int4;
