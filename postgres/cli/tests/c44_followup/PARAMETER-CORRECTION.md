# Recorded parameter expectation correction

C44's `PGDIV-PARAM-OVER-LIMIT-CODE` is the Describe of `SELECT v FROM t WHERE id = $65536`. The committed PostgreSQL 18.6 recording gives 42P18, naming the first untyped parameter $1. The older loop expected 42P02 for every extended case and contradicted that recorded answer.

Only that extended expectation changes. Both simple-protocol expectations remain 42P02; extended `SELECT 1 LIMIT $2147483647` remains 42P02. The recovery assertion after every refusal remains unchanged.

Recording: `postgres/cli/tests/pg18/pg18-audit3-a_parameter_number_past_the_limit_is_refused.out`, commands #5 and #8; recorded PostgreSQL 18.6 at 2026-10-15T21:01:15Z.

- Source commit: `9251af1fb72e428c7c31fcd7d07bbd0629265085` (C55; recording unchanged).
- Git blob: `bec6434aa66861b2bb841d2cecc0e3539fc04532`.
- SHA-256: `0980966c6eb39ec887c74bec23ac38e340f3404d4a84d6da0f4642ca2edd207e`.

Authorization: C44's named recorded cause and Ryan's latest blanket authorization, applied by the coordinating root. This is a correction to the committed PostgreSQL answer, not a newly claimed Claude ruling. The new focused regression reproduced 42P02 before the producer fix and requires the recorded 42P18.
