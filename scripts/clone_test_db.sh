#!/bin/bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Install sqlite3 locally if needed
"$PROJECT_ROOT/scripts/install-sqlite3.sh"
SQLITE3_BIN="$PROJECT_ROOT/.sqlite3/sqlite3"

# The test databases moved to testing/system when the TCL tests did, but this
# script kept the old path -- and mangled it, reading "testing/testing/db".
# sqlite3 opened that as a new empty database, .clone printed its errors on
# stderr and still exited 0, so the clone landed in the wrong directory with no
# tables and nothing noticed. bindings/c/tests/compat/mod.rs reads
# testing/system/testing_clone.db, which is what this now produces.
SOURCE_DB="$PROJECT_ROOT/testing/system/testing.db"
CLONE_DB="$PROJECT_ROOT/testing/system/testing_clone.db"

if [ ! -f "$SOURCE_DB" ]; then
    echo "clone_test_db: source database missing: $SOURCE_DB" >&2
    exit 1
fi

rm -f "$CLONE_DB" "$CLONE_DB-wal" "$CLONE_DB-shm"

"$SQLITE3_BIN" "$SOURCE_DB" ".clone $CLONE_DB" > /dev/null

# .clone reports failures on stderr but still exits 0, so the exit code cannot be
# trusted here. An empty clone would let the compat tests run green against a
# database with no tables, which is worse than failing.
TABLE_COUNT="$("$SQLITE3_BIN" "$CLONE_DB" \
    "SELECT count(*) FROM sqlite_schema WHERE type='table'" 2>/dev/null || echo 0)"
if [ "${TABLE_COUNT:-0}" -lt 1 ]; then
    echo "clone_test_db: $CLONE_DB has no tables after cloning $SOURCE_DB" >&2
    exit 1
fi
