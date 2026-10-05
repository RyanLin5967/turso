#!/usr/bin/env python3
"""Print the turso_core lib test executable named in `cargo test --no-run --message-format=json`
output (argv[1]). Exits 1 unless exactly one is named."""
import json
import sys

exes = set()
for line in open(sys.argv[1]):
    try:
        m = json.loads(line)
    except ValueError:
        continue
    t = m.get("target", {})
    if (m.get("reason") == "compiler-artifact" and t.get("name") == "turso_core"
            and "lib" in t.get("kind", []) and m.get("profile", {}).get("test")
            and m.get("executable")):
        exes.add(m["executable"])
if len(exes) != 1:
    sys.exit(f"expected one turso_core lib test executable, got {sorted(exes)}")
print(exes.pop())
