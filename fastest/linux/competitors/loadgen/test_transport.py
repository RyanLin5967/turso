#!/usr/bin/env python3
"""C02 refusal reds; pass the built bbload path. No server is required.

A connection failure alone does not count: the transport-policy reason must
appear with rc 2, before libpq attempts to use the nonexistent socket directory.
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    binary = str(Path(sys.argv[1]).resolve())
    for key, value in (("sslmode", "prefer"), ("sslmode", "require"),
                       ("gssencmode", "prefer"), ("connect_timeout", "0")):
        with tempfile.TemporaryDirectory(prefix="C02-") as tmp:
            root = Path(tmp)
            spec = root / "refuse.spec"
            spec.write_text(f"protocol pg\nconnect host={root / 'no-socket'} user=postgres "
                            f"dbname=postgres {key}={value}\nstep sql SELECT 1\n")
            result = subprocess.run([binary, "--spec", str(spec), "--out", str(root / "out"),
                                     "--clients", "1", "--max-ops", "1"],
                                    capture_output=True, text=True, timeout=10,
                                    env=dict(os.environ, PGCONNECT_TIMEOUT="1"))
            assert result.returncode == 2, (key, result.returncode, result.stderr)
            assert f"REFUSED: merged conninfo requires {key}=" in result.stderr, (key, result.stderr)
            print(f"PASS {key}={value}: rc 2 and transport-policy reason")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
