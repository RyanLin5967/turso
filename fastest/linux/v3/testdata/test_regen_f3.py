#!/usr/bin/env python3
"""C05 regression tests run only in scratch; never regenerate tracked fixtures."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("regen_f3", HERE / "regen_f3.py")
regen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(regen)


class RegenTests(unittest.TestCase):
    def test_repeated_regeneration_and_check_are_current(self):
        with tempfile.TemporaryDirectory() as tmp:
            td = Path(tmp)
            for fx in regen.FIXTURES:
                shutil.copytree(HERE / fx, td / fx)
            with mock.patch.object(regen, "TD", str(td)):
                self.assertEqual(regen.main([]), 0)
                first = {p.relative_to(td): p.read_bytes() for p in td.rglob("*") if p.is_file()}
                self.assertEqual(regen.main([]), 0)
                second = {p.relative_to(td): p.read_bytes() for p in td.rglob("*") if p.is_file()}
                self.assertEqual(first, second)
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(regen.main(["--check"]), 0)
                self.assertEqual(output.getvalue().count("current"), len(regen.FIXTURES))

    def test_second_fixture_void_leaves_both_unchanged(self):
        original = regen.regen
        calls = []

        def void_second(f3, work):
            new, rc = original(f3, work)
            calls.append(f3)
            if len(calls) == 2:
                gate = json.loads(new["gate.json"])
                gate["voids"] = ["C05 planted void"]
                new["gate.json"] = json.dumps(gate)
                rc = 1
            return new, rc

        with tempfile.TemporaryDirectory() as tmp:
            td = Path(tmp)
            for fx in regen.FIXTURES:
                shutil.copytree(HERE / fx, td / fx)
            before = {p.relative_to(td): p.read_bytes() for p in td.rglob("*") if p.is_file()}
            with mock.patch.object(regen, "TD", str(td)), mock.patch.object(regen, "regen", void_second):
                self.assertEqual(regen.main([]), 1)
            self.assertEqual(len(calls), 2, "plant must reach the second fixture")
            after = {p.relative_to(td): p.read_bytes() for p in td.rglob("*") if p.is_file()}
            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
