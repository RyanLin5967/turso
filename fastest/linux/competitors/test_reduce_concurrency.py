"""C04 rework red: reported reduced cells never masquerade as registered C."""
import unittest
import reduce


class ConcurrencyReport(unittest.TestCase):
    def test_reduced_and_control(self):
        cell = {"name": "dolt-b-m1-c4", "verdict": "ok", "notes": []}
        sm = {"clients": 4, "concurrency_label": "REDUCED-CONCURRENCY", "effective_c": 1.25,
              "serial_delete_note": "Dolt 2.4.1 concurrent-delete panic"}
        label, row = reduce.concurrency_row(cell["name"], cell, sm)
        self.assertNotIn("-c4", label)
        self.assertIn("REDUCED-CONCURRENCY", label)
        self.assertIn("1.25", label)
        self.assertEqual(row[0], label)
        self.assertIn("panic", row[-1])
        plain = dict(sm, concurrency_label="registered", effective_c=4)
        label, row = reduce.concurrency_row(cell["name"], cell, plain)
        self.assertEqual(label, cell["name"])
        self.assertEqual(row, reduce.stracecount.table_row(cell))


class TimedVerdict(unittest.TestCase):
    """comp-rev-89ec-b3c3 MED 3: an exploratory cell (a prebranch other than the registered 1 or 1,000) in a real job
    is never credited, so it must fail the run (exit 1), not only print a line."""

    def test_exploratory_cell_fails_a_real_run(self):
        line, is_bad = reduce.timed_line("competitors-dolt-x", "dolt-b-m1-c1", "ok-exploratory-n", "0")
        self.assertTrue(is_bad, f"an exploratory cell in a real job did not count against the run: {line!r}")
        self.assertIn("NOT CREDITED", line)

    def test_controls(self):
        self.assertEqual(reduce.timed_line("j", "c", "ok", "0"), (None, False))
        self.assertEqual(reduce.timed_line("j", "c", "ok-smoke", "1"), (None, False))
        for tv, dry in (("ok", "1"), ("ok-smoke", "0"), ("ok-exploratory-n", "1"), ("MISSING (OSError)", "0")):
            line, is_bad = reduce.timed_line("j", "c", tv, dry)
            self.assertTrue(is_bad and line, f"timed verdict {tv!r} in a dry={dry} job was accepted")


class DeleteCheck(unittest.TestCase):
    """comp-rev-89ec-b3c3 LOW 5: a delete's durability is checkable only where flushes split by op phase (C=1); a C>1
    cell is not checked, which is sound only because its C=1 sibling is, so reduce must refuse a C=1 cell whose
    delete-durable d2 or d0 went unchecked."""

    def test_unchecked_c1_delete_is_refused(self):
        f = getattr(reduce, "delete_check_problem", lambda name, cell: None)
        c1 = {"name": "dolt-b-m1-c1", "delete_durable": "d2", "delete_check": "not checked: no phase split"}
        self.assertIsNotNone(f("dolt-b-m1-c1", c1), "a C=1 d2 cell whose delete was not checked was accepted")
        self.assertIsNotNone(f("dolt-b-m1-c1", dict(c1, delete_check=None)), "a C=1 cell with no delete_check record")

    def test_controls(self):
        f = getattr(reduce, "delete_check_problem", None)
        self.assertIsNotNone(f, "reduce has no delete_check_problem")
        ok = {"name": "dolt-b-m1-c1", "delete_durable": "d2", "delete_check": "checked at C=1 by op phase"}
        self.assertIsNone(f("dolt-b-m1-c1", ok))
        self.assertIsNone(f("dolt-select1-c1", {"delete_durable": "none"}))
        self.assertIsNone(f("dolt-b-m1-c4", dict(ok, delete_check="not checked: no phase split (C>1 ops overlap)")))


if __name__ == "__main__":
    unittest.main()
