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


if __name__ == "__main__":
    unittest.main()
