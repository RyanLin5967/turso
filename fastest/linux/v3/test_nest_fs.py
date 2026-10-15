#!/usr/bin/env python3
"""C06: filesystem identity fixtures; no mounts or root privileges required."""
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import check


class NestFsTests(unittest.TestCase):
    def judge(self, cell, first, rebuilt):
        devices = {"/": 1, "/mnt/fastest-v3-ext4": 1, "/mnt/brd": 2}
        with patch.object(check.os, "stat", side_effect=lambda p: SimpleNamespace(st_dev=devices[p])):
            return check.nest_n1_problems(cell, first, rebuilt)

    def test_ext4_directory_on_root_is_same_filesystem(self):
        self.assertEqual(self.judge("/", "/mnt/fastest-v3-ext4/v3fx-n1.img",
                                    "/mnt/fastest-v3-ext4/v3fx-n1.img"), [])

    def test_brd_with_first_n1_on_root_is_refused(self):
        self.assertTrue(self.judge("/mnt/brd", "/v3fx-n1.img", "/mnt/brd/v3fx-n1.img"))

    def test_brd_with_rebuilt_n1_on_root_is_refused(self):
        self.assertTrue(self.judge("/mnt/brd", "/mnt/brd/v3fx-n1.img", "/v3fx-n1.img"))

    def test_unreadable_device_is_refused(self):
        with patch.object(check.os, "stat", side_effect=OSError("C06 unreadable")):
            self.assertTrue(check.nest_n1_problems("/", "/v3fx-n1.img", "/v3fx-n1.img"))


if __name__ == "__main__":
    unittest.main()
