"""Snapshot safety checks against real SQLite databases; no meter required."""
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import db_snapshot


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "live.sqlite"
        self.destination = self.root / "download.sqlite"
        self.writer = sqlite3.connect(self.source)
        self.addCleanup(self.writer.close)
        self.writer.execute("PRAGMA journal_mode=WAL")
        self.writer.execute("CREATE TABLE readings (id INTEGER PRIMARY KEY, payload BLOB)")
        self.writer.executemany("INSERT INTO readings(payload) VALUES (zeroblob(8192))",
                                [()] * 256)
        self.writer.commit()
        self.destination.write_bytes(b"previous published download")

    def assert_previous_preserved(self):
        self.assertEqual(self.destination.read_bytes(), b"previous published download")
        self.assertEqual(list(self.root.glob("download.sqlite.tmp-*")), [])

    def test_wal_snapshot_is_complete_and_standalone(self):
        result = db_snapshot.snapshot(self.source, self.destination)
        copy = sqlite3.connect(self.destination)
        try:
            self.assertEqual(copy.execute("SELECT COUNT(*) FROM readings").fetchone()[0], 256)
            self.assertEqual(copy.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            self.assertEqual(copy.execute("PRAGMA quick_check").fetchall(), [("ok",)])
        finally:
            copy.close()
        self.assertEqual(result["quick_check"], "ok")
        self.assertEqual(list(self.root.glob("download.sqlite.tmp-*")), [])

    def test_expired_budget_retains_previous_download(self):
        with self.assertRaises(db_snapshot.SnapshotTimeout):
            db_snapshot.snapshot(self.source, self.destination, max_seconds=1e-9)
        self.assert_previous_preserved()

    def test_deadline_interrupts_sqlite_validation(self):
        connect = sqlite3.connect
        clock = [0.0]

        class SlowValidation(sqlite3.Connection):
            def execute(connection, sql, *args, **kwargs):
                if sql == "PRAGMA quick_check":
                    clock[0] = 1600
                return super().execute(sql, *args, **kwargs)

        def controlled_connect(*args, **kwargs):
            return connect(*args, factory=SlowValidation, **kwargs)

        with patch.object(db_snapshot.sqlite3, "connect", side_effect=controlled_connect), \
                patch.object(db_snapshot.time, "monotonic", side_effect=lambda: clock[0]):
            with self.assertRaises(db_snapshot.SnapshotTimeout):
                db_snapshot.snapshot(self.source, self.destination)
        self.assert_previous_preserved()

    def test_deadline_interrupts_sqlite_copy(self):
        connect = sqlite3.connect
        clock = [0.0]

        class SlowCopy(sqlite3.Connection):
            def backup(connection, *args, **kwargs):
                clock[0] = 1600
                return super().backup(*args, **kwargs)

        with patch.object(db_snapshot.sqlite3, "connect", side_effect=lambda *a, **k:
                          connect(*a, factory=SlowCopy, **k)), \
                patch.object(db_snapshot.time, "monotonic", side_effect=lambda: clock[0]):
            with self.assertRaises(db_snapshot.SnapshotTimeout):
                db_snapshot.snapshot(self.source, self.destination)
        self.assert_previous_preserved()

    def test_invalid_budget_changes_nothing(self):
        for budget in (0, -1, float("nan"), float("inf")):
            with self.subTest(budget=budget), self.assertRaises(ValueError):
                db_snapshot.snapshot(self.source, self.destination, max_seconds=budget)
            self.assert_previous_preserved()


if __name__ == "__main__":
    unittest.main()
