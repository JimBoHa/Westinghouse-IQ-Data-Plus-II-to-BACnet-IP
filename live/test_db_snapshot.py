"""Snapshot safety checks against real SQLite databases; no meter required."""
from pathlib import Path
import json
import select
import signal
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
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

    def snapshot(self, **options):
        options.setdefault("min_free_bytes", 0)
        return db_snapshot.snapshot(self.source, self.destination, **options)

    def test_wal_snapshot_is_complete_and_standalone(self):
        result = self.snapshot()
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
            self.snapshot(max_seconds=1e-9)
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
                self.snapshot()
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
                self.snapshot()
        self.assert_previous_preserved()

    def test_invalid_budget_changes_nothing(self):
        for budget in (0, -1, float("nan"), float("inf")):
            with self.subTest(budget=budget), self.assertRaises(ValueError):
                self.snapshot(max_seconds=budget)
            self.assert_previous_preserved()

    def start_slow_snapshot(self):
        # Pause inside a real SQLite backup callback, with a partial copy on disk.
        code = '''
import sqlite3, sys, time
sys.path.insert(0, sys.argv[1])
import db_snapshot
connect = sqlite3.connect
class SlowCopy(sqlite3.Connection):
    def backup(self, target, **kwargs):
        check = kwargs['progress']
        def progress(status, remaining, total):
            print('COPYING', flush=True)
            while True:
                check(status, remaining, total)
                time.sleep(.01)
        kwargs['progress'] = progress
        return super().backup(target, **kwargs)
db_snapshot.sqlite3.connect = lambda *a, **k: connect(*a, factory=SlowCopy, **k)
db_snapshot.snapshot(sys.argv[2], sys.argv[3], max_seconds=10, min_free_bytes=0)
'''
        process = subprocess.Popen([sys.executable, "-c", code, str(Path(db_snapshot.__file__).parent),
                                    str(self.source), str(self.destination)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        def finish():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)
        self.addCleanup(finish)
        self.assertTrue(select.select([process.stdout], [], [], 5)[0], "backup did not start")
        self.assertEqual(process.stdout.readline().strip(), "COPYING")
        self.assertTrue(list(self.root.glob("download.sqlite.tmp-*")))
        return process

    def test_sigterm_and_sigint_remove_partial_copy(self):
        for number in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=number):
                previous = signal.getsignal(number)
                process = self.start_slow_snapshot()
                process.send_signal(number)
                process.communicate(timeout=5)
                self.assertNotEqual(process.returncode, 0)
                self.assert_previous_preserved()
                self.assertEqual(signal.getsignal(number), previous)

    def test_startup_recovers_after_sigkill(self):
        process = self.start_slow_snapshot()
        process.kill()
        process.communicate(timeout=5)
        abandoned = {p.name for p in self.root.glob("download.sqlite.tmp-*")}
        self.assertTrue(abandoned)
        result = self.snapshot()
        self.assertEqual(set(result["orphan_files_removed"]), abandoned)
        self.assertEqual(list(self.root.glob("download.sqlite.tmp-*")), [])
        self.assertEqual(result["quick_check"], "ok")

    def test_cleanup_preserves_unrelated_files_and_live_wal(self):
        owned = self.destination.with_name(self.destination.name + ".tmp-" + "a" * 32)
        for path in db_snapshot.temporary_paths(owned):
            path.write_bytes(b"abandoned")
        unrelated = self.root / "download.sqlite.tmp-not-a-snapshot"
        unrelated.write_bytes(b"keep")
        link = self.root / ("download.sqlite.tmp-" + "b" * 32)
        link.symlink_to(self.source)
        directory = self.root / ("download.sqlite.tmp-" + "c" * 32)
        directory.mkdir()
        result = db_snapshot.cleanup_snapshot(self.source, self.destination)
        self.assertEqual(len(result["removed"]), 4)
        self.assertEqual(self.destination.read_bytes(), b"previous published download")
        self.assertEqual(unrelated.read_bytes(), b"keep")
        self.assertTrue(link.is_symlink())
        self.assertTrue(directory.is_dir())
        self.assertTrue(Path(str(self.source) + "-wal").is_file())
        self.assertEqual(self.writer.execute("SELECT COUNT(*) FROM readings").fetchone()[0], 256)

    def test_live_source_matching_temporary_namespace_is_protected(self):
        protected = self.root / ("download.sqlite.tmp-" + "d" * 32)
        for path in db_snapshot.temporary_paths(protected):
            path.write_bytes(b"keep live source and sidecars")
        self.assertEqual(db_snapshot.cleanup_snapshot(protected, self.destination)["removed"], [])
        for path in db_snapshot.temporary_paths(protected):
            self.assertTrue(path.is_file())

    def test_lock_prevents_cleanup_or_concurrent_snapshot(self):
        process = self.start_slow_snapshot()
        abandoned = list(self.root.glob("download.sqlite.tmp-*"))
        for operation in (db_snapshot.cleanup_snapshot, db_snapshot.snapshot):
            with self.subTest(operation=operation.__name__), self.assertRaises(db_snapshot.SnapshotBusy):
                operation(self.source, self.destination)
            self.assertTrue(all(path.exists() for path in abandoned))
        cleanup = subprocess.run([sys.executable, db_snapshot.__file__, "--cleanup-only",
                                  str(self.source), str(self.destination)], capture_output=True, text=True)
        self.assertEqual(cleanup.returncode, 0, cleanup.stderr)
        self.assertIn("cleanup_skipped", cleanup.stdout)
        process.terminate()
        process.communicate(timeout=5)
        self.assert_previous_preserved()

    def test_space_preflight_counts_logical_pages_still_in_live_wal(self):
        self.assertGreater(Path(str(self.source) + "-wal").stat().st_size,
                           self.source.stat().st_size)
        # Enough for the tiny main file, but not for the WAL-resident contents.
        with patch.object(db_snapshot.shutil, "disk_usage", return_value=SimpleNamespace(free=1024**2)):
            with self.assertRaises(db_snapshot.SnapshotSpaceError):
                self.snapshot(min_free_bytes=65536)
        self.assert_previous_preserved()
        status = db_snapshot.snapshot_status(self.destination)
        self.assertEqual(status["state"], "failed")
        self.assertEqual(status["error_type"], "SnapshotSpaceError")
        self.assertEqual(status["phase"], "preflight")

    def test_space_loss_during_copy_or_validation_aborts_cleanly(self):
        connect = sqlite3.connect
        for stage in ("copy", "validation"):
            with self.subTest(stage=stage):
                pressure = [False]
                clock = [0.0]

                class Pressure(sqlite3.Connection):
                    def backup(connection, *args, **kwargs):
                        if stage == "copy":
                            pressure[0], clock[0] = True, 2.0
                        return super().backup(*args, **kwargs)

                    def execute(connection, sql, *args, **kwargs):
                        if stage == "validation" and sql == "PRAGMA quick_check":
                            pressure[0], clock[0] = True, 2.0
                        return super().execute(sql, *args, **kwargs)

                with patch.object(db_snapshot.sqlite3, "connect", side_effect=lambda *a, **k:
                                  connect(*a, factory=Pressure, **k)), \
                        patch.object(db_snapshot.time, "monotonic", side_effect=lambda: clock[0]), \
                        patch.object(db_snapshot.shutil, "disk_usage", side_effect=lambda _:
                                     SimpleNamespace(free=0 if pressure[0] else 1024**4)):
                    with self.assertRaises(db_snapshot.SnapshotSpaceError):
                        self.snapshot(min_free_bytes=65536)
                self.assert_previous_preserved()

    def test_failure_retains_last_success_and_exposes_age(self):
        good = self.snapshot()
        before = self.destination.read_bytes()
        with patch.object(db_snapshot.shutil, "disk_usage", return_value=SimpleNamespace(free=0)):
            with self.assertRaises(db_snapshot.SnapshotSpaceError):
                self.snapshot(min_free_bytes=65536)
        status = db_snapshot.snapshot_status(self.destination)
        self.assertEqual(status["state"], "failed")
        self.assertEqual(status["last_success"], good)
        self.assertGreaterEqual(status["published_age_seconds"], 0)
        self.assertEqual(status["published_bytes"], len(before))
        self.assertEqual(self.destination.read_bytes(), before)

    def test_cleanup_marks_killed_job_failed(self):
        process = self.start_slow_snapshot()
        process.kill()
        process.communicate(timeout=5)
        self.assertEqual(db_snapshot.snapshot_status(self.destination)["state"], "running")
        db_snapshot.cleanup_snapshot(self.source, self.destination)
        self.assertEqual(db_snapshot.snapshot_status(self.destination)["state"], "failed")
        self.assert_previous_preserved()

    def test_cli_failure_is_machine_readable_and_nonzero(self):
        result = subprocess.run([sys.executable, db_snapshot.__file__, "--min-free-gib", "1000000000",
                                 str(self.source), str(self.destination)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stderr)["error_type"], "SnapshotSpaceError")
        self.assert_previous_preserved()

    def test_live_wal_can_checkpoint_while_destination_is_validated(self):
        connect = sqlite3.connect
        self.writer.execute("PRAGMA busy_timeout=0")
        checkpoints = []
        writer = self.writer

        class CheckpointDuringValidation(sqlite3.Connection):
            def execute(connection, sql, *args, **kwargs):
                if sql == "PRAGMA quick_check":
                    writer.execute("INSERT INTO readings(payload) VALUES ('new live reading')")
                    writer.commit()
                    checkpoints.append(writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone())
                return super().execute(sql, *args, **kwargs)

        with patch.object(db_snapshot.sqlite3, "connect", side_effect=lambda *a, **k:
                          connect(*a, factory=CheckpointDuringValidation, **k)):
            self.snapshot()
        self.assertEqual(checkpoints, [(0, 0, 0)], "validation must not pin the live WAL")
        self.assertEqual(self.writer.execute("SELECT COUNT(*) FROM readings").fetchone()[0], 257)
        with connect(self.destination) as copied:
            self.assertEqual(copied.execute("SELECT COUNT(*) FROM readings").fetchone()[0], 256)
        copied.close()

    def test_copy_remains_consistent_while_live_data_changes(self):
        connect = sqlite3.connect
        self.writer.executemany("INSERT INTO readings(payload) VALUES (zeroblob(8192))", [()] * 512)
        self.writer.commit()
        writer = self.writer
        writes = []

        class ConcurrentWrite(sqlite3.Connection):
            def backup(connection, target, **kwargs):
                check = kwargs["progress"]
                def progress(status, remaining, total):
                    check(status, remaining, total)
                    if remaining and not writes:
                        writer.execute("INSERT INTO readings(payload) VALUES ('during backup')")
                        writer.commit()
                        writes.append(remaining)
                kwargs["progress"] = progress
                return super().backup(target, **kwargs)

        with patch.object(db_snapshot.sqlite3, "connect", side_effect=lambda *a, **k:
                          connect(*a, factory=ConcurrentWrite, **k)):
            self.snapshot()
        self.assertEqual(len(writes), 1)
        self.assertEqual(self.writer.execute("SELECT COUNT(*) FROM readings").fetchone()[0], 769)
        with connect(self.destination) as copied:
            self.assertEqual(copied.execute("SELECT COUNT(*) FROM readings").fetchone()[0], 768)
        copied.close()

    def test_failed_validation_keeps_previous_download(self):
        connect = sqlite3.connect

        class InvalidCopy(sqlite3.Connection):
            def execute(connection, sql, *args, **kwargs):
                if sql == "PRAGMA quick_check":
                    return SimpleNamespace(fetchall=lambda: [("damaged test copy",)])
                return super().execute(sql, *args, **kwargs)

        with patch.object(db_snapshot.sqlite3, "connect", side_effect=lambda *a, **k:
                          connect(*a, factory=InvalidCopy, **k)):
            with self.assertRaisesRegex(RuntimeError, "integrity check failed"):
                self.snapshot()
        self.assert_previous_preserved()

    def test_existing_download_reader_survives_atomic_publication(self):
        with self.destination.open("rb") as reader:
            self.snapshot()
            self.assertEqual(reader.read(), b"previous published download")
        self.assertTrue(self.destination.read_bytes().startswith(b"SQLite format 3"))


if __name__ == "__main__":
    unittest.main()
