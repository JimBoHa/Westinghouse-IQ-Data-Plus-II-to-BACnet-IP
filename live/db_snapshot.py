#!/usr/bin/env python3
"""Make a consistent, standalone SQLite copy while acquisition keeps running."""
import argparse
from contextlib import closing, contextmanager
import datetime as dt
import fcntl
import json
import math
import os
from pathlib import Path
import re
import signal
import sqlite3
import threading
import time
import uuid


class SnapshotTimeout(TimeoutError):
    """The cooperative copy/validation deadline expired."""


class SnapshotCancelled(RuntimeError):
    """SIGINT or SIGTERM requested a clean shutdown."""


class SnapshotBusy(RuntimeError):
    """Another snapshot or cleanup owns this destination."""


class Budget:
    def __init__(self, seconds):
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("Snapshot time budget must be finite and positive")
        self.started = time.monotonic()
        self.deadline = self.started + seconds
        self.signal_number = None

    def check(self):
        if self.signal_number is not None:
            raise SnapshotCancelled(f"Snapshot cancelled by {signal.Signals(self.signal_number).name}")
        if time.monotonic() >= self.deadline:
            raise SnapshotTimeout("Snapshot time budget expired; previous download retained")

    def sqlite_progress(self):
        try:
            self.check()
        except (SnapshotTimeout, SnapshotCancelled):
            return 1
        return 0


@contextmanager
def cancellation_signals(budget):
    # Library callers in worker threads cannot install process signal handlers.
    previous = {}
    if threading.current_thread() is threading.main_thread():
        def cancel(number, _frame):
            # Defer exceptions to a safe point, including SQLite callbacks.
            budget.signal_number = number
        for number in (signal.SIGTERM, signal.SIGINT):
            previous[number] = signal.signal(number, cancel)
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


@contextmanager
def destination_lock(destination):
    lock_path = destination.with_name(destination.name + ".lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotBusy(f"Snapshot destination is already in use: {destination}") from error
        yield
    finally:
        # Keep the lock inode: unlinking it would allow two independent locks.
        os.close(descriptor)


def temporary_paths(path):
    return (path, *(Path(str(path) + suffix) for suffix in ("-journal", "-wal", "-shm")))


def cleanup_orphans(source, destination):
    """Called only under destination_lock; never touch live or published files."""
    owned = re.compile(re.escape(destination.name) + r"\.tmp-[0-9a-f]{32}(?:-(?:journal|wal|shm))?")
    protected = set(temporary_paths(source))
    removed = []
    with os.scandir(destination.parent) as entries:
        for entry in entries:
            path = Path(entry.path)
            if (owned.fullmatch(entry.name) and entry.is_file(follow_symlinks=False)
                    and path not in protected):
                path.unlink()
                removed.append(entry.name)
    return sorted(removed)


def snapshot_paths(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError("Snapshot must not overwrite the live database")
    destination.parent.mkdir(parents=True, exist_ok=True)
    return source, destination


def cleanup_snapshot(source, destination):
    source, destination = snapshot_paths(source, destination)
    with destination_lock(destination):
        return {"snapshot": str(destination), "removed": cleanup_orphans(source, destination)}


def snapshot(source, destination, *, max_seconds=1500):
    budget = Budget(max_seconds)
    source, destination = snapshot_paths(source, destination)
    with destination_lock(destination), cancellation_signals(budget):
        budget.check()
        removed = cleanup_orphans(source, destination)
        result = snapshot_locked(source, destination, budget, max_seconds)
        result["orphan_files_removed"] = removed
        return result


def snapshot_locked(source, destination, budget, max_seconds):
    temporary = destination.with_name(destination.name + ".tmp-" + uuid.uuid4().hex)
    try:
        budget.check()
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True,
                                     timeout=min(30, max_seconds))) as src:
            # Hold a fixed WAL read snapshot while new polls keep committing.
            # Otherwise a large incremental backup can restart after each write.
            src.execute("BEGIN")
            src.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            dst = sqlite3.connect(temporary)
            try:
                src.backup(dst, pages=1024, sleep=0.01,
                           progress=lambda *_: budget.check())
                budget.check()
                dst.set_progress_handler(budget.sqlite_progress, 1000)
                dst.execute("PRAGMA journal_mode=DELETE")
                if dst.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise RuntimeError("Snapshot integrity check failed")
            finally:
                dst.close()
                src.execute("ROLLBACK")
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        budget.check()
        os.replace(temporary, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return {"snapshot": str(destination), "bytes": destination.stat().st_size,
                "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "quick_check": "ok",
                "duration_seconds": time.monotonic() - budget.started}
    except sqlite3.OperationalError:
        # SQLite reports a progress-handler cancellation as "interrupted".
        budget.check()
        raise
    finally:
        for path in temporary_paths(temporary):
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=float, default=1500,
                        help="Cooperative copy/validation budget (default: 1500 seconds)")
    parser.add_argument("--cleanup-only", action="store_true",
                        help="Remove abandoned temporary snapshots under the destination lock")
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.cleanup_only:
        try:
            result = cleanup_snapshot(args.source, args.destination)
        except SnapshotBusy:
            result = {"cleanup_skipped": "destination is in use"}
    else:
        result = snapshot(args.source, args.destination, max_seconds=args.max_seconds)
    print(json.dumps(result))
