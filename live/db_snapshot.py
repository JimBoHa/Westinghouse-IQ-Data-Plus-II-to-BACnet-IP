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
import shutil
import signal
import sqlite3
import sys
import threading
import time
import uuid


class SnapshotTimeout(TimeoutError):
    """The cooperative copy/validation deadline expired."""


class SnapshotCancelled(RuntimeError):
    """SIGINT or SIGTERM requested a clean shutdown."""


class SnapshotBusy(RuntimeError):
    """Another snapshot or cleanup owns this destination."""


class SnapshotSpaceError(RuntimeError):
    """A copy would consume the reserved recording space."""


class Budget:
    def __init__(self, seconds):
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("Snapshot time budget must be finite and positive")
        self.started = time.monotonic()
        self.deadline = self.started + seconds
        self.signal_number = None
        self.space_check = None
        self.failure = None

    def check(self):
        if self.failure is not None:
            raise self.failure
        if self.signal_number is not None:
            raise SnapshotCancelled(f"Snapshot cancelled by {signal.Signals(self.signal_number).name}")
        if time.monotonic() >= self.deadline:
            raise SnapshotTimeout("Snapshot time budget expired; previous download retained")
        if self.space_check is not None:
            self.space_check()

    def sqlite_progress(self):
        try:
            self.check()
        except (SnapshotTimeout, SnapshotCancelled, SnapshotSpaceError) as error:
            self.failure = error
            return 1
        return 0


class SpaceGuard:
    def __init__(self, source, destination, reserve_bytes):
        if not isinstance(reserve_bytes, int) or reserve_bytes < 0:
            raise ValueError("Free-space reserve must be a nonnegative integer number of bytes")
        self.destination = destination.parent
        self.source = source.parent if source.stat().st_dev != self.destination.stat().st_dev else None
        self.reserve = reserve_bytes
        self.remaining_bytes = 0
        self.next_check = 0
        self.free_bytes = None

    def check(self, *, force=False):
        now = time.monotonic()
        if not force and now < self.next_check:
            return
        self.next_check = now + 1
        self.free_bytes = shutil.disk_usage(self.destination).free
        required = self.reserve + self.remaining_bytes
        if self.free_bytes < required:
            raise SnapshotSpaceError(f"Insufficient snapshot space: {self.free_bytes} bytes free; "
                                     f"{required} required including {self.reserve} reserved for recording")
        if self.source is not None and shutil.disk_usage(self.source).free < self.reserve:
            raise SnapshotSpaceError("Live database filesystem is below the recording-space reserve")


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def status_path(destination):
    return destination.with_name(destination.name + ".status.json")


def snapshot_status(destination):
    destination = Path(destination).resolve()
    try:
        report = json.loads(status_path(destination).read_text())
        if not isinstance(report, dict):
            raise ValueError("Status must be an object")
    except (FileNotFoundError, ValueError):
        report = {"state": "unknown", "snapshot": str(destination)}
    try:
        published = destination.stat()
    except FileNotFoundError:
        report.update(published_bytes=None, published_age_seconds=None)
    else:
        report.update(published_bytes=published.st_size,
                      published_age_seconds=max(0, time.time() - published.st_mtime))
    return report


def write_status(destination, report):
    path = status_path(destination)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(report, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


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
    status_temporary = Path(str(status_path(destination)) + ".tmp")
    if status_temporary.is_file() and not status_temporary.is_symlink() and status_temporary not in protected:
        status_temporary.unlink()
        removed.append(status_temporary.name)
    return sorted(removed)


def snapshot_paths(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError("Snapshot must not overwrite the live database")
    if source in (status_path(destination), Path(str(status_path(destination)) + ".tmp"),
                  destination.with_name(destination.name + ".lock")):
        raise ValueError("Snapshot metadata must not overwrite the live database")
    destination.parent.mkdir(parents=True, exist_ok=True)
    return source, destination


def cleanup_snapshot(source, destination):
    source, destination = snapshot_paths(source, destination)
    with destination_lock(destination):
        removed = cleanup_orphans(source, destination)
        report = snapshot_status(destination)
        if report.get("state") == "running":
            report.update(state="failed", finished_utc=utc(),
                          error="Interrupted snapshot recovered by cleanup",
                          service_result=os.environ.get("SERVICE_RESULT"))
            write_status(destination, report)
        return {"snapshot": str(destination), "removed": removed}


def snapshot(source, destination, *, max_seconds=1500, min_free_bytes=20 * 1024**3):
    budget = Budget(max_seconds)
    source, destination = snapshot_paths(source, destination)
    with destination_lock(destination), cancellation_signals(budget):
        removed = cleanup_orphans(source, destination)
        previous = snapshot_status(destination)
        report = {"snapshot": str(destination), "state": "running", "phase": "preflight",
                  "started_utc": utc(), "max_seconds": max_seconds,
                  "reserve_bytes": min_free_bytes, "orphan_files_removed": removed,
                  "last_success": previous.get("last_success")}
        try:
            budget.check()
            write_status(destination, report)
            space = SpaceGuard(source, destination, min_free_bytes)
            budget.space_check = space.check
            result = snapshot_locked(source, destination, budget, max_seconds, space, report)
            result["orphan_files_removed"] = removed
            report.update(state="ok", phase="finished", finished_utc=result["finished_utc"],
                          duration_seconds=result["duration_seconds"], last_success=result)
            write_status(destination, report)
            return result
        except Exception as error:
            report.update(state="failed", finished_utc=utc(), error=str(error),
                          error_type=type(error).__name__,
                          duration_seconds=time.monotonic() - budget.started)
            try:
                write_status(destination, report)
            except OSError as status_error:
                print(json.dumps({"status_write_error": str(status_error)}), file=sys.stderr, flush=True)
            raise


def snapshot_locked(source, destination, budget, max_seconds, space, report):
    temporary = destination.with_name(destination.name + ".tmp-" + uuid.uuid4().hex)
    try:
        budget.check()
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True,
                                     timeout=min(30, max_seconds))) as src:
            # Hold a fixed WAL read snapshot while new polls keep committing.
            # Otherwise a large incremental backup can restart after each write.
            src.execute("BEGIN")
            src.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            report["source_snapshot_utc"] = utc()
            page_size = src.execute("PRAGMA page_size").fetchone()[0]
            space.remaining_bytes = src.execute("PRAGMA page_count").fetchone()[0] * page_size
            space.check(force=True)
            report.update(phase="copying", source_bytes=space.remaining_bytes, free_bytes=space.free_bytes)
            write_status(destination, report)

            def copy_progress(_status, remaining, _total):
                space.remaining_bytes = remaining * page_size
                budget.check()

            dst = sqlite3.connect(temporary)
            try:
                src.backup(dst, pages=1024, sleep=0.01,
                           progress=copy_progress)
                budget.check()
                report.update(phase="validating", free_bytes=space.free_bytes)
                write_status(destination, report)
                dst.set_progress_handler(budget.sqlite_progress, 1000)
                dst.execute("PRAGMA journal_mode=DELETE")
                if dst.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise RuntimeError("Snapshot integrity check failed")
            finally:
                dst.close()
                src.execute("ROLLBACK")
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        space.check(force=True)
        budget.check()
        os.replace(temporary, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return {"snapshot": str(destination), "bytes": destination.stat().st_size,
                "finished_utc": utc(), "quick_check": "ok",
                "source_snapshot_utc": report["source_snapshot_utc"],
                "duration_seconds": time.monotonic() - budget.started}
    except sqlite3.OperationalError:
        # SQLite reports a progress-handler cancellation as "interrupted".
        budget.check()
        raise
    finally:
        for path in temporary_paths(temporary):
            path.unlink(missing_ok=True)


def reserve_gib(value):
    try:
        amount = float(value)
        if not math.isfinite(amount) or amount < 0:
            raise ValueError
        return int(amount * 1024**3)
    except (ValueError, OverflowError) as error:
        raise argparse.ArgumentTypeError("Reserve must be a finite, nonnegative GiB value") from error


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=float, default=1500,
                        help="Cooperative copy/validation budget (default: 1500 seconds)")
    parser.add_argument("--min-free-gib", type=reserve_gib, default=20 * 1024**3,
                        help="Free space reserved for recording (default: 20 GiB)")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--cleanup-only", action="store_true",
                        help="Remove abandoned temporary snapshots under the destination lock")
    action.add_argument("--status-only", action="store_true",
                        help="Show last result and current published snapshot age without copying")
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.status_only:
            result = snapshot_status(args.destination)
        elif args.cleanup_only:
            try:
                result = cleanup_snapshot(args.source, args.destination)
            except SnapshotBusy:
                result = {"cleanup_skipped": "destination is in use"}
        else:
            result = snapshot(args.source, args.destination, max_seconds=args.max_seconds,
                              min_free_bytes=args.min_free_gib)
    except Exception as error:
        print(json.dumps({"state": "failed", "error_type": type(error).__name__,
                          "error": str(error), "finished_utc": utc()}), file=sys.stderr, flush=True)
        return 1
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
