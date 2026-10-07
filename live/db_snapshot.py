#!/usr/bin/env python3
"""Make a consistent, standalone SQLite copy while acquisition keeps running."""
import argparse
from contextlib import closing
import datetime as dt
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid


class SnapshotTimeout(TimeoutError):
    """The cooperative copy/validation deadline expired."""


class Budget:
    def __init__(self, seconds):
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("Snapshot time budget must be finite and positive")
        self.started = time.monotonic()
        self.deadline = self.started + seconds

    def check(self):
        if time.monotonic() >= self.deadline:
            raise SnapshotTimeout("Snapshot time budget expired; previous download retained")

    def sqlite_progress(self):
        return int(time.monotonic() >= self.deadline)


def snapshot(source, destination, *, max_seconds=1500):
    budget = Budget(max_seconds)
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError("Snapshot must not overwrite the live database")
    destination.parent.mkdir(parents=True, exist_ok=True)
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
        for path in (temporary, *(Path(str(temporary) + suffix)
                                  for suffix in ("-journal", "-wal", "-shm"))):
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=float, default=1500,
                        help="Cooperative copy/validation budget (default: 1500 seconds)")
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    print(json.dumps(snapshot(args.source, args.destination, max_seconds=args.max_seconds)))
