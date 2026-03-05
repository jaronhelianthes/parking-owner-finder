# pipeline/output_writer.py
"""
Centralised CSV output writer.

Two usage patterns:

1. Single-row (quick_test):
       from pipeline.output_writer import write_result
       write_result(row_dict, output_path)

2. Streaming / incremental (runner) — keeps the file handle open across
   the whole run so each row is flushed to disk immediately. A crash mid-run
   will never lose a row that already completed.

       from pipeline.output_writer import OutputWriter
       with OutputWriter(output_path, fieldnames) as writer:
           writer.write(row_dict)   # flushes after every row
"""
import csv
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_newline(path: Path) -> None:
    """Guarantee the file ends with a newline so appended rows land on their
    own line. No-op if the file is empty or does not exist."""
    if not path.exists() or path.stat().st_size == 0:
        return
    with open(path, "rb+") as f:
        f.seek(-1, os.SEEK_END)
        if f.read(1) != b"\n":
            f.write(b"\n")


def _ensure_header(path: Path, fieldnames: list) -> None:
    """Write a header row if the file is missing or empty.
    If the file exists but has no header, prepend one."""
    if not path.exists() or path.stat().st_size == 0:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        return

    with open(path, newline="", encoding="utf-8") as f:
        first_line = f.readline().strip()

    if not first_line.startswith("property_id"):
        existing = path.read_text(encoding="utf-8")
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()
            f.write(existing)


def load_processed_ids(output_path: str) -> set:
    """Return the set of property_id strings already present in the output CSV."""
    path = Path(output_path)
    if not path.exists():
        return set()
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return {r["property_id"] for r in csv.DictReader(f) if r.get("property_id")}
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Pattern 1 — single-row write (used by quick_test)
# ---------------------------------------------------------------------------

def write_result(row_dict: dict, output_path: str) -> bool:
    """Append a single result row to the output CSV.

    Returns True if the row was written, False if it was already present.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pid = str(row_dict.get("property_id", ""))
    fieldnames = list(row_dict.keys())

    _ensure_header(path, fieldnames)

    # Check for duplicates before appending
    with open(path, newline="", encoding="utf-8") as f:
        existing_ids = {r.get("property_id") for r in csv.DictReader(f)}
    if pid in existing_ids:
        return False

    _ensure_newline(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writerow(row_dict)

    return True


# ---------------------------------------------------------------------------
# Pattern 2 — streaming writer context manager (used by runner)
# ---------------------------------------------------------------------------

class OutputWriter:
    """Context manager that keeps the output file open across many rows.

    Each call to .write() flushes immediately so no completed row is ever
    lost if the process crashes.

    Usage:
        with OutputWriter(output_path, fieldnames) as writer:
            for row_dict in results:
                writer.write(row_dict)
    """

    def __init__(self, output_path: str, fieldnames: list):
        self.path = Path(output_path)
        self.fieldnames = fieldnames
        self._fh = None
        self._writer = None
        self._processed_ids: set = set()

    def __enter__(self) -> "OutputWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _ensure_header(self.path, self.fieldnames)
        _ensure_newline(self.path)
        self._processed_ids = load_processed_ids(str(self.path))
        self._fh = open(self.path, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=self.fieldnames)
        return self

    def write(self, row_dict: dict) -> bool:
        """Write a row and flush immediately.

        Returns True if written, False if already present (skipped).
        """
        pid = str(row_dict.get("property_id", ""))
        if pid in self._processed_ids:
            return False
        self._writer.writerow(row_dict)
        self._fh.flush()
        self._processed_ids.add(pid)
        return True

    def __exit__(self, *_) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None