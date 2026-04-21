from __future__ import annotations

import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path


def snapshot_database(
    db_path: Path,
    *,
    snapshot_date: date | None = None,
    snapshot_root: Path | None = None,
) -> Path | None:
    """Copy the current DuckDB file into a date-partitioned snapshot directory."""
    if not db_path.exists():
        return None

    target_date = snapshot_date or datetime.now(UTC).date()
    target_root = snapshot_root or db_path.parent / "snapshots"
    target_dir = target_root / target_date.isoformat()
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / db_path.name
    shutil.copy2(db_path, target_path)
    return target_path


def cleanup_date_directories(
    base_dir: Path, *, keep_days: int, today: date | None = None
) -> int:
    if keep_days < 0 or not base_dir.exists():
        return 0

    cutoff = (today or datetime.now(UTC).date()) - timedelta(days=keep_days)
    removed = 0
    for child in base_dir.iterdir():
        if not child.is_dir():
            continue
        try:
            child_date = date.fromisoformat(child.name)
        except ValueError:
            continue

        if child_date < cutoff:
            shutil.rmtree(child)
            removed += 1
    return removed


def cleanup_dated_reports(
    report_dir: Path, *, keep_days: int, today: date | None = None
) -> int:
    if keep_days < 0 or not report_dir.exists():
        return 0

    cutoff = (today or datetime.now(UTC).date()) - timedelta(days=keep_days)
    removed = 0
    for html_file in report_dir.glob("*.html"):
        if html_file.name == "index.html":
            continue

        stem = html_file.stem
        if len(stem) < 8 or not stem[-8:].isdigit():
            continue

        try:
            stamp = date.fromisoformat(f"{stem[-8:-4]}-{stem[-4:-2]}-{stem[-2:]}")
        except ValueError:
            continue

        if stamp < cutoff:
            html_file.unlink()
            removed += 1
    return removed
