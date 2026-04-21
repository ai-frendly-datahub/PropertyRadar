from __future__ import annotations

from pathlib import Path

import duckdb
from radar_core.exceptions import StorageError
from radar_core.storage import RadarStorage as CoreRadarStorage

from .date_storage import cleanup_date_directories, snapshot_database


class RadarStorage(CoreRadarStorage):
    def create_daily_snapshot(self, snapshot_dir: str | None = None):
        snapshot_root = self.db_path.parent / "snapshots"
        if snapshot_dir:
            snapshot_root = Path(snapshot_dir)
        _ = self.conn.execute("CHECKPOINT")
        self.conn.close()
        try:
            return snapshot_database(self.db_path, snapshot_root=snapshot_root)
        finally:
            self.conn = duckdb.connect(str(self.db_path))
            self._ensure_tables()

    def cleanup_old_snapshots(self, snapshot_dir: str | None = None, keep_days: int = 90) -> int:
        snapshot_root = self.db_path.parent / "snapshots"
        if snapshot_dir:
            snapshot_root = Path(snapshot_dir)
        return cleanup_date_directories(snapshot_root, keep_days=keep_days)

__all__ = ["RadarStorage", "StorageError"]
