from __future__ import annotations

from datetime import date

from propertyradar.date_storage import (
    cleanup_date_directories,
    cleanup_dated_reports,
    snapshot_database,
)


def test_snapshot_database_uses_date_partition(tmp_path):
    db_path = tmp_path / "data" / "radar_data.duckdb"
    db_path.parent.mkdir(parents=True)
    db_path.write_text("duckdb", encoding="utf-8")

    snapshot = snapshot_database(
        db_path,
        snapshot_date=date(2026, 4, 12),
        snapshot_root=tmp_path / "data" / "snapshots",
    )

    assert snapshot == tmp_path / "data" / "snapshots" / "2026-04-12" / "radar_data.duckdb"
    assert snapshot.read_text(encoding="utf-8") == "duckdb"


def test_cleanup_date_directories_removes_only_expired_date_dirs(tmp_path):
    raw_dir = tmp_path / "raw"
    (raw_dir / "2026-01-01").mkdir(parents=True)
    (raw_dir / "2026-04-10").mkdir()
    (raw_dir / "latest").mkdir()

    removed = cleanup_date_directories(
        raw_dir,
        keep_days=30,
        today=date(2026, 4, 12),
    )

    assert removed == 1
    assert not (raw_dir / "2026-01-01").exists()
    assert (raw_dir / "2026-04-10").exists()
    assert (raw_dir / "latest").exists()


def test_cleanup_dated_reports_removes_only_expired_reports(tmp_path):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    (reports_dir / "property_report_20260101.html").write_text("old", encoding="utf-8")
    (reports_dir / "property_report_20260410.html").write_text("new", encoding="utf-8")
    (reports_dir / "property_report.html").write_text("latest", encoding="utf-8")
    (reports_dir / "index.html").write_text("index", encoding="utf-8")

    removed = cleanup_dated_reports(
        reports_dir,
        keep_days=30,
        today=date(2026, 4, 12),
    )

    assert removed == 1
    assert not (reports_dir / "property_report_20260101.html").exists()
    assert (reports_dir / "property_report_20260410.html").exists()
    assert (reports_dir / "property_report.html").exists()
    assert (reports_dir / "index.html").exists()
