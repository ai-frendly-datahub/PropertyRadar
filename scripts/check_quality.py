#!/usr/bin/env python3
"""Run DuckDB checks and write PropertyRadar quality JSON."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from propertyradar.config_loader import (  # noqa: E402
    load_category_config,
    load_category_quality_config,
    load_settings,
)
from propertyradar.quality_report import build_quality_report, write_quality_report  # noqa: E402
from propertyradar.storage import RadarStorage  # noqa: E402


CATEGORY_NAME = "property"


def main() -> None:
    settings = load_settings()
    category = load_category_config(CATEGORY_NAME)
    quality_config = load_category_quality_config(CATEGORY_NAME)

    if not settings.database_path.exists():
        print(f"Database not found: {settings.database_path}")
        sys.exit(1)

    _run_db_checks(settings.database_path)
    with RadarStorage(settings.database_path) as storage:
        articles = storage.recent_articles(
            category.category_name,
            days=14,
            limit=max(500, len(category.sources) * 20),
        )

    report = build_quality_report(
        category=category,
        articles=articles,
        errors=[],
        quality_config=quality_config,
    )
    paths = write_quality_report(
        report,
        output_dir=settings.report_dir,
        category_name=category.category_name,
    )
    _print_summary(report, article_count=len(articles), latest_path=paths["latest"])


def _run_db_checks(db_path: Path) -> None:
    with duckdb.connect(str(db_path), read_only=True) as con:
        total = con.execute("SELECT COUNT(*) FROM articles").fetchone()
        total_count = int(total[0] if total else 0)
        print(f"Total records: {total_count}")

        print("\n=== Missing Field Check ===\n")
        null_conditions = {
            "title": "title IS NULL OR title = ''",
            "link": "link IS NULL OR link = ''",
            "summary": "summary IS NULL OR summary = ''",
            "published": "published IS NULL",
        }
        for field, condition in null_conditions.items():
            missing = con.execute(f"SELECT COUNT(*) FROM articles WHERE {condition}").fetchone()
            missing_count = int(missing[0] if missing else 0)
            rate = (missing_count / total_count * 100.0) if total_count else 0.0
            print(f"  {field}: {missing_count} / {total_count} ({rate:.1f}%)")

        duplicates = con.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT link
                FROM articles
                GROUP BY link
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
        print("\n=== Duplicate URL Check ===\n")
        print(f"  duplicate links: {int(duplicates[0] if duplicates else 0)}")

        date_row = con.execute(
            """
            SELECT
                MIN(COALESCE(published, collected_at)),
                MAX(COALESCE(published, collected_at)),
                SUM(CASE WHEN published > CURRENT_TIMESTAMP THEN 1 ELSE 0 END)
            FROM articles
            """
        ).fetchone()
        print("\n=== Date Check ===\n")
        print(f"  oldest: {_row_value(date_row, 0)}")
        print(f"  newest: {_row_value(date_row, 1)}")
        print(f"  future dates: {_row_value(date_row, 2) or 0}")


def _print_summary(report: dict[str, Any], *, article_count: int, latest_path: Path) -> None:
    summary = report.get("summary", {})
    if not isinstance(summary, dict):
        return
    print(f"scoped_articles={article_count}")
    print(f"quality_report={latest_path}")
    print(f"tracked_sources={summary.get('tracked_sources', 0)}")
    print(f"fresh_sources={summary.get('fresh_sources', 0)}")
    print(f"stale_sources={summary.get('stale_sources', 0)}")
    print(f"missing_sources={summary.get('missing_sources', 0)}")
    print(f"not_tracked_sources={summary.get('not_tracked_sources', 0)}")


def _row_value(row: object, index: int) -> object:
    if isinstance(row, tuple) and len(row) > index:
        return row[index]
    return None


if __name__ == "__main__":
    main()
