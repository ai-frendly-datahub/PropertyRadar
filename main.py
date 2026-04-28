from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from radar_core.ontology import annotate_articles_with_ontology

from propertyradar.analyzer import apply_entity_rules
from propertyradar.collector import collect_sources
from propertyradar.config_loader import (
    load_category_config,
    load_category_quality_config,
    load_settings,
)
from propertyradar.date_storage import (
    cleanup_date_directories,
    cleanup_dated_reports,
    snapshot_database,
)
from propertyradar.logger import configure_logging, get_logger
from propertyradar.quality_report import build_quality_report, write_quality_report
from propertyradar.raw_logger import RawLogger
from propertyradar.reporter import generate_index_html, generate_report
from propertyradar.storage import RadarStorage


logger = get_logger(__name__)


def _daily_report_path(cycle_start: datetime, report_dir: Path, category: str) -> Path:
    stamp = cycle_start.astimezone(UTC).strftime("%Y%m%d")
    return report_dir / f"{category}_report_{stamp}.html"


def _summary_report_path(cycle_start: datetime, report_dir: Path, category: str) -> Path:
    stamp = cycle_start.astimezone(UTC).strftime("%Y%m%d")
    return report_dir / f"{category}_{stamp}_summary.json"


def _update_latest_report(report_path: Path, category: str) -> Path:
    latest_path = report_path.parent / f"{category}_report.html"
    if report_path != latest_path:
        shutil.copy2(report_path, latest_path)
    return latest_path


def _augment_summary_with_quality(
    summary_path: Path,
    quality_report: dict[str, object] | None,
) -> None:
    if not summary_path.exists() or not isinstance(quality_report, dict):
        return

    quality_summary = quality_report.get("summary")
    if not isinstance(quality_summary, dict) or not quality_summary:
        return

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    warnings = list(summary.get("warnings") or [])
    collection_errors = int(quality_summary.get("collection_error_count") or 0)
    stale_sources = int(quality_summary.get("stale_sources") or 0)
    missing_sources = int(quality_summary.get("missing_sources") or 0)

    if collection_errors:
        warnings.append(f"collection errors detected: {collection_errors}")
    if stale_sources or missing_sources:
        warnings.append(
            f"freshness gaps detected: stale={stale_sources}, missing={missing_sources}"
        )

    summary["quality_summary"] = quality_summary
    if warnings:
        summary["warnings"] = warnings

    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def run(
    *,
    category: str,
    config_path: Path | None = None,
    categories_dir: Path | None = None,
    per_source_limit: int = 30,
    recent_days: int = 7,
    timeout: int = 15,
    keep_days: int = 90,
    keep_raw_days: int = 180,
    keep_report_days: int = 90,
    snapshot_db: bool = False,
) -> Path:
    """Execute the lightweight collect -> analyze -> report pipeline."""
    configure_logging()
    cycle_start = datetime.now(UTC)
    settings = load_settings(config_path)
    category_cfg = load_category_config(category, categories_dir=categories_dir)
    quality_config = load_category_quality_config(category, categories_dir=categories_dir)

    logger.info(
        "pipeline_start",
        category=category_cfg.category_name,
        sources_count=len(category_cfg.sources),
    )
    collected, errors = collect_sources(
        category_cfg.sources,
        category=category_cfg.category_name,
        limit_per_source=per_source_limit,
        timeout=timeout,
    )

    raw_logger = RawLogger(settings.raw_data_dir)
    for source in category_cfg.sources:
        source_articles = [article for article in collected if article.source == source.name]
        if source_articles:
            _ = raw_logger.log(source_articles, source_name=source.name)

    analyzed = apply_entity_rules(collected, category_cfg.entities)
    analyzed = annotate_articles_with_ontology(
        analyzed,
        repo_name="PropertyRadar",
        sources_by_name={source.name: source for source in category_cfg.sources},
        category_name=category_cfg.category_name,
        search_from=Path(__file__),
        attach_event_model_payload=True,
    )

    with RadarStorage(settings.database_path) as storage:
        storage.upsert_articles(analyzed)
        _ = storage.delete_older_than(keep_days)
        recent_articles = storage.recent_articles(category_cfg.category_name, days=recent_days)
        quality_articles = storage.recent_articles(
            category_cfg.category_name,
            days=max(recent_days, 14),
            limit=max(500, per_source_limit * max(len(category_cfg.sources), 1) * 2),
        )

    snapshot_path = (
        snapshot_database(
            settings.database_path,
            snapshot_root=settings.database_path.parent / "snapshots",
        )
        if snapshot_db
        else None
    )
    raw_removed = cleanup_date_directories(settings.raw_data_dir, keep_days=keep_raw_days)
    report_removed = cleanup_dated_reports(settings.report_dir, keep_days=keep_report_days)
    logger.info(
        "date_storage_complete",
        snapshot_path=str(snapshot_path) if snapshot_path else None,
        raw_removed=raw_removed,
        report_removed=report_removed,
    )

    matched_count = sum(1 for a in collected if a.matched_entities)
    logger.info(
        "collection_complete",
        collected_count=len(collected),
        errors_count=len(errors),
    )
    logger.info("analysis_complete", matched_count=matched_count)

    stats = {
        "sources": len(category_cfg.sources),
        "collected": len(collected),
        "matched": matched_count,
        "window_days": recent_days,
    }
    quality_report = build_quality_report(
        category=category_cfg,
        articles=quality_articles,
        errors=errors,
        quality_config=quality_config,
        generated_at=cycle_start,
    )

    output_path = _daily_report_path(cycle_start, settings.report_dir, category_cfg.category_name)
    report_path = generate_report(
        category=category_cfg,
        articles=recent_articles,
        output_path=output_path,
        stats=stats,
        errors=errors,
        quality_report=quality_report,
    )
    latest_path = _update_latest_report(report_path, category_cfg.category_name)
    summary_path = _summary_report_path(cycle_start, settings.report_dir, category_cfg.category_name)
    _augment_summary_with_quality(summary_path, quality_report)
    quality_paths = write_quality_report(
        quality_report,
        output_dir=settings.report_dir,
        category_name=category_cfg.category_name,
    )
    logger.info("report_generated", output_path=str(report_path), latest_path=str(latest_path))
    logger.info("quality_report_generated", output_path=str(quality_paths["latest"]))
    generate_index_html(settings.report_dir)
    if errors:
        logger.warning("collection_errors", errors_count=len(errors))

    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PropertyRadar - Korean real estate news collector"
    )
    _ = parser.add_argument(
        "--category",
        required=True,
        help="Category name matching a YAML in config/categories/",
    )
    _ = parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config/config.yaml (optional)",
    )
    _ = parser.add_argument(
        "--categories-dir",
        type=Path,
        default=None,
        help="Custom directory for category YAML files",
    )
    _ = parser.add_argument(
        "--per-source-limit",
        type=int,
        default=30,
        help="Max items to pull from each source",
    )
    _ = parser.add_argument(
        "--recent-days", type=int, default=7, help="Window (days) to show in the report"
    )
    _ = parser.add_argument(
        "--timeout", type=int, default=15, help="HTTP timeout per request (seconds)"
    )
    _ = parser.add_argument(
        "--keep-days", type=int, default=90, help="Retention window for stored items"
    )
    _ = parser.add_argument(
        "--keep-raw-days",
        type=int,
        default=180,
        help="Retention window for date-partitioned raw data directories",
    )
    _ = parser.add_argument(
        "--keep-report-days",
        type=int,
        default=90,
        help="Retention window for dated HTML reports",
    )
    _ = parser.add_argument(
        "--snapshot-db",
        action="store_true",
        help="Copy the DuckDB file to data/snapshots/YYYY-MM-DD after collection",
    )
    return parser.parse_args()


def _to_path(value: object) -> Path | None:
    if isinstance(value, Path):
        return value
    return None


def _to_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _to_bool(value: object) -> bool:
    return bool(value) if isinstance(value, bool) else False


if __name__ == "__main__":
    args = cast(dict[str, object], vars(parse_args()))
    _ = run(
        category=str(args.get("category", "")),
        config_path=_to_path(args.get("config")),
        categories_dir=_to_path(args.get("categories_dir")),
        per_source_limit=_to_int(args.get("per_source_limit"), 30),
        recent_days=_to_int(args.get("recent_days"), 7),
        timeout=_to_int(args.get("timeout"), 15),
        keep_days=_to_int(args.get("keep_days"), 90),
        keep_raw_days=_to_int(args.get("keep_raw_days"), 180),
        keep_report_days=_to_int(args.get("keep_report_days"), 90),
        snapshot_db=_to_bool(args.get("snapshot_db")),
    )
