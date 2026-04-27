from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from propertyradar.models import Article
from propertyradar.storage import RadarStorage


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "check_quality.py"
    spec = importlib.util.spec_from_file_location("propertyradar_check_quality_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generate_quality_artifacts_uses_latest_stored_checkpoint(
    tmp_path: Path,
    capsys,
) -> None:
    project_root = tmp_path
    (project_root / "config" / "categories").mkdir(parents=True)

    (project_root / "config" / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "database_path": "data/radar_data.duckdb",
                "report_dir": "reports",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (project_root / "config" / "categories" / "property.yaml").write_text(
        yaml.safe_dump(
            {
                "category_name": "property",
                "display_name": "Property Radar",
                "sources": [
                    {
                        "id": "molit_trades",
                        "name": "MOLIT Trades",
                        "type": "mcp",
                        "url": "https://example.com/property",
                        "content_type": "price",
                        "trust_tier": "T1_official",
                        "enabled": True,
                        "config": {
                            "event_model": "transaction_record",
                            "allow_source_context_event": True,
                            "region_code": "11680",
                            "freshness_sla_days": 7,
                        },
                    }
                ],
                "entities": [],
                "data_quality": {
                    "quality_outputs": {
                        "tracked_event_models": ["transaction_record"],
                    }
                },
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    article_time = datetime.now(UTC) - timedelta(days=30)
    db_path = project_root / "data" / "radar_data.duckdb"
    with RadarStorage(db_path) as storage:
        storage.upsert_articles(
            [
                Article(
                    title="Gangnam apartment trade",
                    link="https://example.com/property/1",
                    summary=(
                        "Complex: Raemian. Area: 84.9. "
                        "Transaction price: KRW 1200000000. Deal date: 2026-04-01."
                    ),
                    published=article_time,
                    collected_at=article_time,
                    source="MOLIT Trades",
                    category="property",
                    matched_entities={"PropertyType": ["apartment"]},
                )
            ]
        )

    module = _load_script_module()
    paths, report = module.generate_quality_artifacts(project_root)

    assert Path(paths["latest"]).exists()
    assert Path(paths["dated"]).exists()
    assert report["summary"]["tracked_sources"] == 1
    assert report["summary"]["transaction_record_events"] == 1

    module.PROJECT_ROOT = project_root
    module.main()
    captured = capsys.readouterr()
    assert "quality_report=" in captured.out
    assert "tracked_sources=1" in captured.out
    assert "property_signal_event_count=1" in captured.out
