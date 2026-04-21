from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import main as property_main
import propertyradar.date_storage as date_storage
import propertyradar.raw_logger as raw_logger
from propertyradar.models import Article


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 4, 12, 9, 30, tzinfo=tz or UTC)


def test_run_writes_dated_report_latest_copy_and_snapshot(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"database_path: {tmp_path / 'data' / 'radar_data.duckdb'}",
                f"report_dir: {tmp_path / 'reports'}",
                f"raw_data_dir: {tmp_path / 'data' / 'raw'}",
                f"search_db_path: {tmp_path / 'data' / 'search_index.db'}",
            ]
        ),
        encoding="utf-8",
    )
    categories_dir = tmp_path / "categories"
    categories_dir.mkdir()
    (categories_dir / "property.yaml").write_text(
        "\n".join(
            [
                "category_name: property",
                "display_name: Property",
                "sources:",
                "  - name: Test",
                "    type: rss",
                "    url: https://example.com/feed",
                "entities:",
                "  - name: Location",
                "    display_name: Location",
                "    keywords: [Seoul]",
            ]
        ),
        encoding="utf-8",
    )

    article = Article(
        title="Seoul apartment",
        link="https://example.com/1",
        summary="Seoul market update",
        published=FixedDateTime.now(UTC),
        source="Test",
        category="property",
        matched_entities={},
    )

    def fake_collect_sources(*args, **kwargs):
        return [article], []

    def fake_generate_report(*, output_path: Path, **kwargs):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("<html>report</html>", encoding="utf-8")
        (output_path.parent / "property_20260412_summary.json").write_text(
            json.dumps(
                {
                    "category": "property",
                    "date": "2026-04-12",
                    "generated_at": "2026-04-12T09:30:00+00:00",
                    "article_count": 1,
                    "source_count": 1,
                    "matched_count": 0,
                    "sources": ["Test"],
                    "top_entities": [],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return output_path

    monkeypatch.setattr(property_main, "datetime", FixedDateTime)
    monkeypatch.setattr(date_storage, "datetime", FixedDateTime)
    monkeypatch.setattr(raw_logger, "datetime", FixedDateTime)
    monkeypatch.setattr(property_main, "collect_sources", fake_collect_sources)
    monkeypatch.setattr(property_main, "generate_report", fake_generate_report)
    monkeypatch.setattr(property_main, "generate_index_html", lambda report_dir: report_dir / "index.html")

    report_path = property_main.run(
        category="property",
        config_path=config_path,
        categories_dir=categories_dir,
        snapshot_db=True,
    )

    assert report_path == tmp_path / "reports" / "property_report_20260412.html"
    assert report_path.exists()
    latest_path = tmp_path / "reports" / "property_report.html"
    assert latest_path.read_text(encoding="utf-8") == "<html>report</html>"
    summary_path = tmp_path / "reports" / "property_20260412_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert "quality_summary" in summary
    snapshot_path = tmp_path / "data" / "snapshots" / "2026-04-12" / "radar_data.duckdb"
    assert snapshot_path.exists()
    raw_path = tmp_path / "data" / "raw" / "2026-04-12" / "Test.jsonl"
    assert raw_path.exists()
    assert "Seoul apartment" in raw_path.read_text(encoding="utf-8")
