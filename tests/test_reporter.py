from __future__ import annotations

from datetime import datetime, timezone

from propertyradar.models import Article, CategoryConfig
from propertyradar.reporter import generate_index_html, generate_report


def _sample_category() -> CategoryConfig:
    return CategoryConfig(
        category_name="test",
        display_name="Test Radar",
        sources=[],
        entities=[],
    )


def _sample_articles() -> list[Article]:
    now = datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc)
    return [
        Article(
            title="테스트 부동산 기사",
            link="https://example.com/1",
            summary="테스트 요약",
            published=now,
            source="TestSource",
            category="test",
            matched_entities={"강남구": ["강남"]},
            collected_at=now,
        ),
    ]


def test_generate_report_creates_file(tmp_path, monkeypatch):
    """generate_report creates an HTML file at output_path."""
    fixed_now = datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr("radar_core.report_utils.datetime", FixedDateTime)

    output_path = tmp_path / "reports" / "test_report.html"
    result = generate_report(
        category=_sample_category(),
        articles=_sample_articles(),
        output_path=output_path,
        stats={"sources": 1, "collected": 1, "matched": 1, "window_days": 7},
    )
    assert result == output_path
    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_generate_report_html_content(tmp_path, monkeypatch):
    """Generated report contains expected HTML content."""
    fixed_now = datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr("radar_core.report_utils.datetime", FixedDateTime)

    output_path = tmp_path / "reports" / "test_report.html"
    generate_report(
        category=_sample_category(),
        articles=_sample_articles(),
        output_path=output_path,
        stats={"sources": 1, "collected": 1, "matched": 1, "window_days": 7},
    )
    html = output_path.read_text(encoding="utf-8")
    assert "Test Radar" in html
    assert "테스트 부동산 기사" in html


def test_generate_index_html(tmp_path):
    """generate_index_html creates index.html with report links."""
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "test_20240315.html").write_text("<html>test</html>", encoding="utf-8")

    index_path = generate_index_html(report_dir)
    assert index_path == report_dir / "index.html"
    assert index_path.exists()

    rendered = index_path.read_text(encoding="utf-8")
    assert "Property Radar" in rendered
    assert "test_20240315.html" in rendered


def test_generate_report_with_errors(tmp_path, monkeypatch):
    """generate_report includes error messages in HTML."""
    fixed_now = datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr("radar_core.report_utils.datetime", FixedDateTime)

    output_path = tmp_path / "reports" / "test_report.html"
    generate_report(
        category=_sample_category(),
        articles=_sample_articles(),
        output_path=output_path,
        stats={"sources": 1, "collected": 1, "matched": 1, "window_days": 7},
        errors=["소스 타임아웃"],
    )
    html = output_path.read_text(encoding="utf-8")
    assert "소스 타임아웃" in html
