from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock, patch

from propertyradar.collector import _collect_single, collect_sources
from propertyradar.models import Article, Source


def _article(source: str) -> Article:
    return Article(
        title=f"{source} article",
        link=f"https://example.com/{source}",
        summary="summary",
        published=datetime(2026, 4, 21, tzinfo=UTC),
        source=source,
        category="property",
    )


def test_collect_sources_routes_rss_and_browser_and_skips_disabled_sources() -> None:
    active_rss = Source(name="RSS", type="rss", url="https://example.com/feed")
    active_js = Source(
        name="JS",
        type="javascript",
        url="https://example.com/app",
        config={"wait_for": "body"},
    )
    disabled = Source(name="Disabled", type="rss", url="https://example.com/off", enabled=False)
    health_disabled = Source(
        name="HealthDisabled",
        type="rss",
        url="https://example.com/health",
    )

    mock_breaker = Mock()
    mock_breaker.call.side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
    mock_manager = Mock()
    mock_manager.get_breaker.return_value = mock_breaker
    fake_session = Mock()
    fake_health = Mock()
    fake_health.is_disabled.side_effect = lambda name: name == "HealthDisabled"

    with (
        patch("propertyradar.collector.get_circuit_breaker_manager", return_value=mock_manager),
        patch("propertyradar.collector._create_session", return_value=fake_session),
        patch("propertyradar.collector.CrawlHealthStore", return_value=fake_health),
        patch("propertyradar.collector._collect_single", return_value=[_article("RSS")]) as mock_single,
        patch(
            "propertyradar.collector.collect_browser_sources",
            return_value=([_article("JS")], []),
        ) as mock_browser,
    ):
        articles, errors = collect_sources(
            [active_rss, active_js, disabled, health_disabled],
            category="property",
            max_workers=1,
        )

    assert [article.source for article in articles] == ["RSS", "JS"]
    assert errors == []
    assert mock_single.call_count == 1
    mock_browser.assert_called_once()
    browser_sources = mock_browser.call_args.args[0]
    assert [source.name for source in browser_sources] == ["JS"]


def test_collect_single_falls_back_to_source_url_for_invalid_entry_id() -> None:
    source = Source(name="Fallback Feed", type="rss", url="https://example.com/feed")
    mock_response = Mock()
    mock_response.content = b"""<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Fallback title</title>
      <guid>invalid-guid</guid>
    </item>
  </channel>
</rss>"""
    mock_response.raise_for_status = Mock()
    mock_response.headers = {}

    with patch("propertyradar.collector._fetch_url_with_retry", return_value=mock_response):
        articles = _collect_single(source, category="property", limit=5, timeout=5)

    assert len(articles) == 1
    assert articles[0].title == "Fallback title"
    assert articles[0].summary == "Fallback title"
    assert articles[0].link == "https://example.com/feed"
