from __future__ import annotations

from importlib import import_module


def test_collect_browser_sources_forwards_source_config(monkeypatch) -> None:
    module = import_module("propertyradar.browser_collector")
    source = import_module("propertyradar.models").Source(
        name="LH 공사",
        type="javascript",
        url="https://www.lh.or.kr/gallery.es?mid=a10502000000&bid=0003",
        config={"wait_for": ".board_list"},
    )
    captured: dict[str, object] = {}

    def fake_collect(*, sources, category, timeout, health_db_path):
        captured["sources"] = sources
        captured["category"] = category
        captured["timeout"] = timeout
        captured["health_db_path"] = health_db_path
        return [], []

    monkeypatch.setattr(module, "_BROWSER_COLLECTION_AVAILABLE", True)
    monkeypatch.setattr(module, "_core_collect", fake_collect)

    articles, errors = module.collect_browser_sources(
        [source],
        "property",
        timeout=20_000,
        health_db_path="data/radar_data.duckdb",
    )

    assert articles == []
    assert errors == []
    assert captured["category"] == "property"
    assert captured["timeout"] == 20_000
    assert captured["health_db_path"] == "data/radar_data.duckdb"
    assert captured["sources"] == [
        {
            "name": "LH 공사",
            "type": "javascript",
            "url": "https://www.lh.or.kr/gallery.es?mid=a10502000000&bid=0003",
            "config": {"wait_for": ".board_list"},
        }
    ]
