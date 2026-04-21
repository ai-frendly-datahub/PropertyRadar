from __future__ import annotations

import json
from datetime import UTC, datetime

from propertyradar.models import Article, CategoryConfig, Source
from propertyradar.quality_report import build_quality_report, write_quality_report


def _quality_config() -> dict[str, object]:
    return {
        "data_quality": {
            "quality_outputs": {
                "tracked_event_models": [
                    "transaction_record",
                    "presale_competition",
                    "listing_inventory",
                    "permit_completion",
                ]
            },
            "event_models": {
                "transaction_record": {
                    "required_fields": [
                        "region_code",
                        "complex_name",
                        "area",
                        "transaction_price",
                        "source_url",
                    ]
                },
                "listing_inventory": {
                    "required_fields": [
                        "region_code",
                        "property_type",
                        "listing_count",
                        "source_url",
                    ]
                },
            },
        },
        "source_backlog": {
            "operational_candidates": [
                {
                    "name": "MOLIT real estate transaction API",
                    "signal_type": "transaction_record",
                    "activation_gate": "API key and lawd code mapping",
                }
            ]
        },
    }


def test_build_quality_report_extracts_transaction_canonical_key() -> None:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    source = Source(
        name="MOLIT Trades",
        type="mcp",
        url="https://example.com/mcp",
        trust_tier="T1_official",
        content_type="price",
        config={"tool": "get_apartment_trades", "lawdCd": "11680"},
    )
    category = CategoryConfig(
        category_name="property",
        display_name="Property",
        sources=[source],
        entities=[],
    )
    article = Article(
        title="Gangnam apartment trade",
        link="https://example.com/trade/1",
        summary=(
            "Complex: Raemian. Area: 84.9. "
            "Transaction price: KRW 1200000000. Deal date: 2026-04-01."
        ),
        published=now,
        source="MOLIT Trades",
        category="property",
        matched_entities={"PropertyType": ["apartment"]},
    )

    report = build_quality_report(
        category=category,
        articles=[article],
        quality_config=_quality_config(),
        generated_at=now,
    )

    event = report["events"][0]
    assert event["event_model"] == "transaction_record"
    assert event["region_code"] == "11680"
    assert event["complex_name"] == "Raemian"
    assert event["area"] == 84.9
    assert event["transaction_price"] == 1200000000.0
    assert event["canonical_key"] == "property:11680:raemian:84-9"
    assert event["canonical_key_status"] == "complete"
    assert event["required_field_gaps"] == []
    assert report["summary"]["property_signal_event_count"] == 1
    assert report["summary"]["complete_canonical_key_count"] == 1
    assert any(
        item["reason"] == "source_backlog_pending"
        for item in report["daily_review_items"]
    )


def test_build_quality_report_flags_news_listing_proxy_gaps() -> None:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    source = Source(
        name="Realtor.com News",
        type="rss",
        url="https://www.realtor.com/news/feed/",
        trust_tier="T2_institutional",
        content_type="news",
    )
    category = CategoryConfig(
        category_name="property",
        display_name="Property",
        sources=[source],
        entities=[],
    )
    article = Article(
        title="Housing inventory tightened again",
        link="https://example.com/inventory",
        summary="Inventory fell as apartment listings stayed scarce.",
        published=now,
        source="Realtor.com News",
        category="property",
        matched_entities={"PropertyType": ["apartment"]},
    )

    report = build_quality_report(
        category=category,
        articles=[article],
        quality_config=_quality_config(),
        generated_at=now,
    )

    event = report["events"][0]
    assert event["event_model"] == "listing_inventory"
    assert event["canonical_key"] == "listing_inventory:source:realtor-com-news:apartment"
    assert event["canonical_key_status"] == "source_proxy"
    assert set(event["required_field_gaps"]) == {"region_code", "listing_count"}
    assert report["summary"]["news_proxy_event_count"] == 1
    assert report["summary"]["event_required_field_gap_count"] == 0
    assert not any(
        item["reason"] == "missing_required_fields"
        for item in report["daily_review_items"]
    )
    assert any(
        item["reason"] == "news_or_community_proxy_source"
        for item in report["daily_review_items"]
    )


def test_build_quality_report_avoids_broad_buy_price_as_transaction() -> None:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    source = Source(
        name="Realtor.com News",
        type="rss",
        url="https://www.realtor.com/news/feed/",
        trust_tier="T2_institutional",
        content_type="news",
    )
    category = CategoryConfig(
        category_name="property",
        display_name="Property",
        sources=[source],
        entities=[],
    )
    article = Article(
        title="Billionaire bunker buy reaches $170 million",
        link="https://example.com/buy",
        summary="Celebrity home purchase coverage without source-level deal fields.",
        published=now,
        source="Realtor.com News",
        category="property",
        matched_entities={"Market": ["buy"]},
    )

    report = build_quality_report(
        category=category,
        articles=[article],
        quality_config=_quality_config(),
        generated_at=now,
    )

    assert report["events"] == []


def test_build_quality_report_accepts_opt_in_source_context_event() -> None:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    source = Source(
        name="Apartment Trade Browser",
        type="javascript",
        url="https://example.com/housing",
        trust_tier="T3_professional",
        content_type="price",
        config={
            "event_model": "transaction_record",
            "allow_source_context_event": True,
            "region_code": "11680",
        },
    )
    category = CategoryConfig(
        category_name="property",
        display_name="Property",
        sources=[source],
        entities=[],
    )
    article = Article(
        title="Latest apartment trade dashboard",
        link="https://example.com/housing",
        summary="Gangnam apartment dashboard snapshot.",
        published=now,
        source="Apartment Trade Browser",
        category="property",
        matched_entities={"PropertyType": ["apartment"]},
    )

    report = build_quality_report(
        category=category,
        articles=[article],
        quality_config=_quality_config(),
        generated_at=now,
    )

    assert report["sources"][0]["status"] == "fresh"
    assert report["events"][0]["event_model"] == "transaction_record"
    assert report["events"][0]["signal_basis"] == "source_context_signal"
    assert report["summary"]["event_required_field_gap_count"] == 0


def test_build_quality_report_does_not_count_subway_line_as_listing_count() -> None:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    source = Source(
        name="Korean Property News",
        type="rss",
        url="https://example.com/feed",
        trust_tier="T2_institutional",
        content_type="news",
    )
    category = CategoryConfig(
        category_name="property",
        display_name="Property",
        sources=[source],
        entities=[],
    )
    article = Article(
        title="7호선 station area redevelopment",
        link="https://example.com/project",
        summary="Project supply includes 792가구 apartment units.",
        published=now,
        source="Korean Property News",
        category="property",
        matched_entities={"PropertyType": ["apartment"]},
    )

    report = build_quality_report(
        category=category,
        articles=[article],
        quality_config=_quality_config(),
        generated_at=now,
    )

    event = report["events"][0]
    assert event["event_model"] == "presale_competition"
    assert event["listing_count"] == 792


def test_write_quality_report_writes_latest_and_dated_json(tmp_path) -> None:
    report = {
        "category": "property",
        "generated_at": "2026-04-14T00:00:00+00:00",
        "summary": {},
        "sources": [],
        "events": [],
    }

    paths = write_quality_report(report, output_dir=tmp_path, category_name="property")

    assert paths["latest"].name == "property_quality.json"
    assert paths["dated"].name == "property_20260414_quality.json"
    assert json.loads(paths["latest"].read_text(encoding="utf-8"))["category"] == "property"
