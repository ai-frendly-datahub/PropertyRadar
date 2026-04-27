from __future__ import annotations

from collections.abc import Iterable, Mapping
from html import escape
from pathlib import Path
from typing import Any

from radar_core.ontology import build_summary_ontology_metadata
from radar_core.report_utils import (
    generate_index_html as _core_generate_index_html,
    generate_report as _core_generate_report,
)

from .models import Article, CategoryConfig


def generate_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    output_path: Path,
    stats: dict[str, int],
    errors: list[str] | None = None,
    store=None,
    quality_report: Mapping[str, Any] | None = None,
) -> Path:
    """Generate HTML report (delegates to radar-core)."""
    articles_list = list(articles)
    plugin_charts = []
    extra_sections: list[dict[str, Any]] = []

    # --- Universal plugins (entity heatmap + source reliability) ---
    try:
        from radar_core.plugins.entity_heatmap import get_chart_config as _heatmap_config

        _heatmap = _heatmap_config(articles=articles_list)
        if _heatmap is not None:
            plugin_charts.append(_heatmap)
    except Exception:
        pass
    try:
        from radar_core.plugins.source_reliability import get_chart_config as _reliability_config

        _reliability = _reliability_config(store=store)
        if _reliability is not None:
            plugin_charts.append(_reliability)
    except Exception:
        pass
    if quality_report:
        extra_sections.append(_build_property_quality_section(quality_report))

    return _core_generate_report(
        category=category,
        articles=articles_list,
        output_path=output_path,
        stats=stats,
        errors=errors,
        plugin_charts=plugin_charts if plugin_charts else None,
        extra_sections=extra_sections or None,
        ontology_metadata=build_summary_ontology_metadata(
            "PropertyRadar",
            category_name=category.category_name,
            search_from=Path(__file__).resolve(),
        ),
    )


def generate_index_html(
    report_dir: Path,
    summaries_dir: Path | None = None,
) -> Path:
    """Generate index.html (delegates to radar-core)."""
    radar_name = "Property Radar"
    return _core_generate_index_html(report_dir, radar_name)


def _build_property_quality_section(quality_report: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(quality_report.get("summary"))
    events = _list_of_mappings(quality_report.get("events"))
    review_items = _list_of_mappings(quality_report.get("daily_review_items"))
    cards = [
        ("Property signals", summary.get("property_signal_event_count", 0)),
        ("Transactions", summary.get("transaction_record_events", 0)),
        ("Presale", summary.get("presale_competition_events", 0)),
        ("Listings", summary.get("listing_inventory_events", 0)),
        ("Permits", summary.get("permit_completion_events", 0)),
        ("Required gaps", summary.get("event_required_field_gap_count", 0)),
        ("Proxy keys", summary.get("proxy_canonical_key_count", 0)),
        ("Review items", summary.get("daily_review_item_count", 0)),
    ]
    cards_html = "\n".join(
        "<div class=\"metric-card\">"
        f"<span>{escape(label)}</span><strong>{escape(str(value))}</strong>"
        "</div>"
        for label, value in cards
    )
    return {
        "id": "property-quality",
        "title": "Property Quality",
        "panel_title": "Operational Property Signal Coverage",
        "subtitle": "Operational property evidence is separated from news and community proxy signals.",
        "badges": ["property_quality.json", "transaction", "proxy-review"],
        "body_html": (
            f"<div class=\"metric-grid\">{cards_html}</div>"
            "<div><h3>Observed Events</h3>"
            f"{_render_quality_events(events)}"
            "</div>"
            "<div><h3>Daily Review</h3>"
            f"{_render_quality_review(review_items)}"
            "</div>"
        ),
    }


def _render_quality_events(events: list[Mapping[str, Any]]) -> str:
    if not events:
        return "<p>No property quality events were observed in this report window.</p>"
    rows = []
    for event in events[:10]:
        gaps = ", ".join(str(value) for value in event.get("required_field_gaps") or [])
        rows.append(
            "<tr>"
            f"<td>{escape(str(event.get('event_model') or ''))}</td>"
            f"<td>{escape(str(event.get('source') or ''))}</td>"
            f"<td>{escape(str(event.get('canonical_key') or ''))}</td>"
            f"<td>{escape(str(event.get('canonical_key_status') or ''))}</td>"
            f"<td>{escape(gaps)}</td>"
            "</tr>"
        )
    return (
        "<div style=\"overflow-x:auto;\">"
        "<table class=\"data-table\"><thead><tr>"
        "<th>Model</th><th>Source</th><th>Canonical key</th><th>Status</th><th>Gaps</th>"
        "</tr></thead><tbody>"
        + "\n".join(rows)
        + "</tbody></table></div>"
    )


def _render_quality_review(review_items: list[Mapping[str, Any]]) -> str:
    if not review_items:
        return "<p>No daily review items.</p>"
    items = []
    for item in review_items[:10]:
        label = item.get("source") or item.get("event_model") or item.get("signal_type") or ""
        items.append(
            "<li>"
            f"{escape(str(item.get('reason') or 'review'))}: {escape(str(label))}"
            "</li>"
        )
    return "<ul class=\"review-list\">" + "\n".join(items) + "</ul>"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list_of_mappings(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]
