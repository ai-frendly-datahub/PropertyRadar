from __future__ import annotations

from collections.abc import Iterable, Mapping
from html import escape
from pathlib import Path
from typing import Any

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

    report_path = _core_generate_report(
        category=category,
        articles=articles_list,
        output_path=output_path,
        stats=stats,
        errors=errors,
        plugin_charts=plugin_charts if plugin_charts else None,
    )
    if quality_report:
        _inject_property_quality_panel(report_path, quality_report)
    return report_path


def generate_index_html(
    report_dir: Path,
    summaries_dir: Path | None = None,
) -> Path:
    """Generate index.html (delegates to radar-core)."""
    radar_name = "Property Radar"
    return _core_generate_index_html(report_dir, radar_name)


def _inject_property_quality_panel(
    report_path: Path,
    quality_report: Mapping[str, Any],
) -> None:
    if not report_path.exists():
        return
    html = report_path.read_text(encoding="utf-8")
    panel = _render_property_quality_panel(quality_report)
    marker = "</body>"
    if marker in html:
        html = html.replace(marker, panel + "\n" + marker, 1)
    else:
        html += "\n" + panel
    report_path.write_text(html, encoding="utf-8")


def _render_property_quality_panel(quality_report: Mapping[str, Any]) -> str:
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
        "<div class=\"property-quality-card\">"
        f"<span>{escape(label)}</span><strong>{escape(str(value))}</strong>"
        "</div>"
        for label, value in cards
    )
    return f"""
<section id="property-quality" class="property-quality-panel">
  <style>
    .property-quality-panel {{ margin: 32px auto; max-width: 1180px; padding: 24px; border: 1px solid #d8dee4; border-radius: 8px; background: #fff; color: #24292f; }}
    .property-quality-panel h2 {{ margin: 0 0 8px; font-size: 1.35rem; }}
    .property-quality-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin: 16px 0 22px; }}
    .property-quality-card {{ border: 1px solid #d8dee4; border-radius: 8px; padding: 10px 12px; background: #f6f8fa; }}
    .property-quality-card span {{ display: block; font-size: .82rem; color: #57606a; }}
    .property-quality-card strong {{ display: block; margin-top: 4px; font-size: 1.2rem; }}
    .property-quality-table {{ width: 100%; border-collapse: collapse; margin-top: 12px; font-size: .9rem; }}
    .property-quality-table th, .property-quality-table td {{ border-top: 1px solid #d8dee4; padding: 8px; text-align: left; vertical-align: top; }}
    .property-quality-review {{ margin: 12px 0 0; padding-left: 18px; }}
  </style>
  <h2>Property Quality</h2>
  <p>Operational property evidence is separated from news or community proxy signals.</p>
  <div class="property-quality-grid">
    {cards_html}
  </div>
  {_render_quality_events(events)}
  {_render_quality_review(review_items)}
</section>
""".strip()


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
        "<h3>Observed Events</h3>"
        "<table class=\"property-quality-table\"><thead><tr>"
        "<th>Model</th><th>Source</th><th>Canonical key</th><th>Status</th><th>Gaps</th>"
        "</tr></thead><tbody>"
        + "\n".join(rows)
        + "</tbody></table>"
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
    return "<h3>Daily Review</h3><ul class=\"property-quality-review\">" + "\n".join(items) + "</ul>"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list_of_mappings(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]
