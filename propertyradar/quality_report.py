from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import Article, CategoryConfig, Source


TRACKED_EVENT_MODEL_ORDER = [
    "transaction_record",
    "presale_competition",
    "listing_inventory",
    "permit_completion",
]
TRACKED_EVENT_MODELS = set(TRACKED_EVENT_MODEL_ORDER)
SUMMARY_LABELS = [
    "Region code",
    "LAWD code",
    "lawdCd",
    "Region",
    "Complex",
    "Complex name",
    "Apartment",
    "Area",
    "excluUseAr",
    "Transaction price",
    "Deal amount",
    "Price",
    "Deal date",
    "Report date",
    "Project ID",
    "Project",
    "Project name",
    "Competition rate",
    "Property type",
    "Listing count",
    "Permit date",
    "Completion date",
]
REGION_CODE_HINTS = {
    "seoul": "11000",
    "서울": "11000",
    "gangnam": "11680",
    "강남": "11680",
    "seocho": "11650",
    "서초": "11650",
    "songpa": "11710",
    "송파": "11710",
    "mapo": "11440",
    "마포": "11440",
    "bundang": "41135",
    "분당": "41135",
    "incheon": "28000",
    "인천": "28000",
    "busan": "26000",
    "부산": "26000",
}
PROPERTY_TYPE_HINTS = {
    "apartment": "apartment",
    "apartments": "apartment",
    "apt": "apartment",
    "아파트": "apartment",
    "condo": "condo",
    "villa": "villa",
    "빌라": "villa",
    "home": "home",
    "housing": "housing",
    "주택": "housing",
    "commercial": "commercial",
    "office": "office",
    "오피스": "office",
    "land": "land",
    "토지": "land",
}


def build_quality_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    errors: Iterable[str] | None = None,
    quality_config: Mapping[str, object] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _as_utc(generated_at or datetime.now(UTC))
    article_rows = list(articles)
    error_rows = [str(error) for error in (errors or [])]
    quality = _dict(quality_config or {}, "data_quality")
    event_model_config = _dict(quality, "event_models")
    tracked_models = _tracked_event_models(quality)

    events = _build_events(
        articles=article_rows,
        sources=category.sources,
        tracked_models=tracked_models,
        event_model_config=event_model_config,
    )
    source_rows = [
        _build_source_row(
            source=source,
            articles=article_rows,
            events=events,
            errors=error_rows,
            quality=quality,
            tracked_models=tracked_models,
            generated_at=generated,
        )
        for source in category.sources
    ]

    status_counts = Counter(str(row["status"]) for row in source_rows)
    event_counts = Counter(str(row["event_model"]) for row in events)
    summary: dict[str, Any] = {
        "total_sources": len(source_rows),
        "enabled_sources": sum(1 for row in source_rows if row["enabled"]),
        "tracked_sources": sum(1 for row in source_rows if row["tracked"]),
        "fresh_sources": status_counts.get("fresh", 0),
        "stale_sources": status_counts.get("stale", 0),
        "missing_sources": status_counts.get("missing", 0),
        "missing_event_sources": status_counts.get("missing_event", 0),
        "unknown_event_date_sources": status_counts.get("unknown_event_date", 0),
        "not_tracked_sources": status_counts.get("not_tracked", 0),
        "skipped_disabled_sources": status_counts.get("skipped_disabled", 0),
        "collection_error_count": len(error_rows),
    }
    for event_model in TRACKED_EVENT_MODEL_ORDER:
        summary[f"{event_model}_events"] = event_counts.get(event_model, 0)
    summary.update(
        _event_quality_summary(
            events=events,
            source_rows=source_rows,
            quality_config=quality_config or {},
            tracked_models=tracked_models,
        )
    )
    daily_review_items = _daily_review_items(
        events=events,
        source_rows=source_rows,
        quality_config=quality_config or {},
        tracked_models=tracked_models,
    )
    summary["daily_review_item_count"] = len(daily_review_items)

    return {
        "category": category.category_name,
        "generated_at": generated.isoformat(),
        "scope_note": (
            "Property quality rows separate operating signals such as transaction, "
            "presale, listing inventory, and permit/completion evidence from broad "
            "news or community market commentary. Proxy rows are review items until "
            "source-level identifiers and dates are collected."
        ),
        "summary": summary,
        "sources": source_rows,
        "events": events,
        "daily_review_items": daily_review_items,
        "source_backlog": (quality_config or {}).get("source_backlog", {}),
        "errors": error_rows,
    }


def write_quality_report(
    report: Mapping[str, object],
    *,
    output_dir: Path,
    category_name: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = _parse_datetime(str(report.get("generated_at") or "")) or datetime.now(UTC)
    date_stamp = _as_utc(generated_at).strftime("%Y%m%d")
    latest_path = output_dir / f"{category_name}_quality.json"
    dated_path = output_dir / f"{category_name}_{date_stamp}_quality.json"
    encoded = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    latest_path.write_text(encoded + "\n", encoding="utf-8")
    dated_path.write_text(encoded + "\n", encoding="utf-8")
    return {"latest": latest_path, "dated": dated_path}


def _build_events(
    *,
    articles: list[Article],
    sources: list[Source],
    tracked_models: list[str],
    event_model_config: Mapping[str, object],
) -> list[dict[str, Any]]:
    source_map = {source.name: source for source in sources}
    rows: list[dict[str, Any]] = []
    for article in articles:
        source = source_map.get(_article_source(article))
        if source is None:
            continue
        event_model = _article_event_model(article, source, tracked_models)
        if event_model not in tracked_models:
            continue
        event_at = _event_datetime(article)
        rows.append(_event_row(article, source, event_model, event_at, event_model_config))
    return rows


def _event_row(
    article: Article,
    source: Source,
    event_model: str,
    event_at: datetime | None,
    event_model_config: Mapping[str, object],
) -> dict[str, Any]:
    region_name = _region_name(article, source)
    row: dict[str, Any] = {
        "source": source.name,
        "source_type": source.type,
        "trust_tier": source.trust_tier,
        "content_type": source.content_type,
        "collection_tier": source.collection_tier,
        "producer_role": source.producer_role,
        "info_purpose": source.info_purpose,
        "event_model": event_model,
        "title": _article_title(article),
        "url": _article_link(article),
        "source_url": _article_link(article) or source.url,
        "event_at": event_at.isoformat() if event_at else None,
        "matched_entities": _article_entities(article),
        "signal_basis": _signal_basis(article, source, event_model),
        "region_code": _region_code(article, source, region_name),
        "region_name": region_name,
        "complex_name": _complex_name(article),
        "building": _summary_value(article, "Building"),
        "area": _area(article),
        "transaction_price": _transaction_price(article),
        "transaction_type": _transaction_type(article),
        "deal_date": _summary_value(article, "Deal date"),
        "report_date": _summary_value(article, "Report date"),
        "project_id": _project_id(article),
        "project_name": _project_name(article),
        "competition_rate": _competition_rate(article),
        "property_type": _property_type(article),
        "listing_count": _listing_count(article),
        "permit_date": _summary_value(article, "Permit date"),
        "completion_date": _summary_value(article, "Completion date"),
    }
    canonical_key, canonical_key_status = _canonical_key(row)
    row["canonical_key"] = canonical_key
    row["canonical_key_status"] = canonical_key_status
    row["event_key"] = _event_key(row, event_at)
    row["required_field_proxy"] = _required_field_proxy(row, event_model, event_model_config)
    row["required_field_gaps"] = _required_field_gaps(row, event_model, event_model_config)
    return row


def _build_source_row(
    *,
    source: Source,
    articles: list[Article],
    events: list[dict[str, Any]],
    errors: list[str],
    quality: Mapping[str, object],
    tracked_models: list[str],
    generated_at: datetime,
) -> dict[str, Any]:
    source_articles = [article for article in articles if _article_source(article) == source.name]
    event_model = _source_event_model(source, tracked_models)
    source_events = [
        row
        for row in events
        if row["source"] == source.name and row["event_model"] == event_model
    ]
    latest_event = _latest_event(source_events)
    latest_event_at = (
        _parse_datetime(str(latest_event.get("event_at") or "")) if latest_event else None
    )
    sla_days = _source_sla_days(source, event_model, _dict(quality, "freshness_sla"))
    age_days = _age_days(generated_at, latest_event_at) if latest_event_at else None
    source_errors = [
        error
        for error in errors
        if error.startswith(f"{source.name}:") or error.startswith(f"[{source.name}]")
    ]

    status = _source_status(
        source=source,
        tracked=event_model in tracked_models,
        article_count=len(source_articles),
        event_count=len(source_events),
        latest_event_at=latest_event_at,
        sla_days=sla_days,
        age_days=age_days,
    )

    return {
        "source": source.name,
        "source_type": source.type,
        "enabled": source.enabled,
        "trust_tier": source.trust_tier,
        "content_type": source.content_type,
        "collection_tier": source.collection_tier,
        "producer_role": source.producer_role,
        "info_purpose": source.info_purpose,
        "tracked": event_model in tracked_models,
        "event_model": event_model,
        "freshness_sla_days": sla_days,
        "status": status,
        "article_count": len(source_articles),
        "event_count": len(source_events),
        "latest_event_at": latest_event_at.isoformat() if latest_event_at else None,
        "age_days": round(age_days, 2) if age_days is not None else None,
        "latest_title": str(latest_event.get("title", "")) if latest_event else "",
        "latest_url": str(latest_event.get("url", "")) if latest_event else "",
        "latest_canonical_key": str(latest_event.get("canonical_key", "")) if latest_event else "",
        "latest_required_field_gaps": (
            latest_event.get("required_field_gaps", []) if latest_event else []
        ),
        "errors": source_errors,
    }


def _tracked_event_models(quality: Mapping[str, object]) -> list[str]:
    outputs = _dict(quality, "quality_outputs")
    raw = outputs.get("tracked_event_models")
    if isinstance(raw, list):
        values = [str(value).strip() for value in raw if str(value).strip()]
        values = [value for value in values if value in TRACKED_EVENT_MODELS]
        if values:
            return values
    return list(TRACKED_EVENT_MODEL_ORDER)


def _source_event_model(source: Source, tracked_models: list[str]) -> str:
    raw = source.config.get("event_model")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()

    tool = str(source.config.get("tool") or "").lower()
    if "apartment_trade" in tool or "trade" in tool:
        return "transaction_record" if "transaction_record" in tracked_models else ""
    if "subscription" in tool or "presale" in tool:
        return "presale_competition" if "presale_competition" in tracked_models else ""

    text = _source_context(source)
    if _has_any(text, ["transaction", "trade", "deal", "price", "실거래", "매매"]):
        return "transaction_record" if "transaction_record" in tracked_models else ""
    if _has_any(text, ["presale", "competition", "청약", "분양", "lh", "sh"]):
        return "presale_competition" if "presale_competition" in tracked_models else ""
    if _has_any(text, ["listing", "inventory", "매물"]):
        return "listing_inventory" if "listing_inventory" in tracked_models else ""
    if _has_any(text, ["permit", "completion", "준공", "착공"]):
        return "permit_completion" if "permit_completion" in tracked_models else ""
    return ""


def _article_event_model(article: Article, source: Source, tracked_models: list[str]) -> str:
    configured = _source_event_model(source, tracked_models)
    if configured and (
        source.type.lower() in {"api", "mcp"}
        or bool(source.config.get("allow_source_context_event"))
        or _has_article_evidence(article, configured)
    ):
        return configured

    text = _article_text(article)
    if _has_any(text, ["청약", "분양", "공급", "가구", "세대", "presale", "competition rate"]):
        return "presale_competition" if "presale_competition" in tracked_models else ""
    if _has_any(text, ["listing", "listings", "inventory", "for sale", "to let", "매물"]):
        return "listing_inventory" if "listing_inventory" in tracked_models else ""
    if _has_any(text, ["permit", "completion", "준공", "착공", "사용승인", "인허가"]):
        return "permit_completion" if "permit_completion" in tracked_models else ""
    if _has_any(text, ["transaction", "sold", "sale", "매매", "거래"]):
        if _transaction_price(article) is not None:
            return "transaction_record" if "transaction_record" in tracked_models else ""
    return ""


def _has_article_evidence(article: Article, event_model: str) -> bool:
    text = _article_text(article)
    if event_model == "transaction_record":
        return _transaction_price(article) is not None and _has_any(
            text, ["transaction", "sold", "sale", "매매", "거래"]
        )
    if event_model == "presale_competition":
        return _has_any(text, ["청약", "분양", "공급", "가구", "세대", "presale", "competition"])
    if event_model == "listing_inventory":
        return _has_any(text, ["listing", "listings", "inventory", "for sale", "to let", "매물"])
    if event_model == "permit_completion":
        return _has_any(text, ["permit", "completion", "준공", "착공", "사용승인", "인허가"])
    return False


def _source_sla_days(
    source: Source,
    event_model: str,
    freshness_sla: Mapping[str, object],
) -> float | None:
    raw_source_sla = source.config.get("freshness_sla_days")
    parsed_source_sla = _as_float(raw_source_sla)
    if parsed_source_sla is not None:
        return parsed_source_sla

    for key in (f"{event_model}_days", f"{event_model}_day"):
        parsed_days = _as_float(freshness_sla.get(key))
        if parsed_days is not None:
            return parsed_days
    for key in (f"{event_model}_hours", f"{event_model}_hour"):
        parsed_hours = _as_float(freshness_sla.get(key))
        if parsed_hours is not None:
            return parsed_hours / 24.0
    return None


def _source_status(
    *,
    source: Source,
    tracked: bool,
    article_count: int,
    event_count: int,
    latest_event_at: datetime | None,
    sla_days: float | None,
    age_days: float | None,
) -> str:
    if not source.enabled:
        return "skipped_disabled"
    if not tracked:
        return "not_tracked"
    if article_count == 0:
        return "missing"
    if event_count == 0:
        return "missing_event"
    if latest_event_at is None or age_days is None:
        return "unknown_event_date"
    if sla_days is not None and age_days > sla_days:
        return "stale"
    return "fresh"


def _event_quality_summary(
    *,
    events: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    quality_config: Mapping[str, object],
    tracked_models: list[str],
) -> dict[str, int]:
    event_counts = Counter(str(row.get("event_model") or "") for row in events)
    return {
        "property_signal_event_count": sum(event_counts.get(model, 0) for model in tracked_models),
        "official_or_operational_event_count": sum(
            1
            for row in events
            if str(row.get("trust_tier") or "").startswith("T1_")
            or str(row.get("source_type") or "").lower() in {"api", "mcp"}
        ),
        "news_proxy_event_count": sum(
            1
            for row in events
            if str(row.get("content_type") or "").lower() in {"news", "community"}
        ),
        "complete_canonical_key_count": sum(
            1 for row in events if row.get("canonical_key_status") == "complete"
        ),
        "proxy_canonical_key_count": sum(
            1
            for row in events
            if str(row.get("canonical_key_status") or "").endswith("_proxy")
        ),
        "missing_canonical_key_count": sum(1 for row in events if not row.get("canonical_key")),
        "region_code_present_count": sum(1 for row in events if row.get("region_code")),
        "project_key_present_count": sum(
            1
            for row in events
            if row.get("project_id")
            or str(row.get("canonical_key") or "").startswith("property_project:")
        ),
        "transaction_price_present_count": sum(
            1 for row in events if row.get("transaction_price") is not None
        ),
        "listing_count_present_count": sum(
            1 for row in events if row.get("listing_count") is not None
        ),
        "competition_rate_present_count": sum(
            1 for row in events if row.get("competition_rate") is not None
        ),
        "event_required_field_gap_count": sum(
            len(row.get("required_field_gaps") or [])
            for row in events
            if _counts_as_structured_gap(row)
        ),
        "tracked_source_gap_count": sum(
            1
            for row in source_rows
            if row.get("tracked")
            and row.get("status") in {"missing", "missing_event", "unknown_event_date", "stale"}
        ),
        "missing_event_model_count": sum(
            1 for model in tracked_models if event_counts.get(model, 0) == 0
        ),
        "source_backlog_candidate_count": len(_source_backlog_items(quality_config)),
    }


def _daily_review_items(
    *,
    events: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    quality_config: Mapping[str, object],
    tracked_models: list[str],
) -> list[dict[str, Any]]:
    review: list[dict[str, Any]] = []
    for row in events:
        gaps = [str(value) for value in row.get("required_field_gaps") or []]
        if gaps and _counts_as_structured_gap(row):
            review.append(
                {
                    "reason": "missing_required_fields",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "canonical_key": row.get("canonical_key"),
                    "required_field_gaps": gaps,
                }
            )
        if not row.get("canonical_key"):
            review.append(
                {
                    "reason": "missing_canonical_key",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "event_key": row.get("event_key"),
                }
            )
        if str(row.get("canonical_key_status") or "").endswith("_proxy"):
            review.append(
                {
                    "reason": "proxy_canonical_key",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "canonical_key": row.get("canonical_key"),
                    "canonical_key_status": row.get("canonical_key_status"),
                }
            )
        if str(row.get("content_type") or "").lower() in {"news", "community"}:
            review.append(
                {
                    "reason": "news_or_community_proxy_source",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "signal_basis": row.get("signal_basis"),
                }
            )

    for source in source_rows:
        if not source.get("tracked"):
            continue
        if source.get("status") in {"missing", "missing_event", "unknown_event_date", "stale"}:
            review.append(
                {
                    "reason": f"source_{source.get('status')}",
                    "source": source.get("source"),
                    "event_model": source.get("event_model"),
                    "age_days": source.get("age_days"),
                    "latest_title": source.get("latest_title"),
                }
            )

    event_counts = Counter(str(row.get("event_model") or "") for row in events)
    for event_model in TRACKED_EVENT_MODEL_ORDER:
        if event_model in tracked_models and event_counts.get(event_model, 0) == 0:
            review.append({"reason": "missing_event_model", "event_model": event_model})

    for item in _source_backlog_items(quality_config):
        review.append(
            {
                "reason": "source_backlog_pending",
                "source": item.get("name") or item.get("id"),
                "signal_type": item.get("signal_type"),
                "activation_gate": item.get("activation_gate"),
            }
        )
    return review[:50]


def _counts_as_structured_gap(row: Mapping[str, Any]) -> bool:
    return str(row.get("signal_basis") or "") == "operational_source"


def _source_backlog_items(quality_config: Mapping[str, object]) -> list[Mapping[str, object]]:
    backlog = _dict(quality_config, "source_backlog")
    candidates = backlog.get("operational_candidates")
    if not isinstance(candidates, list):
        return []
    return [item for item in candidates if isinstance(item, Mapping)]


def _required_field_proxy(
    row: Mapping[str, Any],
    event_model: str,
    event_model_config: Mapping[str, object],
) -> dict[str, bool]:
    event_config = _dict(event_model_config, event_model)
    raw_fields = event_config.get("required_fields")
    if not isinstance(raw_fields, list):
        return {}
    return {str(field): _field_present(row, str(field)) for field in raw_fields if str(field).strip()}


def _required_field_gaps(
    row: Mapping[str, Any],
    event_model: str,
    event_model_config: Mapping[str, object],
) -> list[str]:
    return [
        field
        for field, present in _required_field_proxy(row, event_model, event_model_config).items()
        if not present
    ]


def _field_present(row: Mapping[str, Any], field: str) -> bool:
    aliases = {
        "region_code": ("region_code",),
        "complex_name": ("complex_name",),
        "area": ("area",),
        "transaction_price": ("transaction_price",),
        "source_url": ("source_url", "url"),
        "project_id": ("project_id",),
        "competition_rate": ("competition_rate",),
        "property_type": ("property_type",),
        "listing_count": ("listing_count",),
    }
    for alias in aliases.get(field.lower(), (field.lower(),)):
        value = row.get(alias)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def _canonical_key(row: Mapping[str, Any]) -> tuple[str, str]:
    event_model = str(row.get("event_model") or "")
    region_code = _slug(row.get("region_code") or "")
    complex_name = _slug(row.get("complex_name") or "")
    area = _slug(row.get("area") or "")
    project_id = _slug(row.get("project_id") or "")
    project_name = _slug(row.get("project_name") or "")
    property_type = _slug(row.get("property_type") or "")
    source = _slug(row.get("source") or "")

    if event_model == "transaction_record":
        if region_code and complex_name and area:
            return f"property:{region_code}:{complex_name}:{area}", "complete"
        if region_code and complex_name:
            return f"property:{region_code}:{complex_name}", "property_proxy"
        if region_code:
            return f"property_region:{region_code}", "region_proxy"
        return "", "missing"
    if event_model in {"presale_competition", "permit_completion"}:
        if project_id:
            return f"property_project:{project_id}", "complete"
        if region_code and project_name:
            return f"property_project:{region_code}:title:{_digest(project_name)}", "title_proxy"
        if project_name:
            return f"property_project:title:{_digest(project_name)}", "title_proxy"
        if region_code:
            return f"property_region:{region_code}", "region_proxy"
        return "", "missing"
    if event_model == "listing_inventory":
        if region_code and property_type and row.get("listing_count") is not None:
            return f"listing_inventory:{region_code}:{property_type}", "complete"
        if region_code and property_type:
            return f"listing_inventory:{region_code}:{property_type}", "region_proxy"
        if source and property_type:
            return f"listing_inventory:source:{source}:{property_type}", "source_proxy"
        return "", "missing"
    return "", "missing"


def _event_key(row: Mapping[str, Any], event_at: datetime | None) -> str:
    observed = _as_utc(event_at).strftime("%Y%m%d") if event_at else "undated"
    basis = row.get("canonical_key") or row.get("source_url") or row.get("title") or ""
    return f"{row.get('event_model')}:{_digest(basis)}:{observed}"


def _signal_basis(article: Article, source: Source, event_model: str) -> str:
    source_model = _source_event_model(source, list(TRACKED_EVENT_MODEL_ORDER))
    if source_model == event_model and source.type.lower() in {"api", "mcp"}:
        return "operational_source"
    if source_model == event_model:
        return "source_context_signal"
    if _has_article_evidence(article, event_model):
        return "article_text_signal"
    return "proxy_signal"


def _region_code(article: Article, source: Source, region_name: str) -> str:
    configured = _first_non_empty(
        source.config.get("region_code"),
        source.config.get("lawdCd"),
        source.config.get("lawd_code"),
    )
    if configured:
        return _digits(configured) or _slug(configured)
    labeled = _summary_value(article, "Region code", "LAWD code", "lawdCd")
    if labeled:
        return _digits(labeled) or _slug(labeled)
    normalized = region_name.strip().lower()
    return REGION_CODE_HINTS.get(normalized, "")


def _region_name(article: Article, source: Source) -> str:
    labeled = _summary_value(article, "Region")
    if labeled:
        return labeled
    matches = _matches(article, "Location")
    if matches:
        return matches[0]
    return _first_non_empty(source.region, source.country)


def _complex_name(article: Article) -> str:
    return _summary_value(article, "Complex", "Complex name", "Apartment", "aptNm")


def _area(article: Article) -> float | None:
    labeled = _summary_value(article, "Area", "excluUseAr")
    if labeled:
        return _first_number(labeled)
    match = re.search(
        r"(\d+(?:[.,]\d+)?)\s*(?:m2|㎡|sqm|square meters|평)",
        _article_text(article),
        flags=re.IGNORECASE,
    )
    return _parse_number(match.group(1)) if match else None


def _transaction_price(article: Article) -> float | None:
    labeled = _summary_value(article, "Transaction price", "Deal amount", "Price")
    if labeled:
        return _amount(labeled)
    text = _article_text(article)
    patterns = [
        r"(?:KRW|₩)\s*(\d[\d,]*(?:\.\d+)?)",
        r"(\d[\d,]*(?:\.\d+)?)\s*(?:원|만원|억원|억)",
        r"(?:USD|\$|£)\s*(\d[\d,]*(?:\.\d+)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            amount = _parse_number(match.group(1))
            if amount is not None:
                if "만원" in match.group(0):
                    return amount * 10000
                if "억원" in match.group(0) or "억" in match.group(0):
                    return amount * 100000000
                return amount
    return None


def _transaction_type(article: Article) -> str:
    labeled = _summary_value(article, "Transaction type")
    if labeled:
        return labeled
    text = _article_text(article)
    if _has_any(text, ["rent", "lease", "전세", "월세"]):
        return "rent"
    if _has_any(text, ["buy", "sale", "sold", "매매", "매입", "분양"]):
        return "sale"
    return ""


def _project_id(article: Article) -> str:
    return _slug(_summary_value(article, "Project ID"))


def _project_name(article: Article) -> str:
    labeled = _summary_value(article, "Project", "Project name")
    if labeled:
        return labeled
    title = _article_title(article)
    quoted = re.search(r"['\"]([^'\"]{3,80})['\"]", title)
    if quoted:
        return quoted.group(1)
    return title[:120]


def _competition_rate(article: Article) -> float | None:
    labeled = _summary_value(article, "Competition rate")
    if labeled:
        return _first_number(labeled)
    match = re.search(
        r"(\d+(?:[.,]\d+)?)\s*(?:[:대]\s*1|to\s*1)",
        _article_text(article),
        flags=re.IGNORECASE,
    )
    return _parse_number(match.group(1)) if match else None


def _property_type(article: Article) -> str:
    labeled = _summary_value(article, "Property type")
    if labeled:
        return _slug(labeled)
    for match in _matches(article, "PropertyType") + _matches(article, "PropertyGeneral"):
        normalized = match.strip().lower()
        if normalized in {"property", "real estate"}:
            continue
        return PROPERTY_TYPE_HINTS.get(normalized, _slug(match))
    text = _article_text(article)
    for token, normalized in PROPERTY_TYPE_HINTS.items():
        if re.search(rf"\b{re.escape(token)}\b", text, flags=re.IGNORECASE) or token in text:
            return normalized
    return ""


def _listing_count(article: Article) -> int | None:
    labeled = _summary_value(article, "Listing count")
    if labeled:
        number = _first_number(labeled)
        return int(number) if number is not None else None
    match = re.search(
        r"(\d[\d,]*)\s*(?:listings|homes|units|properties|가구|세대)",
        _article_text(article),
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    parsed = _parse_number(match.group(1))
    return int(parsed) if parsed is not None else None


def _latest_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    dated: list[tuple[datetime, dict[str, Any]]] = []
    undated: list[dict[str, Any]] = []
    for row in events:
        parsed = _parse_datetime(str(row.get("event_at") or ""))
        if parsed is None:
            undated.append(row)
        else:
            dated.append((parsed, row))
    if dated:
        return max(dated, key=lambda row: row[0])[1]
    return undated[0] if undated else None


def _event_datetime(article: Article) -> datetime | None:
    published = getattr(article, "published", None)
    collected = getattr(article, "collected_at", None)
    value = published if isinstance(published, datetime) else collected
    return _as_utc(value) if isinstance(value, datetime) else None


def _article_source(article: Article) -> str:
    return str(getattr(article, "source", "") or "")


def _article_title(article: Article) -> str:
    return str(getattr(article, "title", "") or "")


def _article_link(article: Article) -> str:
    return str(getattr(article, "link", "") or "")


def _article_summary(article: Article) -> str:
    return str(getattr(article, "summary", "") or getattr(article, "abstract", "") or "")


def _article_entities(article: Article) -> dict[str, Any]:
    raw = getattr(article, "matched_entities", {})
    return raw if isinstance(raw, dict) else {}


def _matches(article: Article, entity_name: str) -> list[str]:
    raw = _article_entities(article).get(entity_name, [])
    if isinstance(raw, list):
        return [str(value).strip() for value in raw if str(value).strip()]
    if raw:
        return [str(raw).strip()]
    return []


def _summary_value(article: Article, *labels: str) -> str:
    text = " ".join(_article_text(article).split())
    for label in labels:
        match = re.search(rf"\b{re.escape(label)}\s*[:=]\s*", text, flags=re.IGNORECASE)
        if not match:
            continue
        start = match.end()
        end = len(text)
        for next_label in SUMMARY_LABELS:
            next_match = re.search(
                rf"\b{re.escape(next_label)}\s*[:=]\s*",
                text[start:],
                flags=re.IGNORECASE,
            )
            if next_match:
                end = min(end, start + next_match.start())
        return text[start:end].strip(" \t\r\n.;,")
    return ""


def _article_text(article: Article) -> str:
    return f"{_article_title(article)} {_article_summary(article)} {_article_link(article)}"


def _source_context(source: Source) -> str:
    return " ".join(
        [
            source.name,
            source.type,
            source.content_type,
            source.collection_tier,
            source.producer_role,
            " ".join(source.info_purpose),
            " ".join(str(value) for value in source.config.values()),
        ]
    ).lower()


def _has_any(text: str, needles: list[str]) -> bool:
    lowered = text.lower()
    return any(needle.lower() in lowered for needle in needles)


def _dict(value: Mapping[str, object], key: str) -> Mapping[str, object]:
    raw = value.get(key)
    if isinstance(raw, Mapping):
        return {str(k): v for k, v in raw.items()}
    return {}


def _as_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: str) -> datetime | None:
    normalized = value.strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _age_days(generated_at: datetime, event_at: datetime) -> float:
    return max(0.0, (_as_utc(generated_at) - _as_utc(event_at)).total_seconds() / 86400)


def _amount(text: str) -> float | None:
    amount = _first_number(text)
    if amount is None:
        return None
    if "만원" in text:
        return amount * 10000
    if "억원" in text or "억" in text:
        return amount * 100000000
    return amount


def _first_number(text: str) -> float | None:
    match = re.search(r"\d[\d,]*(?:\.\d+)?", text)
    return _parse_number(match.group(0)) if match else None


def _parse_number(raw: str) -> float | None:
    normalized = raw.strip()
    if not normalized:
        return None
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(",", "")
    elif "," in normalized:
        parts = normalized.split(",")
        if len(parts) > 1 and all(len(part) == 3 for part in parts[1:]):
            normalized = normalized.replace(",", "")
        else:
            normalized = normalized.replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def _digits(value: object) -> str:
    return "".join(re.findall(r"\d+", str(value)))


def _first_non_empty(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _slug(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9가-힣_-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:120]


def _digest(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
