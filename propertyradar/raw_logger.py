from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from .models import Article


class RawLogger:
    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir

    def log(
        self,
        articles: Iterable[Article],
        *,
        source_name: str,
        run_id: str | None = None,
    ) -> Path:
        now = datetime.now(UTC)
        date_dir = self.raw_dir / now.date().isoformat()
        safe_source_name = source_name.replace("/", "_").replace("\\", "_")
        output_path = (
            date_dir / f"{safe_source_name}_{run_id}.jsonl"
            if run_id is not None
            else date_dir / f"{safe_source_name}.jsonl"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("a", encoding="utf-8") as handle:
            for article in articles:
                record = {
                    "title": article.title,
                    "link": article.link,
                    "summary": article.summary,
                    "published": article.published.isoformat() if article.published else None,
                    "source": article.source,
                    "category": article.category,
                    "matched_entities": article.matched_entities,
                    "logged_at": now.isoformat(),
                }
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")

        return output_path
