from __future__ import annotations

from datetime import UTC, datetime, timedelta

from propertyradar.models import Article
from propertyradar.storage import RadarStorage


def _make_article(
    title: str = "기사",
    link: str = "https://example.com/1",
    category: str = "test",
    published: datetime | None = None,
) -> Article:
    return Article(
        title=title,
        link=link,
        summary="요약",
        published=published,
        source="TestSource",
        category=category,
        matched_entities={},
    )


def test_upsert_and_query(tmp_db):
    """Upsert articles then retrieve via recent_articles."""
    with RadarStorage(tmp_db) as storage:
        art = _make_article(published=datetime.now(UTC))
        storage.upsert_articles([art])
        results = storage.recent_articles("test", days=1)
        assert len(results) == 1
        assert results[0].title == "기사"
        assert results[0].link == "https://example.com/1"


def test_deduplication(tmp_db):
    """Inserting same link twice should update, not duplicate."""
    with RadarStorage(tmp_db) as storage:
        art1 = _make_article(title="원본", published=datetime.now(UTC))
        storage.upsert_articles([art1])

        art2 = _make_article(title="수정본", published=datetime.now(UTC))
        storage.upsert_articles([art2])

        results = storage.recent_articles("test", days=1)
        assert len(results) == 1
        assert results[0].title == "수정본"


def test_recent_articles(tmp_db):
    """Only articles within the time window are returned."""
    with RadarStorage(tmp_db) as storage:
        old = _make_article(
            title="오래된 기사",
            link="https://example.com/old",
            published=datetime.now(UTC) - timedelta(days=30),
        )
        new = _make_article(
            title="최신 기사",
            link="https://example.com/new",
            published=datetime.now(UTC),
        )
        storage.upsert_articles([old, new])
        results = storage.recent_articles("test", days=7)
        assert len(results) == 1
        assert results[0].title == "최신 기사"


def test_delete_older_than(tmp_db):
    """delete_older_than removes old articles and returns count."""
    with RadarStorage(tmp_db) as storage:
        old = _make_article(
            link="https://example.com/old",
            published=datetime.now(UTC) - timedelta(days=60),
        )
        new = _make_article(
            link="https://example.com/new",
            published=datetime.now(UTC),
        )
        storage.upsert_articles([old, new])

        deleted = storage.delete_older_than(days=30)
        assert deleted == 1

        results = storage.recent_articles("test", days=365)
        assert len(results) == 1
        assert results[0].link == "https://example.com/new"


def test_context_manager(tmp_db):
    """RadarStorage works as context manager."""
    with RadarStorage(tmp_db) as storage:
        assert storage.conn is not None
        storage.upsert_articles([_make_article(published=datetime.now(UTC))])
    # After exit, connection should be closed
    with RadarStorage(tmp_db) as storage2:
        results = storage2.recent_articles("test", days=1)
        assert len(results) == 1


def test_upsert_empty_list(tmp_db):
    """Upserting empty list should not raise."""
    with RadarStorage(tmp_db) as storage:
        storage.upsert_articles([])
        results = storage.recent_articles("test", days=1)
        assert len(results) == 0


def test_matched_entities_roundtrip(tmp_db):
    """matched_entities JSON survives upsert and query."""
    with RadarStorage(tmp_db) as storage:
        art = _make_article(published=datetime.now(UTC))
        art.matched_entities = {"강남구": ["강남", "부동산"]}
        storage.upsert_articles([art])
        results = storage.recent_articles("test", days=1)
        assert results[0].matched_entities == {"강남구": ["강남", "부동산"]}
