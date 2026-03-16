from __future__ import annotations

from propertyradar.analyzer import apply_entity_rules
from propertyradar.models import Article, EntityDefinition


def _make_article(title: str = "", summary: str = "") -> Article:
    return Article(
        title=title,
        link="https://example.com/1",
        summary=summary,
        published=None,
        source="TestSource",
        category="test",
    )


def _make_entity(name: str, keywords: list[str]) -> EntityDefinition:
    return EntityDefinition(name=name, display_name=name, keywords=keywords)


def test_keyword_match():
    """Korean keyword in title triggers match."""
    article = _make_article(title="강남구 아파트 가격 상승")
    entities = [_make_entity("강남구", ["강남구"])]
    results = apply_entity_rules([article], entities)
    assert len(results) == 1
    assert "강남구" in results[0].matched_entities


def test_no_match():
    """Article without matching keywords has empty matched_entities."""
    article = _make_article(title="날씨 뉴스", summary="오늘 맑음")
    entities = [_make_entity("강남구", ["강남구"])]
    results = apply_entity_rules([article], entities)
    assert len(results) == 1
    assert results[0].matched_entities == {}


def test_case_insensitive():
    """ASCII keyword matching is case-insensitive."""
    article = _make_article(title="Seoul Property Market Update")
    entities = [_make_entity("Seoul", ["seoul"])]
    results = apply_entity_rules([article], entities)
    assert "Seoul" in results[0].matched_entities


def test_multiple_entities():
    """Multiple entities can match the same article."""
    article = _make_article(
        title="강남구 서초구 아파트 시세",
        summary="부동산 시장 동향",
    )
    entities = [
        _make_entity("강남구", ["강남구"]),
        _make_entity("서초구", ["서초구"]),
    ]
    results = apply_entity_rules([article], entities)
    assert "강남구" in results[0].matched_entities
    assert "서초구" in results[0].matched_entities


def test_empty_articles():
    """Empty article list returns empty result."""
    entities = [_make_entity("테스트", ["키워드"])]
    results = apply_entity_rules([], entities)
    assert results == []


def test_keyword_in_summary():
    """Keyword in summary (not title) also triggers match."""
    article = _make_article(title="부동산 뉴스", summary="분당구 신축 아파트 분양")
    entities = [_make_entity("분당구", ["분당구"])]
    results = apply_entity_rules([article], entities)
    assert "분당구" in results[0].matched_entities


def test_ascii_word_boundary():
    """ASCII keywords respect word boundaries."""
    article = _make_article(title="Apartments are popular")
    entities = [_make_entity("Apart", ["apart"])]
    results = apply_entity_rules([article], entities)
    # "apart" should NOT match "Apartments" due to word boundary
    assert results[0].matched_entities == {}
