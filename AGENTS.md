# PROPERTYRADAR

부동산 정보를 수집·분석하는 Standard Tier 레이더입니다. 부동산 RSS와 국토교통부 보도자료를 수집하고 지역·부동산 유형·정책·시장동향 기준으로 엔티티를 분류합니다.

## STRUCTURE

```
PropertyRadar/
├── propertyradar/
│   ├── collector.py              # collect_sources() — 부동산 RSS / 보도자료 수집
│   ├── analyzer.py               # apply_entity_rules() — 지역/유형/정책 키워드 매칭
│   ├── reporter.py               # generate_report(), generate_index_html()
│   ├── storage.py                # RadarStorage — DuckDB upsert/query/retention
│   ├── raw_logger.py             # data/raw/YYYY-MM-DD/*.jsonl 원문 기록
│   ├── models.py                 # radar-core 기반 모델 재사용
│   ├── config_loader.py          # YAML 로딩
│   ├── logger.py                 # 구조화 로깅
│   ├── resilience.py             # 재시도/장애 격리
│   └── exceptions.py             # 커스텀 예외
├── config/
│   ├── config.yaml
│   └── categories/property.yaml  # 소스 + 엔티티 정의
├── data/                         # DuckDB, raw data
├── reports/                      # 날짜별 HTML report + latest + index.html
├── tests/                        # analyzer / reporter / storage 테스트
├── docs/                         # 분석 산출물
├── index.html                    # 정적 landing/배포 자산
└── main.py                       # CLI 엔트리포인트
```

## ENTITIES

| Entity | Examples |
|--------|----------|
| Location | 서울, 경기, 강남, 송파, 지방 광역시 등 |
| PropertyType | 아파트, 오피스텔, 빌라, 상가, 토지 |
| Policy | 대출규제, 세제, 공급정책, 재건축 |
| Market | 가격상승, 거래량, 분양, 전세, 매매 |

## DEVIATIONS FROM TEMPLATE

- 루트 `index.html` 정적 자산이 추가로 존재
- `reports/`에 날짜별 `property_report_YYYYMMDD.html`과 latest `property_report.html`이 존재
- `data/snapshots/YYYY-MM-DD/radar_data.duckdb`로 매일 DB 스냅샷을 보존
- `data/raw/YYYY-MM-DD/<source>.jsonl`로 수집 원문을 날짜별 보존
- 부동산 도메인 특성상 정책/시장 지표 엔티티 비중이 큼

## COMMANDS

```bash
python main.py --category property --recent-days 7 --snapshot-db
pytest tests/ -v
```

## NOTES

- 리포트/landing 경로를 바꾸면 `radar-dashboard`와 배포 링크 영향 여부를 확인
- 정책 엔티티와 지역 엔티티는 과도한 중복 매칭이 생기기 쉬우므로 analyzer 변경 시 테스트를 같이 본다
