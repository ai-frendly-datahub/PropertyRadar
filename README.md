# PropertyRadar - 부동산 레이더

**🌐 Live Report**: https://ai-frendly-datahub.github.io/PropertyRadar/

부동산 정보를 수집·분석하는 레이더. 한경·매일경제·서울경제 부동산 RSS와 국토교통부 보도자료를 매일 수집하여 지역·부동산 유형·정책·시장동향별로 분류하고 GitHub Pages에 배포합니다.

## 개요

- **수집 소스**: 한경 부동산, 국토교통부 보도자료, 매일경제 부동산, 서울경제 부동산
- **분석 대상**: 지역(Location), 부동산 유형(PropertyType), 정책(Policy), 시장동향(Market)
- **출력**: GitHub Pages HTML 리포트 (Flatpickr 캘린더 + Chart.js 트렌드)

## 빠른 시작

```bash
pip install -e ".[dev]"
python main.py --category property --recent-days 7 --snapshot-db
```

## 구조

```
PropertyRadar/
  propertyradar/
    collector.py    # 부동산 RSS 수집
    analyzer.py     # 엔티티 분석 (radar-core 위임)
    raw_logger.py   # data/raw/YYYY-MM-DD/*.jsonl 원문 기록
    storage.py      # DuckDB 저장 (radar-core 위임)
    reporter.py     # HTML 리포트 생성 (radar-core 위임)
  config/
    config.yaml                  # database_path, report_dir
    categories/property.yaml     # 수집 소스 + 엔티티 정의
  main.py           # CLI 진입점
  tests/            # 단위 테스트
```

## 설정

`config/config.yaml` 및 `config/categories/property.yaml` 참조.

## 개발

```bash
pytest tests/ -v
```

## 스케줄

GitHub Actions로 매일 자동 수집 후 `reports/property_report_YYYYMMDD.html`과
`reports/property_report.html`을 생성하고, DuckDB는
`data/snapshots/YYYY-MM-DD/radar_data.duckdb`에 날짜별로 보관합니다.
수집 원문은 `data/raw/YYYY-MM-DD/<source>.jsonl`에 누적합니다.

<!-- DATAHUB-OPS-AUDIT:START -->
## DataHub Operations

- CI/CD workflows: `deploy-pages.yml`, `radar-crawler.yml`.
- GitHub Pages visualization: `reports/index.html` (valid HTML); root static pages: `index.html`; https://ai-frendly-datahub.github.io/PropertyRadar/.
- Latest remote Pages check: HTTP 200, HTML.
- Local workspace audit: 21 Python files parsed, 0 syntax errors.
- Re-run audit from the workspace root: `python scripts/audit_ci_pages_readme.py --syntax-check --write`.
- Latest audit report: `_workspace/2026-04-14_github_ci_pages_readme_audit.md`.
- Latest Pages URL report: `_workspace/2026-04-14_github_pages_url_check.md`.
<!-- DATAHUB-OPS-AUDIT:END -->
