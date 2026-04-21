# Data Quality Plan

- 생성 시각: `2026-04-11T16:05:37.910248+00:00`
- 우선순위: `P2`
- 데이터 품질 점수: `75`
- 가장 약한 축: `운영 깊이`
- Governance: `high`
- Primary Motion: `conversion`

## 현재 이슈

- 고거버넌스 저장소 대비 공식 근거 source가 얕음
- 가장 약한 품질 축은 운영 깊이(47)

## 필수 신호

- 실거래·분양 경쟁률·매물 inventory
- 인허가·준공·입주 예정 데이터
- 지역·단지·면적 기준 canonical property key

## 품질 게이트

- 거래일·계약일·신고일·수집일을 별도 필드로 유지
- 뉴스와 실거래/재고 신호를 같은 품질 등급으로 병합하지 않음
- 지역 코드와 단지명 alias를 추적

## 다음 구현 순서

- 실거래, 분양 경쟁률, inventory source를 운영 레이어로 보강
- 인허가·준공 source를 공식 레이어로 추가
- 지역/단지 canonicalization과 중복 거래 검증 리포트를 추가

## 운영 규칙

- 원문 URL, 수집일, 이벤트 발생일은 별도 필드로 유지한다.
- 공식 source와 커뮤니티/시장 source를 같은 신뢰 등급으로 병합하지 않는다.
- collector가 인증키나 네트워크 제한으로 skip되면 실패를 숨기지 말고 skip 사유를 기록한다.
- 이 문서는 `scripts/build_data_quality_review.py --write-repo-plans`로 재생성한다.
