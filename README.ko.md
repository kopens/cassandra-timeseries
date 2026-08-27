[English](README.md) · [한국어](README.ko.md)

# cassandra-timeseries

**Apache Cassandra for Industrial Timeseries Workload**
— 산업 현장의 센서·태그 데이터를 위한 분산 시계열 데이터베이스.

공장·플랜트의 시계열 데이터는 몇 가지 고유한 성질을 가집니다: 태그(시리즈)마다 초 단위로 끝없이 쌓이고, 몇 년치를 규정상 보관해야 하며, 엣지 장비가 통신 두절 뒤 며칠치를 한꺼번에 밀어 넣고(지각 백필), 조회는 거의 항상 "이 태그의 이 기간"입니다. 범용 Cassandra는 이 워크로드를 감당하지만, 압축·보존·집계는 전부 애플리케이션 몫으로 남습니다.

이 포크는 그 부분을 **데이터베이스 안으로 가져옵니다** — 시계열 연산을 서버에서 끝내고(21종 CQL 함수 + gap-fill), 오래된 데이터를 자동으로 압축·보존하며(계층형 저장 + 시계열 전용 컴팩션), 그러면서도 **CQL은 그대로**입니다. 압축된 과거 데이터도 평범한 `SELECT`로 읽힙니다(투명 읽기). 애플리케이션은 데이터가 압축돼 있는지 알 필요가 없습니다.

[apache/cassandra](https://github.com/apache/cassandra)(`cassandra-6.0` 브랜치)의 포크이며, 온디스크 포맷·CQL 문법은 업스트림 그대로라 **기존 6.0 데이터를 그대로 읽습니다**(새 기능은 전부 옵트인). Spark 연동은 짝이 되는 포크 [cassandra-spark-connector](https://dev.kopens.io/common/cassandra-spark-connector)(Spark 4.1.2)로 제공됩니다.

## 🎯 핵심 — 무엇이 좋아지나 (업스트림 Cassandra 6.0.0 대비)

**1. 서버에서 끝나는 시계열 연산.** 버킷팅·집계·보간·회귀를 CQL 한 줄로 처리합니다. 애플리케이션이 원시 데이터를 끌어와 계산하던 왕복이 사라집니다.

```sql
-- 시간별 평균 + 빈 구간 자동 채움 — 업스트림에서는 앱이 100k행을 받아 직접 계산해야 하는 작업
SELECT time_bucket_gapfill(1h, timestamp, '2026-07-01', '2026-07-02'), locf(avg(latency))
FROM pp.tm_tag_point
WHERE tag_id='TAG-001' AND timestamp >= '2026-07-01' AND timestamp < '2026-07-02'
GROUP BY tag_id, time_bucket_gapfill(1h, timestamp, '2026-07-01', '2026-07-02')
ORDER BY timestamp ASC;
```

**2. 오래된 데이터는 자동 압축, 조회는 그대로.** 계층형 저장이 지난 데이터를 컬럼 지향 청크로 압축해 옮기고, `SELECT`는 압축 여부를 몰라도 됩니다(투명 읽기가 자동 병합).

**3. 압축이 조회까지 빠르게 만듭니다 — 저장과 질의 양쪽에서 이깁니다.** 운영 테이블
형태(`tm_tag_point`, 일반 컬럼 8개) 2,000만 건 실측 — 호스트 234(Xeon Silver 4114T, 40스레드),
chunk format v4 — [벤치마크 전문](doc/timeseries/tiering-benchmark.md):

| 항목 | 계층화 전 | 계층화 후 (v4) | 효과 |
| --- | --- | --- | --- |
| 저장 용량 | 237.8 MB | **33.3 MB** | **7.1× 절감** (11.9 → ~1.7 B/행) |
| `count(*)` (4만 행 파티션) | 303 ms | **50 ms** | **6.1× 빠름** |
| `time_bucket` + 집계 | 150~270 ms | **31~64 ms** | **3~6× 빠름** |
| gap-fill (locf / interpolate) | 162~284 ms | **30~53 ms** | **5.4× 빠름** |
| 90개 태그 태그별 p95 (360만 행) | 14.2 s | **2.6 s** | **5.4× 빠름** |
| 90개 태그 시간별 평균 (360만 행) | 14.9 s | **4.2 s** | **3.5× 빠름** |
| 1시간 범위 + 컬럼 투영 | 55~56 ms | **30~33 ms** | **1.8× 빠름** |
| 최신 1,000행 `SELECT *` (시간 범위 없음) | 52~56 ms | **38~45 ms** | 동률 이상 |
| 재인코딩 처리량 | — | **108k rows/s** | 게이트(50k)의 2.2배 |

> **측정한 모든 질의에서 계층화 후가 같거나 빠릅니다** — 시간 범위 없는 조회·행 단위 조회·static
> 조회 포함. 시간 범위 없는 `LIMIT` 조회는 질의 방향 순서로(DESC면 최신 창부터) 창을 하나씩
> 디코드하다 `LIMIT`이 차면 멈추므로 느려지지 않습니다.
>
> **`hot_window` 안쪽 질의는 비용이 없습니다** — 콜드 경계 위에서 시작하는 질의는 병합을 건너뛰고
> 핫 이터레이터를 그대로 돌려줍니다. 위 수치는 전 구간을 콜드로 만든 최대 부하 조건입니다.
>
> **주의 두 가지.** 파티션 키 없는 풀스캔 집계는 계층화된 테이블에서 오답을 냅니다 — 청크를
> 병합하지 않는 경로입니다([벤치마크 §주의](doc/timeseries/tiering-benchmark.md) 참고). 그리고
> 저장 절감 폭은 형태에 좌우됩니다 — 접을 상수·null 컬럼이 없는 최소 형태(고엔트로피 `double`
> 1컬럼)는 v4 기준 재측정 전이니, 도입 전 자기 테이블 형태로 재보십시오.

**4. 시계열에 맞는 컴팩션·보존.** 시간 창 단위로 SSTable을 정렬·동결(창당 1개)하고, 보존 만료 창은 컴팩션 없이 통째 삭제합니다. 엣지 장비의 지각 백필도 자기 시간대로 자동 격리됩니다.

**5. 로그·이벤트 본문 검색.** SAI `LIKE` + `index_analyzer`로 한글 포함 부분문자열 검색이 `ALLOW FILTERING` 없이 동작합니다.

## ✨ 구현 기능 (업스트림 대비 이 포크의 델타)

| 기능 | 내용 | 상세 |
| --- | --- | --- |
| **시계열 CQL 함수 21종** | `time_bucket`, `first`/`last`, `delta`/`rate`/`derivative`, 리셋 보정 `counter_delta`/`counter_rate`, `percentile`, `time_weighted_average`, `integral`, `variance`/`stddev`, `histogram`, `approx_count_distinct`, 이변량 `corr`/`covar_*`/`regr_*` | [사용법 §2~9](#시계열-cql-사용법) |
| **Gap-fill** | `GROUP BY time_bucket_gapfill(width, ts, start, finish)` — 빈 버킷 실체화 + `locf()`/`interpolate()` 채움 정책 | [사용법 §3](#3-빈-구간-채우기-time_bucket_gapfill) |
| **풀텍스트 검색** | SAI `LIKE` + `index_analyzer`(ngram/standard/cjk/keyword + JSON) — 단어 중간 조각·공백 걸침·한글까지 진짜 부분문자열 매치, ALLOW FILTERING 불필요 | [fulltext-search.md](doc/timeseries/fulltext-search.md) |
| **시계열 컴팩션 (TSCS)** | `TimeSeriesCompactionStrategy` — 창 정렬 + 창 내부 UCS 위임 + retention 창 통삭제 + 닫힌 창 동결(창당 1 SSTable, `WindowFrozenListener` 이벤트 훅, far-future 가드 `max_future_window`, **동결 시점에** 이미 만료된 TTL 데이터는 retention 없이 회수 — 동결 이후 만료되는 데이터는 `retention` 필요) + 지각 격리(flush/스트리밍 창 경계 스플릿 — 백필이 과거 창에 국소 편입, 레거시 걸침 SSTable 자동 분할) + **전용 memtable**(테이블별 옵트인 — 행을 쓰기 시점에 TSCS 창으로 배정해 flush 라우팅·64 MiB 파티션 상한 제거, 원시 배열 컬럼 저장으로 행당 힙 **5.5×↓** 실측, 계층화 테이블의 콜드 창은 flush 시점에 바로 청크로) | [timeseries-compaction.md](doc/timeseries/timeseries-compaction.md) · [timeseries-memtable.md](doc/timeseries/timeseries-memtable.md) · [설계 스펙](docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md) |
| **컬럼 지향 청크 코덱 (chunk format v4)** *(계층형 저장 1단계)* | 창 1개 = 공유 타임스탬프 축 + 일반 컬럼별 독립 섹션의 무손실 압축, 모든 블록이 독립 디코드·랜덤 접근. `double`은 ALP/ALP-RD(유일한 double 코덱), 정수·시각 계열은 FOR/델타 비트팩, `boolean`은 1비트팩, `text`·불투명 바이트는 사전(DICT)/RAW. 값이 일정한 컬럼은 CONSTANT, 전부 null인 컬럼은 ALL_NULL로 O(1) 처리. 운영 형태 2,000만 건 실측 **~1.7 B/행** (행 저장 11.9 B/행 대비 **7.1×**, 호스트 234) | [포맷 규격](doc/timeseries/chunk-format-v4.md) · [코덱 실측 비교](doc/timeseries/codec-bakeoff.md) · [설계 스펙](docs/superpowers/specs/2026-07-31-industrial-tiered-storage-design.md) |
| **계층형 저장 (청크 스토어)** *(계층형 저장 2단계)* | 테이블 확장 `timeseries_tiering` 정책 — 백그라운드 재인코더가 hot_window를 지난 창을 청크로 압축해 `<테이블>__chunks`로 이동(지각 데이터 병합, cold_window 만료, CL 쿼럼 하한). `nodetool retier`/`tieringstatus`, `system_views.timeseries_tiering`. **투명 읽기(SP3)**: 베이스 테이블 SELECT가 핫 로우+청크를 자동 병합 — 시간범위·포인트·집계·gap-fill·LIMIT/DESC가 핫·콜드에 걸쳐 동작 | [tiered-storage.md](doc/timeseries/tiered-storage.md) |
| **테스트 인프라** | 도커 통합 테스트 93건(릴리스 게이트), 3노드 클러스터 테스트 49건, 1억 건 스케일 하네스, jvm-dtest, JMH 성능 회귀 게이트, GC 비교(ZGC vs G1) | [보고서들](doc/timeseries/) |
| **배포/CI** | Testcontainers 호환 도커 이미지, GitLab CI(빌드→테스트→이미지→통합 게이트→릴리스), 태그 릴리스 자동화 | [.gitlab-ci.yml](.gitlab-ci.yml) |

## 📖 문서

| 문서 | 내용 |
| --- | --- |
| **[사용 예제 (examples.md)](doc/timeseries/examples.md)** | 아래 "시계열 CQL 사용법"의 원본 예제 모음 (영문) |
| [시계열 함수 설계 (timeseries-functions-design.md)](doc/timeseries/timeseries-functions-design.md) | 각 함수의 시그니처·의미론(semantics), 분산 환경에서의 정확성, 코드 위치 |
| [Gap-Fill 설계 (gapfill-design.md)](doc/timeseries/gapfill-design.md) | `time_bucket_gapfill`의 CQL 문법, 보간 규칙, 가드레일 |
| [Continuous Aggregates 설계 (continuous-aggregates-design.md)](doc/timeseries/continuous-aggregates-design.md) | 시간 버킷 롤업(연속 집계) 설계안 — 진행 중 |
| **[풀텍스트 검색 (fulltext-search.md)](doc/timeseries/fulltext-search.md)** | SAI `LIKE` + `index_analyzer` — 로그/메시지 본문 부분문자열 검색 (한글 포함) |
| **[프로덕션 투입 체크리스트 (production-rollout.md)](doc/timeseries/production-rollout.md)**  | 계층화를 실 운영 테이블에 처음 켜기 전 확인 목록 — 되돌릴 수 없는 지점, 스키마 요건, TTL→`cold_window` 이관, 애플리케이션 영향, 검증 절차 |
| **[계층화 벤치마크 (tiering-benchmark.md)](doc/timeseries/tiering-benchmark.md)** | 운영 형태 2,000만 건 전/후 실측 (호스트 234, chunk v4) — 저장 **7.1×↓**, 집계 **3~6×↑**, gap-fill **5.4×↑**, 범위 없는 조회 포함 전 질의 동률 이상, 재인코딩 108k rows/s |
| **[운영 튜닝 가이드 (operations-tuning.md)](doc/timeseries/operations-tuning.md)** | 장기 보존(10년) 전환 실전 가이드 — 용량 산수, 적용 순서, 원본·**청크 테이블** 튜닝값과 근거, TTL과 계층화의 관계, 점검 목록 |
| **[시계열 컴팩션 (timeseries-compaction.md)](doc/timeseries/timeseries-compaction.md)** | `TimeSeriesCompactionStrategy` — 창 크기·동결·`retention` 설정, 창의 일생, 지각 데이터 격리, 파킹된 창의 두 원인과 진단법, TTL이 아니라 `retention`이 만료를 담당하는 이유, **운영 노드 실측** |
| **[시계열 전용 Memtable (timeseries-memtable.md)](doc/timeseries/timeseries-memtable.md)** | `TimeSeriesMemtable` — 켜는 법(yaml 설정 키 + `ALTER TABLE`, 두 단계를 틀리기 쉬운 이유), 지원/미지원 스키마와 폴백 동작, 파킹 원인 제거·행당 힙 5.5× 실측·**스트리밍 읽기**(슬라이스 이진 탐색, 필요한 행만 조립, 보유 0)·콜드 창 청크 직접 flush(내구성 순서), 확인 절차 |
| **[운영 투입 보고서 2026-08-02 (prod-ops-report-2026-08-02.md)](doc/timeseries/prod-ops-report-2026-08-02.md)** | 실 노드 12시간 기록 — 배포 5회, 사고 2건의 전말과 재발 방지, 판단이 뒤집힌 것들, 실측 절차 |
| **[운영 TSCS 설정과 파킹 진단 (prod-tscs-settings.md)](doc/timeseries/prod-tscs-settings.md)** | 75개 테이블의 현재 설정과 근거, 파킹된 창의 두 원인을 가르는 진단 절차, 24k rows/s 유입 중 실측값 |
| **[계층형 저장 (tiered-storage.md)](doc/timeseries/tiered-storage.md)** | `timeseries_tiering` 정책·청크 재인코더 — 설정, 청크 조회 패턴, 운영(nodetool/가상 테이블), 불변식과 제한사항 |
| **[압축 설명 (compression.md)](doc/timeseries/compression.md)** | 컬럼별로 무엇이 왜 얼마나 줄어드는가 — 두 압축 층의 관계, 타입별 인코딩과 행당 비용, 절감폭의 컬럼별 분해(8컬럼 중 4개가 0바이트), 내 테이블 추정 규칙과 실측 방법 |
| **[청크 포맷 v4 (chunk-format-v4.md)](doc/timeseries/chunk-format-v4.md)** | 유일한 청크 포맷의 **와이어 포맷 규격** — 헤더·디렉토리·블록 테이블·presence 4모드·타입별 블록 인코딩(ALP 포함), 결정성 규칙, 크기 한계 (v1~v3는 제거된 포맷 — 읽으면 `UnsupportedChunkFormatException`) |
| [코덱 bake-off (codec-bakeoff.md)](doc/timeseries/codec-bakeoff.md) | double 코덱 실측 비교 — **ALP/ALP-RD 단일화** 결론과 분포별 B/값 (Gorilla·Chimp128 대비) |
| [통합 테스트 보고서](doc/timeseries/integration-test-report.md) | 실제 컨테이너에서 실행한 각 검증의 CQL·결과·소요 시간 |
| [스케일 테스트 보고서 (1억 건)](doc/timeseries/scale-test-report.md) | 1억 행 용량 검증 — 적재·집계의 스캔 행 수 선형성 (구형 호스트 기록; 현재 수치는 계층화 벤치마크) |
| **[읽기/쓰기 처리량 벤치마크 (rw-throughput-benchmark.md)](doc/timeseries/rw-throughput-benchmark.md)** | 초당 처리량 실측 — 적재 **233k rows/s**(호스트 234) · 쓰기 경로 424k rows/s·청크 인코딩 684k rows/s·청크 풀스캔 740µs/3,600행(호스트 237, JMH) · 재인코딩 108k rows/s · 패턴별 읽기 ops/s는 v4 재측정 대기 |
| **[Memtable 쓰기 튜닝 기록 (memtable-write-tuning.md)](doc/timeseries/memtable-write-tuning.md)** | 쓰기 경로 최적화 기록 — DESC가 끄는 두 고속 경로, 설정 튜닝의 한계, 코드 수정 3라운드(min 가드·역순 long 스토어·꼬리 인덱스), ALTER 순서 함정 |
| [SP4 계획 (sp4-plan.md)](doc/timeseries/sp4-plan.md) | SP4 로드맵 — Compressed Query·vectorized 집계 커널·SIMD 등의 삽입 지점·마일스톤·검증 게이트 (진행 상태는 문서 참고) |
| [GC 비교: ZGC generational vs G1](doc/timeseries/gc-comparison.md) | 같은 1억 건 데이터로 두 GC의 쿼리 시간·쓰기 처리량 비교 (원자료) |
| **[아티클: 시계열 DB에서 G1GC vs Generational ZGC](doc/timeseries/g1gc-vs-zgc-article.md)** | 위 측정을 정리한 성능 비교 아티클 (환경·방법·해석·권장 설정) |

전체 문서 디렉터리: [doc/timeseries/](doc/timeseries/)

---

# 시계열 CQL 사용법

별도 설치나 UDF 등록 없이 `cqlsh`에서 바로 쓸 수 있는 네이티브 함수입니다. 아래 예제는 모두 실행 가능한 CQL입니다.

## 함수 레퍼런스

**인자 순서가 중요합니다.** 대부분의 시계열 집계는 `(값, 타임스탬프)` 순서입니다.

| 함수 | 시그니처 | 반환 | 설명 |
| --- | --- | --- | --- |
| `time_bucket` | `time_bucket(duration, ts [, origin])` | `timestamp` | 버킷 시작 시각 (다운샘플링용 스칼라) |
| `time_bucket_gapfill` | `time_bucket_gapfill(width, ts, start, finish)` | `timestamp` | 빈 버킷까지 생성하는 `GROUP BY` 셀렉터 |
| `locf` | `locf(집계)` | 인자와 동일 | 빈 버킷을 직전 값으로 채움 (LOCF) |
| `interpolate` | `interpolate(집계)` | `double` | 빈 버킷을 앞뒤 값의 선형 보간으로 채움 |
| `first` / `last` | `first(value, ts)` / `last(value, ts)` | `value`의 타입 | 시각 기준 최초/최종 값 |
| `delta` | `delta(value, ts)` | `double` | 마지막 − 첫 샘플 |
| `rate` | `rate(value, ts)` | `double` | `delta` ÷ 경과 초 (양 끝점 기준) |
| `derivative` | `derivative(value, ts)` | `double` | 최소제곱 회귀 기울기 (초당) |
| `counter_delta` / `counter_rate` | `counter_delta(value, ts)` / `counter_rate(value, ts)` | `double` | 리셋을 보정한 카운터 증가량 / 초당 증가율 |
| `percentile` | `percentile(value, q)` — `q`는 `[0,1]` | `double` | 정확한 연속 백분위 (선형 보간) |
| `time_weighted_average` | `time_weighted_average(value, ts)` | `double` | 시간 가중 평균 |
| `integral` | `integral(value, ts)` | `double` | 곡선 아래 면적 (value·초) |
| `variance` / `stddev` | `variance(value)` / `stddev(value)` | `double` | 표본 분산 / 표준편차 |
| `corr` / `covar_pop` / `covar_samp` | `corr(y, x)` 등 | `double` | 상관계수 / 모공분산 / 표본공분산 |
| `regr_slope` / `regr_intercept` / `regr_r2` | `regr_slope(y, x)` 등 | `double` | y의 x에 대한 선형 회귀 |
| `histogram` | `histogram(value, min, max, nbuckets)` | `list<bigint>` | 등간격 히스토그램 (길이 `nbuckets+2`) |
| `approx_count_distinct` | `approx_count_distinct(value)` | `bigint` | HyperLogLog 근사 고유값 개수 |

## 1. 스키마와 샘플 데이터

아래 예제는 모두 산업 현장의 실제 태그 테이블 `tm_tag_point` 위에서 돕니다 — 태그당 파티션 하나, 시간으로 클러스터링, **최신 데이터가 앞**(`DESC`). 컴팩션은 이 포크의 시계열 전용 전략 `TimeSeriesCompactionStrategy`(TSCS)를 씁니다 — SSTable을 시간 창으로 정렬하고, 닫힌 창은 창당 1 SSTable로 동결하며, 보존기간이 지난 창은 컴팩션 없이 통째 삭제합니다. 현재 창 내부의 컴팩션 선택은 UCS 컨트롤러에 위임되므로 UCS의 쓰기 최적 특성은 그대로 유지됩니다.

```sql
CREATE KEYSPACE IF NOT EXISTS pp
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pp;

CREATE TABLE tm_tag_point (
    tag_id     text,                              -- 파티션 키: 태그 하나 = 파티션 하나
    timestamp  timestamp,
    area_id    text static, asset_id text static, line_id text static,
    opc_id     text static, site_id  text static, tag_name text static,
    type       text static,                       -- 'boolean' | 'long' | 'double' | ...
    attribute  frozen<map<text,text>>,
    error_code int,
    latency    int,                               -- 수집 지연, 항상 존재
    quality    int,
    value      text,                              -- 판독값의 문자열 사본
    value_boolean boolean,                        -- type='boolean'일 때 채워짐
    value_numeric double,                         -- type이 숫자형일 때 채워짐
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {'class': 'TimeSeriesCompactionStrategy',
                     'window_size': '1d',          -- 시간 창 폭 (계층화 chunk_window와 일치)
                     'freeze_after': '2h',         -- 창이 닫히고 이 시간 후 창당 1 SSTable로 동결
                     'scaling_parameters': 'T4',   -- 현재 창 내부는 UCS 위임 (쓰기 최적 4-way)
                     'retention': '62d'}           -- 창 상한이 62일을 지나면 통째 삭제
   AND default_time_to_live = 5356800;   -- 62일 (retention과 병행 시 먼저 도래하는 쪽 적용)

-- static은 태그당 한 번만 씁니다 (샘플마다가 아니라).
INSERT INTO tm_tag_point (tag_id, area_id, asset_id, line_id, opc_id, site_id, tag_name, type)
     VALUES ('TAG-001', 'A1', 'AS1', 'L1', 'OPC1', 'S1', 'boiler.temp', 'double');

INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 09:05:00+0000', {}, 0,  17, 192, '20.1', 20.1);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 09:35:00+0000', {}, 0, 431, 192, '20.8', 20.8);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 10:15:00+0000', {}, 0,   3, 192, '21.4', 21.4);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 10:45:00+0000', {}, 0, 902, 192, '22.0', 22.0);
```

### 1.0 어느 컬럼을 집계할 수 있나 — 이 스키마 최대의 함정

**`value`는 `text`라서 수치 집계가 불가능합니다.** `avg(value)`·`percentile(value, 0.95)`·`delta(value, timestamp)`는 전부 거부됩니다(수치 타입만 허용: `tinyint`/`smallint`/`int`/`bigint`/`varint`/`float`/`double`/`decimal`/`counter`). 거부보다 위험한 건 **통과하는 쪽**입니다 — `min(value)`/`max(value)`/`count(value)`는 `text`에도 동작하지만 **사전순**으로 비교하므로, `'9.1'`과 `'20.76'` 중 `max`는 `'9.1'`입니다.

산술이 가능한 컬럼은 `latency`(`int`, 항상 존재 — 예제·스모크 테스트의 기본값), `value_numeric`(`double`, 수치형 태그에서만), 그리고 상수인 `error_code`·`quality`입니다. 예외는 `first`/`last`/`approx_count_distinct` — 타입을 가리지 않으므로 `first(value, timestamp)`는 `text`를 그대로 돌려줍니다.

### 1.1 TSCS 컴팩션 옵션 요약

| 옵션 | 설명 |
| --- | --- |
| `window_size` | 시간 창 폭 (`<정수><m\|h\|d>`). 계층형 저장의 `chunk_window`와 맞추길 권장 |
| `freeze_after` | 창이 닫힌 뒤 이 시간이 지나면 동결 대상 — 지각 데이터 유예 기간 |
| `scaling_parameters`, `target_sstable_size` | 현재 창 내부 UCS 위임 파라미터 (UCS 문법 그대로: `T4` = 쓰기 최적 4-way tiered 등) |
| `retention` | 선택 — 창 상한이 `now - retention`을 지나면 SSTable 통째 삭제 (`window_size + freeze_after` 이상) |
| `max_future_window` | 미래 타임스탬프 가드(기본 `1d`) — 오입력이 창을 오염시키지 않게 격리 |

동결(창당 1 SSTable) 시점에 이미 만료된 TTL 데이터는 `retention` 없이도 회수되고, 파티션+시간범위 조회의 읽기 증폭이 최소화됩니다. 다만 창이 SSTable 1개로 줄면 다시는 동결 후보가 되지 않으므로, **동결 이후에 만료되는 데이터는 `retention`을 설정해야 회수됩니다.** 지각(백필) 데이터는 flush 시 창 경계에서 분리되어 자기 창에 국소 편입됩니다. 상세: [§11 시계열 컴팩션 설정](#11-시계열-컴팩션tscs-설정)

## 2. 버킷팅 · 다운샘플링 — `time_bucket`

### 2.1 각 행에 버킷 붙이기 (스칼라로 사용)

```sql
SELECT timestamp, time_bucket(1h, timestamp) AS bucket, latency, value
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

기간 인자는 **따옴표 없는 CQL duration 리터럴**(`1h`)입니다 — `time_bucket('1h', ts)`처럼 문자열로 주면 시그니처가 맞지 않아 실패합니다. 반면 origin/start/finish는 `timestamp`라서 따옴표를 씁니다.

### 2.2 `GROUP BY`로 고정 간격 다운샘플링

```sql
-- 시간별 평균 / 최소 / 최대 / 개수 (수집 지연)
SELECT time_bucket(1h, timestamp) AS bucket,
       avg(latency), min(latency), max(latency), count(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- 판독값 자체 (수치형 태그)
SELECT time_bucket(1h, timestamp) AS bucket, avg(value_numeric)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- 다른 간격: 5분, 1일
SELECT time_bucket(5m, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(5m, timestamp);

SELECT time_bucket(1d, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1d, timestamp);
```

`time_bucket`은 `GROUP BY`의 **마지막 요소**(파티션 키 컬럼 뒤)로 와야 그룹핑이 읽기 경로로 내려갑니다. `DESC` 테이블에서도 그대로 동작하며, 버킷이 최신순으로 나올 뿐입니다. `int` 컬럼의 `avg`는 `int`로 절삭된다는 업스트림 규칙이 그대로 적용됩니다.

### 2.3 기준점(origin)을 옮긴 버킷

```sql
-- 30분 밀린 1시간 버킷: [08:30, 09:30), [09:30, 10:30), ...
SELECT time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000') AS bucket, avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000');
```

## 3. 빈 구간 채우기 — `time_bucket_gapfill`

일반 `time_bucket`은 **데이터가 있는 버킷만** 반환합니다. `time_bucket_gapfill`은 `[start, finish)` 범위의 **모든** 버킷에 대해 행을 만들어 주므로, 대시보드가 끊김 없는 시간축을 얻습니다. 데이터가 없는 버킷의 집계값은 기본적으로 null입니다.

> **⚠️ `DESC` 테이블에서는 `ORDER BY timestamp ASC`가 필수입니다.** gap-fill의 densify는 버킷이 **오름차순**으로 도착한다고 가정하는데, 이를 강제하는 검사가 없습니다. `DESC` 클러스터링 테이블에서 정렬 없이 실행하면 행이 최신순으로 들어와 채움이 거꾸로 적용되며 **에러도 나지 않습니다**. `ORDER BY timestamp ASC`를 붙이면 `DESC` 선언과 `ASC` 요청이 상쇄되어 읽기 자체가 오름차순이 되고, 이것이 densify가 원하는 형태입니다. 이 조합은 아직 테스트로 덮여 있지 않습니다 — [gapfill-design.md §4](doc/timeseries/gapfill-design.md) 참고.

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`WHERE timestamp >= <start>`는 장식이 아닙니다 — 스캔되는 행 중 gap-fill `start`보다 **오래된** 것이 하나라도 있으면 *"The floor function starting time is greater than the provided time"*으로 실패합니다.

### 3.1 `locf()` — 직전 값 이어가기

집계를 `locf(...)`로 감싸면 빈 버킷이 null 대신 **직전 비어있지 않은 버킷의 값**을 그대로 이어받습니다(last-observation-carried-forward).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       locf(avg(latency))   -- 빈 버킷은 직전 시간의 평균을 반복
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`locf`는 실제 데이터가 있는 행에는 아무 영향이 없습니다(인자를 그대로 반환). 첫 실제 값보다 앞선 버킷은 이어받을 값이 없으므로 null로 남습니다.

### 3.2 `interpolate()` — 선형 보간

빈 버킷을 앞뒤 값 사이에서 선형 보간하려면 `interpolate(...)`를 씁니다(결과 타입은 `double`).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       interpolate(avg(value_numeric))   -- 빈 버킷은 양옆 값 사이를 직선으로 채움
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

첫 실제 값 이전 / 마지막 실제 값 이후의 빈 버킷은 보간할 대상이 없으므로 null로 남습니다.

### 3.3 여러 태그

태그별로 독립적으로 채워집니다. 파티션 키를 `SELECT`와 `GROUP BY` 양쪽에 포함하세요.

```sql
SELECT tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point WHERE tag_id IN ('TAG-001', 'TAG-002')
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`IN` + `ORDER BY`는 파티션을 **가로지르는** 후처리 정렬을 걸기 때문에 결과가 태그별이 아니라 버킷 기준 전역 순서로 나옵니다. 이 스키마에서는 태그 하나씩 도는 쪽이 다루기 쉽습니다.

### 3.4 제약 사항

- 폭(width)은 **고정 길이**여야 합니다(월 단위 성분 불가).
- 버킷 컬럼과 `locf(...)`/`interpolate(...)`에 **별칭(alias)을 붙이지 마세요** — 결과 메타데이터의 함수 이름으로 찾기 때문에, 별칭을 붙이면 gap-fill이 조용히 아무 일도 하지 않습니다.
- 채울 집계는 **수치 컬럼**이어야 합니다 — `latency` 또는 `value_numeric`, `value`(text)는 불가.
- 버킷 범위를 가로지르는 페이징은 피하세요.
- 범위 ÷ 폭이 **1,000,000 버킷**을 넘으면 쿼리가 거부됩니다.

## 4. 최초/최종 값 — `first`, `last`

`first`/`last`는 값 타입을 가리지 않으므로 `text`인 `value`에도 그대로 쓸 수 있습니다 — 이 컬럼으로 서버에서 할 수 있는 몇 안 되는 일 중 하나입니다.

```sql
-- 태그의 최초/최종 판독값 (text 그대로 반환)
SELECT first(value, timestamp) AS first_reading,
       last(value, timestamp)  AS last_reading
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

시간별 OHLC(시가/고가/저가/종가) 캔들 — 여기서는 **수치 컬럼**을 써야 합니다. `text`에 `max`/`min`을 걸면 사전순 비교가 됩니다:

```sql
SELECT time_bucket(1h, timestamp) AS bucket,
       first(value_numeric, timestamp) AS open,
       max(value_numeric)              AS high,
       min(value_numeric)              AS low,
       last(value_numeric, timestamp)  AS close
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

`first`/`last`는 **삽입 순서가 아니라 타임스탬프 인자 기준**으로 정렬하므로, 순서가 뒤바뀐 쓰기가 있어도, 그리고 `DESC` 클러스터링이어도 시가/종가가 정확합니다.

## 5. 변화량 — `delta`, `rate`, `derivative`

```sql
SELECT time_bucket(1h, timestamp) AS bucket,
       delta(value_numeric, timestamp)      AS change,
       rate(value_numeric, timestamp)       AS per_second,
       derivative(value_numeric, timestamp) AS slope_per_second
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

- `delta` = 버킷 내 마지막 샘플 − 첫 샘플
- `rate` = `delta` ÷ 경과 초 (양 끝점 기준 변화율)
- `derivative` = 최소제곱 회귀 기울기. 모든 점을 사용하므로 시계열이 비선형일 때 `rate`와 값이 달라집니다.

두 번째 인자는 `timestamp` 또는 `bigint`(epoch 밀리초)여야 합니다 — `int`·`timeuuid` 시각 컬럼은 거부됩니다. 수치형이 아닌 태그라면 `value_numeric` 대신 `latency`를 넣으세요.

### 5.1 리셋 보정 처리량 — `counter_rate`

`counter_delta`/`counter_rate`는 **수치 컬럼**이면 되고 CQL `counter` 타입을 요구하지 않습니다 — 단조 증가하는 `int`/`bigint` 게이지면 충분하며, 테스트가 덮고 있는 것도 이 형태입니다. 리셋 가능성이 있으면 `rate()` 대신 이쪽을 쓰세요. `rate()`는 리셋을 큰 음수 계단으로 오해합니다.

```sql
CREATE TABLE tag_counters (
    tag_id text, timestamp timestamp, total bigint,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- 분당 초당 이벤트 수 (리셋 보정)
SELECT time_bucket(1m, timestamp) AS minute, counter_rate(total, timestamp) AS per_sec
FROM   tag_counters
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1m, timestamp);
```

`tm_tag_point`에는 단조 증가 컬럼이 없어 별도 테이블을 씁니다. CQL `counter` **타입**을 쓴 테이블은 **계층화가 아예 불가능**하다는 점도 함께 고려하세요 — 재인코더가 행을 삭제 후 재삽입하는데 삭제된 카운터는 다시 쓸 수 없기 때문입니다. `bigint` 게이지가 계층화 호환 선택지입니다.

## 6. 백분위 · SLO — `percentile`

```sql
-- 분당 p50 / p95 / p99 지연시간
SELECT time_bucket(1m, ts) AS minute,
       percentile(latency_ms, 0.50) AS p50,
       percentile(latency_ms, 0.95) AS p95,
       percentile(latency_ms, 0.99) AS p99
FROM   latencies
WHERE  service = 'checkout'
GROUP  BY service, time_bucket(1m, ts);

-- 전체 구간 중앙값
SELECT percentile(value, 0.5) AS median FROM metrics WHERE series = 'cpu';
```

`percentile`은 인접 값 사이를 선형 보간하는 **정확한** 연속 백분위입니다(`q`는 0~1). 그룹의 값을 메모리에 유지하므로, 무제한 스캔보다는 크기가 제한된 다운샘플 버킷에 적합합니다.

## 7. 분포 · 산포 · 카디널리티

```sql
-- 시간 가중 평균: 각 값이 유효했던 시간만큼 가중.
-- 샘플 간격이 불규칙할 때는 avg() 대신 이것을 쓰세요.
SELECT time_bucket(1h, ts) AS bucket, time_weighted_average(value, ts) AS twa
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- 곡선 아래 면적 (value·초). 예: 전력(W) → 에너지(J)
SELECT time_bucket(1h, ts) AS bucket, integral(value, ts) AS area
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- 버킷별 산포
SELECT time_bucket(1h, ts) AS bucket, variance(value) AS var, stddev(value) AS sd
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- [0, 1000) ms를 10개 등간격 버킷으로 나눈 히스토그램.
-- 결과 리스트: [ <0ms, bucket1, .. bucket10, >=1000ms ]
SELECT histogram(latency_ms, 0, 1000, 10) AS dist
FROM   latencies WHERE service = 'checkout';

-- 분당 고유 클라이언트 IP 근사 개수 (HyperLogLog, 메모리 사용량 고정)
SELECT time_bucket(1m, ts) AS minute, approx_count_distinct(client_ip) AS unique_ips
FROM   requests WHERE service = 'api' GROUP BY service, time_bucket(1m, ts);
```

## 8. 이변량 통계 · 회귀

두 컬럼 사이의 관계를 서버에서 바로 계산합니다. 인자 순서는 `(y, x)` — y가 종속변수입니다.

```sql
-- 온도와 전력 사용량의 상관관계 (시간별)
SELECT time_bucket(1h, ts)          AS bucket,
       corr(power, temperature)     AS r,
       covar_samp(power, temperature) AS cov,
       regr_slope(power, temperature) AS slope,      -- 1도당 전력 증가량
       regr_intercept(power, temperature) AS intercept,
       regr_r2(power, temperature)  AS r_squared
FROM   sensors
WHERE  site = 'plant1'
GROUP  BY site, time_bucket(1h, ts);
```

## 9. 대시보드 종합 쿼리

버킷팅, OHLC, 변화량, 백분위를 한 번에:

```sql
SELECT time_bucket(1h, ts)     AS bucket,
       count(value)            AS samples,
       first(value, ts)        AS open,
       last(value, ts)         AS close,
       min(value)              AS low,
       max(value)              AS high,
       avg(value)              AS mean,
       delta(value, ts)        AS change,
       rate(value, ts)         AS per_second,
       percentile(value, 0.95) AS p95
FROM   metrics
WHERE  series = 'cpu'
  AND  ts >= '2024-01-01 00:00:00+0000'
  AND  ts <  '2024-01-02 00:00:00+0000'
GROUP  BY series, time_bucket(1h, ts);
```

## 10. 풀텍스트 검색 — SAI `LIKE` + `index_analyzer`

로그·이벤트 메시지 본문을 시계열 조회 패턴 안에서 검색합니다. `ngram` 분석기가 진짜 부분문자열 매치를 제공합니다(단어 중간 조각, 공백 걸침, 한글 전부 지원). 자세한 내용: [fulltext-search.md](doc/timeseries/fulltext-search.md)

```sql
CREATE TABLE logs (
    device text, ts timestamp, msg text,
    PRIMARY KEY (device, ts)
) WITH CLUSTERING ORDER BY (ts ASC);

CREATE INDEX logs_msg_idx ON logs(msg) USING 'sai'
  WITH OPTIONS = { 'index_analyzer': 'ngram' };

-- 장비 1대 · 1시간 구간에서 본문 검색 (ALLOW FILTERING 불필요)
SELECT ts, msg FROM logs
 WHERE device = 'pump-01'
   AND ts >= '2026-07-31 00:00' AND ts < '2026-07-31 01:00'
   AND msg LIKE '%타임아웃%';

-- 단어 중간 조각도 매치: '%imeou%' 가 "timeout" 을 찾음
-- 접두/접미/완전일치: 'connection%', '%9042', LIKE 'connection refused'
-- 다중 조각 AND: msg LIKE '%connection%' AND msg LIKE '%refused%'

-- 시계열 함수와 조합: 5분 버킷별 에러 건수
SELECT time_bucket(5m, ts), count(*) FROM logs
 WHERE device='pump-01' AND ts >= ? AND ts < ? AND msg LIKE '%timeout%'
 GROUP BY device, time_bucket(5m, ts);
```

동작 원리: 값 전체를 2~3글자 n-gram으로 색인(재현율) → 그램 교집합으로 후보 추출 → **원문에 LIKE 패턴 재적용**(정밀도). 색인은 원본 컬럼의 수 배 크기가 되므로 로그성 테이블에 선별 적용하세요. 2글자 미만 조각은 명시적 에러로 거부됩니다. `=`는 완전일치 의미를 유지합니다.

## 11. 시계열 컴팩션(TSCS) 설정

TWCS의 시간 정렬·통삭제와 UCS의 창 내부 컴팩션을 결합한 전용 전략입니다. 테이블 생성(또는 ALTER) 시 지정합니다:

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d',           -- 시간 창 폭 (계층화 chunk_window와 반드시 일치)
  'freeze_after': '2h',          -- 창이 닫히고 이 시간이 지나면 동결(창당 1 SSTable로 수렴)
  'scaling_parameters': 'T4',    -- 현재 창 내부는 UCS에 위임 (UCS 문법 그대로)
  'target_sstable_size': '256MiB',
  'retention': '62d',            -- 선택: 창 상한이 now-62d를 지나면 컴팩션 없이 통째 삭제
  'max_future_window': '1d'      -- 선택: 미래 타임스탬프 가드 (기본 1d)
};
```

- 닫힌 창은 자동으로 **창당 1 SSTable**로 동결되어 읽기 증폭이 최소화되고, 동결 시점에 이미 만료된 TTL 데이터는 retention 없이 회수됩니다. 동결 이후에 만료되는 데이터의 회수는 `retention`이 담당합니다.
- 지각(백필) 데이터는 flush/스트리밍 시 창 경계에서 분리되어 **자기 창에 국소 편입**됩니다 — 현재 창 컴팩션을 오염시키지 않습니다.
- 상세: [설계 스펙](docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md)

## 12. 계층형 저장(tiered storage) 설정

오래된 창을 컬럼 지향 청크(일반 컬럼 전부를 창의 타임스탬프 축 하나에 담습니다)로 압축해 `<테이블>__chunks`로 옮기고, **SELECT는 그대로**(투명 읽기가 핫+콜드 자동 병합) 쓰는 기능입니다. 테이블 `extensions`에 JSON 정책을 문자열로 넣습니다:

### 12.1 대상 스키마

**시간축(`timestamp` 클러스터링 컬럼)이 하나인 시계열 테이블이면 형태를 가리지 않습니다.** 파티션 키는 복합이어도 되고, 일반 컬럼은 개수·타입 무관, static 컬럼은 몇 개든 그대로 보존됩니다 (static 셀은 청크화 대상이 아니고, 재인코더의 클러스터링 레인지 딜리트가 건드리지 않습니다).

아래는 릴리스 게이트가 실제로 계층화를 검증하는 산업 현장 테이블입니다 — static 7개 + 일반 컬럼 8개, `DESC` 클러스터링:

```sql
CREATE TABLE pp.tm_tag_point (
    tag_id     text,                              -- 파티션 키: 개수 무관 (복합 키 가능)
    timestamp  timestamp,                         -- 클러스터링 1개, timestamp (ASC/DESC 모두 가능)
    area_id    text static, asset_id text static, line_id text static,
    opc_id     text static, site_id  text static, tag_name text static,
    type       text static,                       -- static: 개수·타입 무관, 계층화 후에도 그대로 보존
    attribute  frozen<map<text,text>>,            -- 항상 {} → CONSTANT (0바이트)
    error_code int,                               -- 항상 0 → CONSTANT
    latency    int,                               -- 고엔트로피 → zigzag varint 델타
    quality    int,                               -- 항상 192 → CONSTANT
    value      text,                              -- 판독값의 문자열 사본
    value_boolean boolean,                        -- type=boolean 태그에서만 채워짐
    value_numeric double,                         -- type이 숫자형일 때만 채워짐
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**`DESC` 클러스터링이 산업 현장의 기본 관용구**입니다(최신부터 읽는 조회가 압도적). 투명 읽기의 경계 산술이 오름차순을 가정하면 콜드 행 0개를 **에러 없이** 돌려주므로, 양쪽 바운드와 양쪽 정렬이 통합 테스트에 고정돼 있습니다.

판독값이 `value_boolean`에 들어가는지 `value_numeric`에 들어가는지는 **static `type`이 정합니다.** static이라 태그 단위로 고정되고, 청크 1개 = 태그 1개 × 창 1개이므로 한 청크 안에서 각 값 컬럼은 전부 채워져 있거나 전부 비어 있거나 둘 중 하나입니다 — 쓰이는 쪽은 전용 코덱, 쓰이지 않는 쪽은 ALL_NULL로 0바이트. `value`(text)는 그 판독값의 문자열 사본입니다(`value_numeric = 20.76` ↔ `value = '20.76'`).

지원되지 않는 형태에 정책을 걸면 60초마다 **사유를 밝힌** ERROR 로그를 남기고 건너뜁니다. 거부 대상은 다섯 가지뿐입니다:

| 형태 | 사유 |
| --- | --- |
| `counter` 컬럼 | 재인코더는 행을 삭제 후 재삽입하는데, 삭제된 카운터는 영구히 다시 쓸 수 없습니다 |
| 비frozen 컬렉션 **일반** 컬럼 | 멀티셀 값은 청크로 인코딩할 수 없습니다 — `frozen<...>`으로 감싸면 지원됩니다 |
| 클러스터링이 0개·2개 이상이거나 `timestamp`가 아님 | 인코딩할 시간축이 없습니다 |
| **static이 아닌 컬럼**에 걸린 보조 인덱스(SAI 포함) | 인덱스 엔트리는 행 단위라, 재인코딩된 행이 사라지면 인덱스 질의가 콜드 데이터를 조용히 누락합니다 — 일반 컬럼·클러스터링 컬럼·복합 파티션 키 구성 컬럼 모두 해당. **static 컬럼 인덱스만** 무방 |
| 이 테이블 위의 머티리얼라이즈드 뷰 | 투명 읽기는 베이스 테이블만 복원하므로 뷰가 오래된 이력을 영구히 잃습니다 |

> `default_time_to_live`가 있고 `hot_window >= TTL`이면 재인코더가 데이터를 보기 전에 TTL이 먼저 지워 **아무것도 압축되지 않습니다**. 거부하지는 않지만 두 값을 밝힌 WARN을 남깁니다.

> **⚠️ 계층화 켜기 전에 반드시 알아야 할 두 가지 (상세: [tiered-storage.md §1.1, §5.1.2](doc/timeseries/tiered-storage.md))**
> 1. **콜드 데이터는 불변입니다** — 핫 윈도를 넘어서는 `DELETE`(셀·행·레인지·파티션)와 `SET col = null`은 **거부**됩니다. 청크가 유일한 사본이라 톰스톤은 `gc_grace` 때까지만 가려주고 그 뒤 데이터가 되살아나기 때문입니다. 콜드 데이터 삭제는 `cold_window` 만료를 쓰십시오. 핫 윈도 안의 삭제, 그리고 콜드 구간에 **값을 쓰는** 지각 `UPDATE`는 그대로 동작합니다.
> 2. **TTL은 청크화되면서 사라집니다** — `default_time_to_live`로 만료시키던 데이터가 청크로 옮겨지면 영구 보존됩니다. TTL에 의존했다면 같은 기간을 `cold_window`에 설정하십시오.

재인코더는 **일반 컬럼 전부**를 청크 1개에 담습니다: 창의 타임스탬프 축을 한 번만 저장하고 컬럼마다 독립 섹션에 **직렬화 바이트 그대로** 넣으므로, `double`/`boolean`/`int`/`bigint`/`timestamp`/`date`/`text`는 전용 코덱을, 나머지(`blob`·`uuid`·`timeuuid`·frozen 컬렉션 등)는 불투명 바이트 폴백을 탑니다. `null` 셀은 `null`로 그대로 왕복하고, 값이 전부 같은 컬럼은 O(1)(0바이트)로 접힙니다. 자세한 표는 [tiered-storage.md §3.1.1](doc/timeseries/tiered-storage.md) 참고.

### 12.2 압축 켜기 — CQL 한 줄

정책 JSON을 테이블 `extensions`에 그대로 넣으면 끝입니다 (hex 변환 불필요):

```sql
ALTER TABLE pp.tm_tag_point WITH extensions = {
  'timeseries_tiering': '{"hot_window":"2d","chunk_window":"1d","cold_window":"3650d","interval":"1h"}'
};

-- 적용 확인 (정책과 실행 통계가 함께 보입니다)
SELECT * FROM system_views.timeseries_tiering;
```

> `extensions`는 스키마상 blob 맵이지만, 이 포크는 **평문 문자열을 UTF-8 바이트로 저장**합니다.
> `0x`로 시작하는 값만 hex 블롭으로 해석하므로 기존 hex 표기(`0x7b22...`)도 그대로 동작합니다.

적용 후에는 60초 스위퍼가 `interval` 주기로 알아서 압축합니다. **바로 확인하고 싶으면** 수동으로 한 사이클 실행:

```bash
nodetool retier pp tm_tag_point   # 1회 재인코딩 (동기 실행, 청크 테이블이 이때 자동 생성됨)
nodetool tieringstatus            # 테이블별 정책·마지막 실행·누적 통계
```

```sql
-- 압축 결과 확인: 청크가 생기고, SELECT 결과는 그대로 (투명 읽기)
SELECT count(*) FROM pp.tm_tag_point__chunks WHERE tag_id='TAG-001';
SELECT count(*) FROM pp.tm_tag_point
 WHERE tag_id='TAG-001' AND timestamp >= '2026-07-01 00:00:00+0000'
                        AND timestamp <  '2026-07-02 00:00:00+0000';   -- 압축 전과 동일한 값

-- static은 청크화 대상이 아니라 계층화 후에도 베이스 테이블에 그대로 남습니다
SELECT site_id, tag_name, type FROM pp.tm_tag_point WHERE tag_id='TAG-001' LIMIT 1;
```

> 계층화 여부를 세어서 확인할 때는 **반드시 클러스터링 범위를 거세요.** 클러스터링 행이 하나도 남지 않은 파티션에 범위 없이 `count(*)`를 하면 static만 있는 행 때문에 `1`이 나옵니다. 그리고 파티션 키 없는 풀스캔 `SELECT count(*)`는 애초에 병합하지 않으므로(범위 스캔은 핫 로우만 봅니다) 전량 청크화된 테이블에서 **0**을 돌려줍니다.

### 12.3 정책 필드

| 필드 | 의미 |
| --- | --- |
| `hot_window` | **이 기간 안의 데이터는 건드리지 않습니다**(행 그대로). 실시간 조회·수정이 잦은 구간보다 넉넉히 잡으세요 (예: `7d`) |
| `chunk_window` | 청크 1개가 담는 시간 폭 (최대 `31d`). TSCS `window_size`와 맞추길 권장. 1초 주기 데이터면 `1h`(=3,600샘플)가 무난 |
| `cold_window` | 선택 — 이 기간을 지난 청크는 통째 삭제(보존 정책). 미지정(`-1`)이면 영구 보관 |
| `interval` | 백그라운드 재인코딩 주기 (예: `1h`). 60초 스위퍼가 주기 도래 테이블만 처리 |
| `consistency` | 재인코더 CL — `LOCAL_QUORUM`(기본) / `QUORUM` / `EACH_QUORUM` / `ALL`만 허용 (약한 CL은 데이터 유실 위험이라 차단) |

**코덱 선택은 없습니다**: `double`은 ALP/ALP-RD가 유일한 청크 코덱이라 고를 것이 없습니다 (예전 `codec` 옵션은 제거됐고, 남아 있으면 `ALTER TABLE`이 거부합니다). 값이 거의 변하지 않는 상수 계열은 코덱을 타기 전에 컬럼 지향 청크의 CONSTANT 플래그가 O(1)로 처리합니다 — [실측](doc/timeseries/codec-bakeoff.md) 참고.

### 12.4 끄기·바꾸기

```sql
-- 정책 변경: 같은 방식으로 새 JSON을 넣으면 다음 사이클부터 적용
-- 완전히 끄기: 확장에서 키 제거 (이미 만들어진 청크는 그대로 남습니다)
ALTER TABLE pp.tm_tag_point WITH extensions = {};
```

**정책을 제거해도 이미 청크에 들어간 데이터는 계속 보입니다.** 투명 읽기의 병합 판단은 현재 정책이 아니라 **실제 청크 커버리지**를 기준으로 하므로, 확장 제거는 *새 인코딩을 멈출 뿐*입니다. 같은 이유로 그 구간의 `DELETE`도 계속 거부됩니다(콜드 불변성). `hot_window`를 늘리거나 `chunk_window`를 줄여도 마찬가지로 과거 데이터가 숨겨지지 않습니다. 콜드 데이터를 실제로 없애는 방법은 `cold_window` 만료 또는 청크 테이블 `DROP`뿐입니다.

운영 참고: 지각 데이터는 이미 청크화된 창에 들어와도 다음 사이클에 자동 병합됩니다(같은 타임스탬프면 나중에 들어온 행이 이김). 상세·제한사항(범위 스캔·페이징 등): [tiered-storage.md](doc/timeseries/tiered-storage.md) · 실측: [벤치마크](doc/timeseries/tiering-benchmark.md)

## 13. 운영 팁

- **항상 파티션을 지정하세요**(`WHERE series = ...`) 그리고 시간 범위도 함께. 시계열 스캔은 `ts`로 정렬된 단일 파티션 안에서 가장 저렴합니다.
- **파티션 크기를 제한하세요.** 고빈도 시리즈라면 파티션 키에 굵은 시간 버킷을 넣어 무한정 커지는 파티션을 막습니다. 예: `PRIMARY KEY ((series, day), ts)`.
- **TSCS 컴팩션**(`TimeSeriesCompactionStrategy`)을 쓰세요 — 창 정렬·동결·통삭제가 시계열에 맞게 자동화되고, 현재 창 내부는 UCS(`scaling_parameters: 'T4'`)에 위임됩니다. 보존은 `retention`(창 통삭제) 또는 `default_time_to_live`로 지정하면 됩니다.
- `time_bucket(interval, ts)`는 `GROUP BY`의 마지막 요소(파티션 키 컬럼들 뒤)여야 그룹핑이 읽기 경로로 푸시다운됩니다.

---

## 빌드

요구 사항: **Java 21**, Ant 1.10 이상(테스트 실행 시 ant-junit 포함). `modules/accord`는 git 서브모듈이므로 `git submodule update --init`이 필요합니다.

```bash
.build/sh/ai-build     # clean + jar + checkstyle -> build/apache-cassandra-6.0.0.jar
```

빌드 산출물은 항상 `apache-cassandra-6.0.0.jar`입니다(`base.version`이 6.0.0으로 고정되어 있습니다).

> `ant`이 PATH에 없는 머신에서는 `ai-build`가 **아무것도 빌드하지 않고** `BUILD SUCCESSFUL`을 출력합니다 — 로그 요약기가 빈 입력에 그렇게 찍습니다. 그런 환경에서는 CI 컨테이너로 빌드하십시오: `.build/sh/ai-build-image` 후 `.build/sh/ai-in-container '<명령>'`, 게이트 전체는 `.build/sh/ci-local`. 확인은 로그가 아니라 jar의 타임스탬프로 하십시오.

## 검증

`.build/sh/ci-local`이 릴리스 게이트의 스테이지를 같은 순서로 로컬에서 돕니다 — jar+checkstyle → 포크 테스트 클래스 → 도커 이미지 → 통합 테스트.

```bash
.build/sh/ci-local                  # 3노드 클러스터 테스트까지 포함하려면 --with-cluster
.build/sh/ci-local --stage image    # 단일 스테이지: jar | tests | image | integration | cluster
```

## 통합 테스트 (릴리스 게이트)

유닛 테스트는 함수를 프로세스 안에서 검증하지만, [docker/integration-test.sh](docker/integration-test.sh)는 **실제 이미지를 띄워** 스키마 생성부터 읽기 경로·집계·네이티브 프로토콜까지 통과하는 시계열 CQL 결과를 손으로 계산한 값과 대조합니다(93개 검증, 프로세스 재시작 포함).

```bash
docker build -t cassandra-timeseries:6.0.0 -f docker/Dockerfile .
./docker/integration-test.sh cassandra-timeseries:6.0.0     # CONTAINER_RUNTIME=podman 도 지원
```

실행하면 항목·CQL·결과가 그대로 출력되고, `build/timeseries-it-report.html`(+ 같은 내용의 `.md`)에 보고서가 생성됩니다. **실행 결과 예시: [통합 테스트 보고서](doc/timeseries/integration-test-report.md)** — 각 검증의 CQL·응답·소요 시간이 그대로 들어 있습니다.

CI에서는 태그를 밀면 `docker-image → docker-integration-test → docker-image-publish + release` 순서로 자동 실행되며, **이 테스트가 통과해야만** 이미지 배포와 릴리스가 진행됩니다. 기본 브랜치에서는 이미지 빌드 비용 때문에 수동(manual) 실행입니다.

### 클러스터 테스트 (3노드)

[docker/cluster-test.sh](docker/cluster-test.sh)는 도커 네트워크 위에 실제 컨테이너 3개를 RF=3으로 띄워 49개를 검증합니다 — 단일 노드가 닿지 못하는 것들입니다: 코디네이터 3개 각각을 통한 집계·gap-fill, 레플리카마다 독립적으로 일어나는 TSCS 동결 수렴, OS 프로세스 간 실제 repair 스트리밍, 레플리카를 정말 정지시킨 상태의 QUORUM.

```bash
./docker/cluster-test.sh cassandra-timeseries:6.0.0
```

CI에서는 수동입니다(2G JVM 3개가 공용 러너에 안 들어갈 수 있음). 컴팩션·스트리밍·repair·계층화를 건드린 릴리스라면 손으로 한 번 돌리십시오.

### 성능 회귀 게이트

[.build/sh/ci-perf](.build/sh/ci-perf)가 청크 코덱·커서 JMH 클래스를 돌려 기록된 baseline과 비교하고, 임계값을 넘는 회귀에서 실패합니다.

```bash
.build/sh/ci-perf                   # doc/timeseries/perf-baseline.json 과 비교
.build/sh/ci-perf --record          # 기준선을 의도적으로 옮길 때만
```

3회 스윕의 최소값을 취하며(공용 머신에서 1회 스윕은 측정이 되지 못합니다), baseline을 뜬 호스트가 아니면 판정하지 않고 보고만 합니다.

### 스케일 테스트 (1억 건)

[docker/scale-test.sh](docker/scale-test.sh)는 컨테이너 노드에 대량 데이터를 적재하고 각 시계열 쿼리의 **CQL 실행 시간**을 측정합니다. 적재와 쿼리 모두 컨테이너 안에서 cqlsh 번들 파이썬 드라이버로 수행하므로(→ [docker/scale-workload.py](docker/scale-workload.py)) 측정값에 cqlsh 기동 시간이 섞이지 않습니다.

```bash
SCALE_ROWS=100000000 SCALE_SERIES=1000 SCALE_LOADERS=16 SCALE_HEAP=16G \
  ./docker/scale-test.sh cassandra-timeseries:6.0.0
# 적재된 데이터를 재사용해 쿼리만 다시 재기: SCALE_SKIP_LOAD=1
```

GC를 바꿔 비교할 수도 있습니다 — `SCALE_GC=g1`(기본은 `zgc`, `conf/jvm21-server.options`에 이미 generational ZGC가 켜져 있음), `SCALE_PASSES=2`(웜업 후 측정), `SCALE_WBENCH_ROWS=10000000`(쓰기 벤치). 두 실행 결과를 `docker/gc-compare.py <prefix-a> <prefix-b>`에 넣으면 비교표가 나옵니다 → **[GC 비교 결과](doc/timeseries/gc-comparison.md)**.

결과는 `build/timeseries-scale-report.html`(+ 같은 내용의 `.md`)에 생성됩니다. 용량 검증 기록: **[스케일 테스트 보고서 (1억 건)](doc/timeseries/scale-test-report.md)** — 1억 행 완주와 스캔 행 수 선형성. 현재 기준 성능 수치는 [계층화 벤치마크](doc/timeseries/tiering-benchmark.md)에 있습니다.

주의: 수백만 행 이상을 집계하려면 서버 타임아웃을 올려야 합니다. `read/range_request_timeout`뿐 아니라 **`native_transport_timeout`(기본 12초)** 이 요청 전체를 자르므로 이 값도 함께 올려야 하며, 이 키는 기본 `cassandra.yaml`에 없어서 추가해야 합니다. 스크립트가 이 설정을 대신 해 줍니다.

### 초당 처리량 (ops/s) 벤치마크

위 스케일 테스트가 분석 쿼리 1건의 실행 시간을 잰다면, **초당 몇 건을 처리하는가**는 별도로 측정합니다: 쓰기는 `scale-workload.py load`의 적재 속도(rows/s)가 곧 측정값이고, 읽기는 [docker/rwbench-read.py](docker/rwbench-read.py)(운영 형태 3패턴 — 태그 최신값 / 단건 / 100행 시간창)와 번들 `cassandra-stress`(서버 한계 확인용)로 잽니다. **실행 결과: [읽기/쓰기 처리량 벤치마크](doc/timeseries/rw-throughput-benchmark.md)** — 적재 233k rows/s(호스트 234, 100행 배치), 쓰기 경로 424k rows/s·청크 인코딩 684k rows/s(호스트 237, JMH); 패턴별 읽기 ops/s는 v4 기준 재측정 대기. 재현 명령 전체가 리포트에 있습니다.

## CI 및 릴리스

- 푸시할 때마다 jar를 빌드하고 시계열 테스트 스위트를 실행합니다(`.gitlab-ci.yml`).
- 최신 master 빌드의 jar: *CI/CD → Pipelines → build-jar 아티팩트*.
- 태그 푸시(예: `v6.0.0`) 시 jar 다운로드 링크가 포함된 [Release](../../-/releases)가 발행됩니다.

> **CI가 지금 무엇을 말하고 있는지 먼저 확인하십시오.** 2026-08-07 이후 프로젝트 러너가 전부 offline이라 파이프라인이 잡을 시작조차 못 하고 `stuck_pending_no_matching_runners`로 실패합니다 — 그 기간의 빨간 파이프라인은 코드에 대한 진술이 아닙니다. `glab ci list`, `glab ci get -p <id>`로 사유가 보입니다. 러너가 복구될 때까지 검증은 `.build/sh/ci-local`과 `docker/cluster-test.sh`입니다. [production-rollout.md §6](doc/timeseries/production-rollout.md) 참고.

## 브랜치 및 업스트림 정책

- `master`(= `6.0.0` 브랜치): 릴리스 라인. apache/cassandra의 최신 업스트림 `cassandra-6.0` 브랜치(리모트 `upstream`)와 **항상 머지된 상태로 유지**해야 합니다.
- 자주 충돌하는 지점: `CHANGES.txt`, `debian/changelog`, `modules/accord` 서브모듈 포인터, `cql3/statements/SelectStatement.java`(gap-fill 연결부).

## 개발

빌드/테스트/코드 스타일 규칙은 [CLAUDE.md](CLAUDE.md)와 [AGENTS.md](AGENTS.md)를 참고하세요(전체 테스트 스위트는 몇 시간이 걸리므로 대상 테스트만 실행합니다). 테스트 레이아웃은 [TESTING.md](TESTING.md)에 있습니다. 시계열 테스트 진입점: `org.apache.cassandra.cql3.functions.TimeSeriesFctsTest`, `org.apache.cassandra.db.aggregation.TimeBucketGapFillerTest`.
