<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# 계층형 저장 프로덕션 투입 체크리스트

계층화를 실제 운영 테이블에 처음 켜기 전에 확인할 것들입니다.

**현황 (2026-08-28 실측):** 운영 노드(41)는 **v6.0.0**으로 돌고 있고(2026-08-28 배포, 첫 CI
게이트 통과 릴리스 — 릴리스 태그는 v6.0.0 하나로 유지한다), 계층화는 **운영 테이블에 이미 켜져 있습니다** — `pp.tm_tag_point`(윈도우
14,238개 / 행 720만 인코딩됨)와 `pp.tm_asset_data_based_second`. 8/28 부팅 로그에서 청크 테이블
sstable을 여는 것이 확인되므로 투입 시점은 그 이전이다. §0의 되돌릴 수 없는 지점들은 따라서 이미
지나갔고(청크가 해당 구간의 유일한 사본), 이 문서의 나머지는 "첫 투입 전 점검"이 아니라 **추가
테이블에 켜기 전 점검이자 운영 중 참조**로 읽어야 합니다. v6.0.0 배포로 8/7 빌드에 없던
티어링 수정 3건(태그 열거 타임아웃, 배치 콜드 삭제 가드, 커버리지 원장 과대 신고)이 반영됐고,
투명 티어드 읽기는 배포 직후 운영 콜드 데이터 집계로 검증했습니다.

---

## 0. 되돌릴 수 없는 지점들

투입 전에 이것들을 이해하고 시작하십시오.

### 0.0 청크 포맷은 v4 하나입니다 — v4 빌드에서만 켜십시오

현재 빌드가 쓰고 읽는 청크 페이로드는 **청크 포맷 v4**뿐입니다([chunk-format-v4.md](chunk-format-v4.md)).
v4 이전 빌드가 만든 청크(v1/v2/v3)는 읽을 때 `UnsupportedChunkFormatException`으로 **쿼리가
실패**하며, 변환 도구는 의도적으로 없습니다. 반대로 v4 청크는 구버전 빌드가 읽지 못합니다.
따라서 **클러스터의 모든 노드가 v4 빌드(배선 머지 `5cbdf914fa` + 정리 머지 `6f4ce9faa7` 포함)로
올라온 뒤에만** 계층화를 켜십시오. 구버전 빌드로 만들어 둔 청크 테이블이 남아 있다면 켜기 전에
`DROP`해야 합니다 — 그 데이터는 복구되지 않으므로(§0.2) 재생성 가능한 데이터인지 먼저 확인하십시오.

### 0.1 구버전 빌드로 켜면 노드가 죽습니다

`ChunkTables.ensureChunkTable`은 초기 구현에서 청크 테이블을 **프로그래매틱 스키마 변경**으로
만들었습니다. TCM은 스키마 변경을 **CQL 텍스트로** 직렬화해 되읽는데, 그 경로는 `cql()` 기본값인
문자열 `"null"`을 기록했습니다. 결과:

- 피어가 그 로그 엔트리를 파싱하지 못해 **클러스터 전체의 스키마 전파가 그 지점에서 멈춥니다**
- 그 엔트리를 쓴 **노드 자신도 재시작 시 메타데이터 로그 재생에 실패해 기동하지 못합니다**

**고친 빌드로도 이미 쓰인 엔트리는 복구되지 않습니다.** 메타데이터 로그는 append-only라 해당 노드는
데이터 디렉토리를 비우고 재구축해야 합니다.

> **따라서: 계층화를 켤 노드는 반드시 이 수정이 포함된 빌드여야 합니다.** 배포 전에
> `nodetool version`과 jar 빌드 시점을 확인하십시오. 확신이 없으면 **켜지 마십시오** — 확인 비용보다
> 복구 비용이 비교할 수 없이 큽니다.

### 0.2 계층화는 일방통행입니다

재인코더는 청크를 쓴 뒤 **원본 행을 삭제합니다.** 그 시점부터 콜드 데이터의 유일한 사본은
`<테이블>__chunks`입니다.

- 청크를 읽을 줄 모르는 빌드(업스트림 카산드라 포함)로 롤백하면 **그 데이터는 읽히지 않습니다**
- 청크 테이블을 `DROP`하면 그 데이터는 **영구히 사라집니다** (원본 행은 이미 없습니다)

투입 전에 백업 정책이 `<테이블>__chunks`를 함께 포함하는지 확인하십시오.

---

## 0.5 기존 6.0.0에 올리기 — jar 하나만 교체하면 됩니다

업스트림(`cassandra-6.0`)과 실제로 비교한 결과입니다:

| 항목 | 포크가 바꿨나 | 교체 필요 |
| --- | --- | --- |
| `lib/apache-cassandra-6.0.0.jar` | **예** — 포크 코드 전부가 여기 들어 있습니다 | **예** |
| `bin/` (`nodetool`, `cassandra`, `cqlsh` …) | 아니오 | 아니오 |
| `lib/` 의 다른 jar | 아니오 — Lucene도 업스트림 SAI가 이미 씁니다 | 아니오 |
| `conf/cassandra.yaml` | 주석 처리된 가이드레일 설명 7줄뿐 | 아니오 |

`nodetool`은 셸 스크립트이고 `retier`/`tieringstatus` 클래스는 메인 jar 안에 있습니다
(`org/apache/cassandra/tools/nodetool/Retier.class` 확인). 그래서 스크립트는 건드릴 필요가 없습니다.

```bash
nodetool drain                                   # memtable flush + 커밋로그 정리
systemctl stop cassandra
# 기존 jar 교체 (설치본의 기존 jar 파일명 그대로 덮어쓰기)
cp apache-cassandra-6.0.0.jar "$CASSANDRA_HOME/lib/"
systemctl start cassandra
nodetool version && nodetool status              # 올라왔는지, 링에 붙었는지
```

**주의 두 가지:**

1. **`lib/`에 6.0.0 jar이 하나뿐인지 확인하십시오.** 이름이 다른 6.0.0 jar이 남아 있으면 둘 다
   클래스패스에 올라갑니다 — `ls $CASSANDRA_HOME/lib/apache-cassandra*.jar`. 설치본마다 jar
   파일명 관례가 다를 수 있으므로(운영 노드 41은 `apache-cassandra-timeseries-6.0.0.jar`),
   **기존 파일명을 그대로 유지한 채** 교체하십시오 — 다른 이름으로 추가하면 두 jar이 함께
   올라갑니다.
2. **롤링으로 하십시오.** 온디스크 포맷과 CQL 문법은 업스트림 그대로이고 새 기능은 전부
   옵트인(테이블 확장 / 컴팩션 전략)이므로, **jar만 바꾼 시점에는 동작이 전혀 달라지지 않습니다.**
   계층화나 TSCS는 그 뒤에 테이블별로 켜면 됩니다. 이 성질 덕분에 바이너리 교체와 기능 활성화를
   분리해서 각각 검증할 수 있습니다.

### 0.6 운영 노드 41의 배포 절차 (실측으로 확립된 것)

노드 41(Haswell E5-2676 v3, 48스레드)에서 실제로 쓰는 절차와, 이 노드 특유의 함정입니다.

```bash
# 배포: jar 검증 → drain → 정지 → lib-backup 백업 → 교체 → 기동 → OOM 보호 확인
python3 -c "import zipfile;z=zipfile.ZipFile('x.jar');print(len(z.namelist()),z.testzip())"
nodetool drain && bash bin/stop.sh
mv lib/apache-cassandra-timeseries-6.0.0.jar lib-backup/...jar.$(date +%Y%m%d-%H%M%S)
\cp -f /tmp/new.jar lib/apache-cassandra-timeseries-6.0.0.jar && bash bin/start.sh
cat /proc/$(pgrep -f '[o]rg.apache.cassandra.service.CassandraDaemon' | head -1)/oom_score_adj  # -1000
# 롤백 (실측 3분): lib-backup 의 최신 타임스탬프 jar 로 위 절차 역방향
```

- **HA가 살아 있는지부터 확인하십시오 — 절차가 갈립니다 (2026-08-30 실측).** 8/30 배포에서
  `plantpulse.ha.Main`이 활성 상태였고(당일 00:46 기동), 수동 `start.sh`로 올린 카산드라를 HA가
  **자기 관리 하에 다시 내렸다 올리는 것을 두 번 반복**했습니다(10:55, 11:55 — 배포 자체는 무사:
  jar는 lib/에 이미 교체돼 있어 매번 새 jar로 떴고, HA 경유 기동은 `oom_score_adj=-1000`도 스스로
  잡았다). 결론: 배포 전 `pgrep -f plantpulse.ha.Main`으로 HA 활성 여부를 확인하고, **활성이면
  새 jar를 lib/에 넣고 카산드라를 내리는 것까지만 하라** — 기동은 HA가 하며, 수동으로 올리면
  불필요한 재시동만 늘어난다. 비활성이면 아래 항목대로 직접 올린다.
  **추가 실측(같은 날 저녁):** 사이클은 배포와 무관하게 하루 종일 이어졌다 — 10:55, 11:55,
  13:40, 19:35 등 최소 5회, 전부 정상 종료→재기동(크래시·OOM 아님, dmesg 깨끗, 접속 사용자
  유휴 12h). 주체 규명은 HA 내부라 플랫폼 소관이고, 카산드라 쪽 결과만 적는다: 재시동마다
  티어링 태그 레지스트리(메모리)가 초기화되어 **증분 스캔이 커서 null부터 전 링 재스캔**을
  반복한다(WARN "over all of the remaining ring"이 재발하는 이유). 반복 재시동이 계속되는 한
  tm_asset_data_based_second 같은 대형 테이블의 스캔 수렴은 기대할 수 없다.
  **정정(2026-08-31, 운영자 확인):** 41은 개발 서버라 재시동이 일상이다 — 사이클은 이상 징후가
  아니라 환경 특성이므로 조사 대상이 아니다. 스캔 수렴 리셋은 그 환경 비용으로 받아들이면 되고,
  진짜 감시 대상은 재시동 자체가 아니라 크래시(비정상 종료)와 ERROR다.
- **HA 비활성이면 자동 재기동은 없다고 전제하십시오.** 이 문서는 원래 `plantpulse-ha`가 정지된 카산드라를
  ~30초 만에 자동 재기동한다고 적었지만, 2026-08-28 배포에서 재기동하지 않았고 원인도 확인됐습니다:
  배포 시각에는 HA 데몬 자체가 내려가 있었고(플랫폼 서비스가 8/26부터 failed), 이후 다시 뜬
  HA도 로그에 `HA disabled(single-cluster mode)`를 남기고 **비활성 상태로 대기만 합니다**
  (`ha.enabled=false`). HA 활성화 여부는 플랫폼 소관이므로 이 문서는 판단하지 않는다 — 카산드라
  배포 절차로서의 결론만 적는다: **새 jar을 `lib/`에 먼저 넣고**(만에 하나 다른 무언가가 올려도
  새 jar로 뜨도록) 내린 뒤, **직접 `start.sh`로 올리고 확인하는 것까지가 배포 절차다.** 그리고
  카산드라가 스스로 죽는 경우 현재 설정에서는 아무도 올려주지 않으므로, 노드 다운 감시는 상시
  감시 항목(아래) 중 최우선이다. 참고: HA 데몬 존재 확인을 `pgrep -f plantpulse-ha` 따위로 하면
  로그 tail 프로세스가 걸려 오탐한다 — 메인 클래스(`plantpulse.ha.Main`)로 확인할 것.
- **`start.sh`의 OOM 보호는 기동 후 반드시 확인하십시오.** `oom_score_adj = -1000`이어야 합니다.
  스크립트 수정본은 플랫폼(ppctl) 재동기화가 되돌릴 수 있으므로, 플랫폼 소스 템플릿에 반영되기
  전까지는 기동 후 확인·수동 설정이 운영 절차입니다.
- **상시 감시 항목** (5분 주기 스크립트, JMX 포함): 노드 상태·예외·ERROR·드롭 / 쓰기 지연·데이터
  신선도 / 청크 증가·콜드 flush 실패(`cf_fail`)·memtable 폴백(`fallback`) / JMX
  `ParkedTimeSeriesWindows`·`FarFutureTimeSeriesSSTables`(비어야 정상).

---

## 1. 스키마 적합성 확인

계층화가 거부하는 형태가 하나라도 있으면 그 테이블은 계층화가 **통째로 멈추고**, 콜드 청크 만료도
함께 멈춥니다. 켜기 전에 확인하십시오 ([전체 규칙](tiered-storage.md#1-대상-스키마--시간으로-클러스터링된-아무-테이블)):

```sql
DESCRIBE TABLE pp.tm_tag_point;
```

| 확인 항목 | 요구 |
| --- | --- |
| 클러스터링 | **정확히 1개**, `timestamp` 타입 (ASC/DESC 무관) |
| `counter` 컬럼 | **없어야 함** — 삭제된 카운터는 다시 쓸 수 없습니다 |
| 비frozen 컬렉션 (일반 컬럼) | **없어야 함** — `frozen<...>`은 무방 |
| 보조 인덱스 | **static 컬럼에 걸린 것만** 허용 |
| 머티리얼라이즈드 뷰 | 이 테이블 위에 **없어야 함** |
| `transactional_mode` | **`off`여야 함** (§3 참고) |

---

## 2. TTL을 `cold_window`로 넘기기

`tm_tag_point`의 `default_time_to_live`는 현재 315,360,000초(10년)입니다 — 62일에서 상향된
값이며, TTL은 **쓰기 시점에** 셀에 박히므로 **상향 이전에 쓰인 행은 여전히 원래의 62일 만료를
들고 있습니다.** 계층화를 켤 때 이 관계를 반드시 정리해야 합니다:

- **청크로 옮겨진 데이터에는 TTL이 적용되지 않습니다.** 재인코더는 셀의 `WRITETIME`만 읽고 `TTL`은
  읽지 않으며, 청크 포맷에 TTL 자리가 없습니다. 청크화된 데이터의 유일한 만료 장치는 `cold_window`입니다.
- **`hot_window >= TTL`이면 아무것도 압축되지 않습니다** — TTL이 먼저 지웁니다. WARN만 남고 조용히
  아무 일도 일어나지 않습니다.

> **⚠️ `default_time_to_live` 변경은 기존 행에 소급 적용되지 않았습니다.** TTL은 **쓰기 시점에**
> 셀에 박히므로, 62일 → 10년 상향은 신규 행에만 적용됐고 **상향 전에 저장된 행은 원래의 62일
> 만료를 그대로 들고 있습니다** — 그 행들은 예정대로 사라집니다.
>
> **계층화가 이걸 구해 줍니다.** `hot_window`가 남은 TTL보다 짧으면(예: `hot_window 7d` vs
> 62일 잔여 TTL), 기존 행은 TTL이 터지기 전에 청크로 옮겨지고 **그 순간 TTL이 벗겨집니다** —
> 이후로는 `cold_window`가 지배합니다. 따라서 **62일 TTL을 담은 기존 행을 살리려면 그 만료 전에
> 계층화를 켜야 합니다.**
>
> TTL 상한은 20년(`Attributes.MAX_TTL` = 630,720,000초)이므로 10년(315,360,000초)은 허용됩니다.

따라서 `hot_window < TTL`이어야 하고, `cold_window`가 실제 목표 보존 기간이 됩니다:

```
hot_window   : 대시보드가 실제로 때리는 구간 (예: 7d ~ 30d)
             → 이 안쪽 질의는 계층화 전과 완전히 동일한 속도 (병합 자체를 건너뜁니다)
default_ttl  : hot_window 보다 길게 (또는 계층화 도입과 함께 제거)
cold_window  : 목표 보존 기간 (예: 10y)
```

---

## 3. 애플리케이션 영향 — 코드 확인이 필요한 두 가지

### 3.1 파티션 키 없는 집계는 0을 돌려줍니다

레인지 스캔은 청크를 병합하지 않습니다. 전부 계층화된 테이블에서:

```sql
SELECT count(*) FROM pp.tm_tag_point;        -- → 태그 수 (실제 행 수 아님)
```

**클러스터링 행을 하나도 못 봅니다.** static 컬럼이 있으면 태그마다 static 행 하나가 남아 **태그
수**가 나오고(실측 3,000만 행 테이블에서 `600`), static이 없으면 `0`이 나옵니다. `600` 같은 값은
그럴듯해서 오답인 걸 알아채기 더 어렵습니다. 클라이언트 경고가 나가지만 대부분의 드라이버는 이를
노출하지 않습니다. 애플리케이션·배치·모니터링 쿼리에 파티션 키 없는 집계나 전체 스캔이 있다면
투입 전에 찾아 두십시오.

### 3.2 콜드 구간 쓰기는 거부됩니다

`hot_window` 이전 구간에 대한 `DELETE`, `UPDATE ... SET col = null`, 파티션 삭제, `INSERT`의 null
바인딩은 **거부**됩니다(`InvalidRequestException`). 콜드 데이터는 설계상 불변입니다 — 청크만 가리는
툼스톤은 `gc_grace` 이후 무력화되어 값이 되살아나기 때문입니다.

과거 데이터를 정정·삭제하는 배치가 있다면 계층화와 양립하지 않습니다.

### 3.2.1 ⚠️ 백필: `null` 바인딩이 거부됩니다

콜드 구간 쓰기 가드가 무엇을 막는지 정확히 보면:

```java
if (!row.deletion().isLive())   → 거부   // 행 삭제
else if (hasCellTombstone(row)) → 거부   // 셀 툼스톤
```

카산드라에서 **컬럼을 생략하면** 아무것도 쓰지 않지만, **`null`을 명시적으로 바인딩하면 툼스톤**이
됩니다. 따라서:

| 백필 형태 | 결과 |
| --- | --- |
| `INSERT INTO t (tag_id, timestamp, value, value_boolean) VALUES (...)` — 안 쓰는 컬럼 **생략** | ✅ 허용. 다음 재인코딩 주기에 청크로 병합 |
| `INSERT INTO t (...모든 컬럼...) VALUES (?, ?, ?, null, ?)` — **명시적 null** | ❌ 거부 |

**이게 왜 함정인가**: 드라이버의 prepared statement는 보통 **모든 컬럼을 바인딩**합니다. 그리고
`tm_tag_point`는 태그 타입에 따라 `value_numeric`/`value_boolean` 중 하나가 **항상 비어 있습니다.**
8개 컬럼을 전부 바인딩하는 수집기라면 **모든 행이 null을 하나씩 들고 있고**, `hot_window` 이전
구간으로 들어오는 백필은 **전부 거부됩니다.**

**투입 전 확인**: 수집기가 안 쓰는 컬럼을 (a) 컬럼 목록에서 빼는지, (b) `unset`으로 두는지,
(c) `null`을 바인딩하는지. (c)라면 (a)나 (b)로 바꿔야 백필이 동작합니다.

### 3.3 `transactional_mode`을 켜지 마십시오

Accord 트랜잭션 **읽기**는 청크 병합을 거치지 않아 `hot_window` 이전 이력이 통째로 빠진 결과를
조용히 돌려줍니다. 계층화는 그런 테이블을 거부하지만 **`ALTER TABLE`을 막지는 못합니다** — 실패는
다음 사이클의 ERROR 로그로만 나타납니다.

---

## 3.5 TSCS만 먼저 적용하기 (권장 순서)

계층화와 TSCS는 **완전히 독립**입니다 — 계층화는 테이블 확장, TSCS는 컴팩션 전략입니다. 둘 다 켤
필요가 없고, **TSCS를 먼저 켜는 편이 위험이 낮습니다.**

**되돌릴 수 있기 때문입니다.** 컴팩션 전략은 언제든 되돌리면 그만이고 데이터는 그대로입니다.
계층화는 원본 행을 삭제하므로 되돌릴 수 없습니다(§0.2).

TSCS만으로 얻는 것: 창 단위 SSTable 정렬(시간 범위 조회의 지역성), **만료 창 통삭제**, 지각 백필
격리, 닫힌 창 동결(창당 1 SSTable → 읽기 증폭 감소).

### ⚠️ 만료는 TTL이 아니라 `retention`에 맡기십시오

동결된 창은 다시 컴팩션 후보로 선택되지 않습니다. 따라서:

- **동결 시점에** 이미 만료된 TTL 데이터는 그때 회수됩니다
- **동결 이후에** 만료되는 데이터는 **TTL만으로는 영원히 회수되지 않습니다**

TSCS의 `retention`을 목표 보존 기간으로 설정하십시오. 만료된 창을 **컴팩션 없이 통째로 삭제**하므로
TTL 기반 회수보다 오히려 효율적입니다.

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d',        -- 창 크기 (조회 패턴에 맞춤)
  'freeze_after': '2d',       -- 창이 닫힌 뒤 이 시간이 지나면 동결
  'retention': '3650d'        -- 보존 기간. 만료 창은 통삭제
};
```

### 투입 시 주의

1. **전략 변경은 전체 재컴팩션을 유발합니다.** 기존 대용량 테이블에서는 상당한 IO 이벤트이므로
   비피크 시간대에, 한 노드씩 진행하십시오.
2. **분산 검증은 3노드 jvm-dtest까지입니다.** TSCS는 스트리밍 창 스플릿(repair/bootstrap으로
   SSTable이 도착할 때 동작), 노드별 동결 수렴, 만료 통삭제, 지각 백필 격리가
   `TimeSeriesCompactionDistributedTest`(3노드)로 덮여 있습니다. 실 운영 다중 노드 클러스터
   경험은 아직 없습니다(§6).
3. **되돌리기**: `ALTER TABLE ... WITH compaction = {'class': 'UnifiedCompactionStrategy', ...}`.
   데이터 손실 없이 원복되며, 다시 전체 재컴팩션이 일어납니다.

---

## 4. 투입 절차

한 번에 전체를 켜지 마십시오.

1. **테이블 하나로 시작** — 보존이 가장 급한 테이블 하나. 계층화는 테이블 확장 옵션이라 테이블마다
   독립적으로 켜고 끌 수 있습니다.
2. **`hot_window`를 넉넉하게 시작** — 처음에는 실제 필요보다 길게 잡아 압축 대상을 좁힌 뒤, 동작을
   확인하고 줄이십시오. 되돌릴 수 없는 작업이므로 처음에는 적게 건드리는 편이 낫습니다.
3. **`nodetool retier`로 수동 1회 실행** — 60초 스위프를 기다리지 말고 직접 돌려 결과를 확인합니다.
4. **검증** — 아래 §5.
5. **관측하며 확대** — 며칠 지켜본 뒤 다음 테이블로.

---

## 4.5 배포 후 운영 스모크 — 필수

jar 교체 후 §0.6의 기동 확인(UN·oom_score_adj)에 더해, **시계열 함수 배터리를 운영 실데이터로 한
번 돌리는 것까지가 배포 검증이다** (2026-08-29부터 필수 절차). 활성 태그 하나와 **콜드(청크화된)
시간 범위**를 골라서 실행한다 — 콜드 범위를 쓰는 것이 핵심인데, 같은 쿼리가 투명 티어드 읽기(청크
병합)까지 함께 검증하기 때문이다. 이 스키마에서 수치 집계는 `latency`(항상 존재)로, `value`는
`first`/`last`만 가능하다는 점을 기억할 것(§3.1의 함정).

```sql
-- ① 다운샘플링: 버킷별 count/avg/min/max가 상식적 값인지
SELECT time_bucket(1h, timestamp), count(*), avg(latency), min(latency), max(latency)
FROM pp.tm_tag_point WHERE tag_id=? AND timestamp >= ? AND timestamp < ?
GROUP BY tag_id, time_bucket(1h, timestamp);
-- ② 순서 의존 계열: first/last(value), delta/rate/derivative(latency)
-- ③ 분포: percentile(latency,0.5), percentile(latency,0.95), stddev, approx_count_distinct
-- ④ 시간가중: time_weighted_average(latency,timestamp), integral(latency,timestamp)
-- ⑤ 갭필 (DESC 테이블이므로 ORDER BY timestamp ASC 필수):
SELECT time_bucket_gapfill(1h, timestamp, ?, ?), locf(avg(latency))
FROM pp.tm_tag_point WHERE tag_id=? AND timestamp >= ? AND timestamp < ?
GROUP BY tag_id, time_bucket_gapfill(1h, timestamp, ?, ?) ORDER BY timestamp ASC;
```

읽기 다섯 묶음에 더해 **쓰기·경계·생존 네 묶음**(v6.0.0 배포 검증에서 추가·실측):

```sql
-- ⑥ hot 쓰기/삭제 — 합성 태그로, 전부 성공해야 함. 커버리지 원장 버그(v6.0.0에서 수정)가
--    부수던 경로가 정확히 이것이라, 이 한 쌍이 그 회귀를 배포마다 재확인한다.
INSERT INTO pp.tm_tag_point (tag_id, timestamp, latency, quality, value)
     VALUES ('SMOKE-TEST', <now_ms>, 1, 192, 'smoke');
SELECT value FROM pp.tm_tag_point WHERE tag_id='SMOKE-TEST' AND timestamp=<now_ms>;
DELETE FROM pp.tm_tag_point WHERE tag_id='SMOKE-TEST' AND timestamp=<now_ms>;
-- ⑦ cold 삭제 시도 — 반드시 거부돼야 함(에러 메시지에 immutable / cold_window).
--    가드는 행 존재와 무관하게 클러스터링 경계로 판정하므로, 합성 태그의 오래된 클러스터링에
--    시도하면 실데이터 위험이 0이다. 이것이 "성공"하면 그 자체가 결함 신호다.
DELETE FROM pp.tm_tag_point WHERE tag_id='SMOKE-TEST' AND timestamp=<now_ms - hot_window - 2h>;
-- ⑧ hot/cold 경계 걸침 읽기 — 콜드에 데이터가 있는 시작점부터 now까지. count가 콜드 행 수와
--    맞고, LIMIT 1(DESC 기본)과 ORDER BY timestamp ASC LIMIT 1이 양 끝 행을 정확히 돌려줄 것.
--    (경계 산술이 틀리면 콜드 행 0개를 에러 없이 돌려주던 버그 계열을 이 끝점 쌍이 고정한다.)
-- ⑨ 티어링 생존 — nodetool tieringstatus에서 운영 테이블들의 Last Run이 interval의 2배 안이고
--    Tags Skipped가 직전 확인 대비 증가하지 않을 것.
```

```sql
-- ⑩ SAI LIKE (풀텍스트) — 운영 테이블에는 index_analyzer 인덱스가 없으므로 전용 스모크
--    픽스처 tstest.smoke_fulltext(4행, ngram SAI 인덱스, 배포마다 재사용)로 배포된 바이너리의
--    analyzer 경로를 검증한다. 여섯 쿼리와 기대값:
--    '%timeout%'→1  '%imeou%'(단어 중간)→1  '%정지%'→1  '%프 정%'(공백 걸침)→1
--    'connection%'→2  = 'connection refused'(완전일치 유지)→1
SELECT count(*) FROM tstest.smoke_fulltext WHERE device='pump-01' AND msg LIKE '%timeout%';
```

통과 기준: 열 묶음 전부 타임아웃 없이 기대 결과를 내고, ⑤가 ①에 없던 빈 버킷을 직전 값으로
채우고(그것이 gap-fill이 실제로 동작한다는 증거), ⑦만 유일하게 **실패(거부)가 정답**이다.
v6.0.0 배포(2026-08-28~29) 실측 예: 8/26 콜드 6시간 범위에서 ①~⑤ 정상(16:00 빈 버킷 locf=64),
⑥ 통과, ⑦ `immutable`/`cold_window`로 거부, ⑧ 8,026행 병합 + 양방향 끝점 정확,
⑨ Last Run 170s/17s (interval 300s), ⑩ 여섯 쿼리 전부 기대값 일치(한국어 공백 걸침 포함).

## 5. 검증 항목

```sql
-- 청크가 실제로 쓰였는가
SELECT count(*) FROM pp.tm_tag_point__chunks;

-- 계층화 사이클 상태 (에러·건너뜀 포함)
SELECT * FROM system_views.timeseries_tiering;

-- 같은 데이터가 계층화 전과 동일하게 읽히는가 (반드시 파티션 키를 걸고)
SELECT count(*) FROM pp.tm_tag_point
 WHERE tag_id = '<태그>' AND timestamp >= '<콜드 구간 시작>' AND timestamp < '<끝>';

-- static은 그대로 남아 있는가
SELECT site_id, tag_name, type FROM pp.tm_tag_point WHERE tag_id = '<태그>' LIMIT 1;
```

로그에서 확인할 것:

- `ERROR` — 스키마 거부 사유 (§1의 어느 항목에 걸렸는지 명시됩니다)
- `WARN` — TTL/`hot_window` 충돌, 건너뛴 태그

JMX (`org.apache.cassandra.db:type=Tables,keyspace=pp,table=tm_tag_point`):

| 속성 | 의미 |
| --- | --- |
| `ParkedTimeSeriesWindows` | 컴팩션이 진척을 못 내 파킹한 창 — 비어 있는 것이 정상 |
| `FarFutureTimeSeriesSSTables` | `max_future_window` 밖이라 모든 자동 경로에서 제외된 sstable |

빌드 자체의 검증 배터리(릴리스 게이트, 현재 master/v4 기준 전부 초록): 단위 398 어서션 ·
통합([docker/integration-test.sh](../../docker/integration-test.sh)) 75 어서션 ·
3노드 실컨테이너([docker/cluster-test.sh](../../docker/cluster-test.sh)) 49 어서션, 그리고
JIT 티어 간 인코더 바이트 결정성 테스트(`ChunkV4CodecTest.encoderIsDeterministicAcrossJitTiers`).

---

## 6. 아직 검증되지 않은 것 (투입 판단에 필요한 정보)

정직하게 남깁니다:

- **검증한 산출물과 배포하는 산출물이 다릅니다.** 릴리스 게이트가 끝까지 검증하는 것은 도커
  이미지입니다(`docker/Dockerfile`이 소스에서 빌드 → `docker/integration-test.sh`가 그 이미지를
  띄워 검증). 실제 배포는 §0.6대로 **jar 하나를 기존 설치본에 넣습니다.** 이미지는 자기
  `conf/`·`bin/`·엔트리포인트·JVM 플래그로 돌지만 노드 41은 그 노드의 `cassandra.yaml`,
  `jvm*.options`, `bin/start.sh`(OOM 보호), HA 워치독과 함께 돕니다. 따라서 통합 테스트가 초록인
  것은 **"이미지 환경에서 코드가 동작한다"**는 뜻이지 **"노드 41 설정에서 동작한다"**는 뜻이
  아닙니다. §0.5의 비교표(포크가 바꾼 것은 메인 jar뿐)가 그 간극이 좁다는 근거이지만, 그것은
  *읽어서 확인한* 것이지 *그 조합으로 실행해서 확인한* 것이 아닙니다. 운영 `cassandra.yaml`과
  `jvm*.options`를 마운트해 통합 테스트를 한 번 더 돌리면 실측으로 바뀝니다.
- **GitLab CI는 2026-08-07 이후 아무것도 확인해 주지 않았습니다.** 프로젝트 러너 4개가 전부
  stale이고 `common` 그룹에는 러너가 없어, 파이프라인이 `stuck_pending_no_matching_runners`로
  잡을 시작조차 못 합니다. 그 기간의 빨간 파이프라인은 **코드에 대한 진술이 아닙니다** — 그리고
  초록이었던 적도 없습니다. 러너가 복구될 때까지 유일한 검증은 `.build/sh/ci-local`(빌드 → 포크
  테스트 → 이미지 → 통합 테스트)과 `docker/cluster-test.sh`를 손으로 돌리는 것이며, 무엇을 실제로
  돌렸는지 릴리스마다 적어 두십시오. 확인: `glab ci list`, `glab ci get -p <id>`.
- **성능 수치는 이식되지 않습니다.** 이 문서와 [벤치마크](tiering-benchmark.md)의 모든 수치는
  호스트 **234**(Xeon Silver 4114T, Skylake-SP)에서 났고 운영 노드는 **41**(Haswell E5-2676 v3)
  입니다. 저장 7.1× 절감은 CPU와 무관하니 그대로 유효하지만, 질의 4~6× 가속은 옮겨오지 않습니다.
  같은 이유로 `.build/sh/ci-perf`는 baseline을 뜬 호스트가 아니면 판정하지 않고 보고만 합니다.
- **`hot_window`와 `chunk_window`를 같게 두면 여유가 사라집니다.** 쓰기 가드는 콜드 구간에 대한
  툼스톤을 거부하고, 그 경계는 `max(청크 커버리지 상단, now - hot_window)`입니다. 두 값이 같으면
  창이 닫히는 순간 그 창의 가장 최근 행이 곧바로 재인코딩 대상이 되어, 방금 쓴 행도 hot 구간에
  머무는 시간이 사실상 없습니다. 삭제·`null` 갱신이 필요한 테이블이라면 **`hot_window`를
  `chunk_window`보다 넉넉히 크게** 잡으십시오(예: `chunk_window 1h` / `hot_window 6h`). 테스트가
  1h/1h를 쓰는 것은 최대 부하 조건을 만들기 위한 것이지 권장 설정이 아닙니다.

- **운영 백업은 존재하지 않는 것으로 확인됐습니다 (2026-08-28 실측).** medusa 기반 백업이 매일
  돌지만 저장소 전체가 22 MB(노드 데이터 622 GiB)이고, 매 실행이 "Creating snapshot failed"
  ERROR 후 "Backup done, 0 files"라는 **거짓 성공**으로 끝난다. 원인은 설정 키 오타로 확정:
  `/etc/medusa/medusa.ini` `[cassandra]`의 `nodetool = <경로>`는 medusa가 모르는 키라
  (`config.py`의 유효 키는 **`nodetool_executable`**) PATH의 존재하지 않는 `nodetool`을 실행한다.
  추가 결함 둘: 백업 타깃이 데이터와 **같은 파일시스템**(`/data1`)이고, 감시가 종료 코드만 봐서
  0-파일 백업이 초록으로 지나간다. 수리는 플랫폼(`plantpulse-backup`) 소관 — 카산드라 쪽
  임시 안전판으로 두 청크 테이블의 스냅샷(`chunks-safety-20260828`, 60 GB)을 호스트 234의
  `/data1/backup-node41-chunks/`로 복사해 두었다.
- **실제 다중 노드 운영 클러스터에서 돌려본 적이 없습니다.** 3노드 jvm-dtest는 통과했고(재인코더의
  프라이머리 레인지 분할, 코디네이터 독립 투명 읽기, 지각 행 생존, 노드 재시작), 그 테스트가 실제
  스키마 전파 결함을 잡아냈지만, 이는 실 운영 부하와 다릅니다.
- **질의 성능** ([벤치마크](tiering-benchmark.md), 2026-08-04 · 234 = Xeon Silver 4114T · v4 실측):
  저장 **7.1×** 절감(237.8 → 33.3 MB, 20M행), 집계·gap-fill은 비계층 대비 **4~6× 빠릅니다** —
  v3 시절의 "저장 절감의 대가로 질의 감속" 트레이드오프는 v4에서 사라졌습니다. 재인코딩 처리량
  108k rows/s. 남는 주의점은 **시간 범위 없는 질의**입니다(범위가 없으면 병합이 그 파티션의 창
  목록 전체를 대상으로 하며, `LIMIT` 질의는 창 단위 지연 디코드로 필요한 창만 풉니다 — 벤치마크
  문서의 운영 규칙 참고). `hot_window` 안쪽만 보는 질의는 병합을 건너뛰므로 **비용이 없습니다.**
