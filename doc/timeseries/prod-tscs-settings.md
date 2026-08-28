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

# 운영 TSCS 설정과 파킹 진단

운영 노드 41(단일 노드 448 GiB), `pp` 키스페이스 75개 테이블에 TSCS를 적용한 상태에서 측정한
값과, 파킹된 창을 만났을 때의 진단 절차입니다.

## 1. 현재 설정 — 전부 적정합니다

| 그룹 | 개수 | `window_size` | `freeze_after` | `retention` | TTL |
| --- | --- | --- | --- | --- | --- |
| `tm_tag_point` | 1 | `1d` | `2d` | `3651d` | 3650d (10년) |
| `tm_tag_point_archive` | 1 | `1d` | `2d` | `366d` | 365d |
| `tm_tag_point_snapshot` | 1 | `1d` | `2d` | `94d` | 93d |
| `tm_asset_data`, `tm_asset_data_based_timestamp` | 2 | `1d` | `2d` | `11d` | 10d |
| `tm_blob`, `tm_blob_object` | 2 | `1d` | `2d` | `94d`, `32d` | 93d, 31d |
| `tm_asset_alarm_timeline*` | 4 | `7d` | `14d` | `37d`, `97d` | 30d, 90d |
| `tm_asset_*` 나머지 | 64 | `7d` | `14d` | 없음 | **0** |

전부 **`retention = TTL + window_size`** 를 만족합니다. TTL이 0인 64개에 `retention`이 없는 것도
정상입니다 — 만료시킬 대상이 없습니다.

> `tm_asset_oee` 계열 5개는 `memtable = 'timeseries'`가 걸려 있으나 컴팩션이 UCS라 기본
> memtable로 폴백 중입니다(2026-08-02 확인) — TSCS로 전환하면 자동 적용됩니다.
> [timeseries-memtable.md §2.3](timeseries-memtable.md) 참고.

### `window_size`를 더 키울 이유가 없습니다

10년 보존에 `1d` 창이면 3,651개가 됩니다. 많아 보이지만 읽기에 비용을 물리지 않습니다:

| 지표 (`nodetool tablehistograms pp.tm_tag_point`) | 값 |
| --- | --- |
| 읽기당 SSTable | p50 **1.00**, p95 **1.00**, p99 2.00 |
| 읽기 지연 | p50 642 µs, p95 1.6 ms, p99 2.3 ms |
| 블룸 필터 오탐 | 0 |

오히려 `1d`가 유리합니다. 만료가 하루 단위로 곱게 회수되고, 동결 SSTable이 하루치 ≈ 120 MB로
작습니다(`7d`면 ≈ 850 MB가 되어 스트리밍·repair·컴팩션 중 디스크 여유 요구가 커집니다).

**컴팩션 전략 옵션 변경은 해당 테이블 전체 재컴팩션을 유발합니다.** 측정으로 뒷받침되지 않는
재컴팩션은 하지 마십시오.

### 오프힙(다이렉트) 메모리 — `file_cache_size` < `MaxDirectMemorySize`

노드 수준 불변식입니다. 현재 값: `MaxDirectMemorySize=6G`(jvm 옵션),
`file_cache_size: 2GiB`(cassandra.yaml).

- 읽기 버퍼 풀(청크캐시 BufferPool)은 `file_cache_size`까지 자유 청크를 **반납하지 않고
  보유**합니다. **`file_cache_enabled: false`는 LRU 캐시 층만 끄고 이 풀의 상한은 그대로**이므로,
  상한이 다이렉트 한도보다 크면 읽기 트래픽에 비례해 자라다 JVM 벽에 먼저 부딪힙니다.
- 벽에 닿으면 컴팩션·flush writer가 버퍼를 못 얻어 생성 즉시 죽습니다 — `pending tasks`가
  고정된 채 처리량이 0으로 보이고, 클라이언트 읽기가 타임아웃됩니다.
- 증상 식별: 로그의 `Cannot reserve … direct buffer memory`. `JVMStabilityInspector`의
  `Force heap space OutOfMemoryError` 문구는 **힙이 아니라 다이렉트 고갈에서도** 나옵니다 —
  스택을 먼저 확인하십시오.
- 감시: `java.nio:type=BufferPool,name=direct`(총량)과
  `org.apache.cassandra.metrics:type=BufferPool,scope=chunk-cache,name=Size`(풀 자체).
  강제 GC(`jcmd <pid> GC.run`)로 줄지 않는 다이렉트 총량은 라이브 참조입니다.

## 2.0 파킹 실측과 테이블별 처방 (2026-08-28)

재기동 직후 파킹된 63창 / 15테이블의 전수 실측이다. 파티션 크기는 `nodetool tablestats`의
compacted max/mean, 예산은 `WindowRoutingIterator.maxBufferedBytesPerPartition` = 64 MiB.

**A그룹 — 예산 초과가 원인 (코드 기전과 일치, 확정):**

| 테이블 | 파티션 max / mean | 파티션 키 | 처방 |
| --- | --- | --- | --- |
| `tm_tag_point_snapshot` | **1.16 GB** / 65 MB | `((site_id, snapshot_id, date), timestamp)` | 파티션 키에 시간 세분(시간 단위 등) 추가, 또는 `window_size` 확대로 경계 걸침 빈도 축소 |
| `tm_flow_log` | 668 MB / **144 MB** | `(bucket, ts, log_id)` | `bucket` 입도를 좁혀 파티션 축소 — mean이 예산의 2.3배라 창마다 걸릴 수 있는 상태 |
| `tm_option_listener_push_cache` | 187 MB / 0.2 MB | `(url, timestamp)` | 소수 핫 url의 아웃라이어. 캐시 성격이면 TSCS가 아니라 UCS가 맞는지부터 재검토 |

**B그룹 — 같은 기전, 압축이 가렸을 뿐 (조사로 확정, 이 절의 이전 판이 "잠재 버그"라 했던
분류는 틀렸다):** `tm_asset_ram`, `tm_asset_ram_history_by_{timestamp,site,area}`,
`tm_asset_oee_history_by_{timestamp,site,area}`, `tm_asset_aggregation_by_{site,area}`,
`tm_asset_ems_history_by_timestamp` — `tablestats` max 30~52 MB는 예산 미만처럼 보이지만
**그 수치는 압축된 디스크 바이트이고 라우팅 예산은 비압축 직렬화 바이트를 센다.** 이 테이블들의
압축비는 0.079(12.7×)라 디스크 30 MB ≈ **비압축 ~380 MB**로 예산을 6배 초과한다. 로그가
직접 증언한다: 같은 밤 overflow WARN 44건이 B그룹 전 테이블에 대해 초과한 파티션 키까지
명시했다(`Partition ... "ASSET_DJ_M_01_1" ... exceeded the 67108864-byte window-routing buffer`).

**진단 규칙 두 가지가 이 오류에서 나온다:** ① 파킹 원인 판정에는 `tablestats`가 아니라
**overflow WARN**을 먼저 보라 — 초과 파티션을 이름까지 찍어주며, `tablestats`와 예산은 압축
전후라 직접 비교할 수 없다. ② 그 WARN은 NoSpamLogger로 분당 1건씩 테이블별 제한되므로
개수가 아니라 **존재 여부**로 읽어라(§2의 "silently parked" 관찰 참고).

처방은 A그룹과 같다 — 파티션이 비압축 기준으로 너무 크다. asset 단위 무한 파티션이므로
파티션 키에 굵은 시간 버킷을 추가하는 것이 근본 처방이고, 여파는 무해하며 `retention`이
회수한다는 점도 같다.

## 2. 파킹된 창 진단

> **"해결됨"이라고 적혀 있던 주장은 틀렸습니다 (2026-08-28 실측으로 정정).** 이 절의 이전 판은
> 2026-08-02 정리 후 "재발 0건"이라 적었지만, 2026-08-28 재기동 직후 **15개 테이블에서 63개
> 창이 파킹**됐고 파킹된 창들은 8/6 이후 생성분입니다 — 즉 재발이 아니라 **상시 상태**입니다.
> 실측 파티션 크기가 이유를 그대로 보여줍니다: `tm_tag_point_snapshot` max 1.16 GB / mean 65 MB,
> `tm_flow_log` mean 149 MB — 64 MiB 라우팅 예산을 일상적으로 넘습니다. 이런 테이블은 파킹이
> 정상 동작이고, 데이터 정합성에는 무해하며, `retention`(94d/62d)이 창째 회수하므로 수명도
> 유한합니다. 근본 해법은 파티션 축소·`window_size` 확대·비시계열 테이블의 UCS 전환 중 하나다.
>
> `pp.tm_tag_point`의 [`memtable = 'timeseries'`](timeseries-memtable.md)가 64 MiB 경로를 없애는
> 것은 **flush에 한해서**다 — 디스크 위 걸침 SSTable을 다시 쓰는 `SplitRefreezeCompactionTask`는
> memtable과 무관하게 같은 라우팅 예산을 쓰므로, tm_tag_point도 레거시 걸침 SSTable로는 여전히
> 파킹될 수 있다(8/28에 1건 관찰).
>
> **재시작 비용 하나를 알아두십시오:** 파킹 상태(`windowProgress`)는 메모리에만 있어 재기동마다
> 리셋되고, 전략이 창마다 재작성을 2회씩 다시 시도한 뒤 다시 파킹합니다 — 8/28 재기동 직후
> 약 5분간 63창 × 2회의 재작성 웨이브가 그것입니다. 재기동 직후의 Parking WARN 폭주와 컴팩션
> 부하는 이 고정 비용이지 새 문제가 아닙니다.

파킹은 동결과 분할이 서로를 되돌리는 무한 반복을 막는 가드입니다. 증상은 그 시간대 SSTable이
합쳐지지 않고 남는 것이고, 파킹된 창은 백로그에서도 빠지므로 **할 일 없는 테이블과 겉모습이
같습니다.**

**원인은 하나입니다 — 라우팅 버퍼 오버플로.**

파킹 조건은 "split-refreeze가 이미 만든 적 있는 모양을 되돌려주는 것"이고, 그것은
`WindowRoutingIterator`가 오버플로했을 때만 일어납니다. 오버플로는 **파티션 크기**에만
의존하며 SSTable이 걸친 기간과는 무관합니다.

`WindowRoutingIterator.maxBufferedBytesPerPartition`은 64 MiB입니다. `SSTableWriter`가 파티션을
한 번에 받고 키를 한 번만 받으므로, 창 경계로 쪼개려면 파티션 전체를 힙에 올려야 합니다. 예산을
넘으면 라우팅이 분할을 포기하고 통째로 씁니다 — 데이터는 온전하지만 결과가 창을 걸친
SSTable이라 모양이 안 변하고, 가드가 파킹합니다.

2026-08-02 운영 노드에서 파킹된 4개 테이블은 **전부** 64 MiB를 넘는 파티션을 갖고 있었고,
**전부** 오버플로 로그가 찍혔습니다:

| 테이블 | 최대 파티션 | 오버플로 로그 |
| --- | --- | --- |
| `tm_tag_point_snapshot` | 918 MB | 있음 |
| `tm_tag_point_archive` | 638 MB | 있음 |
| `tm_tag_point` | 531 MB | 있음 |
| `tm_asset_oee_history_by_timestamp` | 178 MB | 있음 |

### 쓰기 타임스탬프 오염은 별개 문제입니다

같은 노드에서 `USING TIMESTAMP`에 µs 대신 ms를 넣은 과거 데이터가 발견됐습니다. 이것은
**파킹의 원인이 아닙니다** — 파티션이 작았다면 flush가 1970년 행과 2026년 행을 알아서 다른
SSTable로 갈랐을 것입니다. 다만 파킹 메시지의 창 범위를 56년으로 보이게 만들어 진단을 헷갈리게
하고, 그 자체로 따로 처리해야 할 문제입니다:

- 그 셀들은 쓰기 타임스탬프가 1000배 작아 **이후의 어떤 정상 쓰기에도 무조건 집니다**
  (같은 셀을 다시 쓰면 조용히 덮어써집니다) — 값을 신뢰할 수 없습니다
- SSTable은 **최대** 타임스탬프로 창에 배정되므로, 파킹된 채로 두면 `retention` 기한까지
  (`tm_tag_point`라면 10년) 디스크에 남습니다

**① 파킹 메시지의 창 범위를 본다**

```bash
grep "Parking window" system.log | grep -oE '\[[0-9]+\.\.[0-9]+\]'
```

수십 년이면 타임스탬프 오염이 섞인 것입니다. 파킹 자체는 파티션 크기 문제이지만, 오염 데이터도
같이 처리해야 합니다.

**② `sstablemetadata`로 확정한다**

```bash
sstablemetadata <sstable>-Data.db | grep -i "timestamp"
```

```
Minimum timestamp: 01/22/1970 00:39:21 (1784361462000)     ← 13자리 = ms 를 µs 로 해석
Maximum timestamp: 07/30/2026 16:27:07 (1785396427002000)  ← 16자리 = 정상 µs
```

`1784361462000`을 밀리초로 읽으면 2026-07-16입니다. `USING TIMESTAMP`에 ms 값을 넣어
카산드라가 µs로 해석하면 쓰기 타임스탬프가 1000배 작아집니다. TSCS는 쓰기 타임스탬프로 창을
정하므로 그 SSTable은 1970년부터 현재까지 **창 2만 개**에 걸치고, `window_size`를 1년으로 키워도
56개 창이라 split-refreeze가 수렴할 수 없습니다.

**③ 오염이 섞였으면 그 데이터도 걷어낸다**

오염된 셀은 이후의 어떤 정상 쓰기에도 무조건 지므로(같은 셀을 다시 쓰면 조용히 덮어써집니다),
값 자체도 신뢰할 수 없습니다. 걷어내지 않으면 파킹된 SSTable이 `retention` 기한까지 —
`tm_tag_point`라면 10년 — 디스크에 남습니다. 최대 타임스탬프 기준으로 창이 배정되기 때문입니다.

`auto_snapshot: false`이면 `TRUNCATE`가 스냅샷으로 디스크를 잡지 않습니다. 실행 전
`cassandra.yaml`에서 확인하십시오.

**④ 파티션을 줄인다 — 근본 해결**

`WindowRoutingIterator.maxBufferedBytesPerPartition`(64 MiB)을 넘는 파티션은 창 경계로 분할되지
않습니다. `SSTableWriter`가 파티션을 한 번에 받고 키를 한 번만 받으므로, 쪼개려면 파티션 전체를
힙에 올려야 하기 때문입니다. 예산을 넘으면 라우팅이 분할을 포기하고 통째로 씁니다 — 데이터는
온전하지만 결과가 창을 걸친 SSTable이라 모양이 안 변하고, 가드가 파킹합니다.

근본 해결은 파티션 키에 시간 버킷을 넣어 파티션을 줄이는 것입니다
(`PRIMARY KEY ((tag_id, day), timestamp)`). 100 MB를 넘는 파티션은 TSCS와 무관하게
repair·스트리밍·읽기를 모두 나쁘게 합니다.

**예산을 키우는 것은 권하지 않습니다.** 500 MB 파티션 × 동시 컴팩터 16개는 16 GB 힙에서
살아남지 못합니다. 실패 방식이 "느려짐"이 아니라 "노드 사망"입니다.

### 파킹을 방치할 때의 실제 비용

- 그 창은 동결되지 않습니다 → 해당 범위의 읽기 증폭이 내려가지 않습니다
- **만료는 정상입니다.** `isParked()`는 동결·분할 후보 선정에서만 참조되고 `retention` 경로에는
  없으므로, 때가 되면 창째로 삭제됩니다
- 데이터는 안전하고 조회도 정상입니다

## 3. 확인 명령

```bash
# 파킹·오버플로 발생 테이블
grep "Parking window"        system.log | grep -o "of [a-z_]*\.[a-z_]*" | sort | uniq -c
grep "window-routing buffer" system.log | grep -o "of [a-z_]*\.[a-z_]*" | sort | uniq -c

# 파킹된 창 목록 (JMX 전용 — nodetool 서브커맨드 없음)
#   org.apache.cassandra.db:type=Tables,keyspace=<ks>,table=<t>
#     ParkedTimeSeriesWindows      파킹된 창 -> 물고 있는 SSTable
#     FarFutureTimeSeriesSSTables  max_future_window 밖이라 모든 자동 경로에서 제외된 SSTable
# 둘 다 비어 있는 것이 정상입니다.
```

## 4. 운영 실측 (노드 41, 2026-08-02, 24k rows/s 유입)

| 항목 | 값 |
| --- | --- |
| 쓰기 처리량 | 24,224 rows/s (키스페이스 전체) |
| 로컬 쓰기 지연 | 0.070 ~ 0.103 ms |
| 드롭 메시지 · Blocked | 0 · 0 |
| 읽기당 SSTable | p95 1.00개 |
| 컴팩션 백로그 | 107 ~ 129 사이 진동, 누적 없음 |
| SSTable 압축률 | 0.190 (5.3배) |
| CPU | 48코어에 load 45 — **여유 없음.** 이 상태에서 무거운 읽기는 타임아웃 |

TSCS 수명주기 전체를 분 단위로 압축해 확인했습니다
(`window_size 2m` / `freeze_after 4m` / `retention 10m` / TTL 8분):

- 메모테이블 하나가 창 경계에 정확히 맞춰 SSTable 6개로 분할 (`:16 :18 :20 :22 :24 :26`)
- TTL 지점(t+8분)에서 행 수가 평평해짐 — TTL 회수 동작
- 오래된 창부터 파일째 삭제되어 6 → 5 → 4 → 3 → 2개
- 삭제 시각이 `windowStart <= now - retention - window_size` 공식과 초 단위로 일치

## 5. 예제

### 5.1 테이블에 TSCS 켜기

`retention = TTL + window_size` 규칙을 지켜야 합니다. TTL 10년(315,360,000초)에
`window_size 1d`면 `retention`은 3651d입니다.

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size':  '1d',
  'freeze_after': '2d',
  'retention':    '3651d'
} AND default_time_to_live = 315360000
  AND gc_grace_seconds = 86400;
```

`gc_grace_seconds`는 `freeze_after`보다 **작아야** 합니다. 동결은 실제
`CompactionController`로 도는데, `gc_grace`가 더 크면 그 시점에 톰스톤을 회수하지 못하고
창이 SSTable 1개로 줄어든 뒤에는 다시 동결 후보가 되지 않아 영영 남습니다.

옵션 검증 규칙은 `retention >= window_size + freeze_after` 입니다. 위반하면
`ConfigurationException`으로 `ALTER`가 거부됩니다.

### 5.2 TSCS가 실제로 도는지 20분 만에 확인하기

하루 단위 설정으로는 며칠을 기다려야 볼 수 있는 수명주기를 분 단위로 압축합니다.
운영 클러스터에서도 돌릴 수 있을 만큼 가볍습니다(100 rows/s).

```sql
CREATE KEYSPACE IF NOT EXISTS tstest
  WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE tstest.retention_probe (
    series text, ts timestamp, v double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 480          -- 8분
  AND gc_grace_seconds = 60               -- freeze_after(4m=240s) 보다 작게
  AND compaction = {
    'class': 'TimeSeriesCompactionStrategy',
    'window_size':  '2m',
    'freeze_after': '4m',
    'retention':    '10m'                 -- >= 2m + 4m
  };
```

데이터를 넣으면서 30초마다 관찰합니다:

```bash
DIR=$(ls -d $CASSANDRA_DATA/tstest/retention_probe-*/ | head -1)
while true; do
  echo "$(date +%H:%M:%S) SSTable=$(ls $DIR/*-Data.db 2>/dev/null | wc -l)"
  for f in $DIR/*-Data.db; do
    sstablemetadata "$f" 2>/dev/null | awk '/Minimum timestamp/{a=$0} /Maximum timestamp/{print "   ", a, "->", $0}'
  done
  sleep 30
done
```

**정상이면 이렇게 나옵니다** (실측):

```
t+16m  SSTable=6   04:16:00-04:17:57  04:18:00-04:19:59  04:20:01-04:21:59
                   04:22:01-04:23:59  04:24:02-04:25:58  04:26:01-04:27:29
t+18m  SSTable=5   04:18 ~ 04:26                  <- 04:16 창이 통째로 삭제됨
t+18m  SSTable=4   04:20 ~ 04:26                  <- 04:18 창 삭제
t+20m  SSTable=3   04:22 ~ 04:26                  <- 04:20 창 삭제
```

세 가지를 한 번에 확인하는 셈입니다:

1. **창 경계 분할** — 각 SSTable의 시간 범위가 `:16 :18 :20 :22 :24 :26` 경계에 정확히 맞고,
   어느 것도 창을 걸치지 않습니다
2. **TTL 회수** — 행 수가 TTL 지점(t+8분)에서 평평해집니다
3. **retention 통삭제** — 오래된 창부터 파일째 사라집니다. 삭제 시각은
   `windowStart <= now - retention - window_size`(= now − 12분)와 일치해야 합니다

끝나면 지웁니다:

```sql
DROP KEYSPACE tstest;
```

### 5.3 파킹 진단 한 번에

```bash
L=$CASSANDRA_HOME/logs/system.log

# 어느 테이블이 파킹됐나
grep "Parking window" $L | grep -o "of [a-z_]*\.[a-z_]*" | sort | uniq -c | sort -rn

# 창 범위 -- 수십 년이면 타임스탬프 오염, 며칠~몇 주면 라우팅 버퍼 초과
grep "Parking window" $L | grep -oE '\[[0-9]+\.\.[0-9]+\]' | sort -u

# 버퍼를 넘긴 테이블 (메시지가 테이블명을 담는다)
grep "window-routing buffer" $L | grep -o "of [a-z_]*\.[a-z_]*" | sort | uniq -c
```

창 범위를 사람이 읽는 시각으로:

```bash
python3 - <<'PY'
import datetime, re, sys
for line in sys.stdin:
    for a, b in re.findall(r'\[(\d+)\.\.(\d+)\]', line):
        f = lambda x: datetime.datetime.fromtimestamp(int(x) / 1000)
        print(f"{f(a):%Y-%m-%d} ~ {f(b):%Y-%m-%d}   ({(int(b)-int(a))/86400000:.0f}일)")
PY
```

`1970-01-21 ~ 2026-07-28 (20642일)` 처럼 나오면 타임스탬프 오염입니다.

### 5.4 오염 여부 확정

```bash
sstablemetadata <sstable>-Data.db | grep -i "timestamp"
```

```
Minimum timestamp: 01/22/1970 00:39:21 (1784361462000)      <- 13자리, 오염
Maximum timestamp: 07/30/2026 16:27:07 (1785396427002000)   <- 16자리, 정상
```

테이블 전체에서 오염 SSTable을 세려면:

```bash
for f in $DIR/*-Data.db; do
  m=$(sstablemetadata "$f" 2>/dev/null | awk '/Minimum timestamp/{print $NF}' | tr -d '()')
  [ -n "$m" ] && [ "$m" -lt 1000000000000000 ] && echo "오염: $(basename $f)"
done
```

현재 유입되는 쓰기가 정상인지도 같이 봅니다 — 16자리여야 합니다:

```sql
SELECT writetime(value) FROM pp.tm_tag_point WHERE tag_id = 'TAG-001' LIMIT 1;
```
