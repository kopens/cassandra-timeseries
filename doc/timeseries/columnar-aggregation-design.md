<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# 컬럼나 직접 집계 경로 — 설계 검토

상태: **보류 (2026-08-27, 사용자 결정).** 측정과 설계 검토까지 마치고 구현 전에 멈췄다.
보류 사유는 §5의 게이트가 미충족이어서가 아니라 §2.2의 리스크 그 자체다: 합법성 게이트는 읽기
복구의 화해 순서를 건드리고, 틀리는 방식이 "재인코더가 지운 데이터의 부활"이며, 업스트림 머지
비용이 영구히 늘어난다. 재론하려면 §5의 서버 측 분해 측정부터 하면 된다 — 조립 층 실측(§0.2)과
`assembledScan` 벤치마크는 남겨 두었으므로 그 지점부터 이어진다.
[simd-decode-design.md](simd-decode-design.md) §10이 벡터 커널을 보류하면서 기록한 재론 트리거가
"행 조립을 우회하는 컬럼나 직접 집계 경로의 존재"였다. 이 문서는 그 경로를 실제로 만들 값어치가
있는지를 코드와 실측으로 따진 기록이다.

## 0. 결론 먼저

> **조립 층은 실측으로 크다. 그런데 그 위층은 아직 재본 적이 없고, 그 값이 API를 어디까지
> 끌어올려야 하는지를 결정한다.** 재기 전에 만들면 어제 이 저장소가 한 실수를 반복한다.

### 0.1 처음 세운 전제는 틀렸다

이 작업의 근거로 `ChunkReadBench.fullScan` 758 µs와 "커서·행 조립이 fullScan의 ~92%"를 인용해
왔다. 그러나 **투영은 이미 푸시다운되어 있다** —
[`ChunkReadSupport`](../../src/java/org/apache/cassandra/db/timeseries/tiering/ChunkReadSupport.java)의
클래스 javadoc "Projection" 절이 `queriedColumns()`를 커서에 넘긴다고 명시하고, `ChunkRowSource`가
실제로 그렇게 호출한다. 따라서 `avg(value_numeric)` 같은 집계 질의의 기준선은 8컬럼 `fullScan`이
아니라 **1컬럼 `projectedScan`**이다. 92%라는 지분은 집계 질의에 그대로 적용되지 않는다.

### 0.2 그래서 다시 쟀다

`ChunkReadBench.assembledScan`을 추가했다 — `projectedScan`과 **같은 투영**으로
`ChunkReadSupport.rowsFromChunk`를 통해 실제 `Row`까지 조립한다. 두 값의 차이가 정확히 직접 경로가
제거할 층이다(호스트 234, 3,600행, value + 타임스탬프 축):

| 벤치마크 | 결과 | 행당 |
| --- | --- | --- |
| `projectedScan` — 커서에서 멈춤 | 137.3 µs | 38 ns |
| `assembledScan` — 같은 투영, `Row`까지 | **379.3 µs** | **105 ns** |
| 차이 = 조립 층 | **242 µs** | **67 ns** |

**조립이 조립된 경로의 64%, 디코드 자체의 2.8배다.** 단일 컬럼 투영에서 그렇다는 점이 중요하다 —
컬럼이 늘면 조립은 컬럼 수에 비례해 늘고 디코드는 그보다 덜 늘어난다.

## 1. 지금 한 행이 지나는 경로

코드로 확인한 순서다.

```
ChunkV4Codec.Cursor                     디코드. 값은 fixedValues[]에 long으로 있다. 투영됨
 └─ ChunkReadSupport.RowAssembler       셀마다 byte[] 새로 직렬화(toFixedBytes) → ArrayCell → BTree → Row
     └─ SelectStatement.processPartition   행마다 result.newRow(), 컬럼마다 result.add(ColumnData)
         └─ ResultSetBuilder.newRow        행마다 groupMaker.isNewGroup(pk, clustering)
             └─ Selector.InputRow.add      ColumnData를 다시 ByteBuffer로
                 └─ Selectors.addInputRow
                     └─ Aggregate.addInput(Arguments)   arguments.get(0) → Number 역직렬화
```

디코더가 이미 `long`으로 쥐고 있는 값이 `byte[]` → `ArrayCell` → `Row` → `ByteBuffer` → `Number`를
왕복한다. **없애야 할 것은 코덱이 아니라 이 왕복이다.** 코덱은 38 ns/행으로 이미 싸다.

`toFixedBytes`가 "접근마다 새로 직렬화"하는 것은 회피 가능한 낭비가 아니다 — `ArrayCell`이
`byte[]`를 보유하고 셀이 밖으로 나가므로 스크래치 버퍼를 돌려쓸 수 없다. **셀을 만들지 않는 경로가
아니면 이 비용은 사라지지 않는다.** 그것이 이 설계의 전부다.

## 2. 설계 — 두 층

### 2.1 층 1: 컬럼나 집계 입력 API

`AggregateFunction.Aggregate`에 선택적 진입점을 추가한다.

```java
/**
 * @return true면 batch를 소비했다. false면 호출자는 행 단위 addInput으로 폴백한다.
 *         기본 구현이 false를 돌려주므로 기존 집계는 손대지 않아도 된다.
 */
default boolean addColumnarInput(ColumnarBatch batch) { return false; }
```

`ColumnarBatch`는 `long[] timestamps`와, 인자 컬럼마다 원시 뷰(`double[]`/`long[]`, null 비트맵
포함) 또는 폴백용 `ByteBuffer[]`를 노출한다. 청크 커서의 `fixedValues[]`가 그대로 그 뷰가 된다.

**이 API가 잘 맞는 이유는 우연이 아니다.** `TimeSeriesFcts`의 버퍼링 집계들(`integral`,
`time_weighted_average`, `counter_delta`/`counter_rate`, `percentile`, `variance`/`stddev`)은 **이미
원시 배열에 값을 모으고 있다** — `long[] times` + `double[] vals`. 지금은 그 배열을 행마다 한 칸씩
채우는데, 컬럼나 배치는 그 배열을 통째로 건네준다. `delta`/`rate`/`derivative`/`first`/`last`도
같고, 업스트림의 `avg`/`min`/`max`/`count`/`sum`도 마찬가지다.

얻는 것: 셀 `byte[]`·`ArrayCell`·BTree·`Row`·`InputRow`·`ByteBuffer`→`Number` 역직렬화가 전부
사라지고, 누적이 JIT가 벡터화할 수 있는 원시 루프가 된다. **그리고 이것이 곧 SIMD 재론 조건이다** —
`simd-decode-design.md` §10 부수 발견 1이 "조립을 우회하는 경로가 생기면 블록 수준 90%가 노출되고
AVX-512 ALP 커널이 실질 다중배 이득"이라고 적어 둔 그 경로다. **순서가 있다: 이 경로가 먼저이고
SIMD는 그 다음이다.**

### 2.2 층 2: 합법성 게이트 — 어려운 쪽

직접 경로는 아래가 **전부** 성립할 때만 정당하다.

| 조건 | 왜 |
| --- | --- |
| 단일 파티션(또는 IN)의 **순수 집계** — 행 단위 출력 없음 | 행을 돌려줘야 하면 조립을 피할 수 없다 |
| 그 범위에 **핫 행도 툼스톤도 없음** | 지금은 `ChunkMergeUnfilteredIterator`가 화해시킨다. 직접 경로는 화해할 것이 없음을 **먼저 증명**해야 한다. 이것이 이 설계에서 제일 어려운 부분이고, 패치가 아니라 프로젝트인 이유다 |
| group 경계를 타임스탬프에서 계산 가능 | `time_bucket`/`time_bucket_gapfill`은 버킷 = f(ts)라 배치 단위로 경계를 잘라낼 수 있다. 임의 `GROUP BY`는 안 된다 — 그때는 폴백 |
| 투영 밖 컬럼에 `WHERE` 제약 없음, 클러스터링을 벗어나는 `ORDER BY` 없음, `writetime`/`ttl` 선택 없음 | 전부 행 수준 정보를 요구한다 |
| 다이제스트·읽기 복구가 이 행들을 보지 않음 | 지금은 `TransparentReads`가 서는 위치가 그것을 보장한다(`resolveInternal` 이후). 직접 경로는 이터레이터를 통째로 우회하므로 **그 보장을 다시 세워야 한다** — 합성 행이 읽기 복구에 닿으면 재인코더가 지운 데이터를 베이스 테이블에 되살려 쓴다 |

두 번째 조건이 이 작업의 실질적 난이도다. 나머지는 판별식이지만, 이것은 읽기 경로의 화해 순서를
건드린다.

## 3. 무엇을 먼저 만드나

1. **스파이크(폐기 전제).** 층 2를 만들지 않고, 가장 단순한 형태 하나(단일 파티션 · 단일
   `avg(double)` · 핫 행 없음)에 대해서만 층 1을 뚫어 **서버 측 질의 시간을 실측**한다. 목적은 기능이
   아니라 숫자다.
2. 그 숫자가 §5의 임계를 넘으면 층 1의 API를 확정하고, `TimeSeriesFcts`의 버퍼링 집계부터 옮긴다.
3. 층 2의 합법성 판별을 세우고, 폴백이 **조용하지 않게** 만든다 — 직접 경로를 못 타면 왜 못 탔는지
   추적 가능해야 한다. 조용한 폴백은 "빨라졌다"고 믿으면서 아무것도 안 바뀐 상태를 만든다.

## 4. 하지 말아야 할 것

- **코덱을 건드리지 마라.** 디코드는 38 ns/행이다. 여기서 얻을 것은 없다.
- **SIMD를 먼저 하지 마라.** `simd-decode-design.md`가 이미 실측으로 보류했고, 재론 조건은 이
  경로의 존재다. 순서를 뒤집으면 같은 판정을 다시 받는다.
- **폴백을 조용히 만들지 마라.** §3.3 참고.

## 5. 착수 게이트 — 아직 미충족

**층 1의 API가 어디까지 올라가야 하는지는 조립 층 위의 비용에 달려 있는데, 그 값을 재본 적이
없다.**

- 조립까지: 105 ns/행 (실측, §0.2)
- 그 위(`processPartition` → `ResultSetBuilder` → `InputRow` → `Arguments` → `addInput`): **미측정**

간접 근거는 있다. [tiering-benchmark.md](tiering-benchmark.md)의 `time_bucket 1h + avg/min/max`가
40,000행 파티션에서 56 ms인데, 105 ns/행이면 디코드+조립은 4.2 ms — 전체의 7.5%다. 나머지 92%가
위층이라는 뜻이 되지만, **56 ms는 클라이언트 관측값이라 네트워크·프로토콜·코디네이션이 섞여
있다.** 이 숫자로 설계를 정할 수는 없다.

따라서 **착수 전에 서버 측 분해를 재야 한다.** 재는 방법은 두 가지 중 하나다: 코디네이터에서
집계 구간만 계측하거나, `processPartition`부터 `addInput`까지를 도는 JMH 하네스를 만드는 것.

두 결과에 따라 설계가 갈린다:

- **위층이 조립과 비슷하거나 작다** → 층 1을 `RowAssembler` 바로 위에 두면 된다. 작업량이 작다.
- **위층이 지배한다** → API가 `ResultSetBuilder`/`Selectors`까지 올라가야 하고, 이는 업스트림
  질의 머신을 건드리는 일이라 업스트림 머지 비용이 영구히 늘어난다. **그 경우 이득이 그 비용을
  넘는지가 별도 판단이 된다.**

이 게이트를 통과하기 전에는 착수하지 않는다. 이 저장소가 SIMD 판정에서 쓴 절차와 같다 — 임계를
미리 적고, 실측으로 종결한다.

## 부록: 재현

```bash
.build/sh/ci-perf --record        # 또는 PERF_CLASSES=ChunkReadBench PERF_PASSES=1 .build/sh/ci-perf
```

`ChunkReadBench.projectedScan`과 `ChunkReadBench.assembledScan`이 §0.2의 쌍이다. 두 벤치마크는
같은 청크·같은 투영을 쓰며, `assembledScan`만 `ChunkReadSupport.rowsFromChunk`를 통과한다.
