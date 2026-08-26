# SIMD 디코드 설계 — 실제로 할 일은 "SIMD 구현"이 함의하는 것보다 훨씬 적다

상태: **판정 완료 (2026-08-04) — 벡터 커널 보류 확정.** 사전 등록한 임계값에 대한 실측(§10)으로
종결됐다: whole-chunk 스캔에서 ALP+언팩 수학은 **5~8%**로 15% 임계값 미달 — 지배 비용은 커서·행
조립(~92%)이다. 스칼라 이득 두 건(§0.3의 RLE 워드 단위 전개, running-rank 규칙 — v4 커서가
`rankCalls()==0`으로 강제)은 반영되어 있고, 인코더 게이트는 여유 13.7×로 통과했다(§10).

**기록된 재론 트리거는 SIMD 기술이 아니라 실행 경로다**: 행 조립을 우회하는 **컬럼나 직접 집계
경로**가 생기는 순간 블록 수준의 ALP 지분(재구성 76%, 언팩+ALP 90%)이 노출되고, 그때 AVX-512
ALP 커널(`vcvtqq2pd`+`vmulpd`)이 실질 다중배 이득이 된다(§10 부수 발견 1). 대상 CPU 기준선은
사용자 결정으로 **AVX-512 서버급**이다 — AVX2뿐인 노드(운영 node 41, Haswell E5-2676 v3)는
스칼라 폴백으로 동작한다.

아래 본문은 그 판정에 이른 분석·게이트 설계의 기록이다. SP4 Phase 3([sp4-plan.md](sp4-plan.md))의
"Java Vector API 뒤에서 ALP·타임스탬프·null 비트맵을 벡터화한다"는 한 줄을 **범위를 좁히는
방향으로** 대체했다.

## 0. 결론 먼저

> **이 작업의 올바른 결과물은 대부분 SIMD가 아니다.** 측정 가능한 이득의 거의 전부는
> width-specialized **스칼라** 커널과 두 건의 알고리즘 수정에서 나오고, vector kernel은
> 게이트를 통과하지 못하면 **삭제하는 것이 성공**이다.

이 결론을 강제하는 발견이 셋이다.

### 0.1 Vector API는 하드 의존성이 될 수 없다

[`build.xml:47-48`](../../build.xml)이 지원 JDK를 선언한다.

```xml
<property name="java.default" value="11" />
<property name="java.supported" value="11,17,21" />
```

`jdk.incubator.vector`는 **JDK 16 이전에 존재하지 않는다.** 따라서 `src/java` 안의 import 한
줄이 JDK 11 빌드를 깬다 — 런타임 fallback을 아무리 잘 써도 소용없다. 컴파일이 먼저 죽는다.
(이 저장소는 CLAUDE.md 기준 Java 21로 빌드하지만, `java.supported`는 upstream과 함께 관리되는
값이고 이 한 줄을 위해 그것을 좁히는 것은 upstream 병합 비용을 영구히 늘린다.)

필요한 구조:

- **별도 source set `src/java-vector`** — JDK 21에서만 컴파일되는 `<javac>` 타깃, 결과물은
  선택적 모듈/디렉토리.
- **reflective holder** — `Class.forName`으로 커널을 찾고 `catch (Throwable)`로 스칼라에
  떨어지는 홀더. 이 패턴은 이미 저장소에 있다:
  [`src/java/org/apache/cassandra/utils/FastByteOperations.java:148-178`](../../src/java/org/apache/cassandra/utils/FastByteOperations.java)
  의 `BestHolder.getBest()` — `Class.forName(UNSAFE_COMPARER_NAME)` → 실패 시
  `PureJavaOperations`. 새 패턴을 발명하지 말고 이것을 그대로 따른다.

즉 vector kernel의 **최소 비용이 이미 "빌드 시스템 특수 케이스 + 리플렉션 홀더"** 다. 이 비용은
커널이 아무리 빨라도 사라지지 않으며, §6의 게이트가 존재하는 이유이기도 하다.

### 0.2 종단 이득의 상한이 약 1%다

[tiering-benchmark.md:41-53](tiering-benchmark.md)의 단일 파티션 50,000행 질의는 계층화 경로에서
**66~125 ms** 다(`variance`/`stddev` 66 ms ~ `OHLC + change + p95` 125 ms. 호스트: 2 × Xeon
X5670, Westmere, SSE4.2 — [rw-throughput-benchmark.md:30](rw-throughput-benchmark.md)).

그 안에서 bit unpacking이 차지하는 비중은 **0.3~1.8%** 다. 따라서 **완벽한 4배 커널의 종단
기여는 0.25~1.4%** 다. 4배가 아니라 무한대여도 2% 를 넘지 못한다.

이 숫자가 설계 전체를 지배한다. "몇 % 를 위해 인큐베이터 의존성과 빌드 특수 케이스와 두 번째
구현을 들인다"는 거래는 대부분의 경우 성립하지 않으며, 성립하는지 여부는 **미리 적어둔
임계값**(§6)으로만 판정한다.

### 0.3 디코드 최대 이득 두 개는 SIMD가 아니다

1. **RLE bit-setting 수정** — `BlockPresence.setBits`가 present run을 **행마다** read-modify-write
   하던 문제. 1024행 블록의 단일 present run이 16개 워드를 채우는 데 1024회 종속 반복을 썼다.
   head partial word masked OR + `Arrays.fill` + tail partial word masked OR 로 교체하면
   O(rows) → O(words). 이 함수 기준 10~60배.
2. **running rank 규칙** — `BlockPresence.rank`는 O(offset/64)다. seek에는 맞고 sequential scan
   에는 틀렸다. 순차 스캔이 행마다 호출하면 O(n · n/64)가 된다. 커서는 running value index를
   들고 다니며 행의 presence bit만큼 증가시켜야 한다.

둘 다 벡터 명령을 한 개도 쓰지 않는다. 둘 다 §0.2의 상한과 무관하게 이득이 실재한다.

## 1. 이득 순위

| # | 항목 | SIMD? | 판정 |
|---|---|---|---|
| 1 | RLE word-at-a-time expansion | 아니오 | **최대 이득. 먼저 한다.** |
| 2 | v4 cursor의 running rank | 아니오 | **미작성 코드에 대한 설계 제약. 지금이 유일하게 싼 시점.** |
| 3 | width-specialized **스칼라** unpack kernel | 아니오 | **이득의 대부분.** |
| 4 | vector unpack kernel | 예 | 진짜로 벡터화됨. AVX2로 충분. 게이트 대상. |
| 5 | ALP scaled-integer reconstruct | 부분 | AVX-512DQ에서만. 스칼라 dense/exception 분리가 선행. |
| 6 | ALP-RD recombination | 예 | 우아하지만 **운영에 없는 데이터 형태**를 위한 것. |
| 7 | FOR / delta-FOR | — | C2가 이미 한다. **아무것도 하지 않는다.** |
| 8 | presence BITMAP / ALL_PRESENT / ALL_NULL | — | 이미 최적. |
| 9 | vector `rank` | 불가 | `AVX512-VPOPCNTDQ` 필요. 호스트 237(Cascade Lake)에 **없다.** |
| 10 | 본질적 직렬 구간 | 불가 | RLE run walking, canonical varint read, dictionary expansion, Chimp XOR chain. |

### 1.3 (#3) "C2가 이미 unpack loop을 auto-vectorize한다"는 **거짓이다**

이 주장이 이 설계에서 가장 자주 나오는 오답이고, 코드를 보면 바로 반증된다.
[`BitPacking.unpack`](../../src/java/org/apache/cassandra/db/timeseries/BitPacking.java) `:239-254`:

```java
for (int i = 0; i < count; i++)
{
    long v = cur >>> bitOffset;
    bitOffset += width;
    if (bitOffset >= 64)
    {
        bitOffset -= 64;
        wordIndex++;
        cur = wordIndex < words ? src.getLong(base + (wordIndex << 3)) : 0L;
        if (bitOffset != 0)
            v |= cur << (width - bitOffset);
    }
    dst[i] = v & mask;
}
```

`cur`, `bitOffset`, `wordIndex`가 **반복 간에 이월(loop-carried)** 되고, 그 위에 분기가 둘
얹힌다. C2의 SuperWord는 loop-carried dependency와 control flow가 있는 루프를 건드리지 못한다.
따라서 이 루프는 지금 **한 줄도 벡터화되어 있지 않다.**

폭 `w`를 컴파일 타임 상수로 고정한 width-specialized 커널을 만들면 `bitOffset` 수열이 **정적**이
되고(주기 `lcm(w,64)/w` 값마다 반복), 워드 경계 분기가 **펼쳐져 사라진다.** 그 시점에서야 비로소
자동 벡터화의 여지가 생기고, 그전에도 이미 분기 제거와 상수 shift만으로 상당한 이득이 있다.

Lucene의 출하 경험이 정확히 이것이다: **생성된 스칼라 커널이 이득의 대부분**이고, vector API는
그 위에 **추가로 1.3~2배**를 얹는다. 즉 #3을 건너뛰고 #4로 가면 vector kernel은 잘못된 기준선
(현재의 branchy 스칼라 루프)과 비교되어 실제보다 훨씬 좋아 보인다. §6 게이트 A가 비교 대상을
**specialized scalar** 로 못박는 이유다.

### 1.5 (#5) ALP scaled-integer reconstruct는 AVX-512DQ에서만 벡터화된다

`value = (double) encoded * factor` 형태의 재구성에서 핵심은 `long` → `double` 변환이다. JIT가
내는 `L2D`는 **`vcvtqq2pd`** 로 컴파일되는데 이 명령은 **AVX-512DQ** 에서만 존재한다. AVX2에는
`long` 벡터 → `double` 벡터 변환 명령 자체가 없다(32비트 정수 변환만 있다).

그러므로 #5는 하드웨어 조건부다. 다만 **스칼라 dense/exception 분리는 조건과 무관하게 먼저
한다** — exception 처리 분기를 dense 루프 밖으로 빼는 것은 어떤 ISA에서도 이득이고, 벡터화의
전제이기도 하다.

### 1.6 (#6) ALP-RD recombination은 우아하지만 대상 데이터가 없다

left/right part를 8-entry dictionary로 합치는 연산은 벡터로 **한 개의 `vpermq`** 다. 설계로서는
가장 깔끔하다. 그러나 ALP-RD가 선택되는 데이터는 full-precision, 십진 양자화되지 않은 double
계열이고, [codec-bakeoff.md](codec-bakeoff.md)가 기록하듯 **측정된 운영 분포에는 그런 컬럼이
없다**(`docker/scale-workload.py` — 운영 판독값은 전부 십진 양자화). 즉 존재하지 않는 워크로드를
위해 두 번째 구현을 들이는 항목이다. 하지 않는다.

## 2. 결정적 API 제약 — 커널은 `long[]`에서 로드해야 한다

JDK 21 기준:

- `Vector.fromByteBuffer(...)` — **삭제됨**(JDK 19에서 deprecate, 이후 제거).
- `Vector.fromMemorySegment(...)` — **preview API.** `--enable-preview`가 필요하고, 그것은
  §7의 금지 목록에 있다(preview 플래그는 클래스 파일에 minor version을 새겨서 정확히 그 JDK
  빌드에서만 실행되게 만든다 — 운영 노드에 배포할 수 없는 산출물이 된다).

남는 것은 **`fromArray(SPECIES, long[], offset)`** 뿐이다. 결과적으로 커널의 입력은
`ByteBuffer`가 아니라 `long[]` + `wordOffset`이어야 하고, 이는 API 설계 결정이 아니라 **제약**
이다. 청크 읽기 경로가 `ByteBuffer`를 들고 있으므로, 커널 진입 전에 워드 배열로 올리는 지점이
어디인지가 설계의 실질 내용이 된다.

> **250줄을 쓰기 전에 30분짜리 compile spike로 이것부터 검증한다.** `fromArray` 하나만 쓰는
> 최소 클래스를 `src/java-vector`에 두고 JDK 21로 컴파일 + 리플렉션 로드가 되는지 확인한다.
> 이 30분이 실패하면 §1의 #4는 그 자리에서 끝나고 #1~#3만 남는다 — 그것도 정상적인 결과다.

## 3. 가장 어려운 규칙: 벡터 경로는 **영원히 디코드 전용**

**`pack`, `chooseWidth`, `BlockPresence.encode`, `BlockPresence.chooseMode`, 그리고 모든 ALP
planning은 단일 구현 스칼라로 남는다.** 예외 없음. "인코드도 벡터화하면 flush가 빨라진다"는
제안은 미래에 반드시 다시 나오고, 그때 이 절을 인용해서 거절한다.

이유는 성능이 아니라 **실패 모드의 비대칭성** 이다.

| | 디코드 불일치 | 인코드 불일치 |
|---|---|---|
| 증상 | 틀린 값을 반환 | 같은 입력이 다른 바이트를 생성 |
| 탐지 | **differential test가 즉시 잡는다** | round-trip test는 **전부 green으로 통과한다** |
| 운영 영향 | 질의 결과 오류(눈에 보임) | `chunkUnchanged`가 영원히 "달라졌다"고 보고 → **re-encoder livelock** |

인코드 쪽 두 번째 구현은 바이트 결정성([chunk-format-v4.md](chunk-format-v4.md) §5 rule 5)을
깨는 가장 값싼 방법이고, 그 깨짐은 테스트를 통과한 채로 배포되어 컴팩션이 멈추지 않는 형태로
드러난다. 디코드 쪽 두 번째 구현은 틀리면 시끄럽게 틀린다. 그래서 한쪽만 허용한다.

## 4. 정확성 — differential property test

오라클은 **기존 스칼라 `BitPacking.unpack`** 이고, 그 스칼라 자체는 이미 스펙의 golden vector에
핀되어 있다. 그 위에 다음 곱집합 전체를 도는 differential property test를 둔다.

| 축 | 값 |
|---|---|
| width | **65개 전부** (0..64) |
| count | 0, 1, 63, 64, 65, 127, 128, 1023, 1024, 1025, 4096 |
| value pattern | adversarial (all-zero, all-ones, 최상위 비트만, 경계값, 난수) |
| wordOffset | 0, non-zero |
| kernel | Scalar, vector @ `SPECIES_128` / `SPECIES_256` / `SPECIES_512` / `SPECIES_PREFERRED` |

**양쪽 모두 직접 인스턴스화로 강제한다** — 홀더가 고른 구현을 쓰면, 벡터 커널이 로드되지 않은
환경에서 테스트가 조용히 **스칼라 대 스칼라**를 비교하며 green이 된다. 그 green은 정보가 0이고
정보가 0인 green은 위험하다.

같은 이유로 **`Assume.assumeTrue`로 skip하는 테스트는 없는 것보다 나쁘다.** JDK 21 테스트 JVM
인자에 **`-Dcassandra.test.require_vector_kernel=true`** 를 넣어, 커널이 없으면 skip이 아니라
**fail** 하게 한다. 그래야 커널이 CI에서 조용히 사라지지 않는다.

### 4.1 실행 구성 3종

| 구성 | 무엇을 증명하는가 | 무엇을 증명하지 못하는가 |
|---|---|---|
| AVX 없는 벤치 호스트 (2 × Xeon X5670, SSE4.2) | **모든 lane arithmetic의 bit-identity** — Vector API는 SIMD가 없으면 스칼라로 폴백해 실행되므로 정확성은 전부 검증된다 | 속도, AVX-512 코드 경로 |
| 호스트 237 (Cascade Lake) | AVX-512 경로 + 실측 속도 | AVX2-only 하드웨어에서의 거동 |
| 호스트 237 + `-XX:UseAVX=2` | **AVX2-only 하드웨어의 대역**(node 41이 그럴 수 있다) | — |

세 번째 구성이 있는 이유는 §8이다. 운영 노드가 AVX2까지만이면 게이트는 그 조건에서 통과해야
한다.

## 5. 게이트 — 순차적이며, 하나라도 실패하면 vector 경로를 삭제한다

**임계값은 측정 전에 적는다. 측정 후에 적으면 그것은 게이트가 아니라 사후 합리화다.**

| 게이트 | 대상 | 임계값 |
|---|---|---|
| **A** | kernel microbench vs **specialized scalar** (branchy 스칼라 아님) | ≥ **2.0×** |
| **B** | block decode 전체 | ≥ **1.25×** |
| **C** | chunk read | ≥ **1.10×** |
| **D** | query battery ([tiering-benchmark.md](tiering-benchmark.md)와 동일 질의) | ≥ **1.05×** |
| **E** | 이득이 **AVX-512DQ에서만** 존재하고 node 41에 그 플래그가 없다면 | **무조건 삭제** |

### 5.1 SKU 함정 — Silver 4210R

호스트 237의 Xeon Silver 4210R은 **512비트 FMA 포트가 하나뿐**이고 AVX-512 실행 시 **주파수가
내려간다**(downclocking). 즉 SPECIES_512가 SPECIES_256보다 이론상 2배여도 실측은 그렇지 않고,
경우에 따라 **더 느리다.** 게다가 downclock은 같은 코어의 **다른 스레드**(= Cassandra의 나머지
전부)까지 느리게 만든다 — microbench에는 절대 나타나지 않고 게이트 C/D에서만 나타나는 종류의
손해다.

> **규칙: SPECIES_256이 SPECIES_512의 ~20% 이내면 256으로 고정한다.** `SPECIES_PREFERRED`를
> 그냥 쓰지 않는다.

## 6. 예상 결과 — 미리 적어둔다

**A 통과, B 애매, C·D 실패.** 그 경우 **vector kernel과 빌드 배선을 삭제하고 스칼라 작업만
남긴다.**

이것은 실패가 아니라 **성공이다.** 결과물:

- 약 **4.5일**
- 인큐베이터 의존성 **없음**
- 빌드 특수 케이스 **없음**
- 시작 시 경고 **없음**
- 그리고 **얻을 수 있었던 이득의 거의 전부**(§0.3의 #1·#2 + §1의 #3)

이 문단이 미리 적혀 있는 이유는, 4일을 쓴 뒤에는 "그래도 커널은 남기자"는 결론이 반드시
매력적으로 보이기 때문이다. 그 시점의 판단은 이미 투입한 비용에 오염되어 있으므로, 삭제 결정은
투입 전에 내려두고 게이트 결과만 기계적으로 적용한다.

## 7. 전제 0 (해소됨) — node 41의 CPU 플래그

운영 node 41은 **Haswell E5-2676 v3: AVX2까지, AVX-512 없음**이다. 확인 방법은
`grep -m1 flags /proc/cpuinfo`에서 `avx2`/`avx512f`/`avx512dq`/`avx512vpopcntdq`를 보는 것.

이 답 자체는 벡터 경로를 탈락시키지 않는다 — 대상 CPU 기준선이 사용자 결정으로 **AVX-512
서버급**(237급 이상)으로 확정됐고, AVX2뿐인 노드는 스칼라 폴백으로 동작하기 때문이다. 이로써
§5의 게이트 E(AVX-512 전용 이득 시 삭제)는 삭제 사유가 아니게 됐다. 벡터 경로를 접은 실제
근거는 §10의 측정이다.

## 8. 하지 말 것

명시적으로 금지한다 — 나중에 누가 "좋은 아이디어"로 다시 제안하지 않도록.

1. **스칼라 경로를 느리게 만드는 어떤 변경도** 하지 않는다. 스칼라가 모든 JDK·모든 호스트의
   기본 경로다.
2. **쓰기 쪽 두 번째 구현**을 만들지 않는다(§3).
3. **SIMD를 돕기 위한 포맷 변경**을 하지 않는다. [chunk-format-v4.md](chunk-format-v4.md) §6이
   정렬 문제를 이미 정리했다.
4. "vector-friendly 변형"을 위한 **새 `blockEncoding`** 을 만들지 않는다. 인코딩이 하나 늘면
   디코더 경로가 하나 늘고, 그것은 §3의 비대칭성을 인코드 쪽으로 되가져온다.
5. **vectorized `rank`** 를 시도하지 않는다(§1 #9).
6. 버전 독립적인 **`conf/jvm-server.options`에 `--add-modules`** 를 넣지 않는다. 그 파일은 JDK
   11에서도 읽히고, 거기 들어간 `--add-modules jdk.incubator.vector`는 JDK 11 노드의 **시작을
   막는다.**
7. 게이트 C·D 통과 전에 **모듈을 기본 활성화**하지 않는다.
8. 모듈이 없을 때 **`StartupCheck`·WARN·예외**를 내지 않는다. 없는 것이 정상 상태다. 부재를
   알리는 로그 한 줄은 모든 노드의 모든 재시작에 붙는 노이즈이고, 운영자가 고칠 수 있는 문제를
   가리킨다는 잘못된 신호를 준다.
9. **`--enable-preview`** 를 어디에도 쓰지 않는다(§2).
10. **CPU 이름이 옆에 적혀 있지 않은 호스트의 벤치마크 숫자를 인용하지 않는다.** 이 문서의 모든
    수치에는 호스트가 붙어 있다. 그 규율이 깨지면 §0.2의 상한 계산이 무의미해진다.

## 9. 작업 순서 (최종 상태)

1. **전제 0** — node 41 CPU 플래그 확인(§7). *(완료 — AVX2까지; AVX-512 기준선 결정으로 무해)*
2. **#1 RLE word-at-a-time expansion** — `BlockPresence.setBits`. *(완료: differential property
   test 포함)*
3. **#2 running rank 규칙** — `BlockPresence.rank` javadoc에 규범으로 명시, v4 커서가 준수.
   *(완료 — 커서가 `rankCalls()==0`을 강제하고 테스트가 단언, 실측 6.7×: §10)*
4. **#3 width-specialized 스칼라 커널** — 65폭. *(미착수 — 벡터 재론 시 게이트 A의 비교
   기준선이 될 선행 작업으로만 남음. §10의 폭별 언팩 실측이 그 기준선 데이터)*
5. **compile spike**(§2) · 6. **#4 vector unpack kernel** + 게이트(§4, §5) — *(착수하지 않음.
   §10의 사전 등록 측정이 15% 임계값 미달을 보여 보류 확정. 재론 트리거는 문서 상단 상태 참조)*

## 10. 실측 (2026-08-04) — 판정: 벡터 보류 확정, 인코더 게이트 통과

§9의 순서를 뒤집는 결정 두 건이 먼저 있었다. ① 사용자 확정: **대상 CPU 기준선은 AVX-512
서버급**(237급 이상; node 41 Haswell E5-2676 v3 — §7의 전제 0은 해소됐고 답은 "AVX2까지만" —
은 스칼라 폴백으로 동작). 이로써 게이트 E는 삭제 사유가 아니게 됐다. ② 사용자 확정: **범위를
"측정만"으로 축소** — 아래 판정 기준을 측정 **전에** 고정하고, JMH 분모 실측(§5의 T1~T3에
해당)만 수행한다. 벡터 커널(§9의 5~7번)은 기준 충족 시에만 재론.

사전 고정한 기준: (a) whole-chunk 디코드에서 (언팩+ALP 수학) **≥15%면 Phase V 재론**,
미만이면 보류 확정. (b) 인코더 **≥50k rows/s**(v4 스펙 §12 재인코딩 게이트 — double 블록이
플래닝 패스를 3번 도는 비용의 검증, v4의 유일한 미측정 항목이었다).

측정: `test/microbench/.../Chunk{BitUnpack,Presence,BlockDecode,Read,Encode}Bench` 5종.
호스트는 **237 = Xeon Silver 4210R(Cascade Lake, AVX-512)**, CI와 동일한
`eclipse-temurin:21-jdk` 도커에서 `org.openjdk.jmh.Main` 직접 실행(237 호스트 javac는 JDK 11이라
호스트 빌드 금지). 참고 1벌은 **로컬 = 2×X5670(Westmere, SSE4.2)**. JMH avgt/thrpt 5×1s, fork 1.

| 측정 (1024값 블록 / 3600행×8컬럼 청크) | 237 (4210R) | X5670 참고 |
|---|---|---|
| `unpackOnly` (w=10 레인) | 0.778 µs | 2.360 µs |
| `alpReconstructOnly` (FOR가산+곱셈2+스캐터) | 4.201 µs | 4.071 µs |
| `wholeBlockDouble` | 5.518 µs | 6.855 µs |
| `wholeBlockTimestamp` (DELTA w=0) | 2.898 µs | 3.142 µs |
| `fullScan` (청크 전체, 8컬럼) | 740.3 µs | 1188.6 µs |
| `projectedScan` (value+ts만) | 149.9 µs | 190.5 µs |
| `rankPerRow` vs `runningIndex` | 8.07 vs 1.21 µs (**6.7×**) | — |
| `encodeProduction` | 190.1 ops/s = **684k rows/s** | 146.3 = 527k |
| `encodeDoubleHeavy` (double 8컬럼) | 61.4 ops/s = **221k rows/s** | 55.1 = 198k |

### 판정

**(a) 벡터 보류 확정.** 청크 수준에서 ALP+언팩 수학은 ALP 디코드되는 double 블록 8~12개 ×
4.98µs = 40~60µs, `fullScan` 740µs의 **5~8%** — 어떤 구성 가정으로도 15% 미달. §0.2의 ~1%
추정은 방향이 옳았다(청크 수준에서 수학은 소수 지분).

**(b) 인코더 게이트 통과, 여유 13.7×.** 운영 형태 684k rows/s, ALP 최악 케이스도 221k.
3중 플래닝 패스는 비문제로 판명. §12 게이트 종결.

### 부수 발견 (다음 결정의 입력)

1. **블록 내부에서는 ALP 재구성이 76%, (언팩+ALP)가 90.2%다** (4.98/5.52). 청크 수준에서
   그것이 5~8%로 희석되는 이유는 **커서·행 조립이 fullScan의 ~92%를 차지**하기 때문
   (블록 수학 총합 ~60µs vs 740µs). 즉 §0의 구조 논증("병목은 수학이 아니라 조립")이 실측으로
   확인됐다. **조건부 기록: 조립을 우회하는 컬럼나 직접 집계 경로가 언젠가 생기면, 그 순간
   블록 수준 90%가 노출되고 AVX-512 ALP 커널(`vcvtqq2pd`+`vmulpd`)이 실질 다중배 이득이 된다.
   Phase V 재론의 진짜 트리거는 SIMD가 아니라 그 경로의 존재다.**
2. running index 규칙의 가치가 수치화됐다: rank-per-row 대비 **6.7×**. v4 커서의
   `rankCalls()==0` 강제가 지키는 것이 바로 이것.
3. 폭별 언팩(237): w=1 0.72µs → w=55 1.84µs(최악, straddle 최다), w=64 0.66µs(순수 복사).
   특화 스칼라 커널(§1 #3)의 비교 기준선으로 기록.

### 후속 (2026-08-26): 조립 비용의 절반은 컬럼 이름 조회였다

부수 발견 1이 지목한 "커서·행 조립 ~92%"를 파고든 결과, **그 중 절반 가까이가 디코드도 조립도
아니라 컬럼을 이름으로 다시 찾는 비용**이었다. 커서의 이름 기반 접근자는 호출마다 컬럼 디렉터리를
이진 탐색하고 프로브마다 `String.compareTo`를 돈다 — 스캔이 읽는 컬럼 집합은 스캔 내내 불변인데도
셀마다 그 조회를 반복했다(8컬럼 × 3,600행 = 28,800회, 조회당 ~3회 비교).

`ColumnarCursor.columnSlot(String)`으로 스캔당 한 번 해석하고 `getBytes(int)`/`getByteArray(int)`로
주소지정하도록 바꿨고, 투명 읽기 경로(`ChunkReadSupport`)가 이를 쓴다. 같은 호스트(234) 실측:

| 벤치마크 | 결과 (3회 스윕 최소값) |
| --- | --- |
| `ChunkReadBench.fullScan` (이름 기반, 종전) | 757.9 µs/op |
| `ChunkReadBench.fullScanBySlot` (슬롯 기반) | **392.7 µs/op** |

**1.93×, 청크 전체 스캔의 48%.**

> 수치는 [`.build/sh/ci-perf`](../../.build/sh/ci-perf)가 기록한
> [`perf-baseline.json`](perf-baseline.json)에서 왔고, **3회 스윕의 최소값**이다. 단일 스윕은 이
> 호스트에서 측정이 되지 못한다 — 같은 커밋으로 연속 측정했을 때 `fullScanBySlot`이 545 / 433 /
> 393 µs로 흔들렸다. 소음은 지연 벤치마크에 시간을 **더하기만** 하므로 최소값이 정직한 추정치이고,
> 평균은 그 순간 머신이 뭘 하고 있었는지 쪽으로 끌려간다. 긴 벤치마크(`fullScan`)만 단일 스윕에서도
> 안정적이었다(812 / 812 / 830 / 758) — 짧은 것일수록 소음 지분이 크다.

따라서 부수 발견 1의 지분 계산은 갱신되어야 한다: 블록 수학 ~60µs는 이제 758이 아니라 **393µs
기준으로 ~15%**다. 벡터 커널 보류 판정 자체는 바뀌지 않는다(여전히 15% 임계값 부근이고, 지배
비용은 여전히 조립이다) — 그러나 **컬럼나 직접 집계 경로라는 재론 트리거는 그만큼 가까워졌다.**
