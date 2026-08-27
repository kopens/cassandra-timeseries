[English](README.md) · [한국어](README.ko.md)

# cassandra-timeseries

**Apache Cassandra for industrial time-series workloads** — a distributed time-series database for sensor and tag data from factories and plants.

Time-series data from industrial sites has a few properties of its own: every tag (series) accumulates endlessly at second resolution, years of it must be retained for compliance, edge devices that lost connectivity push days of backlog in one go (late backfill), and queries are almost always "this tag, this period". Stock Cassandra handles this workload, but compression, retention and aggregation all stay the application's problem.

This fork moves that part **into the database** — time-series computation finishes on the server (21 CQL functions plus gap-fill), old data is compressed and expired automatically (tiered storage plus a time-series compaction strategy), and **CQL does not change**. Compressed history reads back through an ordinary `SELECT` (transparent reads). The application never has to know whether the data is compressed.

It is a fork of [apache/cassandra](https://github.com/apache/cassandra) (`cassandra-6.0` branch). The on-disk format and CQL grammar are upstream's, so it **reads existing 6.0 data as-is** — every new feature is opt-in. Spark integration comes from the companion fork [cassandra-spark-connector](https://dev.kopens.io/common/cassandra-spark-connector) (Spark 4.1.2).

> Deep-dive documents under [doc/timeseries/](doc/timeseries/) are currently written in Korean; [examples.md](doc/timeseries/examples.md) is in English.

## 🎯 What it buys you (against upstream Cassandra 6.0.0)

**1. Time-series computation that finishes on the server.** Bucketing, aggregation, interpolation and regression in one line of CQL. The round trip where the application pulls raw rows and computes them itself disappears.

```sql
-- Hourly average with empty buckets filled in — in upstream, the app receives 100k rows and does this itself
SELECT time_bucket_gapfill(1h, timestamp, '2026-07-01', '2026-07-02'), locf(avg(latency))
FROM pp.tm_tag_point
WHERE tag_id='TAG-001' AND timestamp >= '2026-07-01' AND timestamp < '2026-07-02'
GROUP BY tag_id, time_bucket_gapfill(1h, timestamp, '2026-07-01', '2026-07-02')
ORDER BY timestamp ASC;
```

**2. Old data compresses itself; queries stay the same.** Tiered storage moves past windows into column-oriented chunks, and `SELECT` does not need to know — transparent reads merge them back automatically.

**3. Compression makes queries faster too — it wins on storage *and* latency.** Measured on the production table shape (`tm_tag_point`, 8 regular columns) at 20 million rows — host 234 (Xeon Silver 4114T, 40 threads), chunk format v4 — [full benchmark](doc/timeseries/tiering-benchmark.md):

| Metric | Before tiering | After tiering (v4) | Effect |
| --- | --- | --- | --- |
| Storage | 237.8 MB | **33.3 MB** | **7.1× smaller** (11.9 → ~1.7 B/row) |
| `count(*)` (40k-row partition) | 303 ms | **50 ms** | **6.1× faster** |
| `time_bucket` + aggregates | 150–270 ms | **31–64 ms** | **3–6× faster** |
| Gap-fill (locf / interpolate) | 162–284 ms | **30–53 ms** | **5.4× faster** |
| p95 per tag, 90 tags (3.6M rows) | 14.2 s | **2.6 s** | **5.4× faster** |
| Hourly average, 90 tags (3.6M rows) | 14.9 s | **4.2 s** | **3.5× faster** |
| 1-hour range + column projection | 55–56 ms | **30–33 ms** | **1.8× faster** |
| Newest 1,000 rows, `SELECT *` (no time range) | 52–56 ms | **38–45 ms** | Equal or better |
| Re-encode throughput | — | **108k rows/s** | 2.2× the design gate (50k) |

> **Every query measured is the same or faster after tiering** — including unbounded scans, single-row lookups and static-column reads. An unbounded `LIMIT` query decodes windows one at a time in query order (newest first on a `DESC` table) and stops once `LIMIT` is satisfied, so it does not slow down.
>
> **A query inside `hot_window` costs nothing** — a query starting above the cold boundary skips the merge entirely and returns the hot iterator unchanged. The figures above are the maximum-load case, with the whole range forced cold.
>
> **Two caveats.** A full-scan aggregate with no partition key returns wrong answers on a tiered table — that path does not merge chunks (see [the benchmark's caveats](doc/timeseries/tiering-benchmark.md)). And the storage saving depends on shape — a minimal shape with no constant or all-null columns to fold (one high-entropy `double`) has not been re-measured on v4, so measure your own table before committing.

**4. Compaction and retention that fit time series.** SSTables are ordered by time window and frozen one-per-window; windows past their retention are dropped whole, without compacting. Late backfill from edge devices is isolated into its own window automatically.

**5. Full-text search over log and event bodies.** SAI `LIKE` with `index_analyzer` gives real substring matching — Korean included — without `ALLOW FILTERING`.

## ✨ What this fork adds

| Feature | Summary | Detail |
| --- | --- | --- |
| **21 time-series CQL functions** | `time_bucket`, `first`/`last`, `delta`/`rate`/`derivative`, reset-aware `counter_delta`/`counter_rate`, `percentile`, `time_weighted_average`, `integral`, `variance`/`stddev`, `histogram`, `approx_count_distinct`, bivariate `corr`/`covar_*`/`regr_*` | [Usage §2–9](#using-time-series-cql) |
| **Gap-fill** | `GROUP BY time_bucket_gapfill(width, ts, start, finish)` — materialises empty buckets, with `locf()`/`interpolate()` fill policies | [Usage §3](#3-filling-gaps-time_bucket_gapfill) |
| **Full-text search** | SAI `LIKE` with `index_analyzer` (ngram/standard/cjk/keyword or JSON) — true substring matching including mid-word fragments, fragments spanning a space, and Korean, with no `ALLOW FILTERING` | [fulltext-search.md](doc/timeseries/fulltext-search.md) |
| **Time-series compaction (TSCS)** | `TimeSeriesCompactionStrategy` — window ordering, in-window UCS delegation, whole-window retention drops, closed-window freeze (one SSTable per window, `WindowFrozenListener` event hook, far-future guard `max_future_window`, and reclamation of data already expired **at the moment of freeze** without retention — data expiring after the freeze needs `retention`), plus late-data isolation (flush and streaming split at window boundaries so backfill lands locally in its own past window; legacy spanning SSTables are split automatically) and a **dedicated memtable** (opt-in per table — rows are assigned to their TSCS window at write time, removing flush routing and the 64 MiB partition cap; primitive-array column storage measured at **5.5× less heap per row**; cold windows on a tiered table flush straight to chunks) | [timeseries-compaction.md](doc/timeseries/timeseries-compaction.md) · [timeseries-memtable.md](doc/timeseries/timeseries-memtable.md) |
| **Column-oriented chunk codec (chunk format v4)** *(tiered storage, stage 1)* | One window = a shared timestamp axis plus an independent section per regular column, losslessly compressed, every block independently decodable and randomly addressable. `double` uses ALP/ALP-RD (the only double codec); integers and time types use FOR/delta bit-packing; `boolean` packs to one bit; `text` and opaque bytes use a dictionary (DICT) or RAW. A column whose value never changes becomes CONSTANT and an all-null column becomes ALL_NULL, both O(1). Measured at **~1.7 B/row** on 20M rows of the production shape — **7.1×** against row storage's 11.9 B/row, host 234 | [Format spec](doc/timeseries/chunk-format-v4.md) · [Codec bake-off](doc/timeseries/codec-bakeoff.md) |
| **Tiered storage (chunk store)** *(tiered storage, stage 2)* | A `timeseries_tiering` table extension policy — a background re-encoder compresses windows past `hot_window` into chunks and moves them to `<table>__chunks` (late-row merge, `cold_window` expiry, a consistency-level quorum floor). `nodetool retier`/`tieringstatus`, `system_views.timeseries_tiering`. **Transparent reads**: a `SELECT` on the base table merges hot rows with chunks automatically — ranges, point lookups, aggregates, gap-fill and `LIMIT`/`DESC` all work across hot and cold | [tiered-storage.md](doc/timeseries/tiered-storage.md) |
| **Test infrastructure** | 93 docker integration assertions (the release gate), a 49-assertion three-node cluster test, a 100-million-row scale harness, jvm-dtests, a JMH performance regression gate, and a GC comparison (ZGC vs G1) | [Reports](doc/timeseries/) |
| **Packaging / CI** | Testcontainers-compatible docker image, GitLab CI (build → test → image → integration gate → release), automated tag releases | [.gitlab-ci.yml](.gitlab-ci.yml) |

## 📖 Documentation

| Document | Contents |
| --- | --- |
| **[examples.md](doc/timeseries/examples.md)** | The source examples behind "Using time-series CQL" below (English) |
| [timeseries-functions-design.md](doc/timeseries/timeseries-functions-design.md) | Each function's signature and semantics, correctness under distribution, where the code lives |
| [gapfill-design.md](doc/timeseries/gapfill-design.md) | `time_bucket_gapfill` CQL syntax, interpolation rules, guardrails |
| [continuous-aggregates-design.md](doc/timeseries/continuous-aggregates-design.md) | Time-bucket rollups (continuous aggregates) — design in progress |
| **[fulltext-search.md](doc/timeseries/fulltext-search.md)** | SAI `LIKE` with `index_analyzer` — substring search over log and message bodies, Korean included |
| **[production-rollout.md](doc/timeseries/production-rollout.md)** | The checklist before enabling tiering on a real table — one-way doors, schema requirements, migrating TTL to `cold_window`, application impact, verification procedure, and what is still unverified |
| **[tiering-benchmark.md](doc/timeseries/tiering-benchmark.md)** | Before/after on 20M rows of the production shape (host 234, chunk v4) — storage **7.1×↓**, aggregates **3–6×↑**, gap-fill **5.4×↑**, every query equal or better including unbounded ones, re-encode 108k rows/s |
| **[operations-tuning.md](doc/timeseries/operations-tuning.md)** | Practical guide to moving to long retention (10 years) — capacity arithmetic, order of application, tuning values for the base **and chunk** tables with reasoning, how TTL relates to tiering, a checklist |
| **[timeseries-compaction.md](doc/timeseries/timeseries-compaction.md)** | `TimeSeriesCompactionStrategy` — window size, freeze and `retention` settings, the life of a window, late-data isolation, the two causes of a parked window and how to tell them apart, why `retention` rather than TTL owns expiry, **measurements from the production node** |
| **[timeseries-memtable.md](doc/timeseries/timeseries-memtable.md)** | `TimeSeriesMemtable` — how to enable it (a yaml key *and* an `ALTER TABLE`, and why that two-step is easy to get wrong), supported and unsupported schemas with fallback behaviour, removing the causes of parking, 5.5× less heap per row, **streaming reads** (binary search over slices, assembling only the rows needed, zero retention), cold-window direct chunk flush and its durability ordering, how to verify |
| **[prod-ops-report-2026-08-02.md](doc/timeseries/prod-ops-report-2026-08-02.md)** | Twelve hours on a real node — five deployments, two incidents in full with their fixes, the judgements that reversed, the measured procedures |
| **[prod-tscs-settings.md](doc/timeseries/prod-tscs-settings.md)** | Current settings across 75 tables and why, the diagnostic that separates the two causes of a parked window, values measured under 24k rows/s ingest |
| **[tiered-storage.md](doc/timeseries/tiered-storage.md)** | The `timeseries_tiering` policy and chunk re-encoder — configuration, chunk query patterns, operations (nodetool and virtual tables), invariants and limitations |
| **[compression.md](doc/timeseries/compression.md)** | What shrinks, why and by how much, column by column — how the two compression layers relate, per-type encoding and per-row cost, the saving broken down by column (4 of 8 columns cost zero bytes), how to estimate for your own table and how to measure it |
| **[chunk-format-v4.md](doc/timeseries/chunk-format-v4.md)** | **Wire format specification** for the one chunk format — header, directory, block table, the four presence modes, per-type block encodings including ALP, determinism rules, size limits. (v1–v3 are removed formats; reading one raises `UnsupportedChunkFormatException`) |
| [codec-bakeoff.md](doc/timeseries/codec-bakeoff.md) | Measured comparison of double codecs — why it settled on **ALP/ALP-RD alone**, with bytes-per-value by distribution (against Gorilla and Chimp128) |
| [integration-test-report.md](doc/timeseries/integration-test-report.md) | The CQL, results and timings of every assertion, as run in a real container |
| [scale-test-report.md](doc/timeseries/scale-test-report.md) | 100-million-row capacity verification — load and aggregate linearity in rows scanned (recorded on an older host; current figures are in the tiering benchmark) |
| **[rw-throughput-benchmark.md](doc/timeseries/rw-throughput-benchmark.md)** | Measured throughput — ingest **233k rows/s** (host 234), write path 424k rows/s, chunk encoding 684k rows/s, chunk full scan 740 µs / 3,600 rows (host 237, JMH), re-encode 108k rows/s; per-pattern read ops/s awaiting re-measurement on v4 |
| **[memtable-write-tuning.md](doc/timeseries/memtable-write-tuning.md)** | The write-path optimisation record — the two fast paths `DESC` turns off, the limits of configuration tuning, three rounds of code changes (min guard, reverse-order long store, tail index), the `ALTER` ordering trap |
| [sp4-plan.md](doc/timeseries/sp4-plan.md) | SP4 roadmap — insertion points, milestones and verification gates for compressed query, vectorised aggregation kernels, SIMD (see the document for current status) |
| [gc-comparison.md](doc/timeseries/gc-comparison.md) | Generational ZGC vs G1 on the same 100M rows — query time and write throughput (raw data) |
| **[g1gc-vs-zgc-article.md](doc/timeseries/g1gc-vs-zgc-article.md)** | The measurements above written up as a comparison article (environment, method, interpretation, recommended settings) |

Full directory: [doc/timeseries/](doc/timeseries/)

---

# Using time-series CQL

These are native functions — no separate installation, no UDF registration. Every example below is runnable CQL.

## Function reference

**Argument order matters.** Most time-series aggregates take `(value, timestamp)`.

| Function | Signature | Returns | Description |
| --- | --- | --- | --- |
| `time_bucket` | `time_bucket(duration, ts [, origin])` | `timestamp` | Bucket start time (a scalar, for downsampling) |
| `time_bucket_gapfill` | `time_bucket_gapfill(width, ts, start, finish)` | `timestamp` | A `GROUP BY` selector that also produces empty buckets |
| `locf` | `locf(aggregate)` | Same as its argument | Fills an empty bucket with the previous value (LOCF) |
| `interpolate` | `interpolate(aggregate)` | `double` | Fills an empty bucket by linear interpolation between neighbours |
| `first` / `last` | `first(value, ts)` / `last(value, ts)` | Type of `value` | Earliest / latest value by time |
| `delta` | `delta(value, ts)` | `double` | Last sample − first sample |
| `rate` | `rate(value, ts)` | `double` | `delta` ÷ elapsed seconds (endpoint to endpoint) |
| `derivative` | `derivative(value, ts)` | `double` | Least-squares regression slope, per second |
| `counter_delta` / `counter_rate` | `counter_delta(value, ts)` / `counter_rate(value, ts)` | `double` | Counter increase / per-second rate, corrected for resets |
| `percentile` | `percentile(value, q)` — `q` in `[0,1]` | `double` | Exact continuous percentile (linear interpolation) |
| `time_weighted_average` | `time_weighted_average(value, ts)` | `double` | Time-weighted mean |
| `integral` | `integral(value, ts)` | `double` | Area under the curve (value·seconds) |
| `variance` / `stddev` | `variance(value)` / `stddev(value)` | `double` | Sample variance / standard deviation |
| `corr` / `covar_pop` / `covar_samp` | `corr(y, x)` etc. | `double` | Correlation / population covariance / sample covariance |
| `regr_slope` / `regr_intercept` / `regr_r2` | `regr_slope(y, x)` etc. | `double` | Linear regression of y on x |
| `histogram` | `histogram(value, min, max, nbuckets)` | `list<bigint>` | Equal-width histogram (length `nbuckets+2`) |
| `approx_count_distinct` | `approx_count_distinct(value)` | `bigint` | HyperLogLog approximate distinct count |

## 1. Schema and sample data

Every example below runs on `tm_tag_point`, a real industrial tag table — one partition per tag, clustered by time, **newest first** (`DESC`). Compaction uses this fork's time-series strategy, `TimeSeriesCompactionStrategy` (TSCS): SSTables are ordered into time windows, closed windows freeze to one SSTable each, and windows past their retention are dropped whole without compacting. Compaction choices *inside* the current window are delegated to the UCS controller, so UCS's write-optimised behaviour is preserved.

```sql
CREATE KEYSPACE IF NOT EXISTS pp
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pp;

CREATE TABLE tm_tag_point (
    tag_id     text,                              -- partition key: one tag = one partition
    timestamp  timestamp,
    area_id    text static, asset_id text static, line_id text static,
    opc_id     text static, site_id  text static, tag_name text static,
    type       text static,                       -- 'boolean' | 'long' | 'double' | ...
    attribute  frozen<map<text,text>>,
    error_code int,
    latency    int,                               -- collection latency, always present
    quality    int,
    value      text,                              -- string copy of the reading
    value_boolean boolean,                        -- populated when type='boolean'
    value_numeric double,                         -- populated when type is numeric
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {'class': 'TimeSeriesCompactionStrategy',
                     'window_size': '1d',          -- window width (match the tiering chunk_window)
                     'freeze_after': '2h',         -- freeze to one SSTable this long after a window closes
                     'scaling_parameters': 'T4',   -- inside the current window, delegate to UCS (write-optimised 4-way)
                     'retention': '62d'}           -- drop a window whole once its upper bound passes 62 days
   AND default_time_to_live = 5356800;   -- 62 days (with retention, whichever comes first applies)

-- Statics are written once per tag, not per sample.
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

### 1.0 Which columns can be aggregated — this schema's biggest trap

**`value` is `text`, so numeric aggregation is impossible on it.** `avg(value)`, `percentile(value, 0.95)` and `delta(value, timestamp)` are all rejected (only numeric types are accepted: `tinyint`/`smallint`/`int`/`bigint`/`varint`/`float`/`double`/`decimal`/`counter`). More dangerous than the rejections are the calls that **succeed** — `min(value)`, `max(value)` and `count(value)` work on `text`, but compare **lexicographically**, so between `'9.1'` and `'20.76'` the `max` is `'9.1'`.

The columns you can do arithmetic on are `latency` (`int`, always present — the default in examples and smoke tests), `value_numeric` (`double`, numeric tags only), and the constant `error_code` and `quality`. The exceptions are `first`/`last`/`approx_count_distinct`, which are type-agnostic — `first(value, timestamp)` returns the `text` unchanged.

### 1.1 TSCS compaction options

| Option | Description |
| --- | --- |
| `window_size` | Window width (`<int><m\|h\|d>`). Match the tiering `chunk_window` |
| `freeze_after` | Freeze once this long has passed since the window closed — the grace period for late data |
| `scaling_parameters`, `target_sstable_size` | UCS delegation parameters for the current window (UCS syntax verbatim: `T4` = write-optimised 4-way tiered, etc.) |
| `retention` | Optional — drop the SSTable whole once the window's upper bound passes `now - retention` (must be at least `window_size + freeze_after`) |
| `max_future_window` | Guard against future timestamps (default `1d`) — keeps bad input from polluting windows |

At the moment of freeze (one SSTable per window), TTL data that has already expired is reclaimed without `retention`, and read amplification for partition+range queries is minimised. But a window that is already down to one SSTable never becomes a freeze candidate again, so **data expiring after the freeze needs `retention` to be reclaimed.** Late (backfill) data is separated at window boundaries on flush and lands locally in its own window. Detail: [§11 TSCS configuration](#11-time-series-compaction-tscs-configuration)

## 2. Bucketing and downsampling — `time_bucket`

### 2.1 Tagging each row with its bucket (as a scalar)

```sql
SELECT timestamp, time_bucket(1h, timestamp) AS bucket, latency, value
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

The duration argument is an **unquoted CQL duration literal** (`1h`) — passing a string, as in `time_bucket('1h', ts)`, fails to match the signature. The origin/start/finish arguments are `timestamp`s, so those are quoted.

### 2.2 Fixed-interval downsampling with `GROUP BY`

```sql
-- Hourly avg / min / max / count (collection latency)
SELECT time_bucket(1h, timestamp) AS bucket,
       avg(latency), min(latency), max(latency), count(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- The reading itself (numeric tags)
SELECT time_bucket(1h, timestamp) AS bucket, avg(value_numeric)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- Other intervals: 5 minutes, 1 day
SELECT time_bucket(5m, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(5m, timestamp);

SELECT time_bucket(1d, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1d, timestamp);
```

`time_bucket` has to be the **last element** of `GROUP BY` (after the partition key columns) for the grouping to be pushed into the read path. It works unchanged on a `DESC` table; the buckets simply come back newest first. Upstream's rule that `avg` of an `int` column truncates to `int` applies here too.

### 2.3 Buckets with a shifted origin

```sql
-- Hourly buckets offset by 30 minutes: [08:30, 09:30), [09:30, 10:30), ...
SELECT time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000') AS bucket, avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000');
```

## 3. Filling gaps — `time_bucket_gapfill`

Plain `time_bucket` returns **only the buckets that have data**. `time_bucket_gapfill` produces a row for **every** bucket in `[start, finish)`, so a dashboard gets an unbroken time axis. Aggregates in an empty bucket are null by default.

> **⚠️ On a `DESC` table, `ORDER BY timestamp ASC` is mandatory.** Gap-fill's densify assumes buckets arrive in **ascending** order, and nothing enforces it. Run it without the sort on a `DESC`-clustered table and rows arrive newest-first, the fill is applied backwards, and **no error is raised**. Adding `ORDER BY timestamp ASC` cancels the `DESC` declaration against the `ASC` request so the read itself is ascending, which is what densify wants. This combination is not yet covered by a test — see [gapfill-design.md §4](doc/timeseries/gapfill-design.md).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`WHERE timestamp >= <start>` is not decoration — if a single scanned row is **older** than the gap-fill `start`, the query fails with *"The floor function starting time is greater than the provided time"*.

### 3.1 `locf()` — carry the previous value forward

Wrap an aggregate in `locf(...)` and an empty bucket inherits **the value of the previous non-empty bucket** instead of null (last observation carried forward).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       locf(avg(latency))   -- an empty bucket repeats the previous hour's average
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`locf` has no effect on rows that do have data (it returns its argument unchanged). Buckets before the first real value have nothing to inherit and stay null.

### 3.2 `interpolate()` — linear interpolation

To fill empty buckets by interpolating linearly between neighbours, use `interpolate(...)` (the result type is `double`).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       interpolate(avg(value_numeric))   -- empty buckets follow a straight line between the values either side
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

Empty buckets before the first real value or after the last one have nothing to interpolate between and stay null.

### 3.3 Multiple tags

Each tag is filled independently. Include the partition key in both `SELECT` and `GROUP BY`.

```sql
SELECT tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point WHERE tag_id IN ('TAG-001', 'TAG-002')
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`IN` plus `ORDER BY` applies a post-sort **across** partitions, so results come back in global bucket order rather than grouped by tag. With this schema, iterating one tag at a time is easier to work with.

### 3.4 Limitations

- The width must be a **fixed length** (no month components).
- **Do not alias** the bucket column or the `locf(...)`/`interpolate(...)` expressions — they are located by function name in the result metadata, so an alias makes gap-fill silently do nothing.
- The aggregate being filled must be over a **numeric column** — `latency` or `value_numeric`, not `value` (text).
- Avoid paging across a bucket range.
- A query is rejected if range ÷ width exceeds **1,000,000 buckets**.

## 4. First and last values — `first`, `last`

`first`/`last` are type-agnostic, so they work on `value` even though it is `text` — one of the few things you can do with that column server-side.

```sql
-- A tag's first and last reading (returned as text)
SELECT first(value, timestamp) AS first_reading,
       last(value, timestamp)  AS last_reading
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

Hourly OHLC (open/high/low/close) candles — here you must use a **numeric column**. `max`/`min` on `text` compares lexicographically:

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

`first`/`last` order by **the timestamp argument, not insertion order**, so open and close are correct even with out-of-order writes and on a `DESC` clustering.

## 5. Change — `delta`, `rate`, `derivative`

```sql
SELECT time_bucket(1h, timestamp) AS bucket,
       delta(value_numeric, timestamp)      AS change,
       rate(value_numeric, timestamp)       AS per_second,
       derivative(value_numeric, timestamp) AS slope_per_second
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

- `delta` = last sample in the bucket − first sample
- `rate` = `delta` ÷ elapsed seconds (endpoint-to-endpoint rate of change)
- `derivative` = least-squares regression slope. It uses every point, so it differs from `rate` when the series is non-linear.

The second argument must be a `timestamp` or a `bigint` (epoch millis) — `int` and `timeuuid` time columns are rejected. For a non-numeric tag, use `latency` instead of `value_numeric`.

### 5.1 Reset-corrected throughput — `counter_rate`

`counter_delta`/`counter_rate` only need a **numeric column**; they do not require the CQL `counter` type. A monotonically increasing `int`/`bigint` gauge is enough, and that is the shape the tests cover. Use these instead of `rate()` wherever a reset is possible — `rate()` mistakes a reset for a large negative step.

```sql
CREATE TABLE tag_counters (
    tag_id text, timestamp timestamp, total bigint,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Events per second, per minute (reset-corrected)
SELECT time_bucket(1m, timestamp) AS minute, counter_rate(total, timestamp) AS per_sec
FROM   tag_counters
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1m, timestamp);
```

`tm_tag_point` has no monotonic column, hence the separate table. Note too that a table using the CQL `counter` **type** **cannot be tiered at all** — the re-encoder deletes and re-inserts rows, and a deleted counter can never be written again. A `bigint` gauge is the tiering-compatible choice.

## 6. Percentiles and SLOs — `percentile`

```sql
-- p50 / p95 / p99 latency per minute
SELECT time_bucket(1m, ts) AS minute,
       percentile(latency_ms, 0.50) AS p50,
       percentile(latency_ms, 0.95) AS p95,
       percentile(latency_ms, 0.99) AS p99
FROM   latencies
WHERE  service = 'checkout'
GROUP  BY service, time_bucket(1m, ts);

-- Median over the whole range
SELECT percentile(value, 0.5) AS median FROM metrics WHERE series = 'cpu';
```

`percentile` is an **exact** continuous percentile, interpolating linearly between adjacent values (`q` is 0–1). It holds the group's values in memory, so it suits bounded downsample buckets rather than unbounded scans.

## 7. Distribution, spread and cardinality

```sql
-- Time-weighted average: each value weighted by how long it was in effect.
-- Use this instead of avg() when sample intervals are irregular.
SELECT time_bucket(1h, ts) AS bucket, time_weighted_average(value, ts) AS twa
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- Area under the curve (value·seconds). E.g. power (W) -> energy (J)
SELECT time_bucket(1h, ts) AS bucket, integral(value, ts) AS area
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- Spread per bucket
SELECT time_bucket(1h, ts) AS bucket, variance(value) AS var, stddev(value) AS sd
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- [0, 1000) ms split into 10 equal buckets.
-- Result list: [ <0ms, bucket1, .. bucket10, >=1000ms ]
SELECT histogram(latency_ms, 0, 1000, 10) AS dist
FROM   latencies WHERE service = 'checkout';

-- Approximate distinct client IPs per minute (HyperLogLog, fixed memory)
SELECT time_bucket(1m, ts) AS minute, approx_count_distinct(client_ip) AS unique_ips
FROM   requests WHERE service = 'api' GROUP BY service, time_bucket(1m, ts);
```

## 8. Bivariate statistics and regression

Compute the relationship between two columns on the server. Argument order is `(y, x)` — y is the dependent variable.

```sql
-- Correlation between temperature and power draw, hourly
SELECT time_bucket(1h, ts)          AS bucket,
       corr(power, temperature)     AS r,
       covar_samp(power, temperature) AS cov,
       regr_slope(power, temperature) AS slope,      -- power increase per degree
       regr_intercept(power, temperature) AS intercept,
       regr_r2(power, temperature)  AS r_squared
FROM   sensors
WHERE  site = 'plant1'
GROUP  BY site, time_bucket(1h, ts);
```

## 9. A complete dashboard query

Bucketing, OHLC, change and percentiles at once:

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

## 10. Full-text search — SAI `LIKE` with `index_analyzer`

Search log and event message bodies inside the usual time-series query pattern. The `ngram` analyzer provides true substring matching — mid-word fragments, fragments spanning a space, and Korean all work. Detail: [fulltext-search.md](doc/timeseries/fulltext-search.md)

```sql
CREATE TABLE logs (
    device text, ts timestamp, msg text,
    PRIMARY KEY (device, ts)
) WITH CLUSTERING ORDER BY (ts ASC);

CREATE INDEX logs_msg_idx ON logs(msg) USING 'sai'
  WITH OPTIONS = { 'index_analyzer': 'ngram' };

-- Body search within one device and one hour (no ALLOW FILTERING)
SELECT ts, msg FROM logs
 WHERE device = 'pump-01'
   AND ts >= '2026-07-31 00:00' AND ts < '2026-07-31 01:00'
   AND msg LIKE '%timeout%';

-- Mid-word fragments match too: '%imeou%' finds "timeout"
-- Prefix / suffix / exact: 'connection%', '%9042', LIKE 'connection refused'
-- AND across fragments: msg LIKE '%connection%' AND msg LIKE '%refused%'

-- Combined with time-series functions: error count per 5-minute bucket
SELECT time_bucket(5m, ts), count(*) FROM logs
 WHERE device='pump-01' AND ts >= ? AND ts < ? AND msg LIKE '%timeout%'
 GROUP BY device, time_bucket(5m, ts);
```

How it works: the whole value is indexed as 2–3 character n-grams (recall) → candidates come from the gram intersection → **the LIKE pattern is re-applied to the raw value** (precision). The index is several times the size of the source column, so apply it selectively to log-shaped tables. Fragments shorter than two characters are rejected with an explicit error. `=` keeps its exact-match meaning.

## 11. Time-series compaction (TSCS) configuration

A dedicated strategy combining TWCS's time ordering and whole-window drops with UCS's in-window compaction. Set it at `CREATE TABLE` or `ALTER`:

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d',           -- window width (MUST match the tiering chunk_window)
  'freeze_after': '2h',          -- freeze this long after the window closes (converges to 1 SSTable/window)
  'scaling_parameters': 'T4',    -- inside the current window, delegate to UCS (UCS syntax verbatim)
  'target_sstable_size': '256MiB',
  'retention': '62d',            -- optional: drop whole, without compacting, once the upper bound passes now-62d
  'max_future_window': '1d'      -- optional: future-timestamp guard (default 1d)
};
```

- A closed window automatically freezes to **one SSTable**, minimising read amplification, and TTL data already expired at that moment is reclaimed without retention. Reclaiming data that expires *after* the freeze is `retention`'s job.
- Late (backfill) data is separated at window boundaries during flush and streaming and **lands locally in its own window** — it does not pollute the current window's compaction.

### What to watch when setting these

| Rule | What happens if you get it wrong |
| --- | --- |
| `retention` must be **at least `window_size + freeze_after`** | Rejected at `ALTER` time with an explicit error. The floor exists so a window cannot be dropped while it is still being written to and frozen |
| **A frozen window never freezes again** | Once a window is down to one SSTable it stops being a freeze candidate, so data that expires *after* that point is never reclaimed by the freeze. If rows expire on a TTL longer than `freeze_after`, you need `retention` — `default_time_to_live` alone will leave them on disk |
| Real retention is **`retention` + `window_size`** | A window is dropped when `windowStart <= now - retention - window_size`, so the newest row in it survives roughly one extra window beyond the number you wrote. Budget for it rather than being surprised by it |
| `window_size` should equal the tiering **`chunk_window`** | Not enforced, but a mismatch makes one chunk span two compaction windows, so a window can freeze while part of its data is already chunked. Keep them the same number |
| `timestamp_resolution` must match your writers | It decides how a `USING TIMESTAMP` value is read as a wall-clock time. Set it to `MICROSECONDS` (the default) unless your writers really use milliseconds — get it wrong and every row is classified into a window 1000× away, which looks like data vanishing into the far future |
| `max_future_window` (default `1d`) is a guard, not a filter | Rows whose timestamp is beyond it are **parked**: excluded from compaction, freeze, retention and tiering, and they accumulate one SSTable per flush for as long as the bad clock persists. The table's `FarFutureTimeSeriesSSTables` MBean attribute should be empty; if it is not, fix the writer, then rewrite them with a user-defined or maximal compaction |

Choosing `window_size`: too small and a long retention means thousands of SSTables; too large and freeze and retention both become coarse (you cannot drop less than one window). One day per window with 62 days of retention is 62 SSTables per table, which is comfortable.


## 12. Tiered storage configuration

Compresses old windows into column-oriented chunks (all regular columns onto the window's single timestamp axis), moves them to `<table>__chunks`, and leaves **`SELECT` unchanged** — transparent reads merge hot and cold automatically. The policy goes into the table's `extensions` as a JSON string.

### 12.1 Supported schemas

**Any time-series table with a single time axis (one `timestamp` clustering column) works, whatever its shape.** The partition key may be composite, regular columns are unrestricted in count and type, and any number of static columns is preserved as-is (static cells are not chunked, and the re-encoder's clustering range delete does not touch them).

Below is the industrial table the release gate actually verifies tiering against — 7 statics and 8 regular columns, `DESC` clustering:

```sql
CREATE TABLE pp.tm_tag_point (
    tag_id     text,                              -- partition key: any number of columns (composite allowed)
    timestamp  timestamp,                         -- exactly one clustering column, timestamp (ASC or DESC)
    area_id    text static, asset_id text static, line_id text static,
    opc_id     text static, site_id  text static, tag_name text static,
    type       text static,                       -- statics: any count and type, preserved after tiering
    attribute  frozen<map<text,text>>,            -- always {} -> CONSTANT (0 bytes)
    error_code int,                               -- always 0 -> CONSTANT
    latency    int,                               -- high entropy -> zigzag varint delta
    quality    int,                               -- always 192 -> CONSTANT
    value      text,                              -- string copy of the reading
    value_boolean boolean,                        -- populated only for type=boolean tags
    value_numeric double,                         -- populated only when type is numeric
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**`DESC` clustering is the industrial default idiom** — reading newest-first dominates. If transparent reads' bound arithmetic assumed ascending order it would return zero cold rows **with no error**, so both bound directions and both orderings are pinned in the integration test.

Whether a reading lands in `value_boolean` or `value_numeric` is decided by the **static `type`**. Being static, it is fixed per tag, and one chunk = one tag × one window, so within a chunk each value column is either entirely populated or entirely empty — the used one gets its dedicated codec, the unused one becomes ALL_NULL at zero bytes. `value` (text) is a string copy of the reading (`value_numeric = 20.76` ↔ `value = '20.76'`).

Apply a policy to an unsupported shape and it is skipped, with an ERROR log **stating the reason** every 60 seconds. There are only five grounds for refusal:

| Shape | Reason |
| --- | --- |
| A `counter` column | The re-encoder deletes and re-inserts rows, and a deleted counter can never be written again |
| A non-frozen collection as a **regular** column | Multi-cell values cannot be encoded into a chunk — wrap in `frozen<...>` and it is supported |
| Zero or two-plus clustering columns, or one that is not a `timestamp` | There is no time axis to encode |
| A secondary index (SAI included) on a **non-static column** | Index entries are per row, so when a re-encoded row disappears the index query silently misses cold data — this covers regular columns, clustering columns and columns of a composite partition key. **Only indexes on static columns** are fine |
| A materialized view over this table | Transparent reads restore only the base table, so the view would permanently lose old history |

> If `default_time_to_live` is set and `hot_window >= TTL`, the TTL erases data before the re-encoder ever sees it and **nothing is ever compressed**. This is not refused, but it logs a WARN naming both values.

> **⚠️ Two things to know before enabling tiering** (detail: [tiered-storage.md §1.1, §5.1.2](doc/timeseries/tiered-storage.md))
> 1. **Cold data is immutable** — a `DELETE` past the hot window (cell, row, range or partition) and `SET col = null` are **rejected**. The chunk is the only copy, so a tombstone would mask it only until `gc_grace`, after which the data would come back. Use `cold_window` expiry to remove cold data. Deletes inside the hot window, and a late `UPDATE` that **writes a value** into the cold range, both work normally.
> 2. **TTL is dropped when data is chunked** — data you expected `default_time_to_live` to expire is kept forever once it moves into a chunk. If you relied on TTL, set the same period as `cold_window`.

The re-encoder puts **every regular column** into one chunk: the window's timestamp axis is stored once, and each column goes into its own section as **its serialized bytes verbatim**, so `double`/`boolean`/`int`/`bigint`/`timestamp`/`date`/`text` take a dedicated codec and everything else (`blob`, `uuid`, `timeuuid`, frozen collections, …) takes the opaque-bytes fallback. `null` cells round-trip as `null`, and a column whose values are all identical folds to O(1), zero bytes. Full table: [tiered-storage.md §3.1.1](doc/timeseries/tiered-storage.md).

### 12.2 Turning it on — one line of CQL

Put the policy JSON straight into the table's `extensions`; no hex conversion needed:

```sql
ALTER TABLE pp.tm_tag_point WITH extensions = {
  'timeseries_tiering': '{"hot_window":"2d","chunk_window":"1d","cold_window":"3650d","interval":"1h"}'
};

-- Confirm it applied (the policy and run statistics show together)
SELECT * FROM system_views.timeseries_tiering;
```

> `extensions` is a blob map in the schema, but this fork **stores a plain string as its UTF-8 bytes**. Only a value beginning with `0x` is read as a hex blob, so existing hex notation (`0x7b22...`) still works.

After that the 60-second sweeper compresses on the `interval` schedule. **To see it immediately**, run one cycle by hand:

```bash
nodetool retier pp tm_tag_point   # one re-encode cycle (synchronous; the chunk table is created here)
nodetool tieringstatus            # per-table policy, last run, cumulative statistics
```

```sql
-- Check the result: chunks exist, and SELECT returns what it did before (transparent reads)
SELECT count(*) FROM pp.tm_tag_point__chunks WHERE tag_id='TAG-001';
SELECT count(*) FROM pp.tm_tag_point
 WHERE tag_id='TAG-001' AND timestamp >= '2026-07-01 00:00:00+0000'
                        AND timestamp <  '2026-07-02 00:00:00+0000';   -- same value as before

-- Statics are not chunked, so they stay on the base table after tiering
SELECT site_id, tag_name, type FROM pp.tm_tag_point WHERE tag_id='TAG-001' LIMIT 1;
```

> When counting to confirm tiering, **always include a clustering range.** An unrestricted `count(*)` on a partition with no clustering rows left still returns `1`, for the static-only row. And a full-scan `SELECT count(*)` with no partition key does not merge at all (a range scan sees only hot rows), so it returns **0** on a fully-chunked table.

### 12.3 Policy fields

| Field | Meaning |
| --- | --- |
| `hot_window` | **Data within this period is left alone** (kept as rows). Set it comfortably wider than the range that sees frequent live reads and edits (e.g. `7d`) |
| `chunk_window` | The time width one chunk holds (max `31d`). Match the TSCS `window_size`. For 1-second data, `1h` (3,600 samples) works well |
| `cold_window` | Optional — chunks past this age are dropped whole (the retention policy). Unset (`-1`) keeps them forever |
| `interval` | Background re-encode period (e.g. `1h`). The 60-second sweeper processes only tables whose interval has come due |
| `consistency` | The re-encoder's CL — only `LOCAL_QUORUM` (default), `QUORUM`, `EACH_QUORUM` and `ALL` are accepted (weaker levels risk data loss and are blocked) |

**There is no codec to choose**: ALP/ALP-RD is the only chunk codec for `double` (the old `codec` option was removed, and an `ALTER TABLE` that still sets it is rejected). Near-constant columns are handled by the column-oriented chunk's CONSTANT flag in O(1), before any codec runs — see [the measurements](doc/timeseries/codec-bakeoff.md).

### What to watch when setting these

Three of these are checked at `ALTER` time and simply fail. The rest are legal settings that quietly do something you did not want.

| Rule | What happens if you get it wrong |
| --- | --- |
| `hot_window` **≥** `chunk_window` | Rejected. A hot window narrower than a chunk would mean encoding a window that is still being written |
| `cold_window` **>** `hot_window` | Rejected. Data would have to expire before it was allowed to be encoded |
| `chunk_window` **≤ 31d** | Rejected. The re-encoder reads one window into memory at a time |
| Leave real slack between `hot_window` and `chunk_window` | **This is the setting most worth thinking about.** Writes that would tombstone cold data are refused below `max(chunk coverage, now - hot_window)`, and a window becomes encodable as soon as it closes. With the two equal — `1h`/`1h` — a row is eligible for encoding almost the moment its window closes, so there is effectively no period in which it can still be deleted. If the table ever needs `DELETE` or `SET col = null`, make `hot_window` several times `chunk_window` (e.g. `chunk_window 1h` with `hot_window 6h`). The 1h/1h in the tests is a maximum-load case, not a recommendation |
| `cold_window` is the **only** retention for chunked data | Cell TTLs are dropped when a window is chunked, so a table relying on `default_time_to_live` silently switches from bounded retention to unbounded growth the first time the re-encoder runs. Set `cold_window` to the same period you expected the TTL to enforce — and note this cannot be undone once the base rows are gone |
| `hot_window` must be **shorter** than any `default_time_to_live` | Legal, but if the TTL erases rows before the re-encoder is allowed to touch them, nothing is ever compressed. Tiering appears to be on and does nothing. It logs a WARN naming both values |
| `consistency` accepts only quorum strength | `QUORUM`, `LOCAL_QUORUM` (default), `EACH_QUORUM`, `ALL`. Anything weaker is rejected: the re-encoder deletes the base rows, so a write it believed succeeded but that did not reach a quorum would lose data outright |
| Removing the policy does **not** un-tier | Transparent reads decide from actual chunk coverage, not from the current policy. Clearing `extensions` only stops new encoding — existing chunks stay readable and `DELETE` in that range stays refused. The only ways to remove cold data are `cold_window` expiry or dropping the chunk table |

Choosing `chunk_window`: one chunk holds one tag × one window, so at 1-second data `1h` is 3,600 samples — a good size. Much smaller and per-chunk header overhead dominates; much larger and each merged read decodes more than it needs.


### 12.4 Changing or turning it off

```sql
-- Change the policy: put new JSON in the same way; it applies from the next cycle
-- Turn it off entirely: remove the key (chunks already written stay)
ALTER TABLE pp.tm_tag_point WITH extensions = {};
```

**Removing the policy does not hide data already in chunks.** Transparent reads decide whether to merge from **actual chunk coverage**, not from the current policy, so removing the extension only *stops new encoding*. For the same reason, `DELETE` in that range stays rejected (cold immutability). Widening `hot_window` or narrowing `chunk_window` likewise hides nothing. The only ways to actually remove cold data are `cold_window` expiry or `DROP`ping the chunk table.

Operational note: late data arriving into an already-chunked window is merged automatically on the next cycle (at the same timestamp, the later arrival wins). Detail and limitations (range scans, paging): [tiered-storage.md](doc/timeseries/tiered-storage.md) · measurements: [the benchmark](doc/timeseries/tiering-benchmark.md)

## 13. Operational notes

- **Always specify a partition** (`WHERE series = ...`) and a time range with it. A time-series scan is cheapest inside a single partition ordered by `ts`.
- **Bound your partition size.** For a high-frequency series, put a coarse time bucket in the partition key so partitions cannot grow without limit: `PRIMARY KEY ((series, day), ts)`.
- **Use TSCS compaction** — window ordering, freeze and whole-window drops are automated for time series, and the current window is delegated to UCS (`scaling_parameters: 'T4'`). Express retention with `retention` (whole-window drop) or `default_time_to_live`.
- `time_bucket(interval, ts)` must be the last element of `GROUP BY` (after the partition key columns) for the grouping to be pushed down into the read path.

---

## Building

Requirements: **Java 21**, Ant 1.10 or later (with ant-junit for tests). `modules/accord` is a git submodule, so `git submodule update --init` is needed.

```bash
.build/sh/ai-build     # clean + jar + checkstyle -> build/apache-cassandra-6.0.0.jar
```

The build artifact is always `apache-cassandra-6.0.0.jar` (`base.version` is pinned to 6.0.0).

> On a machine without `ant` on PATH, `ai-build` reports `BUILD SUCCESSFUL` without building anything — its log summarizer prints that on empty input. Build in the CI container instead: `.build/sh/ai-build-image` then `.build/sh/ai-in-container '<command>'`, or `.build/sh/ci-local` for the whole gate. Verify by the timestamp on the jar, not by the log.

## Verification

`.build/sh/ci-local` walks the release gate's stages locally in the same order: jar and checkstyle → the fork's test classes → the docker image → the integration test.

```bash
.build/sh/ci-local                  # add --with-cluster for the three-node test
.build/sh/ci-local --stage image    # one stage: jar | tests | image | integration | cluster
```

### Integration test (the release gate)

Unit tests exercise the functions in-process; [docker/integration-test.sh](docker/integration-test.sh) **boots a real image** and checks time-series CQL results against hand-computed values all the way through schema creation, the read path, aggregation and the native protocol — 93 assertions, including a process restart.

```bash
docker build -t cassandra-timeseries:6.0.0 -f docker/Dockerfile .
./docker/integration-test.sh cassandra-timeseries:6.0.0     # CONTAINER_RUNTIME=podman also works
```

It prints every assertion with the CQL it ran and the rows it got back, and writes a report to `build/timeseries-it-report.html` (and the same content as `.md`). **Example output: [integration test report](doc/timeseries/integration-test-report.md).**

In CI, pushing a tag runs `docker-image → docker-integration-test → docker-image-publish + release` in order, and **only if this test passes** do the image publish and release proceed. On the default branch it is manual, because a full image build is expensive.

### Cluster test (three nodes)

[docker/cluster-test.sh](docker/cluster-test.sh) runs three real containers on a docker network at RF=3 — 49 assertions covering what a single node cannot reach: aggregation and gap-fill through every coordinator, TSCS freeze converging on each replica independently, a real repair stream between operating-system processes, and QUORUM behaviour with a replica actually stopped.

```bash
./docker/cluster-test.sh cassandra-timeseries:6.0.0
```

It is manual in CI (three 2G JVMs may not fit a shared runner), so run it by hand before any release that touches compaction, streaming, repair or tiering.

### Performance regression gate

[.build/sh/ci-perf](.build/sh/ci-perf) runs the chunk codec and cursor JMH classes and compares them against a recorded baseline, failing on a regression beyond the threshold.

```bash
.build/sh/ci-perf                   # compare against doc/timeseries/perf-baseline.json
.build/sh/ci-perf --record          # deliberately move the line
```

It takes the minimum of three sweeps (one sweep is not a measurement on a shared machine) and refuses to fail a build against a baseline recorded on a different host.

### Scale test (100 million rows)

[docker/scale-test.sh](docker/scale-test.sh) loads bulk data into a containerised node and measures the **CQL execution time** of each time-series query. Both load and queries run inside the container through cqlsh's bundled Python driver (see [docker/scale-workload.py](docker/scale-workload.py)), so cqlsh startup time does not pollute the measurements.

```bash
SCALE_ROWS=100000000 SCALE_SERIES=1000 SCALE_LOADERS=16 SCALE_HEAP=16G \
  ./docker/scale-test.sh cassandra-timeseries:6.0.0
# Reuse loaded data and re-measure queries only: SCALE_SKIP_LOAD=1
```

You can compare GCs — `SCALE_GC=g1` (default is `zgc`; generational ZGC is already enabled in `conf/jvm21-server.options`), `SCALE_PASSES=2` (measure after a warm-up), `SCALE_WBENCH_ROWS=10000000` (write benchmark). Feed two runs to `docker/gc-compare.py <prefix-a> <prefix-b>` for a comparison table → **[GC comparison](doc/timeseries/gc-comparison.md)**.

Results land in `build/timeseries-scale-report.html` (and `.md`). Capacity record: **[scale test report](doc/timeseries/scale-test-report.md)**. Current reference figures are in the [tiering benchmark](doc/timeseries/tiering-benchmark.md).

Note: aggregating millions of rows requires raising server timeouts. Besides `read/range_request_timeout`, **`native_transport_timeout` (default 12 s)** cuts the whole request, so it has to be raised too — and that key is absent from the default `cassandra.yaml`, so it must be added. The script does this for you.

### Throughput (ops/s) benchmark

Where the scale test measures the execution time of one analytical query, throughput is measured separately: for writes, the load rate of `scale-workload.py load` (rows/s) is the measurement; for reads, [docker/rwbench-read.py](docker/rwbench-read.py) (three production-shaped patterns — latest value per tag, single row, 100-row time window) and the bundled `cassandra-stress` (to find the server's ceiling). **Results: [read/write throughput benchmark](doc/timeseries/rw-throughput-benchmark.md)** — ingest 233k rows/s (host 234, 100-row batches), write path 424k rows/s and chunk encoding 684k rows/s (host 237, JMH); per-pattern read ops/s awaiting re-measurement on v4. The report carries the full commands to reproduce.

## CI and releases

- Every push builds the jar and runs the time-series test suite (`.gitlab-ci.yml`).
- Jar from the latest master build: *CI/CD → Pipelines → the `build-jar` artifact*.
- Pushing a tag (e.g. `v6.0.0`) publishes a [Release](../../-/releases) with a jar download link.

> **Check what CI is actually telling you.** The project's runners have been offline since 2026-08-07, so pipelines fail at `stuck_pending_no_matching_runners` without starting a job — a red pipeline in that period is not a statement about the code. `glab ci list` and `glab ci get -p <id>` show the reason. Until runners are restored, `.build/sh/ci-local` and `docker/cluster-test.sh` are the verification. See [production-rollout.md §6](doc/timeseries/production-rollout.md).

## Branches and upstream policy

- `master` (= the `6.0.0` branch): the release line. It must be **kept merged** with the latest upstream `cassandra-6.0` branch of apache/cassandra (remote `upstream`).
- Recurring conflict points: `CHANGES.txt`, `debian/changelog`, the `modules/accord` submodule pointer, `cql3/statements/SelectStatement.java` (the gap-fill wiring).

## Development

Build, test and code-style rules are in [CLAUDE.md](CLAUDE.md) and [AGENTS.md](AGENTS.md) — the full test suite takes hours, so run only the targeted tests. Test layout is in [TESTING.md](TESTING.md). Time-series test entry points: `org.apache.cassandra.cql3.functions.TimeSeriesFctsTest`, `org.apache.cassandra.db.aggregation.TimeBucketGapFillerTest`.
