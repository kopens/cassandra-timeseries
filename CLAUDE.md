# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

A fork of **Apache Cassandra 6.0.0** that adds native time-series CQL functions to build a distributed time-series database. Everything from upstream Cassandra applies; the fork-specific delta is:

- [cql3/functions/TimeSeriesFcts.java](src/java/org/apache/cassandra/cql3/functions/TimeSeriesFcts.java) — the time-series scalar/aggregate functions: `time_bucket`, `first`, `last`, `delta`, `rate`, `derivative`, `percentile`, `time_weighted_average`, `variance`, `stddev`, `histogram`, `approx_count_distinct`, `counter_delta`, `counter_rate`, `corr`, `covar_pop`, `covar_samp`, `regr_slope`, `regr_intercept`, `regr_r2`, `integral`.
- Gap-fill: `GROUP BY time_bucket_gapfill(width, ts, start, finish)` with `locf()`/`interpolate()` fill policies. Core densify logic in [db/aggregation/TimeBucketGapFiller.java](src/java/org/apache/cassandra/db/aggregation/TimeBucketGapFiller.java) (operates on the `ResultSet` row representation, `List<List<byte[]>>`); wired into the query path via `GapFillSpec` in [cql3/statements/SelectStatement.java](src/java/org/apache/cassandra/cql3/statements/SelectStatement.java).
- Design docs and CQL examples in [doc/timeseries/](doc/timeseries/) (functions, gap-fill, continuous-aggregates designs).
- Tests: `org.apache.cassandra.cql3.functions.TimeSeriesFctsTest` and `org.apache.cassandra.db.aggregation.TimeBucketGapFillerTest` (both run in CI, see [.gitlab-ci.yml](.gitlab-ci.yml)).

**Versioning rule:** the build must produce `build/apache-cassandra-6.0.0.jar` — keep `base.version` in [build.xml](build.xml) at `6.0.0` (do not let upstream merges reset it to alpha/beta/snapshot versions). Build with **Java 21** (CI image `eclipse-temurin:21-jdk`; despite AGENTS.md's upstream text naming Java 11 as default). Releases: pushing a tag (e.g. `v6.0.0`) publishes a GitLab Release with the jar; branch `6.0.0` mirrors `main`.

**GitLab rule:** this project lives on GitLab at `dev.kopens.io/common/cassandra-timeseries` (remote `origin`). Do every GitLab-side operation through the **`glab` CLI**, which is installed and already authenticated here (`glab auth status` → `dev.kopens.io` as `lsb`) — pipelines (`glab ci list`, `glab ci status`, `glab ci trace`), merge requests (`glab mr ...`), issues (`glab issue ...`), releases (`glab release ...`), and repo/branch queries (`glab repo ...`, `glab api ...`). Do not reach for the web UI, raw `curl` against `/api/v4`, or a hand-rolled token: `glab` already holds the credentials, so anything else means re-authenticating and usually means a token in a command line. Plain `git push`/`git fetch` against `origin` stays plain git — the rule is about GitLab's own surfaces, not the git protocol.

**Upstream tracking rule:** `main` must be kept merged with the latest `cassandra-6.0` branch of GitHub `apache/cassandra` (remote `upstream`). Recurring conflict spots: `CHANGES.txt`, `debian/changelog`, `README.asc` (deleted in this fork so
GitHub renders `README.md` — resolve any modify/delete conflict by deleting it again), the
`modules/accord` submodule pointer, `SelectStatement.java` (gap-fill wiring), `db/compaction/TimeSeries*.java` + `FreezeCompactionTask.java` + `db/compaction/timeseries/` (UCS delegation, freeze hook), and the tiered-storage touchpoints: `cql3/statements/schema/TableAttributes.java` (extensions settable via CQL blob literals), `service/CassandraDaemon.java` (TieredStorageService.setup() wiring), `db/virtual/SystemViewsKeyspace.java` (timeseries_tiering virtual table), `tools/nodetool/NodetoolCommand.java` (retier/tieringstatus registration), `tools/NodeProbe.java` (TieredStorage JMX proxy), `service/reads/DataResolver.java` + `DigestResolver.java` (the transparent-read merge hooks — note the hook sits on `resolveInternal`, the single funnel every classic read path passes through), and `db/ColumnFamilyStore.java` + `ColumnFamilyStoreMBean.java` (the `ParkedTimeSeriesWindows` / `FarFutureTimeSeriesSSTables` accessors, aggregated by `CompactionStrategyManager`). Resolve by keeping both upstream fixes and the time-series features, then build.

## Workflow, build, test, and style

See **[AGENTS.md](AGENTS.md)** — it is the source of truth for environment, build (`.build/sh/ai-build`), targeted testing (`.build/sh/ai-ci-test <FQCN>`), code style, the git workflow, and hard boundaries (never touch `src/gen-java/`, `lib/`, or the CQL grammar without asking). Do not duplicate or contradict it. The notes below cover only what AGENTS.md does not: the big-picture architecture.

Key reminders that bite agents:
- `ai-ci-test` runs a whole test **class**; there is no method-level filter. Never run the full suite.
- Always use the `.build/sh/ai-*` wrappers, never bare `ant` — they summarize logs and set the working directory.
- `modules/accord` is a git submodule (the Accord transaction engine, developed at `apache/cassandra-accord`).

## Architecture: the write and read paths

Cassandra is a leaderless, masterless distributed row store. Every node is a peer; any node can coordinate any request. The two most important flows to understand:

**Coordinator side** — [service/StorageProxy.java](src/java/org/apache/cassandra/service/StorageProxy.java) is the heart of distributed reads and writes. It uses the replication strategy + token ring ([locator/](src/java/org/apache/cassandra/locator/), [dht/](src/java/org/apache/cassandra/dht/)) to pick replicas, fans out messages over [net/](src/java/org/apache/cassandra/net/) (`MessagingService`), applies the requested `ConsistencyLevel`, and runs read repair. Each message type has a `*VerbHandler` (e.g. [db/MutationVerbHandler.java](src/java/org/apache/cassandra/db/MutationVerbHandler.java), [db/ReadCommandVerbHandler.java](src/java/org/apache/cassandra/db/ReadCommandVerbHandler.java)).

**Replica/storage side** — a write becomes a `Mutation` ([db/Mutation.java](src/java/org/apache/cassandra/db/Mutation.java)) applied to a `Keyspace` → `ColumnFamilyStore` ([db/ColumnFamilyStore.java](src/java/org/apache/cassandra/db/ColumnFamilyStore.java), the in-memory handle for one table). The write hits the commit log ([db/commitlog/](src/java/org/apache/cassandra/db/commitlog/)) for durability and a `Memtable` ([db/memtable/](src/java/org/apache/cassandra/db/memtable/)) in memory. Memtables flush to immutable **SSTables** on disk ([io/sstable/](src/java/org/apache/cassandra/io/sstable/)), which are merged over time by **compaction** ([db/compaction/](src/java/org/apache/cassandra/db/compaction/)). Reads merge the memtable with all relevant SSTables. The LSM data model — partitions, rows, cells, clusterings, tombstones — lives in [db/partitions/](src/java/org/apache/cassandra/db/partitions/), [db/rows/](src/java/org/apache/cassandra/db/rows/), and the many `Clustering*`/`*ReadCommand` classes directly under [db/](src/java/org/apache/cassandra/db/).

## Major subsystems (top-level packages under `src/java/org/apache/cassandra/`)

- **cql3/** — CQL parser, statements, and execution. Grammar is generated from `src/antlr/Cql.g` into `src/gen-java/` (both off-limits to edit). This is where a query string becomes a `ReadCommand`/`Mutation`.
- **tcm/** — Transactional Cluster Metadata: the newer log-based, linearizable mechanism for cluster/schema/topology changes. Largely replaces the old gossip-driven schema propagation; understand it before touching ring/membership/schema state.
- **gms/** — Gossip, used for liveness/failure detection and legacy state dissemination.
- **schema/** — table/keyspace/type definitions and the schema model.
- **service/** — node lifecycle and orchestration: [service/CassandraDaemon.java](src/java/org/apache/cassandra/service/CassandraDaemon.java) (startup/main), `StorageService` (ring membership, bootstrap/decommission, nodetool backend), `StartupChecks`.
- **transport/** — the native CQL protocol server (client-facing wire protocol).
- **net/** — internode messaging (Netty-based), verbs, and serialization.
- **streaming/** — bulk SSTable transfer between nodes (bootstrap, repair, rebuild).
- **repair/** + **service/ActiveRepairService** — anti-entropy repair (Merkle trees, sync).
- **db/view/** — materialized views; **index/** + **db/index** — secondary indexes (incl. SAI).
- **concurrent/** — the thread/stage execution model (`Stage`, custom executors); Cassandra uses a staged event-driven (SEDA-style) model.
- **config/** — `cassandra.yaml` mapping (`Config`, `DatabaseDescriptor`), the central config access point.
- **hints/**, **batchlog/**, **journal/** — hinted handoff, batched-write durability, and the generic append log.
- **db/marshal/** — `AbstractType` column type system (validation, comparison, serialization).
- **tools/** + **bin/** — `nodetool`, `cqlsh`, `sstable*` offline tools, stress.

## Tests

Test sources live under `test/`, split by kind (this maps to AGENTS.md's testing rules):
- `test/unit/` — JUnit unit/integration tests (the common case for `ai-ci-test`).
- `test/distributed/` — **jvm-dtest**: multi-node clusters in one JVM, the preferred way to test distributed behavior in Java.
- `test/burn/`, `test/long/`, `test/microbench/` (JMH), `test/simulator/` (deterministic simulation), `test/harry/` (property/fuzz). See [TESTING.md](TESTING.md) for what belongs where.
