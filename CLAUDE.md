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

**GitHub mirror rule:** every push of `main` (and of release tags) goes to BOTH remotes: `origin`
(GitLab, the primary) and `github` = `git@github-cassandra-timeseries:kopens/cassandra-timeseries.git`
(the public mirror at github.com/kopens/cassandra-timeseries — the ssh alias uses the deploy key
`~/.ssh/id_ed25519_cassandra_timeseries`). The mirror must never be left behind: after pushing
origin, push github in the same breath (`git push github main` / `git push github <tag>`; a moved
release tag is replaced with a delete-then-push). GitHub renders `README.md`, which is the English
README — another reason the mirror stays current.

**Upstream tracking rule:** `main` must be kept merged with the latest `cassandra-6.0` branch of GitHub `apache/cassandra` (remote `upstream`). The step-by-step procedure, including how each recurring conflict is resolved, lives in the `upstream-merge` skill (`.claude/skills/upstream-merge/SKILL.md`) — follow it rather than improvising. Recurring conflict spots: `CHANGES.txt`, `debian/changelog`, `conf/cassandra.yaml` + `conf/cassandra_latest.yaml` (the fork's `prepared_statements_require_parameters_*` guardrail block sits where upstream appends new guardrails), `cql3/statements/ModificationStatement.java` (the fork's `validatePrepare` override abuts upstream's disk-usage code), `.build/sh/ant-log-summary.py` (add/add — upstream shipped its own copy in `c574262b10`), `README.asc` (deleted in this fork so
GitHub renders `README.md`, which is the English README — `README.ko.md` is the Korean one and the
two must be kept in step; resolve any modify/delete conflict on `README.asc` by deleting it again), the
`modules/accord` submodule pointer, `SelectStatement.java` (gap-fill wiring), `db/compaction/TimeSeries*.java` + `FreezeCompactionTask.java` + `db/compaction/timeseries/` (UCS delegation, freeze hook), and the tiered-storage touchpoints: `cql3/statements/schema/TableAttributes.java` (extensions settable via CQL blob literals), `service/CassandraDaemon.java` (TieredStorageService.setup() wiring), `db/virtual/SystemViewsKeyspace.java` (timeseries_tiering virtual table), `tools/nodetool/NodetoolCommand.java` (retier/tieringstatus registration), `tools/NodeProbe.java` (TieredStorage JMX proxy), `service/reads/DataResolver.java` + `DigestResolver.java` (the transparent-read merge hooks — note the hook sits on `resolveInternal`, the single funnel every classic read path passes through), and `db/ColumnFamilyStore.java` + `ColumnFamilyStoreMBean.java` (the `ParkedTimeSeriesWindows` / `FarFutureTimeSeriesSSTables` accessors, aggregated by `CompactionStrategyManager`). Resolve by keeping both upstream fixes and the time-series features, then build.

## Workflow, build, test, and style

See **[AGENTS.md](AGENTS.md)** — it is the source of truth for environment, build (`.build/sh/ai-build`), targeted testing (`.build/sh/ai-ci-test <FQCN>`), code style, the git workflow, and hard boundaries (never touch `src/gen-java/`, `lib/`, or the CQL grammar without asking). Do not duplicate or contradict it. The notes below cover only what AGENTS.md does not: the big-picture architecture.

Key reminders that bite agents:
- `ai-ci-test` runs a whole test **class**; there is no method-level filter. Never run the full suite.
- Always use the `.build/sh/ai-*` wrappers, never bare `ant` — they summarize logs and set the working directory.
- `modules/accord` is a git submodule (the Accord transaction engine, developed at `apache/cassandra-accord`).

## Verification constraints — read before claiming anything passed

The traps below are specific to this fork and this machine, and every one of them has produced a
convincing false green. Full procedure in `.claude/skills/docker-integration-test`; operational
constraints in [doc/timeseries/production-rollout.md](doc/timeseries/production-rollout.md) §6.

- **`ant` is not installed on this host, and `.build/sh/ai-build` reports `BUILD SUCCESSFUL` anyway**
  — `ant-log-summary.py` prints that on empty input and exits 0. `ai-ci-test` pipes through the same
  summarizer. Build and test in the container: `.build/sh/ai-build-image`, then
  `.build/sh/ai-in-container '<cmd>'`, or `.build/sh/ci-local` for the whole gate. Both pass
  `--network host`, without which `apt-get` cannot resolve `archive.ubuntu.com` here.
- **GitLab CI runs again — read the failure before dismissing it.** The "every runner is stale,
  pipelines fail at `stuck_pending_no_matching_runners`" era ended: project runner 77
  (kopens-234) has been online since 2026-08-26 and jobs really execute (verified 2026-09-05).
  So a red pipeline is now a statement about the code — pipeline #53500's `timeseries-tests`
  failure was a real one, a test left behind by 417e5d2336. Check with
  `glab api projects/common%2Fcassandra-timeseries/runners` and read the job trace
  (`glab api projects/common%2Fcassandra-timeseries/jobs/<id>/trace`; `glab ci get -p <id>`
  lists the jobs). `.build/sh/ci-local` is still the fuller gate — say which runs you actually did.
- **Test runs must be serial.** `ci-test` starts with a `realclean`, so a second concurrent run
  deletes the first's `build/lib/jars` and the victim fails with hundreds of `package org.slf4j does
  not exist` errors that look like a broken change.
- **Wall-clock assertions must be pinned to the worst case, not to "now."** A write-guard bug reached
  two green integration runs because the assertion only reproduced it in the first minutes of an
  hour. Anything depending on where `now` sits inside a window needs the boundary chosen explicitly.
- **One JMH sweep is not a measurement on this host.** Consecutive sweeps of the same commit moved a
  benchmark 545 / 433 / 393 µs. `.build/sh/ci-perf` takes the minimum of three and refuses to gate
  against a baseline from a different machine; quote its numbers, not a single run's.
- **The release gate verifies a docker image; production deploys a jar** into an existing install
  with its own `conf/`, `jvm*.options` and start scripts. A green integration run says the code works
  in the image's environment, not on node 41.

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
