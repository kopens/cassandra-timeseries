---
name: docker-integration-test
description: Run this fork's container-based end-to-end tests — the single-node CQL release gate (docker/integration-test.sh) and the three-node RF=3 cluster test (docker/cluster-test.sh) — plus the scale and tiering benchmarks. Use this whenever the user asks to run integration tests, e2e tests, the release gate, "도커로 테스트", "통합 테스트 돌려줘", "클러스터 테스트", or wants to verify time-series functions / tiered storage / TSCS against a real node rather than in-JVM. Also use it when deciding which of these to run after a change, or when one of them fails and the failure needs diagnosing.
---

# Container-based integration testing

The jvm-dtests under `org.apache.cassandra.distributed.test.timeseries.*` cover the distributed
invariants in one JVM and run on every push. These scripts are the counterpart that runs the same
invariants through a **real node from the published image** — real schema, real native protocol,
real cqlsh output, real sstables on disk. They catch a class of regression the in-JVM tests
structurally cannot: anything that only shows up once the code is packaged, started by the
entrypoint, and driven from outside the process.

## Build the image first

Everything here consumes `docker/Dockerfile`, which compiles the repo in a builder stage — so the
image *is* the thing under test and must be rebuilt after any source change:

```bash
docker build --network host -t cassandra-timeseries:6.0.0 -f docker/Dockerfile .
```

`--network host` is required: Docker's default bridge has no DNS resolution on this host, so
`apt-get` inside the build fails to resolve `archive.ubuntu.com`. Takes ~5 minutes.

When testing an uncommitted or in-progress change, tag it distinctly (`:6.0.0-<topic>`) and pass
that tag to the scripts, so a stale `:6.0.0` can't be silently tested instead.

## Which one to run

| Script | Cost | What it uniquely proves |
|---|---|---|
| `docker/integration-test.sh` | ~8 min, 1 container | Every time-series function family, gap-fill, SAI LIKE, the whole tiered-storage lifecycle, TSCS freeze, and that all of it survives a real process restart |
| `docker/cluster-test.sh` | ~15 min, 3 containers × 2G heap | Coordinator fan-out, per-replica TSCS convergence, repair streaming between OS processes, QUORUM with a replica down |
| `docker/scale-test.sh` | hours, 16G heap, 20M rows | Query latency on a production-shaped dataset (untiered baseline) — a benchmark, not a gate |
| `docker/tiering-bench.sh` | hours, needs scale-test's dataset | Storage ratio and read latency *after* tiering, comparable to that baseline |
| `docker/soak-workload.py` | days | Slow leaks and drift that no single run can see |

Default to `integration-test.sh`. Add `cluster-test.sh` when the change touches compaction,
streaming, repair, tiering, or anything a coordinator does — that is where the two diverge. The
benchmarks are for answering "how fast", never "is it correct"; don't run them as a verification
step, they take hours and need a 16G heap and a host bind-mount.

## Running

For the whole gate — build the jar, run the fork's test classes, build the image, run the
integration test, in `.gitlab-ci.yml`'s own order — use the one wrapper:

```bash
.build/sh/ci-local                 # add --with-cluster for the 3-node test
.build/sh/ci-local --stage image   # or one stage: jar | tests | image | integration | cluster
```

It tags the image by commit so a run cannot silently test something left behind under
`cassandra-timeseries:6.0.0`, and it fails if the jar predates the run rather than trusting ant's
log. Prefer it over driving the scripts by hand — and note that **it is currently the only thing
that verifies anything**: the project's GitLab runners have been offline since at least 2026-08-07,
so every pipeline fails with `stuck_pending_no_matching_runners` before a job starts.

To drive one script directly against an image you already have:

```bash
./docker/integration-test.sh cassandra-timeseries:6.0.0
./docker/cluster-test.sh     cassandra-timeseries:6.0.0
```

Both exit non-zero if any assertion failed, print one line per assertion with the CQL that ran and
the rows that came back, and write an HTML report (`build/timeseries-it-report.html`,
`build/timeseries-cluster-report.html`) that is kept as a CI artifact.

They take minutes; start them in the background and do other work while they run. Note that
`cluster-test.sh` deliberately **leaves its three containers running** so a failure can be
inspected — it prints the `docker rm -f` line to clean up, and removes them itself on the next run.

`CONTAINER_RUNTIME=podman` works for both. `READY_TIMEOUT` (default 300s) is the per-node wait for
CQL; raise it on a loaded machine rather than concluding the node is broken.

## What they assert (so you know what a pass actually buys)

**`integration-test.sh`** — 92 assertions against hand-computed values on a deterministic fixture:

- the version the node reports, so a run against a stale `:6.0.0` image cannot pass as a run
  against the change under test — and so an upstream merge that reset `base.version` to an alpha
  would land here
- every scalar/aggregate family: `time_bucket`, `first`/`last`/`delta`/`rate`/`derivative`,
  reset-aware `counter_delta`/`counter_rate`, `percentile`/`variance`/`stddev`/`histogram`/
  `approx_count_distinct`, `integral`/`time_weighted_average`, and the two-variable regression set
  on a known `y = 2x + 1`
- gap-fill: bucket materialisation, `locf` (including that it leaves pre-first buckets null) and
  `interpolate` (including the trailing bucket)
- SAI `LIKE` with `index_analyzer`: substring, prefix, suffix, Korean fragments including one that
  crosses a space, that `=` keeps exact semantics on the analyzed column, and that `LIKE` composes
  with `time_bucket`
- tiered storage end to end, twice: once on the simplest shape and once on **`tm_tag_point`'s exact
  production shape** (7 statics, 7 regular columns across text/int/double/boolean and a frozen map,
  DESC clustering) — policy install, `nodetool retier`, chunk creation, transparent merged reads,
  late-row merge, statics surviving the range delete, DESC bound arithmetic in both directions and
  both orderings, cold-write rejection, and `system_views.timeseries_tiering`

- TSCS: that the flush split at window boundaries (T3), that each closed window freezes to exactly
  one sstable (T2), that a converged window is then *not* rewritten again — a freeze/split livelock
  is invisible to every read, its only observable is that the sstable set keeps changing — and that
  no row was lost doing it
- a **restart**: the container is restarted through the image's entrypoint and the tiering policy,
  chunk table, chunk row, merged reads, aggregates, TSCS sstable layout and the SAI index are all
  asserted again on the other side. The jvm-dtests restart an in-JVM instance; only this one
  restarts a real process off a real commit log, which is the shape the chunk-table TCM bug had

The restart section builds its **own** tiered table rather than reusing the ones above, because
both tiering sections end by deleting their chunk row (see the deletion gate below) — there would
be nothing left to reconstruct. Keep it that way if you extend it.

The tiering section ends with a **deletion gate** worth understanding before you touch it: every
other tiering assertion passes whether or not the re-encoder actually deleted the base rows, because
transparent reads reconcile chunk rows with live base rows either way. So it deletes the chunk row
and asserts the window then returns *nothing* — the one signal from cqlsh that the merge cannot
fabricate. If you add a tiering assertion, ask yourself whether it would survive the re-encoder
silently skipping its range delete; if it would, it is not testing what you think.

**`cluster-test.sh`** — 12 assertions plus per-coordinator sweeps:

- one schema version across three nodes after formation
- aggregation and gap-fill asserted through **every** coordinator (a right answer on node 1 says
  nothing about node 2)
- TSCS freezes each closed window down to exactly one sstable **on every replica independently**,
  and a converged window is not rewritten again (freeze/split livelock guard)
- a real repair stream between two OS processes, with the receiving node then serving the rows
- writes and reads at QUORUM with one replica actually stopped
- tiering across three replicas: each tag encoded exactly once cluster-wide, merged reads correct
  from every node

## Diagnosing a failure

The scripts print the exact CQL and the exact rows for every assertion, so start by reading the
failing block rather than re-running. Then:

- **Node never became ready** — `docker logs <container>` (the script tails 40 lines itself). Usually
  the image, not the test: a broken build, or a schema/startup change.
- **A CQL assertion fails** — reproduce by hand against the still-running container
  (`docker exec <c> cqlsh -e '<the CQL from the report>'`) before changing anything.
- **A tiering assertion fails** — check `system_views.timeseries_tiering` for whether the cycle ran
  at all; a policy that failed to install makes every downstream assertion fail confusingly.
- **A cluster assertion fails** — the containers are still up. `nodetool status`, `nodetool
  tablestats`, and the per-node sstable listing the script used are all still reachable.

Distinguish a *product* failure from a *capacity* failure honestly. Three 2G JVMs plus the test on a
busy host can time out on readiness or on compaction convergence without anything being wrong; that
is why `cluster-test.sh` is manual in CI rather than a release gate. Say which one you think it is,
and why.

## Reporting back

Give the assertion counts (`N passed, M failed`), name the failing sections, and point at the HTML
report. If you rebuilt the image, say which tag you tested — testing a stale `:6.0.0` is the easiest
way to report a green run that means nothing.
