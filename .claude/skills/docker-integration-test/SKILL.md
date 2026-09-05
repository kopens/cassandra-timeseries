---
name: docker-integration-test
description: The pre-release verification procedure for this fork — the full local gate (.build/sh/ci-local), the three-node cluster test, the performance regression gate, and the trial-run protocol that must precede enabling tiered storage on real data. Use this whenever the user asks to run integration or e2e tests, to verify a build, to check whether something is ready to ship or "문제없나 / 출시해도 되나 / 프로덕션 써도 되나", before cutting a release tag, and whenever a change touches tiering, compaction, the chunk codec or the read path. Also use it when one of these runs fails and the failure needs diagnosing.
---

# Verifying a build before it ships

The jvm-dtests cover the distributed invariants in one JVM and run on every push. Everything here
runs the same invariants through a **real node built from the image that will actually ship**, which
is the only way to catch what appears once the code is packaged, started by the entrypoint and
driven from outside the process.

Treat this as a checklist, not a menu. The features it covers — tiered storage above all — delete
base rows and cannot be rolled back, so "we ran the unit tests" is not a release decision.

## 0. Know what CI is and is not telling you

This section used to say the runners were all offline and every pipeline died at
`stuck_pending_no_matching_runners`. That is over: project runner 77 (kopens-234) has been online
since 2026-08-26 and jobs execute (verified 2026-09-05). **A red pipeline is now evidence about the
code, not about the runners** — pipeline #53500's `timeseries-tests` failure was a real test bug.
Read the trace before dismissing one; `glab ci trace` renders a TUI that is unreadable when piped,
so use the API instead.

```bash
glab ci list --per-page 5
glab ci get -p <id>                                            # job names + statuses
glab api projects/common%2Fcassandra-timeseries/runners        # is anything online?
glab api projects/common%2Fcassandra-timeseries/pipelines/<id>/jobs
glab api projects/common%2Fcassandra-timeseries/jobs/<id>/trace | sed -e 's/\x1b\[[0-9;]*m//g'
```

Check this before quoting CI as evidence, and say plainly which of the runs below you actually did.
Per CLAUDE.md, all GitLab work goes through `glab`.

## 1. The gate — `.build/sh/ci-local`

Walks `.gitlab-ci.yml`'s stages in order on this machine: jar + checkstyle, the fork's test classes,
the docker image, the integration test.

```bash
.build/sh/ci-local                  # add --with-cluster to include stage 2 below
.build/sh/ci-local --stage image    # one stage: jar | tests | image | integration | cluster
```

It tags the image by commit, so a run cannot silently verify something left behind under
`cassandra-timeseries:6.0.0`, and it fails if the jar predates the run — `ant` reports success
without building when it is missing from PATH, and `ant-log-summary.py` prints `BUILD SUCCESSFUL` on
empty input, so a green log is not evidence on its own.

Stages are strictly serial, and runs must not overlap: `ci-test` begins with a `realclean`, so a
second run deletes the first's resolved dependencies under `build/lib/jars` and the victim fails
with hundreds of `package org.slf4j does not exist` errors that read as a broken change and are not.

## 2. The cluster test — required before a release, not optional

```bash
./docker/cluster-test.sh cassandra-timeseries:<tag>     # or ci-local --with-cluster
```

`.gitlab-ci.yml` marks `docker-cluster-test` manual and explicitly not a release gate, on the
argument that three 2G JVMs plus the job may not fit a shared runner. That argument is about runner
capacity, not about risk — and it leaves the only coverage of tiering's *cluster* invariants outside
every gate:

- each tag encoded exactly once across the cluster (every node re-encodes only its own primary
  ranges, so all three have to run and none may double-encode);
- TSCS freezing each closed window to one sstable **on every replica independently** — compaction is
  node-local, and a node that never converged answers every read correctly while keeping its disk;
- a real repair stream between operating-system processes, and the window-split layout of what the
  receiving node ends up with;
- aggregation and gap-fill computed by each coordinator in turn.

Run it on a machine with the headroom, once, before any release that touches compaction, streaming,
repair or tiering. It leaves its three containers up on purpose so a failure can be inspected;
`docker rm -f cassandra-ts-cluster-{1,2,3}` when done.

## 3. The performance gate — `.build/sh/ci-perf`

```bash
.build/sh/ci-perf                   # compare against doc/timeseries/perf-baseline.json
.build/sh/ci-perf --record          # deliberately move the line (see below)
```

Runs the chunk codec and cursor JMH classes and fails on a regression beyond the threshold (25% by
default). It is a *regression* gate, not a benchmark: `docker/scale-test.sh` and
`docker/tiering-bench.sh` answer "how fast on production-shaped data" and take hours with a 16G heap.

Two rules it enforces so the gate keeps meaning something:

- **It refuses to gate across hosts.** The baseline records the machine it was taken on; elsewhere it
  reports every number and fails nothing. A 4114T and a Haswell E5-2676 v3 differ by more than any
  useful threshold, and a gate that cries wolf on hardware gets ignored exactly when it is right.
  The same applies to quoting numbers: every figure in `doc/timeseries/*` is from host 234, and the
  production node is 41.
- **`--record` is explicit and never automatic.** "The numbers moved so I moved the line" is how a
  perf gate quietly stops existing. Re-record only for a deliberate trade, in the same commit as the
  change, and say so in the message.

**A uniform slowdown across every benchmark is the host, not the code.** `ci-perf` refuses to gate
across *hosts*, but it cannot tell that the right host is busy. On 2026-09-05 a post-merge run
failed 15 benchmarks at +25% to +63% — and the tell was that *everything* moved the same way,
including `ChunkBitUnpackBench.unpack[width=2]` (+62.9%), which is bit-twiddling over a byte array
that the merge provably did not touch. `uptime` showed load average 27 with several browser
processes at 200-290% CPU. Before reading a red `ci-perf` as a regression, check two things:

```bash
uptime; ps -eo pcpu,comm --sort=-pcpu | head          # is this machine actually idle?
git diff --name-only <base> HEAD -- src/java/org/apache/cassandra/db/timeseries/ test/microbench/
```

If the second command is empty for the code under the failing benchmarks, the change cannot be the
cause — say so, and re-run on an idle host rather than re-recording the baseline. A real regression
is *selective*: it hits the benchmarks whose code changed and leaves the rest inside the noise band.

**What a healthy run looks like.** On an unchanged tree against its own baseline, 24 of 27
benchmarks land within ±3% and the worst sits near +18%. That spread is the floor of this machine's
noise, not slack in the gate — which is why the threshold is 25% rather than something tighter, and
why both sides take the minimum of `PERF_PASSES` sweeps (3). A single sweep moved one benchmark
across 545 / 433 / 393 µs on the same commit, so a comparison built on one sweep either hides a real
40% regression or invents one.

**Reading the result.** The long benchmarks are the trustworthy ones: `ChunkReadBench.fullScan`
(~758 µs for 3,600 rows × 8 columns) barely moves between runs, while sub-microsecond entries like
`ChunkPresenceBench.decode[mode=ALL_PRESENT]` are mostly noise at this scale — a large percentage on
a 0.006 µs number is not a finding. Two entries are worth knowing by name because they pin design
decisions rather than raw speed: `fullScan` against `fullScanBySlot` (~393 µs) is the cost of the
cursor's name lookup, and `rankPerRow` (~8.7 µs) against `runningIndex` (~1.2 µs) is the running
value-index rule the v4 cursor enforces with `rankCalls() == 0`. A regression that closes either gap
means a design invariant broke, not that something got slower.

**The benchmarks are not the product measurement.** `ci-perf` says "the decode path did not get
slower". It says nothing about query latency on real data — that is
[tiering-benchmark.md](../../../doc/timeseries/tiering-benchmark.md) (host 234: storage 7.1× smaller,
aggregates 3–6× faster, re-encode 108k rows/s), and those figures do not transfer to the production
node either. Quote the right one for the question being asked.

## 4. Before enabling tiered storage on real data

Tiering deletes the base rows. From that moment `<table>__chunks` is the only copy, a build that
cannot read v4 chunks cannot read the data, and dropping the chunk table destroys it. Read
[production-rollout.md](../../../doc/timeseries/production-rollout.md) §0 in full — it is short and
every item is a one-way door.

Do not skip the trial run:

1. **Every node on a v4 build first.** v4 chunks are unreadable by older builds and v1/v2/v3 chunks
   are unreadable by this one, with no converter. Any chunk table left by an older build must be
   `DROP`ped before enabling — that data does not come back.
2. **A table whose data can be regenerated**, or a copy, for at least a full retention cycle. Long
   enough that the re-encoder, cold-window expiry and at least one node restart have all happened.
3. **Restart a node during the trial** and confirm it comes back and still serves the merged view.
   The chunk-table TCM bug was invisible until a node replayed its own metadata log.
4. **Confirm backups include `<table>__chunks`** before the first re-encode, not after.
5. **Watch `system_views.timeseries_tiering`** and the ledger through the trial rather than only at
   the end — a cycle that stops making progress is the signal, and it is silent otherwise.

Only then consider a real table, one table at a time.

## What the runs assert

**`docker/integration-test.sh`** — assertions against hand-computed values on a deterministic
fixture:

- the version the node reports, so a run against a stale image cannot pass as a run against the
  change under test, and an upstream merge that reset `base.version` lands here
- every scalar/aggregate family: `time_bucket`, `first`/`last`/`delta`/`rate`/`derivative`,
  reset-aware `counter_delta`/`counter_rate`, `percentile`/`variance`/`stddev`/`histogram`/
  `approx_count_distinct`, `integral`/`time_weighted_average`, and the regression set on `y = 2x + 1`
- gap-fill: bucket materialisation, `locf` (including that it leaves pre-first buckets null) and
  `interpolate` (including the trailing bucket)
- SAI `LIKE` with `index_analyzer`: substring, prefix, suffix, Korean fragments including one that
  crosses a space, that `=` keeps exact semantics on the analyzed column, and composition with
  `time_bucket`
- tiered storage end to end, twice: on the simplest shape and on **`tm_tag_point`'s exact production
  shape** (7 statics, 7 regular columns across text/int/double/boolean and a frozen map, DESC
  clustering) — policy install, `nodetool retier`, chunk creation, transparent merged reads, late-row
  merge, statics surviving the range delete, DESC bound arithmetic both directions and both
  orderings, cold-write rejection, and `system_views.timeseries_tiering`
- TSCS: the flush split at window boundaries (T3), each closed window freezing to exactly one sstable
  (T2), that a converged window is then *not* rewritten again — a freeze/split livelock is invisible
  to every read and only shows as an sstable set that keeps changing — and that no row was lost
- a **restart** through the image's entrypoint, re-asserting the tiering policy, chunk table, chunk
  row, merged reads, aggregates, the TSCS sstable layout and the SAI index on the other side

Two things to know before editing that script:

**The deletion gate.** Every other tiering assertion passes whether or not the re-encoder actually
deleted the base rows, because transparent reads reconcile chunk rows with live base rows either
way. So the section ends by deleting the chunk row and asserting the window then returns *nothing* —
the one signal from cqlsh the merge cannot fabricate. If you add a tiering assertion, ask whether it
would survive the re-encoder silently skipping its range delete. If it would, it is not testing what
you think. (This is also why the restart section builds its own tiered table: the earlier ones have
no chunk left to come back from.)

**Wall-clock assertions must be pinned to the worst case, not to "now".** The hot-window DELETE
checks use the last millisecond before the current `chunk_window` boundary, because that is the
newest row a coverage ledger claiming the cycle cutoff as its top would call cold. Written against
`now` it only reproduced when the test happened to run in the first minutes of an hour — which is
how a real write-guard bug survived two green runs before a third caught it. Any new assertion that
depends on where `now` sits inside a window needs the same treatment.

**`docker/cluster-test.sh`** — the cluster invariants listed in §2, plus one schema version across
three nodes and QUORUM behaviour with a replica actually stopped.

## Diagnosing a failure

The scripts print the exact CQL and the exact rows for every assertion, so read the failing block
before re-running. Then:

- **Node never became ready** — `docker logs <container>`. Usually the image, not the test.
- **A CQL assertion fails** — reproduce by hand against the still-running container
  (`docker exec <c> cqlsh -e '<the CQL from the report>'`) before changing anything.
- **A tiering assertion fails** — check `system_views.timeseries_tiering` for whether the cycle ran
  at all; a policy that failed to install makes every downstream assertion fail confusingly. If a
  write was refused as cold, compare the boundary in the error message against what the coverage
  ledger claims — the guard and the read path share `ColdBoundary.coldBelowMs`, and an overstated
  ledger top refuses rows that were never encoded.
- **A cluster assertion fails** — the containers are still up: `nodetool status`, `nodetool
  tablestats`, and the per-node sstable listing are all still reachable.

Distinguish a *product* failure from a *capacity* failure honestly. Three 2G JVMs plus the test on a
busy host can time out on readiness or on compaction convergence with nothing wrong. Say which one
you think it is, and why.

## Reporting back

Give the assertion counts, name the failing sections, and point at the HTML reports
(`build/timeseries-it-report.html`, `build/timeseries-cluster-report.html`). Say which image tag you
tested — testing a stale one is the easiest way to report a green run that means nothing. If asked
whether something is ready to ship, answer against this checklist and name what you did **not** run.
