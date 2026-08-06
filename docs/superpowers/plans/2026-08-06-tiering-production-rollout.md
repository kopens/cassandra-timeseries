# Tiered storage in production: what the first rollout taught

Written 2026-08-06, from taking `timeseries_tiering` from "configured but silently dead" to
"working" on a 450 GiB single-node production cluster (`pp` keyspace, ~15,000 writes/sec).

This is the operational counterpart to
[2026-07-31-chunk-store-sp2.md](2026-07-31-chunk-store-sp2.md), which specifies the re-encode
cycle. Read that for what tiering *does*; read this before enabling it on a new table.

## Where things stand (end of 2026-08-06)

Deployed and running:

| table | hot_window | chunk_window | interval | cold_window | notes |
|---|---|---|---|---|---|
| `pp.tm_tag_point` | 12h | 1h | 5m | *unset* | 44.2M rows encoded; hot_window raised from 3h during an incident, see below |
| `pp.tm_asset_data_based_second` | 12h | 15m | 5m | *unset* | active partitions encoding, backlog being discovered by the cursor walk; on TSCS, see below |
| `tstest.tier_probe` | 10m | 5m | 1m | *unset* | test keyspace |

`timeseries_tiering` accepts exactly five keys and **rejects any it does not know** — `hot_window`
(required), `chunk_window` (default `1h`, max `31d`), `cold_window`, `consistency` (default
`LOCAL_QUORUM`), `interval` (default `5m`). Durations are `<positive int><m|h|d>`. There is **no
`chunk_size`**: a chunk's size is `chunk_window` times the row density of that window, which is why
sizing it means measuring density first (see the worked example below).

Compaction changes made the same day, on tables with a uniform TTL that were on
`UnifiedCompactionStrategy` — see the section on that below for the numbers:

| table | compaction | window_size | freeze_after | retention |
|---|---|---|---|---|
| `pp.tm_flow_log` | TSCS | 1d | 1d | 8d |
| `pp.tm_option_listener_push_cache` | TSCS | 6h | 6h | 36h |
| `pp.tm_asset_data_based_second` | TSCS | 1d | 2d | 3651d |

Measured across the day: node 450 → 250 GiB, `pp` keyspace 487 → 250 GB, compaction backlog
168 → ~45, tag enumeration ~220s → under 1s.

**Still open:** the batch writer runs on `-Xmx3g` against an 800k-row queue and saturates whenever
Cassandra restarts (it recovers on its own once the source stops); `pp.tm_flow_log` has only 74
buckets for 50 GB, which is a write-side schema question; CI runners for this project are all stale,
so local builds are the only verification; and Cassandra's logs are recreated on startup, which
erased the pre-restart evidence twice while diagnosing incidents here.

## The failure that started it

`pp.tm_tag_point` had a tiering policy for days and its `__chunks` table was **empty**: zero
partitions, zero writes, ever. The only visible symptom was a `WARN` every 90 seconds:

```
Tiered storage sweep failed for pp.tm_tag_point; skipping it this tick
ReadTimeoutException while executing SELECT * FROM pp.tm_tag_point
  WHERE token(tag_id) > ... LIMIT 5000 PER PARTITION LIMIT 1 ALLOW FILTERING
```

That is the tag-enumeration query (`SELECT DISTINCT <pk>`; Cassandra renders DISTINCT as
`PER PARTITION LIMIT 1`). Three separate defects stacked:

1. **The page size was catastrophically wrong for the query shape.** Enumeration paged at
   `PAGE_SIZE` (5000). DISTINCT is cheap per *row* and expensive per *partition* — every row is a
   different partition, so the coordinator seeks the first live row of each across every SSTable
   the range touches. Measured on this table: **~19 ms per partition**. A 5000-row page therefore
   needs ~95 s.

2. **The deadline was not the one the operator had configured.** `range_request_timeout` was
   raised to 60 s, and it made no difference, because `RequestTime.computeDeadline` takes
   `min(verb timeout, client deadline)` and the client deadline of an internally-issued query is
   `native_transport_timeout` — **12 s by default**. 20 s of work against a 12 s ceiling is a
   deterministic, permanent failure.

3. **The retry made it worse.** `dueForSweep` gated on the last *completed* run, so a table that
   always fails is permanently "never run" and was re-attempted every 60 s tick regardless of its
   own `interval` (5 m here) — a failing table generating the full-table-scan load that kept it
   failing.

**The lesson that generalises:** an internally-issued paged query is bounded by
`native_transport_timeout`, not by the verb timeout. Size pages by *measured cost per row of that
query shape*, not by a shared constant.

Once fixed, the first cycle encoded **44,222,757 rows into 24,206 windows with 0 tags skipped**.

**And shrinking the page was not the end of it.** A smaller page bought headroom, not a bound:
`SELECT DISTINCT`'s `LIMIT` counts only partitions that still have a *live* row, so a page asked for
256 tags walks past however many already-tiered or static-only partitions lie between them. On the
176 GB table every token range still failed, on every cycle. The scan now stops trying to finish at
all — see "Discovery is a bounded walk, not a scan" below.

## Enumerating tags: why the registry exists

Even with correct paging, walking the ring cost ~220 s per cycle (11,623 tags × 19 ms) against a
300 s `interval` — 73% of the budget spent deciding *what* to work on.

`TagRegistry` moves the tag set into a `<table>__tags` shadow table: one partition, one clustering
row per tag. Measured on the same table: **11,623 tags in 0.7 s** (vs ~220 s). Enumeration stopped
being the bottleneck.

It is filled from two sides, and needs both:

- the **write path** (`TieredWrites.noteTag` → `TagRegistry.noteTagIfTiered`) registers a tag on
  its first write; afterwards it is a set lookup;
- a **reconcile** against the base table every `RECONCILE_INTERVAL_MINUTES` (60) rewrites it from
  the authoritative `SELECT DISTINCT`.

The reconcile is not redundant. A tag whose rows predate the feature and which is never written to
again — a decommissioned sensor, which is exactly the backlog that most needs tiering — has no
registry entry, and *missing from the registry means never encoded, silently*. Every failure path
therefore falls back to the scan: an unreadable registry, an empty one, or a scan that skipped a
range are all treated as "cannot answer", never as "there are no tags".

**Write-path cost is the thing to watch.** Measured under 15,000 writes/sec: 18.35 M writes
produced **zero** registry INSERTs (the cache held), all stages showed zero pending/blocked, and
per-mutation local write latency *fell* from 133 µs to 74 µs. Two ordering rules keep it that way,
and both were bugs first:

- resolve the policy (one extensions-map lookup) *before* building the tag, or every write to
  every non-tiered timestamp-clustered table pays an allocation forever;
- claim the tag in the seen-set *before* the INSERT and return if the claim is lost, or every
  concurrent writer of a newly-seen tag issues its own registration.

## Discovery is a bounded walk, not a scan

The registry answers "which tags are being written to". It cannot answer "which tags exist" — a tag
whose rows predate tiering and that nothing writes to any more has no registry entry, and that is
exactly the backlog most worth compressing. Only the base table knows, and on a large table the
`SELECT DISTINCT` that asks it does not finish.

So the reconcile stopped trying to finish. Each cycle advances a cursor by at most
`scanPagesPerCycle` (8) pages of `token(pk) > cursor` — rows come back in token order, so the last
row's token is where to continue — and registers what it finds. A page that fails leaves the cursor
untouched, so the next cycle retries that ground rather than skipping it. A short page means the
ring is exhausted; the cursor rewinds and the next pass begins. Coverage arrives over hours instead
of never, and no cycle can hang.

Measured on `pp.tm_asset_data_based_second` immediately after deploying it: `tags_skipped` went
**17 → 1** (17 being every token range, every cycle) and enumeration errors in the log went to
**zero**.

The cursor lives in memory only. What must survive a restart is *which tags exist*, and that is in
the registry table; losing the position costs re-walking ground whose tags are already registered
and already being encoded. Two known limitations, both deliberate: the walk covers the whole ring
and `restrictToPrimaryRanges` filters at read time, so on an N-node cluster each node scans every
range — exact, but N-times redundant; and frequent restarts could in principle keep the walk from
reaching the far end of the ring, which is the point at which persisting the cursor becomes worth
its cost. Measure before assuming either.

**Known limitation:** the registry has no eviction. A tag that stops existing keeps its row and
costs one `firstClosedWindowStart` probe per cycle forever. Harmless for a stable tag set; on a
high-churn tag space it grows without bound. Safe removal is possible — a *complete* scan is
authoritative, but only for tokens in this node's primary ranges, since the registry is one
partition shared by every node — and is deliberately not implemented until a workload needs it.

## Enabling tiering on a new table: check these first

Beyond `TieringPolicy.unsupportedSchemaError` (which enforces one timestamp clustering column, no
counters, no non-frozen collections, `transactional_mode = 'off'`, no non-static secondary index):

### 1. `default_time_to_live` — this one is irreversible

**Tiering silently converts bounded retention into unbounded growth.** The chunk table deliberately
does not inherit the base TTL, and the chunk insert carries `USING TIMESTAMP` but no `USING TTL`,
so a row's TTL is dropped the moment its value is copied into a chunk — and the base row is then
deleted. A 31-day table becomes a forever table, and there is no un-tier.

`cold_window` is how retention is expressed on a tiered table.
`TieringPolicy.ttlWithoutColdWindowWarning` now says so once an hour, but the check is a warning,
not a refusal: dropping TTL is legitimate when it is what you want.

### 2. `chunk_window` — size it from bytes, not from time

A chunk is a single cell, and reading one row decompresses the whole chunk. Size it so the payload
stays small, using the partition histogram rather than the table average — the spread is wide.

Worked example, `pp.tm_asset_data_based_second` (176 GB):

| Source | Number |
|---|---|
| `tablehistograms` max cell count | 785,939 |
| ÷ ~9 columns | ~87,000 rows in the busiest partition-day |
| `Compacted partition maximum bytes` | 4.1 GB |
| ⇒ bytes per row (worst case) | ~47 KB |
| `chunk_window = 1h` ⇒ 3,625 rows | ~170 MB per chunk — far too large |
| `chunk_window = 15m` ⇒ 906 rows | ~43 MB — workable |

Note `max_mutation_size` (half of `commitlog_segment_size`; 160 MiB here) is *not* the binding
constraint — read amplification is.

**What the histogram actually told us, once chunks existed:** the real density is one sample per
~10 seconds, so a 15-minute window holds **90 rows**, not 906. The histogram's maximum cell count is
a worst case across the whole table and it over-estimated the typical partition by an order of
magnitude. 15m is therefore comfortable but conservative — 1h would still be safe here and would cut
the chunk-row count by four. Size from the histogram to pick a *safe* starting value, then re-measure
from the chunk table's own `samples` column and tune.

### 3. Expect the first cycle to process the whole backlog

There is no throttle. `tm_tag_point` (3 GB) took ~20 minutes. Plan the first cycle for a table
sized in hundreds of GB accordingly, and watch compaction: the cycle writes chunks *and*
range-deletes every source row it encoded.

### 4. There is a safe abort

Setting `extensions = {}` stops further encoding immediately, and already-written chunks stay
readable — the read path consults `ChunkCoverage` (what chunks actually exist), not the live
policy, precisely so that dropping the extension cannot hide encoded history.

## Restarting a node with tiering enabled

`nodetool stopdaemon` used to hang forever: nothing shut the sweep's executor down, so drain never
converged. The observed state was the worst kind — port 9042 closed (every client saw the node as
down) while the process ran on, the sweep still issuing reads, chunk inserts and range deletes
against a node drain was in the middle of dismantling. Recovery was a `kill -9`.

`TieredStorageService.setup` now registers `stopSweeping` as a `StorageService` pre-shutdown hook.
After the fix the same node stopped cleanly in **29 seconds**.

Cancelling the sweep is necessary but not sufficient: interrupting the thread does not stop the
cycle, because the per-tag handler catches every `RuntimeException` and moves on — by design, so
one bad tag cannot wedge a table. During shutdown *every* remaining tag fails, so the first version
walked the whole 11k-tag backlog logging one ERROR each: thousands of lines in three seconds. The
`sweepStopping` latch is checked by the encode loop, the cold-expiry loop and the per-tag handler
so the cycle leaves quietly.

## A TTL'd table on UnifiedCompactionStrategy is a standing leak

Not a tiering bug, but found while chasing tiering's compaction load, and the single largest win of
the day — so it belongs next to the rest.

`pp.tm_flow_log`: `PRIMARY KEY (bucket, ts, log_id)`, clustered by time, `default_time_to_live` of 7
days, never read, on `UnifiedCompactionStrategy`. It held **1,088 SSTables and 50 GB** and was the
largest single contributor to a compaction backlog that would not clear.

UCS compacts by size and level, with no notion of age. So it rewrote data that was days from
expiring, over and over, while never reclaiming what had already expired — the worst of both. The
fix was one `ALTER`:

```sql
ALTER TABLE pp.tm_flow_log WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d', 'freeze_after': '1d', 'retention': '8d'
};
```

Measured, over five minutes and with no operator action beyond the ALTER:

| | before | after |
|---|---|---|
| SSTables | 1,088 | 11 |
| size | 50 GB | 848 MB |
| its share of the compaction backlog | 36 pending | gone from the list |

The same change on `pp.tm_option_listener_push_cache` (TTL 1 day, 136 SSTables) took it to 7. Total
compaction backlog went 128 → 53 and the `pp` keyspace went 487 GB → 405 GB.

**`retention` is not optional, and it is the part that is easy to get wrong.** TSCS freezes a closed
window to a single SSTable and then never compacts it again — so, as its own javadoc says, *data that
expires after the freeze is not reclaimed by this strategy*. Without `retention` nothing ever drops
the window. `tstest.tier_probe` shows the failure mode in the same cluster: 5-minute windows, no
retention, **988 SSTables for 29 MB**. Set `retention` at or just above the table's TTL — below it and
you delete data before its TTL, which is data loss.

**Two predictions of mine were wrong here, both in the safe direction.** I expected a large one-time
compaction burst on switching strategy; there was none, because the conversion classifies existing
SSTables into windows rather than rewriting them (`active=0` throughout). And I expected the win to
be compaction I/O; the bigger win was 49 GB of expired data that UCS had simply never dropped.

Rule of thumb: **a table with a uniform TTL and time-ordered clustering should not be on UCS.** Check
`default_time_to_live` against `compaction` when a table's SSTable count runs into the hundreds.

### And a tiered table on UCS is the same leak, for a different reason

`pp.tm_asset_data_based_second` was left on UCS on the reasoning that tiering would shrink it on its
own — moving cold windows into chunks and range-deleting the source rows. That reasoning was wrong,
and the numbers said so: **190 SSTables and 271 ms local read latency** while the two tables on TSCS
sat at 13 and 22.

Tiering *deletes* the source rows, but a delete is a tombstone, and UCS has no reason to go back and
purge one. So the table kept occupying **165 GB of space for data it no longer served**. The re-encoder
was doing its job and nothing was reclaiming the result. Switching it to the same settings
`pp.tm_tag_point` already used:

```sql
ALTER TABLE pp.tm_asset_data_based_second WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d', 'freeze_after': '2d', 'retention': '3651d'
};
```

| | before | 4 minutes later |
|---|---|---|
| SSTables | 190 | 12 |
| table size | 165 GB | 3.7 GB |
| node load | 409 GiB | 250 GiB |

`retention` matches the table's 10-year TTL, as it must — shorter would delete data before it expires.

**Tiering and TSCS are one setting, not two.** Tiering moves the data; TSCS is what reclaims the space
the move frees. Either alone under-delivers, and the failure is silent: the chunk table fills, the
counters look healthy, and the base table never shrinks.

## Reading the numbers

- **`system_views.timeseries_tiering` counters are per-process** and reset on restart.
  `last_run_at = -1` after a restart means "no cycle has *completed* yet", not "broken".
- **`windows_encoded = 0` is the healthy steady state** once the backlog is cleared — it only rises
  when a window ages past `hot_window`.
- **Do not verify tiering with a client `SELECT`.** `TransparentReads` merges chunk rows back in, so
  a count of "rows below the cutoff" includes rows that were encoded and deleted. It looks exactly
  like tiering did nothing. Use the `__chunks` table's write count and `tags_skipped` instead.
- **`MutationStage` is a dead counter on a single-node cluster** — mutations are applied inline and
  bypass it. It read 602,448 while 18.35 M writes landed. Alert on `CounterMutationStage` and
  dropped-message counts instead.
- **ZGC's `GCInspector` "GC in NNNms" is cycle wall-clock, not a pause.** ZGC pauses are sub-ms and
  are not in that line at all.
