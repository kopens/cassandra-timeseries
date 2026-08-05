# Tiered storage in production: what the first rollout taught

Written 2026-08-06, from taking `timeseries_tiering` from "configured but silently dead" to
"working" on a 450 GiB single-node production cluster (`pp` keyspace, ~15,000 writes/sec).

This is the operational counterpart to
[2026-07-31-chunk-store-sp2.md](2026-07-31-chunk-store-sp2.md), which specifies the re-encode
cycle. Read that for what tiering *does*; read this before enabling it on a new table.

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
