/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.timeseries.tiering;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.db.timeseries.StatOrder;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * Background re-encoder that turns closed, hot-window-expired time-series rows into columnar
 * chunks ({@link ColumnarChunkCodec}, format version 4) in a shadow {@code "<table>__chunks"} table
 * (see {@link ChunkTables}), then tombstones the source rows it just encoded.
 * <p>
 * <b>Every regular column is encoded</b>, not a designated value column: one chunk per (partition
 * key, window) carries the window's shared timestamp axis plus one independently-compressed section
 * per regular column, each column's values being the exact serialized bytes Cassandra stored (see
 * {@link ChunkColumnTypes}). A null cell round-trips as null, never as a default. Static columns are
 * not chunked and are untouched: the source-row delete is a clustering-range delete, and static cells
 * live outside every clustering range.
 * <p>
 * {@link #runOnce} is the whole cycle for one (keyspace, table) pair, run synchronously and
 * idempotently -- see docs/superpowers/plans/2026-07-31-chunk-store-sp2.md ("재인코딩 사이클") for the
 * normative algorithm this implements. {@link #instance} is the process-wide singleton wired up by
 * {@link org.apache.cassandra.service.CassandraDaemon#setup()} via {@link #setup()}, which registers
 * the {@link TieredStorageServiceMBean} and schedules a single, global sweep (see {@link #sweep}) that
 * drives {@link #runOnce} for every policy-bearing table on its own {@code interval}; callers that just
 * want the core cycle (tests, {@code nodetool retier}) can still call {@link #runOnce} directly, or go
 * through the per-table overlap guard via {@link #retier}.
 * <p>
 * <b>Invariants (violating either fails review):</b>
 * <ul>
 *     <li>Every data read/write/delete goes through {@link QueryProcessor#process} at the policy's
 *     configured {@link ConsistencyLevel} -- never {@code executeInternal}, which would only see
 *     locally-held data and could tombstone rows a distributed delete should have fanned out to.</li>
 *     <li>The range delete of source rows for a re-encoded window always uses
 *     {@code USING TIMESTAMP <maxWritetimeOfRowsEncodedIntoTheChunk>}, so a row written after this
 *     cycle read the window (a "late" row) has a newer write timestamp than the tombstone and
 *     survives it -- to be merged into the chunk on a subsequent cycle. With many columns that
 *     maximum is taken over <b>every column's cell writetime of every row read</b>, plus the
 *     writetime the chunk being replaced was built from; a window that yields no cell writetime at
 *     all is left completely untouched rather than deleted at a guessed timestamp.</li>
 * </ul>
 * <p>
 * Per-tag work is memory-bounded: rather than reading every closed row for a tag into one list,
 * {@link #runOnce} walks the tag's closed windows one at a time (see {@code firstClosedWindowStart}/
 * {@code nextClosedWindowStart}), so peak memory is one window's worth of rows, not the whole
 * backlog. A failure re-encoding one tag (a decode error on a corrupt existing chunk, a read/write
 * timeout, ...) is logged and skipped rather than aborting every other tag on the table.
 * <p>
 * The window walk's loop guards {@code windowStart >= cutoff} on every path that can advance it --
 * initial discovery, the empty-window jump, and the dense (window-after-window) continuation alike --
 * so no window that reaches into the hot region is ever fetched, encoded, or deleted, no matter how
 * densely a tag is being ingested.
 */
public class TieredStorageService implements TieredStorageServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(TieredStorageService.class);

    /**
     * Network page size for the paged scans this cycle issues (per-window row reads, cold-expiry
     * candidates), and for the read path's chunk-window listing ({@link ChunkRowSource}). Those are
     * all single-partition reads of contiguous clusterings, where 5000 rows is one cheap slice --
     * tag enumeration is not, and uses {@link #TAG_PAGE_SIZE} instead.
     */
    static final int PAGE_SIZE = 5000;

    /**
     * Network page size for the {@code SELECT DISTINCT} partition-key scans {@link #enumerateTags}
     * issues, deliberately far smaller than {@link #PAGE_SIZE}.
     * <p>
     * A page is one range request, and it must finish inside the coordinator's read deadline or the
     * whole scan dies with a {@link org.apache.cassandra.exceptions.ReadTimeoutException}. That
     * deadline is <b>not</b> {@code range_request_timeout} alone: {@code RequestTime.computeDeadline}
     * takes the <em>minimum</em> of the verb timeout and the client deadline, and the client deadline
     * of an internally-issued query is {@code native_transport_timeout} (12s by default). Raising
     * {@code range_request_timeout} therefore does nothing for this scan -- the only lever this code
     * owns is how much work it asks for per page.
     * <p>
     * And a page of a DISTINCT scan is expensive per row in a way a clustering slice is not: each row
     * is a different partition, so the coordinator seeks the first live row of every one of them
     * across every SSTable the range touches. On a wide time-series table (many SSTables, multi-MB
     * partitions, compressed) that is milliseconds per partition, not microseconds -- measured at
     * ~19ms/partition on a production table of 11.6k tags, i.e. a 5000-row page would need ~95s
     * against a 12s deadline and could never complete, at any table size that matters. 256 keeps a
     * page to a few seconds there, with the deadline an order of magnitude away.
     */
    static final int TAG_PAGE_SIZE = 256;

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=TieredStorage";

    /** Fixed delay between global sweep ticks; each policy-bearing table is still gated by its own {@code interval}. */
    private static final long SWEEP_DELAY_SECONDS = 60;

    /** Process-wide singleton wired up by {@link org.apache.cassandra.service.CassandraDaemon#setup()} via {@link #setup}. */
    public static final TieredStorageService instance = new TieredStorageService();

    /** Per-{@link #runOnce} call counters, also surfaced via the virtual table / {@link #statusRows}. */
    public static class TierRunStats
    {
        public long windowsEncoded;
        public long rowsEncoded;
        public long lateMerges;
        public long chunksExpired;
        public long bytesWritten;
        /**
         * Tags this cycle could not finish -- a read/write timeout, an unavailable replica, an
         * unreadable existing chunk, an over-dense window. Their windows were neither encoded nor
         * deleted, so nothing is lost, but the cycle <b>under-encoded</b>: it is not the "everything
         * is tiered up to the cutoff" outcome a completed cycle reports. A one-shot
         * {@code nodetool retier} that skipped tags fails rather than reporting success (see
         * {@link #retier}); the scheduled sweep logs and retries them next tick.
         * <p>
         * A failed <em>enumeration</em> range counts here too (see {@link #collectTagsIsolated}), as
         * one unit rather than as the tags it would have yielded -- how many those were is precisely
         * what could not be determined. The distinction does not matter to either consumer: both
         * treat any non-zero value as "this cycle under-encoded, look at the log".
         */
        public long tagsSkipped;
    }

    private final AtomicBoolean setupDone = new AtomicBoolean(false);

    /** One entry per table that has ever been run (sweep- or {@link #retier}-triggered); {@code true} while a run is in flight. */
    private final ConcurrentHashMap<String, AtomicBoolean> runningGates = new ConcurrentHashMap<>();
    /** {@code "keyspace.table"} -> epoch millis of that table's last *completed* run. */
    private final ConcurrentHashMap<String, Long> lastRunAtMillisByTable = new ConcurrentHashMap<>();
    /**
     * {@code "keyspace.table"} -> epoch millis of that table's last *attempted* run, completed or
     * not. This, not {@link #lastRunAtMillisByTable}, is what gates the sweep ({@link #dueForSweep}).
     * <p>
     * Gating on completions instead made a failing table ignore its own {@code interval} entirely:
     * the timestamp only advanced after a successful {@link #runOnce}, so a table that throws every
     * time is permanently "never run" and is re-attempted on <b>every</b> {@value #SWEEP_DELAY_SECONDS}s
     * tick. For a failure mode whose cost is a full-table scan -- the expensive case, and the likely
     * one, since cheap work rarely fails -- that turns a table's own trouble into a load source that
     * makes it worse, and starves every table iterated after it. Attempts are what the interval is
     * meant to space out; completions are what {@link #statusRows} reports.
     */
    private final ConcurrentHashMap<String, Long> lastAttemptAtMillisByTable = new ConcurrentHashMap<>();
    /** {@code "keyspace.table"} -> stats from that table's last *completed* run. */
    private final ConcurrentHashMap<String, TierRunStats> lastStatsByTable = new ConcurrentHashMap<>();

    /**
     * Test-only seam: invoked with {@code (keyspace, table)} at the top of every guarded run, i.e.
     * inside the scope whose failures {@link #sweep} must isolate per table (and {@link #retier} must
     * propagate). Lets a test deterministically fail one table's run -- standing in for, say, an
     * {@code UnavailableException} out of {@link #runOnce}'s tag enumeration -- which cannot be
     * provoked naturally on a healthy single-node cluster.
     */
    @VisibleForTesting
    volatile BiConsumer<String, String> preRunHookForTesting;

    /**
     * Hard cap on the rows a single (tag, window) may accumulate in one cycle, pre-checked while
     * still paging so an over-dense window is aborted with an actionable error instead of being
     * fully materialized first.
     * <p>
     * This is a <b>memory</b> budget, not the format's limit ({@link ColumnarChunkCodec#MAX_ROWS} is
     * 16.7M, which the columnar re-encoder could never afford): one window in flight holds the read
     * rows, a {@code TreeMap<Long, ByteBuffer[]>} entry per row, and a {@code ByteBuffer[count]} per
     * regular column, so peak memory scales with rows x (1 + columns), not with rows alone. 200k
     * covers a full day of 1-second data (86,400 rows) and a month of the measured production cadence
     * (24s -> ~112k rows) with room to spare, while keeping the worst case on a wide table to a few
     * hundred MB; {@code chunk_window} bounds a window in time (max 31d), never in rows, so this is
     * the only thing that bounds it at all. Non-final only as a test seam: shrinking it lets a test
     * trigger the abort with a handful of rows; production code must never write it.
     */
    @VisibleForTesting
    volatile int maxSamplesPerWindow = 200_000;

    /**
     * The sweep's own executor, deliberately <b>not</b> {@link ScheduledExecutors#optionalTasks}.
     * That one is single-threaded and shared with hint buffer flushing, key/row cache saving and the
     * auth cache refresh; a re-encode cycle is not a short task -- {@code ChunkTables.ensureChunkTable}
     * alone will wait up to 10s per table for a lagging peer's schema, and every window it encodes is
     * a distributed read plus a write plus a range delete -- so a slow tiered table would stall those
     * unrelated periodic jobs behind it. Non-periodic on shutdown, like {@code optionalTasks}: an
     * in-flight cycle has nothing that must complete (it claims coverage before it writes, so an
     * interrupted cycle only over-states coverage) and draining must not wait for one.
     */
    private static final ScheduledExecutorPlus tieringExecutor =
        ExecutorFactory.Global.executorFactory().scheduled(false, "TieredStorage");

    /**
     * Registers the {@value #MBEAN_NAME} MBean and schedules the single, process-wide sweep
     * ({@link #sweep}) that drives every policy-bearing table's re-encode cycle, on tiering's own
     * executor at a fixed {@value #SWEEP_DELAY_SECONDS}s delay.
     * <p>
     * Idempotent: guarded by {@link #setupDone}, so a second (or later) call is a silent no-op rather
     * than double-registering the MBean or scheduling a second sweep loop -- callers (namely
     * {@link org.apache.cassandra.service.CassandraDaemon#setup()}) do not need to track whether this
     * has already run.
     */
    public void setup()
    {
        if (!setupDone.compareAndSet(false, true))
            return;

        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
        tieringExecutor.scheduleWithFixedDelay(this::sweep, SWEEP_DELAY_SECONDS, SWEEP_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * One global tick: walks every table of every non-system keyspace ({@link Schema#getUserKeyspaces}),
     * and for each with a {@link TieringPolicy} whose {@code interval} has elapsed since its last
     * completed run, invokes the guarded runner ({@link #runGuarded}). A table whose policy fails to
     * parse is retried every tick too (cheaply -- {@link #runOnce} fails fast and logs the specifics)
     * rather than being permanently ignored until an operator notices and fixes it.
     * <p>
     * Each table's run is individually isolated: one table failing (say, an
     * {@link org.apache.cassandra.exceptions.UnavailableException} because its keyspace cannot
     * currently meet the policy's consistency level) must neither abort the tick for every table
     * iterated after it -- the failing table's {@code lastRunAt} never advances, so it would
     * deterministically fail first and starve the rest every tick -- nor escape into the scheduled
     * executor, whose failure wrapper swallows request-failure exceptions without logging.
     */
    @VisibleForTesting
    void sweep()
    {
        long now = Clock.Global.currentTimeMillis();
        for (KeyspaceMetadata keyspace : Schema.instance.getUserKeyspaces())
        {
            for (TableMetadata table : keyspace.tables)
            {
                try
                {
                    if (dueForSweep(keyspace.name, table, now))
                        runGuarded(keyspace.name, table.name, false);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("Tiered storage sweep failed for {}.{}; skipping it this tick and continuing " +
                                "with the remaining tables", keyspace.name, table.name, t);
                }
            }
        }
    }

    private boolean dueForSweep(String keyspace, TableMetadata table, long now)
    {
        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(table);
        }
        catch (ConfigurationException e)
        {
            return true;
        }
        if (policy == null)
            return false;

        Long lastAttempt = lastAttemptAtMillisByTable.get(key(keyspace, table.name));
        return lastAttempt == null || now - lastAttempt >= policy.intervalMillis;
    }

    /**
     * Runs one re-encode cycle for {@code keyspace.table} under that table's per-table overlap gate,
     * recording the result for {@link #statusRows} / the virtual table.
     * <p>
     * {@code operatorRequested} distinguishes {@link #retier} -- a direct, one-shot instruction whose
     * outcome someone is waiting on -- from the scheduled sweep, which gets another chance every tick.
     * It makes two things fail loudly rather than quietly:
     * <ul>
     *     <li>a run already in flight ({@link IllegalStateException}, rather than silently doing
     *     nothing);</li>
     *     <li>a cycle that finished having skipped tags ({@link TierRunStats#tagsSkipped}). Those tags
     *     were not encoded, so the command did not do what it was asked to do; reporting success would
     *     leave an operator believing the table is tiered up to the cutoff when it is not.</li>
     * </ul>
     * The sweep does neither: it logs (see {@link #runOnceInner}) and retries next tick.
     */
    private void runGuarded(String keyspace, String table, boolean operatorRequested)
    {
        String key = key(keyspace, table);
        AtomicBoolean gate = runningGates.computeIfAbsent(key, ignored -> new AtomicBoolean(false));
        if (!gate.compareAndSet(false, true))
        {
            if (operatorRequested)
                throw new IllegalStateException(format("Tiered storage run already in flight for %s.%s", keyspace, table));

            logger.debug("Tiered storage sweep: {}.{} is already running -- skipping this tick", keyspace, table);
            return;
        }

        TierRunStats stats;
        try
        {
            BiConsumer<String, String> hook = preRunHookForTesting;
            if (hook != null)
                hook.accept(keyspace, table);

            stats = runOnce(keyspace, table, Clock.Global.currentTimeMillis());
            lastRunAtMillisByTable.put(key, Clock.Global.currentTimeMillis());
            lastStatsByTable.put(key, stats);
        }
        finally
        {
            // In the finally, so a run that threw still spaces the next attempt by the table's own
            // interval -- see lastAttemptAtMillisByTable for why gating on completions alone turned a
            // failing table into a per-tick full-table scan.
            lastAttemptAtMillisByTable.put(key, Clock.Global.currentTimeMillis());
            gate.set(false);
        }

        if (operatorRequested && stats.tagsSkipped > 0)
            throw new RuntimeException(
                format("Tiered storage run for %s.%s completed with %d tag(s) skipped: those tags were NOT encoded " +
                       "(their source rows are untouched, so nothing is lost, but the table is not tiered up to the " +
                       "cutoff). See this node's log for the per-tag cause, fix it, and run retier again.",
                       keyspace, table, stats.tagsSkipped));
    }

    @Override
    public void retier(String keyspace, String table)
    {
        runGuarded(keyspace, table, true);
    }

    @Override
    public List<String> statusRows()
    {
        List<String> rows = new ArrayList<>();
        for (KeyspaceMetadata keyspace : Schema.instance.getUserKeyspaces())
        {
            for (TableMetadata table : keyspace.tables)
            {
                TieringPolicy policy;
                try
                {
                    policy = TieringPolicy.fromTable(table);
                }
                catch (ConfigurationException e)
                {
                    continue;
                }
                if (policy == null)
                    continue;

                String key = key(keyspace.name, table.name);
                Long lastRun = lastRunAtMillisByTable.get(key);
                TierRunStats stats = lastStatsByTable.get(key);
                rows.add(format("%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d",
                                keyspace.name, table.name, policy.intervalMillis,
                                lastRun == null ? -1L : lastRun,
                                stats == null ? 0L : stats.windowsEncoded,
                                stats == null ? 0L : stats.rowsEncoded,
                                stats == null ? 0L : stats.lateMerges,
                                stats == null ? 0L : stats.chunksExpired,
                                stats == null ? 0L : stats.tagsSkipped));
            }
        }
        return rows;
    }

    /** @return {@code keyspace.table}'s last *completed* run's stats, or {@code null} if it has never run. */
    public TierRunStats lastStats(String keyspace, String table)
    {
        return lastStatsByTable.get(key(keyspace, table));
    }

    /** @return epoch millis of {@code keyspace.table}'s last *completed* run, or {@code null} if it has never run. */
    public Long lastRunAtMillis(String keyspace, String table)
    {
        return lastRunAtMillisByTable.get(key(keyspace, table));
    }

    /**
     * Test-only hook onto the same per-table overlap gate {@link #runGuarded} uses, so a test can hold
     * it open and assert {@link #retier} throws {@link IllegalStateException} -- without needing a real
     * concurrent re-encode cycle in flight.
     *
     * @return {@code true} if the gate was free and is now held; {@code false} if it was already held
     */
    @VisibleForTesting
    boolean acquireGateForTesting(String keyspace, String table)
    {
        AtomicBoolean gate = runningGates.computeIfAbsent(key(keyspace, table), ignored -> new AtomicBoolean(false));
        return gate.compareAndSet(false, true);
    }

    @VisibleForTesting
    void releaseGateForTesting(String keyspace, String table)
    {
        AtomicBoolean gate = runningGates.get(key(keyspace, table));
        if (gate != null)
            gate.set(false);
    }

    private static String key(String keyspace, String table)
    {
        return keyspace + '.' + table;
    }

    /**
     * Runs one re-encode cycle for {@code keyspace.table}. No-ops (returning all-zero stats) if the
     * table has no {@code timeseries_tiering} policy, the policy is invalid, or the table's schema is
     * one tiering cannot support ({@link TieringPolicy#unsupportedSchemaError}) -- in the latter two
     * cases an error is logged rather than failing silently.
     */
    public TierRunStats runOnce(String keyspace, String table, long nowMillis)
    {
        // The re-encoder's own reads must see raw base rows, never the transparent hot+chunk merge -
        // merged reads would make already-encoded windows look live again (idempotency loop).
        TransparentReads.enterInternalBypass();
        try
        {
            return runOnceInner(keyspace, table, nowMillis);
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }

    private TierRunStats runOnceInner(String keyspace, String table, long nowMillis)
    {
        TierRunStats stats = new TierRunStats();

        TableMetadata base = Schema.instance.getTableMetadata(keyspace, table);
        if (base == null)
        {
            logger.warn("Tiered storage runOnce skipped: {}.{} does not exist", keyspace, table);
            return stats;
        }

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(base);
        }
        catch (ConfigurationException e)
        {
            logger.error("Tiered storage runOnce skipped: {}.{} has an invalid timeseries_tiering policy: {}",
                         keyspace, table, e.getMessage());
            return stats;
        }
        if (policy == null)
            return stats;

        String schemaError = TieringPolicy.unsupportedSchemaError(base);
        if (schemaError != null)
        {
            logger.error("Tiered storage runOnce skipped: {}.{} has a timeseries_tiering policy but a schema " +
                         "tiering cannot support: {}", keyspace, table, schemaError);
            return stats;
        }

        if (base.regularColumns().isEmpty())
        {
            // Accepted by the schema check (the timestamp axis is itself data), but unencodable in
            // practice: with no regular column there is no cell writetime anywhere in the table, and
            // the range delete has no timestamp it could safely use. Every window is therefore left
            // untouched below; say so once rather than letting tiering look like it is working.
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, key(keyspace, table) + ":no-regular-columns",
                             1, TimeUnit.HOURS,
                             "Tiered storage: {}.{} has no regular columns, so no cell writetime exists to bound " +
                             "the re-encoder's range delete with -- no window will ever be encoded or deleted. " +
                             "Tiering is a no-op for this table (cold-chunk expiry still runs).", keyspace, table);
        }

        String ttlWarning = TieringPolicy.ttlShadowsHotWindowWarning(base, policy);
        if (ttlWarning != null)
        {
            // Rate-limited: the sweep re-reads the policy every 60s and this condition does not clear
            // itself, so an unthrottled WARN would be one line per table per minute forever.
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, key(keyspace, table) + ":ttl-shadows-hot-window",
                             1, TimeUnit.HOURS, "{}", ttlWarning);
        }

        ChunkTables.ensureChunkTable(base);
        // Re-read the coverage ledger at the top of every cycle. This is also what warms it after a
        // restart -- the global sweep runs a cycle for every policy-bearing table -- so the read path
        // is never left guessing how far cold data reaches just because this process is new.
        ChunkCoverage.refresh(base, policy.consistency);
        String chunkRef = quotedRef(keyspace, ChunkTables.chunkTableName(table));

        List<ColumnMetadata> tagColumns = base.partitionKeyColumns();
        ColumnMetadata tsColumn = base.clusteringColumns().get(0);
        // EVERY regular column is chunked. Columns iterates in name order (BTree-backed and
        // deterministic -- Columns.java), which is also the order ColumnarChunkCodec writes its
        // directory in, so the encoded payload is byte-stable across runs, nodes and JVMs. That
        // stability is what lets a re-run recognise it has nothing new to write (see chunkUnchanged).
        List<ColumnMetadata> valueColumns = new ArrayList<>();
        for (ColumnMetadata column : base.regularColumns())
            valueColumns.add(column);
        int valueCount = valueColumns.size();
        String[] valueRawNames = new String[valueCount];
        String[] writetimeAliases = new String[valueCount];
        int[] valueTypeCodes = new int[valueCount];
        // Paired with the type code, never derived independently of it: the order a column may
        // declare depends on which code carries its bytes (v4 §4), and ChunkColumnTypes.statOrderFor
        // makes that pairing in one place. A type whose comparator the code cannot express (time:
        // unsigned comparator, signed INT64 extrema) declares NONE and forgoes pruning.
        StatOrder[] valueStatOrders = new StatOrder[valueCount];
        String writetimePrefix = writetimeAliasPrefix(base, valueCount);
        for (int c = 0; c < valueCount; c++)
        {
            valueRawNames[c] = valueColumns.get(c).name.toString();
            writetimeAliases[c] = writetimePrefix + c;
            valueTypeCodes[c] = ChunkColumnTypes.typeCodeFor(valueColumns.get(c).type);
            valueStatOrders[c] = ChunkColumnTypes.statOrderFor(valueColumns.get(c).type);
        }

        // Every chunk-table query names the base table's WHOLE partition key: `tagCqlList` for select/
        // insert column lists, `tagPredicate` for the single-partition restriction. With a one-column
        // key both collapse to what they were before composite keys were supported.
        String tagCqlList = columnList(tagColumns);
        String tagPredicate = equalityPredicate(tagColumns);
        String tsCql = tsColumn.name.toCQLString();
        List<String> tagRawNames = rawNames(tagColumns);
        String tsRaw = tsColumn.name.toString();
        String baseRef = base.toString();

        ConsistencyLevel cl = policy.consistency;
        long cutoff = policy.windowStartFor(nowMillis - policy.hotWindowMillis);

        // Reused/grown across every window of every tag this call -- not reallocated per window.
        long[] tsBuf = new long[1024];
        // A window whose rows carry no cell writetime at all cannot be tombstoned safely, so it is
        // left untouched. Expected (a window holding only bare primary-key inserts) rather than
        // exceptional -- summarized once at the end of the run rather than logged per window.
        long[] windowsWithoutWritetime = { 0 };

        // Bounded, per-window queries -- see the class javadoc. "oldest"/"next" only ever return the
        // single row needed to locate the next window with work; the row scan itself is restricted to
        // exactly one [windowStart, windowEnd) range at a time, so memory is bounded by one window's
        // row count, not a tag's entire closed backlog.
        String oldestRowQuery = format("SELECT %s FROM %s WHERE %s AND %s < ? ORDER BY %s ASC LIMIT 1",
                                       tsCql, baseRef, tagPredicate, tsCql, tsCql);
        // Both LIMIT 1 probes say ORDER BY ... ASC explicitly: on a table declared WITH CLUSTERING
        // ORDER BY (ts DESC) -- the dominant time-series idiom -- the default order is newest-first,
        // so an order-less probe would return the NEWEST row in range and the empty-window jump would
        // leap over every window between here and the cutoff instead of landing on the next one.
        String nextRowQuery = format("SELECT %s FROM %s WHERE %s AND %s >= ? AND %s < ? ORDER BY %s ASC LIMIT 1",
                                     tsCql, baseRef, tagPredicate, tsCql, tsCql, tsCql);
        // The timestamp axis, then every regular column, then every regular column's cell writetime
        // under an alias. WRITETIME is per COLUMN, so a multi-column row has one per column and the
        // delete timestamp has to be the maximum over all of them (a row updated column-by-column has
        // as many distinct writetimes as it has columns).
        StringBuilder windowSelection = new StringBuilder(tsCql);
        for (ColumnMetadata column : valueColumns)
            windowSelection.append(", ").append(column.name.toCQLString());
        for (int c = 0; c < valueCount; c++)
            windowSelection.append(", WRITETIME(").append(valueColumns.get(c).name.toCQLString())
                           .append(") AS ").append(writetimeAliases[c]);
        String windowRowsQuery = format("SELECT %s FROM %s WHERE %s AND %s >= ? AND %s < ? ORDER BY %s ASC",
                                        windowSelection, baseRef, tagPredicate, tsCql, tsCql, tsCql);
        String existingChunkQuery = format("SELECT payload, max_row_writetime, WRITETIME(payload) AS chunk_wt " +
                                           "FROM %s WHERE %s AND window_start = ?", chunkRef, tagPredicate);
        String insertChunkQuery = format("INSERT INTO %s (%s, window_start, codec, samples, max_row_writetime, payload) " +
                                         "VALUES (%s, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                                         chunkRef, tagCqlList, bindMarkers(tagColumns.size()));
        String deleteRowsQuery = format("DELETE FROM %s USING TIMESTAMP ? WHERE %s AND %s >= ? AND %s < ?",
                                        baseRef, tagPredicate, tsCql, tsCql);
        String selectExpiredQuery = format("SELECT window_start FROM %s WHERE %s AND window_start < ?",
                                           chunkRef, tagPredicate);
        String deleteExpiredQuery = format("DELETE FROM %s WHERE %s AND window_start < ?", chunkRef, tagPredicate);

        for (List<ByteBuffer> tag : enumerateTags(base, tagCqlList, tagRawNames, baseRef, cl, stats))
        {
            // One tag's worth of trouble -- a corrupt existing chunk, a read/write timeout, anything --
            // must not wedge every other tag on this table out of being re-encoded for good.
            try
            {
                long windowStart = firstClosedWindowStart(oldestRowQuery, tsRaw, tag, cutoff, policy, cl);
                while (windowStart >= 0)
                {
                    // Belt: no window that reaches into (or past) the hot region may ever be fetched,
                    // encoded, or deleted, no matter which path set windowStart -- discovery, the
                    // empty-window jump below, and the dense continuation at the bottom of this loop
                    // all funnel through this one check before doing anything.
                    if (windowStart >= cutoff)
                        break;

                    long windowEnd = windowStart + policy.chunkWindowMillis;
                    // Suspenders: windowStart is always chunk-window-aligned (windowStartFor's output,
                    // or windowStart+chunkWindowMillis below, which preserves alignment) and so is
                    // cutoff, so windowEnd <= cutoff is already implied by the guard just above -- this
                    // clamp is redundant given that invariant, but applied anyway to the two operations
                    // that actually touch hot data (this read, and the delete below), as a second,
                    // independent guard against the exact class of bug the belt above exists to fix.
                    long readEnd = Math.min(windowEnd, cutoff);
                    int maxSamples = maxSamplesPerWindow;
                    List<UntypedResultSet.Row> windowRows = pagedSelect(windowRowsQuery, cl, boundTo(tag,
                            TimestampType.instance.fromTimeInMillis(windowStart),
                            TimestampType.instance.fromTimeInMillis(readEnd)),
                            maxSamples);
                    if (windowRows.size() > maxSamples)
                    {
                        // Paging stopped as soon as the count crossed the cap (see pagedSelect), so
                        // memory stayed bounded -- but this window can never be encoded as configured.
                        // Encoding a partial window would delete rows the chunk doesn't contain, and
                        // retrying the full read every cycle would wedge this tag forever, so abort the
                        // tag's walk until an operator shrinks chunk_window.
                        logger.error("Tiered storage runOnce: {}.{} tag {} window [{}, {}) holds more than {} rows, " +
                                     "over the {}-sample per-chunk codec limit -- it cannot be encoded. Lower the " +
                                     "table's timeseries_tiering chunk_window so one window holds at most {} samples; " +
                                     "skipping this tag until then", keyspace, table, describeTag(tagColumns, tag),
                                     windowStart, readEnd, maxSamples, maxSamples, maxSamples);
                        stats.tagsSkipped++;
                        break;
                    }

                    if (windowRows.isEmpty())
                    {
                        // Nothing in this window (raced with a concurrent process, or the oldest-row probe
                        // landed on a sparse tag) -- jump straight to the next window that has data instead
                        // of stepping through potentially many empty windows one at a time.
                        windowStart = nextClosedWindowStart(nextRowQuery, tsRaw, tag, windowEnd, cutoff, policy, cl);
                        continue;
                    }

                    UntypedResultSet existingRs = QueryProcessor.process(existingChunkQuery, cl,
                            boundTo(tag, TimestampType.instance.fromTimeInMillis(windowStart)));
                    UntypedResultSet.Row existingRow = (existingRs == null || existingRs.isEmpty()) ? null : existingRs.one();

                    // ts -> one slot per regular column, in valueColumns order; a null slot is a null
                    // cell and stays null all the way into the chunk (presence is encoded per column).
                    TreeMap<Long, ByteBuffer[]> merged = new TreeMap<>();
                    long maxWt = Long.MIN_VALUE;
                    long existingChunkWt = Long.MIN_VALUE;
                    ByteBuffer existingPayload = null;
                    // Whether maxWt is a real writetime rather than the Long.MIN_VALUE sentinel. It is
                    // the precondition for issuing the range delete at all.
                    boolean haveWritetime = false;
                    if (existingRow != null)
                    {
                        existingPayload = existingRow.getBytes("payload");
                        ColumnarCursor cursor = ColumnarChunkCodec.cursor(existingPayload, null);
                        while (cursor.advance())
                        {
                            ByteBuffer[] values = new ByteBuffer[valueCount];
                            for (int c = 0; c < valueCount; c++)
                                // null for a column this chunk does not carry (ADDed to the table after
                                // the chunk was written); a column the chunk carries but the table has
                                // since DROPped is simply never asked for, so it drops out here.
                                values[c] = cursor.getBytes(valueRawNames[c]);
                            merged.put(cursor.timestamp(), values);
                        }
                        maxWt = existingRow.getLong("max_row_writetime");
                        existingChunkWt = existingRow.getLong("chunk_wt");
                        haveWritetime = true;
                    }

                    int rowsThisWindow = 0;
                    for (UntypedResultSet.Row row : windowRows)
                    {
                        long rowTs = row.getTimestamp(tsRaw).getTime();
                        // PER-COLUMN merge, not row-level replace: a base row that reappears at a
                        // timestamp the chunk already holds is a partial update of that stored row
                        // (`UPDATE t SET quality = ? WHERE ...` writes ONE cell and leaves the others
                        // alone -- exactly Cassandra's own per-cell last-write-wins). Replacing the
                        // whole row would blank every column the update did not mention.
                        ByteBuffer[] values = merged.get(rowTs);
                        if (values == null)
                        {
                            values = new ByteBuffer[valueCount];
                            merged.put(rowTs, values);
                        }
                        for (int c = 0; c < valueCount; c++)
                        {
                            // A live cell wins over whatever the chunk held; a null one leaves the
                            // chunk's value in place (see above). row.has() is "the cell is present",
                            // and an empty-but-present value (e.g. text '') counts as present.
                            if (row.has(valueRawNames[c]))
                                values[c] = row.getBytes(valueRawNames[c]);
                            // Every column's writetime feeds the maximum -- the delete below must not
                            // outrun the newest cell of any column of any row it is about to destroy.
                            if (row.has(writetimeAliases[c]))
                            {
                                maxWt = Math.max(maxWt, row.getLong(writetimeAliases[c]));
                                haveWritetime = true;
                            }
                        }
                        // Rows with every regular column null (a bare primary-key insert, or a row whose
                        // cells were all deleted/TTL'd) are ENCODED, not skipped: the row exists, the
                        // range delete would take it, and a chunk that omitted it would be silent data
                        // loss. It contributes no writetime, which is why haveWritetime is tracked
                        // separately from "the window had rows".
                        rowsThisWindow++;
                    }

                    if (!haveWritetime)
                    {
                        // Not one cell writetime anywhere in this window, and no prior chunk to inherit
                        // one from: there is no timestamp the range delete could use that is provably
                        // not newer than some row it would destroy. Leave the window entirely alone --
                        // nothing encoded, nothing deleted -- and retry (cheaply) on a future cycle.
                        windowsWithoutWritetime[0]++;
                        windowStart = windowEnd;
                        continue;
                    }

                    int count = merged.size();
                    if (count > maxSamples)
                    {
                        // The window's own rows fit the cap, but merged with the existing chunk's
                        // samples (disjoint late-row timestamps) the total doesn't -- encode would
                        // throw, be caught by the per-tag handler, and retry identically forever.
                        logger.error("Tiered storage runOnce: {}.{} tag {} window [{}, {}) merges to {} samples, " +
                                     "over the {}-sample per-chunk codec limit -- it cannot be encoded. Lower the " +
                                     "table's timeseries_tiering chunk_window so one window holds at most {} samples; " +
                                     "skipping this tag until then", keyspace, table, describeTag(tagColumns, tag),
                                     windowStart, readEnd, count, maxSamples, maxSamples);
                        stats.tagsSkipped++;
                        break;
                    }
                    if (tsBuf.length < count)
                    {
                        int newLength = tsBuf.length;
                        while (newLength < count)
                            newLength *= 2;
                        tsBuf = Arrays.copyOf(tsBuf, newLength);
                    }
                    ByteBuffer[][] columnValues = new ByteBuffer[valueCount][count];
                    int idx = 0;
                    for (Map.Entry<Long, ByteBuffer[]> sample : merged.entrySet())
                    {
                        tsBuf[idx] = sample.getKey();
                        ByteBuffer[] values = sample.getValue();
                        for (int c = 0; c < valueCount; c++)
                            columnValues[c][idx] = values[c];
                        idx++;
                    }
                    SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
                    for (int c = 0; c < valueCount; c++)
                        columns.put(valueRawNames[c],
                                    new ChunkV4Codec.ColumnInput(valueTypeCodes[c], valueStatOrders[c],
                                                                 columnValues[c]));

                    // BEFORE the chunk is written and the source rows are deleted: the read path's
                    // fast path is driven by this ledger, so it has to be at least as wide as the
                    // chunk table at every instant. Widening it first means a crash in between only
                    // over-states coverage (an unnecessary chunk read); the reverse order would leave
                    // a chunk whose base rows are gone and which the fast path does not know to look
                    // for. Claimed on every window, not only on windows that write a chunk, so a lost
                    // or truncated ledger is rebuilt by the next cycle over already-encoded data.
                    ChunkCoverage.claim(base, cl, windowStart, cutoff, policy.chunkWindowMillis);

                    ByteBuffer payload = ColumnarChunkCodec.encode(tsBuf, count, columns);
                    // Read the version byte back out of the payload rather than naming a codec
                    // constant here: the `codec` column must describe what was actually written, so
                    // it stays honest if the encode path ever changes underneath this call.
                    byte codecByte = payload.get(payload.position());

                    // The encoding is deterministic, so identical content encodes to identical bytes:
                    // if the stored chunk already IS what we just built, and it was built from the same
                    // maximum writetime, re-writing it would only bump its own write timestamp. Skip the
                    // write (the delete below still runs -- this is also what completes an interrupted
                    // cycle that wrote the chunk and died before the delete).
                    boolean chunkUnchanged = existingPayload != null
                                             && maxWt == existingRow.getLong("max_row_writetime")
                                             && payload.equals(existingPayload);

                    if (!chunkUnchanged)
                    {
                        // Always write strictly after both the rows just encoded AND the chunk row being
                        // replaced -- guards a crash-then-backfill corner where maxWt+1 could otherwise
                        // land exactly on the existing chunk's own write timestamp and tie (same-timestamp
                        // writes to different columns of the same row can resolve per-column, tearing the
                        // chunk).
                        long insertTs = Math.max(maxWt + 1, existingChunkWt + 1);

                        QueryProcessor.process(insertChunkQuery, cl, boundTo(tag,
                                TimestampType.instance.fromTimeInMillis(windowStart),
                                ByteType.instance.decompose(codecByte),
                                Int32Type.instance.decompose(count),
                                LongType.instance.decompose(maxWt),
                                payload,
                                LongType.instance.decompose(insertTs)));
                    }

                    // The USING TIMESTAMP marker precedes the WHERE clause, so this is the one query
                    // whose tag values are not the leading binds.
                    List<ByteBuffer> deleteValues = new ArrayList<>(tag.size() + 3);
                    deleteValues.add(LongType.instance.decompose(maxWt));
                    deleteValues.addAll(tag);
                    deleteValues.add(TimestampType.instance.fromTimeInMillis(windowStart));
                    deleteValues.add(TimestampType.instance.fromTimeInMillis(readEnd));
                    QueryProcessor.process(deleteRowsQuery, cl, deleteValues);

                    // Counted only when a chunk was actually written -- a suppressed re-write encoded
                    // nothing new, and reporting it would make an idle table look like it is churning.
                    if (!chunkUnchanged)
                    {
                        stats.windowsEncoded++;
                        stats.rowsEncoded += rowsThisWindow;
                        if (existingRow != null)
                            stats.lateMerges++;
                        stats.bytesWritten += payload.remaining();
                    }

                    windowStart = windowEnd;
                }

            }
            catch (UnsupportedChunkFormatException e)
            {
                // The tag's existing chunk was written by a build whose chunk format this one cannot
                // read, so the late-merge read of it failed. Retrying achieves nothing -- unlike the
                // generic failure below, this never fixes itself -- and the tag is now permanently
                // stuck, so say what has to happen instead of implying a retry will help. Skipping is
                // still the safe response: no rows are deleted, so nothing further is lost.
                stats.tagsSkipped++;
                logger.error("Tiered storage runOnce: {}.{} cannot re-encode tag {}: its existing chunk was written " +
                             "by an older build and is unreadable ({}). Retrying will not fix this -- drop {} and " +
                             "let tiering re-run; that data is not recoverable. Source rows are left untouched.",
                             keyspace, table, describeTag(tagColumns, tag), e.getMessage(),
                             ChunkTables.chunkTableName(table));
            }
            catch (RuntimeException e)
            {
                // Counted, not just logged. This catch turns any transient distributed failure -- an
                // unavailable replica, a read timeout, a schema that has not propagated yet -- into a
                // tag that this cycle simply did not do; without the counter the cycle returns
                // all-clear stats and a one-shot `nodetool retier` reports success having under-
                // encoded an arbitrary subset of the table. An availability failure must not be
                // reported as success.
                stats.tagsSkipped++;
                logger.error("Tiered storage runOnce: {}.{} failed while re-encoding tag {} -- skipping to the " +
                             "next tag; this tag will be retried next cycle", keyspace, table,
                             describeTag(tagColumns, tag), e);
            }
        }

        if (windowsWithoutWritetime[0] > 0)
        {
            logger.warn("Tiered storage runOnce: {}.{} left {} closed window(s) untouched: no row in them carries a " +
                        "cell writetime (every regular column is null on every row -- bare key inserts, deleted " +
                        "values, or expired TTLs), so there is no timestamp the source-row delete could safely use",
                        keyspace, table, windowsWithoutWritetime[0]);
        }

        // Cold expiry enumerates tags from the CHUNK table, not the base table (SP3 R6): a tag whose
        // base rows were all re-encoded (or TTL'd) away no longer appears in the base DISTINCT scan,
        // but its cold chunks must still expire.
        if (policy.coldWindowMillis >= 0)
        {
            TableMetadata chunkMeta = Schema.instance.getTableMetadata(keyspace, ChunkTables.chunkTableName(table));
            if (chunkMeta != null)
            {
                for (List<ByteBuffer> tag : enumerateTags(chunkMeta, tagCqlList, tagRawNames, chunkRef, cl, stats))
                {
                    try
                    {
                        List<ByteBuffer> coldValues = boundTo(
                                tag, TimestampType.instance.fromTimeInMillis(nowMillis - policy.coldWindowMillis));
                        List<UntypedResultSet.Row> expired = pagedSelect(selectExpiredQuery, cl, coldValues);
                        if (!expired.isEmpty())
                        {
                            stats.chunksExpired += expired.size();
                            QueryProcessor.process(deleteExpiredQuery, cl, coldValues);
                        }
                    }
                    catch (RuntimeException e)
                    {
                        stats.tagsSkipped++;
                        logger.error("Tiered storage runOnce: {}.{} failed while expiring cold chunks of tag {} -- " +
                                     "skipping to the next tag; retried next cycle", keyspace, table,
                                     describeTag(tagColumns, tag), e);
                    }
                }
            }
        }

        if (stats.tagsSkipped > 0)
        {
            logger.warn("Tiered storage runOnce: {}.{} finished with {} tag(s) skipped -- this cycle did NOT do " +
                        "everything it was asked to. Their source rows are untouched (nothing is lost) and the " +
                        "scheduled sweep will retry them; the per-tag causes are logged above.",
                        keyspace, table, stats.tagsSkipped);
        }

        return stats;
    }

    /**
     * Picks the prefix for the {@code WRITETIME(col) AS <prefix><i>} aliases the window-rows query
     * uses, such that none of the {@code count} aliases collides with a real column name of
     * {@code base} -- a collision would make {@code UntypedResultSet.Row} lookups by name ambiguous
     * and silently mix a column's value up with a writetime. Grows the prefix (rather than the index)
     * so the aliases stay a contiguous {@code prefix0..prefixN-1} block.
     */
    private static String writetimeAliasPrefix(TableMetadata base, int count)
    {
        Set<String> taken = new HashSet<>();
        for (ColumnMetadata column : base.columnsInFixedOrder())
            taken.add(column.name.toString());

        String prefix = "wt_";
        while (collides(prefix, count, taken))
            prefix = '_' + prefix;
        return prefix;
    }

    private static boolean collides(String prefix, int count, Set<String> taken)
    {
        for (int i = 0; i < count; i++)
            if (taken.contains(prefix + i))
                return true;
        return false;
    }

    /** @return {@code "a, b, c"} -- the columns' CQL names, for a select/insert column list. */
    private static String columnList(Iterable<ColumnMetadata> columns)
    {
        StringBuilder sb = new StringBuilder();
        for (ColumnMetadata column : columns)
        {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(column.name.toCQLString());
        }
        return sb.toString();
    }

    /** @return {@code "a = ? AND b = ?"} -- the single-partition restriction for a whole partition key. */
    private static String equalityPredicate(List<ColumnMetadata> columns)
    {
        StringBuilder sb = new StringBuilder();
        for (ColumnMetadata column : columns)
        {
            if (sb.length() > 0)
                sb.append(" AND ");
            sb.append(column.name.toCQLString()).append(" = ?");
        }
        return sb.toString();
    }

    /** @return {@code "?, ?, ?"} -- {@code count} positional bind markers. */
    private static String bindMarkers(int count)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append('?');
        }
        return sb.toString();
    }

    private static List<String> rawNames(List<ColumnMetadata> columns)
    {
        List<String> names = new ArrayList<>(columns.size());
        for (ColumnMetadata column : columns)
            names.add(column.name.toString());
        return names;
    }

    /** @return the bind values for a query whose leading markers are the partition key's, followed by {@code rest}. */
    private static List<ByteBuffer> boundTo(List<ByteBuffer> tag, ByteBuffer... rest)
    {
        List<ByteBuffer> values = new ArrayList<>(tag.size() + rest.length);
        values.addAll(tag);
        Collections.addAll(values, rest);
        return values;
    }

    /** @return a human-readable rendering of one partition key's values, for log messages. */
    private static String describeTag(List<ColumnMetadata> tagColumns, List<ByteBuffer> tag)
    {
        if (tagColumns.size() == 1)
            return tagColumns.get(0).type.getString(tag.get(0));

        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < tagColumns.size(); i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(tagColumns.get(i).type.getString(tag.get(i)));
        }
        return sb.append(')').toString();
    }

    /** @return the {@code window_start} of the oldest closed (ts &lt; cutoff) row for {@code tag}, or -1 if none. */
    private static long firstClosedWindowStart(String oldestRowQuery, String tsRaw, List<ByteBuffer> tag, long cutoff,
                                                TieringPolicy policy, ConsistencyLevel cl)
    {
        UntypedResultSet rs = QueryProcessor.process(oldestRowQuery, cl,
                boundTo(tag, TimestampType.instance.fromTimeInMillis(cutoff)));
        if (rs == null || rs.isEmpty())
            return -1;
        return policy.windowStartFor(rs.one().getTimestamp(tsRaw).getTime());
    }

    /**
     * @return the {@code window_start} of the next closed row for {@code tag} with {@code ts} in
     * {@code [fromTsMillis, cutoff)}, or -1 if there is none -- used to skip past a run of empty
     * windows in one query instead of probing them one at a time.
     */
    private static long nextClosedWindowStart(String nextRowQuery, String tsRaw, List<ByteBuffer> tag, long fromTsMillis,
                                               long cutoff, TieringPolicy policy, ConsistencyLevel cl)
    {
        UntypedResultSet rs = QueryProcessor.process(nextRowQuery, cl, boundTo(
                tag, TimestampType.instance.fromTimeInMillis(fromTsMillis), TimestampType.instance.fromTimeInMillis(cutoff)));
        if (rs == null || rs.isEmpty())
            return -1;
        return policy.windowStartFor(rs.one().getTimestamp(tsRaw).getTime());
    }

    /**
     * Enumerates the distinct partition keys ("tags") of {@code base} restricted to this node's local
     * primary token ranges ({@link StorageService#getPrimaryRanges}), so a multi-node cluster's
     * re-encoders partition the tag space instead of every node redundantly re-encoding every tag.
     * <p>
     * A primary range can wrap the ring's zero point (e.g. a single-node cluster's sole range, which
     * is degenerate: {@code (t, t]} covering the whole ring); {@link Range#unwrap()} normalizes that
     * into 1-2 non-wrapping sub-ranges, each turned into its own bound {@code token(tag) > ? AND <= ?}
     * restriction (a bound equal to the partitioner's minimum token is omitted -- it is a sentinel,
     * not a real, ownable token). Bind values are encoded via the partitioner's own
     * {@link org.apache.cassandra.dht.IPartitioner#getTokenValidator()} type, so this works for any
     * partitioner's token representation (a raw {@code long} for Murmur3, a {@code BigInteger} for
     * RandomPartitioner, etc.), not just one whose {@code Token.toString()} happens to be a bare CQL
     * numeric literal. If {@code getPrimaryRanges} itself reports no ranges at all -- which should not
     * happen once a node has joined the ring, but is treated defensively rather than silently scanning
     * nothing -- this falls back to an unrestricted scan of every tag.
     */
    private static List<List<ByteBuffer>> enumerateTags(TableMetadata base, String tagCqlList, List<String> tagRawNames,
                                                        String baseRef, ConsistencyLevel cl, TierRunStats stats)
    {
        // LinkedHashSet of List<ByteBuffer>: List's equals/hashCode are the element-wise ones, so a
        // composite key deduplicates on all of its columns, not on identity.
        Set<List<ByteBuffer>> tags = new LinkedHashSet<>();
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRanges(base.keyspace);

        if (primaryRanges.isEmpty())
        {
            logger.warn("Tiered storage: {}.{} has no local primary ranges reported for this node; falling back " +
                        "to an unrestricted tag scan rather than silently skipping data", base.keyspace, base.name);
            collectTagsIsolated(tags, format("SELECT DISTINCT %s FROM %s", tagCqlList, baseRef), tagRawNames, cl,
                                Collections.emptyList(), base, "the whole ring", stats);
            return new ArrayList<>(tags);
        }

        AbstractType<?> tokenType = base.partitioner.getTokenValidator();
        for (Range<Token> range : primaryRanges)
        {
            for (Range<Token> sub : range.unwrap())
            {
                List<ByteBuffer> values = new ArrayList<>(2);
                String query = tagRangeQuery(tagCqlList, baseRef, sub, tokenType, values);
                collectTagsIsolated(tags, query, tagRawNames, cl, values, base, sub.toString(), stats);
            }
        }

        return new ArrayList<>(tags);
    }

    /**
     * {@link #collectTags} with one token range's failure confined to that range.
     * <p>
     * Enumeration is the one step of a cycle that used to have no isolation at all: every per-tag
     * failure is caught and counted (see {@link #runOnceInner}), but a single range scan throwing --
     * a read timeout, an unavailable replica -- propagated out of the whole {@code runOnce}, so the
     * ranges after it were never even attempted and the table encoded nothing. That is not a
     * hypothetical: a table whose DISTINCT scan cannot finish inside the read deadline fails on the
     * same range every tick, forever, and its chunk table stays empty while the log shows only a
     * generic per-table sweep warning.
     * <p>
     * A range that fails is counted in {@link TierRunStats#tagsSkipped} -- an unknown, non-zero
     * number of tags went unenumerated, so the cycle emphatically did not do everything it was asked
     * to, and {@code nodetool retier} must fail rather than report success (see {@link #runGuarded}).
     */
    private static void collectTagsIsolated(Set<List<ByteBuffer>> out, String query, List<String> tagRawNames,
                                            ConsistencyLevel cl, List<ByteBuffer> values, TableMetadata base,
                                            String rangeDescription, TierRunStats stats)
    {
        try
        {
            collectTags(out, query, tagRawNames, cl, values);
        }
        catch (RuntimeException e)
        {
            stats.tagsSkipped++;
            logger.error("Tiered storage: {}.{} failed to enumerate tags over token range {} -- the tags in that " +
                         "range are not re-encoded this cycle; continuing with the remaining ranges and retrying " +
                         "next cycle", base.keyspace, base.name, rangeDescription, e);
        }
    }

    /**
     * Builds the token-range restricted tag query, appending its bind values (0-2) to {@code valuesOut}
     * in order. {@code tagCqlList} is the whole partition key, so the restriction is
     * {@code token(a, b) > ?} for a composite key -- the token is computed from all of its columns.
     */
    private static String tagRangeQuery(String tagCqlList, String baseRef, Range<Token> sub, AbstractType<?> tokenType,
                                        List<ByteBuffer> valuesOut)
    {
        boolean hasLower = !sub.left.isMinimum();
        boolean hasUpper = !sub.right.isMinimum();

        StringBuilder query = new StringBuilder("SELECT DISTINCT ").append(tagCqlList).append(" FROM ").append(baseRef);
        if (hasLower || hasUpper)
        {
            query.append(" WHERE ");
            if (hasLower)
            {
                query.append("token(").append(tagCqlList).append(") > ?");
                valuesOut.add(tokenType.decomposeUntyped(sub.left.getTokenValue()));
            }
            if (hasLower && hasUpper)
                query.append(" AND ");
            if (hasUpper)
            {
                query.append("token(").append(tagCqlList).append(") <= ?");
                valuesOut.add(tokenType.decomposeUntyped(sub.right.getTokenValue()));
            }
        }
        return query.toString();
    }

    private static void collectTags(Set<List<ByteBuffer>> out, String query, List<String> tagRawNames,
                                     ConsistencyLevel cl, List<ByteBuffer> values)
    {
        for (UntypedResultSet.Row row : pagedSelect(query, cl, values, Integer.MAX_VALUE, TAG_PAGE_SIZE))
        {
            List<ByteBuffer> tag = new ArrayList<>(tagRawNames.size());
            for (String name : tagRawNames)
                tag.add(row.getBytes(name));
            out.add(tag);
        }
    }

    private static String quotedRef(String keyspace, String table)
    {
        return format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(table));
    }

    /**
     * Runs {@code query} to completion across as many pages as needed (network page size
     * {@link #PAGE_SIZE}), via {@link QueryProcessor#instance} directly rather than the static
     * {@link QueryProcessor#process(String, ConsistencyLevel, List)} convenience method, since that
     * overload has no way to carry a {@link PagingState} between calls. Only ever used for queries
     * already bounded to at most one tag/one window/one candidate-list -- never for an unbounded
     * per-tag row scan (see the class javadoc). {@link ChunkRowSource} shares it for the read path's
     * chunk-window listing, which is bounded the same way (one tag, one time range, no payloads).
     */
    static List<UntypedResultSet.Row> pagedSelect(String query, ConsistencyLevel cl, List<ByteBuffer> values)
    {
        return pagedSelect(query, cl, values, Integer.MAX_VALUE);
    }

    /**
     * As {@link #pagedSelect(String, ConsistencyLevel, List)}, but yields rows as it pages instead of
     * collecting them: the next page is fetched only when the current one is exhausted, so a consumer
     * that stops early never pays for the rest of the scan. This is the read-path counterpart of the
     * eager version — see {@code ChunkRowSource.windows}, where materializing every window before the
     * first payload read was the whole cost of an unbounded {@code LIMIT 1}.
     *
     * <p>Exceptions surface from {@code hasNext()}/{@code next()} at the page boundary that failed,
     * not from this call; the caller wraps them.
     */
    static Iterator<UntypedResultSet.Row> pagedSelectLazy(String query, ConsistencyLevel cl, List<ByteBuffer> values)
    {
        QueryState queryState = QueryState.forInternalCalls();
        return new AbstractIterator<UntypedResultSet.Row>()
        {
            private PagingState pagingState = null;
            private boolean exhausted = false;
            private Iterator<UntypedResultSet.Row> page = Collections.emptyIterator();

            @Override
            protected UntypedResultSet.Row computeNext()
            {
                while (!page.hasNext())
                {
                    if (exhausted)
                        return endOfData();

                    QueryOptions options = QueryOptions.create(cl, values, false, PAGE_SIZE, pagingState, null,
                                                               ProtocolVersion.CURRENT, null);
                    CQLStatement statement = QueryProcessor.instance.parse(query, queryState, options);
                    ResultMessage result = QueryProcessor.instance.process(statement, queryState, options,
                                                                           Dispatcher.RequestTime.forImmediateExecution());
                    if (!(result instanceof ResultMessage.Rows))
                        return endOfData();

                    ResultSet resultSet = ((ResultMessage.Rows) result).result;
                    page = UntypedResultSet.create(resultSet).iterator();
                    pagingState = resultSet.metadata.getPagingState();
                    exhausted = pagingState == null;
                }
                return page.next();
            }
        };
    }

    /**
     * As {@link #pagedSelect(String, ConsistencyLevel, List)}, but stops fetching further pages as soon
     * as more than {@code maxRows} rows have accumulated, returning what it has (at most one page over
     * the cap). Callers that pass a real cap must treat {@code result.size() > maxRows} as "the scan
     * overflowed, and the result is truncated" -- see the {@code maxSamplesPerWindow} guard in
     * {@link #runOnce} -- so an over-dense window is detected mid-paging with bounded memory, never
     * fully materialized.
     */
    private static List<UntypedResultSet.Row> pagedSelect(String query, ConsistencyLevel cl, List<ByteBuffer> values,
                                                          int maxRows)
    {
        return pagedSelect(query, cl, values, maxRows, PAGE_SIZE);
    }

    /**
     * As {@link #pagedSelect(String, ConsistencyLevel, List, int)}, but with the network page size
     * named by the caller rather than fixed at {@link #PAGE_SIZE} -- see {@link #TAG_PAGE_SIZE} for
     * why a DISTINCT partition-key scan cannot afford the default.
     */
    private static List<UntypedResultSet.Row> pagedSelect(String query, ConsistencyLevel cl, List<ByteBuffer> values,
                                                          int maxRows, int pageSize)
    {
        List<UntypedResultSet.Row> rows = new ArrayList<>();
        QueryState queryState = QueryState.forInternalCalls();
        PagingState pagingState = null;
        while (true)
        {
            QueryOptions options = QueryOptions.create(cl, values, false, pageSize, pagingState, null,
                                                        ProtocolVersion.CURRENT, null);
            CQLStatement statement = QueryProcessor.instance.parse(query, queryState, options);
            ResultMessage result = QueryProcessor.instance.process(statement, queryState, options,
                                                                    Dispatcher.RequestTime.forImmediateExecution());
            if (!(result instanceof ResultMessage.Rows))
                break;

            ResultSet resultSet = ((ResultMessage.Rows) result).result;
            for (UntypedResultSet.Row row : UntypedResultSet.create(resultSet))
                rows.add(row);

            if (rows.size() > maxRows)
                break;

            pagingState = resultSet.metadata.getPagingState();
            if (pagingState == null)
                break;
        }
        return rows;
    }
}
