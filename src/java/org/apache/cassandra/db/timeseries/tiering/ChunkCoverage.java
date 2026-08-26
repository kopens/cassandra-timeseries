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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * What the {@code __chunks} shadow table <b>actually contains</b>, per base table: how far up the
 * time axis encoded data reaches, how far down it starts, and the widest {@code chunk_window} any
 * existing chunk was written with.
 *
 * <p><b>Why this exists.</b> Tiering deletes the base rows of every window it encodes, so a chunk is
 * the only copy of that data. The read path therefore has exactly one correct question to ask before
 * it decides to skip the chunk merge: <em>do chunks exist at or below this query's timestamps?</em>
 * Asking the <em>current</em> {@link TieringPolicy} instead -- "does the policy call this cold?" --
 * makes three ordinary tuning actions silently hide already-encoded history:
 * <ul>
 *     <li>raising {@code hot_window} moves the hot boundary back past data that was encoded under the
 *     old, shorter window, so queries over that span take the hot-only fast path and see nothing;</li>
 *     <li>dropping the {@code timeseries_tiering} extension makes <em>all</em> cold history vanish
 *     from every {@code SELECT};</li>
 *     <li>shrinking {@code chunk_window} makes the merge's look-back too short to find wider legacy
 *     chunks, whose {@code window_start} sits further before the query range than one current window.</li>
 * </ul>
 * All three are answered by consulting real coverage rather than the live policy.
 *
 * <p><b>Where the numbers come from.</b> They cannot be derived from the chunk table cheaply -- there
 * is no global index on {@code window_start}, so "the highest window ever encoded" would be a full
 * scan of a table designed to hold a decade of data. They are instead maintained as a tiny ledger
 * table beside the chunk table ({@link ChunkTables#coverageTableName}), written by the re-encoder
 * <b>before</b> it inserts a chunk (see {@link #claim}), so a crash between the two can only leave
 * coverage <em>wider</em> than reality -- which costs an unnecessary chunk read, never a wrong answer.
 * One row per node, aggregated by the reader (min/max/max), so a node with a stale view cannot
 * regress another node's claim the way a single last-write-wins row would.
 *
 * <p><b>Cost.</b> Consulted on every read of every table, so it is a cache lookup: one
 * {@link ConcurrentHashMap} hit keyed by {@link TableId}, refreshed at most every
 * {@value #REFRESH_SECONDS}s (a single-partition read of at most one row per node, or -- for the
 * overwhelmingly common case of a table that has no chunk table at all -- a schema lookup).
 *
 * <p><b>Staleness is safe by construction.</b> A stale-low coverage top can only under-state where
 * chunks reach, and {@link TransparentReads} takes {@code max(coverageTop, hotBoundary)}: anything
 * the re-encoder has just written is by definition below the current hot boundary, so it is merged
 * regardless of when this node last refreshed. The ledger is what keeps the answer right after the
 * hot boundary itself moves.
 */
public final class ChunkCoverage
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkCoverage.class);

    /**
     * The ledger's single partition key value. The ledger describes one thing (this base table's
     * chunk table), so it is one partition; the per-node rows are its clustering.
     */
    static final String SCOPE = "chunks";

    /**
     * The oldest {@code window_start} this node has encoded. Recorded and aggregated, but deliberately
     * <b>not</b> consulted by the read path: a symmetric "query ends below the oldest chunk, skip the
     * merge" fast path has no equivalent of the hot-boundary floor that makes the top one safe when
     * coverage is stale, so a node whose cache predates a backfill (or a {@code retier} over historical
     * data) would silently omit the newly encoded windows for up to {@value #REFRESH_SECONDS}s.
     */
    static final String MIN_WINDOW_START = "min_window_start";
    static final String MAX_WINDOW_START = "max_window_start";
    static final String MAX_CHUNK_WINDOW = "max_chunk_window";
    static final String SCOPE_COLUMN = "scope";
    static final String NODE_COLUMN = "node";

    /**
     * The consistency level to read or write the ledger at when the caller does not name a
     * quorum-strength one -- see {@link #ledgerConsistency}. Deliberately not the node-local internal
     * path: the ledger describes the whole cluster's chunks, and a node that holds no replica of its
     * partition would read it as empty. Matches {@link TieringPolicy}'s own documented
     * {@code consistency} default.
     */
    static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;

    private static final long REFRESH_SECONDS = 60;
    private static final long REFRESH_MILLIS = TimeUnit.SECONDS.toMillis(REFRESH_SECONDS);

    /**
     * How long an {@link Coverage#UNKNOWN} answer is remembered. Short, because it is a fault state
     * that should be re-probed soon -- but not zero: an unreadable ledger with no negative cache at
     * all means <em>every</em> read and every tombstone-bearing write of the table issues its own
     * ledger read, each blocking a request thread for up to {@code read_request_timeout}. That turns
     * a degraded ledger into node-wide request-thread exhaustion, which is a far bigger outage than
     * the one being reported. Both consequences of a cached UNKNOWN are the safe ones (reads merge
     * chunks unconditionally, deletes are refused), so paying them for a few seconds is cheap.
     */
    private static final long UNKNOWN_REFRESH_MILLIS = TimeUnit.SECONDS.toMillis(5);

    /** Bounded like {@code TieringPolicy.PARSED}: cleared wholesale rather than grown without limit. */
    private static final int MAX_CACHED_TABLES = 4096;

    private static final ConcurrentHashMap<TableId, Entry> CACHE = new ConcurrentHashMap<>();

    /**
     * One monitor per table, held across a ledger load so concurrent requests for the same table
     * coalesce onto one read instead of each starting their own -- see {@link #forTable}.
     */
    private static final ConcurrentHashMap<TableId, Object> LOADING = new ConcurrentHashMap<>();

    private ChunkCoverage()
    {
    }

    /**
     * A snapshot of what the chunk table holds. Four distinguishable states, because the read path
     * has to treat them differently:
     * <ul>
     *     <li><b>no chunk table</b> -- nothing has ever been encoded for this table, provably: skip
     *     the merge entirely, at zero cost. This is every non-tiered table, and every tiered table
     *     before its first cycle.</li>
     *     <li><b>empty ledger</b> -- the chunk table exists but no cycle has written a chunk yet.
     *     Also "no cold data", but reached through a real read, so it refreshes.</li>
     *     <li><b>known</b> -- chunks exist and reach up to {@link #topExclusiveMs()}.</li>
     *     <li><b>unknown</b> -- the ledger could not be consulted (unreadable, or absent beside an
     *     existing chunk table). Chunks may reach anywhere, so the merge must always run; the
     *     look-back falls back to {@code chunk_window}'s validated maximum, which bounds every chunk
     *     any policy could ever have written.</li>
     * </ul>
     */
    public static final class Coverage
    {
        static final Coverage NO_CHUNK_TABLE = new Coverage(false, true, false, 0, 0, 0);
        static final Coverage EMPTY = new Coverage(true, true, false, 0, 0, 0);
        static final Coverage UNKNOWN = new Coverage(true, false, false, 0, 0, 0);

        private final boolean chunkTableExists;
        private final boolean known;
        private final boolean anyChunks;
        private final long minWindowStartMs;
        private final long maxWindowStartMs;
        private final long widestChunkWindowMs;

        private Coverage(boolean chunkTableExists, boolean known, boolean anyChunks,
                         long minWindowStartMs, long maxWindowStartMs, long widestChunkWindowMs)
        {
            this.chunkTableExists = chunkTableExists;
            this.known = known;
            this.anyChunks = anyChunks;
            this.minWindowStartMs = minWindowStartMs;
            this.maxWindowStartMs = maxWindowStartMs;
            this.widestChunkWindowMs = widestChunkWindowMs;
        }

        /**
         * @return {@code false} only when the shadow table does not exist, which proves nothing has
         * ever been encoded for this base table -- the one condition under which the read path may
         * skip the merge without consulting anything else.
         */
        public boolean chunkTableExists()
        {
            return chunkTableExists;
        }

        /** @return {@code true} if at least one chunk is known to exist. */
        public boolean anyChunks()
        {
            return known && anyChunks;
        }

        /**
         * @return {@code false} when the ledger could not be consulted at all, so nothing here is a
         * statement about the chunk table. Callers that would otherwise <em>act</em> on the numbers
         * -- rather than merely fail safe on the sentinels {@link #topExclusiveMs()} returns -- have
         * to distinguish the two: see {@link TieredWrites} (which narrows what it refuses) and
         * {@link #claim} (which refuses to run at all).
         */
        public boolean known()
        {
            return known;
        }

        /**
         * @return one past the newest timestamp any chunk can hold: a query starting at or after this
         * needs no merge. {@link Long#MIN_VALUE} when nothing is encoded (nothing to merge, ever),
         * {@link Long#MAX_VALUE} when coverage is unknown (merge everything).
         */
        public long topExclusiveMs()
        {
            if (!known)
                return Long.MAX_VALUE;
            if (!anyChunks)
                return Long.MIN_VALUE;
            // Saturating: a corrupt/absurd ledger value must widen coverage, never wrap it to a
            // negative number that would make the fast path swallow every query.
            long top = maxWindowStartMs + widestChunkWindowMs;
            return top < maxWindowStartMs ? Long.MAX_VALUE : top;
        }

        /**
         * @return how far before a query's start the chunk {@code SELECT} must look for a chunk that
         * could still reach into the range -- the <b>widest</b> {@code chunk_window} any existing
         * chunk was written with, not the currently configured one. Falls back to the validated
         * maximum {@code chunk_window} when coverage is unknown, which is a true upper bound on any
         * chunk any accepted policy could have produced.
         */
        public long lookbackMillis()
        {
            return anyChunks() ? widestChunkWindowMs : TieringPolicy.MAX_CHUNK_WINDOW_MILLIS;
        }

        /**
         * @return this coverage widened to also span {@code [minWindowStart, maxWindowStart]} with a
         * chunk width of at least {@code widthMs}.
         * @throws IllegalStateException if this coverage is unknown -- there is nothing to widen, and
         * treating "unknown" as "empty" here would <b>narrow</b> the ledger (see below)
         */
        Coverage widenedBy(long minWindowStart, long maxWindowStart, long widthMs)
        {
            // Deliberately explicit rather than falling through to the anyChunks() branch below.
            // anyChunks() answers `known && anyChunks`, so an UNKNOWN coverage would take the
            // "nothing recorded yet" path and RESET this node's ledger row to just this cycle's
            // window and this cycle's chunk_window -- narrowing the cluster's recorded coverage. That
            // is precisely the failure the ledger exists to prevent: after a chunk_window was
            // shrunk, the aggregate widest would drop to the new value and the read path's look-back
            // would stop reaching the wider legacy chunks, whose base rows are already deleted.
            if (!known)
                throw new IllegalStateException("chunk coverage is unknown, so it cannot be widened");

            if (!anyChunks)
                return new Coverage(true, true, true, minWindowStart, maxWindowStart, widthMs);

            return new Coverage(true, true, true,
                                Math.min(minWindowStartMs, minWindowStart),
                                Math.max(maxWindowStartMs, maxWindowStart),
                                Math.max(widestChunkWindowMs, widthMs));
        }

        boolean sameAs(Coverage other)
        {
            return known == other.known
                   && anyChunks == other.anyChunks
                   && minWindowStartMs == other.minWindowStartMs
                   && maxWindowStartMs == other.maxWindowStartMs
                   && widestChunkWindowMs == other.widestChunkWindowMs;
        }

        @Override
        public String toString()
        {
            if (!known)
                return "Coverage{unknown}";
            if (!anyChunks)
                return format("Coverage{empty, chunkTable=%s}", chunkTableExists);
            return format("Coverage{[%d, %d], widest=%dms, topExcl=%d}",
                          minWindowStartMs, maxWindowStartMs, widestChunkWindowMs, topExclusiveMs());
        }
    }

    private static final class Entry
    {
        final Coverage coverage;
        final long expiresAtMillis;

        Entry(Coverage coverage, long loadedAtMillis)
        {
            this.coverage = coverage;
            this.expiresAtMillis = loadedAtMillis + (coverage.known ? REFRESH_MILLIS : UNKNOWN_REFRESH_MILLIS);
        }
    }

    /**
     * @param cl the consistency level of the query asking, or {@code null} on the internal/local
     *           execution path. Advisory only: the ledger is always consulted at quorum strength
     *           (see {@link #ledgerConsistency}), because a weaker read that missed the ledger row
     *           would be cached as {@link Coverage#EMPTY} -- an authoritative "nothing is cold" that
     *           the write guard would then act on.
     * @return {@code base}'s chunk coverage, from cache when fresh.
     */
    public static Coverage forTable(TableMetadata base, ConsistencyLevel cl)
    {
        Coverage cached = cached(base.id);
        if (cached != null)
            return cached;

        // Single-flight, per table. Without it, every request arriving while a slow (or timing-out)
        // ledger read is in flight starts its own: one blocked request thread per concurrent query
        // instead of one for the whole table. The waiters re-check the cache on entry, so all but the
        // first pay the wait rather than the read -- and, once the loser of the race has published,
        // nothing at all.
        Object lock = LOADING.computeIfAbsent(base.id, ignored -> new Object());
        synchronized (lock)
        {
            cached = cached(base.id);
            if (cached != null)
                return cached;

            Coverage loaded = load(base, cl);
            put(base.id, loaded);
            return loaded;
        }
    }

    /** @return the cached coverage for {@code id} if it has not expired, else {@code null}. */
    private static Coverage cached(TableId id)
    {
        Entry entry = CACHE.get(id);
        return entry != null && Clock.Global.currentTimeMillis() < entry.expiresAtMillis ? entry.coverage : null;
    }

    /**
     * Re-reads {@code base}'s ledger now, discarding any cached value. Called once per re-encode
     * cycle, which is also what warms the cache after a restart (the global sweep runs a cycle for
     * every policy-bearing table).
     */
    public static void refresh(TableMetadata base, ConsistencyLevel cl)
    {
        CACHE.remove(base.id);
        forTable(base, cl);
    }

    @VisibleForTesting
    public static void invalidateAll()
    {
        CACHE.clear();
        LOADING.clear();
    }

    /**
     * @return {@code true} if {@code base}'s coverage is currently cached, i.e. some caller has read
     * the ledger recently. The only observable difference between "the ledger was consulted" and
     * "the ledger was not consulted", which is what the write guard's cost depends on.
     */
    @VisibleForTesting
    public static boolean isCached(TableMetadata base)
    {
        return cached(base.id) != null;
    }

    /**
     * The consistency level the ledger itself is read and written at, which is deliberately not
     * (always) the caller's.
     * <p>
     * The cache is keyed by table alone, so whatever the first caller after an expiry loads is what
     * every later caller sees for the next refresh interval -- including {@link TieredWrites}, whose
     * whole correctness rests on never being handed a coverage that was read too weakly to see the
     * ledger row. A user {@code SELECT} at {@code CL=ONE} landing on a replica that does not have the
     * row would otherwise cache {@link Coverage#EMPTY} -- an authoritative "nothing is cold" -- and
     * for the rest of that interval a {@code DELETE} against chunked data would be accepted, masking
     * the chunk until {@code gc_grace_seconds} purged the tombstone and the data came back. The
     * {@code null} (node-local internal) path is the same mistake by another route: a coordinator
     * holding no replica of the ledger partition reads it as empty.
     * <p>
     * Quorum strength is exactly the set a {@link TieringPolicy} may name for its own writes. That
     * makes a ledger row visible to any later read <em>in the same quorum scope</em> -- it does
     * <b>not</b> make it unconditionally visible: a row written at {@code LOCAL_QUORUM} in DC1 and
     * read at {@code LOCAL_QUORUM} in DC2 shares no replica, so the reader can legitimately see
     * {@link Coverage#EMPTY} until the write has propagated. Same-DC and single-DC deployments, and
     * any policy naming {@code QUORUM}/{@code EACH_QUORUM}/{@code ALL}, do not have the gap.
     * <p>
     * What that costs is bounded, and deliberately so. A reader that misses the ledger row treats the
     * table as having no cold data; with a policy installed that is still safe, because
     * {@link ColdBoundary} floors the cold boundary at the policy's own hot-window edge rather than
     * trusting the ledger to move it -- so the merge still covers everything the re-encoder can have
     * encoded. The exposed case is the one with no policy to floor against: a table whose
     * {@code timeseries_tiering} extension has been dropped while its chunks remain, where a
     * cross-DC-stale ledger reads as "nothing is cold" and the chunks stay unmerged until the write
     * propagates and the {@link #REFRESH_MILLIS} cache entry expires.
     *
     * @param requested the caller's consistency level, or {@code null}
     */
    static ConsistencyLevel ledgerConsistency(ConsistencyLevel requested)
    {
        return TieringPolicy.isQuorumStrength(requested) ? requested : DEFAULT_CONSISTENCY;
    }

    /**
     * Records, <b>before</b> the chunks are written, that {@code base}'s chunk table is about to hold
     * chunks of {@code chunkWindowMillis} covering the windows from {@code minWindowStartMs} to
     * {@code maxWindowStartMs}.
     * <p>
     * Ordering is the point: the ledger has to be at least as wide as the chunk table at every
     * instant, so it is widened first and the chunk written second. The reverse order would leave a
     * crash window in which a chunk exists that the read path's fast path does not know about -- data
     * whose base rows are already deleted, invisible until the next cycle.
     * <p>
     * <b>The top must be a window this call actually encodes, never a cycle-wide ceiling.</b> It used
     * to be the re-encoder's {@code cutoff}, on the reasoning that over-claiming the top is free
     * because it only costs an unnecessary chunk read. That was true when only the read path consumed
     * it, and stopped being true when the write guard started sharing the number through
     * {@link ColdBoundary#coldBelowMs}: there, a top of {@code cutoff} means
     * {@code topExclusiveMs() == cutoff + chunk_window}, so {@code TieredWrites} refuses tombstones on
     * a whole {@code chunk_window} of rows that are inside the hot window and were never encoded. With
     * {@code hot_window == chunk_window} that band runs right up to now and no recent row can be
     * deleted at all. Staleness is still safe without the over-claim, because {@code coldBelowMs}
     * floors the answer at the current hot boundary -- a chunk written since this node last read the
     * ledger is below that floor by definition.
     * <p>
     * A no-op when the claim adds nothing to what is already recorded, which is the steady state.
     *
     * @throws IllegalStateException if the ledger cannot be read, so this node's existing claim is
     * not known. Writing one anyway would <b>narrow</b> the ledger to this cycle's window and this
     * cycle's {@code chunk_window} -- and after a {@code chunk_window} was shrunk, that permanently
     * hides every wider legacy chunk from the read path's look-back.
     */
    public static void claim(TableMetadata base, ConsistencyLevel cl,
                             long minWindowStartMs, long maxWindowStartMs, long chunkWindowMillis)
    {
        ConsistencyLevel ledgerCl = ledgerConsistency(cl);
        Coverage current = forTable(base, ledgerCl);
        if (!current.known())
            throw new IllegalStateException(format(
                "Tiered storage: %s.%s's chunk coverage ledger could not be read, so what this node has already " +
                "claimed is unknown and this window cannot be claimed. Writing a claim built on an unknown " +
                "coverage would overwrite this node's ledger row with only this cycle's window and chunk_window, " +
                "narrowing the cluster's recorded coverage -- after which reads stop looking for the chunks it no " +
                "longer mentions, whose base rows are already deleted. The window is skipped and retried next cycle.",
                base.keyspace, base.name));

        Coverage widened = current.widenedBy(minWindowStartMs, maxWindowStartMs, chunkWindowMillis);
        if (current.sameAs(widened))
            return;

        String insert = format("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)",
                               coverageRef(base), SCOPE_COLUMN, NODE_COLUMN,
                               MIN_WINDOW_START, MAX_WINDOW_START, MAX_CHUNK_WINDOW);
        List<ByteBuffer> values = new ArrayList<>(5);
        values.add(UTF8Type.instance.decompose(SCOPE));
        values.add(UTF8Type.instance.decompose(nodeId()));
        values.add(TimestampType.instance.decompose(new Date(widened.minWindowStartMs)));
        values.add(TimestampType.instance.decompose(new Date(widened.maxWindowStartMs)));
        values.add(LongType.instance.decompose(widened.widestChunkWindowMs));
        // Deliberately unguarded: a failure here must abort the window rather than let the chunk be
        // written behind a ledger that does not cover it. The re-encoder's per-tag handler catches it
        // and counts the tag as skipped.
        QueryProcessor.process(insert, ledgerCl, values);
        put(base.id, widened);
    }

    private static void put(TableId id, Coverage coverage)
    {
        if (CACHE.size() >= MAX_CACHED_TABLES)
        {
            // Wholesale, like TieringPolicy.PARSED: at 4096 distinct tables this is a once-in-a-very-
            // long-while event whose only cost is re-reading each ledger once, and an eviction policy
            // would need per-entry bookkeeping on the hottest lookup in the read path.
            CACHE.clear();
            LOADING.clear();
        }
        CACHE.put(id, new Entry(coverage, Clock.Global.currentTimeMillis()));
    }

    private static Coverage load(TableMetadata base, ConsistencyLevel requestedCl)
    {
        if (Schema.instance.getTableMetadata(base.keyspace, ChunkTables.chunkTableName(base.name)) == null)
            return Coverage.NO_CHUNK_TABLE;

        if (Schema.instance.getTableMetadata(base.keyspace, ChunkTables.coverageTableName(base.name)) == null)
        {
            // A chunk table with no ledger beside it. Nothing this build writes produces that shape
            // (both tables are created together, and the ledger is written before the first chunk),
            // so it means someone dropped the ledger -- in which case the only safe reading of the
            // chunk table is "it may hold anything".
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                             base.keyspace + '.' + base.name + ":coverage-missing", 1, TimeUnit.MINUTES,
                             "Tiered storage: {}.{} has a chunk table but no {} ledger, so how far its cold data " +
                             "reaches cannot be established; every read will merge chunks until the ledger is " +
                             "rebuilt by the next re-encode cycle",
                             base.keyspace, base.name, ChunkTables.coverageTableName(base.name));
            return Coverage.UNKNOWN;
        }

        String select = format("SELECT %s, %s, %s FROM %s WHERE %s = ?",
                               MIN_WINDOW_START, MAX_WINDOW_START, MAX_CHUNK_WINDOW,
                               coverageRef(base), SCOPE_COLUMN);
        UntypedResultSet rows;
        // The ledger is ordinary user-keyspace data, so reading it re-enters the read path. Bracket
        // it: nothing about the ledger is itself tiered, and the bypass keeps this from recursing.
        TransparentReads.enterInternalBypass();
        try
        {
            // Never executeInternal, and never weaker than a quorum -- see ledgerConsistency(). The
            // answer is cached for every later caller, including the write guard, so it has to be one
            // that could not have missed a ledger row simply because of where this read landed.
            rows = QueryProcessor.process(select, ledgerConsistency(requestedCl),
                                          List.of(UTF8Type.instance.decompose(SCOPE)));
        }
        catch (RuntimeException e)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                             base.keyspace + '.' + base.name + ":coverage-read", 1, TimeUnit.MINUTES,
                             "Tiered storage: could not read {}.{}'s chunk coverage ledger; merging chunks into " +
                             "every read of this table until it can be read again", base.keyspace, base.name, e);
            return Coverage.UNKNOWN;
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }

        if (rows == null || rows.isEmpty())
            return Coverage.EMPTY;

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long widest = 0;
        // One row per node that has ever encoded: aggregate rather than trust any single one, so a
        // node whose own claim is behind cannot narrow the cluster's coverage.
        for (UntypedResultSet.Row row : rows)
        {
            if (!row.has(MIN_WINDOW_START) || !row.has(MAX_WINDOW_START) || !row.has(MAX_CHUNK_WINDOW))
                continue;
            min = Math.min(min, row.getTimestamp(MIN_WINDOW_START).getTime());
            max = Math.max(max, row.getTimestamp(MAX_WINDOW_START).getTime());
            widest = Math.max(widest, row.getLong(MAX_CHUNK_WINDOW));
        }
        if (widest <= 0)
            return Coverage.EMPTY;

        return new Coverage(true, true, true, min, max, widest);
    }

    /** @return this node's ledger row key; stable across restarts, distinct per node. */
    private static String nodeId()
    {
        return FBUtilities.getBroadcastAddressAndPort().toString();
    }

    private static String coverageRef(TableMetadata base)
    {
        return format("%s.%s",
                      ColumnIdentifier.maybeQuote(base.keyspace),
                      ColumnIdentifier.maybeQuote(ChunkTables.coverageTableName(base.name)));
    }
}
