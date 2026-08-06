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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * The set of distinct partition keys ("tags") of a tiered base table, kept as a small shadow table
 * ({@link ChunkTables#tagsTableName}) so the re-encoder does not have to rediscover them by scanning
 * the base table.
 *
 * <p><b>Why this exists.</b> Every re-encode cycle has to start by answering "which tags are there?",
 * and the direct answer -- {@code SELECT DISTINCT <pk> FROM base} -- is the most expensive query the
 * cycle issues, by orders of magnitude. DISTINCT is cheap per <em>row</em> and expensive per
 * <em>partition</em>: each row it returns is a different partition, so the coordinator seeks the
 * first live row of every partition in range across every SSTable that range touches. Measured on a
 * production table of 11.6k tags (multi-MB partitions, 14 SSTables, ZSTD): ~19ms per partition, so
 * ~220s to walk the ring -- per cycle, against a {@code interval} of 300s. The same table's registry
 * is 11.6k clustering rows in one partition, read in a handful of pages.
 *
 * <p><b>How it is populated.</b> Two ways, and it needs both:
 * <ul>
 *     <li>the write path, via {@link #noteTag}: the first write to a tag registers it (subsequent
 *     writes are a hash lookup against {@link #seenTags}), so a tag created after this feature
 *     shipped is in the registry from its first write onward;</li>
 *     <li>a periodic reconcile against the base table, via {@link #dueForReconcile}: the full
 *     DISTINCT scan, run every {@value #RECONCILE_INTERVAL_MINUTES} minutes instead of every cycle,
 *     whose results are written back here.</li>
 * </ul>
 * The reconcile is not redundant with the write path, and dropping it would be a data-tiering bug
 * rather than an optimisation: a tag whose rows predate this feature has no registry entry and may
 * never be written to again (a decommissioned sensor is exactly the case where the backlog most
 * needs tiering), and a tag whose registry write failed would otherwise be missed forever. Missing
 * from the registry means <em>never encoded</em>, silently -- so the base table stays authoritative,
 * and the registry is only ever allowed to make the common case cheap.
 *
 * <p><b>Failure is always toward doing more work, never less.</b> A registry read that fails or
 * comes back empty falls through to the full scan. A registry write that fails is logged and
 * dropped -- the next reconcile picks the tag up. Nothing here can cause a tag to be skipped that
 * the base table would have yielded.
 */
public final class TagRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(TagRegistry.class);

    /** The registry's single partition key value -- it describes one thing, so it is one partition. */
    static final String SCOPE_COLUMN = "scope";
    static final String SCOPE = "tags";

    /**
     * How long the registry is trusted on its own before the base table is scanned again to catch
     * tags the write path never saw (see the class javadoc). Deliberately much longer than a cycle's
     * {@code interval} -- the whole point is that the expensive scan stops being per-cycle -- but far
     * shorter than any window in which not tiering a tag would matter.
     */
    private static final long RECONCILE_INTERVAL_MINUTES = 60;

    /**
     * Per-table set of tags this node has already registered, so the write path pays a hash lookup
     * rather than a write per mutation. Bounded: past {@value #MAX_CACHED_TAGS_PER_TABLE} entries it
     * stops growing and later tags simply re-register on every write, which is slower but still
     * correct -- an unbounded cache on a table with unbounded cardinality would be a heap leak, and
     * a heap leak is worse than a redundant write.
     */
    private static final ConcurrentHashMap<TableId, Set<List<ByteBuffer>>> seenTags = new ConcurrentHashMap<>();
    private static final int MAX_CACHED_TAGS_PER_TABLE = 1_000_000;

    /**
     * {@link TableId} -> epoch millis of the last reconcile <b>attempt</b> against the base table,
     * whether or not the scan completed.
     * <p>
     * Attempts, not successes. Gating on success is the same mistake that made a failing table's
     * sweep ignore its own interval (see {@code TieredStorageService.lastAttemptAtMillisByTable}),
     * and it reappeared here with the same shape: on the table this registry exists for, the base
     * scan is exactly the thing that does not reliably finish, so a success-gated timer never
     * advances and <em>every</em> cycle re-runs a multi-minute failing full-table scan instead of one
     * per hour. Observed in production at 19-minute intervals against a 60-minute setting. Throttling
     * failed attempts costs nothing: an incomplete scan's results are already unioned with the
     * registry, and the write path keeps the registry current between reconciles.
     */
    private static final ConcurrentHashMap<TableId, Long> lastReconcileAttemptMillis = new ConcurrentHashMap<>();

    /**
     * {@link TableId} -> the token the incremental base-table scan should resume <em>after</em>;
     * absent means "start at the beginning of the ring".
     *
     * <p>Deliberately in memory only, unlike the registry itself. What must survive a restart is the
     * <em>result</em> of scanning -- which tags exist -- and that is in the registry table. Losing the
     * position only costs re-walking ground already covered, and the tags found there are already
     * registered and already being encoded, so nothing regresses. Persisting it would mean either a
     * fourth shadow table per base table or a column added to an existing one, and neither is worth
     * paying for a value whose loss is this cheap. If frequent restarts are ever shown to stop the
     * walk reaching the far end of the ring, persisting it is the fix -- but that should be measured
     * first, not assumed.
     */
    private static final ConcurrentHashMap<TableId, Token> scanCursor = new ConcurrentHashMap<>();

    private TagRegistry()
    {
    }

    /** @return the token to resume the incremental scan after, or {@code null} to start from the beginning. */
    static Token scanCursor(TableMetadata base)
    {
        return scanCursor.get(base.id);
    }

    /** Records how far the incremental scan got. */
    static void advanceScanCursor(TableMetadata base, Token token)
    {
        scanCursor.put(base.id, token);
    }

    /** Sends the incremental scan back to the start of the ring, a full pass having completed. */
    static void rewindScanCursor(TableMetadata base)
    {
        scanCursor.remove(base.id);
    }

    /**
     * @return {@code true} if {@code base}'s registry should be rebuilt from the base table on this
     * cycle -- because it has never been reconciled on this node, or because
     * {@value #RECONCILE_INTERVAL_MINUTES} minutes have passed since it last was.
     */
    static boolean dueForReconcile(TableMetadata base)
    {
        Long last = lastReconcileAttemptMillis.get(base.id);
        return last == null
               || Clock.Global.currentTimeMillis() - last >= TimeUnit.MINUTES.toMillis(RECONCILE_INTERVAL_MINUTES);
    }

    /**
     * Records that {@code base}'s registry reconcile was attempted -- called on every attempt, not
     * only the ones that finished. See {@link #lastReconcileAttemptMillis} for why.
     */
    static void reconcileAttempted(TableMetadata base)
    {
        lastReconcileAttemptMillis.put(base.id, Clock.Global.currentTimeMillis());
    }

    @VisibleForTesting
    static void resetForTesting()
    {
        seenTags.clear();
        lastReconcileAttemptMillis.clear();
        scanCursor.clear();
    }

    /**
     * Reads every tag in {@code base}'s registry.
     *
     * @return the registered tags, or an empty list if the registry does not exist yet, is empty, or
     * could not be read -- all three of which the caller must treat identically: fall back to the
     * base-table scan (see {@link TieredStorageService#enumerateTags}). Never null.
     */
    static List<List<ByteBuffer>> read(TableMetadata base, ConsistencyLevel cl)
    {
        TableMetadata registry = Schema.instance.getTableMetadata(base.keyspace, ChunkTables.tagsTableName(base.name));
        if (registry == null)
            return Collections.emptyList();

        List<ColumnMetadata> tagColumns = base.partitionKeyColumns();
        String query = format("SELECT %s FROM %s WHERE %s = ?",
                              columnList(tagColumns), quotedRef(registry), ChunkTables.tagsScopeColumn(base));
        try
        {
            List<UntypedResultSet.Row> rows =
                TieredStorageService.pagedSelect(query, cl, Collections.singletonList(scopeValue()));
            List<List<ByteBuffer>> tags = new ArrayList<>(rows.size());
            for (UntypedResultSet.Row row : rows)
            {
                List<ByteBuffer> tag = new ArrayList<>(tagColumns.size());
                for (ColumnMetadata column : tagColumns)
                    tag.add(row.getBytes(column.name.toString()));
                tags.add(tag);
            }
            return tags;
        }
        catch (RuntimeException e)
        {
            logger.warn("Tiered storage: could not read {}.{}'s tag registry; falling back to a base-table scan " +
                        "this cycle", base.keyspace, base.name, e);
            return Collections.emptyList();
        }
    }

    /**
     * Writes {@code tags} into {@code base}'s registry, skipping any this node has already written.
     * Used by the reconcile path to persist what the base-table scan found.
     */
    static void putAll(TableMetadata base, ConsistencyLevel cl, List<List<ByteBuffer>> tags)
    {
        for (List<ByteBuffer> tag : tags)
            noteTag(base, cl, tag);
    }

    /**
     * Registers one tag, if this node has not already registered it.
     * <p>
     * Best-effort by construction: a failure here is logged (rate-limited) and swallowed, because the
     * callers are the write path -- where failing a client's write to maintain a re-encoder's index
     * would be indefensible -- and the reconcile, which will simply try again. The cost of a lost
     * registration is one delayed tag, bounded by {@value #RECONCILE_INTERVAL_MINUTES} minutes.
     */
    static void noteTag(TableMetadata base, ConsistencyLevel cl, List<ByteBuffer> tag)
    {
        Set<List<ByteBuffer>> seen = seenTags.computeIfAbsent(base.id, ignored -> ConcurrentHashMap.newKeySet());
        if (seen.contains(tag))
            return;

        // Past the cache's ceiling, stop registering from the write path altogether rather than
        // registering without caching. The latter is what "bounded cache, unbounded correctness"
        // naively suggests, and it is the worse failure by far: every single mutation to the table
        // would issue its own distributed INSERT, forever -- at this node's write rate that converts
        // a heap concern into a write-amplification outage. The hourly reconcile scan is the backstop
        // that makes stopping safe: it is authoritative, and it is what covers every tag the write
        // path never registered (see the class javadoc).
        if (seen.size() >= MAX_CACHED_TAGS_PER_TABLE)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, base.keyspace + '.' + base.name + ":tag-cache-full",
                             1, TimeUnit.HOURS,
                             "Tiered storage: {}.{} has more than {} distinct tags on this node; the write path has " +
                             "stopped registering new ones and they will be picked up by the reconcile scan instead. " +
                             "Enumeration for this table is back to costing a full base-table scan every {} minutes",
                             base.keyspace, base.name, MAX_CACHED_TAGS_PER_TABLE, RECONCILE_INTERVAL_MINUTES);
            return;
        }

        TableMetadata registry = Schema.instance.getTableMetadata(base.keyspace, ChunkTables.tagsTableName(base.name));
        if (registry == null)
            return; // not ensured yet; the first cycle creates it and reconciles into it

        List<ColumnMetadata> tagColumns = base.partitionKeyColumns();
        String query = format("INSERT INTO %s (%s, %s) VALUES (?%s)",
                              quotedRef(registry), ChunkTables.tagsScopeColumn(base), columnList(tagColumns),
                              bindMarkers(tagColumns.size()));
        List<ByteBuffer> values = new ArrayList<>(tag.size() + 1);
        values.add(scopeValue());
        values.addAll(tag);
        // Claim the tag BEFORE issuing the write, not after. Every concurrent write to a
        // newly-seen tag reaches this point, and marking it seen only on success meant all of them
        // issued their own INSERT -- a burst of identical registrations proportional to the write
        // concurrency, on the client's own write path, exactly when a table is warming up (a restart
        // empties this cache, so it is every tag's first write again). Claiming first bounds it to
        // one write per tag per node; the claim is released again below if the write fails, so a
        // failed registration is still retried by a later write rather than being cached as done.
        // ...and losing the claim race means another thread is already registering this exact tag, so
        // this one returns rather than issuing a duplicate. Checking `contains` above and then adding
        // unconditionally would have let every racer through, which is the burst this exists to stop.
        if (!seen.add(tag))
            return;
        try
        {
            QueryProcessor.process(query, cl, values);
        }
        catch (RuntimeException e)
        {
            seen.remove(tag);
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, base.keyspace + '.' + base.name + ":tag-registry-write",
                             5, TimeUnit.MINUTES,
                             "Tiered storage: could not register a tag of {}.{}; it will be picked up by the next " +
                             "reconcile against the base table ({})",
                             base.keyspace, base.name, e.toString());
        }
    }

    /**
     * Write-path entry point: registers {@code key}'s tag if {@code base} is a tiered table and this
     * node has not registered it already.
     * <p>
     * Ordered so the overwhelmingly common case -- a write to a tag that is already registered -- is
     * one map lookup and one set lookup, with no policy parse, no allocation and no query. Only a
     * miss resolves the policy (which is also how a non-tiered table is recognised and dropped) and
     * only then does anything reach the cluster.
     */
    static void noteTagIfTiered(TableMetadata base, org.apache.cassandra.db.DecoratedKey key)
    {
        Set<List<ByteBuffer>> seen = seenTags.get(base.id);
        List<ByteBuffer> tag = null;
        if (seen != null)
        {
            tag = tagOf(base, key);
            if (seen.contains(tag))
                return; // the overwhelmingly common case: known table, known tag
        }

        // Deliberately after the cache probe and before {@link #tagOf}: this is the path taken by
        // every write to every NON-tiered table that happens to be clustered by a timestamp, on every
        // write forever, and fromTable resolves that to a single extensions-map lookup returning null.
        // Building the tag first (as this once did) put a List allocation -- and, on a composite
        // partition key, a CompositeType.split and array copy -- on that path for no reason at all.
        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(base);
        }
        catch (org.apache.cassandra.exceptions.ConfigurationException e)
        {
            return; // an invalid policy is the re-encoder's problem to report, not the write path's
        }
        if (policy == null)
            return;

        noteTag(base, policy.consistency, tag == null ? tagOf(base, key) : tag);
    }

    /** @return {@code key}'s partition-key column values, in the base table's key order. */
    static List<ByteBuffer> tagOf(TableMetadata base, org.apache.cassandra.db.DecoratedKey key)
    {
        if (base.partitionKeyColumns().size() == 1)
            return Collections.singletonList(key.getKey());

        ByteBuffer[] components = ((CompositeType) base.partitionKeyType).split(key.getKey());
        List<ByteBuffer> tag = new ArrayList<>(components.length);
        Collections.addAll(tag, components);
        return tag;
    }

    /**
     * @return the serialized partition key for {@code tag}, so its token can be computed locally --
     * the registry is one partition holding every node's tags, so restricting enumeration to this
     * node's primary ranges is done here rather than by a {@code token(...)} predicate.
     */
    static ByteBuffer partitionKey(TableMetadata base, List<ByteBuffer> tag)
    {
        if (tag.size() == 1)
            return tag.get(0);
        return CompositeType.build(ByteBufferAccessor.instance, tag.toArray(new ByteBuffer[0]));
    }

    private static ByteBuffer scopeValue()
    {
        return UTF8Type.instance.decompose(SCOPE);
    }

    private static String quotedRef(TableMetadata table)
    {
        return format("%s.%s", ColumnIdentifier.maybeQuote(table.keyspace), ColumnIdentifier.maybeQuote(table.name));
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

    /** @return {@code ", ?, ?"} -- {@code count} positional bind markers, each already comma-prefixed. */
    private static String bindMarkers(int count)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++)
            sb.append(", ?");
        return sb.toString();
    }
}
