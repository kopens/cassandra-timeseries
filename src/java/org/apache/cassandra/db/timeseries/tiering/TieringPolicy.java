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
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;

/**
 * Per-table tiering policy for the background chunk-store re-encoder, stored as JSON in the table's
 * {@code extensions['timeseries_tiering']} (see {@link org.apache.cassandra.cql3.statements.schema.TableAttributes}
 * for how that extension value gets set via {@code ALTER TABLE ... WITH extensions = {...}}).
 *
 * <p>Recognised JSON keys (unknown keys are rejected):
 * <ul>
 *     <li>{@code hot_window} (required) - rows younger than this are left alone.</li>
 *     <li>{@code chunk_window} (default {@code "1h"}, max {@code 31d}) - fixed-length re-encoding window.</li>
 *     <li>{@code cold_window} (optional) - once a chunk is older than this, delete it outright.</li>
 *     <li>{@code consistency} (default {@code "LOCAL_QUORUM"}) - a {@link ConsistencyLevel} name.</li>
 *     <li>{@code interval} (default {@code "5m"}) - re-encoder tick period.</li>
 * </ul>
 * Durations use the grammar {@code <positive int><m|h|d>} (minutes/hours/days).
 *
 * <p>{@code codec} was removed when the chunk codec stopped being a choice; the format (currently
 * chunk format v4, ALP-only for doubles) is decided by the build, not per policy. A policy still
 * carrying the key is rejected by name (see {@link #REMOVED_KEYS}) rather than silently ignored,
 * so an operator who set it learns that the setting no longer does anything.
 */
public final class TieringPolicy
{
    public static final String EXTENSION_KEY = "timeseries_tiering";

    private static final String HOT_WINDOW = "hot_window";
    private static final String CHUNK_WINDOW = "chunk_window";
    private static final String COLD_WINDOW = "cold_window";
    private static final String CONSISTENCY = "consistency";
    private static final String INTERVAL = "interval";

    private static final String DEFAULT_CHUNK_WINDOW = "1h";
    private static final String DEFAULT_CONSISTENCY = "LOCAL_QUORUM";
    private static final String DEFAULT_INTERVAL = "5m";

    private static final Set<String> KNOWN_KEYS = ImmutableSet.of(HOT_WINDOW, CHUNK_WINDOW, COLD_WINDOW,
                                                                    CONSISTENCY, INTERVAL);

    /**
     * Keys this policy used to accept, mapped to what happened to them. Rejected with a message that
     * names the key and its fate, instead of falling through to the generic "Unknown key" error: a
     * stored policy that still sets one was written deliberately, and silently dropping it would let
     * an operator keep believing the option is in effect.
     */
    private static final Map<String, String> REMOVED_KEYS = ImmutableMap.of(
        "codec", "the chunk format is fixed by the build (currently v4), so there is nothing to choose; remove the key");

    /**
     * Consistency levels weaker than a quorum are rejected: at e.g. {@code ONE}, the re-encoder's
     * existing-chunk read can miss the previous cycle's chunk (written at a higher CL, or simply not
     * yet visible to the replica this node read), so a subsequent merge produces a fresh-rows-only
     * chunk that *loses* to the old chunk on a later read (older write, but same or higher timestamp
     * doesn't apply here -- it's a distinct write racing on visibility, not ordering) while the range
     * delete still removes the source rows -- silent, permanent data loss with no error surfaced.
     */
    private static final Set<ConsistencyLevel> ALLOWED_CONSISTENCY_LEVELS =
            ImmutableSet.of(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM,
                            ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.ALL);

    private static final Pattern DURATION = Pattern.compile("([1-9][0-9]*)([mhd])");

    /**
     * Upper bound on {@code chunk_window}. The re-encoder materializes one chunk window's worth of a
     * tag's rows in memory at a time (its whole memory-boundedness story -- see
     * {@link TieredStorageService}), so an over-large window turns "bounded by one window" into
     * "bounded by nothing": a year-long window on a high-rate tag is an OOM.
     * <p>
     * Note this bounds a window in <b>time</b>, never in rows -- an event-driven tag can put any
     * number of rows into 31 days. The row bound is
     * {@code TieredStorageService.maxSamplesPerWindow}, which aborts the tag with an error naming
     * this option when a window exceeds it.
     */
    private static final String MAX_CHUNK_WINDOW = "31d";
    /**
     * Package-visible because it is also the read path's fallback look-back: with no coverage ledger
     * to consult, this is still a true upper bound on the width of any chunk any accepted policy
     * could ever have written (see {@link ChunkCoverage.Coverage#lookbackMillis()}).
     */
    static final long MAX_CHUNK_WINDOW_MILLIS = TimeUnit.DAYS.toMillis(31);

    public final long hotWindowMillis;
    public final long chunkWindowMillis;
    public final long coldWindowMillis; // -1 = unset
    public final long intervalMillis;
    public final ConsistencyLevel consistency;

    private TieringPolicy(long hotWindowMillis,
                           long chunkWindowMillis,
                           long coldWindowMillis,
                           long intervalMillis,
                           ConsistencyLevel consistency)
    {
        this.hotWindowMillis = hotWindowMillis;
        this.chunkWindowMillis = chunkWindowMillis;
        this.coldWindowMillis = coldWindowMillis;
        this.intervalMillis = intervalMillis;
        this.consistency = consistency;
    }

    // Parsed policies, keyed by the raw extension bytes. fromTable is on the hot path of every read
    // (up to three times per read command via the tiered-read hooks, and twice more under replica
    // filtering protection) and of every write, so re-parsing the JSON each time is pure waste. The
    // key is the extension value's content, so a policy edit is a different key and cannot be served
    // stale; the number of distinct policy documents in a cluster is tiny, and the map is cleared
    // wholesale rather than grown without bound if that ever stops being true.
    private static final ConcurrentHashMap<ByteBuffer, TieringPolicy> PARSED = new ConcurrentHashMap<>();
    private static final int MAX_CACHED_POLICIES = 1024;

    /**
     * @return the policy configured on {@code metadata}, or {@code null} if the table has no
     * {@code timeseries_tiering} extension.
     * @throws ConfigurationException if the extension value is present but invalid (malformed JSON,
     * unknown keys, or a rule violation).
     */
    public static TieringPolicy fromTable(TableMetadata metadata)
    {
        ByteBuffer value = metadata.params.extensions.get(EXTENSION_KEY);
        if (value == null)
            return null;

        // A ByteBuffer's hashCode covers its content BETWEEN position and limit, so the key must not
        // be the live schema buffer: any relative get() on that shared instance would move its
        // position and strand this entry -- a permanent cache miss plus a key that never matches
        // anything again, retained until the wholesale clear. A duplicate has its own position.
        ByteBuffer key = value.duplicate();

        TieringPolicy cached = PARSED.get(key);
        if (cached != null)
            return cached;

        String json;
        try
        {
            json = ByteBufferUtil.string(value);
        }
        catch (CharacterCodingException e)
        {
            throw new ConfigurationException(format("%s extension value is not valid UTF-8: %s", EXTENSION_KEY, e.getMessage()));
        }

        // Deliberately NOT computeIfAbsent: parse() throws on an invalid policy, and an invalid
        // policy must keep throwing every time it is read rather than be cached as anything.
        TieringPolicy policy = parse(json);
        if (PARSED.size() >= MAX_CACHED_POLICIES)
            PARSED.clear();
        PARSED.put(key, policy);
        return policy;
    }

    /**
     * Parses and validates a {@code timeseries_tiering} policy JSON document.
     *
     * @throws ConfigurationException on malformed JSON, unknown keys, or a rule violation.
     */
    public static TieringPolicy parse(String json)
    {
        Map<String, Object> raw;
        try
        {
            raw = JsonUtils.fromJsonMap(json);
        }
        catch (RuntimeException e)
        {
            throw new ConfigurationException(format("Invalid %s JSON: %s", EXTENSION_KEY, e.getMessage()));
        }

        for (String key : raw.keySet())
        {
            String removalNote = REMOVED_KEYS.get(key);
            if (removalNote != null)
                throw new ConfigurationException(format("%s.%s is no longer supported: %s",
                                                          EXTENSION_KEY, key, removalNote));
            if (!KNOWN_KEYS.contains(key))
                throw new ConfigurationException(format("Unknown %s key '%s'", EXTENSION_KEY, key));
        }

        String hotWindow = requireString(raw, HOT_WINDOW);
        if (hotWindow == null)
            throw new ConfigurationException(format("%s.%s is required", EXTENSION_KEY, HOT_WINDOW));
        long hotWindowMillis = parseDurationMillis(HOT_WINDOW, hotWindow);

        String chunkWindow = requireString(raw, CHUNK_WINDOW);
        String effectiveChunkWindow = chunkWindow != null ? chunkWindow : DEFAULT_CHUNK_WINDOW;
        long chunkWindowMillis = parseDurationMillis(CHUNK_WINDOW, effectiveChunkWindow);
        if (chunkWindowMillis > MAX_CHUNK_WINDOW_MILLIS)
            throw new ConfigurationException(format("%s.%s ('%s') must not exceed %s: the re-encoder reads one " +
                                                      "chunk_window of rows into memory per window, so a larger window " +
                                                      "can exhaust the heap and exceed the codec's per-chunk sample " +
                                                      "limit on a high-rate table; use a chunk_window of %s or less",
                                                      EXTENSION_KEY, CHUNK_WINDOW, effectiveChunkWindow,
                                                      MAX_CHUNK_WINDOW, MAX_CHUNK_WINDOW));

        String coldWindow = requireString(raw, COLD_WINDOW);
        long coldWindowMillis = coldWindow == null ? -1 : parseDurationMillis(COLD_WINDOW, coldWindow);

        String consistencyStr = requireString(raw, CONSISTENCY);
        ConsistencyLevel consistency = parseConsistency(consistencyStr != null ? consistencyStr : DEFAULT_CONSISTENCY);

        String interval = requireString(raw, INTERVAL);
        long intervalMillis = parseDurationMillis(INTERVAL, interval != null ? interval : DEFAULT_INTERVAL);

        if (hotWindowMillis < chunkWindowMillis)
            throw new ConfigurationException(format("%s.%s (%s) must be >= %s (%s)",
                                                      EXTENSION_KEY, HOT_WINDOW, hotWindow, CHUNK_WINDOW,
                                                      effectiveChunkWindow));

        if (coldWindowMillis >= 0 && coldWindowMillis <= hotWindowMillis)
            throw new ConfigurationException(format("%s.%s (%s) must be > %s (%s)",
                                                      EXTENSION_KEY, COLD_WINDOW, coldWindow, HOT_WINDOW, hotWindow));

        return new TieringPolicy(hotWindowMillis, chunkWindowMillis, coldWindowMillis, intervalMillis, consistency);
    }

    /** Floors {@code tsMillis} to the start of its {@link #chunkWindowMillis}-wide, epoch-aligned window. */
    public long windowStartFor(long tsMillis)
    {
        return tsMillis - Math.floorMod(tsMillis, chunkWindowMillis);
    }

    /**
     * Decides whether tiering can safely handle {@code metadata}'s shape.
     *
     * <p><b>Supported:</b> any number of partition key columns (a composite key is fine); exactly one
     * clustering column whose type is {@code timestamp} ({@code ASC} or {@code DESC} -- newest-first
     * is the dominant time-series idiom, and {@code CLUSTERING ORDER BY (ts DESC)} merely wraps the
     * type in {@link org.apache.cassandra.db.marshal.ReversedType}); any number of regular columns of
     * any supported type; any number of static columns. Static columns are deliberately unconstrained:
     * they are never chunked, and the re-encoder's clustering-range delete cannot touch them (static
     * cells live outside the clustering range), so they survive tiering untouched.
     *
     * <p><b>Rejected</b>, each for a reason that would otherwise cost data or answers silently:
     * <ul>
     *     <li>a <b>counter</b> column anywhere in the table -- the re-encoder deletes rows and
     *     re-inserts their content, which a counter cannot survive (a counter delete is permanent;
     *     the column can never be written again). This is a correctness stop, not a limitation.</li>
     *     <li>a <b>non-frozen collection</b> (or non-frozen UDT) regular column -- multi-cell values
     *     have per-element cells and timestamps that a chunk's opaque per-row bytes cannot represent.
     *     Frozen collections are single-cell and are fine, carried as opaque bytes.</li>
     *     <li>zero or several clustering columns, or a clustering column that is not a timestamp.</li>
     *     <li>a <b>secondary index (SAI or otherwise) on anything but a static column</b> -- index
     *     entries are per base row, so the re-encoder's delete removes them and index queries would
     *     silently return only data younger than {@code hot_window}. That covers regular, clustering
     *     <em>and</em> partition-key-component targets alike; only a static-column index survives,
     *     because its entries sit outside every clustering range the re-encoder deletes.</li>
     *     <li>a <b>materialized view</b> over the table -- the range delete propagates to the view
     *     (view maintenance reads the deleted base rows and issues the matching view deletions), but
     *     transparent reads only reconstruct rows for the <em>base</em> table, so the view would
     *     permanently lose everything older than {@code hot_window} with no error anywhere.</li>
     *     <li>the table being a <b>materialized view</b> itself (as opposed to having one) -- the
     *     policy belongs on the base table; a view's rows are derived and nothing would chunk them.</li>
     *     <li>a partition key column named like one of the chunk table's own columns
     *     ({@link ChunkTables#RESERVED_COLUMN_NAMES}) -- the mirrored chunk table would declare that
     *     name twice.</li>
     *     <li>a <b>{@code transactional_mode} other than {@code off}</b> -- an Accord transactional read
     *     ({@code TxnNamedRead#performLocalKeyRead}) executes its {@code ReadCommand} locally without
     *     going through {@link TransparentReads}, so it returns hot rows only and silently omits every
     *     row already encoded into a chunk. Accord <em>writes</em> are safe (they route through
     *     {@code CQL3CasRequest#createWriteFragments} into the cold-immutability guard), so this is a
     *     read-side hole only -- but it is exactly the guarantee tiering exists to give.</li>
     * </ul>
     *
     * <p>This is a pure function of {@code metadata}, re-evaluated by
     * {@link TieredStorageService#runOnce} on every re-encode cycle, so it does not matter in which
     * order an operator installs a policy and changes the schema: whichever comes second, the next
     * cycle refuses to encode and logs why.
     *
     * @return {@code null} if tiering supports {@code metadata}, else a human-readable reason naming
     * the offending column/index/view.
     */
    public static String unsupportedSchemaError(TableMetadata metadata)
    {
        // A policy on the VIEW itself, rather than on its base table. Nothing would ever chunk it --
        // the re-encoder's writes go through the normal write path, which a view rejects -- and its
        // rows are derived, so the honest answer is that a view is not a tiering target at all.
        if (metadata.isView())
            return format("'%s' is a materialized view: put the timeseries_tiering policy on the base " +
                           "table instead. A view's rows are derived from its base table, so there is nothing " +
                           "for the re-encoder to own", metadata.name);

        // Accord's read path is the one read path that does not merge chunks back in: TxnNamedRead
        // executes the ReadCommand locally, with none of TransparentReads' wrapping, so a transactional
        // read of a tiered table would answer from the hot window alone and silently drop all cold
        // history. Every mode but 'off' enables Accord (TransactionalMode.accordIsEnabled), and
        // 'mixed_reads' routes even a plain SERIAL read through it, so 'off' is the only safe setting
        // until the Accord read path is hooked up.
        TransactionalMode mode = metadata.params.transactionalMode;
        if (mode.accordIsEnabled)
            return format("transactional_mode is '%s': an Accord transactional read executes locally without the " +
                           "chunk merge every other read path applies, so it would return only rows younger than " +
                           "hot_window and silently omit all tiered history. Set transactional_mode = 'off' to make " +
                           "this table tierable",
                           mode.name());

        if (metadata.clusteringColumns().size() != 1)
            return format("expected exactly 1 clustering column, found %d: a chunk encodes one time axis, " +
                           "so the table must be clustered by time alone",
                           metadata.clusteringColumns().size());

        ColumnMetadata clustering = metadata.clusteringColumns().get(0);
        // unwrap(): CLUSTERING ORDER BY (ts DESC) wraps the column type in ReversedType(timestamp);
        // both orders are supported (the re-encoder's queries all say ORDER BY ... ASC explicitly).
        if (!clustering.type.unwrap().equals(TimestampType.instance))
            return format("expected clustering column '%s' to be of type timestamp, found %s",
                           clustering.name.toCQLString(), clustering.type.asCQL3Type());

        // Counters are checked across every column, not just the regular ones: a counter table's
        // deletes are irreversible, and Cassandra refuses the re-encoder's `DELETE ... USING TIMESTAMP`
        // on one outright, so there is no shape of counter table this can work on.
        // columnsInFixedOrder(), not columns(): the latter iterates in hash order, which would make
        // *which* offending column the message names vary between JVMs.
        for (ColumnMetadata column : metadata.columnsInFixedOrder())
        {
            if (column.type.isCounter())
                return format("column '%s' is a counter: the re-encoder deletes rows after encoding them, and a " +
                               "deleted counter can never be written again, so tiering a counter table would " +
                               "destroy it",
                               column.name.toCQLString());
        }

        // regularColumns() is itself a fixed-order (comparator-sorted) collection.
        for (ColumnMetadata column : metadata.regularColumns())
        {
            // isMultiCell() covers non-frozen collections and non-frozen UDTs alike -- both store one
            // cell per element, with per-element timestamps a chunk's per-row bytes cannot carry.
            if (column.type.isMultiCell())
                return format("regular column '%s' (%s) is a non-frozen (multi-cell) type: its per-element cells " +
                               "and timestamps cannot be encoded into a chunk. Freeze it (frozen<...>) to make " +
                               "this table tierable",
                               column.name.toCQLString(), column.type.asCQL3Type());
        }

        for (ColumnMetadata column : metadata.partitionKeyColumns())
        {
            if (ChunkTables.RESERVED_COLUMN_NAMES.contains(column.name.toString()))
                return format("partition key column '%s' collides with a column the chunk table defines itself " +
                               "(%s); rename it to make this table tierable",
                               column.name.toCQLString(), String.join(", ", ChunkTables.RESERVED_COLUMN_NAMES));
        }

        String indexError = unsafeIndexError(metadata);
        if (indexError != null)
            return indexError;

        return materializedViewError(metadata);
    }

    /**
     * @return why {@code metadata}'s secondary indexes make it un-tierable, or {@code null} if none do.
     * Every index is resolved to its target column via {@link TargetParser} (the same parse the index
     * implementations themselves use, so {@code values(m)}/{@code keys(m)}/quoted names all resolve
     * identically); an index whose target cannot be resolved is rejected rather than assumed safe.
     * <p>
     * Only a <b>static</b> target is safe, and the reason is specific to static columns rather than to
     * "not a regular column": every other kind of index entry is keyed by a base <em>row</em>, so the
     * re-encoder's clustering-range delete removes it. That includes a clustering-column index (which
     * CQL permits -- {@code CreateIndexStatement} only bans indexing the sole partition key column) and
     * an index on one component of a composite partition key, whose entries also disappear once every
     * row of the partition has been chunked. A static column's entries sit at
     * {@code Clustering.STATIC_CLUSTERING}, outside every clustering range the re-encoder deletes.
     */
    private static String unsafeIndexError(TableMetadata metadata)
    {
        for (IndexMetadata index : metadata.indexes)
        {
            String target = index.options.get(IndexTarget.TARGET_OPTION_NAME);
            if (target == null)
                return format("index '%s' records no target column, so tiering cannot establish that it is on a " +
                               "static column (the only kind of index that survives tiering)", index.name);

            Pair<ColumnMetadata, IndexTarget.Type> parsed = TargetParser.parse(metadata, target);
            if (parsed == null)
                return format("index '%s' targets '%s', which does not resolve to a column of this table, so " +
                               "tiering cannot establish that it is on a static column (the only kind of index " +
                               "that survives tiering)",
                               index.name, target);

            if (!parsed.left.isStatic())
                return format("index '%s' is on %s column '%s': index entries are per base row, and re-encoded " +
                               "rows are deleted from the base table, so they vanish from the index and index " +
                               "queries would silently return only data younger than hot_window. Only indexes on " +
                               "static columns survive tiering, because static entries live outside every " +
                               "clustering range the re-encoder deletes",
                               index.name, columnKind(parsed.left), parsed.left.name.toCQLString());
        }
        return null;
    }

    private static String columnKind(ColumnMetadata column)
    {
        if (column.isPartitionKey())
            return "partition key";
        if (column.isClusteringColumn())
            return "clustering";
        return "regular";
    }

    /**
     * @return why {@code metadata}'s materialized views make it un-tierable, or {@code null} if it has
     * none. Returns {@code null} when the keyspace is not registered in {@link Schema} (metadata built
     * directly in a test): there is nothing to look up, and the other rules still apply.
     */
    private static String materializedViewError(TableMetadata metadata)
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
        if (keyspace == null)
            return null;

        for (ViewMetadata view : keyspace.views.forTable(metadata.id))
            return format("materialized view '%s' is built on this table: the re-encoder's delete of an encoded " +
                           "window propagates into the view, but transparent reads only rebuild rows for the base " +
                           "table, so the view would silently lose everything older than hot_window. Drop the view " +
                           "to make this table tierable", view.name());

        return null;
    }

    /**
     * @return a warning for the operator when {@code metadata}'s {@code default_time_to_live} expires
     * rows before {@code policy}'s {@code hot_window} ever releases them for encoding -- i.e. tiering
     * is configured but can never compress anything -- else {@code null}.
     * <p>
     * A warning, not a rejection: {@code default_time_to_live} is only the default, and a writer that
     * says {@code USING TTL} per row (or 0) is unaffected by it, so the combination is legal -- just
     * almost always a misconfiguration.
     */
    public static String ttlShadowsHotWindowWarning(TableMetadata metadata, TieringPolicy policy)
    {
        long ttlSeconds = metadata.params.defaultTimeToLive;
        if (ttlSeconds <= 0)
            return null;

        long ttlMillis = TimeUnit.SECONDS.toMillis(ttlSeconds);
        if (policy.hotWindowMillis < ttlMillis)
            return null;

        return format("%s.%s has default_time_to_live = %ds but a timeseries_tiering hot_window of %dms " +
                       "(%ds): rows expire before the re-encoder is allowed to touch them, so nothing will ever " +
                       "be compressed. Lower hot_window below the TTL, or raise/clear default_time_to_live",
                       metadata.keyspace, metadata.name, ttlSeconds, policy.hotWindowMillis,
                       TimeUnit.MILLISECONDS.toSeconds(policy.hotWindowMillis));
    }

    /**
     * @return a warning if {@code metadata} has a finite {@code default_time_to_live} but {@code policy}
     * sets no {@code cold_window}, else {@code null}.
     *
     * <p>Tiering silently converts such a table from bounded retention to unbounded growth, and does it
     * irreversibly. The chunk table deliberately does <b>not</b> inherit the base table's TTL (see
     * {@link ChunkTables#createChunkTableStatement} -- a chunk written today holding month-old rows
     * must not expire on today's schedule), and the re-encoder's chunk insert carries {@code USING
     * TIMESTAMP} but no {@code USING TTL}, so a row's TTL is dropped the moment its value is copied
     * into a chunk. Every row the re-encoder touches therefore stops expiring, while its base copy is
     * deleted -- so the data is not lost, it is kept <em>forever</em>, and there is no un-tier to
     * undo it with.
     *
     * <p>{@code cold_window} is the supported way to express retention on a tiered table, which is why
     * this is a warning about a missing setting rather than a refusal: dropping TTL is legitimate when
     * it is what the operator wants (raising the base TTL to a decade and tiering is a reasonable way
     * to keep a decade of history compressed). It must just not happen by accident, which is exactly
     * what it did before this check existed -- the pre-existing
     * {@link #ttlShadowsHotWindowWarning} covers only the opposite direction, a TTL so short that
     * rows expire before tiering can reach them.
     */
    public static String ttlWithoutColdWindowWarning(TableMetadata metadata, TieringPolicy policy)
    {
        long ttlSeconds = metadata.params.defaultTimeToLive;
        if (ttlSeconds <= 0 || policy.coldWindowMillis >= 0)
            return null;

        return format("%s.%s has default_time_to_live = %ds but its timeseries_tiering policy sets no cold_window: " +
                       "the re-encoder drops a row's TTL when it copies the value into a chunk (chunks carry no TTL " +
                       "of their own), so every window it encodes stops expiring and this table's data is kept " +
                       "forever rather than for %ds. This cannot be undone once the base rows are deleted. Set " +
                       "cold_window to the retention you want, or raise/clear default_time_to_live if keeping " +
                       "everything is intended",
                       metadata.keyspace, metadata.name, ttlSeconds, ttlSeconds);
    }

    @Override
    public String toString()
    {
        return format("TieringPolicy{hot_window=%dms, chunk_window=%dms, cold_window=%s, consistency=%s, interval=%dms}",
                       hotWindowMillis, chunkWindowMillis,
                       coldWindowMillis < 0 ? "unset" : coldWindowMillis + "ms",
                       consistency, intervalMillis);
    }

    private static String requireString(Map<String, Object> raw, String key)
    {
        Object value = raw.get(key);
        if (value == null)
            return null;
        if (!(value instanceof String))
            throw new ConfigurationException(format("%s.%s must be a string, got '%s'", EXTENSION_KEY, key, value));
        return (String) value;
    }

    /**
     * @return {@code true} if {@code cl} is one of the quorum-strength levels a policy may name for
     * the re-encoder's own reads and writes -- i.e. one at which anything written by any accepted
     * policy is guaranteed visible. {@link ChunkCoverage} uses it to refuse to consult the coverage
     * ledger any more weakly than the re-encoder wrote it, whatever the asking query's own CL is.
     */
    static boolean isQuorumStrength(ConsistencyLevel cl)
    {
        return cl != null && ALLOWED_CONSISTENCY_LEVELS.contains(cl);
    }

    private static ConsistencyLevel parseConsistency(String value)
    {
        ConsistencyLevel consistency;
        try
        {
            consistency = ConsistencyLevel.fromString(value);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(format("%s.%s is not a valid consistency level: '%s'",
                                                      EXTENSION_KEY, CONSISTENCY, value));
        }

        if (!ALLOWED_CONSISTENCY_LEVELS.contains(consistency))
            throw new ConfigurationException(format(
                "%s.%s must be one of QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL (got '%s'): a weaker " +
                "consistency lets the re-encoder's existing-chunk read miss the previous cycle's chunk, " +
                "producing a merge that loses data while the source rows still get deleted",
                EXTENSION_KEY, CONSISTENCY, value));

        return consistency;
    }

    private static long parseDurationMillis(String key, String value)
    {
        Matcher matcher = DURATION.matcher(value == null ? "" : value.trim());
        if (!matcher.matches())
            throw new ConfigurationException(format("%s.%s must look like 10m, 1h or 30d, got '%s'", EXTENSION_KEY, key, value));

        long amount;
        try
        {
            amount = Long.parseLong(matcher.group(1));
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(format("%s.%s value '%s' is out of range", EXTENSION_KEY, key, value));
        }

        switch (matcher.group(2))
        {
            case "m": return TimeUnit.MINUTES.toMillis(amount);
            case "h": return TimeUnit.HOURS.toMillis(amount);
            default:  return TimeUnit.DAYS.toMillis(amount);
        }
    }
}
