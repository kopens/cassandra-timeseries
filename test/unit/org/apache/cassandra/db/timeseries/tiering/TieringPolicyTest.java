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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TieringPolicyTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static TableMetadata canonicalTable(String json)
    {
        TableMetadata.Builder builder = TableMetadata.builder("ks", "tbl")
                                                      .addPartitionKeyColumn("tag", UTF8Type.instance)
                                                      .addClusteringColumn("ts", TimestampType.instance)
                                                      .addRegularColumn("value", DoubleType.instance);
        if (json != null)
            builder.params(TableParams.builder()
                                      .extensions(ImmutableMap.of(TieringPolicy.EXTENSION_KEY, ByteBufferUtil.bytes(json)))
                                      .build());
        return builder.build();
    }

    private static TableMetadata tableWithTtl(int ttlSeconds)
    {
        return TableMetadata.builder("ks", "tbl")
                             .addPartitionKeyColumn("tag", UTF8Type.instance)
                             .addClusteringColumn("ts", TimestampType.instance)
                             .addRegularColumn("value", DoubleType.instance)
                             .params(TableParams.builder().defaultTimeToLive(ttlSeconds).build())
                             .build();
    }

    /** The shape of the production table this work exists for: 7 static, 7 regular of mixed types, DESC. */
    private static TableMetadata productionShapedTable()
    {
        return TableMetadata.builder("pp", "tm_tag_point")
                             .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                             .addClusteringColumn("timestamp", ReversedType.getInstance(TimestampType.instance))
                             .addStaticColumn("area_id", UTF8Type.instance)
                             .addStaticColumn("asset_id", UTF8Type.instance)
                             .addStaticColumn("line_id", UTF8Type.instance)
                             .addStaticColumn("opc_id", UTF8Type.instance)
                             .addStaticColumn("site_id", UTF8Type.instance)
                             .addStaticColumn("tag_name", UTF8Type.instance)
                             .addStaticColumn("type", UTF8Type.instance)
                             .addRegularColumn("attribute", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                             .addRegularColumn("error_code", Int32Type.instance)
                             .addRegularColumn("latency", Int32Type.instance)
                             .addRegularColumn("quality", Int32Type.instance)
                             .addRegularColumn("value", UTF8Type.instance)
                             .addRegularColumn("value_boolean", BooleanType.instance)
                             .addRegularColumn("value_numeric", DoubleType.instance)
                             .build();
    }

    /** An index definition carrying only what {@link TieringPolicy} reads from one: its target. */
    private static IndexMetadata index(String name, String target)
    {
        return IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, ImmutableMap.of("target", target));
    }

    // ---- parsing + defaults ----

    @Test
    public void testParsesAllFields()
    {
        TieringPolicy policy = TieringPolicy.parse(
            "{\"hot_window\":\"7d\", \"chunk_window\":\"1h\", \"cold_window\":\"365d\", " +
            "\"consistency\":\"QUORUM\", \"interval\":\"10m\"}");

        assertEquals(TimeUnit.DAYS.toMillis(7), policy.hotWindowMillis);
        assertEquals(TimeUnit.HOURS.toMillis(1), policy.chunkWindowMillis);
        assertEquals(TimeUnit.DAYS.toMillis(365), policy.coldWindowMillis);
        assertEquals(ConsistencyLevel.QUORUM, policy.consistency);
        assertEquals(TimeUnit.MINUTES.toMillis(10), policy.intervalMillis);
    }

    @Test
    public void testDefaults()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"7d\"}");

        assertEquals(TimeUnit.DAYS.toMillis(7), policy.hotWindowMillis);
        assertEquals(TimeUnit.HOURS.toMillis(1), policy.chunkWindowMillis);
        assertEquals(-1, policy.coldWindowMillis);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, policy.consistency);
        assertEquals(TimeUnit.MINUTES.toMillis(5), policy.intervalMillis);
    }

    @Test
    public void testHotWindowEqualToChunkWindowIsAllowed()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1h\", \"chunk_window\":\"1h\"}");
        assertEquals(policy.chunkWindowMillis, policy.hotWindowMillis);
    }

    @Test
    public void testToStringSummarizesPolicy()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"7d\"}");
        String s = policy.toString();
        assertTrue(s.contains("hot_window"));
        assertTrue(s.contains("chunk_window"));
        assertTrue(s.contains("LOCAL_QUORUM"));
    }

    // ---- validation rejections ----

    @Test
    public void testMissingHotWindowRejected()
    {
        assertConfigurationException("{\"chunk_window\":\"1h\"}", "hot_window");
    }

    @Test
    public void testUnknownKeyRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"bogus_key\":\"1h\"}", "bogus_key");
    }

    @Test
    public void testHotWindowLessThanChunkWindowRejected()
    {
        assertConfigurationException("{\"hot_window\":\"1h\", \"chunk_window\":\"2h\"}", null);
    }

    @Test
    public void testChunkWindowAtCapAccepted()
    {
        // 31d is the documented maximum chunk_window -- the boundary itself must parse.
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"40d\", \"chunk_window\":\"31d\"}");
        assertEquals(TimeUnit.DAYS.toMillis(31), policy.chunkWindowMillis);
    }

    @Test
    public void testChunkWindowOverCapRejected()
    {
        // An unbounded chunk_window defeats the re-encoder's one-window-at-a-time memory bound (and can
        // exceed the codec's per-chunk sample limit), so anything over 31d is rejected at parse time.
        // The message must name both the offending value and the cap so the operator can act on it.
        assertConfigurationException("{\"hot_window\":\"33d\", \"chunk_window\":\"32d\"}", "32d");
        assertConfigurationException("{\"hot_window\":\"33d\", \"chunk_window\":\"32d\"}", "31d");
        assertConfigurationException("{\"hot_window\":\"370d\", \"chunk_window\":\"365d\"}", "365d");
        assertConfigurationException("{\"hot_window\":\"370d\", \"chunk_window\":\"365d\"}", "31d");
    }

    @Test
    public void testColdWindowEqualToHotWindowRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"cold_window\":\"7d\"}", null);
    }

    @Test
    public void testColdWindowLessThanHotWindowRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"cold_window\":\"1d\"}", null);
    }

    @Test
    public void testBadDurationUnitRejected()
    {
        assertConfigurationException("{\"hot_window\":\"1w\"}", null);
    }

    @Test
    public void testZeroDurationRejected()
    {
        assertConfigurationException("{\"hot_window\":\"0h\"}", null);
    }

    @Test
    public void testEmptyDurationRejected()
    {
        assertConfigurationException("{\"hot_window\":\"\"}", null);
    }

    @Test
    public void testRemovedCodecKeyRejectedByName()
    {
        // `codec` was removed when the chunk format stopped being a per-policy choice. A stored policy that still sets
        // it must fail loudly and name the key -- silently ignoring it would leave an operator
        // believing a codec choice is still in force.
        assertConfigurationException("{\"hot_window\":\"7d\", \"codec\":\"auto\"}", "codec");
        assertConfigurationException("{\"hot_window\":\"7d\", \"codec\":\"gorilla\"}", "no longer supported");
        assertConfigurationException("{\"hot_window\":\"7d\", \"codec\":\"chimp128\"}", "the chunk format is fixed by the build");
    }

    @Test
    public void testBadConsistencyRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"consistency\":\"NOT_A_LEVEL\"}", "consistency");
    }

    @Test
    public void testWeakConsistencyRejected()
    {
        // ONE (and TWO/THREE/LOCAL_ONE/ANY) would let the existing-chunk read miss a prior cycle's
        // chunk, so weaker-than-quorum levels are rejected outright -- see ALLOWED_CONSISTENCY_LEVELS.
        assertConfigurationException("{\"hot_window\":\"7d\", \"consistency\":\"ONE\"}", "consistency");
    }

    @Test
    public void testMalformedJsonRejected()
    {
        assertConfigurationException("{not json", null);
    }

    private static void assertConfigurationException(String json, String expectedSubstring)
    {
        try
        {
            TieringPolicy.parse(json);
            fail("Expected ConfigurationException for: " + json);
        }
        catch (ConfigurationException e)
        {
            if (expectedSubstring != null)
                assertTrue("Expected message to contain '" + expectedSubstring + "', was: " + e.getMessage(),
                           e.getMessage().contains(expectedSubstring));
        }
    }

    // ---- windowStartFor ----

    @Test
    public void testWindowStartForFloorsToChunkWindow()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1h\", \"chunk_window\":\"1h\"}");
        long hourMillis = TimeUnit.HOURS.toMillis(1);

        assertEquals(0L, policy.windowStartFor(0L));
        assertEquals(0L, policy.windowStartFor(hourMillis - 1));
        assertEquals(hourMillis, policy.windowStartFor(hourMillis));
        assertEquals(hourMillis, policy.windowStartFor(hourMillis + 1));
    }

    @Test
    public void testWindowStartForNegativeTimestamps()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1h\", \"chunk_window\":\"1h\"}");
        long hourMillis = TimeUnit.HOURS.toMillis(1);

        // Math.floorMod behaviour: floors toward negative infinity, not toward zero.
        assertEquals(-hourMillis, policy.windowStartFor(-1L));
        assertEquals(-hourMillis, policy.windowStartFor(-hourMillis));
        assertEquals(-2 * hourMillis, policy.windowStartFor(-hourMillis - 1));
    }

    // ---- unsupportedSchemaError: accepted shapes ----

    @Test
    public void testSingleValueSchemaIsAccepted()
    {
        assertNull(TieringPolicy.unsupportedSchemaError(canonicalTable(null)));
    }

    @Test
    public void testDescClusteredSchemaIsAccepted()
    {
        // CLUSTERING ORDER BY (ts DESC) wraps the clustering column type in ReversedType(timestamp);
        // it is still supported -- newest-first is the dominant time-series clustering idiom.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", ReversedType.getInstance(TimestampType.instance))
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        assertNull(TieringPolicy.unsupportedSchemaError(table));
    }

    @Test
    public void testCompositePartitionKeyAccepted()
    {
        // A real production table: PRIMARY KEY ((asset_id, date, hour), ts).
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("asset_id", UTF8Type.instance)
                                            .addPartitionKeyColumn("date", UTF8Type.instance)
                                            .addPartitionKeyColumn("hour", Int32Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        assertNull(TieringPolicy.unsupportedSchemaError(table));
    }

    @Test
    public void testManyRegularColumnsOfMixedTypesAccepted()
    {
        // The shape of pp.tm_tag_point: seven regular columns, mixed types, incl. a frozen map.
        assertNull(TieringPolicy.unsupportedSchemaError(productionShapedTable()));
    }

    @Test
    public void testStaticColumnsAccepted()
    {
        // Static columns are never chunked: a clustering-range delete leaves them alone (static cells
        // live outside the clustering range), so they survive tiering untouched and need no rules --
        // not even the non-frozen-collection one that applies to regular columns.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addStaticColumn("site_id", UTF8Type.instance)
                                            .addStaticColumn("labels", MapType.getInstance(UTF8Type.instance,
                                                                                            UTF8Type.instance, true))
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        assertNull(TieringPolicy.unsupportedSchemaError(table));
    }

    @Test
    public void testFrozenCollectionRegularColumnAccepted()
    {
        // frozen<map<text,text>> is a single cell -- a chunk can carry it as opaque bytes.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("attribute", MapType.getInstance(UTF8Type.instance,
                                                                                                UTF8Type.instance, false))
                                            .build();
        assertNull(TieringPolicy.unsupportedSchemaError(table));
    }

    @Test
    public void testIndexOnStaticColumnAccepted()
    {
        // The real table carries SAI on static asset_id/opc_id. A static column's index entries live at
        // Clustering.STATIC_CLUSTERING, outside every clustering range the re-encoder deletes, so they
        // are the only index entries that survive tiering.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addStaticColumn("asset_id", UTF8Type.instance)
                                            .addStaticColumn("opc_id", UTF8Type.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .indexes(Indexes.of(index("by_asset", "asset_id"),
                                                                index("by_opc", "opc_id")))
                                            .build();
        assertNull(TieringPolicy.unsupportedSchemaError(table));
    }

    @Test
    public void testTableWithNoRegularColumnsAccepted()
    {
        // A pure event log: the timestamp axis is itself the data, and a chunk encodes it. Deliberately
        // accepted -- there is no reason to demand a value column.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addStaticColumn("site_id", UTF8Type.instance)
                                            .build();
        assertNull(TieringPolicy.unsupportedSchemaError(table));
    }

    // ---- unsupportedSchemaError: rejected shapes ----

    @Test
    public void testNonTimestampClusteringRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", Int32Type.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("timestamp"));
        assertTrue(error, error.contains("ts"));
    }

    @Test
    public void testNoClusteringColumnRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("exactly 1 clustering column"));
    }

    @Test
    public void testTwoClusteringColumnsRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addClusteringColumn("seq", Int32Type.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("exactly 1 clustering column"));
    }

    /**
     * An Accord transactional read is the one read path that never merges chunks back in:
     * {@code TxnNamedRead#performLocalKeyRead} runs the {@code ReadCommand} locally with none of
     * {@code TransparentReads}' wrapping, so it answers from the hot window alone. Every mode but
     * {@code off} enables Accord, and {@code mixed_reads} routes even a plain SERIAL read through it,
     * so the whole non-{@code off} set has to be refused while the Accord read path is unhooked.
     */
    @Test
    public void testTransactionalModeRejected()
    {
        for (TransactionalMode mode : TransactionalMode.values())
        {
            TableMetadata table = canonicalTable(null).unbuild()
                                                       .params(TableParams.builder().transactionalMode(mode).build())
                                                       .build();
            String error = TieringPolicy.unsupportedSchemaError(table);
            if (mode == TransactionalMode.off)
            {
                assertNull("transactional_mode 'off' is the supported setting", error);
                continue;
            }
            assertNotNull("transactional_mode '" + mode.name() + "' must be rejected", error);
            assertTrue(error, error.contains("transactional_mode"));
            assertTrue(error, error.contains(mode.name()));
        }
    }

    @Test
    public void testCounterColumnRejected()
    {
        // 192 of the production keyspace's columns are counters. A counter cannot be deleted and
        // re-inserted, which is exactly what the re-encoder does -- so this is a correctness stop.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("hits", CounterColumnType.instance)
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("counter"));
        assertTrue(error, error.contains("hits"));
    }

    @Test
    public void testNonFrozenCollectionRegularColumnRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("labels", MapType.getInstance(UTF8Type.instance,
                                                                                             UTF8Type.instance, true))
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("labels"));
        assertTrue(error, error.contains("non-frozen"));
    }

    @Test
    public void testIndexOnRegularColumnRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .indexes(Indexes.of(index("by_value", "value")))
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("by_value"));
        assertTrue(error, error.contains("regular column 'value'"));
    }

    @Test
    public void testIndexOnClusteringColumnRejected()
    {
        // CQL permits CREATE INDEX ON t(ts) -- CreateIndexStatement only bans indexing the sole
        // partition key column -- and its entries are per row, so the re-encoder's range delete removes
        // them exactly as it does a regular column's. `SELECT ... WHERE ts = ?` would then silently
        // return only rows younger than hot_window.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .indexes(Indexes.of(index("by_ts", "ts")))
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("by_ts"));
        assertTrue(error, error.contains("clustering column 'ts'"));
    }

    @Test
    public void testIndexOnPartitionKeyComponentRejected()
    {
        // One component of a composite key: also per-row entries, so once every row of a partition has
        // been chunked that partition contributes nothing to the index.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("asset_id", UTF8Type.instance)
                                            .addPartitionKeyColumn("date", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .indexes(Indexes.of(index("by_date", "date")))
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("by_date"));
        assertTrue(error, error.contains("partition key column 'date'"));
    }

    @Test
    public void testIndexWithAnUnresolvableTargetRejected()
    {
        // An index whose target cannot be resolved to a column is rejected rather than assumed safe:
        // "cannot prove it is not on a regular column" must not read as "it is not".
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .indexes(Indexes.of(index("by_ghost", "no_such_column")))
                                            .build();
        String error = TieringPolicy.unsupportedSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("by_ghost"));
    }

    @Test
    public void testPartitionKeyCollidingWithAChunkTableColumnRejected()
    {
        // The chunk table mirrors the base partition key and adds window_start/codec/samples/
        // max_row_writetime/payload -- a base key column of the same name could not be mirrored.
        for (String reserved : ChunkTables.RESERVED_COLUMN_NAMES)
        {
            TableMetadata table = TableMetadata.builder("ks", "tbl")
                                                .addPartitionKeyColumn(reserved, UTF8Type.instance)
                                                .addClusteringColumn("ts", TimestampType.instance)
                                                .addRegularColumn("value", DoubleType.instance)
                                                .build();
            String error = TieringPolicy.unsupportedSchemaError(table);
            assertNotNull(reserved, error);
            assertTrue(error, error.contains(reserved));
        }
    }

    // ---- ttlShadowsHotWindowWarning ----

    @Test
    public void testTtlBelowHotWindowWarns()
    {
        // default_time_to_live 62d with hot_window 90d: the rows are gone before the re-encoder may
        // touch them, so tiering is configured but can never compress anything.
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"90d\"}");
        String warning = TieringPolicy.ttlShadowsHotWindowWarning(tableWithTtl(5_356_800), policy);
        assertNotNull(warning);
        assertTrue(warning, warning.contains("5356800"));
        assertTrue(warning, warning.contains("default_time_to_live"));
        assertTrue(warning, warning.contains("hot_window"));
    }

    @Test
    public void testTtlEqualToHotWindowWarns()
    {
        // Exactly equal is still unusable: a row expires the instant it stops being hot.
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1d\"}");
        assertNotNull(TieringPolicy.ttlShadowsHotWindowWarning(tableWithTtl(86_400), policy));
    }

    @Test
    public void testTtlAboveHotWindowDoesNotWarn()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1d\"}");
        assertNull(TieringPolicy.ttlShadowsHotWindowWarning(tableWithTtl(86_401), policy));
    }

    @Test
    public void testNoTtlDoesNotWarn()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"90d\"}");
        assertNull(TieringPolicy.ttlShadowsHotWindowWarning(canonicalTable(null), policy));
    }

    // ---- ttlWithoutColdWindowWarning ----

    @Test
    public void testTtlWithoutColdWindowWarns()
    {
        // The dangerous, silent, irreversible case: a table with real retention gets a tiering policy
        // that names no cold_window. The re-encoder drops each row's TTL when it copies the value into
        // a chunk (chunks carry none of their own) and then deletes the base row -- so 31 days of
        // retention quietly becomes forever, with no un-tier to undo it.
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"3h\",\"chunk_window\":\"1h\"}");
        String warning = TieringPolicy.ttlWithoutColdWindowWarning(tableWithTtl(2_678_400), policy);
        assertNotNull(warning);
        assertTrue(warning, warning.contains("2678400"));
        assertTrue(warning, warning.contains("cold_window"));
    }

    @Test
    public void testTtlWithColdWindowDoesNotWarn()
    {
        // cold_window is the supported way to express retention on a tiered table, so setting one is
        // exactly the fix this warning asks for -- it must then go quiet.
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"3h\",\"cold_window\":\"31d\"}");
        assertNull(TieringPolicy.ttlWithoutColdWindowWarning(tableWithTtl(2_678_400), policy));
    }

    @Test
    public void testNoTtlWithoutColdWindowDoesNotWarn()
    {
        // Nothing was expiring in the first place, so nothing stops expiring.
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"3h\"}");
        assertNull(TieringPolicy.ttlWithoutColdWindowWarning(canonicalTable(null), policy));
    }

    // ---- fromTable ----

    @Test
    public void testFromTableReturnsNullWhenExtensionAbsent()
    {
        assertNull(TieringPolicy.fromTable(canonicalTable(null)));
    }

    @Test
    public void testFromTableParsesExtension()
    {
        TableMetadata table = canonicalTable("{\"hot_window\":\"7d\"}");
        TieringPolicy policy = TieringPolicy.fromTable(table);
        assertNotNull(policy);
        assertEquals(TimeUnit.DAYS.toMillis(7), policy.hotWindowMillis);
    }
}
