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

import java.util.Date;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * SP4 Task 2: the schemas tiering accepts and rejects, exercised against real CQL schemas rather than
 * hand-built {@link TableMetadata} (see {@link TieringPolicyTest} for the metadata-level matrix).
 * <p>
 * The cases that need a live schema are here because they cannot be built by hand: real secondary
 * indexes with their real target strings, materialized views (which are registered on the keyspace,
 * not the table), and the end-to-end proofs -- that a composite partition key round-trips through the
 * mirrored chunk table, and that static columns survive the re-encoder's clustering-range delete.
 */
public class TieringSchemaSupportTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    // ---- rejections that need a live schema ----

    @Test
    public void materializedViewOverTheTableIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));

        // createViewAsync, not createView: the schema change itself is what this asserts on, and
        // waiting for the (irrelevant, empty-table) view build costs minutes in this harness.
        String view = createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                      "WHERE tag IS NOT NULL AND ts IS NOT NULL AND value IS NOT NULL " +
                                      "PRIMARY KEY (value, tag, ts)");

        // The re-encoder's range delete propagates into the view, but transparent reads only rebuild
        // rows for the base table -- so the view would silently lose all history older than hot_window.
        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains(view));
        assertTrue(error, error.contains("materialized view"));
    }

    @Test
    public void secondaryIndexOnARegularColumnIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'sai'");

        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains(index));
        assertTrue(error, error.contains("value"));
    }

    @Test
    public void secondaryIndexOnTheClusteringColumnIsRejected() throws Throwable
    {
        // Proves the premise as well as the rule: CQL really does allow indexing the timestamp
        // clustering column, and those entries are per row, so the range delete takes them with it.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(ts) USING 'sai'");

        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains(index));
        assertTrue(error, error.contains("clustering column 'ts'"));
    }

    @Test
    public void secondaryIndexOnAStaticColumnIsAccepted() throws Throwable
    {
        // The production table carries SAI on static asset_id/opc_id: static cells are never chunked
        // and never deleted (their index entries sit at Clustering.STATIC_CLUSTERING, outside every
        // clustering range the re-encoder deletes), so the index stays complete.
        createTable("CREATE TABLE %s (tag text, ts timestamp, asset_id text static, opc_id text static, " +
                    "value double, PRIMARY KEY (tag, ts))");
        createIndex("CREATE CUSTOM INDEX ON %s(asset_id) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX ON %s(opc_id) USING 'sai'");

        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));
    }

    /**
     * The chunk table holds the only copy of every tiered row (the base copies are deleted when they
     * are encoded), and transparent reads resolve through the <em>base</em> table's name -- so a
     * {@code DROP TABLE} of the base while its shadows exist would strand the cold data silently.
     * The drop must be refused until the shadows are gone, making their destruction an explicit act.
     */
    @Test
    public void dropOfTheBaseTableIsRefusedWhileShadowTablesExist() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        ChunkTables.ensureChunkTable(metadata());

        String base = KEYSPACE + '.' + currentTable();
        String chunks = KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
        String coverage = KEYSPACE + '.' + ChunkTables.coverageTableName(currentTable());
        String tags = KEYSPACE + '.' + ChunkTables.tagsTableName(currentTable());

        assertInvalidThrowMessage("tiering shadow tables still exist", InvalidRequestException.class,
                                  "DROP TABLE " + base);
        // While the chunk table survives, the refusal says which table holds the data...
        assertInvalidThrowMessage("only copy", InvalidRequestException.class, "DROP TABLE " + base);
        // ...and how to proceed.
        assertInvalidThrowMessage("extensions = {}", InvalidRequestException.class, "DROP TABLE " + base);

        // With the chunk table gone the remaining shadows are metadata-only: still refused (they
        // would linger as orphans), but without the data-loss sentence.
        execute("DROP TABLE " + chunks);
        assertInvalidThrowMessage("tiering shadow tables still exist", InvalidRequestException.class,
                                  "DROP TABLE " + base);

        execute("DROP TABLE " + coverage);
        execute("DROP TABLE " + tags);
        execute("DROP TABLE " + base);
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, currentTable()));
    }

    /**
     * The reverse direction: dropping the chunk table out from under a base table whose policy is
     * still attached destroys the only copy of the tiered rows, and the sweeper then recreates the
     * table empty -- so the loss would be silent. Detaching the policy is the explicit step that
     * unlocks the drop.
     */
    @Test
    public void dropOfTheChunkTableIsRefusedWhileThePolicyIsAttached() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        ChunkTables.ensureChunkTable(metadata());
        alterTable("ALTER TABLE %s WITH extensions = " +
                   "{'" + TieringPolicy.EXTENSION_KEY + "': '{\"hot_window\":\"12h\"}'};");

        String chunks = KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
        assertInvalidThrowMessage("tiering policy is still attached", InvalidRequestException.class,
                                  "DROP TABLE " + chunks);
        assertInvalidThrowMessage("only copy", InvalidRequestException.class, "DROP TABLE " + chunks);

        alterTable("ALTER TABLE %s WITH extensions = {};");
        schemaChange("DROP TABLE " + chunks);
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())));
    }

    /**
     * The guard matches on shape, not just on name: a user table that merely reuses the
     * {@code __chunks} suffix (none of the chunk columns) must not hold the base table's drop hostage.
     */
    @Test
    public void anUnrelatedTableReusingTheChunkSuffixDoesNotBlockTheDrop() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        schemaChange("CREATE TABLE " + KEYSPACE + '.' + table + "__chunks (k text PRIMARY KEY, v int)");

        schemaChange("DROP TABLE " + KEYSPACE + '.' + table);
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, table));

        schemaChange("DROP TABLE " + KEYSPACE + '.' + table + "__chunks");
    }

    /**
     * The Accord read hole, from both ends. {@code TxnNamedRead#performLocalKeyRead} executes its
     * {@code ReadCommand} locally with none of {@code TransparentReads}' wrapping, so a transactional
     * read of a tiered table answers from the hot window alone and silently omits every chunked row.
     * <p>
     * Both orders matter and both are covered here, because the check is a pure function of the
     * table's metadata evaluated by {@link TieredStorageService#runOnce} on every cycle: it does not
     * matter whether the policy or the mode came first, only what the table looks like now.
     */
    @Test
    public void transactionalModeIsRejectedWhicheverWayTheTableGotThere() throws Throwable
    {
        boolean accordWasEnabled = DatabaseDescriptor.getAccordTransactionsEnabled();
        DatabaseDescriptor.setAccordTransactionsEnabled(true);
        try
        {
            // 1. A tierable table that someone then makes transactional.
            createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
            assertNull(TieringPolicy.unsupportedSchemaError(metadata()));
            alterTable("ALTER TABLE %s WITH transactional_mode = 'full'");

            String error = TieringPolicy.unsupportedSchemaError(metadata());
            assertNotNull("making a tierable table transactional must make it un-tierable", error);
            assertTrue(error, error.contains("transactional_mode"));
            assertTrue(error, error.contains("full"));

            // 2. A table that was already transactional when the policy arrived. mixed_reads, not full:
            // it is the mode that reads look most normal under (plain SELECTs keep working) and it still
            // routes SERIAL reads through Accord, so it is the easiest one to install a policy on by
            // mistake.
            createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                        "WITH transactional_mode = 'mixed_reads'");
            error = TieringPolicy.unsupportedSchemaError(metadata());
            assertNotNull("installing a policy on an already-transactional table must be refused", error);
            assertTrue(error, error.contains("mixed_reads"));
        }
        finally
        {
            DatabaseDescriptor.setAccordTransactionsEnabled(accordWasEnabled);
        }
    }

    @Test
    public void counterTableIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, hits counter, PRIMARY KEY (tag, ts))");

        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains("hits"));
        assertTrue(error, error.contains("counter"));
    }

    @Test
    public void nonFrozenCollectionIsRejectedButAFrozenOneIsNot() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, labels map<text,text>, PRIMARY KEY (tag, ts))");
        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains("labels"));

        createTable("CREATE TABLE %s (tag text, ts timestamp, attribute frozen<map<text,text>>, " +
                    "PRIMARY KEY (tag, ts))");
        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));
    }

    @Test
    public void productionShapedTableIsAccepted() throws Throwable
    {
        // pp.tm_tag_point, verbatim: composite-free key, DESC clustering, 7 static columns, 7 regular
        // columns of five different types including a frozen map, and a table-level TTL.
        createTable("CREATE TABLE %s (" +
                    "tag_id text, timestamp timestamp, " +
                    "area_id text static, asset_id text static, line_id text static, opc_id text static, " +
                    "site_id text static, tag_name text static, type text static, " +
                    "attribute frozen<map<text,text>>, error_code int, latency int, quality int, " +
                    "value text, value_boolean boolean, value_numeric double, " +
                    "PRIMARY KEY (tag_id, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (timestamp DESC) AND default_time_to_live = 5356800");

        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));
    }

    // ---- the chunk-table DDL, and its round trip through the cluster metadata log ----

    /**
     * The single-node reduction of the SP4 Task 5 defect. TCM serializes a committed schema change into
     * the cluster metadata log <b>as CQL text</b>, so whatever {@code ensureChunkTable} submits has to
     * come back out of {@code SchemaTransformationSerializer.deserialize} as the same schema change --
     * on a peer, and on this node when it replays its own log after a restart. A programmatic
     * {@code SchemaTransformations.addTable} does not: its {@code cql()} is the literal string
     * {@code "null"}, which nothing can parse.
     */
    @Test
    public void chunkTableDdlSurvivesTheClusterMetadataLogSerializer() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        assertChunkTableEntryIsReadable(metadata());
    }

    @Test
    public void compositeKeyChunkTableDdlSurvivesTheClusterMetadataLogSerializer() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, value double, " +
                    "PRIMARY KEY ((asset_id, date, hour), ts))");
        assertChunkTableEntryIsReadable(metadata());
    }

    @Test
    public void mixedCaseAndReservedIdentifiersSurviveTheChunkTableDdl() throws Throwable
    {
        // A quoted identifier in every position the DDL has to quote: a mixed-case table name, a
        // mixed-case key column, and a key column that is a CQL reserved word.
        createTable(KEYSPACE,
                    "CREATE TABLE %s (\"Asset\" text, \"table\" int, ts timestamp, value double, " +
                    "PRIMARY KEY ((\"Asset\", \"table\"), ts))",
                    "\"MixedCase\"");
        TableMetadata base = Schema.instance.getTableMetadata(KEYSPACE, "MixedCase");
        assertNotNull(base);
        assertNull(TieringPolicy.unsupportedSchemaError(base));

        String ddl = ChunkTables.createChunkTableStatement(base);
        assertTrue(ddl, ddl.contains('"' + ChunkTables.chunkTableName("MixedCase") + '"'));
        assertTrue(ddl, ddl.contains("\"Asset\""));
        assertTrue(ddl, ddl.contains("\"table\""));

        assertChunkTableEntryIsReadable(base);

        ChunkTables.ensureChunkTable(base);
        TableMetadata chunk = Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName("MixedCase"));
        assertNotNull("no chunk table was created for a mixed-case base table", chunk);
        assertSameShape(ChunkTables.chunkTableMetadata(base), chunk);
    }

    /**
     * What {@link ChunkTables#ensureChunkTable} actually commits, read back the way a node reads it.
     * The assertions above test the generated DDL in isolation; this one holds {@code ensureChunkTable}
     * itself to it, by round-tripping every entry in this node's log through
     * {@code Transformation.Kind.toVersionedBytes}/{@code fromVersionedBytes} -- the exact per-entry
     * work {@code SystemKeyspaceStorage} does when it persists an entry and when
     * {@code LocalLog.replayPersisted} reads it back at startup. Against a programmatic
     * {@code SchemaTransformations.addTable} the read side throws
     * {@code SyntaxException: no viable alternative at input 'null'}: a peer that cannot catch up, and
     * a node that will not come back up.
     */
    @Test
    public void ensureChunkTableCommitsAReadableClusterMetadataLogEntry() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, value double, " +
                    "PRIMARY KEY ((asset_id, date, hour), ts))");
        ChunkTables.ensureChunkTable(metadata());

        ImmutableList<Entry> entries = ClusterMetadataService.instance()
                                                             .log()
                                                             .storage()
                                                             .getPersistedLogState()
                                                             .entries;
        assertTrue("no cluster metadata log entries were recorded at all", !entries.isEmpty());
        for (Entry entry : entries)
        {
            Transformation.Kind kind = entry.transform.kind();
            kind.fromVersionedBytes(kind.toVersionedBytes(entry.transform));
        }
    }

    @Test
    public void ddlCreatedChunkTableMatchesChunkTableMetadata() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, value double, " +
                    "PRIMARY KEY ((asset_id, date, hour), ts)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC) AND default_time_to_live = 604800");

        ChunkTables.ensureChunkTable(metadata());
        TableMetadata chunk = Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable()));
        assertNotNull(chunk);
        assertSameShape(ChunkTables.chunkTableMetadata(metadata()), chunk);

        // The chunk table must not inherit the base table's TTL: a chunk that expired on its own would
        // take the only surviving copy of the rows it encodes with it.
        assertEquals(0, chunk.params.defaultTimeToLive);
        // The base clustering order is the base's business; the chunk table is always ASC on
        // window_start, because every chunk query the re-encoder and the read path issue says so.
        assertEquals("ASC", chunk.clusteringColumns().get(0).clusteringOrder().toString());

        // Still idempotent, sweep after sweep -- CREATE TABLE IF NOT EXISTS, not a fresh table each time.
        ChunkTables.ensureChunkTable(metadata());
        assertEquals(chunk.id,
                     Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())).id);
    }

    /**
     * Asserts that the log entry {@link ChunkTables#ensureChunkTable} would write for {@code base} can
     * be read back: serialized exactly as TCM serializes it, then deserialized exactly as a peer (or
     * this node, replaying its own log at startup) deserializes it, and still enacting the same change.
     */
    private static void assertChunkTableEntryIsReadable(TableMetadata base) throws Exception
    {
        String ddl = ChunkTables.createChunkTableStatement(base);
        CQLStatement statement = QueryProcessor.getStatement(ddl, ClientState.forInternalCalls());
        assertTrue(ddl, statement instanceof SchemaTransformation);

        SchemaTransformation transformation = (SchemaTransformation) statement;
        // The defect in one assertion: SchemaTransformation.cql() defaults to the literal "null", and
        // this is the exact string SchemaTransformationSerializer.serialize writes into the log.
        assertEquals(ddl, transformation.cql());

        Version version = NodeVersion.CURRENT.serializationVersion();
        SchemaTransformation decoded;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            SchemaTransformation.serializer.serialize(transformation, out, version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                decoded = SchemaTransformation.serializer.deserialize(in, version);
            }
        }

        Keyspaces after = decoded.apply(ClusterMetadata.current());
        assertNotNull("the re-parsed cluster metadata log entry did not create the chunk table",
                      after.getNullable(base.keyspace).getTableNullable(ChunkTables.chunkTableName(base.name)));
    }

    private static void assertSameShape(TableMetadata expected, TableMetadata actual)
    {
        assertColumnsMatch("partition key", expected.partitionKeyColumns(), actual.partitionKeyColumns());
        assertColumnsMatch("clustering", expected.clusteringColumns(), actual.clusteringColumns());
        assertColumnsMatch("regular",
                           ImmutableList.copyOf(expected.regularColumns()),
                           ImmutableList.copyOf(actual.regularColumns()));
        assertEquals(expected.params.compaction, actual.params.compaction);
    }

    private static void assertColumnsMatch(String kind, List<ColumnMetadata> expected, List<ColumnMetadata> actual)
    {
        assertEquals(kind + " arity", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
        {
            assertEquals(kind + " column " + i + " name", expected.get(i).name, actual.get(i).name);
            assertEquals(kind + " column " + i + " type", expected.get(i).type, actual.get(i).type);
        }
    }

    // ---- composite partition keys, end to end ----

    @Test
    public void compositePartitionKeyIsMirroredByTheChunkTable() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, value double, " +
                    "PRIMARY KEY ((asset_id, date, hour), ts))");
        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));

        TableMetadata chunk = ChunkTables.chunkTableMetadata(metadata());
        List<ColumnMetadata> chunkKey = chunk.partitionKeyColumns();
        assertEquals(3, chunkKey.size());
        List<ColumnMetadata> baseKey = metadata().partitionKeyColumns();
        for (int i = 0; i < baseKey.size(); i++)
        {
            assertEquals(baseKey.get(i).name, chunkKey.get(i).name);
            assertEquals(baseKey.get(i).type, chunkKey.get(i).type);
        }
        assertEquals(1, chunk.clusteringColumns().size());
        assertEquals("window_start", chunk.clusteringColumns().get(0).name.toString());
    }

    @Test
    public void compositePartitionKeyRoundTripsThroughTheReEncoder() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, value double, " +
                    "PRIMARY KEY ((asset_id, date, hour), ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        // Two distinct partitions differing only in the 2nd/3rd key column: they must not be conflated
        // by the DISTINCT-key enumeration, and each must get its own chunk partition.
        insert("a1", "2026-08-01", 0, 10 * 60_000L, 1.0, 101);
        insert("a1", "2026-08-01", 0, 20 * 60_000L, 2.0, 102);
        insert("a1", "2026-08-01", 1, 10 * 60_000L, 3.0, 103);
        // A hot row per partition (window [4h,5h) with now = 5h), which must survive untouched.
        insert("a1", "2026-08-01", 0, 4 * HOUR, 9.0, 110);
        insert("a1", "2026-08-01", 1, 4 * HOUR, 9.0, 111);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(2, stats.windowsEncoded);
        assertEquals(3, stats.rowsEncoded);

        String chunkRef = KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
        UntypedResultSet firstChunk = execute("SELECT samples FROM " + chunkRef +
                                              " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                                              "a1", "2026-08-01", 0, new Date(0L));
        assertEquals(1, firstChunk.size());
        assertEquals(2, firstChunk.one().getInt("samples"));

        UntypedResultSet secondChunk = execute("SELECT samples FROM " + chunkRef +
                                               " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                                               "a1", "2026-08-01", 1, new Date(0L));
        assertEquals(1, secondChunk.size());
        assertEquals(1, secondChunk.one().getInt("samples"));

        // The encoded base rows are physically gone...
        assertEquals(0, raw("SELECT value FROM %s WHERE asset_id = ? AND date = ? AND hour = ? AND ts < ?",
                            "a1", "2026-08-01", 0, new Date(HOUR)).size());

        // ...but a plain SELECT still sees them: the read path split the composite key correctly.
        UntypedResultSet merged = execute("SELECT value FROM %s WHERE asset_id = ? AND date = ? AND hour = ?",
                                          "a1", "2026-08-01", 0);
        assertEquals(3, merged.size());
        double[] expected = { 1.0, 2.0, 9.0 };
        int i = 0;
        for (UntypedResultSet.Row row : merged)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    // ---- static columns ----

    @Test
    public void staticColumnsSurviveTheReEncodersRangeDelete() throws Throwable
    {
        // The whole reason static columns need no rules: the re-encoder deletes a clustering RANGE,
        // and static cells live outside every clustering range, so they are untouched by construction.
        // Shape mirrors the production table: several statics of mixed type alongside several regular
        // columns, so this also proves the many-column delete does not reach the statics.
        createTable("CREATE TABLE %s (tag text, ts timestamp, site_id text static, unit text static, " +
                    "installed_at timestamp static, channels int static, " +
                    "value double, quality int, note text, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, site_id, unit, installed_at, channels) " +
                "VALUES ('t1', 'seoul', 'celsius', ?, 4) USING TIMESTAMP 100", new Date(1_000_000L));
        execute("INSERT INTO %s (tag, ts, value, quality, note) VALUES ('t1', ?, 1.0, 192, 'ok') " +
                "USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value, quality, note) VALUES ('t1', ?, 2.0, 192, 'ok') " +
                "USING TIMESTAMP 102", new Date(20 * 60_000L));

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // Every base row of the encoded window is gone...
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't1' AND ts < ?", new Date(HOUR)).size());

        // ...and every static cell is still there, at its original writetime.
        UntypedResultSet statics = raw("SELECT site_id, unit, installed_at, channels, WRITETIME(site_id) AS wt " +
                                       "FROM %s WHERE tag = 't1'");
        assertEquals(1, statics.size());
        assertEquals("seoul", statics.one().getString("site_id"));
        assertEquals("celsius", statics.one().getString("unit"));
        assertEquals(new Date(1_000_000L), statics.one().getTimestamp("installed_at"));
        assertEquals(4, statics.one().getInt("channels"));
        assertEquals(100L, statics.one().getLong("wt"));

        // ...and every regular column of the chunked rows still reads back through the merge.
        UntypedResultSet merged = execute("SELECT ts, value, quality, note FROM %s WHERE tag = 't1' AND ts < ?",
                                          new Date(HOUR));
        assertEquals(2, merged.size());
        for (UntypedResultSet.Row row : merged)
        {
            assertEquals(192, row.getInt("quality"));
            assertEquals("ok", row.getString("note"));
        }
    }

    // ---- default_time_to_live vs hot_window ----

    @Test
    public void ttlShorterThanHotWindowIsWarnedByTheReEncoder() throws Throwable
    {
        // 1h TTL with a 2h hot_window: every row expires before the re-encoder is allowed to touch it,
        // so tiering is on but nothing is ever compressed. Warn, do not reject -- a per-row USING TTL
        // can differ from the table default.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH default_time_to_live = 3600");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        try
        {
            new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        assertTrue("expected a WARN naming both default_time_to_live and hot_window",
                   appender.list.stream().anyMatch(e -> e.getLevel() == Level.WARN &&
                                                        e.getFormattedMessage().contains("default_time_to_live") &&
                                                        e.getFormattedMessage().contains("hot_window") &&
                                                        e.getFormattedMessage().contains("3600")));

        // It is a warning, not a rejection: the cycle still ran and still created the chunk table.
        assertNotNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())));
    }

    @Test
    public void ttlLongerThanHotWindowIsNotWarned() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH default_time_to_live = 864000");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        try
        {
            new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        // Narrowly: no ttlShadowsHotWindowWarning. It cannot assert "no message mentions
        // default_time_to_live", because ttlWithoutColdWindowWarning legitimately fires here -- this
        // table has a finite TTL and the policy sets no cold_window, so tiering really is about to
        // convert it from bounded retention to unbounded growth. That warning is the other
        // direction, and TieringPolicyTest covers it directly.
        assertTrue("a TTL longer than hot_window must not warn that rows expire before re-encoding",
                   appender.list.stream().noneMatch(e -> e.getFormattedMessage()
                                                          .contains("rows expire before the re-encoder")));
    }

    private TableMetadata metadata()
    {
        return getCurrentColumnFamilyStore().metadata();
    }

    private void insert(String assetId, String date, int hour, long tsMillis, double value, long writetime)
    throws Throwable
    {
        execute("INSERT INTO %s (asset_id, date, hour, ts, value) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                assetId, date, hour, new Date(tsMillis), value, writetime);
    }

    /** Reads the PHYSICAL base rows, bypassing SP3's transparent hot+chunk merge. */
    private UntypedResultSet raw(String query, Object... values) throws Throwable
    {
        TransparentReads.enterInternalBypass();
        try
        {
            return execute(query, values);
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }
}
