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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ChunkV4Directory;
import org.apache.cassandra.db.timeseries.ChunkV4Header;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SP4 Task 3: the re-encoder chunks <b>every</b> regular column, not a designated value column.
 * <p>
 * These are the correctness tests for the code that deletes production rows after encoding them, so
 * they assert on the physical chunk (every column of every row, nulls included), on the physical
 * base table (what the range delete actually removed), and on the merged SELECT (what a client sees
 * afterwards) -- not on any one of the three alone.
 */
public class TieredStorageColumnsTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(TieredStorageColumnsTest.class);

    private static final long HOUR = 3_600_000L;
    private static final MapType<String, String> ATTRIBUTE_TYPE =
        MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false);

    // ---- the production shape: tm_tag_point ----

    /**
     * pp.tm_tag_point's shape, with pp's measured sparsity: a column that is always null
     * ({@code value_numeric}), columns that are constant ({@code quality}, {@code error_code}), a
     * high-entropy one ({@code latency}), a low-cardinality opaque one (the frozen map
     * {@code attribute}), and text/boolean columns with holes in them.
     */
    private String createProductionShapedTable() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (" +
                                   "tag_id text, timestamp timestamp, " +
                                   "area_id text static, asset_id text static, line_id text static, " +
                                   "opc_id text static, site_id text static, tag_name text static, type text static, " +
                                   "attribute frozen<map<text,text>>, error_code int, latency int, quality int, " +
                                   "value text, value_boolean boolean, value_numeric double, " +
                                   "PRIMARY KEY (tag_id, timestamp)) " +
                                   "WITH CLUSTERING ORDER BY (timestamp DESC) AND default_time_to_live = 5356800");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        return table;
    }

    private static final long[] TS = { 0L, 600_000L, 1_200_000L, 1_800_000L, 2_400_000L, 3_000_000L };
    private static final Integer[] LATENCY = { 12, 40, 7, 999, 3, 12 };
    private static final String[] VALUE = { "a", "b", "a", "c", null, "e" };
    private static final Boolean[] VALUE_BOOLEAN = { true, false, null, true, true, false };
    private static final String[] UNIT = { "C", "C", "F", "C", "F", "C" };

    private void loadProductionShapedRows() throws Throwable
    {
        execute("INSERT INTO %s (tag_id, area_id, asset_id, line_id, opc_id, site_id, tag_name, type) " +
                "VALUES ('t1', 'a', 'as', 'l', 'o', 's', 'tn', 'ty') USING TIMESTAMP 100");
        for (int i = 0; i < TS.length; i++)
        {
            // Columns that are null on this row are OMITTED rather than bound as null: binding null
            // writes a cell tombstone, and tombstoning a cold clustering is refused outright now
            // (TieredWrites). Omitting is also what a real writer does.
            StringBuilder columns = new StringBuilder("tag_id, timestamp, attribute, error_code, latency, quality");
            StringBuilder markers = new StringBuilder("?, ?, ?, 0, ?, 192");
            List<Object> binds = new ArrayList<>(List.of("t1", new Date(TS[i]), attribute(UNIT[i]), LATENCY[i]));
            if (VALUE[i] != null)
            {
                columns.append(", value");
                markers.append(", ?");
                binds.add(VALUE[i]);
            }
            if (VALUE_BOOLEAN[i] != null)
            {
                columns.append(", value_boolean");
                markers.append(", ?");
                binds.add(VALUE_BOOLEAN[i]);
            }
            binds.add(101L + i);
            execute("INSERT INTO %s (" + columns + ") VALUES (" + markers + ") USING TIMESTAMP ?", binds.toArray());
        }
        // Hot row (window [4h,5h) against the synthetic now = 5h): must survive untouched.
        execute("INSERT INTO %s (tag_id, timestamp, latency) VALUES ('t1', ?, 1) USING TIMESTAMP 200",
                new Date(4 * HOUR));
    }

    @Test
    public void everyColumnOfEveryRowRoundTripsThroughTheChunk() throws Throwable
    {
        createProductionShapedTable();
        loadProductionShapedRows();

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, stats.windowsEncoded);
        assertEquals(TS.length, stats.rowsEncoded);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t1", new Date(0L)).one();
        assertEquals(TS.length, chunkRow.getInt("samples"));
        assertEquals(ColumnarChunkCodec.VERSION, chunkRow.getByte("codec"));

        // 1) the physical chunk carries every column of every row, nulls included
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        for (int i = 0; i < TS.length; i++)
        {
            assertTrue("expected sample " + i, cursor.advance());
            assertEquals(TS[i], cursor.timestamp());
            assertEquals(attribute(UNIT[i]), cursor.getBytes("attribute"));
            assertEquals(0, (int) Int32Type.instance.compose(cursor.getBytes("error_code")));
            assertEquals(192, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
            assertEquals((int) LATENCY[i], (int) Int32Type.instance.compose(cursor.getBytes("latency")));
            if (VALUE[i] == null)
                assertTrue("value must stay null at sample " + i, cursor.isNull("value"));
            else
                assertEquals(VALUE[i], UTF8Type.instance.compose(cursor.getBytes("value")));
            if (VALUE_BOOLEAN[i] == null)
                assertTrue("value_boolean must stay null at sample " + i, cursor.isNull("value_boolean"));
            else
                assertEquals(VALUE_BOOLEAN[i], BooleanType.instance.compose(cursor.getBytes("value_boolean")));
            // Never written by anyone: an all-null column must not materialise a default.
            assertTrue("value_numeric must stay null at sample " + i, cursor.isNull("value_numeric"));
        }
        assertFalse(cursor.advance());

        // 2) the source rows are physically gone, but the statics are not
        assertEquals(0, raw("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp < ?", new Date(HOUR)).size());
        UntypedResultSet statics = raw("SELECT area_id, asset_id, line_id, opc_id, site_id, tag_name, type, " +
                                       "WRITETIME(site_id) AS wt FROM %s WHERE tag_id = 't1'");
        assertEquals(1, statics.size());
        assertEquals("s", statics.one().getString("site_id"));
        assertEquals("ty", statics.one().getString("type"));
        assertEquals(100L, statics.one().getLong("wt"));

        // 3) a plain SELECT returns every row again, every column exactly as written
        UntypedResultSet merged = execute("SELECT timestamp, attribute, error_code, latency, quality, value, " +
                                          "value_boolean, value_numeric FROM %s WHERE tag_id = 't1' " +
                                          "AND timestamp < ? ORDER BY timestamp ASC", new Date(HOUR));
        assertEquals(TS.length, merged.size());
        int i = 0;
        for (UntypedResultSet.Row row : merged)
        {
            assertEquals(new Date(TS[i]), row.getTimestamp("timestamp"));
            assertEquals(attribute(UNIT[i]), row.getBytes("attribute"));
            assertEquals(0, row.getInt("error_code"));
            assertEquals(192, row.getInt("quality"));
            assertEquals((int) LATENCY[i], row.getInt("latency"));
            assertEquals(VALUE[i], VALUE[i] == null ? null : row.getString("value"));
            assertEquals(VALUE[i] != null, row.has("value"));
            assertEquals(VALUE_BOOLEAN[i] != null, row.has("value_boolean"));
            if (VALUE_BOOLEAN[i] != null)
                assertEquals(VALUE_BOOLEAN[i], (Boolean) row.getBoolean("value_boolean"));
            assertFalse("value_numeric was never written and must read back as null",
                        row.has("value_numeric"));
            i++;
        }

        // 4) the hot row is untouched
        assertEquals(1, raw("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp = ?",
                            new Date(4 * HOUR)).size());
    }

    @Test
    public void descClusteredMergedReadsAreCompleteAndInOrder() throws Throwable
    {
        // Regression: on a table declared WITH CLUSTERING ORDER BY (ts DESC) -- the shape the
        // production table uses -- Slices arrive in DESCENDING timestamp order, so the comparator's
        // start bound is the range's UPPER time bound. Reading them as if ascending turned
        // `WHERE timestamp < X` into `[X+1, +inf)` and served ZERO cold rows, silently.
        createProductionShapedTable();
        loadProductionShapedRows();
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // Natural (DESC) order over everything: 6 cold rows + the hot one, newest first.
        UntypedResultSet all = execute("SELECT timestamp FROM %s WHERE tag_id = 't1'");
        assertEquals(TS.length + 1, all.size());
        long previous = Long.MAX_VALUE;
        for (UntypedResultSet.Row row : all)
        {
            long ts = row.getTimestamp("timestamp").getTime();
            assertTrue("DESC table must return newest first, got " + ts + " after " + previous, ts < previous);
            previous = ts;
        }

        // A bounded slice: the half-open upper bound must land on the right side of the range.
        assertEquals(3, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp < ?",
                                new Date(1_800_000L)).size());
        assertEquals(4, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp <= ?",
                                new Date(1_800_000L)).size());
        assertEquals(3, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp > ? AND timestamp < ?",
                                new Date(0L), new Date(2_400_000L)).size());

        // ORDER BY ASC on a DESC table (the "reversed" read) must come back oldest first.
        UntypedResultSet ascending = execute("SELECT timestamp FROM %s WHERE tag_id = 't1' " +
                                             "AND timestamp < ? ORDER BY timestamp ASC", new Date(HOUR));
        assertEquals(TS.length, ascending.size());
        int i = 0;
        for (UntypedResultSet.Row row : ascending)
            assertEquals(new Date(TS[i++]), row.getTimestamp("timestamp"));

        // A point lookup (names filter, not a slice) on a cold timestamp.
        assertEquals(1, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp = ?",
                                new Date(1_200_000L)).size());
    }

    @Test
    public void aSecondCycleIsAByteIdenticalNoOp() throws Throwable
    {
        createProductionShapedTable();
        loadProductionShapedRows();

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        UntypedResultSet.Row before = execute(chunkPayloadQuery(), "t1", new Date(0L)).one();
        ByteBuffer firstPayload = ByteBufferUtil.clone(before.getBytes("payload"));
        long firstWritetime = before.getLong("chunk_wt");

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(0, second.windowsEncoded);
        assertEquals(0, second.rowsEncoded);
        assertEquals(0, second.lateMerges);
        assertEquals(0, second.bytesWritten);

        UntypedResultSet.Row after = execute(chunkPayloadQuery(), "t1", new Date(0L)).one();
        assertEquals("the chunk payload must be byte-identical after a second cycle",
                     firstPayload, after.getBytes("payload"));
        assertEquals("the chunk row must not even be rewritten", firstWritetime, after.getLong("chunk_wt"));
    }

    // ---- late-row merge semantics ----

    @Test
    public void aLateUpdateOfOneColumnKeepsTheOtherColumnsOfThatRow() throws Throwable
    {
        // `UPDATE t SET quality = ? WHERE ...` writes ONE cell. Merging must be PER COLUMN: the
        // columns the update did not mention keep the values already in the chunk. Replacing the
        // whole chunked row with the (mostly-null) base row would blank them.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, note text, " +
                    "PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, value, quality, note) VALUES ('t', ?, 1.5, 192, 'ok') " +
                "USING TIMESTAMP 100", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value, quality, note) VALUES ('t', ?, 2.5, 192, 'ok') " +
                "USING TIMESTAMP 101", new Date(600_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());

        // Late correction touching ONLY quality, on a row that now exists only inside the chunk.
        execute("UPDATE %s USING TIMESTAMP 300 SET quality = 7 WHERE tag = 't' AND ts = ?", new Date(600_000L));

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, second.windowsEncoded);
        assertEquals(1, second.lateMerges);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t", new Date(0L)).one();
        assertEquals(2, chunkRow.getInt("samples"));         // merged, not appended
        assertEquals(300L, chunkRow.getLong("max_row_writetime"));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(0L, cursor.timestamp());
        assertEquals(1.5, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertEquals(192, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertEquals("ok", UTF8Type.instance.compose(cursor.getBytes("note")));
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertEquals("the update must win on the column it touched",
                     7, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertEquals("an untouched column must keep the value the chunk already held",
                     2.5, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertEquals("ok", UTF8Type.instance.compose(cursor.getBytes("note")));
        assertFalse(cursor.advance());

        // ...and the same through a plain SELECT.
        UntypedResultSet merged = execute("SELECT value, quality, note FROM %s WHERE tag = 't' AND ts = ?",
                                          new Date(600_000L));
        assertEquals(1, merged.size());
        assertEquals(2.5, merged.one().getDouble("value"), 0.0);
        assertEquals(7, merged.one().getInt("quality"));
        assertEquals("ok", merged.one().getString("note"));
    }

    @Test
    public void aLateRowAtANewTimestampAddsAWholeRow() throws Throwable
    {
        // The other half of the merge rule: a late row at a timestamp the chunk does NOT hold is a
        // new row, and the columns it leaves out are null (there is nothing to inherit).
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 1.5, 192) USING TIMESTAMP 100",
                new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        execute("INSERT INTO %s (tag, ts, quality) VALUES ('t', ?, 5) USING TIMESTAMP 300", new Date(600_000L));
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(execute(chunkSelectQuery(), "t", new Date(0L))
                                                          .one().getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(0L, cursor.timestamp());
        assertEquals(1.5, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertEquals(5, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertTrue("a brand-new row's unwritten column must be null, not inherited from another row",
                   cursor.isNull("value"));
        assertFalse(cursor.advance());
    }

    // ---- cold data is immutable: writes that would tombstone it are rejected ----

    private void chunkOneRow() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 1.5, 192) USING TIMESTAMP 100",
                new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());
    }

    /** Asserts {@code query} is refused as an attempt to mutate chunked (cold) data. */
    private void assertRejectedAsColdWrite(String query, Object... values) throws Throwable
    {
        try
        {
            execute(query, values);
            fail("expected the write to be rejected: " + query);
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("immutable"));
            assertTrue(e.getMessage(), e.getMessage().contains("cold_window"));
        }
    }

    @Test
    public void deletingOneCellOfAChunkedRowIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE value FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ?", new Date(0L));
        // ...and nothing changed: the row still reads back complete.
        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(1, rows.size());
        assertEquals(1.5, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void deletingAChunkedRowIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ?", new Date(0L));
    }

    @Test
    public void deletingAChunkedRangeIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts >= ? AND ts < ?",
                                  new Date(0L), new Date(HOUR));
    }

    @Test
    public void deletingAWholePartitionOfATieredTableIsRejected() throws Throwable
    {
        // No clustering bound at all, so it necessarily covers chunked data.
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't'");
    }

    @Test
    public void updatingAChunkedColumnToNullIsRejected() throws Throwable
    {
        // `SET col = null` writes a cell tombstone, which is the same hazard as a DELETE -- and the
        // guard inspects the built mutation, so it is caught without special-casing the statement.
        chunkOneRow();
        assertRejectedAsColdWrite("UPDATE %s USING TIMESTAMP 300 SET value = null WHERE tag = 't' AND ts = ?",
                                  new Date(0L));
    }

    @Test
    public void insertingANullValueIntoAChunkedClusteringIsAllowed() throws Throwable
    {
        // This test used to assert the opposite, and asserting the opposite took ingestion down.
        //
        // An INSERT that binds null for a column does emit a cell tombstone -- the same shape as
        // `SET col = null` -- so the guard refused both. But binding null for the columns a given
        // point does not carry is what every batch writer does on every insert, so once such a
        // writer's backlog aged past hot_window, every write it attempted was refused as if it were a
        // DELETE. It retried, stayed behind, and was refused again: ~2M rows failed and its queue hit
        // 99.3% of capacity. A writer that fell behind the hot window could never catch up, and the
        // rule meant to prevent data loss was causing it.
        //
        // The row marker separates the two: only INSERT writes one. `updatingAChunkedColumnToNull
        // IsRejected` (above) still holds -- that is the shape whose purpose is to un-write.
        chunkOneRow();
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, ?) USING TIMESTAMP 300", new Date(0L), null);
    }

    @Test
    public void insertingALateRowIntoAChunkedClusteringIsAllowed() throws Throwable
    {
        // The case the incident was actually made of: a backlogged writer replaying a point whose
        // timestamp has aged into the cold region, carrying a real value for one column and null for
        // another. The re-encoder merges such a late row into the chunk per column on its next cycle.
        chunkOneRow();
        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, ?, ?) USING TIMESTAMP 300",
                new Date(0L), 42.0, null);
    }

    @Test
    public void conditionalDeleteOfChunkedDataIsRejected() throws Throwable
    {
        // LWT/CAS builds its update in CQL3CasRequest.makeUpdates and never goes through
        // ModificationStatement.getMutations, so without a guard there `IF` was a way straight past
        // the immutability rule. The condition must be one that PASSES against a non-existent base
        // row (chunked data is invisible to LWT conditions -- see below), or the statement no-ops
        // before it ever builds an update.
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s WHERE tag = 't' AND ts = ? IF value = null", new Date(0L));
        assertRejectedAsColdWrite("DELETE value FROM %s WHERE tag = 't' AND ts = ? IF quality = null", new Date(0L));
    }

    @Test
    public void conditionalUpdateToNullOfChunkedDataIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("UPDATE %s SET value = null WHERE tag = 't' AND ts = ? IF quality = null",
                                  new Date(0L));
    }

    @Test
    public void aConditionalBatchCannotSmuggleAColdDeleteThrough() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("BEGIN BATCH " +
                                  "DELETE value FROM %s WHERE tag = 't' AND ts = ? IF quality = null " +
                                  "APPLY BATCH", new Date(0L));
    }

    @Test
    public void aConditionalWriteOfARealValueToAColdClusteringStillSucceeds() throws Throwable
    {
        // The rule is about UN-writing cold data; a conditional late correction is still legal.
        chunkOneRow();
        execute("UPDATE %s SET quality = 7 WHERE tag = 't' AND ts = ? IF value = null", new Date(0L));
        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(7, rows.one().getInt("quality"));
        assertEquals("the columns the UPDATE did not name still come from the chunk",
                     1.5, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void lwtConditionsDoNotSeeChunkedData() throws Throwable
    {
        // Documented limitation, and the reason the conditions above are written against null: a CAS
        // precondition read deliberately bypasses the hot+chunk merge, because Paxos can only
        // linearize data it owns and a chunk is a blob in another table that no ballot orders. So a
        // chunked row reads as ABSENT to `IF`, and `IF EXISTS` simply does not apply -- it writes
        // nothing, which is safe, rather than deleting cold data.
        chunkOneRow();
        UntypedResultSet applied = execute("DELETE FROM %s WHERE tag = 't' AND ts = ? IF EXISTS", new Date(0L));
        assertFalse("IF EXISTS must not apply against a chunked row", applied.one().getBoolean("[applied]"));
        // ...and the row is untouched.
        assertEquals(1.5, execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(0L))
                          .one().getDouble("value"), 0.0);
    }

    @Test
    public void insertIfNotExistsAppliesOnAChunkedClustering() throws Throwable
    {
        // The other direction of the same limitation, documented in tiered-storage.md §5.1.2: a CAS
        // precondition read cannot see the chunk, so `IF NOT EXISTS` finds no row and writes. Nothing
        // is lost -- writing a real value to a cold clustering is legal, and the following SELECT
        // merges the chunk's other columns back into the "new" row -- but it does not mean what an
        // application using it as a duplicate guard over historical data would expect.
        chunkOneRow();
        UntypedResultSet applied = execute("INSERT INTO %s (tag, ts, quality) VALUES ('t', ?, 7) IF NOT EXISTS",
                                           new Date(0L));
        assertTrue("IF NOT EXISTS applies against a chunked row, because the condition cannot see it",
                   applied.one().getBoolean("[applied]"));

        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(7, rows.one().getInt("quality"));
        assertEquals("the columns the INSERT did not name still come from the chunk",
                     1.5, rows.one().getDouble("value"), 0.0);
    }

    // ---- projection push-down must be observationally invisible ----

    /**
     * For <b>every</b> subset of the regular columns, {@code SELECT <subset>} must return exactly
     * what {@code SELECT *} says about those columns -- same row count, same order, byte-identical
     * values -- even though a projected read decodes only the subset out of the chunk.
     * <p>
     * The shape is the production one, so the subsets sweep across a column that is always null
     * ({@code value_numeric}), constant columns ({@code error_code}, {@code quality}), a
     * high-entropy one ({@code latency}), a dictionary-encoded frozen map, columns with holes, and
     * -- added after the window was encoded -- a column <b>no chunk carries at all</b>. That last one
     * is the case a projected read cannot get right by accident: it decodes nothing, so every row
     * reaches the primary-key-liveness fallback, and if that fallback were missing the rows would
     * simply vanish from the result.
     */
    @Test
    public void everyColumnSubsetReadsExactlyWhatSelectStarSays() throws Throwable
    {
        createProductionShapedTable();
        loadProductionShapedRows();
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        execute("ALTER TABLE %s ADD added_later int");

        List<String> columns = List.of("attribute", "error_code", "latency", "quality",
                                       "value", "value_boolean", "value_numeric", "added_later");
        List<Map<String, ByteBuffer>> reference = readColumns("SELECT * FROM %s WHERE tag_id = 't1'", columns);
        // Six chunked rows plus the hot one: whatever a subset selects, it must see all seven.
        assertEquals(TS.length + 1, reference.size());

        for (int subset = 1; subset < (1 << columns.size()); subset++)
        {
            List<String> selected = new ArrayList<>();
            for (int c = 0; c < columns.size(); c++)
                if ((subset & (1 << c)) != 0)
                    selected.add(columns.get(c));

            String cql = "SELECT " + String.join(", ", selected) + " FROM %s WHERE tag_id = 't1'";
            List<Map<String, ByteBuffer>> actual = readColumns(cql, selected);
            assertEquals(cql, reference.size(), actual.size());
            for (int r = 0; r < reference.size(); r++)
                for (String column : selected)
                    assertEquals(cql + ", row " + r + ", column " + column,
                                 reference.get(r).get(column), actual.get(r).get(column));
        }
    }

    /** @return {@code query}'s rows as raw per-column bytes ({@code null} where the column has no cell). */
    private List<Map<String, ByteBuffer>> readColumns(String query, List<String> columns) throws Throwable
    {
        List<Map<String, ByteBuffer>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
        {
            Map<String, ByteBuffer> cells = new LinkedHashMap<>();
            for (String column : columns)
                cells.put(column, row.has(column) ? row.getBytes(column) : null);
            rows.add(cells);
        }
        return rows;
    }

    // ---- immutability is keyed on real chunk coverage, not on the policy being installed ----

    /**
     * The hazard this pair of rules exists to prevent, reached the way an operator actually reaches
     * it: stop tiering a table by dropping the extension, then delete some old data.
     * <p>
     * Reads consult what the chunk table <em>contains</em>, so the encoded rows are still served
     * after the drop. A write guard keyed on {@code policy != null} would meanwhile accept a
     * {@code DELETE} against exactly those rows -- a tombstone that masks the chunk's copy only until
     * {@code gc_grace_seconds} purges it, after which the "deleted" data silently comes back. Both
     * sides have to key off the same coverage.
     */
    @Test
    public void droppingTheExtensionDoesNotMakeChunkedDataDeletable() throws Throwable
    {
        chunkOneRow();
        alterTable("ALTER TABLE %s WITH extensions = {};");
        // The base row is gone -- the chunk is the only copy -- and the merge still serves it.
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't' AND ts = ?", new Date(0L)).size());
        assertEquals(1.5, execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(0L))
                          .one().getDouble("value"), 0.0);

        // Emptied so the guard has to load the ledger itself: with no policy left, nothing else on
        // this node would have warmed the coverage cache for this table.
        ChunkCoverage.invalidateAll();
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ?", new Date(0L));
        assertRejectedAsColdWrite("DELETE value FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ?", new Date(0L));
        assertRejectedAsColdWrite("UPDATE %s USING TIMESTAMP 300 SET value = null WHERE tag = 't' AND ts = ?",
                                  new Date(0L));
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't'");
        assertRejectedAsColdWrite("DELETE FROM %s WHERE tag = 't' AND ts = ? IF value = null", new Date(0L));

        // ...and the chunk's copy is still there and still complete.
        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(1, rows.size());
        assertEquals(1.5, rows.one().getDouble("value"), 0.0);
        assertEquals(192, rows.one().getInt("quality"));
    }

    /**
     * The other half of the rule: coverage bounds the guard, so dropping the extension does not turn
     * the table read-only. Nothing above what the chunk table reaches is protected by anything.
     */
    @Test
    public void droppingTheExtensionStillAllowsDeletingWhatNoChunkCovers() throws Throwable
    {
        chunkOneRow();
        alterTable("ALTER TABLE %s WITH extensions = {};");
        ChunkCoverage.invalidateAll();

        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 4.0) USING TIMESTAMP 400", new Date(7 * HOUR));
        execute("DELETE FROM %s USING TIMESTAMP 500 WHERE tag = 't' AND ts = ?", new Date(7 * HOUR));
        assertEquals(0, execute("SELECT ts FROM %s WHERE tag = 't' AND ts = ?", new Date(7 * HOUR)).size());
    }

    /**
     * Fail closed. With the ledger gone, coverage is unknowable -- chunks may reach anywhere -- so
     * the guard refuses the same {@code DELETE} the test above accepts. A wrongly refused write is an
     * immediate, visible, fixable error; a wrongly accepted one is data resurrection on a
     * {@code gc_grace_seconds} timer that nothing reports.
     */
    @Test
    public void anUnknowableCoverageLedgerRejectsRatherThanAccepts() throws Throwable
    {
        chunkOneRow();
        alterTable("ALTER TABLE %s WITH extensions = {};");
        execute("DROP TABLE " + KEYSPACE + ".\"" + ChunkTables.coverageTableName(currentTable()) + '"');
        ChunkCoverage.invalidateAll();

        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 4.0) USING TIMESTAMP 400", new Date(7 * HOUR));
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 500 WHERE tag = 't' AND ts = ?",
                                  new Date(7 * HOUR));
    }

    /** Drops {@code base}'s coverage ledger and empties the cache, so coverage reads back UNKNOWN. */
    private void makeCoverageUnknowable() throws Throwable
    {
        execute("DROP TABLE " + KEYSPACE + ".\"" + ChunkTables.coverageTableName(currentTable()) + '"');
        ChunkCoverage.invalidateAll();
        assertFalse("precondition: coverage must be unknowable", ChunkCoverage.forTable(baseMetadata(), null).known());
    }

    private TableMetadata baseMetadata()
    {
        return getCurrentColumnFamilyStore().metadata();
    }

    /**
     * ...but "fail closed" has to mean "refuse deletions", not "refuse writes". The write guard sees
     * mutations, not statements, and two shapes of perfectly ordinary <em>live</em> write carry a
     * tombstone in them: assigning any non-frozen collection or UDT emits a complex deletion (that is
     * how a full collection assignment replaces the old one), and binding {@code null} for a column
     * -- what every driver does for an unset field of a prepared INSERT -- emits a cell tombstone.
     * <p>
     * With an unreadable ledger every clustering is nominally cold, so refusing on those shapes
     * refused current-timestamp inserts of the most common CQL shapes there are, for as long as the
     * ledger stayed unreadable. That is a bigger outage than the one being prevented -- and the
     * shapes that are unambiguously deletions are still refused, so nothing slips through.
     */
    @Test
    public void anUnknowableLedgerStillAcceptsOrdinaryLiveWrites() throws Throwable
    {
        chunkOneRow();
        execute("ALTER TABLE %s ADD attrs map<text, text>");
        makeCoverageUnknowable();

        // A prepared INSERT binding null for one column: a cell tombstone, and nothing more.
        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, ?, 7) USING TIMESTAMP 400",
                new Date(7 * HOUR), null);
        assertEquals(7, execute("SELECT quality FROM %s WHERE tag = 't' AND ts = ?", new Date(7 * HOUR))
                        .one().getInt("quality"));

        // Assigning a whole non-frozen collection: a complex deletion, and nothing more.
        execute("UPDATE %s USING TIMESTAMP 401 SET attrs = {'unit': 'C'} WHERE tag = 't' AND ts = ?",
                new Date(7 * HOUR));
        assertEquals(1, execute("SELECT attrs FROM %s WHERE tag = 't' AND ts = ?", new Date(7 * HOUR))
                        .one().getMap("attrs", UTF8Type.instance, UTF8Type.instance).size());

        // The unambiguous shapes are still refused, on the same unreadable ledger.
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 500 WHERE tag = 't' AND ts = ?", new Date(7 * HOUR));
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 500 WHERE tag = 't' AND ts >= ? AND ts < ?",
                                  new Date(0L), new Date(HOUR));
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 500 WHERE tag = 't'");
    }

    /**
     * The guard inspects the update before it asks for coverage, because coverage can cost a ledger
     * read and a write that tombstones nothing cannot un-write anything whatever the answer would
     * have been. Asking first meant every write to a tiered table paid for the ledger -- and, with the
     * ledger timing out, blocked a request thread for {@code read_request_timeout} each, one per
     * mutation.
     */
    @Test
    public void aWriteThatTombstonesNothingNeverReadsTheLedger() throws Throwable
    {
        chunkOneRow();
        ChunkCoverage.invalidateAll();

        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 2.5, 7) USING TIMESTAMP 400",
                new Date(7 * HOUR));
        execute("UPDATE %s USING TIMESTAMP 401 SET value = 3.5 WHERE tag = 't' AND ts = ?", new Date(7 * HOUR));
        assertFalse("a write carrying no tombstone must not pay for a coverage lookup",
                    ChunkCoverage.isCached(baseMetadata()));

        // ...while one that does carry a tombstone still pays for it, and is still judged against it.
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 500 WHERE tag = 't' AND ts = ?", new Date(0L));
        assertTrue(ChunkCoverage.isCached(baseMetadata()));
    }

    /**
     * An unreadable ledger is remembered briefly rather than re-probed by every request. Without a
     * negative cache, a ledger that is timing out costs one blocked request thread per read of the
     * table -- node-wide request-thread exhaustion, which is a far larger failure than the degraded
     * ledger that triggered it. Both consequences of remembering UNKNOWN are the safe ones.
     */
    @Test
    public void anUnreadableLedgerIsNegativelyCached() throws Throwable
    {
        chunkOneRow();
        makeCoverageUnknowable();
        assertTrue("an unreadable ledger must be remembered, or every request re-reads it",
                   ChunkCoverage.isCached(baseMetadata()));
    }

    // ---- the ledger is a monotone claim: nothing may narrow it ----

    /**
     * {@code claim} must refuse to run on a coverage it could not establish.
     * <p>
     * The claim it would otherwise write is built by widening <em>this node's current row</em>, and an
     * UNKNOWN coverage carries no numbers at all -- so the "widened" row would contain only this
     * cycle's window and this cycle's {@code chunk_window}, overwriting whatever this node had already
     * claimed. After a {@code chunk_window} was shrunk (7d to 1h, say) that drops the aggregate widest
     * to 1h, which shortens the read path's look-back to 1h, which stops it ever selecting the 7d-wide
     * chunks whose {@code window_start} sits further back than that -- permanently hiding rows whose
     * base copies were deleted when they were encoded. One unreadable ledger read, one cycle, and the
     * data is gone from every subsequent read.
     */
    @Test
    public void wideningAnUnknownCoverageIsRefusedInsteadOfNarrowingTheLedger()
    {
        try
        {
            ChunkCoverage.Coverage.UNKNOWN.widenedBy(0L, HOUR, HOUR);
            fail("widening an unknown coverage must be refused, not silently treated as 'nothing recorded yet'");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("unknown"));
        }
    }

    @Test
    public void claimIsRefusedWhenTheLedgerCannotBeRead() throws Throwable
    {
        chunkOneRow();
        makeCoverageUnknowable();
        try
        {
            ChunkCoverage.claim(baseMetadata(), null, 0L, HOUR, HOUR);
            fail("claiming against an unreadable ledger must abort the window");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("could not be read"));
        }
    }

    /**
     * The widest {@code chunk_window} ever written is a maximum across claims, so re-claiming with a
     * narrower one leaves the read path's look-back reaching the older, wider chunks.
     */
    @Test
    public void aNarrowerClaimDoesNotShrinkTheRecordedChunkWindow() throws Throwable
    {
        chunkOneRow();                                     // chunk_window = 1h, so widest = 1h
        ChunkCoverage.invalidateAll();
        ChunkCoverage.claim(baseMetadata(), null, 8 * HOUR, 9 * HOUR, HOUR / 4);
        ChunkCoverage.invalidateAll();
        assertEquals("a later, narrower claim must not shrink the look-back",
                     HOUR, ChunkCoverage.forTable(baseMetadata(), null).lookbackMillis());
    }

    /**
     * The ledger's top is the newest window the cycle actually encoded, never the cycle's cutoff.
     *
     * <p>The re-encoder used to claim {@code cutoff} as the top, on the reasoning that over-claiming
     * only costs an unnecessary chunk read. That held while the read path was the only consumer. The
     * write guard shares the number through {@link ColdBoundary#coldBelowMs}, and there the overshoot
     * is a whole {@code chunk_window} of rows that are inside the hot window and were never encoded:
     * {@code TieredWrites} refuses to tombstone them. With {@code hot_window == chunk_window} the band
     * reaches to now and no recent row can be deleted at all.
     *
     * <p>{@link #chunkOneRow} runs a cycle at {@code now = 5h} with {@code hot_window = 2h}, so the
     * cutoff is {@code 3h} and the one window encoded starts at {@code 0}. The top must therefore be
     * {@code 1h} (that window plus its {@code chunk_window}), not {@code 4h}. The row at {@code 4h} is
     * hot and unencoded, and a top of {@code 4h} is exactly what made it undeletable.
     */
    @Test
    public void theLedgerTopIsTheEncodedWindowNotTheCycleCutoff() throws Throwable
    {
        chunkOneRow();
        ChunkCoverage.invalidateAll();
        assertEquals("the ledger must not claim past the newest window actually encoded",
                     HOUR, ChunkCoverage.forTable(baseMetadata(), null).topExclusiveMs());
    }

    /**
     * The ledger is consulted at quorum strength whatever the asking query's own consistency level
     * is. It cannot be the caller's: the cache is keyed by table alone, so a {@code SELECT} at
     * {@code CL=ONE} that landed on a replica without the ledger row would publish
     * {@code Coverage.EMPTY} -- an authoritative "nothing is cold" -- for the write guard to act on,
     * and a {@code DELETE} against chunked data would be accepted until the entry expired. The
     * {@code null} (node-local internal) path is the same mistake by another route.
     */
    @Test
    public void theLedgerIsNeverConsultedBelowQuorumStrength() throws Throwable
    {
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, ChunkCoverage.ledgerConsistency(null));
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.ONE));
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.LOCAL_ONE));
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.TWO));
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.ANY));
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.SERIAL));
        // Quorum-strength levels -- exactly those a tiering policy may name -- pass through unchanged.
        assertEquals(ConsistencyLevel.QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.QUORUM));
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.LOCAL_QUORUM));
        assertEquals(ConsistencyLevel.EACH_QUORUM, ChunkCoverage.ledgerConsistency(ConsistencyLevel.EACH_QUORUM));
        assertEquals(ConsistencyLevel.ALL, ChunkCoverage.ledgerConsistency(ConsistencyLevel.ALL));

        // ...and the load path actually uses it. Asserting the helper alone does not discriminate:
        // reverting the ledger read to executeInternal leaves every assertion above green while the
        // read goes node-local, which is the exact bug this test is named for. Two observations pin
        // the real call:
        //
        //  1. the read is a COORDINATOR read. coordinatorReadLatency is updated by StorageProxy and
        //     by nothing else, so executeInternal (which never enters StorageProxy) cannot move it.
        //  2. it is issued at the UPGRADED level, not the caller's. CL=ANY is illegal for a read
        //     (ConsistencyLevel.validateForRead), so passing the caller's level through would throw,
        //     be caught by load()'s own handler, and cache Coverage.UNKNOWN. Coming back KNOWN is
        //     only possible if ANY was replaced by a quorum-strength level first.
        chunkOneRow();
        ColumnFamilyStore ledger = Keyspace.open(KEYSPACE)
                                            .getColumnFamilyStore(ChunkTables.coverageTableName(currentTable()));
        ChunkCoverage.invalidateAll();
        long coordinatorReadsBefore = ledger.metric.coordinatorReadLatency.getCount();

        ChunkCoverage.Coverage coverage = ChunkCoverage.forTable(baseMetadata(), ConsistencyLevel.ANY);

        assertTrue("a CL=ANY caller must not drag the ledger read below quorum strength: " + coverage,
                   coverage.known() && coverage.anyChunks());
        assertTrue("the ledger read must go through the coordinator, never executeInternal",
                   ledger.metric.coordinatorReadLatency.getCount() > coordinatorReadsBefore);
    }

    // ---- end-to-end: a tombstone written while the row was hot must survive re-encoding ----

    /**
     * Real-world sequence, and the only one the immutability rule leaves legal: delete something,
     * it ages out, tiering runs over its window. The delete must hold both before and after the
     * chunk is written, and the chunk must not contain the deleted data.
     */
    private String loadRecentWindowForDeletion() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, " +
                                   "PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        return table;
    }

    /** Hour-aligned start of the CURRENT window, so the rows below are unambiguously hot right now. */
    private static long currentWindowStart()
    {
        return (System.currentTimeMillis() / HOUR) * HOUR;
    }

    @Test
    public void aCellDeletedWhileHotStaysDeletedAfterReEncoding() throws Throwable
    {
        loadRecentWindowForDeletion();
        long base = currentWindowStart();
        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, ?, 192)",
                    new Date(base + i * 60_000L), i + 1.0);

        // Legal: still inside hot_window.
        execute("DELETE value FROM %s WHERE tag = 't' AND ts = ?", new Date(base + 60_000L));
        assertFalse(execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(base + 60_000L))
                    .one().has("value"));

        // Now let the window age out and re-encode it.
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), base + 5 * HOUR).windowsEncoded);

        UntypedResultSet rows = execute("SELECT ts, value, quality FROM %s WHERE tag = 't'");
        assertEquals(3, rows.size());
        UntypedResultSet.Row[] all = rows.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(1.0, all[0].getDouble("value"), 0.0);
        assertFalse("the cell deleted while hot must not come back from the chunk", all[1].has("value"));
        assertEquals(192, all[1].getInt("quality"));
        assertEquals(3.0, all[2].getDouble("value"), 0.0);

        // ...and the chunk itself never held it.
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(execute(chunkSelectQuery(), "t", new Date(base))
                                                          .one().getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertTrue(cursor.advance());
        assertEquals(base + 60_000L, cursor.timestamp());
        assertTrue("the chunk must not carry the deleted cell", cursor.isNull("value"));
    }

    @Test
    public void aRowDeletedWhileHotStaysDeletedAfterReEncoding() throws Throwable
    {
        loadRecentWindowForDeletion();
        long base = currentWindowStart();
        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, ?)", new Date(base + i * 60_000L), i + 1.0);

        execute("DELETE FROM %s WHERE tag = 't' AND ts = ?", new Date(base + 60_000L));
        assertEquals(2, execute("SELECT ts FROM %s WHERE tag = 't'").size());

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), base + 5 * HOUR).windowsEncoded);

        UntypedResultSet rows = execute("SELECT ts, value FROM %s WHERE tag = 't'");
        assertEquals("the row deleted while hot must not come back from the chunk", 2, rows.size());
        UntypedResultSet.Row[] all = rows.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(new Date(base), all[0].getTimestamp("ts"));
        assertEquals(new Date(base + 120_000L), all[1].getTimestamp("ts"));
        assertEquals(2, execute(chunkSelectQuery(), "t", new Date(base)).one().getInt("samples"));
    }

    @Test
    public void aRangeDeletedWhileHotStaysDeletedAfterReEncoding() throws Throwable
    {
        loadRecentWindowForDeletion();
        long base = currentWindowStart();
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, ?)", new Date(base + i * 60_000L), i + 1.0);

        // Range delete over the first two, while all four are hot. The range tombstone's own
        // timestamp is newer than every row's, so the re-encoder's delete does NOT remove it: it is
        // still in the base partition when the merged read runs against the chunk.
        execute("DELETE FROM %s WHERE tag = 't' AND ts >= ? AND ts < ?",
                new Date(base), new Date(base + 120_000L));
        assertEquals(2, execute("SELECT ts FROM %s WHERE tag = 't'").size());

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), base + 5 * HOUR).windowsEncoded);

        UntypedResultSet rows = execute("SELECT ts, value FROM %s WHERE tag = 't'");
        assertEquals("the range deleted while hot must not come back from the chunk", 2, rows.size());
        UntypedResultSet.Row[] all = rows.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(3.0, all[0].getDouble("value"), 0.0);
        assertEquals(4.0, all[1].getDouble("value"), 0.0);
    }

    // ---- the documented tie at exactly max_row_writetime + 1 ----

    @Test
    public void aLateRowAtExactlyMaxWritetimePlusOneTiesWithTheChunk() throws Throwable
    {
        // Chunk rows are reconstructed at max_row_writetime + 1 (ChunkReadSupport), so a late row
        // written at exactly that microsecond TIES with the reconstruction, and Cassandra breaks cell
        // ties by comparing the serialized values (larger wins). This test pins that documented
        // hazard in both directions -- it is the price of (A), not a desirable behaviour.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('bigger', ?, 1.0) USING TIMESTAMP 100", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('smaller', ?, 1.0) USING TIMESTAMP 100", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('bigger', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('smaller', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));
        assertEquals(2, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // maxWt is 100, so the reconstruction sits at 101; write both late rows at exactly 101.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('bigger', ?, 2.0) USING TIMESTAMP 101", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('smaller', ?, 0.5) USING TIMESTAMP 101", new Date(0L));

        // 2.0 sorts above 1.0 as big-endian IEEE-754 bytes, so the late row wins...
        assertEquals(2.0, execute("SELECT value FROM %s WHERE tag = 'bigger' AND ts = ?", new Date(0L))
                          .one().getDouble("value"), 0.0);
        // ...and 0.5 sorts below 1.0, so the STALE CHUNK VALUE wins. This is the documented hazard:
        // a late correction to a smaller value, written at exactly maxWt + 1, is lost.
        assertEquals(1.0, execute("SELECT value FROM %s WHERE tag = 'smaller' AND ts = ?", new Date(0L))
                          .one().getDouble("value"), 0.0);
    }

    @Test
    public void writingRealValuesToColdClusteringsIsStillAllowed() throws Throwable
    {
        // Only UN-writing cold data is refused. A late correction is a supported operation: the
        // re-encoder merges it into the chunk per column on its next cycle.
        chunkOneRow();
        execute("UPDATE %s USING TIMESTAMP 300 SET quality = 7 WHERE tag = 't' AND ts = ?", new Date(0L));
        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(7, rows.one().getInt("quality"));
        assertEquals("the columns the UPDATE did not name still come from the chunk",
                     1.5, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void deletesConfinedToTheHotWindowStillWork() throws Throwable
    {
        // The rule is about COLD data. Ordinary deletes of current data must be untouched -- use real
        // wall-clock clusterings so the rows are unambiguously inside hot_window.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        long recent = System.currentTimeMillis() - 60_000L;

        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 1.0, 5) ", new Date(recent));
        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 2.0, 6) ", new Date(recent + 1000));

        execute("DELETE value FROM %s WHERE tag = 't' AND ts = ?", new Date(recent));
        assertFalse(execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(recent)).one().has("value"));

        execute("UPDATE %s SET quality = null WHERE tag = 't' AND ts = ?", new Date(recent + 1000));
        assertFalse(execute("SELECT quality FROM %s WHERE tag = 't' AND ts = ?", new Date(recent + 1000))
                    .one().has("quality"));

        execute("DELETE FROM %s WHERE tag = 't' AND ts = ?", new Date(recent + 1000));
        assertEquals(0, execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(recent + 1000)).size());
    }

    @Test
    public void aBatchCannotSmuggleAColdDeleteThrough() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("BEGIN BATCH " +
                                  "DELETE value FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ? " +
                                  "APPLY BATCH", new Date(0L));
    }

    @Test
    public void aBatchCannotSmuggleAColdDeleteThroughByPairingItWithAnInsert() throws Throwable
    {
        // The hole the row-marker rule opens if it is applied naively. A BATCH whose statements target
        // the same partition AND the same clustering is merged into ONE PartitionUpdate row before the
        // guard ever sees it, so that row carries the INSERT's primary-key liveness marker *and* the
        // DELETE's cell tombstone. Testing "does this row have a marker" then answers yes, the
        // tombstone check is skipped, and a genuine delete of chunked data is accepted -- masking the
        // chunk's value until gc_grace_seconds purges the tombstone, after which it silently returns.
        //
        // aBatchCannotSmuggleAColdDeleteThrough (above) does not catch this: a bare DELETE produces no
        // marker, so it is refused whether or not the rule is right.
        chunkOneRow();
        // formatQuery substitutes a single %s, so the second reference is spelled out.
        String table = KEYSPACE + '.' + currentTable();
        assertRejectedAsColdWrite("BEGIN BATCH " +
                                  "INSERT INTO %s (tag, ts) VALUES ('t', ?) USING TIMESTAMP 300 " +
                                  "DELETE value FROM " + table + " USING TIMESTAMP 300 WHERE tag = 't' AND ts = ? " +
                                  "APPLY BATCH", new Date(0L), new Date(0L));
    }

    // ---- values Cassandra accepts that a fixed-width column codec cannot represent ----

    @Test
    public void anEmptyFixedWidthValueDoesNotWedgeTheTag() throws Throwable
    {
        // `blobAsInt(0x)` is legal CQL and Int32Serializer.validate accepts 0 bytes as well as 4, so
        // a present-but-empty int cell can reach the encoder. Before the width check it hit
        // ByteBuffer.getInt() on a 0-byte array, the per-tag handler logged the BufferUnderflow and
        // skipped -- identically every cycle, so that partition never tiered again.
        createTable("CREATE TABLE %s (tag text, ts timestamp, quality int, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, quality, value) VALUES ('t', ?, 192, 1.0) USING TIMESTAMP 100",
                new Date(0L));
        // Non-constant in this window (192 vs empty), which is what makes the column take the
        // fixed-width section path rather than the O(1) constant path.
        execute("INSERT INTO %s (tag, ts, quality, value) VALUES ('t', ?, blobAsInt(0x), 2.0) " +
                "USING TIMESTAMP 101", new Date(600_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, stats.windowsEncoded);
        assertEquals(2, stats.rowsEncoded);
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(execute(chunkSelectQuery(), "t", new Date(0L))
                                                          .one().getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(192, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertTrue(cursor.advance());
        assertEquals("the empty value must round-trip as the empty value, byte for byte",
                     0, cursor.getBytes("quality").remaining());
        assertFalse(cursor.advance());
    }

    // ---- the writetime invariant, across columns ----

    @Test
    public void theDeleteTimestampIsTheMaximumWritetimeAcrossEveryColumn() throws Throwable
    {
        // One row whose three columns were written at three different timestamps. Taking any single
        // column's writetime (say the first column's, 100) would tombstone at 100 and leave the
        // cells written at 200/300 alive -- a half-deleted row that is re-encoded forever.
        createTable("CREATE TABLE %s (tag text, ts timestamp, a double, b int, c text, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, a) VALUES ('t', ?, 1.0) USING TIMESTAMP 100", new Date(0L));
        execute("UPDATE %s USING TIMESTAMP 300 SET b = 5 WHERE tag = 't' AND ts = ?", new Date(0L));
        execute("UPDATE %s USING TIMESTAMP 200 SET c = 'z' WHERE tag = 't' AND ts = ?", new Date(0L));
        execute("INSERT INTO %s (tag, ts, a) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t", new Date(0L)).one();
        assertEquals("max_row_writetime must be the maximum over ALL columns",
                     300L, chunkRow.getLong("max_row_writetime"));

        // Nothing of the row is left: had the delete used 100 (or 200), b (and c) would still be here.
        assertEquals(0, raw("SELECT ts, a, b, c FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());

        // A row written after the cycle read the window is newer than the tombstone and survives it.
        execute("INSERT INTO %s (tag, ts, a) VALUES ('t', ?, 4.0) USING TIMESTAMP 301", new Date(600_000L));
        UntypedResultSet late = raw("SELECT a FROM %s WHERE tag = 't' AND ts = ?", new Date(600_000L));
        assertEquals(1, late.size());
        assertEquals(4.0, late.one().getDouble("a"), 0.0);
    }

    // ---- composite partition keys, many columns ----

    @Test
    public void compositePartitionKeyRoundTripsEveryColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, " +
                    "value double, quality int, note text, flag boolean, PRIMARY KEY ((asset_id, date, hour), ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (asset_id, date, hour, ts, value, quality, note, flag) " +
                "VALUES ('a1', '2026-08-01', 0, ?, 1.0, 192, 'x', true) USING TIMESTAMP 101", new Date(600_000L));
        execute("INSERT INTO %s (asset_id, date, hour, ts, value, quality, flag) " +
                "VALUES ('a1', '2026-08-01', 0, ?, 2.0, 192, false) USING TIMESTAMP 102", new Date(1_200_000L));
        // A different partition differing only in the last key column -- must not be conflated.
        execute("INSERT INTO %s (asset_id, date, hour, ts, value, quality, note, flag) " +
                "VALUES ('a1', '2026-08-01', 1, ?, 3.0, 7, 'y', true) USING TIMESTAMP 103", new Date(600_000L));
        execute("INSERT INTO %s (asset_id, date, hour, ts, value) " +
                "VALUES ('a1', '2026-08-01', 0, ?, 9.0) USING TIMESTAMP 110", new Date(4 * HOUR));
        execute("INSERT INTO %s (asset_id, date, hour, ts, value) " +
                "VALUES ('a1', '2026-08-01', 1, ?, 9.0) USING TIMESTAMP 111", new Date(4 * HOUR));

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(2, stats.windowsEncoded);
        assertEquals(3, stats.rowsEncoded);

        String chunkRef = KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
        UntypedResultSet.Row firstChunk =
            execute("SELECT samples, payload FROM " + chunkRef +
                    " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                    "a1", "2026-08-01", 0, new Date(0L)).one();
        assertEquals(2, firstChunk.getInt("samples"));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(firstChunk.getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertEquals(1.0, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertEquals("x", UTF8Type.instance.compose(cursor.getBytes("note")));
        assertTrue(Boolean.TRUE.equals(BooleanType.instance.compose(cursor.getBytes("flag"))));
        assertTrue(cursor.advance());
        assertEquals(1_200_000L, cursor.timestamp());
        assertEquals(2.0, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertTrue("note was never written on this row", cursor.isNull("note"));
        assertFalse(cursor.advance());

        // The other partition kept its own values.
        UntypedResultSet.Row secondChunk =
            execute("SELECT samples, payload FROM " + chunkRef +
                    " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                    "a1", "2026-08-01", 1, new Date(0L)).one();
        assertEquals(1, secondChunk.getInt("samples"));
        ColumnarCursor other = ColumnarChunkCodec.cursor(secondChunk.getBytes("payload"), null);
        assertTrue(other.advance());
        assertEquals(7, (int) Int32Type.instance.compose(other.getBytes("quality")));
        assertEquals("y", UTF8Type.instance.compose(other.getBytes("note")));

        // ...and both partitions read back completely through the merge.
        UntypedResultSet merged = execute("SELECT ts, value, quality, note, flag FROM %s " +
                                          "WHERE asset_id = ? AND date = ? AND hour = ?", "a1", "2026-08-01", 0);
        assertEquals(3, merged.size());
        List<UntypedResultSet.Row> rows = List.of(merged.stream().toArray(UntypedResultSet.Row[]::new));
        assertEquals(1.0, rows.get(0).getDouble("value"), 0.0);
        assertEquals("x", rows.get(0).getString("note"));
        assertFalse(rows.get(1).has("note"));
        assertEquals(9.0, rows.get(2).getDouble("value"), 0.0);
    }

    // ---- SP4 Task 5 (D2): the production shape at production density ----

    /**
     * One sample a second across the whole 1h chunk window -- pp.tm_tag_point's real density, and the
     * only density at which a bytes-per-row number means anything (six rows would be all header).
     */
    private static final int REAL_SHAPE_ROWS = 3600;
    private static final long REAL_SHAPE_STEP_MS = 1000L;

    /** {@code attribute} as pp actually stores it: an empty frozen map, identical on every row. */
    private static ByteBuffer emptyAttribute()
    {
        return ATTRIBUTE_TYPE.decompose(new LinkedHashMap<>());
    }

    /**
     * Writes one full chunk window of tm_tag_point-shaped rows whose column behaviour is pp's
     * measured behaviour:
     * <ul>
     *   <li>{@code quality} constantly 192, {@code error_code} constantly 0,
     *       {@code attribute} constantly {@code {}} -- the CONSTANT path;</li>
     *   <li>{@code value_numeric} never written at all -- the all-null path;</li>
     *   <li>{@code latency} high-entropy (a deterministic uniform draw over 1..999);</li>
     *   <li>{@code value} a short numeric-looking text, produced by a deterministic random walk
     *       rounded to one decimal -- a slowly-varying sensor reading, not white noise.</li>
     * </ul>
     * {@code value_boolean} is controlled by {@code withValueBoolean}: either omitted (the all-null
     * path, modelling a tag that never carries a state bit) or written on every row as a bit that
     * flips every 300 samples (a slowly-toggling state flag). Nothing else differs between the two --
     * the bit is derived from the row index, never from {@code random} -- so both variants draw the
     * identical latency and value streams and any size difference is attributable to that one column.
     * <p>
     * The generator is deliberately seeded and spelled out here because the encoded size this
     * produces is the bytes-per-row figure doc/timeseries/columnar-chunks.md quotes.
     */
    private void loadRealShapeWindow() throws Throwable
    {
        loadRealShapeWindow(false);
    }

    private void loadRealShapeWindow(boolean withValueBoolean) throws Throwable
    {
        execute("INSERT INTO %s (tag_id, area_id, asset_id, line_id, opc_id, site_id, tag_name, type) " +
                "VALUES ('t1', 'a', 'as', 'l', 'o', 's', 'tn', 'ty') USING TIMESTAMP 100");

        Random random = new Random(20260801L);
        double reading = 23.4;
        ByteBuffer attribute = emptyAttribute();
        for (int i = 0; i < REAL_SHAPE_ROWS; i++)
        {
            reading += (random.nextDouble() - 0.5) * 0.4;
            if (withValueBoolean)
                execute("INSERT INTO %s (tag_id, timestamp, attribute, error_code, latency, quality, value, " +
                        "value_boolean) VALUES ('t1', ?, ?, 0, ?, 192, ?, ?) USING TIMESTAMP ?",
                        new Date(i * REAL_SHAPE_STEP_MS), attribute, 1 + random.nextInt(999),
                        String.format("%.1f", reading), (i / 300) % 2 == 0, 1000L + i);
            else
                execute("INSERT INTO %s (tag_id, timestamp, attribute, error_code, latency, quality, value) " +
                        "VALUES ('t1', ?, ?, 0, ?, 192, ?) USING TIMESTAMP ?",
                        new Date(i * REAL_SHAPE_STEP_MS), attribute, 1 + random.nextInt(999),
                        String.format("%.1f", reading), 1000L + i);
        }
        // One hot row, so the window boundary is exercised exactly as in the other tests.
        execute("INSERT INTO %s (tag_id, timestamp, latency) VALUES ('t1', ?, 1) USING TIMESTAMP 200",
                new Date(4 * HOUR));
    }

    @Test
    public void productionShapedColumnsTakeTheConstantAndAllNullPaths() throws Throwable
    {
        createProductionShapedTable();
        loadRealShapeWindow();

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, stats.windowsEncoded);
        assertEquals(REAL_SHAPE_ROWS, stats.rowsEncoded);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t1", new Date(0L)).one();
        assertEquals(REAL_SHAPE_ROWS, chunkRow.getInt("samples"));
        ByteBuffer payload = chunkRow.getBytes("payload");
        Map<String, ChunkV4Directory.Entry> directory = readDirectory(payload);

        // Round-tripping alone cannot tell a CONSTANT column from one that silently fell back to a
        // full-width section holding 3600 copies of 192, so assert the directory itself: the flag is
        // set AND the column's data section is empty. Both halves matter -- a flag with a non-empty
        // section would mean the bytes were written anyway.
        for (String constant : new String[]{ "quality", "error_code", "attribute" })
        {
            ChunkV4Directory.Entry entry = directory.get(constant);
            assertNotNull("no directory entry for " + constant + " in " + directory.keySet(), entry);
            assertTrue(constant + " must be encoded via the CONSTANT flag, flags=" + entry.colFlags, entry.constant());
            assertEquals(constant + " must occupy no data section at all", 0, entry.sectionLen);
        }

        ChunkV4Directory.Entry numeric = directory.get("value_numeric");
        assertNotNull("no directory entry for value_numeric", numeric);
        assertTrue("value_numeric was never written and must take the all-null flag, flags=" + numeric.colFlags,
                   numeric.allNull());
        assertFalse("an all-null column must not also claim to be constant", numeric.constant());
        assertEquals("value_numeric must occupy no data section at all", 0, numeric.sectionLen);

        // The control: the high-entropy column must NOT have been folded away, or the assertions
        // above would be satisfied by an encoder that constant-folds everything.
        ChunkV4Directory.Entry latency = directory.get("latency");
        assertFalse("latency varies per row and must not be CONSTANT", latency.constant());
        assertTrue("latency must occupy a real data section, got " + latency.sectionLen, latency.sectionLen > 1000);

        // Bytes per row for THIS variant only -- value_boolean unwritten, so two of the eight regular
        // columns cost nothing. That flatters the format, which is why it is not the figure the docs
        // quote; realShapeBytesPerRowIsMeasuredWithValueBooleanPopulated below owns that. Logged here
        // because this test is where the all-null path is proven, and the gate is the production
        // baseline rather than an exact figure so codec tuning does not break it.
        double bytesPerRow = payload.remaining() / (double) REAL_SHAPE_ROWS;
        logger.info("SP4 real-shape chunk (value_boolean absent): {} rows, {} payload bytes, {} bytes/row",
                    REAL_SHAPE_ROWS, payload.remaining(), bytesPerRow);
        assertTrue("real-shape chunk must beat pp's ~9 B/row on-disk baseline, measured " + bytesPerRow,
                   bytesPerRow < 9.0);
    }

    /**
     * The bytes-per-row figure doc/timeseries/columnar-chunks.md quotes, and the control that shows
     * what the earlier figure left out.
     * <p>
     * Two windows of the identical tm_tag_point shape and identical generator, differing only in
     * whether {@code value_boolean} is written. Modelling it as never written -- which the first
     * measurement did -- lets it take ALL_NULL and cost nothing, so the resulting number describes a
     * table with one fewer column than the schema declares. Populating it is the honest measurement,
     * and both are logged so the delta is attributable to exactly that column.
     */
    @Test
    public void realShapeBytesPerRowIsMeasuredWithValueBooleanPopulated() throws Throwable
    {
        createProductionShapedTable();
        loadRealShapeWindow(false);
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        int absentBytes = execute(chunkSelectQuery(), "t1", new Date(0L)).one().getBytes("payload").remaining();

        createProductionShapedTable();
        loadRealShapeWindow(true);
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        ByteBuffer payload = execute(chunkSelectQuery(), "t1", new Date(0L)).one().getBytes("payload");

        // The column has to be carrying real data or this measures the same optimistic thing twice:
        // not ALL_NULL, not folded to CONSTANT, and a section of exactly one packed bit per row plus
        // v4's per-block metadata: an 8-byte block-table entry per block (statWidth 0), and each
        // block's BITPACK1 body of ceil(rows/64) 64-bit lanes -- no presence bytes, the column is
        // ALL_PRESENT. The closed form is v4 §4/§5's, evaluated here rather than hard-coded so the
        // derivation is visible.
        ChunkV4Directory.Entry flag = readDirectory(payload).get("value_boolean");
        assertNotNull("no directory entry for value_boolean", flag);
        assertFalse("value_boolean is written on every row here and must not be ALL_NULL", flag.allNull());
        assertFalse("value_boolean flips and must not fold to CONSTANT", flag.constant());
        int blockSize = 1 << ChunkV4Header.DEFAULT_BLOCK_SIZE_LOG2;
        int blocks = (REAL_SHAPE_ROWS + blockSize - 1) / blockSize;
        int expectedSection = blocks * 8;                          // block-table entries, statWidth 0
        for (int b = 0; b < blocks; b++)
        {
            int rowsInBlock = Math.min(blockSize, REAL_SHAPE_ROWS - b * blockSize);
            expectedSection += ((rowsInBlock + 63) / 64) * 8;      // BITPACK1: one bit per row, 64-bit lanes
        }
        assertEquals("a boolean section is one packed bit per row plus the block table",
                     expectedSection, flag.sectionLen);

        double absent = absentBytes / (double) REAL_SHAPE_ROWS;
        double populated = payload.remaining() / (double) REAL_SHAPE_ROWS;
        logger.info("SP4 real-shape bytes/row over {} rows: value_boolean absent = {} B -> {} B/row; " +
                    "value_boolean populated = {} B -> {} B/row",
                    REAL_SHAPE_ROWS, absentBytes, absent, payload.remaining(), populated);
        assertTrue("writing a column cannot make the chunk smaller: " + absentBytes + " -> " + payload.remaining(),
                   payload.remaining() > absentBytes);
        assertTrue("real-shape chunk must beat pp's ~9 B/row on-disk baseline, measured " + populated,
                   populated < 9.0);
    }

    @Test
    public void everyCellOfTheProductionShapeSurvivesADenseWindow() throws Throwable
    {
        // The dense counterpart to everyColumnOfEveryRowRoundTripsThroughTheChunk: 3600 rows is where
        // the null bitmap RLE, the dictionary threshold and the timestamp DoD stream all actually run,
        // rather than being short-circuited by a six-row window.
        createProductionShapedTable();
        loadRealShapeWindow();
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        Random random = new Random(20260801L);
        double reading = 23.4;
        UntypedResultSet merged = execute("SELECT timestamp, attribute, error_code, latency, quality, value, " +
                                          "value_boolean, value_numeric FROM %s WHERE tag_id = 't1' " +
                                          "AND timestamp < ? ORDER BY timestamp ASC", new Date(HOUR));
        assertEquals(REAL_SHAPE_ROWS, merged.size());
        int i = 0;
        for (UntypedResultSet.Row row : merged)
        {
            reading += (random.nextDouble() - 0.5) * 0.4;
            assertEquals(new Date(i * REAL_SHAPE_STEP_MS), row.getTimestamp("timestamp"));
            assertEquals(emptyAttribute(), row.getBytes("attribute"));
            assertEquals(0, row.getInt("error_code"));
            assertEquals(192, row.getInt("quality"));
            assertEquals("row " + i, 1 + random.nextInt(999), row.getInt("latency"));
            assertEquals("row " + i, String.format("%.1f", reading), row.getString("value"));
            assertFalse("value_boolean was never written", row.has("value_boolean"));
            assertFalse("value_numeric was never written", row.has("value_numeric"));
            i++;
        }

        // The statics are outside every clustering range the re-encoder deletes, so they must still
        // be here after a window that removed 3600 rows.
        UntypedResultSet.Row statics = raw("SELECT area_id, site_id, tag_name, type FROM %s WHERE tag_id = 't1'").one();
        assertEquals("s", statics.getString("site_id"));
        assertEquals("tn", statics.getString("tag_name"));
        assertEquals(0, raw("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp < ?", new Date(HOUR)).size());
    }

    // ---- helpers ----

    /**
     * The chunk's v4 directory entries, re-read from the payload's own metadata. Asserting on the
     * round trip alone cannot distinguish "encoded as CONSTANT" from "encoded at full width and
     * decoded correctly", so these tests assert on the directory itself -- flags and section
     * lengths as stored in the bytes. {@link ChunkV4Codec#open} parses exactly the header and
     * directory (no section, no block table, no body) and the entry layout it reads is
     * golden-pinned by {@code ChunkV4DirectoryTest}, so this is the payload's own claim, not the
     * decoder's reconstruction.
     */
    private static Map<String, ChunkV4Directory.Entry> readDirectory(ByteBuffer payload)
    {
        assertEquals("not a v4 columnar payload", ColumnarChunkCodec.VERSION, payload.get(payload.position()));
        Map<String, ChunkV4Directory.Entry> directory = new LinkedHashMap<>();
        for (ChunkV4Directory.Entry entry : ChunkV4Codec.open(payload).directory().entries())
            directory.put(entry.name, entry);
        return directory;
    }

    private static ByteBuffer attribute(String unit)
    {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("unit", unit);
        return ATTRIBUTE_TYPE.decompose(map);
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

    private String chunkTableRef()
    {
        return KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
    }

    private String chunkSelectQuery()
    {
        return "SELECT * FROM " + chunkTableRef() + " WHERE " + partitionKeyName() + " = ? AND window_start = ?";
    }

    private String chunkPayloadQuery()
    {
        return "SELECT payload, WRITETIME(payload) AS chunk_wt FROM " + chunkTableRef() +
               " WHERE " + partitionKeyName() + " = ? AND window_start = ?";
    }

    private String partitionKeyName()
    {
        return getCurrentColumnFamilyStore().metadata().partitionKeyColumns().get(0).name.toCQLString();
    }
}
