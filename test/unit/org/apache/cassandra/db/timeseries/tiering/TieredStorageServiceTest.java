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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ChunkV4Directory;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.db.timeseries.StatOrder;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for {@link TieredStorageService#runOnce}, exercising the whole re-encode cycle
 * against a real (single-node) schema and real CQL queries -- see
 * docs/superpowers/plans/2026-07-31-chunk-store-sp2.md, Task 2, for the scenarios this covers.
 */
public class TieredStorageServiceTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    // NOT named setUpClass(): that exact name would shadow CQLTester's own @BeforeClass setUpClass()
    // (same name+signature in the hierarchy -- only the most-derived one runs), skipping the server/
    // schema setup it performs and breaking every test in this class, not just the ones added here.
    @BeforeClass
    public static void setUpVirtualKeyspace()
    {
        addVirtualKeyspace(); // registers system_views, for virtualTableShowsPolicyAndStats
    }

    @Test
    public void encodeClosedWindowsAndDeleteRows() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        String[] tags = { "a", "b", "c" };
        long now = 5 * HOUR;
        long wt = 1;
        for (String tag : tags)
        {
            for (int w = 0; w < 3; w++)
            {
                long windowStart = w * HOUR;
                for (int r = 0; r < 4; r++)
                    insertRow(tag, windowStart + r * 600_000L, w * 100.0 + r, wt++);
            }
            insertRow(tag, 4 * HOUR, 999.0, wt++); // hot row -- must survive untouched
        }

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);

        assertEquals(9, stats.windowsEncoded); // 3 tags * 3 closed windows
        assertEquals(36, stats.rowsEncoded);   // 3 tags * 3 windows * 4 rows
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.chunksExpired);
        assertTrue(stats.bytesWritten > 0);

        for (String tag : tags)
        {
            for (int w = 0; w < 3; w++)
            {
                long windowStart = w * HOUR;
                UntypedResultSet chunkRows = execute(chunkSelectQuery(), tag, new Date(windowStart));
                assertEquals(1, chunkRows.size());
                UntypedResultSet.Row chunkRow = chunkRows.one();
                assertEquals(4, chunkRow.getInt("samples"));
                assertEquals(ColumnarChunkCodec.VERSION, chunkRow.getByte("codec"));

                assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                        tag, new Date(windowStart), new Date(windowStart + HOUR)).size());
            }

            assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", tag, new Date(4 * HOUR)).size());
        }
    }

    @Test
    public void hotWindowRowsSurviveDenseIngestion() throws Throwable
    {
        // Regression for a round-2 review finding: the window walk's cutoff check must be enforced on
        // EVERY path that can advance windowStart, including the dense "windowStart = windowEnd"
        // continuation after a successfully-processed window -- not just on initial discovery and the
        // empty-window jump. Continuous, gap-free ingestion across the cutoff boundary is required to
        // exercise this: an empty window anywhere would route through nextClosedWindowStart's own
        // cutoff check and mask the bug, which is exactly how the round-1 test suite missed it (every
        // hot marker row there sat past an empty window).
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long now = 5 * HOUR;
        long cutoff = 3 * HOUR; // windowStartFor(now - hot_window) = windowStartFor(3h) = 3h

        insertRow("dense", 0L, 10.0, 1);          // window [0,1h)      -- closed
        insertRow("dense", HOUR, 11.0, 2);        // window [1h,2h)     -- closed
        insertRow("dense", cutoff - 1, 12.0, 3);  // window [2h,3h)     -- closed; pins cutoff-1ms
        insertRow("dense", cutoff, 13.0, 4);      // window [3h,4h)     -- hot; pins ts == cutoff exactly
        insertRow("dense", 4 * HOUR, 14.0, 5);    // window [4h,5h)     -- hot ("current" window)

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);

        assertEquals(3, stats.windowsEncoded); // only the 3 closed windows
        assertEquals(3, stats.rowsEncoded);

        // Every row at or after cutoff -- including the one exactly at ts == cutoff -- must still be
        // in base, completely untouched.
        assertEquals(2, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ?", "dense", new Date(cutoff)).size());
        assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "dense", new Date(cutoff)).size());
        assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "dense", new Date(4 * HOUR)).size());

        // The closed rows (ts < cutoff) are gone.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "dense", new Date(cutoff)).size());

        // No chunk was ever written for a window at or after cutoff.
        assertEquals(0, execute(chunkSelectQuery(), "dense", new Date(cutoff)).size());
        assertEquals(0, execute(chunkSelectQuery(), "dense", new Date(4 * HOUR)).size());

        // The three closed windows were encoded normally.
        for (long windowStart : new long[]{ 0L, HOUR, 2 * HOUR })
            assertEquals(1, execute(chunkSelectQuery(), "dense", new Date(windowStart)).size());
    }

    @Test
    public void roundtripThroughChunks() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long[] tsValues = { 0L, 600_000L, 1_200_000L, 1_800_000L, 2_400_000L };
        double[] values = { 1.5, -2.25, 3.0, 0.0, 42.125 };
        long wt = 1;
        for (int i = 0; i < tsValues.length; i++)
            insertRow("solo", tsValues[i], values[i], wt++);
        insertRow("solo", 4 * HOUR, 999.0, wt++);

        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "solo", new Date(0L)).one();
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        for (int i = 0; i < tsValues.length; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(tsValues[i], cursor.timestamp());
            assertEquals(values[i], doubleAt(cursor, "value"), 0.0);
        }
        assertFalse(cursor.advance());
    }

    @Test
    public void deleteTimestampPreservesLateRows() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        for (int i = 0; i < 4; i++)
            insertRow("t", i * 600_000L, i * 1.0, 100 + i); // writetimes 100..103
        insertRow("t", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated after the window is cleared

        TieredStorageService service = new TieredStorageService();
        TierRunStats first = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, first.windowsEncoded);
        assertEquals(4, first.rowsEncoded);
        assertEquals(0, first.lateMerges);

        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "t", new Date(0L), new Date(HOUR)).size());

        // Late row: newer writetime than the tombstone the first run issued (USING TIMESTAMP 103),
        // which is all this test needs -- every read here runs under enterInternalBypass() (see
        // raw()), so it asserts on PHYSICAL rows and never exercises the hot+chunk merge. 110 rather
        // than 104 only so the number does not sit on max_row_writetime + 1, where the merge's
        // documented tie would apply if this test were ever changed to read through it;
        // TieredStorageColumnsTest#aLateRowAtExactlyMaxWritetimePlusOneTiesWithTheChunk covers that.
        long lateTs = 2_400_000L;
        insertRow("t", lateTs, 42.0, 110);

        UntypedResultSet survived = raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "t", new Date(lateTs));
        assertEquals(1, survived.size());
        assertEquals(42.0, survived.one().getDouble("value"), 0.0);

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, second.windowsEncoded);
        assertEquals(1, second.rowsEncoded);
        assertEquals(1, second.lateMerges);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t", new Date(0L)).one();
        assertEquals(5, chunkRow.getInt("samples"));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        boolean foundLate = false;
        while (cursor.advance())
        {
            if (cursor.timestamp() == lateTs)
            {
                foundLate = true;
                assertEquals(42.0, doubleAt(cursor, "value"), 0.0);
            }
        }
        assertTrue("expected the late sample to be present in the merged chunk", foundLate);

        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "t", new Date(lateTs)).size());
    }

    @Test
    public void idempotentWhenInterrupted() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        long[] tsValues = { 0L, 600_000L, 1_200_000L, 1_800_000L };
        double[] values = { 1.0, 2.0, 3.0, 4.0 };
        for (int i = 0; i < tsValues.length; i++)
            insertRow("i", tsValues[i], values[i], 297 + i); // writetimes 297..300
        insertRow("i", 4 * HOUR, 999.0, 400); // hot row -- keeps the tag enumerated

        // Simulate "wrote the chunk, crashed before the delete": pre-write a chunk matching the
        // still-live rows, at the same timestamp (301 = maxWt+1 = 300+1) a genuine first pass would
        // have used -- NOT the wall-clock timestamp a plain execute() would default to (which, being
        // far larger than anything runOnce ever writes, would make every cell of this pre-written row
        // permanently win over runOnce's re-merge and pass the assertions below vacuously).
        ByteBuffer payload = encodeDoubleChunk(tsValues, values, tsValues.length);
        execute(chunkInsertQuery(), "i", new Date(0L), tsValues.length, 300L, payload, 301L);

        assertEquals(4, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "i", new Date(0L), new Date(HOUR)).size());

        // The re-encode is deterministic, so the resumed cycle rebuilds byte-for-byte what is already
        // stored and recognises it has nothing new to write: it writes NO chunk (all stats zero) but
        // still issues the delete the crashed cycle never got to.
        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(0, stats.windowsEncoded);
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.rowsEncoded);
        assertEquals(0, stats.bytesWritten);

        UntypedResultSet.Row chunkRow = execute("SELECT samples, payload, WRITETIME(payload) AS chunk_wt FROM " +
                                                chunkTableRef() + " WHERE tag = ? AND window_start = ?",
                                                "i", new Date(0L)).one();
        assertEquals(4, chunkRow.getInt("samples")); // converged, not duplicated
        // Untouched, not rewritten: the chunk row still carries the pre-written cycle's timestamp.
        assertEquals(301L, chunkRow.getLong("chunk_wt"));

        // Decode and check the actual (ts, value) content, not just the sample count.
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        for (int i = 0; i < tsValues.length; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(tsValues[i], cursor.timestamp());
            assertEquals(values[i], doubleAt(cursor, "value"), 0.0);
        }
        assertFalse(cursor.advance());

        // The interrupted cycle's delete now completes.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "i", new Date(0L), new Date(HOUR)).size());
    }

    @Test
    public void runningTheSameCycleTwiceIsANoOp() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, extra int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (tag, ts, value, extra) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "t", new Date(i * 600_000L), i * 1.5, i, 100L + i);
        insertRow("t", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        UntypedResultSet.Row first = execute("SELECT payload, WRITETIME(payload) AS chunk_wt FROM " +
                                             chunkTableRef() + " WHERE tag = ? AND window_start = ?",
                                             "t", new Date(0L)).one();
        ByteBuffer firstPayload = ByteBufferUtil.clone(first.getBytes("payload"));
        long firstWritetime = first.getLong("chunk_wt");

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(0, second.windowsEncoded);
        assertEquals(0, second.rowsEncoded);
        assertEquals(0, second.lateMerges);
        assertEquals(0, second.bytesWritten);

        UntypedResultSet.Row after = execute("SELECT payload, WRITETIME(payload) AS chunk_wt FROM " +
                                             chunkTableRef() + " WHERE tag = ? AND window_start = ?",
                                             "t", new Date(0L)).one();
        assertEquals("the chunk must be byte-identical after a second cycle", firstPayload, after.getBytes("payload"));
        assertEquals(firstWritetime, after.getLong("chunk_wt"));
    }

    @Test
    public void coldWindowExpiry() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\",\"cold_window\":\"2h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        long now = 10 * HOUR;
        insertRow("cold", now - 100_000L, 1.0, 1); // hot row -- keeps the tag enumerated

        ByteBuffer expiring = encodeDoubleChunk(new long[]{ 0L }, new double[]{ 1.0 }, 1);
        execute(chunkInsertQuery(), "cold", new Date(0L), 1, 500L, expiring, 1L);

        ByteBuffer surviving = encodeDoubleChunk(new long[]{ 9 * HOUR }, new double[]{ 2.0 }, 1);
        execute(chunkInsertQuery(), "cold", new Date(9 * HOUR), 1, 600L, surviving, 2L);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);
        assertEquals(1, stats.chunksExpired);

        assertEquals(0, execute(chunkSelectQuery(), "cold", new Date(0L)).size());
        assertEquals(1, execute(chunkSelectQuery(), "cold", new Date(9 * HOUR)).size());
    }

    @Test
    public void deadTagColdChunksStillExpire() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\",\"cold_window\":\"2h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        // SP3 R6: this tag has NO base rows at all (fully re-encoded, then the base rows aged away) -
        // it exists only in the chunk table. Chunk-table-driven expiry enumeration must still find
        // and expire its cold chunk; the old base-DISTINCT enumeration never would have.
        ByteBuffer expiring = encodeDoubleChunk(new long[]{ 0L }, new double[]{ 1.0 }, 1);
        execute(chunkInsertQuery(), "dead", new Date(0L), 1, 500L, expiring, 1L);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 10 * HOUR);
        assertEquals(1, stats.chunksExpired);
        assertEquals(0, execute(chunkSelectQuery(), "dead", new Date(0L)).size());
    }

    @Test
    public void unsupportedSchemaSkipsWithError() throws Throwable
    {
        // A second clustering column: no time axis a chunk could encode. See TieringPolicyTest /
        // TieringSchemaSupportTest for the whole accept/reject matrix.
        createTable("CREATE TABLE %s (tag text, ts timestamp, seq int, value double, PRIMARY KEY (tag, ts, seq))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        assertSkippedWithError("clustering column");
    }

    /** Runs one cycle and asserts it did nothing but log an ERROR containing {@code expectedInMessage}. */
    private void assertSkippedWithError(String expectedInMessage) throws Throwable
    {
        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        TierRunStats stats;
        try
        {
            stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertTrue("expected an ERROR log containing '" + expectedInMessage + "', got: " + appender.list,
                       appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR &&
                                                            e.getFormattedMessage().contains(expectedInMessage)));
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        assertEquals(0, stats.windowsEncoded);
        assertEquals(0, stats.rowsEncoded);
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.chunksExpired);
        assertEquals(0, stats.bytesWritten);
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())));
    }

    @Test
    public void everyChunkIsWrittenWithTheColumnarCodecVersion() throws Throwable
    {
        // The columnar format is the only chunk format written, so the chunk row's `codec` column is
        // a fixed 4 for every pattern -- constant series and quantized walks alike. (This replaces
        // the per-window gorilla/chimp bake-off that used to make this column vary.)
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        int n = 100;
        long wt = 1;

        for (int i = 0; i < n; i++)
            insertRow("const", i * 30_000L, 5.0, wt++);

        Random random = new Random(17);
        double walk = 50.0;
        for (int i = 0; i < n; i++)
        {
            walk += (random.nextInt(3) - 1) * 0.1;
            insertRow("quant", i * 30_000L, Math.round(walk * 10.0) / 10.0, wt++);
        }

        insertRow("const", 4 * HOUR, 999.0, wt++);
        insertRow("quant", 4 * HOUR, 999.0, wt++);

        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        byte constCodec = execute(chunkSelectQuery(), "const", new Date(0L)).one().getByte("codec");
        byte quantCodec = execute(chunkSelectQuery(), "quant", new Date(0L)).one().getByte("codec");

        assertEquals(ColumnarChunkCodec.VERSION, constCodec);
        assertEquals(ColumnarChunkCodec.VERSION, quantCodec);
    }

    @Test
    public void rowWithNoLiveCellIsEncodedRatherThanDeletedUnencoded() throws Throwable
    {
        // A row whose every regular column is null still EXISTS, and the range delete would take it,
        // so it must go into the chunk (as a timestamp with all columns null) rather than be skipped.
        // Skipping it -- what the single-column re-encoder did -- silently destroyed the row.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        insertRow("n", 0L, 7.5, 50);
        // Bare key insert -- a live row with no value cell at all, so WRITETIME(value) is null too.
        execute("INSERT INTO %s (tag, ts) VALUES (?, ?) USING TIMESTAMP 10", "n", new Date(600_000L));
        insertRow("n", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(1, stats.windowsEncoded);
        assertEquals(2, stats.rowsEncoded); // BOTH rows, the value-less one included

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "n", new Date(0L)).one();
        assertEquals(2, chunkRow.getInt("samples"));
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(0L, cursor.timestamp());
        assertEquals(7.5, doubleAt(cursor, "value"), 0.0);
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertTrue("the value-less row must round-trip as null, not as 0.0", cursor.isNull("value"));
        assertFalse(cursor.advance());

        // The range delete covers the whole window regardless of which individual rows had a value.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "n", new Date(0L), new Date(HOUR)).size());

        // ...and a plain (merged) SELECT still sees both rows, the second with a null value.
        UntypedResultSet merged = execute("SELECT ts, value FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                          "n", new Date(0L), new Date(HOUR));
        assertEquals(2, merged.size());
        UntypedResultSet.Row[] rows = merged.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(7.5, rows[0].getDouble("value"), 0.0);
        assertFalse("the reconstructed row must have no value cell", rows[1].has("value"));
    }

    @Test
    public void windowWithNoCellWritetimeIsLeftCompletelyUntouched() throws Throwable
    {
        // Every row in the window is a bare primary-key insert, so no WRITETIME exists anywhere in
        // it. There is then no timestamp the range delete could use that is provably not newer than
        // some row it would destroy, so the cycle must leave the window entirely alone -- not encode
        // it and delete at a guessed timestamp.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts) VALUES (?, ?) USING TIMESTAMP 10", "n", new Date(0L));
        execute("INSERT INTO %s (tag, ts) VALUES (?, ?) USING TIMESTAMP 11", "n", new Date(600_000L));
        insertRow("n", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(0, stats.windowsEncoded);
        assertEquals(0, execute(chunkSelectQuery(), "n", new Date(0L)).size());
        assertEquals(2, raw("SELECT ts FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                            "n", new Date(0L), new Date(HOUR)).size());
    }

    @Test
    public void corruptChunkOnOneTagDoesNotAbortOtherTags() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        // "bad": an existing chunk row whose payload is not a valid codec payload at all, so decoding
        // it for the merge throws mid-cycle.
        insertRow("bad", 0L, 1.0, 10);
        insertRow("bad", 4 * HOUR, 999.0, 20);
        ByteBuffer garbage = ByteBufferUtil.bytes("not a valid chunk payload");
        execute(chunkInsertQuery(), "bad", new Date(0L), 1, 5L, garbage, 6L);

        // "good": an ordinary closed window that must still get encoded despite "bad" throwing.
        insertRow("good", 0L, 2.0, 10);
        insertRow("good", 4 * HOUR, 999.0, 20);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(1, stats.windowsEncoded); // only "good"'s window succeeded
        assertEquals(1, execute(chunkSelectQuery(), "good", new Date(0L)).size());
        // ...and the cycle SAYS it skipped one. Without this the run returns all-clear stats while
        // having silently under-encoded the table, which is an availability failure reported as
        // success -- the same defect class as swallowing a chunk-read timeout on the read path.
        assertEquals(1, stats.tagsSkipped);
    }

    @Test
    public void retierFailsWhenTheCycleSkippedTags() throws Throwable
    {
        // nodetool retier is a one-shot operator instruction, not a background tick: a cycle that
        // could not finish some tags did not do what it was asked, so it must exit non-zero rather
        // than print nothing and return 0.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        // retier() drives the cycle off real wall-clock time, so use a window that is unambiguously
        // closed whenever the test runs.
        long windowStart = (System.currentTimeMillis() / HOUR - 3) * HOUR;
        insertRow("bad", windowStart, 1.0, 10);
        execute(chunkInsertQuery(), "bad", new Date(windowStart), 1, 5L,
                ByteBufferUtil.bytes("not a valid chunk payload"), 6L);

        String table = currentTable();
        try
        {
            TieredStorageService.instance.retier(KEYSPACE, table);
            fail("expected retier to fail after skipping a tag");
        }
        catch (IllegalStateException e)
        {
            throw e;                                      // gate contention, not what this asserts
        }
        catch (RuntimeException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("tag(s) skipped"));
        }

        // The stats are still recorded, so the virtual table / tieringstatus can show the skip.
        assertEquals(1, TieredStorageService.instance.lastStats(KEYSPACE, table).tagsSkipped);
    }

    @Test
    public void oversizedWindowAbortsThatTagOnlyWithError() throws Throwable
    {
        // Regression for a final-review finding: a window holding more samples than the service will
        // encode (TieredStorageService.maxSamplesPerWindow in production; shrunk to 3 here via that
        // same seam) must be detected while paging -- never fully materialized -- and abort that tag's walk
        // with an actionable ERROR, instead of blowing up in encode and re-reading the giant window
        // every cycle forever. Other tags in the same run must still encode.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        // "big": 5 rows in one closed window -- over the injected 3-sample cap.
        for (int i = 0; i < 5; i++)
            insertRow("big", i * 60_000L, i * 1.0, 10 + i);
        insertRow("big", 4 * HOUR, 999.0, 99);

        // "small": 2 rows in one closed window -- under the cap, must encode normally.
        insertRow("small", 0L, 1.0, 10);
        insertRow("small", 60_000L, 2.0, 11);
        insertRow("small", 4 * HOUR, 999.0, 99);

        TieredStorageService service = new TieredStorageService();
        service.maxSamplesPerWindow = 3;

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        TierRunStats stats;
        try
        {
            stats = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        // "big" encoded nothing: no chunk row, and every source row is still in base, untouched.
        assertEquals(0, execute(chunkSelectQuery(), "big", new Date(0L)).size());
        assertEquals(5, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "big", new Date(HOUR)).size());

        // The abort was logged at ERROR, naming the tag and telling the operator what to change.
        assertTrue("expected an ERROR naming tag 'big' and pointing at chunk_window",
                   appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR &&
                                                        e.getFormattedMessage().contains("big") &&
                                                        e.getFormattedMessage().contains("chunk_window")));

        // "small" still encoded on the very same run.
        assertEquals(1, stats.windowsEncoded);
        assertEquals(2, stats.rowsEncoded);
        assertEquals(1, execute(chunkSelectQuery(), "small", new Date(0L)).size());
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "small", new Date(HOUR)).size());
    }

    @Test
    public void descClusteredTableDrainsBacklogInOneRun() throws Throwable
    {
        // Regression for a final-review finding: with CLUSTERING ORDER BY (ts DESC) -- the dominant
        // time-series idiom -- an order-less LIMIT 1 probe returns the NEWEST row in range, so the
        // empty-window jump would leap over every window between the gap and the cutoff and the
        // backlog would drain a couple of windows per cycle instead of completely. The deliberate
        // empty window [1h,2h) below forces the walk through that jump (nextClosedWindowStart): it
        // must land on the OLDEST remaining row's window (2h), not the newest's (3h).
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC)");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long now = 6 * HOUR; // cutoff = windowStartFor(6h - 2h) = 4h
        insertRow("d", 0L, 1.0, 1);        // window [0,1h)  -- closed
        insertRow("d", 600_000L, 1.5, 2);  // window [0,1h)  -- closed
        insertRow("d", 2 * HOUR, 2.0, 3);  // window [2h,3h) -- closed, after the empty [1h,2h) gap
        insertRow("d", 3 * HOUR, 3.0, 4);  // window [3h,4h) -- closed
        insertRow("d", 5 * HOUR, 999.0, 5); // hot -- must survive untouched

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);

        // The whole multi-window backlog drained in this single run...
        assertEquals(3, stats.windowsEncoded);
        assertEquals(4, stats.rowsEncoded);
        for (long windowStart : new long[]{ 0L, 2 * HOUR, 3 * HOUR })
            assertEquals("expected a chunk for window starting at " + windowStart,
                         1, execute(chunkSelectQuery(), "d", new Date(windowStart)).size());
        assertEquals(2, execute(chunkSelectQuery(), "d", new Date(0L)).one().getInt("samples"));

        // ...every closed source row is gone, and the hot row survived.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "d", new Date(4 * HOUR)).size());
        assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "d", new Date(5 * HOUR)).size());
    }

    @Test
    public void virtualTableShowsPolicyAndStats() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        // retier() (unlike runOnce() in the other tests here) drives the cycle off real wall-clock
        // time, so use a window safely in the past -- hour-aligned, a few hours ago -- rather than a
        // synthetic small timestamp, so it is unambiguously closed no matter when the test runs.
        long windowStart = (System.currentTimeMillis() / HOUR - 3) * HOUR;
        insertRow("v", windowStart, 1.0, 1);

        String table = currentTable();
        TieredStorageService.instance.retier(KEYSPACE, table);

        UntypedResultSet rows = execute("SELECT * FROM system_views.timeseries_tiering WHERE keyspace_name = ? AND table_name = ?",
                                        KEYSPACE, table);
        assertEquals(1, rows.size());
        UntypedResultSet.Row row = rows.one();
        assertEquals(2 * HOUR, row.getLong("hot_window_ms"));
        assertEquals(HOUR, row.getLong("chunk_window_ms"));
        assertEquals(1, row.getLong("windows_encoded"));
        assertEquals(1, row.getLong("rows_encoded"));
        assertEquals(0, row.getLong("late_merges"));
        assertEquals(0, row.getLong("chunks_expired"));
        assertEquals(0, row.getLong("tags_skipped"));
        assertTrue("last_run_at should be a real timestamp once retier has run", row.getLong("last_run_at") > 0);
    }

    @Test
    public void sweepIsolatesPerTableFailures() throws Throwable
    {
        // Regression for a review finding: one table's run failing inside the global sweep (e.g. an
        // UnavailableException because its keyspace cannot meet the policy's consistency level) must
        // not abort the tick for every table iterated after it, and must be logged rather than
        // escaping into the scheduled executor (whose failure wrapper swallows request-failure
        // exceptions silently). Two policy-bearing tables; one's run deterministically throws via the
        // preRunHookForTesting seam (a healthy single-node cluster cannot provoke the real failure).
        // The test is order-independent: without the per-table catch the injected throwable escapes
        // sweep() and fails this test no matter which table the schema walk visits first.
        String badTable = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        String goodTable = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TieredStorageService service = new TieredStorageService(); // fresh instance: both tables are due (never run)
        service.preRunHookForTesting = (ks, table) ->
        {
            if (badTable.equals(table))
                throw new RuntimeException("injected failure for " + ks + '.' + table);
        };

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        try
        {
            service.sweep(); // must complete without throwing
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        // The failing table was logged (keyspace.table named), not silently swallowed...
        assertTrue("expected a WARN naming the failed table",
                   appender.list.stream().anyMatch(e -> e.getLevel() == Level.WARN &&
                                                        e.getFormattedMessage().contains(KEYSPACE + "." + badTable)));
        // ...its run never completed...
        assertNull(service.lastRunAtMillis(KEYSPACE, badTable));
        assertNull(service.lastStats(KEYSPACE, badTable));
        // ...and the other table still got its run on the same tick.
        assertNotNull("the second table's run must survive the first table's failure",
                      service.lastRunAtMillis(KEYSPACE, goodTable));
        assertNotNull(service.lastStats(KEYSPACE, goodTable));
    }

    @Test
    public void shutdownStopsTheCycleInsteadOfGrindingThroughEveryTag() throws Throwable
    {
        // Regression for what the first shutdown-hook deploy actually did in production. Cancelling
        // the sweep interrupts its thread, but runOnce's per-tag handler catches every RuntimeException
        // and moves on -- by design, so one bad tag cannot wedge a table. During shutdown EVERY
        // remaining tag fails, so the cycle walked the entire backlog at shutdown speed and logged one
        // ERROR per tag: thousands of lines in seconds, burying anything real. The cycle must notice it
        // is stopping and leave, quietly.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long wt = 1;
        for (int t = 0; t < 5; t++)
            for (int r = 0; r < 4; r++)
                insertRow("tag" + t, r * 600_000L, r, wt++);

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);

        TierRunStats stats;
        boolean previous = TieredStorageService.setSweepStoppingForTesting(true);
        try
        {
            stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            TieredStorageService.setSweepStoppingForTesting(previous);
            serviceLogger.detachAppender(appender);
        }

        // Nothing was encoded, and -- the point of the fix -- nothing was logged as a failure.
        assertEquals(0, stats.windowsEncoded);
        assertFalse("shutdown must not report per-tag failures as errors",
                    appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR));
        assertTrue("the cycle should say once that it stopped because tiering is shutting down",
                   appender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("shutting down")));

        // And it left the data alone: every source row is still there for the next startup to encode.
        for (int t = 0; t < 5; t++)
            assertEquals(4, raw("SELECT * FROM %s WHERE tag = ?", "tag" + t).size());
    }

    @Test
    public void incrementalScanCoversTheRingAcrossCyclesInsteadOfTimingOut() throws Throwable
    {
        // The registry made enumeration cheap for tags the write path has seen, but the backlog -- tags
        // whose rows predate tiering and that nothing writes to any more -- is only discoverable by
        // scanning the base table, and on the table this exists for that scan cannot finish. Paging
        // does not bound it: DISTINCT's LIMIT counts only tags that still have a live row, so a page
        // asked for 256 tags walks past however many already-tiered or static-only partitions lie
        // between them. Measured in production: every token range failed, every cycle, forever.
        //
        // So the scan stops trying to finish. Each cycle advances a cursor by a bounded number of
        // pages and registers what it found; the ring is covered over many cycles instead of one.
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long wt = 1;
        for (int t = 0; t < 6; t++)
            insertRow("tag" + t, 0L, t, wt++);

        TieredStorageService service = new TieredStorageService();
        int previousBudget = service.setScanPagesPerCycleForTesting(1);
        int previousPage = TieredStorageService.setTagPageSizeForTesting(2);
        try
        {
            // One page of two tags per cycle: the ring takes three cycles to cover, and each cycle
            // registers strictly more than the last without ever running an unbounded scan.
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(2, registeredTagCount());
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(4, registeredTagCount());
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(6, registeredTagCount());

            // A further cycle finds the ring exhausted, wraps, and does not lose what it knows.
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(6, registeredTagCount());
        }
        finally
        {
            service.setScanPagesPerCycleForTesting(previousBudget);
            TieredStorageService.setTagPageSizeForTesting(previousPage);
        }

        // And every tag the walk discovered was encoded -- the point of discovering them.
        for (int t = 0; t < 6; t++)
            assertEquals("tag" + t + " should have been chunked", 1, execute(chunkSelectQuery(), "tag" + t, new Date(0L)).size());
    }

    /** @return how many tags are in the current table's registry. */
    private int registeredTagCount() throws Throwable
    {
        return execute(String.format("SELECT tag FROM %s.%s WHERE scope = 'tags'",
                                     KEYSPACE, ChunkTables.tagsTableName(currentTable()))).size();
    }

    @Test
    public void tagRegistryIsPopulatedAndThenDrivesEnumeration() throws Throwable
    {
        // The registry exists so the steady-state cycle stops paying for SELECT DISTINCT over the
        // base table (~19ms per partition on the production table this was built for -- ~220s a
        // cycle against a 300s interval). Two things have to hold for that to be safe: the first
        // cycle's authoritative base-table scan must be written into the registry, and a later cycle
        // reading the registry instead must enumerate exactly the same tags -- encoding new closed
        // windows for them just as the scan would have.
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        String[] tags = { "a", "b", "c" };
        long wt = 1;
        for (String tag : tags)
            for (int r = 0; r < 4; r++)
                insertRow(tag, r * 600_000L, r, wt++); // window 0

        TieredStorageService service = new TieredStorageService();
        TierRunStats first = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(3, first.windowsEncoded);
        assertEquals(0, first.tagsSkipped);

        // The scan's result was persisted -- one clustering row per tag, in one partition.
        UntypedResultSet registered = execute(String.format("SELECT tag FROM %s.%s WHERE scope = 'tags'",
                                                            KEYSPACE, ChunkTables.tagsTableName(currentTable())));
        assertEquals(3, registered.size());
        Set<String> registeredTags = new HashSet<>();
        for (UntypedResultSet.Row row : registered)
            registeredTags.add(row.getString("tag"));
        assertEquals(new HashSet<>(Arrays.asList(tags)), registeredTags);

        // Second cycle: the reconcile interval has not elapsed, so enumeration comes from the
        // registry. New closed windows for the same tags must still be found and encoded.
        for (String tag : tags)
            for (int r = 0; r < 4; r++)
                insertRow(tag, HOUR + r * 600_000L, r, wt++); // window 1

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals("the registry-backed cycle must find every tag the scan would have",
                     3, second.windowsEncoded);
        assertEquals(0, second.tagsSkipped);
        for (String tag : tags)
            assertEquals(1, execute(chunkSelectQuery(), tag, new Date(HOUR)).size());
    }

    @Test
    public void sweepSpacesRetriesOfAFailedTableByItsInterval() throws Throwable
    {
        // Regression for a production incident: the sweep gated on the last *completed* run, so a
        // table whose run always throws never recorded a timestamp, was permanently "never run", and
        // was therefore re-attempted on every 60s tick regardless of its own (here: 1h) interval. The
        // failure that provoked it was a read timeout on a full-table DISTINCT scan, so each retry
        // was also the most expensive thing the cycle can do -- a failing table generating the load
        // that kept it failing. Attempts, not completions, are what the interval spaces out.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\",\"interval\":\"1h\"}");

        TieredStorageService service = new TieredStorageService(); // fresh instance: the table is due
        int[] attempts = { 0 };
        service.preRunHookForTesting = (ks, table) ->
        {
            attempts[0]++;
            throw new RuntimeException("injected failure for " + ks + '.' + table);
        };

        service.sweep();
        service.sweep(); // second tick, well inside the 1h interval

        assertEquals("a failed run must still count as an attempt, so the interval spaces the retry",
                     1, attempts[0]);
        // The failure is still not a completion: status keeps reporting "never successfully run".
        assertNull(service.lastRunAtMillis(KEYSPACE, currentTable()));
        assertNull(service.lastStats(KEYSPACE, currentTable()));
    }

    @Test
    public void reentryGuardRejectsConcurrentRun() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        String table = currentTable();
        TieredStorageService service = TieredStorageService.instance;

        assertTrue("expected the gate to be free before this test acquires it",
                   service.acquireGateForTesting(KEYSPACE, table));
        try
        {
            service.retier(KEYSPACE, table);
            fail("expected retier to throw IllegalStateException while the gate is held");
        }
        catch (IllegalStateException expected)
        {
            // expected -- a run for this table is (fictitiously) already in flight
        }
        finally
        {
            service.releaseGateForTesting(KEYSPACE, table);
        }

        // Gate released -- a normal retier now runs to completion instead of throwing.
        service.retier(KEYSPACE, table);
    }

    private void insertRow(String tag, long tsMillis, double value, long writetime) throws Throwable
    {
        execute("INSERT INTO %s (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?",
                tag, new Date(tsMillis), value, writetime);
    }

    /** The current row's {@code name} column, decoded as a double. */
    private static double doubleAt(ColumnarCursor cursor, String name)
    {
        return DoubleType.instance.compose(cursor.getBytes(name));
    }

    /** A v4 payload holding one {@code double} column called {@code value} -- what the re-encoder writes. */
    private static ByteBuffer encodeDoubleChunk(long[] timestamps, double[] values, int count)
    {
        ByteBuffer[] cells = new ByteBuffer[count];
        for (int i = 0; i < count; i++)
            cells[i] = DoubleType.instance.decompose(values[i]);
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", new ChunkV4Codec.ColumnInput(ChunkV4Directory.TYPE_DOUBLE,
                                                          StatOrder.IEEE754_TOTAL, cells));
        return ColumnarChunkCodec.encode(timestamps, count, columns);
    }

    /**
     * Base-table verification reads must see the PHYSICAL rows, not SP3's transparent hot+chunk
     * merge - these tests assert that the re-encoder actually deleted/kept raw rows. Logical
     * (merged) visibility is TransparentReadTest's job.
     */
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
        return KEYSPACE + "." + ChunkTables.chunkTableName(currentTable());
    }

    private String chunkSelectQuery()
    {
        return "SELECT * FROM " + chunkTableRef() + " WHERE tag = ? AND window_start = ?";
    }

    /** Bind order: tag, window_start, samples, max_row_writetime, payload, USING TIMESTAMP. */
    private String chunkInsertQuery()
    {
        return "INSERT INTO " + chunkTableRef() +
               " (tag, window_start, codec, samples, max_row_writetime, payload) VALUES (?, ?, 4, ?, ?, ?) " +
               "USING TIMESTAMP ?";
    }
}
