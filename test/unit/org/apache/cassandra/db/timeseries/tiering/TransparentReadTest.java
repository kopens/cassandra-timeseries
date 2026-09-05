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

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SP3 end-to-end: after the re-encoder moves closed windows into the chunk table and range-deletes
 * the base rows, a plain SELECT on the base table must still return every row - hot and cold merged
 * transparently (design spec section 3.3.1).
 *
 * Timestamps follow the TieredStorageServiceTest convention: small epoch-millis values as event
 * times (all far below the real "now", so every closed window is cold), synthetic runOnce clock.
 */
public class TransparentReadTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    private void loadTwoWindowsAndReencode() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        // Window [0,1h): 3 samples; window [1h,2h): 2 samples. Written with explicit writetimes.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 3.0) USING TIMESTAMP 103", new Date(30 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 4.0) USING TIMESTAMP 104", new Date(HOUR + 10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 5.0) USING TIMESTAMP 105", new Date(HOUR + 20 * 60_000L));
        // Hot row (stays as a base row: its window is inside hot_window of the synthetic now=5h).
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 6.0) USING TIMESTAMP 106", new Date(4 * HOUR + 10 * 60_000L));

        TieredStorageService.TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(2L, stats.windowsEncoded);
    }

    @Test
    public void fullRangeSelectSeesAllRowsAfterReencoding() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT ts, value FROM %s WHERE tag = 't1'");
        assertEquals(6, rows.size());
        double[] expected = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
        int i = 0;
        for (UntypedResultSet.Row row : rows)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    @Test
    public void coldOnlyRangeSelect() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                        new Date(0), new Date(HOUR));
        assertEquals(3, rows.size());
    }

    @Test
    public void pointLookupOnColdTimestamp() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts = ?", new Date(20 * 60_000L));
        assertEquals(1, rows.size());
        assertEquals(2.0, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void aggregateSpansHotAndCold() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT count(value) AS c, avg(value) AS a FROM %s WHERE tag = 't1'");
        assertEquals(6L, rows.one().getLong("c"));
        assertEquals(3.5, rows.one().getDouble("a"), 0.0001);
    }

    @Test
    public void limitAndDescOrder() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet desc = execute("SELECT value FROM %s WHERE tag = 't1' ORDER BY ts DESC LIMIT 3");
        assertEquals(3, desc.size());
        double[] expected = { 6.0, 5.0, 4.0 };
        int i = 0;
        for (UntypedResultSet.Row row : desc)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    @Test
    public void lateRowInsertedAfterSweepWinsOverChunk() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // A correction written AFTER the window was encoded: newer writetime than the chunk's
        // max_row_writetime, so the merge must prefer it over the encoded 2.0.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 9.9) USING TIMESTAMP 200", new Date(20 * 60_000L));

        assertEquals(9.9, execute("SELECT value FROM %s WHERE tag = 't1' AND ts = ?", new Date(20 * 60_000L))
                          .one().getDouble("value"), 0.0);
        assertEquals(6, execute("SELECT value FROM %s WHERE tag = 't1'").size());
    }

    @Test
    public void gapFillSpansHotAndCold() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Buckets over [0, 5h): cold windows 0 and 1h come from chunks, 2h/3h are empty (densified
        // with null), 4h is the hot base row. Do not alias the bucket column (gap-fill v1 rule).
        String gf = "time_bucket_gapfill(1h, ts, '1970-01-01 00:00:00+0000', '1970-01-01 05:00:00+0000')";
        assertRows(execute("SELECT " + gf + ", avg(value) FROM %s WHERE tag = 't1' GROUP BY tag, " + gf),
                   row(new Date(0L), 2.0),
                   row(new Date(HOUR), 4.5),
                   row(new Date(2 * HOUR), null),
                   row(new Date(3 * HOUR), null),
                   row(new Date(4 * HOUR), 6.0));
    }

    /** Overwrites the [0,1h) window's chunk payload with {@code hexPayload} (a 0x... CQL blob literal). */
    private void overwriteFirstChunkPayload(String hexPayload) throws Throwable
    {
        execute("UPDATE " + KEYSPACE + ".\"" + ChunkTables.chunkTableName(currentTable()) + "\" " +
                "SET payload = " + hexPayload + " WHERE tag = 't1' AND window_start = ?", new Date(0L));
    }

    private void assertOnlySecondWindowAndHotRowServed() throws Throwable
    {
        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals(3, rows.size());
        double[] expected = { 4.0, 5.0, 6.0 };
        int i = 0;
        for (UntypedResultSet.Row row : rows)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    @Test
    public void corruptChunkSkippedWithRemainingDataServed() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // A first byte naming no chunk format at all -- what a scrambled header looks like.
        overwriteFirstChunkPayload("0xdeadbeef");

        // Window [0,1h) is unreadable and skipped; window [1h,2h) and the hot row still serve.
        assertOnlySecondWindowAndHotRowServed();
    }

    @Test
    public void corruptChunkWithASupportedVersionIsStillSkipped() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Version byte 4 (the format this build writes), but a header claiming 0 rows: corrupt
        // CONTENT, not a format this build cannot read. Availability wins here -- skip the one bad
        // chunk, serve the rest.
        overwriteFirstChunkPayload("0x04" + "0".repeat(78));

        assertOnlySecondWindowAndHotRowServed();
    }

    @Test
    public void chunkFromARemovedCodecFailsTheReadInsteadOfTruncatingIt() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Version byte 2 = the removed single-column (chimp128) format; byte 3 = the columnar v3
        // format that v4 replaced outright; byte 1 was gorilla. Unlike corruption these are
        // systematic (every chunk an older build wrote carries them), so skipping would make this
        // SELECT -- and every future one -- succeed while silently omitting the [0,1h) window's
        // history.
        for (String removedVersionPayload : new String[]{ "0x02" + "0".repeat(40), "0x03" + "0".repeat(40) })
        {
            overwriteFirstChunkPayload(removedVersionPayload);

            try
            {
                execute("SELECT value FROM %s WHERE tag = 't1'");
                fail("expected the read to fail rather than silently drop the unreadable window (payload " +
                     removedVersionPayload.substring(0, 4) + ")");
            }
            catch (UnsupportedChunkFormatException e)
            {
                // must name the table, the window that proved it, and what the operator has to do
                assertTrue(e.getMessage(), e.getMessage().contains(currentTable()));
                assertTrue(e.getMessage(), e.getMessage().contains("older build"));
                assertTrue(e.getMessage(), e.getMessage().contains(ChunkTables.chunkTableName(currentTable())));
                assertTrue(e.getMessage(), e.getMessage().contains("not recoverable"));
            }
        }
    }

    @Test
    public void multiPageMergedReadReturnsEveryRow() throws Throwable
    {
        // SP4: the merge moved inside the read machinery and now runs on each PAGE's own read
        // command, so chunk rows flow through the same limits and counters as hot rows and paging
        // composes with the merge. This used to fail with "spans multiple pages" -- a v1 restriction
        // that only existed because the merge was bolted on after the pager had already counted.
        // Also the only coverage of the COORDINATOR hook (DigestResolver.getData); every other test
        // here goes through the local path.
        requireNetwork();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, ?) USING TIMESTAMP ?",
                    new Date(i * 600_000L), i * 1.0, 100L + i);
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 99.0) USING TIMESTAMP 200", new Date(4 * HOUR));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // 6 rows (5 chunked + 1 hot) at page size 2 = three pages.
        java.util.List<com.datastax.driver.core.Row> rows =
            executeNetWithPaging("SELECT ts, value FROM %s WHERE tag = 't1'", 2).all();
        assertEquals(6, rows.size());
        for (int i = 0; i < 5; i++)
            assertEquals(i * 1.0, rows.get(i).getDouble("value"), 0.0);
        assertEquals(99.0, rows.get(5).getDouble("value"), 0.0);
    }

    @Test
    public void worksWithAnyPartitionKeyColumnName() throws Throwable
    {
        // Regression: the chunk lookup must use the base table's actual pk column name (the docker
        // canonical schema uses tag_id, the tests above use tag - hardcoding either breaks the other).
        createTable("CREATE TABLE %s (tag_id text, ts timestamp, value double, PRIMARY KEY (tag_id, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag_id, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag_id, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * 60_000L));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        assertEquals(2, execute("SELECT value FROM %s WHERE tag_id = 't1'").size());
    }

    @Test
    public void nonTieredTableUnaffected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0)", new Date(1000));
        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals(1, rows.size());

        // The row count above is satisfied even if every read is fully wrapped and issues a chunk
        // query per partition -- it asserts nothing about the fast path. The actual requirement is
        // that a table with no tiering policy is not wrapped AT ALL, i.e. maybeWrap hands back the
        // very iterator it was given.
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        UnfilteredPartitionIterator hot = EmptyIterators.unfilteredPartition(metadata);
        assertSame("a table with no tiering policy must not be wrapped", hot, maybeWrapFullPartitionRead(metadata, hot));
    }

    /**
     * (a) Raising {@code hot_window} -- an ordinary tuning change -- must not hide data that was
     * encoded under the old, shorter one. The rows below are inside the NEW hot window but their base
     * rows were deleted when they were chunked, so a merge gate that asks the current policy takes the
     * hot-only fast path and silently returns nothing.
     */
    @Test
    public void raisingHotWindowStillServesRowsEncodedUnderTheOldOne() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\"}");

        // A closed, hour-aligned window ~3h ago: unambiguously cold under hot_window=1h whenever the
        // test runs, and unambiguously hot under hot_window=24h.
        long now = System.currentTimeMillis();
        long window = now / HOUR * HOUR - 3 * HOUR;
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(window + 10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(window + 20 * 60_000L));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), now).windowsEncoded);
        assertEquals("the rows must live only in the chunk table now",
                     2, execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(window)).size());

        // Months later, an operator widens the hot window. Nothing about the already-encoded data changed.
        setPolicy("{\"hot_window\":\"24h\",\"chunk_window\":\"1h\"}");

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(window));
        assertEquals("raising hot_window must not hide already-encoded history", 2, rows.size());
    }

    /**
     * (b) Removing the {@code timeseries_tiering} extension stops new encoding; it must not make every
     * already-encoded window disappear from every {@code SELECT}. Removing a configuration option is
     * not an instruction to hide data whose only copy is the chunk table.
     */
    @Test
    public void droppingTheExtensionDoesNotHideEncodedHistory() throws Throwable
    {
        loadTwoWindowsAndReencode();

        alterTable("ALTER TABLE %s WITH extensions = {};");

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals("dropping the extension must not hide the chunk table's contents", 6, rows.size());
        double[] expected = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
        int i = 0;
        for (UntypedResultSet.Row row : rows)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    /**
     * (c) Shrinking {@code chunk_window} must not orphan the wider chunks already written. The
     * merge's look-back has to use the widest width any existing chunk was written with; one
     * <em>current</em> window is not far enough back to find a 24h chunk whose {@code window_start}
     * sits 20h before the query range.
     */
    @Test
    public void shrinkingChunkWindowStillFindsWiderChunks() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"48h\",\"chunk_window\":\"24h\"}");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(HOUR));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * HOUR));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 120 * HOUR).windowsEncoded);

        // The one chunk covers [0, 24h). Shrink the window: its window_start is now 20 hours -- far
        // more than one chunk_window -- before the range asked for below.
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                        new Date(20 * HOUR), new Date(21 * HOUR));
        assertEquals("a chunk wider than the current chunk_window must still be found", 1, rows.size());
        assertEquals(2.0, rows.one().getDouble("value"), 0.0);
    }

    /**
     * The fast path still exists and is still free: a query whose range is entirely above everything
     * the chunk table covers must not read the chunk table at all. Proved behaviourally -- the one
     * chunk is rewritten as a format this build refuses to read, so any chunk read would fail the
     * query rather than return.
     */
    @Test
    public void queryAboveChunkCoverageDoesNotReadChunksAtAll() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\"}");

        long now = System.currentTimeMillis();
        long window = now / HOUR * HOUR - 3 * HOUR;
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(window + 10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 7.0) USING TIMESTAMP 103", new Date(now + 1000L));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), now).windowsEncoded);

        // Version byte 2 = a removed codec: reading this chunk throws rather than returning anything.
        execute("UPDATE " + KEYSPACE + ".\"" + ChunkTables.chunkTableName(currentTable()) + "\" " +
                "SET payload = 0x02" + "0".repeat(40) + " WHERE tag = 't1' AND window_start = ?", new Date(window));

        // Above the top of coverage: served from hot rows only, without touching the chunk table.
        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(now));
        assertEquals(1, rows.size());
        assertEquals(7.0, rows.one().getDouble("value"), 0.0);

        // ...and a query that DOES reach into coverage reads the chunk, and fails on it.
        try
        {
            execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(window));
            fail("a query reaching into chunk coverage must read the chunk table");
        }
        catch (UnsupportedChunkFormatException expected)
        {
            // the chunk read happened, which is the point
        }
    }

    /**
     * A chunk read that fails must fail the {@code SELECT}. Degrading to hot rows only turns a
     * routine availability fault -- {@code ReadTimeoutException}, {@code UnavailableException},
     * {@code TombstoneOverwhelmingException}, all normal at QUORUM -- into a <em>successful</em>
     * query that is silently missing every cold row, behind a client warning most drivers never
     * surface.
     */
    @Test
    public void chunkReadFailureFailsTheQueryInsteadOfOmittingColdData() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Break the chunk SELECT itself. A dropped column is the only chunk-read failure a
        // single-node unit test can provoke deterministically; the failures that matter in production
        // (timeout, unavailable replica) arrive at exactly this catch by exactly the same route.
        execute("ALTER TABLE " + KEYSPACE + ".\"" + ChunkTables.chunkTableName(currentTable()) + "\" DROP payload");

        try
        {
            UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
            fail("expected the failed chunk read to fail the query; got " + rows.size() + " hot-only row(s)");
        }
        catch (RuntimeException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("payload"));
        }
    }

    /**
     * The premise the projection push-down rests on, asserted rather than assumed: a CQL SELECT of
     * one column <b>fetches</b> every regular column but <b>queries</b> only that one. Projecting a
     * chunk decode on {@code fetchedColumns()} would therefore decode everything and save nothing;
     * only {@code queriedColumns()} is worth pushing down.
     * <p>
     * The filter here is built exactly the way {@code ColumnFilterFactory.fromColumns} builds one for
     * a real SELECT -- {@code allRegularColumnsBuilder} plus the selected/restricted columns.
     */
    @Test
    public void projectionIsQueriedColumnsBecauseEverySelectFetchesThemAll() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, PRIMARY KEY (tag, ts))");
        TableMetadata metadata = currentTableMetadata();
        ColumnMetadata value = metadata.getColumn(ByteBufferUtil.bytes("value"));

        ColumnFilter oneColumn = ColumnFilter.allRegularColumnsBuilder(metadata, false).add(value).build();
        assertEquals("every CQL SELECT fetches all regular columns -- so pushing fetchedColumns() down is a no-op",
                     2, oneColumn.fetchedColumns().regulars.size());
        assertEquals(Collections.singleton("value"), TransparentReads.chunkProjection(oneColumn));

        // SELECT *: queried == fetched, so there is nothing to project and the codec is handed null.
        assertNull(TransparentReads.chunkProjection(ColumnFilter.all(metadata)));
        assertNull(TransparentReads.chunkProjection(null));
    }

    /**
     * Six closed hourly windows of three samples each, plus one hot row. Enough windows that "how
     * many did this query actually pay for" is a meaningful question.
     */
    private void loadSixWindowsAndReencode() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        long writetime = 100L;
        for (int window = 0; window < 6; window++)
            for (int sample = 0; sample < 3; sample++)
                execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, ?) USING TIMESTAMP ?",
                        new Date(window * HOUR + sample * 10 * 60_000L), window * 10.0 + sample, writetime++);
        // now = 9h, hot_window = 2h: everything below 7h is cold, this one is not.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 99.0) USING TIMESTAMP 200",
                new Date(7 * HOUR + 10 * 60_000L));
        assertEquals(6L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 9 * HOUR).windowsEncoded);
    }

    /** Runs {@code query} and reports how many chunk payloads the read had to fetch to answer it. */
    private long payloadReadsFor(String query, Object... values) throws Throwable
    {
        long before = ChunkRowSource.payloadReads.get();
        execute(query, values);
        return ChunkRowSource.payloadReads.get() - before;
    }

    /**
     * The reason the read path is lazy at all. A query with <b>no clustering restriction</b> names
     * every chunk in the partition -- that is what {@code LIMIT 1000} on the newest rows, or a
     * {@code LIMIT 1} probe, looks like to the chunk table -- and {@code LIMIT} cannot run until the
     * merge's inputs exist. Fetching and decoding every window up front therefore made an unbounded
     * tiered read cost the whole retention period for a handful of rows.
     * <p>
     * Nothing about the <em>results</em> changes when that is fixed, which is exactly why this test
     * has to count instead: without it, a future change quietly restores the eager fetch and only a
     * benchmark notices.
     */
    @Test
    public void unboundedLimitedReadDecodesOnlyTheWindowsItNeeds() throws Throwable
    {
        loadSixWindowsAndReencode();

        // Ascending: the first window alone satisfies LIMIT 2, so the other five are never fetched.
        assertEquals("an unbounded LIMIT 2 must not pay for the whole partition's chunks",
                     1, payloadReadsFor("SELECT value FROM %s WHERE tag = 't1' LIMIT 2"));

        // Descending: emission order is reversed, so it is the NEWEST cold window that gets decoded
        // -- one window either way, and the right one.
        assertEquals(1, payloadReadsFor("SELECT value FROM %s WHERE tag = 't1' ORDER BY ts DESC LIMIT 2"));

        // The control: a query that really does want everything still reads every window, so the
        // count above is early termination and not a lookup that silently stopped finding chunks.
        assertEquals(6, payloadReadsFor("SELECT value FROM %s WHERE tag = 't1'"));
    }

    /** Runs {@code query} and reports how many window rows the read had to list to answer it. */
    private long windowRowsListedFor(String query, Object... values) throws Throwable
    {
        long before = ChunkRowSource.windowRowsListed.get();
        execute(query, values);
        return ChunkRowSource.windowRowsListed.get() - before;
    }

    /**
     * The step before the payload read. Decoding one payload is not enough: the window listing that
     * chooses it used to be materialized whole — every window the tag ever had — and, for a
     * newest-first query, reversed in memory afterwards. On a tag with years of hourly windows that
     * listing alone dominated the query, and like the payload count it is invisible in the results,
     * so only a counter can hold the line.
     * <p>
     * The descending case is the one that needs the ORDER BY pushdown: without it the newest window
     * is the LAST row of the listing, so finding it means reading all of them no matter how lazy the
     * consumption is.
     */
    @Test
    public void unboundedLimitedReadListsOnlyTheWindowsItNeeds() throws Throwable
    {
        loadSixWindowsAndReencode();

        assertEquals("an ascending LIMIT 2 must stop listing after the window that satisfied it",
                     1, windowRowsListedFor("SELECT value FROM %s WHERE tag = 't1' LIMIT 2"));

        assertEquals("a newest-first LIMIT 2 must get its window first from the chunk table, " +
                     "not by listing every window and reversing",
                     1, windowRowsListedFor("SELECT value FROM %s WHERE tag = 't1' ORDER BY ts DESC LIMIT 2"));

        // The control: a query that wants everything still lists every window, so the counts above
        // are early termination and not a listing that silently stopped finding chunks.
        assertEquals(6, windowRowsListedFor("SELECT value FROM %s WHERE tag = 't1'"));
    }

    /**
     * ...and the rows are the same rows. Early termination is only correct if it terminates after
     * emitting exactly what the eager version emitted, in the same order, in both directions.
     */
    @Test
    public void limitedReadsReturnTheSameRowsTheyAlwaysDid() throws Throwable
    {
        loadSixWindowsAndReencode();

        UntypedResultSet asc = execute("SELECT value FROM %s WHERE tag = 't1' LIMIT 2");
        assertEquals(2, asc.size());
        double[] expectedAsc = { 0.0, 1.0 };              // window 0, samples 0 and 1
        int i = 0;
        for (UntypedResultSet.Row row : asc)
            assertEquals(expectedAsc[i++], row.getDouble("value"), 0.0);

        // Newest first: the hot row, then the newest cold sample (window 5, sample 2 = 52.0).
        UntypedResultSet desc = execute("SELECT value FROM %s WHERE tag = 't1' ORDER BY ts DESC LIMIT 2");
        assertEquals(2, desc.size());
        double[] expectedDesc = { 99.0, 52.0 };
        i = 0;
        for (UntypedResultSet.Row row : desc)
            assertEquals(expectedDesc[i++], row.getDouble("value"), 0.0);

        // And an unlimited read is still every row, in order.
        UntypedResultSet all = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals(19, all.size());
        i = 0;
        for (UntypedResultSet.Row row : all)
        {
            double expected = i == 18 ? 99.0 : (i / 3) * 10.0 + (i % 3);
            assertEquals("row " + i, expected, row.getDouble("value"), 0.0);
            i++;
        }
    }

    /**
     * A bounded query must not have become lazier than it is entitled to be: every window its range
     * can reach is read, and the ones beyond the range are not touched at all.
     * <p>
     * "Can reach" is the look-back, not the range: a chunk of width W starting at S holds
     * {@code [S, S+W)}, so the window listing starts at {@code startMs - widestChunkWindow} and the
     * range {@code [1h, 3h)} lists windows 0, 1 and 2. Window 0 contributes nothing (all of its rows
     * are below 1h) but it has to be decoded to establish that -- the chunk table records where a
     * window starts, not how wide it is. That is the bound the read path has always used; it is
     * asserted here so that "3" stays a deliberate cost rather than a number nobody chose.
     */
    @Test
    public void boundedReadStillReadsExactlyTheWindowsItsRangeCanReach() throws Throwable
    {
        loadSixWindowsAndReencode();

        assertEquals("a range over two windows reads those two plus the one the look-back admits",
                     3, payloadReadsFor("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                        new Date(HOUR), new Date(3 * HOUR)));
        assertEquals(6, execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                new Date(HOUR), new Date(3 * HOUR)).size());
    }

    /**
     * The strongest form of "laziness must be invisible": every query shape the lazy path changes the
     * work of -- both directions, limited and not, sliced and not -- against a <b>non-tiered table
     * holding the same data</b>, compared cell-by-cell on the raw bytes. The hard-coded expectations
     * elsewhere in this class pin specific rows; this pins the whole contract to the one oracle that
     * cannot drift out of sync with Cassandra's own semantics.
     */
    @Test
    public void lazyReadsMatchANonTieredControlTable() throws Throwable
    {
        String control = KEYSPACE + "." + createTable(
            "CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        loadSixWindowsAndReencode();                      // creates the tiered table; %s targets it

        // The exact inserts loadSixWindowsAndReencode made, replayed against the control table.
        long writetime = 100L;
        for (int window = 0; window < 6; window++)
            for (int sample = 0; sample < 3; sample++)
                execute("INSERT INTO " + control + " (tag, ts, value) VALUES ('t1', ?, ?) USING TIMESTAMP ?",
                        new Date(window * HOUR + sample * 10 * 60_000L), window * 10.0 + sample, writetime++);
        execute("INSERT INTO " + control + " (tag, ts, value) VALUES ('t1', ?, 99.0) USING TIMESTAMP 200",
                new Date(7 * HOUR + 10 * 60_000L));

        // Timestamps inline as epoch-millis literals so one string serves both tables: the slice
        // [1h, 4h) spans three chunked windows and starts mid-coverage, so its LIMIT terminates
        // mid-window on the tiered side.
        String[] shapes = {
            " WHERE tag = 't1' ORDER BY ts DESC LIMIT 5",                          // DESC LIMIT n
            " WHERE tag = 't1' LIMIT 5",                                           // ASC LIMIT n
            " WHERE tag = 't1'",                                                   // full scan
            " WHERE tag = 't1' AND ts >= 3600000 AND ts < 14400000 LIMIT 4",       // slice + LIMIT
            " WHERE tag = 't1' AND ts >= 3600000 AND ts < 14400000 ORDER BY ts DESC LIMIT 4",
        };
        for (String shape : shapes)
            assertIdenticalRows(shape,
                                execute("SELECT ts, value FROM " + control + shape),
                                execute("SELECT ts, value FROM %s" + shape));
    }

    /** Byte-identical, row for row: same count, same order, same serialized cells. */
    private static void assertIdenticalRows(String label, UntypedResultSet expected, UntypedResultSet actual)
    {
        Iterator<UntypedResultSet.Row> control = expected.iterator();
        Iterator<UntypedResultSet.Row> tiered = actual.iterator();
        int i = 0;
        while (control.hasNext() && tiered.hasNext())
        {
            UntypedResultSet.Row expectedRow = control.next();
            UntypedResultSet.Row actualRow = tiered.next();
            assertEquals(label + ": ts of row " + i, expectedRow.getBytes("ts"), actualRow.getBytes("ts"));
            assertEquals(label + ": value of row " + i, expectedRow.getBytes("value"), actualRow.getBytes("value"));
            i++;
        }
        assertFalse(label + ": tiered read returned fewer rows than the control table (" + i + " matched)",
                    control.hasNext());
        assertFalse(label + ": tiered read returned more rows than the control table (" + i + " matched)",
                    tiered.hasNext());
    }

    /**
     * Slices compose with the limit's early termination: the window listing is already bounded by the
     * slice (windows outside {@code [startMs - lookback, endMsExcl)} are never even listed, see
     * {@code boundedReadStillReadsExactlyTheWindowsItsRangeCanReach}), and within that bound a
     * {@code LIMIT} stops the walk. The slice [1h, 4h) admits windows 0-3 (window 0 via the
     * look-back); ascending must decode window 0 just to learn all its rows sit below 1h, so the
     * counts differ by direction -- and both are smaller than the 4 a full slice scan pays.
     */
    @Test
    public void sliceWithLimitDecodesOnlyTheWindowsItNeeds() throws Throwable
    {
        loadSixWindowsAndReencode();

        assertEquals("DESC: the newest in-slice window satisfies LIMIT 1 by itself",
                     1, payloadReadsFor("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ? " +
                                        "ORDER BY ts DESC LIMIT 1", new Date(HOUR), new Date(4 * HOUR)));
        assertEquals("ASC: the look-back window decodes empty, then the first in-slice window serves",
                     2, payloadReadsFor("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ? LIMIT 1",
                                        new Date(HOUR), new Date(4 * HOUR)));
        assertEquals("the un-limited slice still reads every window its range can reach",
                     4, payloadReadsFor("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                        new Date(HOUR), new Date(4 * HOUR)));
    }

    /**
     * The benchmark's worst case (tiering-benchmark.md, "시간 범위를 걸지 않은 질의"): a static-only
     * probe -- {@code SELECT static_col} with no clustering restriction -- used to scan every chunk in
     * the partition for an answer that lives in the base table's static row. Statics are never
     * chunked, but the probe still returns one result row per <em>clustering</em> row, so the merge
     * must produce regular rows -- with {@code LIMIT 1} the first window's first row satisfies it and
     * laziness prunes the other five. Without a {@code LIMIT} it is still a full scan (18 result
     * rows), asserted as the control so the 1 above is early termination, not a skipped merge.
     */
    @Test
    public void staticOnlyProbeDecodesAtMostOneWindow() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, meta text static, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag, meta) VALUES ('t1', 'pump-4') USING TIMESTAMP 99");
        long writetime = 100L;
        for (int window = 0; window < 6; window++)
            for (int sample = 0; sample < 3; sample++)
                execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, ?) USING TIMESTAMP ?",
                        new Date(window * HOUR + sample * 10 * 60_000L), window * 10.0 + sample, writetime++);
        assertEquals(6L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 9 * HOUR).windowsEncoded);

        long before = ChunkRowSource.payloadReads.get();
        UntypedResultSet probe = execute("SELECT meta FROM %s WHERE tag = 't1' LIMIT 1");
        long windowsDecoded = ChunkRowSource.payloadReads.get() - before;
        assertEquals(1, probe.size());
        assertEquals("pump-4", probe.one().getString("meta"));
        assertEquals("a static-only LIMIT 1 probe must not pay for the partition's chunks", 1, windowsDecoded);

        // The control: without a LIMIT the same probe is a full scan -- one result row per clustering
        // row -- so the merge demonstrably runs on this query shape and the 1 above is termination.
        assertEquals(6, payloadReadsFor("SELECT meta FROM %s WHERE tag = 't1'"));
        assertEquals(18, execute("SELECT meta FROM %s WHERE tag = 't1'").size());
    }

    /**
     * @return the chunk-table statements {@code ks.chunkTable} currently has in {@link QueryProcessor}'s
     *         internal-statement cache, keyed by their CQL text. Reading the cache rather than a
     *         counter of our own is the point: it is upstream's cache, upstream's key, and upstream's
     *         schema-change invalidation, so what this asserts is that the chunk statements really are
     *         being shared -- not that {@code ChunkRowSource} says they are.
     */
    private static Map<String, CQLStatement> cachedChunkStatements(String chunkTable)
    {
        Map<String, CQLStatement> statements = new HashMap<>();
        for (Map.Entry<String, QueryHandler.Prepared> entry : QueryProcessor.getInternalStatements().entrySet())
        {
            CQLStatement statement = entry.getValue().statement;
            if (statement instanceof SelectStatement
                && KEYSPACE.equals(((SelectStatement) statement).keyspace())
                && chunkTable.equals(((SelectStatement) statement).table()))
                statements.put(entry.getKey(), statement);
        }
        return statements;
    }

    /**
     * A tiered read runs two CQL statements per window-bearing partition -- the window listing and one
     * payload fetch per window decoded -- and both used to be handed to {@code QueryProcessor} as
     * <b>strings</b>: {@code QueryProcessor.process(String, ConsistencyLevel, List)} calls
     * {@code parse()} on its way in, and {@code TieredStorageService.pagedSelectLazy} calls it once per
     * page. So every payload fetch re-ran ANTLR over the query text and re-prepared the statement --
     * preparation cost {@code O(windows decoded)} per query, on the consistency-level path that every
     * user SELECT takes, for two statements whose text never changes.
     * <p>
     * This has to be measured on the <b>network</b> path. The {@code cl == null} local path executes
     * through {@code executeInternal}, which has always resolved through the internal-statement cache,
     * so every other counter test in this class was already looking at the fixed half.
     * <p>
     * Nothing about the results changes when this is fixed -- the same rows come back -- which is why
     * counting is the only way to hold it.
     */
    @Test
    public void chunkStatementsArePreparedOncePerQueryNotOncePerWindow() throws Throwable
    {
        requireNetwork();
        loadSixWindowsAndReencode();
        String chunkTable = ChunkTables.chunkTableName(currentTable());

        // From here on, every chunk statement in the internal cache was put there by the read below.
        QueryProcessor.clearInternalStatementsCache();

        long preparationsBefore = ChunkRowSource.statementPreparations.get();
        long payloadsBefore = ChunkRowSource.payloadReads.get();
        assertEquals(19, executeNet("SELECT value FROM %s WHERE tag = 't1'").all().size());
        long payloads = ChunkRowSource.payloadReads.get() - payloadsBefore;
        long preparations = ChunkRowSource.statementPreparations.get() - preparationsBefore;

        // The shape this test needs: a full scan of one partition, decoding all six windows. Before
        // the fix that was seven statement preparations (one listing + six payloads); the count must
        // now depend on how many DISTINCT statements the query has, not on how many windows it reads.
        assertEquals("the read must decode every window, or it is not the shape this test needs", 6, payloads);
        assertEquals("one window statement and one payload statement for the whole query, "
                     + "not one per window", 2, preparations);

        // ...and the preparations above parsed nothing that was not new: both statements are in the
        // process-wide internal-statement cache. This is the assertion that fails outright on the old
        // code, which parsed through QueryProcessor.instance.parse and cached nothing at all.
        Map<String, CQLStatement> cached = cachedChunkStatements(chunkTable);
        assertEquals("a CL-path tiered read must leave its two chunk statements in the internal-statement "
                     + "cache: " + cached.keySet(), 2, cached.size());

        // The second query proves the cache is actually load-bearing: same statement OBJECTS, so the
        // ANTLR parse happened once for this process, not once per query and certainly not per window.
        preparationsBefore = ChunkRowSource.statementPreparations.get();
        assertEquals(19, executeNet("SELECT value FROM %s WHERE tag = 't1'").all().size());
        assertEquals(2, ChunkRowSource.statementPreparations.get() - preparationsBefore);
        Map<String, CQLStatement> reused = cachedChunkStatements(chunkTable);
        assertEquals(cached.keySet(), reused.keySet());
        for (Map.Entry<String, CQLStatement> entry : cached.entrySet())
            assertSame("a second identical query must reuse the prepared statement, not re-parse it: "
                       + entry.getKey(), entry.getValue(), reused.get(entry.getKey()));
    }

    /**
     * The same invariant across partitions, which is where "prepared once" could most easily have been
     * faked by a per-source field alone. The coordinator resolves one partition at a time, so an
     * {@code IN} over two tags builds two {@link ChunkRowSource}s -- but they share the process-wide
     * cache, so two partitions x six windows is four statement resolutions and <b>zero</b> parses
     * beyond the first query's, where it used to be fourteen parses.
     */
    @Test
    public void chunkStatementsAreSharedAcrossPartitionsAndQueries() throws Throwable
    {
        requireNetwork();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        long writetime = 100L;
        for (String tag : new String[]{ "t1", "t2" })
        {
            for (int window = 0; window < 6; window++)
                for (int sample = 0; sample < 3; sample++)
                    execute("INSERT INTO %s (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?",
                            tag, new Date(window * HOUR + sample * 10 * 60_000L),
                            window * 10.0 + sample, writetime++);
            // One hot row per tag, so both partitions exist on the hot side too -- the same shape
            // loadSixWindowsAndReencode builds, and not the untested "partition is cold-only" one.
            execute("INSERT INTO %s (tag, ts, value) VALUES (?, ?, 99.0) USING TIMESTAMP 200",
                    tag, new Date(7 * HOUR + 10 * 60_000L));
        }
        assertEquals(12L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 9 * HOUR).windowsEncoded);
        String chunkTable = ChunkTables.chunkTableName(currentTable());

        QueryProcessor.clearInternalStatementsCache();

        long preparationsBefore = ChunkRowSource.statementPreparations.get();
        long payloadsBefore = ChunkRowSource.payloadReads.get();
        assertEquals(38, executeNet("SELECT value FROM %s WHERE tag IN ('t1', 't2')").all().size());
        assertEquals("both partitions' windows must be decoded, or this is not a two-partition read",
                     12, ChunkRowSource.payloadReads.get() - payloadsBefore);
        // A bound rather than an equality, because how many sources the read builds is the read
        // path's business: the coordinator resolves a partition at a time (two sources, two
        // statements each), the local path resolves the group at once (one source). Either is at
        // most four. What must never come back is a count that tracks the twelve windows -- the old
        // code prepared fourteen statements here.
        long preparations = ChunkRowSource.statementPreparations.get() - preparationsBefore;
        assertTrue("statement preparations must scale with partitions read, not with the 12 windows "
                   + "decoded, but was " + preparations, preparations <= 4);
        // Two distinct statement texts for the whole thing: the tag is a bind value, not part of the CQL.
        Map<String, CQLStatement> cached = cachedChunkStatements(chunkTable);
        assertEquals("the partition key is bound, so both partitions share one pair of statements: "
                     + cached.keySet(), 2, cached.size());

        // A newest-first LIMIT is the shape the benchmark measures, and the one the lazy window
        // listing exists for: one window listed, one payload read -- and no new statement parsed.
        long windowsBefore = ChunkRowSource.windowRowsListed.get();
        payloadsBefore = ChunkRowSource.payloadReads.get();
        assertEquals(2, executeNet("SELECT value FROM %s WHERE tag = 't1' ORDER BY ts DESC LIMIT 2").all().size());
        assertEquals("the lazy newest-first listing must still stop at one window",
                     1, ChunkRowSource.windowRowsListed.get() - windowsBefore);
        assertEquals(1, ChunkRowSource.payloadReads.get() - payloadsBefore);
        // The newest-first listing is a third statement text (it carries ORDER BY window_start DESC);
        // the payload statement and the ascending listing are the ones already prepared, reused.
        Map<String, CQLStatement> afterDescending = cachedChunkStatements(chunkTable);
        assertEquals("a newest-first read adds its own listing text and nothing else: "
                     + afterDescending.keySet(), 3, afterDescending.size());
        for (Map.Entry<String, CQLStatement> entry : cached.entrySet())
            assertSame("the descending listing has its own text, but everything else is reused: "
                       + entry.getKey(), entry.getValue(), afterDescending.get(entry.getKey()));
    }

    /**
     * The cache must not outlive the table it was prepared against. This is the reason
     * {@link ChunkRowSource} reuses {@code QueryProcessor.prepareInternal} instead of keeping a cache
     * of its own: dropping the chunk table has to evict the statements, or a chunk table that is
     * dropped and recreated would be read through a statement prepared against the old one. The
     * eviction is upstream's {@code StatementInvalidatingListener}; this asserts that the chunk
     * statements are in fact subject to it.
     */
    @Test
    public void droppingTheChunkTableEvictsItsPreparedStatements() throws Throwable
    {
        requireNetwork();
        loadSixWindowsAndReencode();
        String chunkTable = ChunkTables.chunkTableName(currentTable());

        QueryProcessor.clearInternalStatementsCache();
        assertEquals(19, executeNet("SELECT value FROM %s WHERE tag = 't1'").all().size());
        assertEquals(2, cachedChunkStatements(chunkTable).size());

        // Dropping the chunk table is refused while the policy is attached -- it holds the only copy
        // of the tiered rows (see TieringSchemaSupportTest#dropOfTheChunkTableIsRefusedWhileThePolicyIsAttached).
        // Detaching is the step the refusal prescribes; it must not itself evict the chunk statements,
        // or the assertion below would pass without the drop having done anything.
        alterTable("ALTER TABLE %s WITH extensions = {};");
        assertEquals("detaching the policy is not what evicts the chunk statements",
                     2, cachedChunkStatements(chunkTable).size());

        schemaChange("DROP TABLE " + KEYSPACE + '.' + chunkTable);
        assertTrue("statements prepared against a dropped chunk table must not survive it: "
                   + cachedChunkStatements(chunkTable).keySet(),
                   cachedChunkStatements(chunkTable).isEmpty());
    }

    /** @return what {@link TransparentReads#maybeWrap} does to {@code hot} for a full-partition read of 't1'. */
    private static UnfilteredPartitionIterator maybeWrapFullPartitionRead(TableMetadata metadata,
                                                                          UnfilteredPartitionIterator hot)
    {
        DecoratedKey key = metadata.partitioner.decorateKey(UTF8Type.instance.decompose("t1"));
        SinglePartitionReadCommand command =
            SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);
        return TransparentReads.maybeWrap(metadata, command, hot, null);
    }
}
