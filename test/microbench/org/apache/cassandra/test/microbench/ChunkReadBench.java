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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Iterator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.BlockEncodings;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ChunkV4Directory;
import org.apache.cassandra.db.timeseries.tiering.ChunkReadSupport;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Whole-chunk reads through {@code ChunkV4Codec}: the doc/timeseries/simd-decode-design.md §5
 * gate C shape ("chunk read", vector threshold &ge; 1.10x), on a production-shaped chunk encoded
 * once at setup.
 *
 * <p>The chunk is 3,600 rows (one hour at 1 s cadence) by 8 columns counting the timestamp axis:
 * {@code tag}/{@code site} constant text, {@code value} a 2-decimal sensor walk (ALP),
 * {@code value2} a near-constant setpoint, {@code status} mostly-constant small ints,
 * {@code flag} mostly-true boolean, {@code aux} a partially-null double walk, and the axis itself.
 *
 * <p>Scores are microseconds <b>per chunk</b>; rows/s = 3600 / score_us * 1e6. Each invocation
 * re-opens the chunk (header + directory parse), which is what every real tiered read pays;
 * projection is the production path's shape too, and {@code projectedScan} demonstrates that
 * unprojected sections cost nothing (their sections are never parsed).
 *
 * <p>Allocation: the cursor path allocates by design (per-block value arrays, one {@code byte[]}
 * per fixed-width value handed out) -- that is the real read path, so it is measured, not designed
 * away. Nothing else in the harness allocates: the encoded chunk is a setup constant and
 * {@code ChunkV4Codec.open} duplicates it, so its position is never mutated.
 *
 * <p>Run: {@code ant microbench -Dbenchmark.name=ChunkReadBench}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class ChunkReadBench
{
    private static final int ROWS = 3600;
    private static final Set<String> VALUE_AND_TIME = Collections.singleton("value");

    private ByteBuffer chunk;
    private String[] allColumns;
    private TableMetadata metadata;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.clientInitialization();
        metadata = TableMetadata.builder("bench", "chunkread")
                                .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                .addClusteringColumn("ts", TimestampType.instance)
                                .addRegularColumn("tag", UTF8Type.instance)
                                .addRegularColumn("site", UTF8Type.instance)
                                .addRegularColumn("value", DoubleType.instance)
                                .addRegularColumn("value2", DoubleType.instance)
                                .addRegularColumn("status", LongType.instance)
                                .addRegularColumn("flag", BooleanType.instance)
                                .addRegularColumn("aux", DoubleType.instance)
                                .build();
        long[] timestamps = new long[ROWS];
        long base = 1_700_000_000_000L;
        for (int i = 0; i < ROWS; i++)
            timestamps[i] = base + i * 1000L;

        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("tag", text(constantText("compressor-7")));
        columns.put("site", text(constantText("plant-A")));
        columns.put("value", doubles(sensorWalkTwoDecimals(7)));
        columns.put("value2", doubles(nearConstant()));
        columns.put("status", longs(mostlyConstantStatus()));
        columns.put("flag", booleans());
        columns.put("aux", doubles(partiallyNullWalk(11)));

        chunk = ChunkV4Codec.encode(timestamps, ROWS, columns);
        allColumns = ChunkV4Codec.cursor(chunk, null).columns().toArray(new String[0]);
    }

    /** Every column of every row through the cursor -- the unprojected worst case. */
    @Benchmark
    public long fullScan(Blackhole bh)
    {
        ChunkV4Codec.Cursor cursor = ChunkV4Codec.cursor(chunk, null);
        long rows = 0;
        while (cursor.advance())
        {
            bh.consume(cursor.timestamp());
            for (String name : allColumns)
                bh.consume(cursor.getByteArray(name));
            rows++;
        }
        return rows;
    }

    /**
     * The same scan addressed by slot instead of by name -- what the transparent-read path in
     * {@code ChunkReadSupport} does. {@link #fullScan} is the same work with a name lookup per cell, so the two
     * together price {@link ChunkV4Codec.Cursor#columnSlot}: the columns a scan reads are fixed for its whole
     * life, and resolving them once is the difference.
     */
    @Benchmark
    public long fullScanBySlot(Blackhole bh)
    {
        ChunkV4Codec.Cursor cursor = ChunkV4Codec.cursor(chunk, null);
        int[] slots = new int[allColumns.length];
        for (int i = 0; i < allColumns.length; i++)
            slots[i] = cursor.columnSlot(allColumns[i]);
        long rows = 0;
        while (cursor.advance())
        {
            bh.consume(cursor.timestamp());
            for (int slot : slots)
                bh.consume(cursor.getByteArray(slot));
            rows++;
        }
        return rows;
    }

    /** value + timestamp only: the dominant aggregate-query projection; other sections never parse. */
    @Benchmark
    public long projectedScan(Blackhole bh)
    {
        ChunkV4Codec.Cursor cursor = ChunkV4Codec.cursor(chunk, VALUE_AND_TIME);
        long rows = 0;
        while (cursor.advance())
        {
            bh.consume(cursor.timestamp());
            bh.consume(cursor.getByteArray("value"));
            rows++;
        }
        return rows;
    }

    /**
     * The same projected scan carried all the way to assembled {@code Row}s -- what the transparent
     * read path actually hands the query machinery, and what an aggregate over chunked data pays per
     * row before a single aggregate function is called.
     *
     * <p>Against {@link #projectedScan}, which stops at the cursor, the difference is the whole
     * assembly layer: a {@code byte[]} serialized afresh per cell, an {@code ArrayCell} over it, the
     * row's BTree, and the {@code Row} itself. That is the layer a columnar direct-aggregation path
     * would bypass, so this pair is what decides whether such a path is worth building.
     */
    @Benchmark
    public long assembledScan(Blackhole bh)
    {
        Iterator<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, chunk, 1L,
                                                            Long.MIN_VALUE, Long.MAX_VALUE,
                                                            false, VALUE_AND_TIME);
        long n = 0;
        while (rows.hasNext())
        {
            bh.consume(rows.next());
            n++;
        }
        return n;
    }

    /** The axis alone, as {@code time_bucket}/gap-fill planning reads it. */
    @Benchmark
    public long[] toTimestamps()
    {
        return ChunkV4Codec.toTimestamps(chunk);
    }

    // -----------------------------------------------------------------------------------------
    // production-shaped inputs (distribution shapes from DoubleBlockCodecTest), fixed seeds
    // -----------------------------------------------------------------------------------------

    private static ChunkV4Codec.ColumnInput text(ByteBuffer[] values)
    {
        return input(ChunkV4Directory.TYPE_TEXT, values);
    }

    private static ChunkV4Codec.ColumnInput doubles(ByteBuffer[] values)
    {
        return input(ChunkV4Directory.TYPE_DOUBLE, values);
    }

    private static ChunkV4Codec.ColumnInput longs(ByteBuffer[] values)
    {
        return input(ChunkV4Directory.TYPE_INT64, values);
    }

    private static ChunkV4Codec.ColumnInput input(int typeCode, ByteBuffer[] values)
    {
        return new ChunkV4Codec.ColumnInput(typeCode, ChunkV4Codec.canonicalStatOrder(typeCode), values);
    }

    private static ByteBuffer[] constantText(String value)
    {
        ByteBuffer bytes = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = bytes;
        return out;
    }

    private static ByteBuffer[] sensorWalkTwoDecimals(long seed)
    {
        Random random = new Random(seed);
        double[] steps = { -0.01, 0.0, 0.0, 0.01 };
        double value = 10.0 + random.nextDouble() * 80.0;
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
        {
            value = Math.min(100.0, Math.max(0.0, value + steps[random.nextInt(4)]));
            out[i] = doubleBytes(Math.round(value * 100.0) / 100.0);
        }
        return out;
    }

    private static ByteBuffer[] nearConstant()
    {
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = doubleBytes(i % 997 == 0 ? 192.5 : 192.0);
        return out;
    }

    private static ByteBuffer[] mostlyConstantStatus()
    {
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = longBytes(i % 211 == 0 ? 3L : 0L);
        return out;
    }

    private static ChunkV4Codec.ColumnInput booleans()
    {
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = ByteBuffer.wrap(BlockEncodings.toFixedBytes(i % 97 == 0 ? 0L : 1L,
                                                                 ChunkV4Directory.TYPE_BOOLEAN));
        return input(ChunkV4Directory.TYPE_BOOLEAN, out);
    }

    private static ByteBuffer[] partiallyNullWalk(long seed)
    {
        Random random = new Random(seed);
        double[] steps = { -0.01, 0.0, 0.0, 0.01 };
        double value = 10.0 + random.nextDouble() * 80.0;
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
        {
            value = Math.min(100.0, Math.max(0.0, value + steps[random.nextInt(4)]));
            out[i] = i % 7 == 3 ? null : doubleBytes(Math.round(value * 100.0) / 100.0);
        }
        return out;
    }

    private static ByteBuffer doubleBytes(double value)
    {
        return ByteBuffer.wrap(BlockEncodings.toFixedBytes(Double.doubleToRawLongBits(value),
                                                           ChunkV4Directory.TYPE_DOUBLE));
    }

    private static ByteBuffer longBytes(long value)
    {
        return ByteBuffer.wrap(BlockEncodings.toFixedBytes(value, ChunkV4Directory.TYPE_INT64));
    }
}
