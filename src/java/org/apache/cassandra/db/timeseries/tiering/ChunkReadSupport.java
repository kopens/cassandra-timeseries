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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * SP3 transparent reads: decodes a chunk payload into synthetic CQL rows for coordinator-side
 * merging with hot rows (design spec section 3.3.1, plan R3/R4).
 *
 * A chunk carries every regular column of the base table (SP4: {@link ColumnarChunkCodec}, format
 * version 4), so one sample becomes one row with one cell per column that is non-null on that
 * sample. Columns are matched to the table <b>by name</b>: a column the chunk carries but the table
 * has since dropped is ignored, and a column added after the chunk was written simply reads as null.
 *
 * Every synthetic cell carries {@code max_row_writetime + 1} as its writetime. The {@code + 1} is
 * <b>forced, not chosen</b>: the re-encoder deletes the source rows with
 * {@code USING TIMESTAMP maxWt} where {@code maxWt == max_row_writetime}, and Cassandra's
 * {@code DeletionTime.deletes()} is {@code timestamp <= markedForDeleteAt}, so rows reconstructed at
 * {@code max_row_writetime} are shadowed by tiering's own tombstone the moment they are merged
 * against the unfiltered stream ({@link ChunkMergeUnfilteredIterator}). Reconstructing rows that a
 * tombstone removed requires post-dating that tombstone.
 * <p>
 * What that buys, and what it costs:
 * <ul>
 *   <li>A real user tombstone always post-dates the sweep that chunked the window, so it still
 *       shadows the reconstruction -- deletes are honoured. (Writing one is nonetheless refused; see
 *       {@link TieredWrites} for why a delete that only masks the chunk is not good enough.)</li>
 *   <li>An un-swept hot row survives the tiering tombstone only if its writetime is
 *       &gt; {@code maxWt}, i.e. &gt;= {@code maxWt + 1} -- so the old strict guarantee "a hot row
 *       always beats the chunk" weakens to "ties are possible at exactly {@code maxWt + 1}".
 *       Cassandra breaks cell ties by comparing values, so a late correction landing on exactly that
 *       microsecond can lose to the chunk's stale value if its value sorts lower. Reachable only
 *       with client-supplied {@code USING TIMESTAMP}; server-assigned microsecond timestamps make it
 *       vanishingly unlikely, and it is the same class of hazard explicit timestamps already carry
 *       in plain Cassandra.</li>
 * </ul>
 * The writetime is still an approximation for {@code writetime(col)} selections (documented
 * limitation): every column of every reconstructed row reports the same value.
 *
 * <h2>Projection</h2>
 * A read decodes only the columns it queries ({@code projection}), not every column the chunk
 * carries. Cassandra itself always <em>fetches</em> all regular columns
 * ({@code ColumnFilter.fetchedColumns()} is the full set for every CQL SELECT), so pushing the
 * <em>fetched</em> set down would save nothing; the set worth pushing down is
 * {@code queriedColumns()}, and doing so is observationally equivalent for one reason and subject to
 * one precondition:
 * <ul>
 *   <li><b>Why the equivalence holds.</b> The only thing the extra columns buy Cassandra is the
 *       ability to distinguish "row exists, your column is null" from "no row". These rows carry
 *       that distinction themselves: a sample whose decoded columns are all null takes the
 *       primary-key-liveness branch below and stays visible. Everything else a query reads from a
 *       row -- selected columns, {@code WHERE}-restricted columns ({@code nonPKRestrictedColumns}
 *       are added to the queried set), ordering columns -- is by definition inside
 *       {@code queriedColumns()}. Digests and read repair never see these rows: they are computed on
 *       replicas from base-table data, below every hook that merges chunks in.</li>
 *   <li><b>The precondition: cold data is immutable</b> ({@link TieredWrites}). Projection changes
 *       one thing structurally -- a row that today carries a cell for a non-queried column may now
 *       carry primary-key liveness instead. The two differ only if that cell would have been
 *       shadowed, leaving the row empty, which needs a cell tombstone at or above the chunk's
 *       {@code max_row_writetime + 1} on a chunked clustering. That is precisely the write
 *       {@link TieredWrites} refuses (a tombstone written <em>before</em> the window was encoded
 *       cannot be it: the re-encoder would have read the column as null and stored null). Weaken
 *       that guard and this optimisation stops being a no-op.</li>
 * </ul>
 * A projected row therefore carries a <em>subset</em> of the columns its iterator declares
 * ({@code metadata.regularAndStaticColumns()}), which is always legal; the illegal direction --
 * carrying a cell for an undeclared column -- is impossible here.
 *
 * <h2>Laziness, and the capture walk</h2>
 * {@link #rowsFromChunk} hands back an {@link Iterator}, not a list, so a {@code LIMIT} satisfied
 * part-way through a window stops building rows for the rest of it (see {@link ChunkRowSource},
 * which stops pulling). <b>Neither direction builds a {@link Row} it does not emit.</b> Both
 * directions run off one eager <em>capture</em> walk of the cursor: each in-range sample's
 * timestamp and its already-decoded per-column values -- references the cursor hands over, one
 * array slot each -- are recorded, and the {@code Row} itself (clustering, cells, BTree) is
 * assembled only when the iterator reaches that index, counting up or down. A {@code LIMIT n} on a
 * 3,600-sample window therefore assembles {@code n} rows rather than 3,600, and the capture holds
 * strictly less than the list of built rows it replaced.
 * <p>
 * The per-window cost of the capture walk is linear in the window, which is exactly what the v3
 * decoder already cost: it decoded every projected column of the whole payload inside
 * {@code cursor()} before returning. For descending the walk is also unavoidable --
 * {@link ColumnarCursor} is forward-only, so the last row is reachable only by walking to it.
 * <p>
 * <b>The capture walk is what upholds the corrupt-chunk contract under v4.</b> The v4 cursor opens
 * and decodes a column's blocks lazily as {@code advance()} crosses them, so corruption inside a
 * block would otherwise surface mid-iteration -- after rows had been emitted, and past the
 * skip-and-warn that {@link ChunkRowSource} wraps around this <em>call</em> (a mid-iteration
 * {@link IllegalArgumentException} would fail the whole query instead of skipping one chunk).
 * Walking every row of the cursor here decodes every projected block before a single row is
 * emitted, so a corrupt or unreadable chunk still throws out of the call below and a window is
 * all-or-nothing: the caller's skip-and-warn cannot end up having already published a truncated
 * prefix of it. Assembling a row afterwards reads only arrays that decoding already validated.
 */
public final class ChunkReadSupport
{
    private ChunkReadSupport()
    {
    }

    /**
     * @param metadata          the base table's metadata (one timestamp clustering column; any set
     *                          of regular columns)
     * @param payload           the chunk blob ({@link ColumnarChunkCodec}, version 4)
     * @param maxRowWritetime   the chunk row's max_row_writetime (micros); cells are stamped one
     *                          microsecond LATER, see the class javadoc
     * @param startMsInclusive  emit samples with timestamp &gt;= this (epoch ms)
     * @param endMsExclusive    emit samples with timestamp &lt; this (epoch ms)
     * @param descending        emit newest timestamp first. NOT the same as the read's "reversed"
     *                          flag: on a table declared {@code WITH CLUSTERING ORDER BY (ts DESC)}
     *                          an <em>un</em>-reversed read already runs newest-first
     *                          (see {@code emitDescending} in {@link TransparentReads})
     * @param projection        decode only these columns, or {@code null} for all of them. The
     *                          decoder skips the data section of every column outside the set (each
     *                          directory entry carries its own length), so a
     *                          {@code SELECT one_column} pays for one column rather than all of
     *                          them. Safe because a row left with no cells still gets primary-key
     *                          liveness below -- see the class javadoc.
     * @return synthetic rows in the requested order, built on demand (see the class javadoc); every
     *         decode failure has already been raised by the time this returns
     * @throws IllegalArgumentException on a corrupt payload - callers decide whether to
     *         skip-and-warn (read path, plan R4) or propagate (tests, re-encoder)
     * @throws org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException when the payload
     *         names a chunk format this build does not read; callers must NOT swallow that one -
     *         it is systematic, so skipping would silently truncate history on every read
     */
    public static Iterator<Row> rowsFromChunk(TableMetadata metadata,
                                              ByteBuffer payload,
                                              long maxRowWritetime,
                                              long startMsInclusive,
                                              long endMsExclusive,
                                              boolean descending,
                                              Set<String> projection)
    {
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, projection);
        // See the class javadoc: at max_row_writetime itself the re-encoder's own range tombstone
        // (issued at exactly that timestamp) shadows every row rebuilt here. Saturate rather than
        // overflow -- a corrupt chunk could carry Long.MAX_VALUE.
        long cellTimestamp = maxRowWritetime == Long.MAX_VALUE ? Long.MAX_VALUE : maxRowWritetime + 1;

        RowAssembler assembler = new RowAssembler(cursor, metadata, cellTimestamp);

        // BOTH directions capture eagerly, and the eagerness is load-bearing (see the class
        // javadoc): walking every row here decodes every projected block of the v4 payload, so a
        // corrupt chunk throws out of this call -- where ChunkRowSource's skip-and-warn catches it
        // -- rather than mid-iteration, after rows were already emitted. Row assembly stays lazy:
        // the capture holds references, and a Row is built only when the iterator is pulled.
        //
        // Were the cursor's random access exposed through ColumnarCursor (ChunkV4Codec.Cursor
        // already has seekTo), a reverse read could skip the walk -- but only by also giving up
        // this call's throws-before-first-row contract or re-validating some other way.
        Captured captured = assembler.capture(startMsInclusive, endMsExclusive);
        return descending ? new Descending(assembler, captured) : new Ascending(assembler, captured);
    }

    /**
     * How many rows an iterator handed back by {@link #rowsFromChunk} has actually <em>built</em>.
     * Nothing in production reads it: early termination is invisible in the rows themselves -- the
     * same ones come back whether the rest of the window was assembled or not -- so this is the only
     * honest way for a test to hold the laziness above to its word. Same role, one level down, as
     * {@code ChunkRowSource.payloadReads}.
     * <p>
     * The count lives on the assembler, which is single-threaded by construction (one instance per
     * {@code rowsFromChunk} call, wrapping a cursor with exactly one consumer), so it is a plain
     * {@code long} rather than an atomic.
     */
    @VisibleForTesting
    interface RowCounting extends Iterator<Row>
    {
        long rowsAssembled();
    }

    /**
     * Oldest-first: walks the {@link Captured} window forwards, assembling each row as it is asked
     * for -- {@link Descending}'s mirror image, sharing its capture and its laziness.
     */
    private static final class Ascending implements RowCounting
    {
        private final RowAssembler assembler;
        private final Captured captured;
        /** Index of the row to emit next, counting up; {@code count} once the window is exhausted. */
        private int next;

        Ascending(RowAssembler assembler, Captured captured)
        {
            this.assembler = assembler;
            this.captured = captured;
        }

        @Override
        public boolean hasNext()
        {
            return next < captured.count;
        }

        @Override
        public Row next()
        {
            if (next >= captured.count)
                throw new NoSuchElementException();
            return assembler.assemble(captured, next++);
        }

        @Override
        public long rowsAssembled()
        {
            return assembler.assembled;
        }
    }

    /**
     * Newest-first: walks a {@link Captured} window backwards, assembling each row as it is asked
     * for. Emits exactly what building every row and reversing the list did -- same rows, same
     * order, same cells -- for the rows the consumer actually takes.
     */
    private static final class Descending implements RowCounting
    {
        private final RowAssembler assembler;
        private final Captured captured;
        /** Index of the row to emit next, counting down; negative once the window is exhausted. */
        private int next;

        Descending(RowAssembler assembler, Captured captured)
        {
            this.assembler = assembler;
            this.captured = captured;
            this.next = captured.count - 1;
        }

        @Override
        public boolean hasNext()
        {
            return next >= 0;
        }

        @Override
        public Row next()
        {
            if (next < 0)
                throw new NoSuchElementException();
            return assembler.assemble(captured, next--);
        }

        @Override
        public long rowsAssembled()
        {
            return assembler.assembled;
        }
    }

    /**
     * One forward walk of a chunk, reduced to the least that lets any of its rows be rebuilt later:
     * the in-range samples' timestamps, and their column values as the cursor handed them over.
     * <p>
     * {@link #values} is flat -- row {@code r}'s value for column {@code i} is at
     * {@code r * stride + i} -- so capturing a row costs {@code stride} array stores and no
     * allocation, where a built row costs a clustering, a cell per non-null column, a BTree and the
     * row itself. The values are the same objects the cells would have wrapped (for a constant or
     * dictionary-encoded column, the one array the decoder shares across every row), so this holds
     * strictly less than the list of {@code Row}s it replaced.
     */
    private static final class Captured
    {
        private final long[] timestamps;
        private final Object[] values;
        private final int stride;
        private final int count;

        Captured(long[] timestamps, Object[] values, int stride, int count)
        {
            this.timestamps = timestamps;
            this.values = values;
            this.stride = stride;
            this.count = count;
        }
    }

    /**
     * Turns cursor positions into rows. Holds everything that can be resolved once per read rather
     * than once per row: the chunk's columns matched against the table, the order they have to be
     * emitted in, and the scratch the per-row build writes through.
     * <p>
     * Single-threaded by construction -- one instance serves one
     * {@link ChunkReadSupport#rowsFromChunk} call, and the cursor it wraps is forward-only, so the
     * iterator handed back has exactly one consumer.
     */
    private static final class RowAssembler
    {
        /**
         * Nulled once {@link #capture} has walked it: that walk is the cursor's last use in either
         * direction, and dropping it lets a whole window's decoded columns be collected while the
         * iterator built on the capture is still alive.
         */
        private ColumnarCursor cursor;
        /**
         * The same cursor's {@code byte[]} view, or {@code null} if it does not offer one. Optional
         * on purpose: the fast path is a representation choice, not a contract change, so a cursor
         * implementation without it still reads correctly through {@link ColumnarCursor#getBytes}.
         * Released with {@link #cursor}, which is why the branch on it is {@link #byteArrayValues}
         * and not a null check.
         */
        private ColumnarChunkCodec.ArrayValueCursor arrays;
        /** Whether values come from {@link #arrays} ({@code byte[]}) or the cursor ({@link ByteBuffer}). */
        private final boolean byteArrayValues;
        /**
         * Cursor slots for the chunk's columns, and the table columns they resolve to, in BTree (ColumnData)
         * order. Slots rather than names: the cursor's name-keyed accessors binary-search its column directory
         * on every call, which for a scan is a per-cell lookup of a per-scan constant.
         */
        private final int[] slots;
        private final ColumnMetadata[] columns;
        private final long cellTimestamp;
        /**
         * The cells of the row being built. Reused across rows: {@code BTree.build} bulk-COPIES its
         * input into the nodes it allocates, so nothing downstream can observe this array.
         */
        private final Object[] scratch;
        /** Rows built so far; see {@link RowCounting}. Single-threaded, hence not atomic. */
        private long assembled;
        /**
         * False if any resolved column is complex, which forces the general builder. A chunk cannot
         * legally carry one -- {@link TieringPolicy#unsupportedSchemaError} refuses to tier a table
         * with multi-cell columns, and frozen collections are simple -- but a column DROPped and
         * re-ADDed as a collection under the same name would land here, and the general builder is
         * the only path that wraps such a cell in {@code ComplexColumnData} the way it always did.
         */
        private final boolean allSimple;

        RowAssembler(ColumnarCursor cursor, TableMetadata metadata, long cellTimestamp)
        {
            this.cursor = cursor;
            this.arrays = cursor instanceof ColumnarChunkCodec.ArrayValueCursor
                          ? (ColumnarChunkCodec.ArrayValueCursor) cursor
                          : null;
            this.byteArrayValues = this.arrays != null;
            this.cellTimestamp = cellTimestamp;

            // Resolve the chunk's column names against the table once, not per row, and keep them in
            // ColumnMetadata order -- which is the order a row's BTree wants (ColumnData.comparator
            // is just ColumnMetadata.compareTo). The chunk's own directory cannot supply that order:
            // it is sorted by Java String comparison while columns compare by UTF-8 name bytes, and
            // the two differ for non-ASCII names. Sorting here once is what lets the per-row build
            // skip BTreeRow.unsortedBuilder's sort-and-resolve entirely.
            TreeMap<ColumnMetadata, String> byColumn = new TreeMap<>();
            for (String name : cursor.columns())
            {
                ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(name));
                // Dropped since the chunk was written (or, defensively, no longer a regular column):
                // there is nowhere to put its cells, so leave it out rather than failing the read.
                if (column != null && column.isRegular())
                    byColumn.put(column, name);
            }
            this.columns = byColumn.keySet().toArray(new ColumnMetadata[0]);
            this.slots = new int[byColumn.size()];
            int slot = 0;
            for (String name : byColumn.values())
                this.slots[slot++] = cursor.columnSlot(name);
            this.scratch = new Object[this.columns.length];

            boolean simple = true;
            for (ColumnMetadata column : this.columns)
                simple &= column.isSimple();
            this.allSimple = simple;
        }

        /**
         * Walks the whole chunk once, keeping what it takes to rebuild any of its in-range samples
         * later but building none of them. The cursor is exhausted -- and released -- by the time
         * this returns, which is also what forces every projected block of the payload through the
         * decoder before any row is emitted (the corrupt-chunk contract; see the class javadoc).
         */
        Captured capture(long startMsInclusive, long endMsExclusive)
        {
            int stride = columns.length;
            // Grown by doubling rather than sized from the chunk's row count: a query whose range
            // covers three samples of a 200,000-sample window must not pay for 200,000 slots. The
            // ceiling is one reference per (in-range row, column) plus a long per row -- strictly
            // less than the Row/Cell/BTree graph the eager build held for the same window, so no
            // payload can overflow this that did not already exhaust the heap before.
            int capacity = 16;
            long[] timestamps = new long[capacity];
            Object[] values = new Object[capacity * stride];
            int count = 0;

            while (advanceToSample(startMsInclusive, endMsExclusive))
            {
                if (count == capacity)
                {
                    capacity <<= 1;
                    timestamps = Arrays.copyOf(timestamps, capacity);
                    values = Arrays.copyOf(values, capacity * stride);
                }
                timestamps[count] = cursor.timestamp();
                int base = count * stride;
                for (int i = 0; i < stride; i++)
                    values[base + i] = valueOf(i);
                count++;
            }

            cursor = null;
            arrays = null;
            return new Captured(timestamps, values, stride, count);
        }

        /** Builds the {@code index}-th captured sample's row, exactly as an eager build would have. */
        Row assemble(Captured captured, int index)
        {
            return buildRow(clusteringAt(captured.timestamps[index]), captured.values, index * captured.stride);
        }

        /**
         * Advances to the next sample inside {@code [startMsInclusive, endMsExclusive)}, skipping
         * the ones outside it.
         *
         * @return false once the chunk is exhausted
         */
        private boolean advanceToSample(long startMsInclusive, long endMsExclusive)
        {
            while (cursor.advance())
            {
                long ts = cursor.timestamp();
                if (ts >= startMsInclusive && ts < endMsExclusive)
                    return true;
            }
            return false;
        }

        private static Clustering<?> clusteringAt(long ts)
        {
            // fromTimeInMillis is decompose(new Date(ts)) with the Date left out: a timestamp's
            // serialized form IS the 8-byte epoch-millis long, so the bytes are the same ones.
            return Clustering.make(TimestampType.instance.fromTimeInMillis(ts));
        }

        /**
         * @param values raw column values, flat, this row's starting at {@code base}
         *               (see {@link Captured})
         */
        private Row buildRow(Clustering<?> clustering, Object[] values, int base)
        {
            assembled++;
            return allSimple ? buildDirect(clustering, values, base) : buildViaBuilder(clustering, values, base);
        }

        /**
         * Assembles the BTree directly. Legal only because every input is fixed by construction:
         * cells arrive in BTree order (see the constructor), one per column so no two can collide,
         * all simple, and all with the same live {@code (cellTimestamp, NO_TTL, NO_DELETION_TIME)} --
         * which is what makes {@code minDeletionTime} an O(1) constant instead of an accumulate over
         * the tree. A chunk carries values only; it has no way to express a TTL or a tombstone.
         */
        private Row buildDirect(Clustering<?> clustering, Object[] values, int base)
        {
            int n = 0;
            for (int i = 0; i < columns.length; i++)
            {
                Cell<?> cell = cellFor(i, values[base + i]);
                if (cell != null)
                    scratch[n++] = cell;                  // null value: stays null, no cell emitted
            }

            if (n == 0)
            {
                // Every DECODED column null on this sample. The row still EXISTS - the re-encoder
                // chunked it precisely so its existence would not be lost to the range delete - and a
                // row with neither cells nor primary-key liveness is indistinguishable from no row at
                // all, so give it the liveness a bare `INSERT INTO t (key, ts) VALUES (...)` would
                // have had.
                //
                // This is also what makes `projection` safe. Cassandra normally fetches every regular
                // column so it can tell "row exists, queried column is null" from "no row"; a
                // projected read cannot, so it reaches this branch instead and reconstructs the same
                // distinction from liveness. See the class javadoc for why that is equivalent.
                return BTreeRow.noCellLiveRow(clustering, LivenessInfo.create(cellTimestamp));
            }

            Object[] tree;
            if (n == 1)
            {
                tree = BTree.singleton(scratch[0]);
            }
            else
            {
                // The BulkIterator is pooled and must be closed, or its thread-local slot leaks.
                try (BulkIterator<Object> bulk = BulkIterator.of(scratch, 0))
                {
                    tree = BTree.build(bulk, n, UpdateFunction.noOp());
                }
            }
            return BTreeRow.create(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE, tree, Cell.MAX_DELETION_TIME);
        }

        /** The pre-existing path, kept verbatim for the complex-column case {@link #allSimple} guards. */
        private Row buildViaBuilder(Clustering<?> clustering, Object[] values, int base)
        {
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(clustering);
            boolean anyCell = false;
            for (int i = 0; i < columns.length; i++)
            {
                Cell<?> cell = cellFor(i, values[base + i]);
                if (cell == null)
                    continue;
                builder.addCell(cell);
                anyCell = true;
            }
            if (!anyCell)
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(cellTimestamp));
            return builder.build();
        }

        /**
         * The current cursor row's raw value for column {@code i}, or {@code null} when the sample
         * has no value there. Kept in the decoder's own representation until a cell is wanted for
         * it, which is what lets a row be captured without being built.
         */
        private Object valueOf(int i)
        {
            return byteArrayValues ? arrays.getByteArray(slots[i]) : cursor.getBytes(slots[i]);
        }

        /**
         * A cell over {@code value} as returned by {@link #valueOf}, or {@code null} for a value the
         * sample does not have. {@link ArrayCell} over the decoder's {@code byte[]} rather than
         * {@link BufferCell} over a {@link ByteBuffer}: the value is the same bytes either way, and
         * the buffer was pure wrapper overhead on a path that only ever reads the value back out
         * through a {@code ValueAccessor}.
         */
        private Cell<?> cellFor(int i, Object value)
        {
            if (value == null)
                return null;
            return byteArrayValues ? ArrayCell.live(columns[i], cellTimestamp, (byte[]) value, null)
                                   : BufferCell.live(columns[i], cellTimestamp, (ByteBuffer) value);
        }
    }
}
