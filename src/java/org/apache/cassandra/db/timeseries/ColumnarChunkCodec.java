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
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.SortedMap;

/**
 * The columnar chunk codec entry point the tiering tree consumes: many named columns per chunk
 * against one shared timestamp axis, instead of the one-column-per-chunk layout of the retired
 * single-column format (version 2).
 * <p>
 * <b>As of chunk format v4 this class is a routing layer over {@link ChunkV4Codec}</b>, which owns
 * the block-based layout (see doc/timeseries/chunk-format-v4.md). {@link #encode} and
 * {@link #cursor} delegate to it, and {@link #VERSION} is v4's version byte. The class survives as
 * the entry point -- rather than every reader learning a new name -- because its <em>contracts</em>
 * are unchanged and v4 was built to slot in behind them:
 * <ul>
 *   <li>{@link ChunkV4Codec.Cursor} implements both {@link ColumnarCursor} and
 *       {@link ArrayValueCursor}, so {@code ChunkReadSupport}, {@code TieredStorageService} and
 *       {@code ColdWindowChunkFlush} consume it without a shape change;</li>
 *   <li>the header peeks below read offsets 0/1/5/13, which v4 deliberately kept at v3's
 *       positions (v4 §3) precisely so these stay O(1) header lookups;</li>
 *   <li>corruption is {@link IllegalArgumentException} and the read path skips the one chunk; a
 *       version byte naming a real-but-removed format (1, 2, and now 3) is an
 *       {@link UnsupportedChunkFormatException} that must never be swallowed. The classification
 *       lives in {@link ChunkCodecs#unsupportedVersion}, in one place;</li>
 *   <li>encoding is byte-deterministic: identical input encodes to identical bytes on every node,
 *       JVM and JIT tier, which is what lets the re-encoder's {@code chunkUnchanged} compare
 *       payload bytes (v4 §5).</li>
 * </ul>
 * <p>
 * <b>Buffer contract for decoded values</b> (carried over unchanged from v3, restated because every
 * caller depends on it). The {@link ByteBuffer}s a {@link ColumnarCursor} hands back are read-only
 * <em>by contract, not by type</em> ({@code asReadOnlyBuffer()} is deliberately not used -- a
 * read-only heap buffer reports {@code hasArray() == false}, which sends Cassandra's
 * {@code FastByteOperations} down its direct-buffer branch and segfaults the JVM on the first
 * comparison). A caller must treat every returned buffer as immutable, because within one cursor
 * <b>backing arrays are shared</b>: a constant column returns buffers over a single array, and a
 * dictionary-encoded text/opaque column returns one array per distinct value shared by every row
 * that uses it. The same contract governs {@link ArrayValueCursor#getByteArray}, which is the same
 * value without the wrapper.
 * <p>
 * <b>The v3 implementation is gone</b> -- the delta-of-delta timestamp bitstream, the RLE null
 * bitmaps, the 25-byte-header encode/decode -- deleted together with the MSB-first
 * {@code BitWriter}/{@code BitReader} pair, {@code TimestampCodec}, {@code Chimp128Codec} and
 * {@code AlpCodec}'s v3 container framing, once v4 became the only format written and read. No v3
 * chunk was ever written by a production deployment (tiering was never enabled), so no v3 read
 * path was kept: all that remains of v3 is its version byte, which
 * {@link ChunkCodecs#unsupportedVersion} rejects as a removed <em>format</em>
 * ({@link UnsupportedChunkFormatException}), never as one corrupt chunk.
 */
public final class ColumnarChunkCodec
{
    /** The one format written and read: v4 (§9: the byte means "the entire layout is v4"). */
    public static final byte VERSION = ChunkV4Codec.VERSION;

    /**
     * Hard per-chunk row limit. The format itself would take any positive {@code int}, but a chunk
     * is decoded whole (every projected column materialises one row-indexed primitive array), so an
     * unbounded row count is an unbounded allocation on the read path. Callers that assemble chunks
     * from live data (the tiering re-encoder) pre-check against this while still paging, so an
     * over-dense window is reported as a configuration problem instead of failing here.
     */
    public static final int MAX_ROWS = 16_777_216;

    private ColumnarChunkCodec()
    {
    }

    /**
     * Encodes one chunk in the v4 layout: routes to {@link ChunkV4Codec#encode} at v4.0's block
     * size. The returned payload spans exactly one chunk from position 0 to its limit, is
     * heap-backed and big-endian, and is a total function of the input -- identical rows encode to
     * identical bytes on every node and JIT tier, which the re-encoder's {@code chunkUnchanged}
     * byte comparison depends on (v4 §5).
     *
     * @param columns one {@link ChunkV4Codec.ColumnInput} per column. Any {@link SortedMap} is
     *                accepted and its comparator is ignored: the encoder re-sorts into natural
     *                {@code String} order itself (§5 rule 1), so the directory's determinism does
     *                not depend on how the caller built the map.
     */
    public static ByteBuffer encode(long[] timestamps, int count, SortedMap<String, ChunkV4Codec.ColumnInput> columns)
    {
        return ChunkV4Codec.encode(timestamps, count, columns);
    }

    /**
     * A forward-only cursor over the chunk's rows, restricted to {@code projection} ({@code null} =
     * every column): routes to {@link ChunkV4Codec#cursor}. Unprojected columns' sections are never
     * parsed. The returned cursor also implements {@link ArrayValueCursor}, and its buffers carry
     * the immutability contract in the class javadoc.
     * <p>
     * A corrupt payload throws {@link IllegalArgumentException}; a version byte naming a removed
     * format (1, 2, 3) throws {@link UnsupportedChunkFormatException}, which callers must never
     * swallow. Note that v4 opens sections lazily, so corruption <em>inside a column's blocks</em>
     * can also surface from a later {@code advance()} -- a caller that needs the whole window
     * validated up front (the transparent-read path's all-or-nothing skip contract) must walk the
     * cursor before emitting anything, as {@code ChunkReadSupport} does.
     */
    public static ColumnarCursor cursor(ByteBuffer payload, Set<String> projection)
    {
        return ChunkV4Codec.cursor(payload, projection);
    }

    public static int rowCount(ByteBuffer payload)
    {
        try
        {
            ByteBuffer buffer = checkedHeader(payload);
            return buffer.getInt(buffer.position() + 1);
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: truncated header", e);
        }
    }

    public static long firstTimestamp(ByteBuffer payload)
    {
        try
        {
            ByteBuffer buffer = checkedHeader(payload);
            return buffer.getLong(buffer.position() + 5);
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: truncated header", e);
        }
    }

    public static long lastTimestamp(ByteBuffer payload)
    {
        try
        {
            ByteBuffer buffer = checkedHeader(payload);
            return buffer.getLong(buffer.position() + 13);
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: truncated header", e);
        }
    }

    /**
     * Duplicates {@code payload}, pins the duplicate's byte order to big-endian, and verifies the
     * version byte. Bounds/format problems surfacing from here (an {@link IndexOutOfBoundsException}
     * from a too-short buffer, for instance) are caught and rewrapped by each of this method's three
     * callers above, not here -- {@link #rowCount}/{@link #firstTimestamp}/{@link #lastTimestamp}
     * each read further into the header after this call returns, so the header peeks need one
     * wrapping layer around their whole body, not just around this version check.
     * <p>
     * The peeks above did not need re-routing when v4 was wired in: v4 §3 deliberately kept
     * {@code version}/{@code rowCount}/{@code firstTimestamp}/{@code lastTimestamp} at offsets
     * 0/1/5/13, so the same arithmetic reads a v4 header verbatim -- and unlike
     * {@link ChunkV4Header}'s own peeks, going through {@link ChunkCodecs#unsupportedVersion} here
     * keeps the removed-format dispatch (v1/v2/v3 propagate as {@link UnsupportedChunkFormatException},
     * an unknown byte skips as corruption) on these entry points too.
     */
    private static ByteBuffer checkedHeader(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get(buffer.position());
        if (version != VERSION)
            throw ChunkCodecs.unsupportedVersion(version, "columnar chunk");
        return buffer;
    }

    /**
     * Zero-copy escape hatch for readers that build {@code byte[]}-backed cells: the value
     * {@link ColumnarCursor#getBytes} would wrap, handed over bare.
     * <p>
     * Deliberately <b>not</b> folded into {@link ColumnarCursor}. That interface's contract is the
     * {@link ByteBuffer} one documented on this class, and its other callers -- the re-encoders in
     * {@code TieredStorageService} and {@code ColdWindowChunkFlush} -- feed what they read straight
     * back into {@link ColumnarChunkCodec#encode}, which takes {@code ByteBuffer}s; wrapping is not
     * waste for them.
     * A reader that would only unwrap the buffer again (the transparent-read path in
     * {@code ChunkReadSupport}, which assembles {@code ArrayCell}s) tests for this interface instead,
     * so the fast path stays optional and any cursor that does not offer it still works.
     * <p>
     * The array carries the same immutability contract as the buffers, and for a constant or a
     * dictionary-encoded column it <em>is</em> the shared backing array rather than a copy.
     */
    public interface ArrayValueCursor
    {
        /**
         * The current row's serialized value for column {@code name}, or {@code null} if the column
         * is absent (unknown to this cursor, or outside the projection) or null on this row. Same
         * precondition as {@link ColumnarCursor#getBytes}: {@link ColumnarCursor#advance} first.
         */
        byte[] getByteArray(String name);

        /**
         * The current row's serialized value for the column {@link ColumnarCursor#columnSlot} resolved to
         * {@code slot}, or {@code null} if that column is absent or null on this row. Same contract as
         * {@link #getByteArray(String)}, addressed by slot so a scan pays no per-cell name lookup.
         */
        byte[] getByteArray(int slot);
    }
}
