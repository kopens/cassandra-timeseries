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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Chunk format v4: the whole-chunk encoder and reader that assembles the layers below it into one
 * payload, and takes it apart again.
 *
 * <p>Nothing here invents a layout. {@link ChunkV4Header} owns the forty-byte header and the §2 block
 * arithmetic, {@link ChunkV4Directory} owns the per-column entry and its cross-field rules,
 * {@link ChunkV4Section} owns a section's preamble/table/bodies, {@link ChunkV4BlockTable} owns the
 * block entry, {@link BlockPresence} the four presence modes, {@link BlockEncodings} the per-block
 * encodings and their argmin, {@link StatOrder} the meaning of a statistic. This class decides three
 * things those layers cannot: which columns exist and in what order, where each section lands, and
 * what a column's chunk-level flags and statistics are.
 *
 * <h2>The assembled layout (§4.5)</h2>
 *
 * <pre>
 *   0                       header, 40 bytes (§3)
 *   40                      directory, {@code directoryLen} bytes, padded to 8 (§4)
 *   tsSectionOffset         timestamp section, {@code tsSectionLen} bytes
 *   ...                     one section per column, in directory order, zero-byte columns skipped
 * </pre>
 *
 * <p>The timestamp axis is <b>structurally a column</b>: {@code INT64}, {@code SIGNED_INT},
 * {@code ALL_PRESENT}, built by the same {@link ChunkV4Section#encodeFixed} call as any other
 * eight-byte column and read by the same {@link ChunkV4Section#read}. §0 retired v3's
 * delta-of-delta bitstream precisely so that there would be no timestamp-specific encoding left, and
 * the only thing that distinguishes the axis here is <em>where it is located</em> -- by the header's
 * {@code tsSectionOffset}/{@code tsSectionLen} rather than by a directory entry, because it has no
 * name to sort among the columns and every reader needs it. A consequence worth stating: the axis
 * has no directory entry and therefore no chunk-level {@code min}/{@code max}; the header's
 * {@code firstTimestamp}/{@code lastTimestamp} are those extrema, which is what makes the chunk's
 * time bounds an O(1) peek that touches no section (§3).
 *
 * <p>The timestamp section is placed immediately after the directory. §3 does not fix its position
 * -- every section carries its own offset so the layout need not be an implied prefix sum -- but
 * fixing it here means the one section every read needs is the one adjacent to the metadata every
 * read has already touched.
 *
 * <h2>Byte determinism (§5)</h2>
 *
 * <p>Column order is natural {@code String} order, obtained by {@code new TreeMap<>()} followed by
 * {@code putAll}. <b>Not {@code new TreeMap<>(columns)}</b>: per {@code TreeMap(SortedMap)} that
 * constructor <em>adopts the source map's comparator</em>, so a caller who built a map with any other
 * ordering would get a plausible, stable, wrong order and a chunk no other replica reproduces byte
 * for byte. §5 rule 1 names this trap because v3 fell into it. Everything else determinism needs is
 * enforced a layer down -- exact-size argmins, canonical varints, zero-filled and verified padding --
 * and this class adds no choice of its own that is not a total function of the input.
 *
 * <p>Two properties follow and are pinned by tests: encoding the same input twice is byte-identical,
 * and {@code encode(decode(encode(x)))} equals {@code encode(x)}. The second is not a nicety, it is
 * the tiering re-encoder's late-merge path: a chunk that decoded and re-encoded to different bytes
 * would be reported changed on every cycle and rewritten forever.
 *
 * <h2>The decoded-value contract</h2>
 *
 * <p>Every {@link ByteBuffer} a cursor hands back satisfies {@code hasArray() == true}, and
 * {@code asReadOnlyBuffer()} is <b>forbidden</b> on this path: a read-only heap buffer answers
 * {@code false} to both {@code hasArray()} and {@code isDirect()}, so Cassandra's
 * {@code FastByteOperations} takes its direct-buffer branch and dereferences a zero {@code address}
 * -- a JVM-level SIGSEGV on the first cell comparison, which ordinary reconciliation performs. The
 * buffers are therefore read-only <em>by contract, not by type</em>, exactly as in v3, and callers
 * must treat them as immutable: within one cursor a {@code TEXT}/{@code OPAQUE} value may be the
 * dictionary's own array, shared by every row that cites it, and a zero-byte {@code CONSTANT}
 * column's value is the directory's single array.
 *
 * <h2>Corruption versus unsupported format (§11)</h2>
 *
 * <p>A bad length, a non-monotonic offset, a reserved flag bit, an unknown inner code, a non-canonical
 * varint, a non-zero pad byte, a {@code directoryLen} disagreeing with what was parsed: all
 * {@link IllegalArgumentException}, and the read path skips that one chunk. A version byte naming a
 * real-but-removed format is an {@link UnsupportedChunkFormatException} that must never be swallowed,
 * and the decision of which byte is which is made in one place -- {@link ChunkCodecs#unsupportedVersion}
 * -- rather than duplicated here. An unknown code <em>inside</em> a v4 payload is corruption and not a
 * future format, because a future format would carry a different version byte (§9).
 *
 * <p>Every allocation is bounded before it is made. The per-block arrays are sized from
 * {@code min(blockSize, rowCount)} and allocated only for a column a caller actually reads; the
 * decode scratch is shared by every column of one cursor; and {@link #open} rejects a
 * {@code rowCount} whose §2 block count could not fit a block table in the timestamp section the
 * header declares, which transitively bounds {@code rowCount} against the payload's own size. A
 * corrupt length throws; it cannot reserve memory.
 */
public final class ChunkV4Codec
{
    /** §9: the version byte means "the entire layout below is v4". Not a feature flag, not a minor. */
    public static final byte VERSION = ChunkV4Header.VERSION;

    /**
     * The ALP seam, as a build-time constant. §12 names a codec that some replicas have and others do
     * not as the format's largest risk, so there is no field, no parameter and no property that
     * removes it -- a node either runs a build with {@code 0x20}/{@code 0x21} or runs a different
     * version.
     */
    private static final BlockEncodings.DoubleBlockCodec ALP = AlpBlockCodec.INSTANCE;

    private ChunkV4Codec()
    {
    }

    // -----------------------------------------------------------------------------------------
    // encoder input
    // -----------------------------------------------------------------------------------------

    /**
     * One column's input to {@link #encode}: its §4 type code, the order its statistics are to be
     * computed under, and its values in row order ({@code null} = absent).
     *
     * <p><b>The type code selects an encoding family, never a meaning</b> (§4), and it is
     * authoritative only as a starting point: a present value that the code's fixed width cannot
     * round-trip downgrades this column, <em>in this chunk only</em>, to {@code OPAQUE} (§12). A
     * zero-length value in a fixed-width column is the canonical trigger -- {@code blobAsInt(0x)} is
     * legal CQL and {@code Int32Serializer.validate} accepts it -- and is not an error here.
     *
     * <p><b>{@code statOrder} must be {@link StatOrder#NONE} or the type code's canonical order</b>
     * ({@link #canonicalStatOrder}). That is stricter than §4's "the reader must match on the
     * declared order", and deliberately so: a column's <em>block</em> extrema are computed by
     * {@link ChunkV4Section} under {@link BlockEncodings#compareValues}, which is a function of the
     * type code alone, so a column declaring a different chunk-level order would carry two families
     * of statistics under two orders with only one of them declared, and a reader that matched on the
     * declared one would prune blocks with the wrong comparator. §12 ranks that class of mistake --
     * extrema that no longer describe the values -- fourth on its risk list. A caller whose Cassandra
     * type has an order the type code cannot express (a {@code time} column is the live example: it
     * is eight bytes, so {@code INT64}, but {@link org.apache.cassandra.db.marshal.TimeType} compares
     * <em>unsigned</em>) must declare {@code NONE} and forgo pruning; {@link #statOrderFor} does that
     * reduction for it.
     */
    public static final class ColumnInput
    {
        public final int typeCode;
        public final StatOrder statOrder;
        public final ByteBuffer[] values;

        public ColumnInput(int typeCode, StatOrder statOrder, ByteBuffer[] values)
        {
            ChunkV4Directory.statWidth(typeCode);       // rejects 0x00 and every unallocated code
            if (statOrder == null)
                throw new IllegalArgumentException("null statOrder; declare StatOrder.NONE to carry no statistics");
            if (values == null)
                throw new IllegalArgumentException("null values array");
            StatOrder canonical = canonicalStatOrder(typeCode);
            if (statOrder != StatOrder.NONE && statOrder != canonical)
                throw new IllegalArgumentException("type code " + typeCode + " carries block extrema under " +
                                                   canonical + ", so a column-level " + statOrder +
                                                   " would declare an order the block statistics are not in; " +
                                                   "declare " + canonical + " or NONE (v4 §4)");
            this.typeCode = typeCode;
            this.statOrder = statOrder;
            this.values = values;
        }
    }

    /**
     * The order a column of this type code is <em>already</em> sorted under by
     * {@link BlockEncodings#compareValues}, which is what its block extrema are computed with. The
     * two must agree or a block statistic is not a bound; {@code BOOLEAN} and {@code OPAQUE} have no
     * usable order at all ({@code BooleanType} normalises any non-zero byte to true; §4 forces
     * {@code NONE} on {@code OPAQUE} because a decoder cannot tell a downgraded column from a native
     * blob).
     */
    public static StatOrder canonicalStatOrder(int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_DOUBLE:
                return StatOrder.IEEE754_TOTAL;
            case ChunkV4Directory.TYPE_INT32:
            case ChunkV4Directory.TYPE_INT64:
                return StatOrder.SIGNED_INT;
            case ChunkV4Directory.TYPE_DATE32:
                return StatOrder.UNSIGNED_INT;
            case ChunkV4Directory.TYPE_TEXT:
                return StatOrder.BYTES_UNSIGNED;
            case ChunkV4Directory.TYPE_BOOLEAN:
            case ChunkV4Directory.TYPE_OPAQUE:
                return StatOrder.NONE;
            default:
                ChunkV4Directory.statWidth(typeCode);   // throws with the right message
                throw new AssertionError();
        }
    }

    /**
     * The order a column of Cassandra type {@code type} stored under {@code typeCode} may declare:
     * the type's own order when the type code carries its extrema in that order, and
     * {@link StatOrder#NONE} otherwise.
     *
     * <p>The reduction to {@code NONE} is the sound direction and it is not hypothetical.
     * {@code time} is {@code ComparisonType.BYTE_ORDER}, i.e. unsigned, but is eight bytes and so
     * stored as {@code INT64}, whose block extrema are signed; over the legal domain
     * (0 .. 86,399,999,999,999 ns) the two agree, so a declaration of {@code SIGNED_INT} would pass
     * every test built from legal values and disagree with Cassandra only on a value with bit 63 set.
     * Losing pruning on that column costs time; declaring an order the statistics are not in costs
     * rows.
     */
    public static StatOrder statOrderFor(int typeCode, AbstractType<?> type)
    {
        StatOrder declared = StatOrder.forType(type);
        return declared == canonicalStatOrder(typeCode) ? declared : StatOrder.NONE;
    }

    // -----------------------------------------------------------------------------------------
    // encode
    // -----------------------------------------------------------------------------------------

    /** {@link #encode(long[], int, SortedMap, int)} at §2's v4.0 block size of 1,024 rows. */
    public static ByteBuffer encode(long[] timestamps, int rowCount, SortedMap<String, ColumnInput> columns)
    {
        return encode(timestamps, rowCount, columns, ChunkV4Header.DEFAULT_BLOCK_SIZE_LOG2);
    }

    /**
     * Builds one v4 payload: header, directory, timestamp section, and one section per column, in the
     * §4.5 order. The returned buffer is heap-backed, big-endian, and spans exactly the chunk from
     * position 0 to its limit.
     *
     * <p>Timestamps must be strictly increasing. The format does not need them to be -- the axis is a
     * column like any other -- but the header's {@code firstTimestamp}/{@code lastTimestamp} are
     * defined as the first and last row's, and every pruning decision made from them assumes they are
     * also the minimum and maximum. A window whose rows are not time-ordered is a merge bug upstream,
     * and accepting it here would turn that bug into missing rows at read time rather than an
     * exception at write time.
     *
     * @param blockSizeLog2 §7's 6..15; v4.0 writes {@link ChunkV4Header#DEFAULT_BLOCK_SIZE_LOG2} and
     *                      the parameter exists so the layout can be exercised at both bounds
     */
    public static ByteBuffer encode(long[] timestamps,
                                    int rowCount,
                                    SortedMap<String, ColumnInput> columns,
                                    int blockSizeLog2)
    {
        if (rowCount < 1)
            throw new IllegalArgumentException("rowCount must be >= 1, got " + rowCount);
        if (rowCount > ChunkV4Header.MAX_ROWS)
            throw new IllegalArgumentException("rowCount " + rowCount + " exceeds MAX_ROWS " + ChunkV4Header.MAX_ROWS);
        if (timestamps == null || timestamps.length < rowCount)
            throw new IllegalArgumentException("timestamps array shorter than rowCount " + rowCount);
        for (int i = 1; i < rowCount; i++)
            if (timestamps[i] <= timestamps[i - 1])
                throw new IllegalArgumentException("timestamps must be strictly increasing: " + timestamps[i] +
                                                   " after " + timestamps[i - 1]);
        ChunkV4Header.blockCount(rowCount, blockSizeLog2);   // validates blockSizeLog2 against §7

        // §5 rule 1. `new TreeMap<>(columns)` would ADOPT the caller's comparator instead of forcing
        // natural order -- the exact trap the rule names, and the one v3 fell into.
        SortedMap<String, ColumnInput> sorted = new TreeMap<>();
        sorted.putAll(columns);
        if (sorted.size() > ChunkV4Header.MAX_COLUMNS)
            throw new IllegalArgumentException("too many columns: " + sorted.size() + ", §7 caps them at " +
                                               ChunkV4Header.MAX_COLUMNS);

        List<PlannedColumn> planned = new ArrayList<>(sorted.size());
        for (Map.Entry<String, ColumnInput> column : sorted.entrySet())
            planned.add(plan(column.getKey(), column.getValue(), rowCount, blockSizeLog2));

        // The axis, through the same call an INT64 column makes. No column constant is offered: it is
        // what would make the section zero-byte, and a chunk with no timestamps is not a chunk.
        ChunkV4Section.Encoded timestampSection =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT64, timestamps, null, rowCount, blockSizeLog2,
                                       null, ALP);

        // An entry's length is independent of its section offset -- every offset is a fixed-width u32
        // (§5 rule 4) -- so the directory can be sized before the offsets it will carry are known.
        List<ChunkV4Directory.Entry> provisional = new ArrayList<>(planned.size());
        for (PlannedColumn column : planned)
            provisional.add(column.entry(0));
        int directoryLen = ChunkV4Directory.encodedLength(provisional);

        int tsSectionOffset = ChunkV4Header.HEADER_SIZE + directoryLen;
        long running = (long) tsSectionOffset + timestampSection.sectionLen();
        List<ChunkV4Directory.Entry> entries = new ArrayList<>(planned.size());
        for (PlannedColumn column : planned)
        {
            int sectionLen = column.section.sectionLen();
            if (running > Integer.MAX_VALUE)
                throw new IllegalArgumentException("chunk overflows int at section offset " + running);
            // §5 rule 6: a zero-byte section has no offset, and an unused field is zero.
            entries.add(column.entry(sectionLen == 0 ? 0 : (int) running));
            running += sectionLen;
        }
        if (running > Integer.MAX_VALUE)
            throw new IllegalArgumentException("chunk of " + running + " bytes overflows int");
        int total = (int) running;

        ChunkV4Header header = new ChunkV4Header(rowCount, timestamps[0], timestamps[rowCount - 1], blockSizeLog2,
                                                 entries.size(), directoryLen, tsSectionOffset,
                                                 timestampSection.sectionLen());
        ByteBuffer out = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        header.write(out);
        ChunkV4Directory.write(entries, rowCount, blockSizeLog2, out);
        out.put(timestampSection.bytes);
        for (PlannedColumn column : planned)
            out.put(column.section.bytes);
        if (out.position() != total)
            throw new IllegalStateException("wrote " + out.position() + " bytes, planned " + total);
        out.flip();
        return out;
    }

    /**
     * A column decided but not yet placed: everything the directory entry needs except the offset,
     * which is not known until every section has been sized.
     */
    private static final class PlannedColumn
    {
        final String name;
        final int typeCode;
        final int colFlags;
        final StatOrder statOrder;
        final byte[] constBytes;
        final byte[] statMin;
        final byte[] statMax;
        final ChunkV4Section.Encoded section;

        PlannedColumn(String name, int typeCode, int colFlags, StatOrder statOrder, byte[] constBytes,
                      byte[] statMin, byte[] statMax, ChunkV4Section.Encoded section)
        {
            this.name = name;
            this.typeCode = typeCode;
            this.colFlags = colFlags;
            this.statOrder = statOrder;
            this.constBytes = constBytes;
            this.statMin = statMin;
            this.statMax = statMax;
            this.section = section;
        }

        ChunkV4Directory.Entry entry(int sectionOffset)
        {
            return new ChunkV4Directory.Entry(typeCode, colFlags, statOrder, name, sectionOffset,
                                              section.sectionLen(), section.blockCount(), constBytes,
                                              statMin, statMax);
        }
    }

    private static PlannedColumn plan(String name, ColumnInput input, int rowCount, int blockSizeLog2)
    {
        if (input == null)
            throw new IllegalArgumentException("column " + name + ": null input");
        if (input.values.length < rowCount)
            throw new IllegalArgumentException("column " + name + ": values array holds " + input.values.length +
                                               " < rowCount " + rowCount);

        // Row-indexed, null == absent. The copy is not waste: a caller's ByteBuffer may be a view
        // whose position moves, and every decision below compares these bytes many times.
        byte[][] values = new byte[rowCount][];
        int presentCount = 0;
        for (int i = 0; i < rowCount; i++)
        {
            ByteBuffer value = input.values[i];
            if (value == null)
                continue;
            byte[] bytes = new byte[value.remaining()];
            value.duplicate().get(bytes);
            values[i] = bytes;
            presentCount++;
        }

        int typeCode = input.typeCode;
        StatOrder statOrder = input.statOrder;
        // §12, fourth on the risk list: one width-incompatible value downgrades this column, in this
        // chunk only, to the code that stores whatever bytes it is given -- and statOrder MUST follow
        // it to NONE, because the extrema of a downgraded column are no longer in an order any reader
        // can match. That is one line and it is the easy one to forget.
        if (!widthCompatible(typeCode, values, rowCount))
        {
            typeCode = ChunkV4Directory.TYPE_OPAQUE;
            statOrder = StatOrder.NONE;
        }
        if (typeCode == ChunkV4Directory.TYPE_OPAQUE)
            statOrder = StatOrder.NONE;

        boolean allPresent = presentCount == rowCount;
        boolean allNull = presentCount == 0;

        // §4: constancy is judged over the PRESENT values only, so a partially-null column can be
        // CONSTANT. It then still needs a section -- to carry its presence -- which is why
        // ChunkV4Directory.Entry allows a zero-byte section only for ALL_NULL and all-present
        // CONSTANT, and why the blocks of such a column encode as PRESENCE_ONLY or CONSTANT.
        byte[] constBytes = null;
        boolean constant = false;
        if (!allNull)
        {
            constant = true;
            for (int i = 0; i < rowCount; i++)
            {
                if (values[i] == null)
                    continue;
                if (constBytes == null)
                    constBytes = values[i];
                else if (!Arrays.equals(constBytes, values[i]))
                {
                    constant = false;
                    break;
                }
            }
            if (!constant)
                constBytes = null;
        }

        int colFlags = 0;
        if (allPresent)
            colFlags |= ChunkV4Directory.FLAG_ALL_PRESENT;
        if (allNull)
            colFlags |= ChunkV4Directory.FLAG_ALL_NULL;
        if (constant)
            colFlags |= ChunkV4Directory.FLAG_CONSTANT;

        byte[] statMin = null;
        byte[] statMax = null;
        if (allNull || constant || !statOrder.hasOrder())
        {
            // §4: a CONSTANT or ALL_NULL column carries no statistics -- the constant IS the pair of
            // extrema, and nothing bounds an empty set -- and HAS_STATS and statOrder must agree, so
            // the order goes with them.
            statOrder = StatOrder.NONE;
        }
        else
        {
            for (int i = 0; i < rowCount; i++)
            {
                if (values[i] == null)
                    continue;
                if (statMin == null)
                {
                    statMin = values[i];
                    statMax = values[i];
                }
                else
                {
                    if (statOrder.compare(values[i], statMin) < 0)
                        statMin = values[i];
                    if (statOrder.compare(values[i], statMax) > 0)
                        statMax = values[i];
                }
            }
            // §7: omitted, never truncated. Chop a maximum to 256 bytes and it compares BELOW the
            // value it was meant to bound, so a reader that trusts it skips a block that does contain
            // matching rows -- a missing row, not a slow query. There is no outward rounding that
            // survives every StatOrder, so the whole column goes without.
            if (statMin.length > ChunkV4Directory.MAX_STAT_BYTES || statMax.length > ChunkV4Directory.MAX_STAT_BYTES)
            {
                statMin = null;
                statMax = null;
                statOrder = StatOrder.NONE;
            }
            else
            {
                colFlags |= ChunkV4Directory.FLAG_HAS_STATS;
            }
        }

        ChunkV4Section.Encoded section;
        if (BlockEncodings.isFixedWidth(typeCode))
        {
            long[] longs = new long[rowCount];
            boolean[] present = allPresent ? null : new boolean[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (values[i] == null)
                    continue;
                longs[i] = BlockEncodings.fromFixedBytes(values[i], typeCode);
                if (present != null)
                    present[i] = true;
            }
            section = ChunkV4Section.encodeFixed(typeCode, longs, present, rowCount, blockSizeLog2, constBytes, ALP);
        }
        else
        {
            section = ChunkV4Section.encodeVariable(typeCode, values, rowCount, blockSizeLog2, constBytes);
        }

        return new PlannedColumn(name, typeCode, colFlags, statOrder, constBytes, statMin, statMax, section);
    }

    /**
     * True when every present value can round-trip through {@code typeCode}'s fixed-width encoding.
     * A false answer is the §12 {@code OPAQUE} downgrade trigger, not an error: Cassandra admits a
     * zero-length {@code int}, and a boolean byte other than 0 or 1 is a second byte form of a value
     * v4 gives exactly one ({@link BlockEncodings#checkValueDomain}), so both are values this code
     * cannot carry rather than values the caller got wrong.
     */
    private static boolean widthCompatible(int typeCode, byte[][] values, int rowCount)
    {
        if (!BlockEncodings.isFixedWidth(typeCode))
            return true;
        int width = BlockEncodings.fixedWidthBytes(typeCode);
        for (int i = 0; i < rowCount; i++)
        {
            byte[] value = values[i];
            if (value == null)
                continue;
            if (value.length != width)
                return false;
            if (typeCode == ChunkV4Directory.TYPE_BOOLEAN && value[0] != 0 && value[0] != 1)
                return false;
        }
        return true;
    }

    // -----------------------------------------------------------------------------------------
    // O(1) header peeks (§3)
    // -----------------------------------------------------------------------------------------
    //
    // Delegated rather than reimplemented: §3's promise is that these read one field out of the first
    // 21 bytes and never parse a directory, a block table or a section, and there is exactly one
    // place that can keep it.

    /** §3, offset 1: the chunk's row count, without touching a section. */
    public static int rowCount(ByteBuffer payload)
    {
        return ChunkV4Header.rowCount(payload);
    }

    /** §3, offset 5: the timestamp of row 0 -- the chunk's lower time bound -- without touching a section. */
    public static long firstTimestamp(ByteBuffer payload)
    {
        return ChunkV4Header.firstTimestamp(payload);
    }

    /** §3, offset 13: the timestamp of the last row -- the upper time bound -- without touching a section. */
    public static long lastTimestamp(ByteBuffer payload)
    {
        return ChunkV4Header.lastTimestamp(payload);
    }

    // -----------------------------------------------------------------------------------------
    // read
    // -----------------------------------------------------------------------------------------

    /**
     * Parses {@code payload}'s header and directory and nothing else: no section preamble, no block
     * table, no body. That is what makes the returned {@link Chunk} usable as a pruning handle -- the
     * chunk-level statistics and every column's shape are available for the cost of the metadata the
     * reader had to touch anyway.
     *
     * <p>{@code payload} must span exactly one chunk from its position to its limit. It is not
     * modified, and the {@link Chunk} keeps a duplicate.
     */
    public static Chunk open(ByteBuffer payload)
    {
        ByteBuffer chunk = payload.duplicate();
        chunk.order(ByteOrder.BIG_ENDIAN);
        if (chunk.remaining() < 1)
            throw new IllegalArgumentException("Corrupt v4 chunk: empty payload");
        byte version = chunk.get(chunk.position());
        if (version != VERSION)
            // §10, §11: the decision of which version byte is a removed format and which is a
            // scrambled byte lives in ChunkCodecs and is not duplicated here.
            throw ChunkCodecs.unsupportedVersion(version, "v4 chunk");
        try
        {
            return new Chunk(chunk);
        }
        catch (RuntimeException e)
        {
            throw corrupt(e);
        }
    }

    /**
     * A forward-only cursor over the chunk's rows, restricted to {@code projection} ({@code null} =
     * every column). The sections of unprojected columns are never parsed -- not their preamble, not
     * their block table, not a byte of a body.
     */
    public static Cursor cursor(ByteBuffer payload, Set<String> projection)
    {
        return open(payload).cursor(projection);
    }

    /**
     * The chunk's timestamps, row-indexed. Walks the axis block by block through the ordinary cursor,
     * so nothing here is sized by anything but the header's {@code rowCount}, which {@link #open} has
     * already bounded against the timestamp section the payload actually carries.
     */
    public static long[] toTimestamps(ByteBuffer payload)
    {
        Cursor cursor = cursor(payload, Collections.emptySet());
        long[] timestamps = new long[cursor.rowCount()];
        for (int i = 0; i < timestamps.length; i++)
        {
            if (!cursor.advance())
                throw new IllegalArgumentException("Corrupt v4 chunk: timestamp axis ended at row " + i + " of " +
                                                   timestamps.length);
            timestamps[i] = cursor.timestamp();
        }
        return timestamps;
    }

    /**
     * The chunk's columns as encoder inputs, taking each column's type code and declared order from
     * the directory rather than from a schema.
     *
     * <p>This is the re-encoder's late-merge path in miniature, and the reason
     * {@code encode(toTimestamps(p), rowCount(p), toColumnInputs(p), blockSizeLog2(p))} must equal
     * {@code p} byte for byte: the merge path decodes chunks it did not write and re-encodes the
     * union, and a chunk that came back different would be reported changed on every cycle and
     * rewritten forever.
     */
    public static SortedMap<String, ColumnInput> toColumnInputs(ByteBuffer payload)
    {
        Chunk chunk = open(payload);
        int rowCount = chunk.rowCount();
        List<ChunkV4Directory.Entry> entries = chunk.directory().entries();
        List<ByteBuffer[]> values = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++)
            values.add(new ByteBuffer[rowCount]);

        Cursor cursor = chunk.cursor(null);
        for (int row = 0; row < rowCount; row++)
        {
            if (!cursor.advance())
                throw new IllegalArgumentException("Corrupt v4 chunk: rows ended at " + row + " of " + rowCount);
            for (int i = 0; i < entries.size(); i++)
                values.get(i)[row] = cursor.getBytes(entries.get(i).name);
        }

        SortedMap<String, ColumnInput> columns = new TreeMap<>();
        for (int i = 0; i < entries.size(); i++)
        {
            ChunkV4Directory.Entry entry = entries.get(i);
            columns.put(entry.name, new ColumnInput(entry.typeCode, entry.statOrder, values.get(i)));
        }
        return columns;
    }

    /**
     * Rethrows what §11 already classifies and converts everything else. {@link UnsupportedChunkFormatException}
     * extends {@link IllegalArgumentException}, so the first branch passes it through untouched -- §10
     * forbids swallowing an unsupported format, and a wrapper would hide it from a caller that catches
     * the subtype first.
     */
    private static RuntimeException corrupt(RuntimeException e)
    {
        if (e instanceof IllegalArgumentException)
            return e;
        return new IllegalArgumentException("Corrupt v4 chunk: " + e, e);
    }

    // -----------------------------------------------------------------------------------------
    // the chunk handle
    // -----------------------------------------------------------------------------------------

    /**
     * A parsed header and directory over one payload, plus the sections that have been opened since.
     *
     * <p><b>Sections are opened lazily and cached.</b> That is what makes projection and pruning cost
     * what they claim to: a column nobody reads contributes nothing but its directory entry, and a
     * block-level statistic costs the section's preamble and block table but not one body byte.
     * {@link #bytesOpened} accounts for exactly that, in bytes, at the finest granularity the format
     * allows -- metadata whole, bodies individually -- so "projection skipped that section" is a
     * number a test can assert rather than a duration it has to time.
     *
     * <p>Not thread-safe: one chunk handle belongs to one read.
     */
    public static final class Chunk
    {
        private final ByteBuffer payload;
        private final ChunkV4Header header;
        private final ChunkV4Directory directory;
        private final ChunkV4Section[] sections;
        private final BlockEncodings.Scratch scratch;
        private final int valuesPerBlock;

        private ChunkV4Section timestampSection;
        private long bytesOpened;
        private long rankCalls;

        private Chunk(ByteBuffer payload)
        {
            this.payload = payload;
            this.header = ChunkV4Header.read(payload);
            // §11, bounded allocation: every block of the timestamp axis needs a 24-byte block-table
            // entry (statWidth 8), so the header's own tsSectionLen caps the block count and
            // therefore rowCount. Without this a header claiming 16,777,216 rows inside a 64-byte
            // payload would be believed all the way to a row-indexed array. The check reads header
            // fields only, so it does not cost the peeks their O(1).
            long maxBlocks = header.tsSectionLen / ChunkV4BlockTable.ENTRY_SIZE_STAT8;
            if (header.blockCount() > maxBlocks)
                throw new IllegalArgumentException("Corrupt v4 chunk: rowCount " + header.rowCount + " implies " +
                                                   header.blockCount() + " blocks, whose timestamp block table needs " +
                                                   ((long) header.blockCount() * ChunkV4BlockTable.ENTRY_SIZE_STAT8) +
                                                   " bytes, but tsSectionLen is " + header.tsSectionLen);
            this.directory = ChunkV4Directory.read(payload, header);
            this.sections = new ChunkV4Section[directory.size()];
            this.valuesPerBlock = Math.min(header.blockSize(), header.rowCount);
            this.scratch = new BlockEncodings.Scratch(valuesPerBlock);
            this.bytesOpened = ChunkV4Header.HEADER_SIZE + (long) header.directoryLen;
        }

        public ChunkV4Header header()
        {
            return header;
        }

        public ChunkV4Directory directory()
        {
            return directory;
        }

        public int rowCount()
        {
            return header.rowCount;
        }

        /** §3: the chunk's lower time bound, from the header. */
        public long firstTimestamp()
        {
            return header.firstTimestamp;
        }

        /** §3: the chunk's upper time bound, from the header. */
        public long lastTimestamp()
        {
            return header.lastTimestamp;
        }

        public int blockSizeLog2()
        {
            return header.blockSizeLog2;
        }

        public int blockCount()
        {
            return header.blockCount();
        }

        /** §2: rows block {@code k} covers. */
        public int blockRows(int block)
        {
            return header.blockRows(block);
        }

        /** The chunk's column names, in natural {@code String} order. */
        public List<String> columnNames()
        {
            List<String> names = new ArrayList<>(directory.size());
            for (ChunkV4Directory.Entry entry : directory.entries())
                names.add(entry.name);
            return Collections.unmodifiableList(names);
        }

        /** The directory entry for {@code name}, or null. */
        public ChunkV4Directory.Entry column(String name)
        {
            return directory.column(name);
        }

        /**
         * Payload bytes this handle has parsed: the header and directory always, plus each opened
         * section's preamble and block table, plus each decoded block's body. A section never opened
         * contributes zero.
         */
        public long bytesOpened()
        {
            return bytesOpened;
        }

        /**
         * How many times a cursor over this chunk has called {@link BlockPresence#rank}. Diagnostic,
         * and the one way to assert the rule that method declares normative: a sequential scan must
         * carry a running value index and never call {@code rank} per row, so this stays at zero
         * across a whole forward scan and grows only on a seek.
         */
        public long rankCalls()
        {
            return rankCalls;
        }

        public Cursor cursor(Set<String> projection)
        {
            try
            {
                return new Cursor(this, projection);
            }
            catch (RuntimeException e)
            {
                throw corrupt(e);
            }
        }

        // -------------------------------------------------------------------------------------
        // pruning (§1): sound in one direction only
        // -------------------------------------------------------------------------------------

        /**
         * Whether any row of this chunk can fall in {@code [from, to]}, from the header alone. The
         * whole point of §3 keeping the bounds at v3's offsets: deciding not to open a chunk must not
         * cost what opening it would.
         */
        public boolean mayContainTime(long from, long to)
        {
            return header.lastTimestamp >= from && header.firstTimestamp <= to;
        }

        /**
         * Whether any row of block {@code k} can fall in {@code [from, to]}, from the timestamp
         * section's block table. Opens the axis's preamble and table -- no body.
         */
        public boolean blockMayContainTime(int block, long from, long to)
        {
            checkBlock(block);
            ChunkV4BlockTable.Entry entry = timestampSection().blockTable().entry(block);
            if (entry.presenceMode() == BlockPresence.MODE_ALL_NULL)
                return false;
            return entry.max >= from && entry.min <= to;
        }

        /** The declared order of {@code column}'s chunk-level statistics, or {@link StatOrder#NONE}. */
        public StatOrder statOrder(String column)
        {
            ChunkV4Directory.Entry entry = directory.column(column);
            return entry == null ? StatOrder.NONE : entry.statOrder;
        }

        /**
         * The order {@code column}'s <em>block</em> extrema are in: the type code's canonical order,
         * which is what {@link ChunkV4Section} computed them under, or {@link StatOrder#NONE} for a
         * type that has no per-block extrema field at all (§5's {@code statWidth 0}: {@code BOOLEAN},
         * {@code TEXT}, {@code OPAQUE}).
         *
         * <p>It is deliberately not the same accessor as {@link #statOrder}. A {@code CONSTANT}
         * column carries no chunk statistics and so declares {@code NONE}, yet its blocks still have
         * real extrema; conflating the two would either lose that pruning or claim an order the
         * directory does not declare.
         */
        public StatOrder blockStatOrder(String column)
        {
            ChunkV4Directory.Entry entry = directory.column(column);
            if (entry == null || ChunkV4Directory.statWidth(entry.typeCode) == 0)
                return StatOrder.NONE;
            return canonicalStatOrder(entry.typeCode);
        }

        /** {@code column}'s chunk-level minimum under {@link #statOrder}, or null when it carries none. */
        public byte[] chunkMin(String column)
        {
            ChunkV4Directory.Entry entry = directory.column(column);
            return entry == null ? null : entry.statMin;
        }

        /** {@code column}'s chunk-level maximum under {@link #statOrder}, or null when it carries none. */
        public byte[] chunkMax(String column)
        {
            ChunkV4Directory.Entry entry = directory.column(column);
            return entry == null ? null : entry.statMax;
        }

        /**
         * The serialized minimum of block {@code k} of {@code column} under {@link #blockStatOrder},
         * or null when there is none to report: an unknown column, a type with no per-block extrema,
         * a column whose whole section is zero bytes (§2's O(1) columns, whose value is the directory
         * constant or nothing at all), or a block with no present row.
         *
         * <p>Reads the block table and no body.
         */
        public byte[] blockMin(String column, int block)
        {
            return blockStat(column, block, true);
        }

        /** {@link #blockMin}'s counterpart. */
        public byte[] blockMax(String column, int block)
        {
            return blockStat(column, block, false);
        }

        private byte[] blockStat(String column, int block, boolean wantMin)
        {
            checkBlock(block);
            int index = indexOf(column);
            if (index < 0)
                return null;
            ChunkV4Directory.Entry entry = directory.entry(index);
            if (entry.sectionLen == 0 || ChunkV4Directory.statWidth(entry.typeCode) == 0)
                return null;
            ChunkV4BlockTable.Entry blockEntry = section(index).blockTable().entry(block);
            if (blockEntry.presenceMode() == BlockPresence.MODE_ALL_NULL)
                return null;
            long bits = wantMin ? blockEntry.min : blockEntry.max;
            return BlockEncodings.toFixedBytes(BlockEncodings.fromStatBits(bits, entry.typeCode), entry.typeCode);
        }

        /**
         * Whether any row of this chunk can hold a value of {@code column} in {@code [lo, hi]} (either
         * bound may be null for open-ended), judged from the chunk-level statistics alone.
         *
         * <p>§1: a pruning answer may be wrong only in the direction of opening something that need
         * not have been opened. A {@code false} here therefore has to be provable from the bytes --
         * an all-null column, a constant that misses the range, or a declared extremum that excludes
         * it -- and every case this cannot prove returns {@code true}.
         */
        public boolean mayContain(String column, byte[] lo, byte[] hi)
        {
            ChunkV4Directory.Entry entry = directory.column(column);
            if (entry == null)
                return true;
            if (entry.allNull())
                return false;
            if (entry.constant())
                return withinRange(canonicalStatOrder(entry.typeCode), entry.constBytes, entry.constBytes, lo, hi);
            if (!entry.hasStats())
                return true;
            return withinRange(entry.statOrder, entry.statMin, entry.statMax, lo, hi);
        }

        /** {@link #mayContain} for one block, from the block table. Opens no body. */
        public boolean blockMayContain(String column, int block, byte[] lo, byte[] hi)
        {
            checkBlock(block);
            ChunkV4Directory.Entry entry = directory.column(column);
            if (entry == null)
                return true;
            if (entry.allNull())
                return false;
            if (entry.constant() && entry.sectionLen == 0)
                return withinRange(canonicalStatOrder(entry.typeCode), entry.constBytes, entry.constBytes, lo, hi);
            byte[] min = blockMin(column, block);
            if (min == null)
            {
                // Either the block holds no present row -- provably no match -- or the type carries no
                // per-block extrema, in which case nothing is proved and the block must be opened.
                if (entry.sectionLen != 0 && ChunkV4Directory.statWidth(entry.typeCode) != 0)
                    return false;
                return true;
            }
            return withinRange(blockStatOrder(column), min, blockMax(column, block), lo, hi);
        }

        /**
         * §2 gives every axis the same block boundaries, so one block index is valid for the whole
         * chunk -- including for a column whose own {@code blockCount} is zero because its section is.
         * Checked here so a corrupt or miscomputed index surfaces as {@link IllegalArgumentException}
         * (§11) rather than as a list's {@link IndexOutOfBoundsException}.
         */
        private void checkBlock(int block)
        {
            if (block < 0 || block >= header.blockCount())
                throw new IllegalArgumentException("block " + block + " outside 0.." + (header.blockCount() - 1));
        }

        private static boolean withinRange(StatOrder order, byte[] min, byte[] max, byte[] lo, byte[] hi)
        {
            if (!order.hasOrder() || min == null || max == null)
                return true;
            if (lo != null && order.compare(max, lo) < 0)
                return false;
            return hi == null || order.compare(min, hi) <= 0;
        }

        // -------------------------------------------------------------------------------------
        // sections
        // -------------------------------------------------------------------------------------

        /**
         * The directory index of {@code name}, or -1. A binary search over the sorted entries rather
         * than a map: §5 rule 7 keeps hash-ordered structures out of the layers that touch bytes, and
         * a sorted sequence cannot grow a hash-ordered view of itself.
         */
        private int indexOf(String name)
        {
            List<ChunkV4Directory.Entry> entries = directory.entries();
            int low = 0;
            int high = entries.size() - 1;
            while (low <= high)
            {
                int mid = (low + high) >>> 1;
                int cmp = entries.get(mid).name.compareTo(name);
                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid;
            }
            return -1;
        }

        private ChunkV4Section section(int index)
        {
            ChunkV4Section section = sections[index];
            if (section == null)
            {
                ChunkV4Directory.Entry entry = directory.entry(index);
                section = ChunkV4Section.read(payload, entry.sectionOffset, entry.sectionLen, entry.typeCode,
                                              header.rowCount, header.blockSizeLog2, entry.constBytes, ALP);
                sections[index] = section;
                bytesOpened += metadataBytes(section, entry.typeCode);
            }
            return section;
        }

        private ChunkV4Section timestampSection()
        {
            if (timestampSection == null)
            {
                timestampSection = ChunkV4Section.read(payload, header.tsSectionOffset, header.tsSectionLen,
                                                       ChunkV4Directory.TYPE_INT64, header.rowCount,
                                                       header.blockSizeLog2, null, ALP);
                bytesOpened += metadataBytes(timestampSection, ChunkV4Directory.TYPE_INT64);
            }
            return timestampSection;
        }

        /** A section's non-body bytes: its preamble (text and opaque only) and its block table. */
        private static long metadataBytes(ChunkV4Section section, int typeCode)
        {
            return section.preambleLength()
                   + (long) ChunkV4BlockTable.tableLength(section.blockCount(),
                                                          ChunkV4Directory.statWidth(typeCode));
        }

        private int rank(long[] words, int offset)
        {
            rankCalls++;
            return BlockPresence.rank(words, offset);
        }
    }

    // -----------------------------------------------------------------------------------------
    // the cursor
    // -----------------------------------------------------------------------------------------

    /**
     * A cursor over one chunk's rows. Call {@link #advance} before every accessor; it returns false
     * once the rows are exhausted.
     *
     * <p><b>A sequential scan carries a running value index and never calls
     * {@link BlockPresence#rank}.</b> That method's javadoc makes the rule normative and explains the
     * cost: {@code rank} is O(offset/64) by construction, which is the right shape for a seek and the
     * wrong shape for a scan -- calling it per row turns a 1,024-row block walk into 8,192
     * {@code bitCount} intrinsics instead of 1,024 increments, and 16x worse again at §7's maximum
     * block size. So {@link #advance} adds the row's own presence bit to a running index, which is
     * exactly {@code rank(words, row + 1) - rank(words, row)}, and {@link #seekTo} is the only method
     * that calls {@code rank} at all. {@link Chunk#rankCalls} exists so that stays true.
     *
     * <p>Implements v3's {@link ColumnarCursor} and {@link ColumnarChunkCodec.ArrayValueCursor}
     * unchanged, so the transparent-read and re-encode paths do not have to learn a second shape when
     * v4 is wired in. The buffer contract is v3's too, and is documented on {@link ChunkV4Codec}:
     * treat every returned buffer as immutable, and never turn one into a read-only buffer.
     */
    public static final class Cursor implements ColumnarCursor, ColumnarChunkCodec.ArrayValueCursor
    {
        private final Chunk chunk;
        private final ColumnState timestamps;
        /** Ascending by name -- the directory's order, filtered by the projection. */
        private final ColumnState[] columns;
        private final Set<String> columnNames;
        private final int rowCount;
        private int row = -1;

        private Cursor(Chunk chunk, Set<String> projection)
        {
            this.chunk = chunk;
            this.rowCount = chunk.header.rowCount;
            this.timestamps = ColumnState.timestampAxis(chunk);

            List<ColumnState> states = new ArrayList<>();
            Set<String> names = new LinkedHashSet<>();
            List<ChunkV4Directory.Entry> entries = chunk.directory.entries();
            for (int i = 0; i < entries.size(); i++)
            {
                ChunkV4Directory.Entry entry = entries.get(i);
                if (projection != null && !projection.contains(entry.name))
                    continue;   // not projected: this column's section is never parsed
                states.add(ColumnState.column(chunk, i, entry));
                names.add(entry.name);
            }
            this.columns = states.toArray(new ColumnState[0]);
            this.columnNames = Collections.unmodifiableSet(names);
        }

        public int rowCount()
        {
            return rowCount;
        }

        /** The row this cursor is on, or -1 before the first {@link #advance}. */
        public int row()
        {
            return row;
        }

        @Override
        public boolean advance()
        {
            if (row + 1 >= rowCount)
                return false;
            row++;
            try
            {
                timestamps.step(row);
                for (ColumnState column : columns)
                    column.step(row);
            }
            catch (RuntimeException e)
            {
                throw corrupt(e);
            }
            return true;
        }

        /**
         * Moves to {@code row} discontinuously. This is the one place {@link BlockPresence#rank} is
         * called: the scan position has moved by an unknown amount, so the running value index has to
         * be recomputed rather than incremented. A subsequent {@link #advance} resumes the cheap path
         * from here.
         */
        public void seekTo(int target)
        {
            if (target < 0 || target >= rowCount)
                throw new IllegalArgumentException("row " + target + " outside 0.." + (rowCount - 1));
            row = target;
            try
            {
                timestamps.seek(row);
                for (ColumnState column : columns)
                    column.seek(row);
            }
            catch (RuntimeException e)
            {
                throw corrupt(e);
            }
        }

        @Override
        public long timestamp()
        {
            requirePositioned();
            // The axis is ALL_PRESENT by construction, so its value index is never -1 on a chunk this
            // build wrote; a payload that says otherwise is corrupt rather than a row without a time.
            if (timestamps.valueIndex < 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: row " + row + " has no timestamp");
            return timestamps.fixedValues[timestamps.valueIndex];
        }

        @Override
        public boolean hasColumn(String name)
        {
            return columnNames.contains(name);
        }

        @Override
        public boolean isNull(String name)
        {
            requirePositioned();
            ColumnState column = find(name);
            return column == null || column.valueIndex < 0;
        }

        @Override
        public byte[] getByteArray(String name)
        {
            requirePositioned();
            ColumnState column = find(name);
            return column == null ? null : column.value();
        }

        /**
         * {@inheritDoc}
         *
         * <p>The slot IS the index into {@link #columns}, which {@link #find} binary-searches by name. Resolving
         * it once per scan is the whole point: {@link #columns} is fixed for the cursor's lifetime, so a name
         * lookup per cell is repeated work on a per-scan constant.
         */
        @Override
        public int columnSlot(String name)
        {
            for (int i = 0; i < columns.length; i++)
                if (columns[i].name.equals(name))
                    return i;
            return ABSENT_COLUMN;
        }

        @Override
        public byte[] getByteArray(int slot)
        {
            requirePositioned();
            return slot == ABSENT_COLUMN ? null : columns[slot].value();
        }

        @Override
        public ByteBuffer getBytes(int slot)
        {
            byte[] value = getByteArray(slot);
            // wrap(), NEVER asReadOnlyBuffer() -- see getBytes(String).
            return value == null ? null : ByteBuffer.wrap(value);
        }

        @Override
        public ByteBuffer getBytes(String name)
        {
            byte[] value = getByteArray(name);
            // wrap(), NEVER asReadOnlyBuffer(): a read-only HEAP buffer reports hasArray() == false
            // and isDirect() == false, so FastByteOperations takes its direct branch and dereferences
            // address 0 -- a SIGSEGV the first time anything compares a decoded cell. See the buffer
            // contract on ChunkV4Codec; callers must treat the result as immutable.
            return value == null ? null : ByteBuffer.wrap(value);
        }

        @Override
        public Set<String> columns()
        {
            return columnNames;
        }

        /** Payload bytes this cursor's chunk has parsed so far; see {@link Chunk#bytesOpened}. */
        public long bytesOpened()
        {
            return chunk.bytesOpened();
        }

        /** {@link BlockPresence#rank} calls made; zero across a whole sequential scan. */
        public long rankCalls()
        {
            return chunk.rankCalls();
        }

        private ColumnState find(String name)
        {
            int low = 0;
            int high = columns.length - 1;
            while (low <= high)
            {
                int mid = (low + high) >>> 1;
                int cmp = columns[mid].name.compareTo(name);
                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return columns[mid];
            }
            return null;
        }

        private void requirePositioned()
        {
            if (row < 0)
                throw new IllegalStateException("advance() not called");
        }
    }

    /**
     * One column's decode state: the block it currently holds, that block's presence words and dense
     * values, and the running index of the current row's value inside them.
     *
     * <p>Values stay in <em>lane</em> order -- dense, present-only -- rather than being scattered into
     * a row-indexed array, which is what {@link ChunkV4Section#decodeFixedBlock} hands back and what
     * makes the running-index rule possible. Scattering would cost a row-indexed array per column per
     * block and would tempt exactly the per-row {@code rank} the format's second-largest risk is
     * about.
     */
    private static final class ColumnState
    {
        private final Chunk chunk;
        private final int directoryIndex;      // -1 for the timestamp axis
        final String name;
        private final int typeCode;
        private final int sectionLen;
        private final byte[] constant;
        private final boolean allNull;
        private final boolean fixed;

        private ChunkV4Section section;
        private int loadedBlock = -1;
        private long[] words;
        long[] fixedValues;
        private byte[][] variableValues;
        private int running;
        int valueIndex = -1;

        private ColumnState(Chunk chunk, int directoryIndex, String name, int typeCode, int sectionLen,
                            byte[] constant, boolean allNull)
        {
            this.chunk = chunk;
            this.directoryIndex = directoryIndex;
            this.name = name;
            this.typeCode = typeCode;
            this.sectionLen = sectionLen;
            this.constant = constant;
            this.allNull = allNull;
            this.fixed = BlockEncodings.isFixedWidth(typeCode);
        }

        static ColumnState column(Chunk chunk, int index, ChunkV4Directory.Entry entry)
        {
            return new ColumnState(chunk, index, entry.name, entry.typeCode, entry.sectionLen, entry.constBytes,
                                   entry.allNull());
        }

        /** The axis (§3): {@code INT64}, all present, located by the header rather than by a name. */
        static ColumnState timestampAxis(Chunk chunk)
        {
            return new ColumnState(chunk, -1, "", ChunkV4Directory.TYPE_INT64, chunk.header.tsSectionLen, null, false);
        }

        /** Sequential: the running index is already the count of present rows below this one. */
        void step(int row)
        {
            if (allNull)
            {
                valueIndex = -1;
                return;
            }
            if (sectionLen == 0)
            {
                // §2's other O(1) column: CONSTANT and every row present, so there is no section to
                // read and the directory's single array is every row's value.
                valueIndex = 0;
                return;
            }
            int block = row >>> chunk.header.blockSizeLog2;
            int offset = row & (chunk.header.blockSize() - 1);
            if (block != loadedBlock)
            {
                load(block);
                // A forward scan enters every block at offset 0, where the rank is 0 by definition;
                // the call is only reached when a caller advanced after seeking into a block's middle.
                running = offset == 0 ? 0 : chunk.rank(words, offset);
            }
            take(offset);
        }

        /** Discontinuous: recompute the running index, which is what {@code rank} is for. */
        void seek(int row)
        {
            if (allNull)
            {
                valueIndex = -1;
                return;
            }
            if (sectionLen == 0)
            {
                valueIndex = 0;
                return;
            }
            int block = row >>> chunk.header.blockSizeLog2;
            int offset = row & (chunk.header.blockSize() - 1);
            if (block != loadedBlock)
                load(block);
            running = chunk.rank(words, offset);
            take(offset);
        }

        private void take(int offset)
        {
            boolean present = ((words[offset >>> 6] >>> (offset & 63)) & 1L) != 0;
            valueIndex = present ? running : -1;
            if (present)
                running++;
        }

        private void load(int block)
        {
            if (section == null)
                section = directoryIndex < 0 ? chunk.timestampSection() : chunk.section(directoryIndex);
            if (words == null)
            {
                // Sized from the block geometry, never from a length in the payload, and allocated
                // only for a column somebody actually reads.
                int capacity = chunk.valuesPerBlock;
                words = new long[BlockPresence.wordCount(capacity)];
                if (fixed)
                    fixedValues = new long[capacity];
                else
                    variableValues = new byte[capacity][];
            }
            if (fixed)
                section.decodeFixedBlock(block, words, fixedValues, chunk.scratch);
            else
                section.decodeVariableBlock(block, words, variableValues, chunk.scratch);
            chunk.bytesOpened += section.blockTable().bodyLength(block);
            loadedBlock = block;
        }

        /** The current row's serialized value, or null when the row has none. */
        byte[] value()
        {
            if (valueIndex < 0)
                return null;
            if (sectionLen == 0)
                return constant;
            // Fixed-width values are serialized afresh per access, as in v3; a TEXT/OPAQUE value is
            // the decoded array itself, which for a DICT block is the dictionary entry shared by
            // every row that cites it. Both are covered by the immutability contract.
            return fixed ? BlockEncodings.toFixedBytes(fixedValues[valueIndex], typeCode)
                         : variableValues[valueIndex];
        }
    }
}
