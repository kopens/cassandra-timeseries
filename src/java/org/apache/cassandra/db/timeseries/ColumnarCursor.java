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
import java.util.Set;

/**
 * A forward-only cursor over the rows of a version-3 (columnar) chunk codec payload, restricted
 * to the columns the caller projected (see {@link ColumnarChunkCodec#cursor}). Call
 * {@link #advance()} before every other accessor; it returns {@code false} once the payload is
 * exhausted.
 * <p>
 * Unlike the removed single-column format's {@code SampleCursor} (one value stream, no names),
 * a chunk can outlive a table's schema at encode time, so columns
 * are looked up by name rather than assumed to exist: {@link #hasColumn} reports whether a
 * column is known to this cursor at all (false for a column ADDed to the table after this chunk
 * was written, or for one excluded from the projection), and {@link #isNull}/{@link #getBytes}
 * report per-row presence for columns that are known.
 */
public interface ColumnarCursor
{
    boolean advance();

    long timestamp();

    /** True if {@code name} is a column this cursor knows about (see class javadoc). */
    boolean hasColumn(String name);

    /** True if {@code name} is unknown to this cursor, or known but null on the current row. */
    boolean isNull(String name);

    /**
     * The canonical serialized value of column {@code name} on the current row, or {@code null}
     * if the column is absent (unknown to this cursor, or not projected) or null on this row.
     */
    ByteBuffer getBytes(String name);

    /** The columns this cursor knows about -- the chunk's own columns intersected with the projection. */
    Set<String> columns();

    /**
     * Resolves {@code name} to a slot for {@link #getBytes(int)}, or {@link #ABSENT_COLUMN} when the column is
     * absent (unknown to this cursor, or outside the projection).
     *
     * <p>A scan reads the same handful of columns on every one of its rows, and the name-keyed accessors have to
     * find the column again each time -- a binary search over the column names, {@code String.compareTo} per
     * probe, per cell. That is per-cell work on a per-scan constant. Resolving each name once up front and
     * addressing by slot removes it; a chunk-wide scan of a production-shaped table is thousands of rows times
     * its column count, and {@code doc/timeseries/simd-decode-design.md} §10 measures cursor and row assembly at
     * ~92% of a whole-chunk scan.
     *
     * <p>A slot is valid only for the cursor that issued it.
     */
    int columnSlot(String name);

    /**
     * The canonical serialized value on the current row of the column {@code slot} resolves to, or {@code null}
     * if that column is absent or null on this row. Same contract and precondition as
     * {@link #getBytes(String)}, addressed by {@link #columnSlot} rather than by name.
     */
    ByteBuffer getBytes(int slot);

    /** Returned by {@link #columnSlot} for a column this cursor does not have; every accessor maps it to null. */
    int ABSENT_COLUMN = -1;
}
