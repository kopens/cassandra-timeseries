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

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class ThreadLocalReadAheadBuffer implements Closeable
{

    private static class Block
    {
        ByteBuffer buffer = null;
        int index = -1;
    }

    protected final ChannelProxy channel;

    private final Supplier<ByteBuffer> bufferSupplier;

    // One read-ahead block per reader thread, owned by THIS instance. Deliberately not a
    // thread-local: close() runs on whichever thread releases the reader's last reference, and it
    // must release blocks faulted in by OTHER (still-live, pooled) threads. The previous design —
    // a static FastThreadLocal<Map<filePath, Block>> — could only clean the closing thread's own
    // entry, so eternal compaction threads accumulated one direct buffer per (thread, sstable)
    // until MaxDirectMemorySize was exhausted (node 41, 2026-08-02: 6 GiB in ~2.5 h, OOM storm).
    private final ConcurrentHashMap<Thread, Block> blocks = new ConcurrentHashMap<>();

    private volatile int bufferSize = -1;
    private final long channelSize;

    public ThreadLocalReadAheadBuffer(ChannelProxy channel, int bufferSize, BufferType bufferType)
    {
        this(channel, () -> bufferType.allocate(bufferSize));
    }

    public ThreadLocalReadAheadBuffer(ChannelProxy channel, Supplier<ByteBuffer> bufferSupplier)
    {
        this.channel = channel;
        this.channelSize = channel.size();
        this.bufferSupplier = bufferSupplier;
    }

    public boolean hasBuffer()
    {
        return block().buffer != null;
    }

    public int remaining()
    {
        return getBlock().buffer.remaining();
    }

    public void allocateBuffer()
    {
        getBlock();
    }

    private Block getBlock()
    {
        Block block = block();
        if (block.buffer == null)
        {
            block.buffer = bufferSupplier.get();
            block.buffer.clear();
            if (bufferSize == -1)
                bufferSize = block.buffer.capacity();
        }
        return block;
    }

    private Block block()
    {
        return blocks.computeIfAbsent(Thread.currentThread(), t -> new Block());
    }

    public void fill(long position) throws CorruptBlockException
    {
        Block block = getBlock();
        ByteBuffer blockBuffer = block.buffer;
        if (position >= channelSize)
            throw new CorruptBlockException(channel.filePath(), position, bufferSize);

        int blockNo = (int) (position / bufferSize);
        long blockPosition = blockNo * (long) bufferSize;

        long remaining = channelSize - blockPosition;
        int sizeToRead = (int) Math.min(remaining, bufferSize);
        if (block.index != blockNo)
        {
            blockBuffer.flip();
            loadBlock(blockBuffer, blockPosition, sizeToRead);
            block.index = blockNo;
        }

        blockBuffer.flip();
        blockBuffer.limit(sizeToRead);
        blockBuffer.position((int) (position - blockPosition));
    }

    protected void loadBlock(ByteBuffer blockBuffer, long blockPosition, int sizeToRead)
    {
        blockBuffer.limit(sizeToRead);
        if (channel.read(blockBuffer, blockPosition) != sizeToRead)
            throw new CorruptSSTableException(null, channel.filePath());
    }

    public int read(ByteBuffer dest, int length)
    {
        Block block = getBlock();
        ByteBuffer blockBuffer = block.buffer;
        ByteBuffer tmp = blockBuffer.duplicate();
        tmp.limit(tmp.position() + length);
        dest.put(tmp);
        blockBuffer.position(blockBuffer.position() + length);

        return length;
    }

    public void clear(boolean deallocate)
    {
        // avoid calling block() here to reduce unintended allocations
        Block block = blocks.get(Thread.currentThread());
        if (block == null)
            return;

        block.index = -1;
        if (block.buffer == null)
            return;

        ByteBuffer blockBuffer = block.buffer;
        blockBuffer.clear();
        if (deallocate)
        {
            cleanBuffer(blockBuffer);
            block.buffer = null;
        }
    }

    protected void cleanBuffer(ByteBuffer buffer)
    {
        MemoryUtil.clean(buffer);
    }

    @Override
    public void close()
    {
        // Sweeps every thread's block, not just the closing thread's. Safe: by the reader contract
        // (SharedCloseable ref-counting) close() runs only after all reads through this instance
        // have completed, so no block is in use.
        for (Block block : blocks.values())
        {
            block.index = -1;
            if (block.buffer != null)
            {
                cleanBuffer(block.buffer);
                block.buffer = null;
            }
        }
        blocks.clear();
    }
}
