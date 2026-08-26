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


import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.WithQuickTheories;
import org.quicktheories.core.Gen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.Pair;

import static java.lang.Math.max;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;

public class ThreadLocalReadAheadBufferTest implements WithQuickTheories
{
    private static final int numFiles = 5;
    private static final Logger logger = LoggerFactory.getLogger(ThreadLocalReadAheadBufferTest.class);
    protected static final File[] files = new File[numFiles];
    protected static Integer seed;

    @BeforeClass
    public static void setup()
    {
        seed = new Random().nextInt();
        logger.info("Seed: {}", seed);

        for (int i = 0; i < numFiles; i++)
        {
            int size = new Random(seed).nextInt((Integer.MAX_VALUE - 1) / 8);
            files[i] = writeFile(seed, size);
        }
    }

    @AfterClass
    public static void cleanup()
    {
        for (File f : files)
        {
            try
            {
                f.delete();
            }
            catch (Exception e)
            {
                // ignore
            }
        }
    }

    /**
     * Production shape of the 2026-08-02 direct-memory exhaustion: pooled compaction threads fault in
     * per-thread read-ahead blocks, then close() runs on a different thread once the shared reader's
     * last reference is released. close() must release every thread's block — the worker threads stay
     * alive (pool threads are eternal in production), so anything close() misses is retained until
     * MaxDirectMemorySize is exhausted.
     */
    @Test
    public void closeReleasesBuffersAllocatedByOtherThreads() throws Exception
    {
        BufferPoolMXBean directPool = directBufferPool();
        int threads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try (ChannelProxy channel = openChannel(files[0]))
        {
            ThreadLocalReadAheadBuffer tlrab = newReadAheadBuffer(channel);
            long minBlockBytes = minBlockBytes(files[0]);
            long before = directPool.getMemoryUsed();

            // Each pool thread faults in its own read-ahead block; barrier on completion.
            List<Future<?>> allocations = new ArrayList<>();
            for (int i = 0; i < threads; i++)
                allocations.add(pool.submit(() -> {
                    // fill() declares CorruptBlockException (CASSANDRA-21519); a Runnable cannot.
                    try
                    {
                        tlrab.fill(0);
                    }
                    catch (CorruptBlockException e)
                    {
                        throw new RuntimeException(e);
                    }
                }));
            for (Future<?> allocation : allocations)
                allocation.get(30, TimeUnit.SECONDS);

            long allocated = directPool.getMemoryUsed() - before;
            Assert.assertTrue("each reader thread should have allocated a direct block; delta was " + allocated,
                              allocated >= threads * minBlockBytes);

            // Worker threads are still alive; close() runs on this (different) thread.
            tlrab.close();

            long retained = directPool.getMemoryUsed() - before;
            Assert.assertTrue("close() must release blocks allocated by other threads; still retaining " +
                              retained + " of " + allocated + " bytes",
                              retained < minBlockBytes);
        }
        finally
        {
            pool.shutdownNow();
        }
    }

    protected ChannelProxy openChannel(File file)
    {
        return new ChannelProxy(file);
    }

    protected ThreadLocalReadAheadBuffer newReadAheadBuffer(ChannelProxy channel)
    {
        return new ThreadLocalReadAheadBuffer(channel, new DataStorageSpec.IntKibibytesBound("256KiB").toBytes(), BufferType.OFF_HEAP);
    }

    protected long minBlockBytes(File file)
    {
        return new DataStorageSpec.IntKibibytesBound("256KiB").toBytes();
    }

    protected static BufferPoolMXBean directBufferPool()
    {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
            if (pool.getName().equals("direct"))
                return pool;
        throw new IllegalStateException("Direct buffer pool not found");
    }

    @Test
    public void testLastBlockReads()
    {
        qt().withFixedSeed(seed).forAll(lastBlockReads())
            .checkAssert(this::testReads);
    }

    @Test
    public void testReadsLikeChannelProxy()
    {
        qt().withFixedSeed(seed).forAll(reads())
            .checkAssert(this::testReads);
    }

    protected void testReads(InputData propertyInputs)
    {
        try (ChannelProxy channel = new ChannelProxy(propertyInputs.file);
             ThreadLocalReadAheadBuffer tlrab = new ThreadLocalReadAheadBuffer(channel, new DataStorageSpec.IntKibibytesBound("256KiB").toBytes(), BufferType.OFF_HEAP); )
        {
            for (Pair<Long, Integer> read : propertyInputs.positionsAndLengths)
            {
                testRead(read, channel, tlrab);
            }
        }
    }

    protected static void testRead(Pair<Long, Integer> read, ChannelProxy bufferedChannel, ThreadLocalReadAheadBuffer tlrab)
    {
        int readSize = Math.min(read.right, (int) (bufferedChannel.size() - read.left));
        ByteBuffer buf1 = ByteBuffer.allocate(readSize);
        bufferedChannel.read(buf1, read.left);

        ByteBuffer buf2 = ByteBuffer.allocate(readSize);
        try
        {
            int copied = 0;
            while (copied < readSize)
            {
                tlrab.fill(read.left + copied);
                int leftToRead = readSize - copied;
                if (tlrab.remaining() >= leftToRead)
                    copied += tlrab.read(buf2, leftToRead);
                else
                    copied += tlrab.read(buf2, tlrab.remaining());
            }
        }
        catch (CorruptSSTableException | CorruptBlockException e)
        {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(buf1, buf2);
    }

    protected Gen<InputData> reads()
    {
        return arbitrary().pick(List.of(files))
                          .flatMap((file) ->
                                   lists().of(longs().between(0, fileSize(file)).zip(integers().between(1, 100), Pair::create))
                                          .ofSizeBetween(5, 10)
                                          .map(positionsAndLengths -> new InputData(file, positionsAndLengths)));
    }

    protected Gen<InputData> lastBlockReads()
    {
        int blockSize = new DataStorageSpec.IntKibibytesBound("256KiB").toBytes();
        return arbitrary().pick(List.of(files))
                          .flatMap((file) ->
                                   lists().of(longs().between(max(0, fileSize(file) - blockSize), fileSize(file)).zip(integers().between(1, 100), Pair::create))
                                          .ofSizeBetween(5, 10)
                                          .map(positionsAndLengths -> new InputData(file, positionsAndLengths)));
    }

    // need this because generators don't handle the IOException
    private long fileSize(File file)
    {
        try
        {
            return Files.size(file.toPath());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static class InputData
    {

        protected final File file;
        protected final List<Pair<Long, Integer>> positionsAndLengths;

        public InputData(File file, List<Pair<Long, Integer>> positionsAndLengths)
        {
            this.file = file;
            this.positionsAndLengths = positionsAndLengths;
        }
    }

    private static File writeFile(int seed, int length)
    {
        String fileName = "data+" + length + ".bin";

        byte[] dataChunk = new byte[4096 * 8];
        java.util.Random random = new Random(seed);
        int writtenData = 0;

        File file = new File(JAVA_IO_TMPDIR.getString(), fileName);
        try (FileOutputStream fos = new FileOutputStream(file.toJavaIOFile()))
        {
            while (writtenData < length)
            {
                random.nextBytes(dataChunk);
                int toWrite = Math.min((length - writtenData), dataChunk.length);
                fos.write(dataChunk, 0, toWrite);
                writtenData += toWrite;
            }
            fos.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return file;
    }
}
