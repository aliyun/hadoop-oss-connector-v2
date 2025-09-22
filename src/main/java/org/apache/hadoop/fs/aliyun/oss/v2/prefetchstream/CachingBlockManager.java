/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.BlockLogContext;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.MergeGetLogContext;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.ReadAheadLogContext;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.RemoteLogContext;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.StreamUtils.cleanupWithLogger;
import static org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Validate.checkNotNegative;

/**
 * Provides read access to the underlying file one block at a time.
 * Improve read performance by prefetching and locall caching blocks.
 */
public abstract class CachingBlockManager extends BlockManager {
    private static final Logger LOG = LoggerFactory.getLogger(CachingBlockManager.class);
    private static final int TIMEOUT_MINUTES = 60;

    /**
     * Asynchronous tasks are performed in this pool.
     */
    private final ExecutorServiceFuturePool futurePool;
    private final String steamUuid;

    /**
     * Pool of shared ByteBuffer instances.
     */
    private BufferPool bufferPool;

    /**
     * Size of the in-memory cache in terms of number of blocks.
     * Total memory consumption is up to bufferPoolSize * blockSize.
     */
    private final int bufferPoolSize;

    /**
     * Local block cache.
     */
    private BlockCache cache;

    /**
     * Error counts. For testing purposes.
     */
    private final AtomicInteger numCachingErrors;
    private final AtomicInteger numReadErrors;

    /**
     * Operations performed by this block manager.
     */
    private final BlockOperations ops;

    private boolean closed;

    /**
     * If a single caching operation takes more than this time (in seconds),
     * we disable caching to prevent further perf degradation due to caching.
     */
    private static final int SLOW_CACHING_THRESHOLD = 5;

    /**
     * Once set to true, any further caching requests will be ignored.
     */
    private final AtomicBoolean cachingDisabled;


    private final Configuration conf;

    private final LocalDirAllocator localDirAllocator;

    /**
     * Constructs an instance of a {@code CachingBlockManager}.
     *
     * @param blockManagerParameters params for block manager.
     * @throws IllegalArgumentException if bufferPoolSize is zero or negative.
     */
    public CachingBlockManager(@Nonnull final BlockManagerParameters blockManagerParameters) {
        super(blockManagerParameters.getBlockData());

        Validate.checkPositiveInteger(blockManagerParameters.getBufferPoolSize(), "bufferPoolSize");

        this.futurePool = requireNonNull(blockManagerParameters.getFuturePool());
        this.bufferPoolSize = blockManagerParameters.getBufferPoolSize();
        this.numCachingErrors = new AtomicInteger();
        this.numReadErrors = new AtomicInteger();
        this.cachingDisabled = new AtomicBoolean();
        this.steamUuid = blockManagerParameters.getStreamUuid();

        this.conf = requireNonNull(blockManagerParameters.getConf());

        if (this.getBlockData().getFileSize() > 0) {
            this.bufferPool = new BufferPool(bufferPoolSize, this.getBlockData().getBlockSize());
            LOG.info("AliyunOSS  createCache: blocks {} size {}", blockManagerParameters.getMaxDiskBlocksCount(), this.getBlockData().getFileSize());
            this.cache = this.createCache(steamUuid, blockManagerParameters.getMaxDiskBlocksCount());
        }

        this.ops = new BlockOperations();
        this.ops.setDebug(false);
        this.localDirAllocator = blockManagerParameters.getLocalDirAllocator();
    }


    public BufferData get(int toBlockNumber, final long startOffset, int endBlockNumber, ObjectAttributes objectAttributes, ReadAheadLogContext readAheadLogContext) throws IOException {
        final int maxRetryDelayMs = 120 * 1000;
        final int statusUpdateDelayMs = 120 * 1000;
        Retryer retryer = new Retryer(10, maxRetryDelayMs, statusUpdateDelayMs);
        boolean done;
        List<BufferData> dataList = new ArrayList<BufferData>();
        MergeGetLogContext mergeGetLogContext = new MergeGetLogContext(readAheadLogContext);
        BufferData data;
//        StringBuilder errinfo = new StringBuilder();


        do {
            mergeGetLogContext.increaseRetryTime();
            if (closed) {
                throw new IOException("this stream is already closed");
            }

            data = bufferPool.acquire(toBlockNumber);
            BufferData.State firstState = data.getState();
            LOG.debug("acquire block {}: {}", data.getBlockNumber(), firstState);
            mergeGetLogContext.setFirstState(firstState);


            if (!checkBlockWriteAvailbale(data)) {
                BlockLogContext blockLogContext = new BlockLogContext(mergeGetLogContext);
                blockLogContext.setType("current");
                blockLogContext.setToBlockNumber(toBlockNumber);

                done = getInternal(data, objectAttributes, blockLogContext);
                mergeGetLogContext.setLastError(blockLogContext.getLastError());
                mergeGetLogContext.setRestryRes(true, 1, done, mergeGetLogContext.getCurrentTime());
            }
            else {
                if (shouldReadFromCache(data)) {
                    //getFromCache
                    if (cache.containsBlock(toBlockNumber)) {
                        if (cache.tryGet(toBlockNumber, data.getBuffer())) {
                            data.setReady(BufferData.State.BLANK);
                            done=true;
                            break;
                        }
                    }
                }

                dataList.add(data);
                for (int blockNumber = toBlockNumber + 1; blockNumber <= endBlockNumber; blockNumber++) {
                    if (bufferPool.numAvailable() <= 0) {
                        break;
                    }
                    BufferData tempData;
                    tempData = bufferPool.tryAcquire(blockNumber);
                    if (tempData == null) {
                        break;
                    }
                    if (checkBlockWriteAvailbale(tempData)) {
                        dataList.add(tempData);
                    }
                }
                mergeGetLogContext.setDataListSize(dataList.size());
                done = getInternal(dataList, objectAttributes, mergeGetLogContext);
                mergeGetLogContext.setRestryRes(false, dataList.size(), done, mergeGetLogContext.getCurrentTime());

                if (retryer.updateStatus()) {
                    LOG.warn("waiting to get toBlockNumber: {} endBlockNumber {}", toBlockNumber, endBlockNumber);
                    LOG.info("state = {}", this.toString());
                }
            }
        }
        while (!done && retryer.continueRetry());

        if (done) {
            return data;
        } else {
            String message = String.format("Wait failed for get(%d - %d) startOffset(%d) " +
                            "bufferPoolsize(%d) bufferPool.numAvailable(%d) objectAttributes(%s) blockSize(%d) errinfo(%s)",
                    toBlockNumber, endBlockNumber, startOffset, bufferPoolSize, bufferPool.numAvailable()
                    , objectAttributes.toString(), getBlockData().getBlockSize());
            throw new IllegalStateException(message);
        }
    }

    boolean checkBlockCached(BufferData data) {
        synchronized (data) {
            if (data.stateEqualsOneOf(
                    BufferData.State.CACHING,
                    BufferData.State.READY,
                    BufferData.State.PREFETCHING)) {
                return true;
            }

            return false;
        }
    }


    boolean checkBlockWriteAvailbale(BufferData data) {
        synchronized (data) {
            if (data.stateEqualsOneOf(
                    BufferData.State.PREFETCHING,
                    BufferData.State.CACHING,
                    BufferData.State.READY,
                    BufferData.State.DONE)) {
                return false;
            }
            return true;
        }
    }

    boolean shouldReadFromCache(BufferData data) {
        synchronized (data) {
            if (data.stateEqualsOneOf(
                    BufferData.State.BLANK)) {
                return true;
            }
            return false;
        }
    }


    int getAllSize(List<BufferData> realDataList) {
        long size = realDataList.stream().mapToLong(data -> getBlockData().getSize(data.getBlockNumber())).sum();
        Preconditions.checkArgument(
                size <= Integer.MAX_VALUE, "getAllSize invalid %d", size);
        return (int) size;
    }

    private boolean getInternal(List<BufferData> dataList, ObjectAttributes objectAttributes, RemoteLogContext remoteLogContext) throws IOException {
        List<BufferData> realDataList = new ArrayList<BufferData>();
        for (BufferData data : dataList) {
            synchronized (data) {
                if (!checkBlockWriteAvailbale(data)) {
                    break;
                }
                data.throwIfStateIncorrect(BufferData.State.BLANK);
                data.updateState(BufferData.State.PREFETCHING, BufferData.State.BLANK);
                realDataList.add(data);
            }
        }

        //realDataList范围合并为一个大的BufferData
        long start = (long) realDataList.get(0).getBlockNumber() * (long) getBlockData().getBlockSize();
        int size = getAllSize(realDataList);

//        LOG.error("start {} blocknum {} blocksize {}", start, realDataList.get(0).getBlockNumber(), getBlockData().getBlockSize());
        ByteBuffer newBuffer = ByteBuffer.allocate(size);
//        BufferData newData = new BufferData(0,newBuffer);

        //使用OSSManager查询数据
//        ossManager.getObject(objectAttributes.getBucket(), objectAttributes.getKey(), newData.getBuffer(), objectAttributes.getVersionId());

//        read(newData, objectAttributes);
        BlockLogContext blockLogContext = new BlockLogContext(remoteLogContext);
        blockLogContext.setType("ME");
        blockLogContext.setToBlockNumber(realDataList.get(0).getBlockNumber());
        blockLogContext.setEndBlockNumber(realDataList.get(realDataList.size() - 1).getBlockNumber());
        blockLogContext.setIsEndBlock(getBlockData().isLastBlock(realDataList.get(realDataList.size() - 1).getBlockNumber()));
        read(newBuffer, start, size, objectAttributes, blockLogContext);
        newBuffer.flip();

        // 将数据newBuffer的数据复制到realDataList[0],realDataList[1]...中
//        for (int i = 0; i < realDataList.size(); i++) {
//            BufferData data = realDataList.get(i);
//            synchronized (data) {
//                //copy BufferData data to newBuffer分为realDataList的 数据
//                data.getBuffer().put(newBuffer);
//                data.getBuffer().flip();
//                data.updateState(BufferData.State.READY);
//
//            }
//        }
        splitBuffer(newBuffer, realDataList);


        return true;
    }

    /**
     * 拆分源 ByteBuffer 数据到目标 List 中的多个 BufferData
     *
     * @param newBuffer    源 ByteBuffer（含数据）
     * @param realDataList 目标列表，用于存放拆分后的 BufferData 实例
     */
    private void splitBuffer(ByteBuffer newBuffer, List<BufferData> realDataList) {
        if (realDataList == null || realDataList.isEmpty()) {
            return;
        }

        for (BufferData data : realDataList) {
            synchronized (data) {
                ByteBuffer dataBuffer = data.getBuffer();
                int remaining = Math.min(newBuffer.remaining(), dataBuffer.remaining());

                // 从源 buffer 中读取数据到目标 buffer
                if (remaining > 0) {
                    // 创建一个限制大小的视图来复制数据
                    ByteBuffer slice = newBuffer.slice();
                    slice.limit(remaining);
                    dataBuffer.put(slice);
                    dataBuffer.flip();

                    // 更新源 buffer 的位置
                    newBuffer.position(newBuffer.position() + remaining);
                }

                // 更新状态为 READY
//                data.updateState(BufferData.State.READY);
                data.setReady(BufferData.State.PREFETCHING);

            }
        }
        newBuffer.clear();
    }


    /**
     * Gets the block having the given {@code blockNumber}.
     *
     * @throws IllegalArgumentException if blockNumber is negative.
     */
    @Override
    public BufferData get(int blockNumber, ObjectAttributes objectAttributes, RemoteLogContext remoteLogContext) throws IOException {
        checkNotNegative(blockNumber, "blockNumber");

        BufferData data;
        final int maxRetryDelayMs = 120 * 1000;
        final int statusUpdateDelayMs = 120 * 1000;
        Retryer retryer = new Retryer(10, maxRetryDelayMs, statusUpdateDelayMs);
        boolean done;

        do {
            if (closed) {
                throw new IOException("this stream is already closed");
            }

            data = bufferPool.acquire(blockNumber);
            done = getInternal(data, objectAttributes, remoteLogContext);

            if (retryer.updateStatus()) {
                LOG.warn("waiting to get block: {}", blockNumber);
                LOG.info("state = {}", this.toString());
            }
        }
        while (!done && retryer.continueRetry());


        if (done) {
            return data;
        } else {
            String message = String.format("Wait failed for get(%d)", blockNumber);
            throw new IllegalStateException(message);
        }
    }

    private boolean getInternal(BufferData data, ObjectAttributes objectAttributes, RemoteLogContext blockLogContext) throws IOException {
        Validate.checkNotNull(data, "data");

        // Opportunistic check without locking.
        if (data.stateEqualsOneOf(
                BufferData.State.PREFETCHING,
                BufferData.State.CACHING,
                BufferData.State.DONE)) {
            blockLogContext.setLastError("r1-" + data.getState());
            return false;
        }

        synchronized (data) {
            // Reconfirm state after locking.
            if (data.stateEqualsOneOf(
                    BufferData.State.PREFETCHING,
                    BufferData.State.CACHING,
                    BufferData.State.DONE)) {
                blockLogContext.setLastError("r2-" + data.getState());
                return false;
            }

            int blockNumber = data.getBlockNumber();
            if (data.getState() == BufferData.State.READY) {
                BlockOperations.Operation op = ops.getPrefetched(blockNumber);
                ops.end(op);
                return true;
            }

            data.throwIfStateIncorrect(BufferData.State.BLANK);
            read(data, objectAttributes, blockLogContext);
            return true;
        }
    }

    /**
     * Releases resources allocated to the given block.
     *
     * @throws IllegalArgumentException if data is null.
     */
    @Override
    public void release(BufferData data) {
        if (closed) {
            return;
        }

        Validate.checkNotNull(data, "data");

        BlockOperations.Operation op = ops.release(data.getBlockNumber());
        bufferPool.release(data);
        ops.end(op);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;

        final BlockOperations.Operation op = ops.close();

        // Cancel any prefetches in progress.
        cancelPrefetches();

        cleanupWithLogger(LOG, cache);

        ops.end(op);
        LOG.debug(ops.getSummary(false));

        bufferPool.close();
        bufferPool = null;
    }

    /**
     * Requests optional prefetching of the given block.
     * The block is prefetched only if we can acquire a free buffer.
     *
     * @throws IllegalArgumentException if blockNumber is negative.
     */
    @Override
    public void requestPrefetch(int blockNumber, ObjectAttributes objectAttributes, RemoteLogContext remoteLogContext) {
        checkNotNegative(blockNumber, "blockNumber");

        if (closed) {
            return;
        }

        // We initiate a prefetch only if we can acquire a buffer from the shared pool.
        BufferData data = bufferPool.tryAcquire(blockNumber);
        if (data == null) {
            LOG.debug("can't tryAcquire {}.", blockNumber);
            return;
        }

        // Opportunistic check without locking.
        if (!data.stateEqualsOneOf(BufferData.State.BLANK)) {
            // The block is ready or being prefetched/cached.
            LOG.debug(" Block {} is ready or being prefetched/cached 1.", blockNumber);
            return;
        }

        synchronized (data) {
            // Reconfirm state after locking.
            if (!data.stateEqualsOneOf(BufferData.State.BLANK)) {
                // The block is ready or being prefetched/cached.
                LOG.debug(" Block {} is ready or being prefetched/cached 2.", blockNumber);
                return;
            }

            BlockLogContext blockLogContext = new BlockLogContext(remoteLogContext);
            blockLogContext.setType("prefetch");
            blockLogContext.setToBlockNumber(blockNumber);


            BlockOperations.Operation op = ops.requestPrefetch(blockNumber);
            PrefetchTask prefetchTask = new PrefetchTask(data, this, Instant.now(), objectAttributes, blockLogContext);
            Future<Void> prefetchFuture = futurePool.executeFunction(prefetchTask);
            data.setPrefetch(prefetchFuture);
            ops.end(op);
        }
    }

    /**
     * Requests cancellation of any previously issued prefetch requests.
     */
    @Override
    public void cancelPrefetches() {
        BlockOperations.Operation op = ops.cancelPrefetches();
        for (BufferData data : bufferPool.getAll()) {
            // We add blocks being prefetched to the local cache so that the prefetch is not wasted.
            if (data.stateEqualsOneOf(BufferData.State.PREFETCHING, BufferData.State.READY)) {
                requestCaching(data);
            }
        }

        ops.end(op);
    }

    private void read(BufferData data, ObjectAttributes objectAttributes, RemoteLogContext remoteLogContext) throws IOException {
        synchronized (data) {
            try {
                readBlock(data, false, objectAttributes, remoteLogContext, BufferData.State.BLANK);
            } catch (IOException e) {
                LOG.error("error reading block {}", data.getBlockNumber(), e);
                throw e;
            }
        }
    }

    private void prefetch(BufferData data, Instant taskQueuedStartTime, ObjectAttributes objectAttributes, RemoteLogContext remoteLogContext) throws IOException {
        synchronized (data) {
            readBlock(
                    data,
                    true,
                    objectAttributes,
                    remoteLogContext, BufferData.State.PREFETCHING

            );
        }
    }

    private void readBlock(BufferData data, boolean isPrefetch, ObjectAttributes objectAttributes, RemoteLogContext blockLogContext, BufferData.State expectedState)
            throws IOException {

        if (closed) {
            return;
        }

        BlockOperations.Operation op = null;
        DurationTracker tracker = null;

        synchronized (data) {
            try {
                if (data.stateEqualsOneOf(BufferData.State.DONE, BufferData.State.READY)) {
                    // DONE  : Block was released, likely due to caching being disabled on slow perf.
                    // READY : Block was already fetched by another thread. No need to re-read.
                    return;
                }

                data.throwIfStateIncorrect(expectedState);
                int blockNumber = data.getBlockNumber();

                // Prefer reading from cache over reading from network.
                if (cache.containsBlock(blockNumber)) {
                    op = ops.getCached(blockNumber);
                    if (cache.tryGet(blockNumber, data.getBuffer())) {
                        data.setReady(expectedState);
                        return;
                    }
                }

                if (isPrefetch) {
//          tracker = prefetchingStatistics.prefetchOperationStarted();
                    op = ops.prefetch(data.getBlockNumber());
                } else {
                    op = ops.getRead(data.getBlockNumber());
                }

                long offset = getBlockData().getStartOffset(data.getBlockNumber());
                int size = getBlockData().getSize(data.getBlockNumber());
                ByteBuffer buffer = data.getBuffer();
                ((java.nio.Buffer) buffer).clear();
                read(buffer, offset, size, objectAttributes, blockLogContext);
                buffer.flip();
                data.setReady(expectedState);
            } catch (Exception e) {
                if (isPrefetch && tracker != null) {
                    tracker.failed();
                }

                numReadErrors.incrementAndGet();
                data.setDone();
                throw e;
            } finally {
                if (op != null) {
                    ops.end(op);
                }

                if (isPrefetch) {
//          prefetchingStatistics.prefetchOperationCompleted();
                    if (tracker != null) {
                        tracker.close();
                    }
                }
            }
        }
    }


    /**
     * 将 List BufferData 按照 blockSize 和 threshold 切分为多个子列表。
     *
     * @param data      待切分的数据列表
     * @param blockSize 每个 BufferData 的大小（字节数）
     * @param threshold 每个子列表的最大累计大小（字节数），达到即切分
     * @return 切分后的子列表集合
     */
    public static List<List<BufferData>> splitData(
            List<BufferData> data,
            long blockSize,
            long threshold) {

        // 计算每组最多包含多少个元素（基于固定大小）
        int itemsPerGroup = (int) (threshold / blockSize);
        List<List<BufferData>> result = new ArrayList<>();

        if (itemsPerGroup <= 0) {
//            throw new IllegalArgumentException("Threshold must be >= blockSize");
            result.add(data);

        } else {
            for (int i = 0; i < data.size(); i += itemsPerGroup) {
                int end = Math.min(i + itemsPerGroup, data.size());
                List<BufferData> subList = new ArrayList<>(data.subList(i, end));
                result.add(subList);
            }
        }

        return result;
    }

    /**
     * Read task that is submitted to the future pool.
     */
    private static class PrefetchTask implements Supplier<Void> {
        private final BufferData data;
        private final CachingBlockManager blockManager;
        private final Instant taskQueuedStartTime;
        private final ObjectAttributes objectAttributes;
        private final RemoteLogContext remoteLogContext;

        PrefetchTask(BufferData data, CachingBlockManager blockManager, Instant taskQueuedStartTime, ObjectAttributes objectAttributes, RemoteLogContext remoteLogContext) {
            this.data = data;
            this.blockManager = blockManager;
            this.taskQueuedStartTime = taskQueuedStartTime;
            this.objectAttributes = objectAttributes;
            this.remoteLogContext = remoteLogContext;
        }

        public Void get() {
            try {
                blockManager.prefetch(data, taskQueuedStartTime, objectAttributes, remoteLogContext);
            } catch (Exception e) {
                LOG.info("error prefetching block {}. {}", data.getBlockNumber(), e.getMessage());
            }
            return null;
        }
    }

    private static final BufferData.State[] EXPECTED_STATE_AT_CACHING =
            new BufferData.State[]{
                    BufferData.State.PREFETCHING, BufferData.State.READY
            };

    /**
     * Requests that the given block should be copied to the local cache.
     * The block must not be accessed by the caller after calling this method
     * because it will released asynchronously relative to the caller.
     *
     * @throws IllegalArgumentException if data is null.
     */
    @Override
    public void requestCaching(BufferData data) {
        if (closed) {
            return;
        }

        if (cachingDisabled.get() || this.cache.getMaxDiskBlocksCount() <= 0) {
            data.setDone();
            return;
        }

        Validate.checkNotNull(data, "data");

        // Opportunistic check without locking.
        if (!data.stateEqualsOneOf(EXPECTED_STATE_AT_CACHING)) {
            return;
        }

        synchronized (data) {
            // Reconfirm state after locking.
            if (!data.stateEqualsOneOf(EXPECTED_STATE_AT_CACHING)) {
                return;
            }

            if (cache.containsBlock(data.getBlockNumber())) {
                data.setDone();
                return;
            }

            BufferData.State state = data.getState();

            BlockOperations.Operation op = ops.requestCaching(data.getBlockNumber());
            Future<Void> blockFuture;
            if (state == BufferData.State.PREFETCHING) {
                blockFuture = data.getActionFuture();
            } else {
                CompletableFuture<Void> cf = new CompletableFuture<>();
                cf.complete(null);
                blockFuture = cf;
            }

            CachePutTask task =
                    new CachePutTask(data, blockFuture, this, Instant.now());
            Future<Void> actionFuture = futurePool.executeFunction(task);
            data.setCaching(actionFuture);
            ops.end(op);
        }
    }

    private void addToCacheAndRelease(BufferData data, Future<Void> blockFuture,
                                      Instant taskQueuedStartTime) {
        if (closed) {
            return;
        }

        if (cachingDisabled.get()) {
            data.setDone();
            return;
        }

        try {
            blockFuture.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (data.stateEqualsOneOf(BufferData.State.DONE)) {
                // There was an error during prefetch.
                return;
            }
        } catch (Exception e) {
            LOG.info("error waiting on blockFuture: {}. {}", data, e.getMessage());
            LOG.debug("error waiting on blockFuture: {}", data, e);
            data.setDone();
            return;
        }

        if (cachingDisabled.get()) {
            data.setDone();
            return;
        }

        BlockOperations.Operation op = null;

        synchronized (data) {
            try {
                if (data.stateEqualsOneOf(BufferData.State.DONE)) {
                    return;
                }

                if (cache.containsBlock(data.getBlockNumber())) {
                    data.setDone();
                    return;
                }

                op = ops.addToCache(data.getBlockNumber());
                ByteBuffer buffer = data.getBuffer().duplicate();
                buffer.rewind();
                cachePut(data.getBlockNumber(), buffer);
                data.setDone();
            } catch (Exception e) {
                numCachingErrors.incrementAndGet();
                LOG.info("error adding block to cache after wait: {}. {}", data, e.getMessage());
                LOG.debug("error adding block to cache after wait: {}", data, e);
                data.setDone();
            }

            if (op != null) {
                BlockOperations.End endOp = (BlockOperations.End) ops.end(op);
                if (endOp.duration() > SLOW_CACHING_THRESHOLD) {
                    if (!cachingDisabled.getAndSet(true)) {
                        String message = String.format(
                                "Caching disabled because of slow operation (%.1f sec)", endOp.duration());
                        LOG.warn(message);
                    }
                }
            }
        }
    }

    protected BlockCache createCache(String steamUuid, int maxBlocksCount) {
        return new SingleFilePerBlockCache(steamUuid, maxBlocksCount);
    }

    protected void cachePut(int blockNumber, ByteBuffer buffer) throws IOException {
        if (closed) {
            return;
        }

        cache.put(blockNumber, buffer, conf, localDirAllocator);
    }

    private static class CachePutTask implements Supplier<Void> {
        private final BufferData data;

        // Block being asynchronously fetched.
        private final Future<Void> blockFuture;

        // Block manager that manages this block.
        private final CachingBlockManager blockManager;

        private final Instant taskQueuedStartTime;

        CachePutTask(
                BufferData data,
                Future<Void> blockFuture,
                CachingBlockManager blockManager,
                Instant taskQueuedStartTime) {
            this.data = data;
            this.blockFuture = blockFuture;
            this.blockManager = blockManager;
            this.taskQueuedStartTime = taskQueuedStartTime;
        }

        @Override
        public Void get() {
            blockManager.addToCacheAndRelease(data, blockFuture, taskQueuedStartTime);
            return null;
        }
    }

    /**
     * Number of ByteBuffers available to be acquired.
     *
     * @return the number of available buffers.
     */
    public int numAvailable() {
        return bufferPool.numAvailable();
    }

    /**
     * Number of caching operations completed.
     *
     * @return the number of cached buffers.
     */
    public int numCached() {
        return cache.size();
    }

    /**
     * Number of errors encountered when caching.
     *
     * @return the number of errors encountered when caching.
     */
    public int numCachingErrors() {
        return numCachingErrors.get();
    }

    /**
     * Number of errors encountered when reading.
     *
     * @return the number of errors encountered when reading.
     */
    public int numReadErrors() {
        return numReadErrors.get();
    }

    BufferData getData(int blockNumber) {
        return bufferPool.tryAcquire(blockNumber);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("cache(");
        sb.append(cache.toString());
        sb.append("); ");

        sb.append("pool: ");
        sb.append(bufferPool.toString());

        return sb.toString();
    }
}
