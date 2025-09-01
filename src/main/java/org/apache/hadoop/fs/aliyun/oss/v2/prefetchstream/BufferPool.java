/**
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
package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;

import static org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Validate.checkNotNegative;
import static org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Validate.checkState;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkNotNull;

public class BufferPool implements Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

    private final int size;
    private final int bufferSize;
    
    private BoundedResourcePool<ByteBuffer> pool;
    private Map<BufferData, ByteBuffer> allocated;

    public BufferPool(int size, int bufferSize) {
        Validate.checkPositiveInteger(size, "size");
        Validate.checkPositiveInteger(bufferSize, "bufferSize");

        this.size = size;
        this.bufferSize = bufferSize;
        this.allocated = new IdentityHashMap<BufferData, ByteBuffer>();
        this.pool = new BoundedResourcePool<ByteBuffer>(size) {
            @Override
            public ByteBuffer createNew() {
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                return buffer;
            }
        };
    }

    public List<BufferData> getAll() {
        synchronized (allocated) {
            return Collections.unmodifiableList(new ArrayList<>(allocated.keySet()));
        }
    }

    public synchronized BufferData acquire(int blockNumber) {
        BufferData data;
        final int maxRetryDelayMs = 600 * 1000;
        final int statusUpdateDelayMs = 120 * 1000;
        Retryer retryer = new Retryer(10, maxRetryDelayMs, statusUpdateDelayMs);

        do {
            if (retryer.updateStatus()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("waiting to acquire block: {}", blockNumber);
                    LOG.debug("state = {}", this);
                }
                releaseReadyBlock(blockNumber);
            }
            data = tryAcquire(blockNumber);
        }
        while ((data == null) && retryer.continueRetry());

        if (data != null) {
            return data;
        } else {
            String message =
                    String.format("Wait failed for acquire(%d)", blockNumber);
            throw new IllegalStateException(message);
        }
    }

    public synchronized BufferData tryAcquire(int blockNumber) {
        return acquireHelper(blockNumber, false);
    }

    private synchronized BufferData acquireHelper(int blockNumber, boolean canBlock) {
        checkNotNegative(blockNumber, "blockNumber");

        releaseDoneBlocks();

        BufferData data = find(blockNumber);
        if (data != null) {
            return data;
        }

        ByteBuffer buffer = canBlock ? pool.acquire() : pool.tryAcquire();
        if (buffer == null) {
            return null;
        }

        buffer.clear();
        data = new BufferData(blockNumber, buffer.duplicate());

        synchronized (allocated) {
            checkState(find(blockNumber) == null, "buffer data already exists");

            allocated.put(data, buffer);
        }

        return data;
    }

    private synchronized void releaseDoneBlocks() {
        for (BufferData data : getAll()) {
            if (data.stateEqualsOneOf(BufferData.State.DONE)) {
                release(data);
            }
        }
    }

    private synchronized void releaseReadyBlock(int blockNumber) {
        BufferData releaseTarget = null;
        for (BufferData data : getAll()) {
            if (data.stateEqualsOneOf(BufferData.State.READY)) {
                if (releaseTarget == null) {
                    releaseTarget = data;
                } else {
                    if (distance(data, blockNumber) > distance(releaseTarget, blockNumber)) {
                        releaseTarget = data;
                    }
                }
            }
        }

        if (releaseTarget != null) {
            LOG.warn("releasing 'ready' block: {}", releaseTarget);
            releaseTarget.setDone();
        }
    }

    private int distance(BufferData data, int blockNumber) {
        return Math.abs(data.getBlockNumber() - blockNumber);
    }

    public synchronized void release(BufferData data) {
        checkNotNull(data, "data");

        synchronized (data) {
            checkArgument(canRelease(data), String.format("Unable to release buffer: %s", data));

            ByteBuffer buffer = allocated.get(data);
            if (buffer == null) {
                return;
            }
            buffer.clear();
            pool.release(buffer);
            allocated.remove(data);
        }

        releaseDoneBlocks();
    }

    @Override
    public synchronized void close() {
        for (BufferData data : getAll()) {
            Future<Void> actionFuture = data.getActionFuture();
            if (actionFuture != null) {
                actionFuture.cancel(true);
            }
        }

        int currentPoolSize = pool.numCreated();

        pool.close();
        pool = null;

        allocated.clear();
        allocated = null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(pool.toString());
        sb.append("\n");
        List<BufferData> allData = new ArrayList<>(getAll());
        Collections.sort(allData, (d1, d2) -> d1.getBlockNumber() - d2.getBlockNumber());
        for (BufferData data : allData) {
            sb.append(data.toString());
            sb.append("\n");
        }

        return sb.toString();
    }

    public synchronized int numCreated() {
        return pool.numCreated();
    }

    public synchronized int numAvailable() {
        releaseDoneBlocks();
        return pool.numAvailable();
    }

    private BufferData find(int blockNumber) {
        synchronized (allocated) {
            for (BufferData data : allocated.keySet()) {
                if ((data.getBlockNumber() == blockNumber) && !data.stateEqualsOneOf(BufferData.State.DONE)) {
                    return data;
                }
            }
        }

        return null;
    }

    private boolean canRelease(BufferData data) {
        return data.stateEqualsOneOf(BufferData.State.DONE, BufferData.State.READY);
    }
}
