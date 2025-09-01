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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Validate.checkNotNegative;
import static org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Validate.checkNotNull;

public abstract class BlockManager implements Closeable {

    private final PrefetchBlockData blockData;
    public BlockManager(PrefetchBlockData blockData) {
        checkNotNull(blockData, "blockData");

        this.blockData = blockData;
    }

    public PrefetchBlockData getBlockData() {
        return blockData;
    }

    public BufferData get(int blockNumber , ObjectAttributes objectAttributes) throws IOException {
        checkNotNegative(blockNumber, "blockNumber");

        int size = blockData.getSize(blockNumber);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        long startOffset = blockData.getStartOffset(blockNumber);
        read(buffer, startOffset, size, objectAttributes);
        buffer.flip();
        return new BufferData(blockNumber, buffer);
    }

    public abstract int read(ByteBuffer buffer, long startOffset, int size, ObjectAttributes objectAttributes) throws IOException;

    public void release(BufferData data) {
        checkNotNull(data, "data");

        // Do nothing because we allocate a new buffer each time.
    }

    public void requestPrefetch(int blockNumber, ObjectAttributes objectAttributes) {
        checkNotNegative(blockNumber, "blockNumber");

        // Do nothing because we do not support prefetches.
    }

    public void cancelPrefetches() {
        // Do nothing because we do not support prefetches.
    }

    public void requestCaching(BufferData data) {
        // Do nothing because we do not support caching.
    }

    @Override
    public void close() {
    }
}
