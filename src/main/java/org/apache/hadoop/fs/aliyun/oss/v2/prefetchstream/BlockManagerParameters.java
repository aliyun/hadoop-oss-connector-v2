/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;

public final class BlockManagerParameters {

    private ExecutorServiceFuturePool futurePool;

    private PrefetchBlockData blockData;

    private int bufferPoolSize;

    private Configuration conf;

    private LocalDirAllocator localDirAllocator;

    private int maxDiskBlocksCount;
    private OSSPerformanceStatistics statistics;

    private String streamUuid;

    public ExecutorServiceFuturePool getFuturePool() {
        return futurePool;
    }

    public PrefetchBlockData getBlockData() {
        return blockData;
    }

    public String getStreamUuid() {
         return streamUuid;
    }


    public int getBufferPoolSize() {
        return bufferPoolSize;
    }

    public Configuration getConf() {
        return conf;
    }

    public LocalDirAllocator getLocalDirAllocator() {
        return localDirAllocator;
    }

    public int getMaxDiskBlocksCount() {
        return maxDiskBlocksCount;
    }

    public BlockManagerParameters withFuturePool(
            final ExecutorServiceFuturePool pool) {
        this.futurePool = pool;
        return this;
    }

    public BlockManagerParameters withBlockData(
            final PrefetchBlockData data) {
        this.blockData = data;
        return this;
    }

    public BlockManagerParameters withBufferPoolSize(
            final int poolSize) {
        this.bufferPoolSize = poolSize;
        return this;
    }

    public BlockManagerParameters withConf(
            final Configuration configuration) {
        this.conf = configuration;
        return this;
    }

    public BlockManagerParameters withLocalDirAllocator(
            final LocalDirAllocator dirAllocator) {
        this.localDirAllocator = dirAllocator;
        return this;
    }

    public BlockManagerParameters withMaxDiskBlocksCount(
            final int blocksCount) {
        this.maxDiskBlocksCount = blocksCount;
        return this;
    }

    public BlockManagerParameters withStatistics(OSSPerformanceStatistics streamStatistics) {
        this.statistics = streamStatistics;
        return this;
    }

    public BlockManagerParameters withStreamUuid(String uuid) {
        this.streamUuid = uuid;
        return this;
    }
}

