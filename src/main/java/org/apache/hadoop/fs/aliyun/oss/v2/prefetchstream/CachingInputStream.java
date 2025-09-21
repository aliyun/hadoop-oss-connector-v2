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
import org.apache.hadoop.fs.aliyun.oss.v2.OssManager;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.ReadLogContext;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.StreamLogContext;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;

import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.ReadAheadLogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * Provides an {@code InputStream} that allows reading from an oss file.
 * Prefetched blocks are cached to local disk if a seek away from the
 * current block is issued.
 */
public class CachingInputStream extends RemoteInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(
            CachingInputStream.class);

    private final int numBlocksToPrefetch;
    private final int prefetchNumAfterSeek;
    private final int prefetchThreshold;
    private final int bigIoThreshold;
    private final int ossMergeSize;

    private final OSSCachingBlockManager blockManager;
    private final OSSPerformanceStatistics statistics;
    private final int maxDiskBlocksCount;
    private int bigIoPrefetchSize;
    private int amplificationFactor;



    public CachingInputStream(
            ReadOpContext context,
            ObjectAttributes ObjectAttributes,
            OssManager client,
            Configuration conf,
            LocalDirAllocator localDirAllocator,
            OSSPerformanceStatistics streamStatistics) {
        super(context, ObjectAttributes, client);
        this.logContext = new StreamLogContext();
        logContext.setType("CS");
        this.numBlocksToPrefetch = this.getContext().getPrefetchBlockCount();
        this.prefetchNumAfterSeek = this.getContext().getPrefetchNumAfterSeek();
        this.prefetchThreshold = this.getContext().getPrefetchThreshold();
        this.bigIoThreshold = this.getContext().getBigIoThreshold();
        this.ossMergeSize = this.getContext().getOssMergeSize();
        this.bigIoPrefetchSize = this.getContext().getBigIoPrefetchSize();
        this.amplificationFactor = this.getContext().getAmplificationFactor();
        this.maxDiskBlocksCount = conf.getInt(PREFETCH_MAX_DISK_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_DISK_BLOCKS_COUNT);

        this.statistics = streamStatistics;
        long fileSize = ObjectAttributes.getLen();

        int mergerSize = conf.getInt(MERGE_MAX_POOL_SIZE, DEFAULT_MERGE_MAX_POOL_SIZE);
        long maxPoolRangeSize = min(fileSize, max(mergerSize, bigIoPrefetchSize));
        int maxPoolSize = (int) (maxPoolRangeSize / getBlockData().getBlockSize());
        int mergePooSize = (int) (bigIoPrefetchSize / getBlockData().getBlockSize());
        int bufferPoolSize = this.numBlocksToPrefetch + 1 + maxPoolSize + mergePooSize;
        //create a string contain maxPoolRangeSize，maxPoolSize，numBlocksToPrefetch，numBlocksToPrefetch
        StringBuilder sb = new StringBuilder();
        sb.append("fileSize").append(":").append(fileSize)
                .append("maxPoolRangeSize:").append(maxPoolRangeSize)
                .append(",maxPoolSize:").append(maxPoolSize)
                .append(",mergePooSize:").append(mergePooSize)
                .append(",numBlocksToPrefetch:").append(numBlocksToPrefetch)
                .append(",bufferPoolSize:").append(bufferPoolSize)
                .append("amplificationFactor:").append(amplificationFactor)
                .append(" bufferPoolSize");


        Validate.checkPositiveInteger(bufferPoolSize, sb.toString());

        BlockManagerParameters blockManagerParamsBuilder =
                new BlockManagerParameters()
                        .withFuturePool(this.getContext().getFuturePool())
                        .withBlockData(this.getBlockData())
                        .withBufferPoolSize(bufferPoolSize)
                        .withConf(conf)
                        .withLocalDirAllocator(localDirAllocator)
                        .withMaxDiskBlocksCount(maxDiskBlocksCount)
                        .withStatistics(streamStatistics)
                        .withStreamUuid(logContext.getUuid());
        this.blockManager = this.createBlockManager(blockManagerParamsBuilder, this.getReader());

        logContext.setDataBlockSize(getBlockData().getBlockSize())
                .setPrefetchThreshold(prefetchThreshold);
        LOG.debug("Created caching input stream for {} (size = {})", this.getName(),
                fileSize);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Closing CacheingInputStream!");
        blockManager.close();
        super.close();
        LOG.info("closed: {}", getName());
    }

    @Override
    protected boolean ensureCurrentBuffer(int len, long loop, ReadLogContext readLogContext) throws IOException {
        if (isClosed()) {
            return false;
        }
        long readPos = getNextReadPos();
        if (!getBlockData().isValidOffset(readPos)) {
            return false;
        }

        FilePosition filePosition = getFilePosition();
        boolean outOfOrderRead = !filePosition.setAbsolute(readPos);

        if (!outOfOrderRead && filePosition.buffer().hasRemaining()) {
            return true;
        }

        ReadAheadLogContext readAheadLogContext = new ReadAheadLogContext(readLogContext);

        if (filePosition.isValid()) {
            if (filePosition.bufferFullyRead()) {
                readAheadLogContext.setRelease(filePosition.data().getBlockNumber());
                blockManager.release(filePosition.data());
            } else if (maxDiskBlocksCount > 0) {
                readAheadLogContext.setCache(filePosition.data().getBlockNumber());
                blockManager.requestCaching(filePosition.data());
            }
            filePosition.invalidate();
        }


        int prefetchCount;
        if (outOfOrderRead) {
            LOG.debug("lazy-seek({})", getOffsetStr(readPos));
            readAheadLogContext.setOutOfOrderRead();
            readAheadLogContext.cancelPrefetches();
            blockManager.cancelPrefetches();
            prefetchCount = prefetchNumAfterSeek;
        } else {
            prefetchCount = numBlocksToPrefetch;
        }

        int toBlockNumber = getBlockData().getBlockNumber(readPos);
        long startOffset = getBlockData().getStartOffset(toBlockNumber);
        BufferData data = null;

        readAheadLogContext.setToBlockNumber(toBlockNumber);
        readAheadLogContext.setReadPos(readPos);
        readAheadLogContext.setPrefetchCount(prefetchCount);
        readAheadLogContext.setLen(len);
        readAheadLogContext.setLoop(loop);

        if (getBlockData().getBlockSize() >= prefetchThreshold) {
            readAheadLogContext.setType("PR");
            for (int i = 1; i <= prefetchCount; i++) {
                int b = toBlockNumber + i;
                if (b < getBlockData().getNumBlocks()) {
                    blockManager.requestPrefetch(b, getObjectAttributes(),readAheadLogContext);
                }
            }
            LOG.debug("read model prefetch  :len {} prefetchThreshold {} blocksize {}  readPos {} toBlockNumber {} prefetchCount {}"
                    , len, prefetchThreshold, getBlockData().getBlockSize(), readPos, toBlockNumber, prefetchCount);

            data = blockManager.get(toBlockNumber, getObjectAttributes(),readAheadLogContext);
        } else {
            long fileSize = filePosition.getBlockData().getFileSize();
            long readTo = readPos + len - 1;
            long shouldReadTo = min(readTo, fileSize - 1);
            int endBlockNumber = getBlockData().getBlockNumber(shouldReadTo);

            LOG.debug("read model  readahead  :len {} prefetchThreshold {} blocksize {} fileSize {} readPos {} toBlockNumber {}  " +
                            "readTo{} shouldReadTo {} endBlockNumber {} bigIoThreshold {} prefetchThreshold {}"
                    , len, prefetchThreshold, getBlockData().getBlockSize(), fileSize, readPos,
                    toBlockNumber, readTo, shouldReadTo, endBlockNumber, bigIoThreshold, prefetchThreshold);
            readAheadLogContext.setEndBlockNumber(endBlockNumber);
            if (toBlockNumber == endBlockNumber) {
                readAheadLogContext.setType("AH1");
                data = blockManager.get(toBlockNumber, getObjectAttributes(), readAheadLogContext);
            } else {
                readAheadLogContext.setType("AH2");
                data = blockManager.get(toBlockNumber, startOffset, endBlockNumber, getObjectAttributes(),readAheadLogContext);
            }
        }

        filePosition.setData(data, startOffset, readPos);
        return true;
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "closed";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s%n", super.toString()));
        sb.append(blockManager.toString());
        return sb.toString();
    }

    protected OSSCachingBlockManager createBlockManager(
            @Nonnull final BlockManagerParameters blockManagerParameters,
            final RemoteObjectReader reader) {
        return new OSSCachingBlockManager(blockManagerParameters, reader);
    }
}
