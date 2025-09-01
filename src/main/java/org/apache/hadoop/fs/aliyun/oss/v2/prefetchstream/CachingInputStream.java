/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.DEFAULT_PREFETCH_MAX_BLOCKS_COUNT;
import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.PREFETCH_MAX_BLOCKS_COUNT;

/**
 * Provides an {@code InputStream} that allows reading from an oss file.
 * Prefetched blocks are cached to local disk if a seek away from the
 * current block is issued.
 */
public class CachingInputStream extends RemoteInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(
      CachingInputStream.class);

  private final int numBlocksToPrefetch;

  private final BlockManager blockManager;
  private final OSSPerformanceStatistics statistics;


  public CachingInputStream(
          ReadOpContext context,
          ObjectAttributes ObjectAttributes,
          OssManager client,
          Configuration conf,
          LocalDirAllocator localDirAllocator,
          OSSPerformanceStatistics streamStatistics) {
    super(context, ObjectAttributes, client);

    this.numBlocksToPrefetch = this.getContext().getPrefetchBlockCount();
    this.statistics = streamStatistics;
    int bufferPoolSize = this.numBlocksToPrefetch + 1;
    BlockManagerParameters blockManagerParamsBuilder =
        new BlockManagerParameters()
            .withFuturePool(this.getContext().getFuturePool())
            .withBlockData(this.getBlockData())
            .withBufferPoolSize(bufferPoolSize)
            .withConf(conf)
            .withLocalDirAllocator(localDirAllocator)
            .withMaxBlocksCount(
                conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT))
                .withStatistics(streamStatistics);
    this.blockManager = this.createBlockManager(blockManagerParamsBuilder, this.getReader());
    int fileSize = (int) ObjectAttributes.getLen();
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
  protected boolean ensureCurrentBuffer() throws IOException {
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

    if (filePosition.isValid()) {
      if (filePosition.bufferFullyRead()) {
        blockManager.release(filePosition.data());
      } else {
        blockManager.requestCaching(filePosition.data());
      }
      filePosition.invalidate();
    }

    int prefetchCount;
    if (outOfOrderRead) {
      LOG.debug("lazy-seek({})", getOffsetStr(readPos));
      blockManager.cancelPrefetches();

      prefetchCount = 1;
    } else {
      prefetchCount = numBlocksToPrefetch;
    }

    int toBlockNumber = getBlockData().getBlockNumber(readPos);
    long startOffset = getBlockData().getStartOffset(toBlockNumber);

    for (int i = 1; i <= prefetchCount; i++) {
      int b = toBlockNumber + i;
      if (b < getBlockData().getNumBlocks()) {
        blockManager.requestPrefetch(b,getObjectAttributes());
      }
    }

    BufferData data = blockManager.get(toBlockNumber,getObjectAttributes());

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

  protected BlockManager createBlockManager(
      @Nonnull final BlockManagerParameters blockManagerParameters,
      final RemoteObjectReader reader) {
    return new OSSCachingBlockManager(blockManagerParameters, reader);
  }
}
