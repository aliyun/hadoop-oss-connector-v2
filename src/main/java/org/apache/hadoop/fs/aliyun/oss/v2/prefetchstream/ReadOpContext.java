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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Preconditions;

import static java.util.Objects.requireNonNull;

/**
 * Read-specific operation context struct.
 */
public class ReadOpContext {

  /**
   * Path of read.
   */
  private final Path path;

  /**
   * Initial input policy of the stream.
   */
  private InputPolicy inputPolicy;

  /**
   * How to detect and deal with the object being updated during read.
   */
  private ChangeDetectionPolicy changeDetectionPolicy;

  /**
   * Readahead for GET operations/skip, etc.
   */
  private long readahead;

  /**
   * Threshold for stream reads to switch to
   * asynchronous draining.
   */
  private long asyncDrainThreshold;

  private ExecutorServiceFuturePool futurePool;

  // Size in bytes of a single prefetch block.
  private final int prefetchBlockSize;

  // Size of prefetch queue (in number of blocks).
  private final int prefetchBlockCount;


  //Threshold for small file
  private final int smallFileThreshold;
  private int prefetchNumAfterSeek;
  private int prefetchThreshold;
  private int bigIoThreshold;
  private int ossMergeSize;
  private final int bigIoPrefetchSize;
  private int amplificationFactor;


  public ReadOpContext(
          final Path path,
          FileStatus dstFileStatus,
          ExecutorServiceFuturePool futurePool,
          int prefetchBlockSize,
          int prefetchBlockCount,
          int smallFileThreshold,
          int prefetchNumAfterSeek,
          int prefetchThreshold,
          int bigIoThreshold,
          int ossMergeSize,
          int bigIoPrefetchSize,
          int amplificationFactor) {

    this.path = requireNonNull(path);
    this.futurePool = futurePool;
    Preconditions.checkArgument(
        prefetchBlockSize > 0, "invalid prefetchBlockSize %d", prefetchBlockSize);
    this.prefetchBlockSize = prefetchBlockSize;
    Preconditions.checkArgument(
        prefetchBlockCount >= 0, "invalid prefetchBlockCount %d", prefetchBlockCount);
    this.prefetchBlockCount = prefetchBlockCount;
    this.smallFileThreshold = smallFileThreshold;
    this.prefetchNumAfterSeek = prefetchNumAfterSeek;
    this.prefetchThreshold = prefetchThreshold;
    this.bigIoThreshold = bigIoThreshold;
    this.ossMergeSize = ossMergeSize;
    this.bigIoPrefetchSize = bigIoPrefetchSize;
    this.amplificationFactor = amplificationFactor;
  }

  /**
   * validate the context.
   * @return a read operation context ready for use.
   */
  public ReadOpContext build() {
    requireNonNull(changeDetectionPolicy, "changeDetectionPolicy");
    requireNonNull(inputPolicy, "inputPolicy");
    Preconditions.checkArgument(readahead >= 0,
        "invalid readahead %d", readahead);
    Preconditions.checkArgument(asyncDrainThreshold >= 0,
        "invalid drainThreshold %d", asyncDrainThreshold);

    return this;
  }

  /**
   * Get invoker to use for read operations.
   * @return invoker to use for read codepaths
   */
//  public Invoker getReadInvoker() {
//    return invoker;
//  }

  /**
   * Get the path of this read.
   * @return path.
   */
  public Path getPath() {
    return path;
  }

  /**
   * Get the IO policy.
   * @return the initial input policy.
   */
  public InputPolicy getInputPolicy() {
    return inputPolicy;
  }

  public ChangeDetectionPolicy getChangeDetectionPolicy() {
    return changeDetectionPolicy;
  }

  /**
   * Get the readahead for this operation.
   * @return a value {@literal >=} 0
   */
  public long getReadahead() {
    return readahead;
  }

  public int getSmallFileThreshold() {
    return smallFileThreshold;
  }

  /**
   * Get the audit which was active when the file was opened.
   * @return active span
   */
//  public AuditSpan getAuditSpan() {
//    return auditSpan;
//  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ReadOpContext withInputPolicy(final InputPolicy value) {
    inputPolicy = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ReadOpContext withChangeDetectionPolicy(
      final ChangeDetectionPolicy value) {
    changeDetectionPolicy = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ReadOpContext withReadahead(final long value) {
    readahead = value;
    return this;
  }



  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ReadOpContext withAsyncDrainThreshold(final long value) {
    asyncDrainThreshold = value;
    return this;
  }

  public long getAsyncDrainThreshold() {
    return asyncDrainThreshold;
  }

  /**
   * Get Vectored IO context for this this read op.
   * @return vectored IO context.
   */
//  public VectoredIOContext getVectoredIOContext() {
//    return vectoredIOContext;
//  }


  /**
   * Gets the {@code ExecutorServiceFuturePool} used for asynchronous prefetches.
   *
   * @return the {@code ExecutorServiceFuturePool} used for asynchronous prefetches.
   */
  public ExecutorServiceFuturePool getFuturePool() {
    return this.futurePool;
  }

  /**
   * Gets the size in bytes of a single prefetch block.
   *
   * @return the size in bytes of a single prefetch block.
   */
  public int getPrefetchBlockSize() {
    return this.prefetchBlockSize;
  }

  /**
   * Gets the size of prefetch queue (in number of blocks).
   *
   * @return the size of prefetch queue (in number of blocks).
   */
  public int getPrefetchBlockCount() {
    return this.prefetchBlockCount;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ReadOpContext{");
    sb.append("path=").append(path);
    sb.append(", inputPolicy=").append(inputPolicy);
    sb.append(", readahead=").append(readahead);
    sb.append(", changeDetectionPolicy=").append(changeDetectionPolicy);
    sb.append('}');
    return sb.toString();
  }

  public int getPrefetchNumAfterSeek() {
    return prefetchNumAfterSeek;
  }

  public int getPrefetchThreshold() {
    return prefetchThreshold;
  }

  public int getBigIoThreshold() {
    return bigIoThreshold;
  }

  public int getOssMergeSize() {
    return ossMergeSize;
  }

    public int getBigIoPrefetchSize() {
        return bigIoPrefetchSize;
    }

    public int getAmplificationFactor() {
    return amplificationFactor;
    }
}
