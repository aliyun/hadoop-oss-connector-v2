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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.aliyun.oss.v2.OssManager;

import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PrefetchingInputStream
    extends FSInputStream
    implements CanSetReadahead, StreamCapabilities, IOStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(
      PrefetchingInputStream.class);
  private RemoteInputStream inputStream;
  private long lastReadCurrentPos = 0;
  private IOStatistics ioStatistics = null;


  public PrefetchingInputStream(
          ReadOpContext context,
          ObjectAttributes objectAttributes,
          OssManager client,
          Configuration conf,
          LocalDirAllocator localDirAllocator,
          OSSPerformanceStatistics streamStatistics) {

    Validate.checkNotNull(context, "context");
    Validate.checkNotNull(objectAttributes, "objectAttributes");
    Validate.checkNotNullAndNotEmpty(objectAttributes.getBucket(),
        "objectAttributes.getBucket()");
    Validate.checkNotNullAndNotEmpty(objectAttributes.getKey(),
        "objectAttributes.getKey()");
    Validate.checkNotNegative(objectAttributes.getLen(), "objectAttributes.getLen()");
    Validate.checkNotNull(client, "client");
    Validate.checkNotNull(streamStatistics, "streamStatistics");

    long fileSize = objectAttributes.getLen();

    if (fileSize <= context.getPrefetchBlockSize()) {
      LOG.debug("Creating in memory input stream for {}", context.getPath());
      this.inputStream = new InMemoryInputStream(
              context,
              objectAttributes,
              client,
              streamStatistics);
    }
    else {


      LOG.debug("Creating in caching input stream for {}", context.getPath());
      this.inputStream = new CachingInputStream(
              context,
              objectAttributes,
              client,
              conf,
              localDirAllocator,
              streamStatistics);
    }

  }

  /**
   * Returns the number of bytes available for reading without blocking.
   *
   * @return the number of bytes available for reading without blocking.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized int available() throws IOException {
    throwIfClosed();
    return inputStream.available();
  }

  /**
   * Gets the current position. If the underlying oss input stream is closed,
   * it returns last read current position from the underlying steam. If the
   * current position was never read and the underlying input stream is closed,
   * this would return 0.
   *
   * @return the current position.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized long getPos() throws IOException {
    if (!isClosed()) {
      lastReadCurrentPos = inputStream.getPos();
    }
    return lastReadCurrentPos;
  }

  /**
   * Reads and returns one byte from this stream.
   *
   * @return the next byte from this stream.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized int read() throws IOException {
    throwIfClosed();
    return inputStream.read();
  }

  /**
   * Reads up to {@code len} bytes from this stream and copies them into
   * the given {@code buffer} starting at the given {@code offset}.
   * Returns the number of bytes actually copied in to the given buffer.
   *
   * @param buffer the buffer to copy data into.
   * @param offset data is copied starting at this offset.
   * @param len max number of bytes to copy.
   * @return the number of bytes actually copied in to the given buffer.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized int read(byte[] buffer, int offset, int len)
      throws IOException {
    throwIfClosed();
    return inputStream.read(buffer, offset, len);
  }

  /**
   * Closes this stream and releases all acquired resources.
   *
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized void close() throws IOException {
    LOG.debug("Closing PrefetchingInputStream!");

    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
      super.close();
    }
  }

  /**
   * Updates internal data such that the next read will take place at the given {@code pos}.
   *
   * @param pos new read position.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    throwIfClosed();
    inputStream.seek(pos);
  }

  /**
   * Sets the number of bytes to read ahead each time.
   *
   * @param readahead the number of bytes to read ahead each time..
   */
  @Override
  public synchronized void setReadahead(Long readahead) {
    if (!isClosed()) {
      inputStream.setReadahead(readahead);
    }
  }

  /**
   * Indicates whether the given {@code capability} is supported by this stream.
   *
   * @param capability the capability to check.
   * @return true if the given {@code capability} is supported by this stream, false otherwise.
   */
  @Override
  public boolean hasCapability(String capability) {
    if (!isClosed()) {
      return inputStream.hasCapability(capability);
    }

    return false;
  }



  /**
   * Gets the internal IO statistics.
   *
   * @return the internal IO statistics.
   */
  @Override
  public IOStatistics getIOStatistics() {
    if (!isClosed()) {
      ioStatistics = inputStream.getIOStatistics();
    }
    return ioStatistics;
  }

  protected boolean isClosed() {
    return inputStream == null;
  }

  protected void throwIfClosed() throws IOException {
    if (isClosed()) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  // Unsupported functions.

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
