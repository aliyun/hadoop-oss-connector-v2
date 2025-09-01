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

import java.nio.ByteBuffer;

import static org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Validate.*;

public final class FilePosition {
  private PrefetchBlockData blockData;

  private BufferData data;
  private ByteBuffer buffer;
  private long bufferStartOffset;
  private long readStartOffset;
  private int numSingleByteReads;
  private int numBytesRead;
  private int numBufferReads;

  public FilePosition(long fileSize, int blockSize) {
    checkNotNegative(fileSize, "fileSize");
    if (fileSize == 0) {
      checkNotNegative(blockSize, "blockSize");
    } else {
      checkPositiveInteger(blockSize, "blockSize");
    }

    this.blockData = new PrefetchBlockData(fileSize, blockSize);

    // The position is valid only when a valid buffer is associated with this file.
    this.invalidate();
  }

  public void setData(BufferData bufferData,
      long startOffset,
      long readOffset) {
    checkNotNull(bufferData, "bufferData");
    checkNotNegative(startOffset, "startOffset");
    checkNotNegative(readOffset, "readOffset");
    checkWithinRange(
        readOffset,
        "readOffset",
        startOffset,
        startOffset + bufferData.getBuffer().limit());

    data = bufferData;
    buffer = bufferData.getBuffer().duplicate();
    bufferStartOffset = startOffset;
    readStartOffset = readOffset;
    setAbsolute(readOffset);

    resetReadStats();
  }

  public ByteBuffer buffer() {
    throwIfInvalidBuffer();
    return buffer;
  }

  public BufferData data() {
    throwIfInvalidBuffer();
    return data;
  }

  public long absolute() {
    throwIfInvalidBuffer();
    return bufferStartOffset + relative();
  }

  public boolean setAbsolute(long pos) {
    if (isValid() && isWithinCurrentBuffer(pos)) {
      int relativePos = (int) (pos - bufferStartOffset);
      buffer.position(relativePos);
      return true;
    } else {
      return false;
    }
  }

  public int relative() {
    throwIfInvalidBuffer();
    return buffer.position();
  }

  public boolean isWithinCurrentBuffer(long pos) {
    throwIfInvalidBuffer();
    long bufferEndOffset = bufferStartOffset + buffer.limit();
    return (pos >= bufferStartOffset) && (pos <= bufferEndOffset);
  }

  public int blockNumber() {
    throwIfInvalidBuffer();
    return blockData.getBlockNumber(bufferStartOffset);
  }

  public boolean isLastBlock() {
    return blockData.isLastBlock(blockNumber());
  }

  public boolean isValid() {
    return buffer != null;
  }

  public void invalidate() {
    buffer = null;
    bufferStartOffset = -1;
    data = null;
  }

  public long bufferStartOffset() {
    throwIfInvalidBuffer();
    return bufferStartOffset;
  }

  public boolean bufferFullyRead() {
    throwIfInvalidBuffer();
    return (bufferStartOffset == readStartOffset)
        && (relative() == buffer.limit())
        && (numBytesRead == buffer.limit());
  }

  public void incrementBytesRead(int n) {
    numBytesRead += n;
    if (n == 1) {
      numSingleByteReads++;
    } else {
      numBufferReads++;
    }
  }

  public int numBytesRead() {
    return numBytesRead;
  }

  public int numSingleByteReads() {
    return numSingleByteReads;
  }

  public int numBufferReads() {
    return numBufferReads;
  }

  private void resetReadStats() {
    numBytesRead = 0;
    numSingleByteReads = 0;
    numBufferReads = 0;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (buffer == null) {
      sb.append("currentBuffer = null");
    } else {
      int pos = buffer.position();
      int val;
      if (pos >= buffer.limit()) {
        val = -1;
      } else {
        val = buffer.get(pos);
      }
      String currentBufferState =
          String.format("%d at pos: %d, lim: %d", val, pos, buffer.limit());
      sb.append(String.format(
          "block: %d, pos: %d (CBuf: %s)%n",
          blockNumber(), absolute(),
          currentBufferState));
      sb.append("\n");
    }
    return sb.toString();
  }

  private void throwIfInvalidBuffer() {
    checkState(buffer != null, "'buffer' must not be null");
  }
}
