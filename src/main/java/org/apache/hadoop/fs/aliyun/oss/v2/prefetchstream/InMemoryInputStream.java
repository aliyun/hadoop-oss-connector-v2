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


import org.apache.hadoop.fs.aliyun.oss.v2.OssManager;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class InMemoryInputStream extends RemoteInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(
      InMemoryInputStream.class);

  private ByteBuffer buffer;

  public InMemoryInputStream(
      ReadOpContext context,
      ObjectAttributes objectAttributes,
      OssManager client,
      OSSPerformanceStatistics streamStatistics) {
    super(context, objectAttributes, client);
    int fileSize = (int) objectAttributes.getLen();
    this.buffer = ByteBuffer.allocate(fileSize);
    LOG.debug("Created in-memory input stream for {} (size = {})",
        getName(), fileSize);
  }

  @Override
  protected boolean ensureCurrentBuffer() throws IOException {
    if (isClosed()) {
      return false;
    }

    if (getBlockData().getFileSize() == 0) {
      return false;
    }

    FilePosition filePosition = getFilePosition();
    if (filePosition.isValid()) {
      // Update current position (lazy seek).
      filePosition.setAbsolute(getNextReadPos());
    } else {
      // Read entire file into buffer.
      buffer.clear();
      int numBytesRead =
          getReader().read(buffer, 0, buffer.capacity(), getObjectAttributes());
      if (numBytesRead <= 0) {
        return false;
      }
      BufferData data = new BufferData(0, buffer);
      filePosition.setData(data, 0, getNextReadPos());
    }

    return filePosition.buffer().hasRemaining();
  }
}
