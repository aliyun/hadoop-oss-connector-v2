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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;

public class OSSCachingBlockManager extends CachingBlockManager {


  private final RemoteObjectReader reader;


  public OSSCachingBlockManager(
      @Nonnull final BlockManagerParameters blockManagerParameters,
      final RemoteObjectReader reader) {

    super(blockManagerParameters);

    Validate.checkNotNull(reader, "reader");

    this.reader = reader;
  }

  protected RemoteObjectReader getReader() {
    return this.reader;
  }


  @Override
  public int read(ByteBuffer buffer, long startOffset, int size, ObjectAttributes objectAttributes)
      throws IOException {
    return this.reader.read(buffer, startOffset, size, objectAttributes);
  }

  @Override
  public synchronized void close() {
    this.reader.close();

    super.close();
  }
}
