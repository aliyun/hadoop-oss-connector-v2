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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface BlockCache extends Closeable {

  boolean containsBlock(int blockNumber);

  Iterable<Integer> blocks();

  int size();

  void get(int blockNumber, ByteBuffer buffer) throws IOException;

  boolean tryGet(int blockNumber, ByteBuffer buffer);

  void put(int blockNumber, ByteBuffer buffer, Configuration conf,
      LocalDirAllocator localDirAllocator) throws IOException;

  int getMaxDiskBlocksCount();
}
