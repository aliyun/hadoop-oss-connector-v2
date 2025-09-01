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

import org.apache.hadoop.fs.Path;

/**
 * This class holds attributes of an object independent of the
 * file status type.
 * It is used in {@link ossAInputStream} and elsewhere.
 * as a way to reduce parameters being passed
 * to the constructor of such class,
 * and elsewhere to be a source-neutral representation of a file status.
 */

public class ObjectAttributes {
  private final String bucket;
  private final Path path;
  private final String key;
//  private final String serverSideEncryptionAlgorithm;
//  private final String serverSideEncryptionKey;
  private final String eTag;
  private final String versionId;
  private final long len;

  /**
   * Constructor.
   * @param bucket oss bucket
   * @param path path
   * @param key object key
   * @param serverSideEncryptionAlgorithm current encryption algorithm
   * @param serverSideEncryptionKey any server side encryption key?
   * @param len object length
   * @param eTag optional etag
   * @param versionId optional version id
   */
  public ObjectAttributes(
      String bucket,
      Path path,
      String key,
      String eTag,
      String versionId,
      long len) {
    this.bucket = bucket;
    this.path = path;
    this.key = key;
    this.eTag = eTag;
    this.versionId = versionId;
    this.len = len;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

//  public String getServerSideEncryptionAlgorithm() {
//    return serverSideEncryptionAlgorithm;
//  }
//
//  public String getServerSideEncryptionKey() {
//    return serverSideEncryptionKey;
//  }

  public String getETag() {
    return eTag;
  }

  public String getVersionId() {
    return versionId;
  }

  public long getLen() {
    return len;
  }

  public Path getPath() {
    return path;
  }
}
