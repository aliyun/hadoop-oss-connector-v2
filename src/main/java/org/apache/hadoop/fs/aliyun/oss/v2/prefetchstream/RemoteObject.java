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

import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import org.apache.hadoop.fs.aliyun.oss.v2.OssManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class RemoteObject {

  private static final Logger LOG =
      LoggerFactory.getLogger(RemoteObject.class);

  private final ReadOpContext context;

  private final ObjectAttributes objectAttributes;

  private final OssManager client;

  private final String uri;

  private static final int DRAIN_BUFFER_SIZE = 16384;


  public RemoteObject(
      ReadOpContext context,
      ObjectAttributes objectAttributes,
     OssManager client
  ) {

    this.context = context;
    this.objectAttributes = objectAttributes;
    this.client = client;
    this.uri = this.getPath();
  }

  public String getPath() {
    return getPath(objectAttributes);
  }

  public static String getPath(ObjectAttributes ObjectAttributes) {
    return String.format("oss://%s/%s", ObjectAttributes.getBucket(),
        ObjectAttributes.getKey());
  }

  public long size() {
    return objectAttributes.getLen();
  }

  public GetObjectResult openForRead(long offset, int size, ObjectAttributes objectAttributes)
      throws IOException {

      String operation = String.format(
              "%s %s at %d", "open", uri, offset);
      GetObjectResult request;
      try {
          request = client.getObjectWithResult(objectAttributes.getBucket(), objectAttributes.getKey(), objectAttributes, offset, offset + size - 1);
      } catch (IOException e) {
          throw e;
      }

      return request;
  }

  void close(GetObjectResult getObjectResult, int numRemainingBytes) {
      LOG.debug("Closing RemoteObject!");

    if (numRemainingBytes <= context.getAsyncDrainThreshold()) {

        try {
            getObjectResult.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    } else {
      LOG.debug("initiating asynchronous drain of {} bytes", numRemainingBytes);

      getObjectResult.abort();
    }
  }
}
