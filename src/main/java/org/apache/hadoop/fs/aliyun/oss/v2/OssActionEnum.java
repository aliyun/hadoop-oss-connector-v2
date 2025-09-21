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
package org.apache.hadoop.fs.aliyun.oss.v2;

/**
 * Enumeration of OSS action types.
 */
public enum OssActionEnum {
    GET_OBJECT("getObject"),
    GET_OBJECT_ASYNC("getObjectAsync"),
    PUT_OBJECT("putObject"),
    LIST_V2("listObjectsV2"),
    DELETE("deleteObject"),
    Other("other"),
    INITIATE_MULTIPART_UPLOAD("initiateMultipartUpload"),
    MULTIPART_UPLOAD("multipartUpload"),
    COMPLETE_MULTIPART_UPLOAD("completeMultipartUpload"),
    ABORT_MULTIPART_UPLOAD("abortMultipartUpload"),
    COPY_OBJECT("copyObject"),
    DELETE_OBJECTS("deleteObjects"),
    GET_OBJECT_METADATA("getObjectMetadata");


    private final String operationName;

    OssActionEnum(String operationName) {
        this.operationName = operationName;
    }

    public String getOperationName() {
        return operationName;
    }
}