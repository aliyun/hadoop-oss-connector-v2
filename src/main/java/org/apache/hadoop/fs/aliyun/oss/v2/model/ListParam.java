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
package org.apache.hadoop.fs.aliyun.oss.v2.model;

import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils;

public class ListParam {
    private String bucket;


    private String prefix;
    private long maxListingLength;
    private String marker;
    private String continuationToken;
    private boolean recursive;
    private String delimiter;

    public ListParam(String bucket, String prefix, long maxListingLength, String marker,
                     String continuationToken, boolean recursive) {
        this.bucket = bucket;
        this.delimiter = recursive ? null : "/";
        this.prefix = AliyunOSSUtils.maybeAddTrailingSlash(prefix);

        this.maxListingLength = maxListingLength;
        this.marker = marker;
        this.continuationToken = continuationToken;
        this.recursive = recursive;


    }

    public void setContinuationToken(String nextContinuationToken) {
        this.continuationToken = nextContinuationToken;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public long getMaxListingLength() {
        return maxListingLength;
    }

    public void setMaxListingLength(int maxListingLength) {
        this.maxListingLength = maxListingLength;
    }

    public String getMarker() {
        return marker;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }


    @Override
    public String toString() {
        return new StringBuilder(128)
                .append("p=").append(prefix).append("&")
                .append("l=").append(maxListingLength).append("&")
                .append("m=").append(marker).append("&")
                .append("t='").append(continuationToken).append("&")
                .append("r=").append(recursive).append("&")
                .append("d=").append(delimiter)
                .toString();
    }
}
