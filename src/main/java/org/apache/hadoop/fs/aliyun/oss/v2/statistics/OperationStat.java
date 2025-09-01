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
package org.apache.hadoop.fs.aliyun.oss.v2.statistics;

import org.apache.hadoop.fs.aliyun.oss.v2.OssActionEnum;

import java.time.Instant;

/**
 * 操作统计信息类
 */
public class OperationStat {

    private OssActionEnum operationName;
    private String uploadId;

    public String getBucketName() {
        return bucketName;
    }

    private String bucketName;
    private String resource;
    private Instant operationTime;
    private long durationMicros;
    private boolean success;
    private String parentOperationName;
    private long startTime;
    private long bytesProcessed;
    private boolean useAcc = false;
    private long byteStart;
    private long byteEnd;
    private int partNumber;


    public OperationStat(OssActionEnum operationName, String bucketName, String resource, Instant operationTime, boolean useAcc, boolean success) {
        this.bucketName = bucketName;
        this.operationName = operationName;
        this.resource = resource;
        this.operationTime = operationTime;
        this.useAcc = useAcc;
        this.success = success;
    }

    public OssActionEnum getOperationName() {
        return operationName;
    }

    public String getResource() {
        return resource;
    }

    public Instant getOperationTime() {
        return operationTime;
    }

    public long getDurationMicros() {
        return durationMicros;
    }

    public String getParentOperationName() {
        return parentOperationName;
    }

    public void setParentOperationName(String parentOperationName) {
        this.parentOperationName = parentOperationName;
    }

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getBytesProcessed() {
        return bytesProcessed;
    }

    public void setBytesProcessed(long bytesProcessed) {
        this.bytesProcessed = bytesProcessed;
    }

    public boolean isUseAcc() {
        return useAcc;
    }

    public void setUseAcc(boolean useAcc) {
        this.useAcc = useAcc;
    }

    public long getByteEnd() {
        return byteEnd;
    }

    public long getByteStart() {
        return byteStart;
    }

    private void setByteStart(long byteStart) {
        this.byteStart = byteStart;
    }

    private void setByteEnd(long byteEnd) {
        this.byteEnd = byteEnd;

    }


    private void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public int getPartNumber() {
        return partNumber;
    }

    public void setPartNumber(int partNumber) {
        this.partNumber = partNumber;
    }

    public void setDurationMicros(long durationMicros) {
        this.durationMicros = durationMicros;
    }


    /**
     * OperationStat的Builder类，用于构建OperationStat实例
     */
    public static class Builder {
        private OssActionEnum operationName;
        private String bucketName;
        private String resource;
        private Instant operationTime;
        private long durationMicros;
        private boolean success;
        private String parentOperationName;
        private long startTime;
        private long bytesProcessed;
        private boolean useAcc = false;
        private long byteStart;
        private long byteEnd;
        private String uploadId;
        private int partNumber;

        public Builder() {
        }

        public Builder operationName(OssActionEnum operationName) {
            this.operationName = operationName;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder resource(String resource) {
            this.resource = resource;
            return this;
        }

        public Builder operationTime(Instant operationTime) {
            this.operationTime = operationTime;
            return this;
        }

        public Builder durationMicros(long durationMicros) {
            this.durationMicros = durationMicros;
            return this;
        }

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder parentOperationName(String parentOperationName) {
            this.parentOperationName = parentOperationName;
            return this;
        }

        public Builder startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder bytesProcessed(long bytesProcessed) {
            this.bytesProcessed = bytesProcessed;
            return this;
        }

        public Builder useAcc(boolean useAcc) {
            this.useAcc = useAcc;
            return this;
        }

        public Builder byteStart(long byteStart) {
            this.byteStart = byteStart;
            return this;
        }

        public Builder byteEnd(long byteEnd) {
            this.byteEnd = byteEnd;
            return this;
        }

        public Builder uploadId(String uploadId) {
            this.uploadId = uploadId;
            return this;
        }

        public Builder partNumber(int idx) {
            this.partNumber = idx;
            return this;
        }

        public OperationStat build() {
            OperationStat stat = new OperationStat(operationName, bucketName, resource, operationTime, useAcc, success);
            stat.setParentOperationName(parentOperationName);
            stat.setStartTime(startTime);
            stat.setBytesProcessed(bytesProcessed);
            stat.setByteEnd(byteEnd);
            stat.setByteStart(byteStart);
            stat.setUploadId(uploadId);
            return stat;
        }


    }


}