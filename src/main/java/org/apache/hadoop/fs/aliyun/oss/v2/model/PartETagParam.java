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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.model;

/**
 * The result information for a part's upload in a multipart upload.
 */
public class PartETagParam implements Comparable<PartETagParam> {
    private int partNumber;
    private String eTag;
    private long partSize;

    /**
     * Constructor
     *
     * @param partNumber Part number.
     * @param eTag       Part ETag.
     */
    public PartETagParam(int partNumber, String eTag) {
        this.partNumber = partNumber;
        this.eTag = eTag;
    }

    /**
     * Constructor
     *
     * @param partNumber Part number.
     * @param eTag       Part ETag.
     * @param partSize   Part Size.
     * @param partCRC    Part's CRC value.
     */
    public PartETagParam(int partNumber, String eTag, long partSize) {
        this.partNumber = partNumber;
        this.eTag = eTag;
        this.partSize = partSize;
    }

    /**
     * Gets part number.
     *
     * @return Part number.
     */
    public int getPartNumber() {
        return partNumber;
    }

    /**
     * Sets part number.
     *
     * @param partNumber Part number.
     */
    public void setPartNumber(int partNumber) {
        this.partNumber = partNumber;
    }

    /**
     * Gets the part's ETag
     *
     * @return Part ETag.
     */
    public String getETag() {
        return eTag;
    }

    /**
     * Sets the part's ETag.
     *
     * @param eTag Part ETag.
     */
    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public long getPartSize() {
        return partSize;
    }

    public void setPartSize(long partSize) {
        this.partSize = partSize;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((eTag == null) ? 0 : eTag.hashCode());
        result = prime * result + partNumber;
        return result;
    }

    //这个类型要可以比较排序
    @Override
    public int compareTo(PartETagParam o) {
        return this.partNumber - o.partNumber;
    }

}
