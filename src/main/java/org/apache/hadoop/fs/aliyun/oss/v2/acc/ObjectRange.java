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
package org.apache.hadoop.fs.aliyun.oss.v2.acc;

import java.util.Objects;

/**
 * Size range for file size matching.
 */
public class ObjectRange {
    private final long minSize;
    private final long maxSize;

    /**
     * Constructor for SizeRange.
     *
     * @param minSize minimum size (inclusive)
     * @param maxSize maximum size (inclusive)
     */
    public ObjectRange(long minSize, long maxSize) {
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    /**
     * Check if the given size is within this range.
     *
     * @param size the size to check
     * @return true if within range, false otherwise
     */
    public boolean contains(long size) {
        return size >= minSize && size <= maxSize;
    }

    public long getMinSize() {
        return minSize;
    }

    public long getMaxSize() {
        return maxSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectRange sizeRange = (ObjectRange) o;
        return minSize == sizeRange.minSize && maxSize == sizeRange.maxSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minSize, maxSize);
    }

    @Override
    public String toString() {
        return "SizeRange{" +
                "minSize=" + minSize +
                ", maxSize=" + maxSize +
                '}';
    }
}
