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
 * IO size range for file access pattern matching.
 * Supports head access [start, start+x] and tail access [end-y, end].
 */
public class IOSizeRange {
    public enum IOType {
        HEAD, TAIL, SIZE
    }

    private final IOType type;
    private final long size;
    private final long maxSize;

    /**
     * Constructor for IOSizeRange.
     *
     * @param type the IO type (HEAD or TAIL)
     * @param size the size of the range
     */
    public IOSizeRange(IOType type, long size) {
        this(type, size, 0);
    }

    /**
     * Constructor for IOSizeRange with max size for SIZE type.
     *
     * @param type the IO type (HEAD, TAIL or SIZE)
     * @param size the size of the range (min size for SIZE type)
     * @param maxSize the max size of the range (only used for SIZE type)
     */
    public IOSizeRange(IOType type, long size, long maxSize) {
        this.type = type;
        this.size = size;
        this.maxSize = maxSize;
    }

    /**
     * Check if the given IO range matches this IO size range.
     *
     * @param fileSize the file size
     * @param ioStart  the start position of IO
     * @param ioEnd    the end position of IO
     * @return true if matches, false otherwise
     */
    public boolean contains(long fileSize, long ioStart, long ioEnd) {
        switch (type) {
            case HEAD:
                // Head access: [start, start + size)
                return ioEnd < size;
            case TAIL:
                // Tail access: [fileSize - size, fileSize)
                return ioStart >= fileSize - size;
            case SIZE:
                // Size-based access: matches when IO size is within the configured range [size, maxSize]
                long ioSize = ioEnd - ioStart + 1;
                if (maxSize > 0) {
                    return ioSize >= size && ioSize <= maxSize;
                } else {
                    return ioSize <= size;
                }
            default:
                return false;
        }
    }

    public IOType getType() {
        return type;
    }

    public long getSize() {
        return size;
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
        IOSizeRange that = (IOSizeRange) o;
        return type == that.type && size == that.size && maxSize == that.maxSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, size, maxSize);
    }

    @Override
    public String toString() {
        return "IOSizeRange{" +
                "type=" + type +
                ", size=" + size +
                ", maxSize=" + maxSize +
                '}';
    }
}