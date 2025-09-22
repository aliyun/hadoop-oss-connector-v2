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

import java.util.List;
import java.util.Objects;

/**
 * Rule for determining whether to use ACC client or OSS client for operations.
 * <p>
 * A rule consists of:
 * 1. Bucket name pattern (default "*" for all buckets)
 * 2. Key prefixes (e.g., "a/,b/")
 * 3. Key suffixes (e.g., "jpg,jpeg,parquet")
 * 4. File size ranges (multiple ranges supported)
 * 5. Operations (e.g., getObject, putObject)
 * 6. IO size ranges (head or tail access patterns)
 */
public class OssAccRule {
    private final String bucketPattern;
    private final List<String> keyPrefixes;
    private final List<String> keySuffixes;
    private final List<ObjectRange> sizeRanges;
    private final List<String> operations;
    private final List<IOSizeRange> ioSizeRanges;

    /**
     * Constructor for OssAccRule.
     *
     * @param bucketPattern the bucket name pattern, "*" for all buckets
     * @param keyPrefixes   list of key prefixes
     * @param keySuffixes   list of key suffixes
     * @param sizeRanges    list of file size ranges
     * @param operations    list of operations
     */
    public OssAccRule(String bucketPattern, List<String> keyPrefixes, List<String> keySuffixes,
                      List<ObjectRange> sizeRanges, List<String> operations) {
        this(bucketPattern, keyPrefixes, keySuffixes, sizeRanges, operations, null);
    }

    /**
     * Constructor for OssAccRule with IO size ranges.
     *
     * @param bucketPattern the bucket name pattern, "*" for all buckets
     * @param keyPrefixes   list of key prefixes
     * @param keySuffixes   list of key suffixes
     * @param sizeRanges    list of file size ranges
     * @param operations    list of operations
     * @param ioSizeRanges  list of IO size ranges
     */
    public OssAccRule(String bucketPattern, List<String> keyPrefixes, List<String> keySuffixes,
                      List<ObjectRange> sizeRanges, List<String> operations, List<IOSizeRange> ioSizeRanges) {
        this.bucketPattern = bucketPattern != null ? bucketPattern : "*";
        this.keyPrefixes = keyPrefixes;
        this.keySuffixes = keySuffixes;
        this.sizeRanges = sizeRanges;
        this.operations = operations;
        this.ioSizeRanges = ioSizeRanges;
    }

    /**
     * Check if this rule matches the given bucket, key, size and operation.
     *
     * @param bucket      the bucket name
     * @param key         the object key
     * @param objectSize  the object size
     * @param byteStart   the start byte position of the IO request
     * @param byteEnd     the end byte position of the IO request
     * @param operation   the operation name
     * @return true if the rule matches, false otherwise
     */
    public boolean matches(String bucket, String key, long objectSize, long byteStart, long byteEnd, String operation) {
        // Check bucket pattern
        if (!matchesBucket(bucket)) {
            return false;
        }

        // Check key prefix
        if (!matchesKeyPrefix(key)) {
            return false;
        }

        // Check key suffix
        if (!matchesKeySuffix(key)) {
            return false;
        }

        // Check size range
        if (!matchesSize(objectSize)) {
            return false;
        }


        // Check IO size range - for backward compatibility, if ioSizeRanges is specified,
        // we only match when there's a match with default IO range (0 to size-1)
        if (!matchesIOSize(objectSize, byteStart, Math.min(byteEnd, objectSize - 1))) {
            return false;
        }

        // Check operation
        if (!matchesOperation(operation)) {
            return false;
        }

        return true;
    }


    /**
     * Check if the bucket matches the pattern.
     *
     * @param bucket the bucket name
     * @return true if matches, false otherwise
     */
    private boolean matchesBucket(String bucket) {
        return bucketPattern.isEmpty() || "*".equals(bucketPattern) || bucketPattern.equals(bucket);
    }

    /**
     * Check if the key prefix matches.
     *
     * @param key the object key
     * @return true if matches, false otherwise
     */
    private boolean matchesKeyPrefix(String key) {
        // If no prefixes are configured, match all
        if (keyPrefixes == null || keyPrefixes.isEmpty()) {
            return true;
        }

        for (String prefix : keyPrefixes) {
            if (key != null && key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the key suffix matches.
     *
     * @param key the object key
     * @return true if matches, false otherwise
     */
    private boolean matchesKeySuffix(String key) {
        // If no suffixes are configured, match all
        if (keySuffixes == null || keySuffixes.isEmpty()) {
            return true;
        }

        for (String suffix : keySuffixes) {
            if (key != null && key.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the size is within any of the configured ranges.
     *
     * @param size the object size
     * @return true if matches, false otherwise
     */
    private boolean matchesSize(long size) {
        // If no size ranges are configured, match all
        if (sizeRanges == null || sizeRanges.isEmpty()) {
            return true;
        }

        for (ObjectRange range : sizeRanges) {
            if (range.contains(size)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the IO size is within any of the configured ranges.
     *
     * @param fileSize the file size
     * @param ioStart  the start position of IO
     * @param ioEnd    the end position of IO
     * @return true if matches, false otherwise
     */
    private boolean matchesIOSize(long objectSize, long ioStart, long ioEnd) {
        // If no IO size ranges are configured, match all
        if (ioSizeRanges == null || ioSizeRanges.isEmpty()) {
            return true;
        }

        for (IOSizeRange range : ioSizeRanges) {
            if (range.contains(objectSize, ioStart, ioEnd)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the operation matches.
     *
     * @param operation the operation name
     * @return true if matches, false otherwise
     */
    private boolean matchesOperation(String operation) {
        // If no operations are configured, match all
        if (operations == null || operations.isEmpty()) {
            return true;
        }

        return operations.contains(operation);
    }

    public String getBucketPattern() {
        return bucketPattern;
    }

    public List<String> getKeySuffixes() {
        return keySuffixes;
    }

    public List<ObjectRange> getSizeRanges() {
        return sizeRanges;
    }

    public List<String> getOperations() {
        return operations;
    }

    public List<IOSizeRange> getIoSizeRanges() {
        return ioSizeRanges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OssAccRule that = (OssAccRule) o;
        return Objects.equals(bucketPattern, that.bucketPattern) &&
                Objects.equals(keySuffixes, that.keySuffixes) &&
                Objects.equals(sizeRanges, that.sizeRanges) &&
                Objects.equals(operations, that.operations) &&
                Objects.equals(ioSizeRanges, that.ioSizeRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketPattern, keySuffixes, sizeRanges, operations, ioSizeRanges);
    }

    @Override
    public String toString() {
        return "OssAccRule{" +
                "bucketPattern='" + bucketPattern + '\'' +
                ", keySuffixes=" + keySuffixes +
                ", sizeRanges=" + sizeRanges +
                ", operations=" + operations +
                ", ioSizeRanges=" + ioSizeRanges +
                '}';
    }

}