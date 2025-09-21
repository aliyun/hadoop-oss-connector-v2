package org.apache.hadoop.fs.aliyun.oss.v2.model;

import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ObjectAttributes;

public class GetObjectReq {
    String bucketName;
    String key;
    ObjectAttributes objectAttributes;
    long byteStart;
    long byteEnd;

    public GetObjectReq(String bucketName, String key, ObjectAttributes objectAttributes, long byteStart, long byteEnd) {
        this.bucketName = bucketName;
        this.key = key;
        this.objectAttributes = objectAttributes;
        this.byteStart = byteStart;
        this.byteEnd = byteEnd;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }

    public ObjectAttributes getObjectAttributes() {
        return objectAttributes;
    }

    public long getByteStart() {
        return byteStart;
    }

    public long getByteEnd() {
        return byteEnd;
    }

}
