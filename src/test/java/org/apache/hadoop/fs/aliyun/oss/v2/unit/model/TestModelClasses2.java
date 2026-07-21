package org.apache.hadoop.fs.aliyun.oss.v2.unit.model;

import org.apache.hadoop.fs.aliyun.oss.v2.model.*;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ObjectAttributes;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ListResultV2, PartETagParam, AliyunOSSDirEmptyFlag,
 * GetObjectReq, OSSHeaders, ObjectAlreadyExistsException, ObjectNotFoundException.
 */
public class TestModelClasses2 {

    // ==================== ListResultV2 ====================

    @Test
    public void testListResultV2Default() {
        ListResultV2 result = new ListResultV2();
        assertNotNull(result.getObjectSummaries());
        assertTrue(result.getObjectSummaries().isEmpty());
        assertNotNull(result.getCommonPrefixes());
        assertTrue(result.getCommonPrefixes().isEmpty());
        assertNull(result.getBucketName());
        assertEquals(0, result.getKeyCount());
        assertFalse(result.isTruncated());
    }

    @Test
    public void testListResultV2SettersGetters() {
        ListResultV2 result = new ListResultV2();
        result.setBucketName("myBucket");
        result.setPrefix("dir/");
        result.setMaxKeys(100);
        result.setDelimiter("/");
        result.setEncodingType("url");
        result.setTruncated(true);
        result.setKeyCount(5);
        result.setContinuationToken("ct1");
        result.setNextContinuationToken("nct1");
        result.setStartAfter("start");

        assertEquals("myBucket", result.getBucketName());
        assertEquals("dir/", result.getPrefix());
        assertEquals(100, result.getMaxKeys());
        assertEquals("/", result.getDelimiter());
        assertEquals("url", result.getEncodingType());
        assertTrue(result.isTruncated());
        assertEquals(5, result.getKeyCount());
        assertEquals("ct1", result.getContinuationToken());
        assertEquals("nct1", result.getNextContinuationToken());
        assertEquals("start", result.getStartAfter());
    }

    @Test
    public void testListResultV2AddObjectSummary() {
        ListResultV2 result = new ListResultV2();
        ObjectSummaryParam summary = new ObjectSummaryParam();
        summary.setKey("file1.txt");
        result.addObjectSummary(summary);
        assertEquals(1, result.getObjectSummaries().size());
        assertEquals("file1.txt", result.getObjectSummaries().get(0).getKey());
    }

    @Test
    public void testListResultV2AddCommonPrefix() {
        ListResultV2 result = new ListResultV2();
        result.addCommonPrefix("dir1/");
        result.addCommonPrefix("dir2/");
        assertEquals(2, result.getCommonPrefixes().size());
    }

    @Test
    public void testListResultV2GetListNum() {
        ListResultV2 result = new ListResultV2();
        assertEquals(0, result.getListNum());

        ObjectSummaryParam s = new ObjectSummaryParam();
        s.setKey("file");
        result.addObjectSummary(s);
        result.addCommonPrefix("dir/");

        assertEquals(2, result.getListNum());
    }

    @Test
    public void testListResultV2GetListResultContent() {
        ListResultV2 result = new ListResultV2();
        ObjectSummaryParam s = new ObjectSummaryParam();
        s.setKey("file.txt");
        result.addObjectSummary(s);
        result.addCommonPrefix("dir/");

        String content = result.getListResultContent();
        assertNotNull(content);
        assertTrue(content.contains("file.txt"));
        assertTrue(content.contains("dir/"));
    }

    @Test
    public void testListResultV2RepresentsEmptyDirectory() {
        ListResultV2 result = new ListResultV2();
        ObjectSummaryParam s = new ObjectSummaryParam();
        s.setKey("dir/");
        result.addObjectSummary(s);
        assertTrue(result.representsEmptyDirectory("dir/"));
    }

    @Test
    public void testListResultV2NotRepresentsEmptyDirectory() {
        ListResultV2 result = new ListResultV2();
        ObjectSummaryParam s1 = new ObjectSummaryParam();
        s1.setKey("dir/");
        ObjectSummaryParam s2 = new ObjectSummaryParam();
        s2.setKey("dir/file.txt");
        result.addObjectSummary(s1);
        result.addObjectSummary(s2);
        assertFalse(result.representsEmptyDirectory("dir/"));
    }

    @Test
    public void testListResultV2NotRepresentsEmptyDirectoryWithPrefix() {
        ListResultV2 result = new ListResultV2();
        ObjectSummaryParam s = new ObjectSummaryParam();
        s.setKey("dir/");
        result.addObjectSummary(s);
        result.addCommonPrefix("dir/sub/");
        assertFalse(result.representsEmptyDirectory("dir/"));
    }

    // ==================== PartETagParam ====================

    @Test
    public void testPartETagParamConstructor() {
        PartETagParam p = new PartETagParam(1, "etag1");
        assertEquals(1, p.getPartNumber());
        assertEquals("etag1", p.getETag());
    }

    @Test
    public void testPartETagParamConstructorWithSize() {
        PartETagParam p = new PartETagParam(2, "etag2", 1024);
        assertEquals(2, p.getPartNumber());
        assertEquals("etag2", p.getETag());
        assertEquals(1024, p.getPartSize());
    }

    @Test
    public void testPartETagParamSetters() {
        PartETagParam p = new PartETagParam(0, "e");
        p.setPartNumber(3);
        p.setETag("newEtag");
        p.setPartSize(2048);
        assertEquals(3, p.getPartNumber());
        assertEquals("newEtag", p.getETag());
        assertEquals(2048, p.getPartSize());
    }

    @Test
    public void testPartETagParamCompareTo() {
        PartETagParam p1 = new PartETagParam(1, "a");
        PartETagParam p2 = new PartETagParam(2, "b");
        PartETagParam p3 = new PartETagParam(1, "c");

        assertTrue(p1.compareTo(p2) < 0);
        assertTrue(p2.compareTo(p1) > 0);
        assertEquals(0, p1.compareTo(p3));
    }

    @Test
    public void testPartETagParamSort() {
        List<PartETagParam> parts = new ArrayList<>();
        parts.add(new PartETagParam(3, "c"));
        parts.add(new PartETagParam(1, "a"));
        parts.add(new PartETagParam(2, "b"));
        Collections.sort(parts);
        assertEquals(1, parts.get(0).getPartNumber());
        assertEquals(2, parts.get(1).getPartNumber());
        assertEquals(3, parts.get(2).getPartNumber());
    }

    @Test
    public void testPartETagParamHashCode() {
        PartETagParam p1 = new PartETagParam(1, "etag");
        PartETagParam p2 = new PartETagParam(1, "etag");
        assertEquals(p1.hashCode(), p2.hashCode());
    }

    @Test
    public void testPartETagParamHashCodeDifferent() {
        PartETagParam p1 = new PartETagParam(1, "etag1");
        PartETagParam p2 = new PartETagParam(2, "etag2");
        assertNotEquals(p1.hashCode(), p2.hashCode());
    }

    // ==================== AliyunOSSDirEmptyFlag ====================

    @Test
    public void testDirEmptyFlagValues() {
        AliyunOSSDirEmptyFlag[] values = AliyunOSSDirEmptyFlag.values();
        assertEquals(3, values.length);
    }

    @Test
    public void testDirEmptyFlagToString() {
        assertEquals("EMPTY", AliyunOSSDirEmptyFlag.EMPTY.toString());
        assertEquals("NOT_EMPTY", AliyunOSSDirEmptyFlag.NOT_EMPTY.toString());
        assertEquals("UNKNOWN", AliyunOSSDirEmptyFlag.UNKNOWN.toString());
    }

    @Test
    public void testDirEmptyFlagValueOf() {
        assertEquals(AliyunOSSDirEmptyFlag.EMPTY, AliyunOSSDirEmptyFlag.valueOf("EMPTY"));
        assertEquals(AliyunOSSDirEmptyFlag.NOT_EMPTY, AliyunOSSDirEmptyFlag.valueOf("NOT_EMPTY"));
        assertEquals(AliyunOSSDirEmptyFlag.UNKNOWN, AliyunOSSDirEmptyFlag.valueOf("UNKNOWN"));
    }

    // ==================== GetObjectReq ====================

    @Test
    public void testGetObjectReq() {
        Path path = new Path("oss://bucket/key");
        ObjectAttributes attrs = new ObjectAttributes("bucket", path, "key", "etag", "v1", 100);
        GetObjectReq req = new GetObjectReq("bucket", "key", attrs, 0, 99);
        assertEquals("bucket", req.getBucketName());
        assertEquals("key", req.getKey());
        assertSame(attrs, req.getObjectAttributes());
        assertEquals(0, req.getByteStart());
        assertEquals(99, req.getByteEnd());
    }

    // ==================== OSSHeaders ====================

    @Test
    public void testOSSHeadersConstants() {
        assertEquals("Authorization", OSSHeaders.AUTHORIZATION);
        assertEquals("Content-Type", OSSHeaders.CONTENT_TYPE);
        assertEquals("Content-Length", OSSHeaders.CONTENT_LENGTH);
        assertEquals("ETag", OSSHeaders.ETAG);
        assertEquals("x-oss-request-id", OSSHeaders.OSS_HEADER_REQUEST_ID);
        assertEquals("x-oss-storage-class", OSSHeaders.STORAGE_CLASS);
        assertEquals("x-oss-security-token", OSSHeaders.OSS_SECURITY_TOKEN);
    }

    @Test
    public void testOSSHeadersPrefix() {
        assertEquals("x-oss-", OSSHeaders.OSS_PREFIX);
        assertEquals("x-oss-meta-", OSSHeaders.OSS_USER_METADATA_PREFIX);
    }

    // ==================== Exceptions ====================

    @Test
    public void testObjectAlreadyExistsException() {
        ObjectAlreadyExistsException e = new ObjectAlreadyExistsException("exists", new RuntimeException("cause"));
        assertEquals("exists", e.getMessage());
        assertNotNull(e.getCause());
    }

    @Test
    public void testObjectNotFoundException() {
        ObjectNotFoundException e = new ObjectNotFoundException("not found", null);
        assertEquals("not found", e.getMessage());
        assertNull(e.getCause());
    }
}
