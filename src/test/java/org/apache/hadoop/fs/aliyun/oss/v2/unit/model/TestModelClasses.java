package org.apache.hadoop.fs.aliyun.oss.v2.unit.model;

import org.apache.hadoop.fs.aliyun.oss.v2.model.AliyunOSSStatusProbeEnum;
import org.apache.hadoop.fs.aliyun.oss.v2.model.DateUtil;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ListParam;
import org.apache.hadoop.fs.aliyun.oss.v2.model.OSSErrorCode;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ObjectMetadataParam;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ObjectSummaryParam;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for model package: DateUtil, ObjectMetadataParam,
 * ListParam, ObjectSummaryParam, AliyunOSSStatusProbeEnum, OSSErrorCode.
 */
public class TestModelClasses {

    // ==================== DateUtil Tests ====================

    @Test
    public void testFormatAndParseRfc822Date() throws ParseException {
        Date date = new Date(1705312200000L); // 2024-01-15T10:30:00Z
        String formatted = DateUtil.formatRfc822Date(date);
        assertNotNull(formatted);
        Date parsed = DateUtil.parseRfc822Date(formatted);
        assertEquals(date, parsed);
    }

    @Test
    public void testFormatAndParseIso8601Date() throws ParseException {
        Date date = new Date(1705312200123L);
        String formatted = DateUtil.formatIso8601Date(date);
        assertNotNull(formatted);
        assertTrue(formatted.endsWith("Z"));
        Date parsed = DateUtil.parseIso8601Date(formatted);
        assertEquals(date, parsed);
    }

    @Test
    public void testFormatAlternativeIso8601Date() throws ParseException {
        Date date = new Date(1705312200000L);
        String formatted = DateUtil.formatAlternativeIso8601Date(date);
        assertNotNull(formatted);
        // Alternative format has no fractional seconds
        assertFalse(formatted.contains("."));
        Date parsed = DateUtil.parseIso8601Date(formatted);
        assertEquals(date, parsed);
    }

    @Test
    public void testParseIso8601FallbackToAlternative() throws ParseException {
        // Alternative format (without fractional seconds)
        Date parsed = DateUtil.parseIso8601Date("2024-01-15T10:30:00Z");
        assertNotNull(parsed);
    }

    @Test
    public void testParseInvalidDateThrows() {
        assertThrows(ParseException.class,
                () -> DateUtil.parseRfc822Date("invalid"));
    }

    @Test
    public void testParseInvalidIsoDateThrows() {
        assertThrows(ParseException.class,
                () -> DateUtil.parseIso8601Date("not-a-date"));
    }

    // ==================== ObjectMetadataParam Tests ====================

    @Test
    public void testObjectMetadataParamContentLength() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertEquals(0, meta.getContentLength());
        meta.setContentLength(1024);
        assertEquals(1024, meta.getContentLength());
    }

    @Test
    public void testObjectMetadataParamContentType() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getContentType());
        meta.setContentType("application/json");
        assertEquals("application/json", meta.getContentType());
    }

    @Test
    public void testObjectMetadataParamContentEncoding() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getContentEncoding());
        meta.setContentEncoding("gzip");
        assertEquals("gzip", meta.getContentEncoding());
    }

    @Test
    public void testObjectMetadataParamETag() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getETag());
        meta.setETag("abc123");
        assertEquals("abc123", meta.getETag());
    }

    @Test
    public void testObjectMetadataParamServerSideEncryption() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getServerSideEncryption());
        meta.setServerSideEncryption("AES256");
        assertEquals("AES256", meta.getServerSideEncryption());
    }

    @Test
    public void testObjectMetadataParamServerSideDataEncryption() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getServerSideDataEncryption());
        meta.setServerSideDataEncryption("AES256");
        assertEquals("AES256", meta.getServerSideDataEncryption());
    }

    @Test
    public void testObjectMetadataParamLastModified() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getLastModified());
        Date now = new Date();
        meta.setLastModified(now);
        assertEquals(now, meta.getLastModified());
    }

    @Test
    public void testObjectMetadataParamServerCRC() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getServerCRC());
        meta.setHeader("x-oss-hash-crc64ecma", "123456789");
        Long crc = meta.getServerCRC();
        assertNotNull(crc);
        assertEquals(123456789L, crc);
    }

    @Test
    public void testObjectMetadataParamRemoveHeader() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        meta.setContentType("text/plain");
        assertNotNull(meta.getContentType());
        meta.removeHeader("Content-Type");
        assertNull(meta.getContentType());
    }

    @Test
    public void testObjectMetadataParamContentMD5() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        assertNull(meta.getContentMD5());
        meta.setContentMD5("md5hash");
        assertEquals("md5hash", meta.getContentMD5());
    }

    @Test
    public void testObjectMetadataParamContentDisposition() {
        ObjectMetadataParam meta = new ObjectMetadataParam();
        meta.setContentDisposition("attachment; filename=\"test.txt\"");
        // Just verify no exception
    }

    // ==================== ListParam Tests ====================

    @Test
    public void testListParamRecursive() {
        ListParam param = new ListParam("bucket", "prefix", 1000, null, null, true);
        assertEquals("bucket", param.getBucket());
        assertTrue(param.isRecursive());
        assertNull(param.getDelimiter());
        // prefix gets trailing slash
        assertEquals("prefix/", param.getPrefix());
    }

    @Test
    public void testListParamNonRecursive() {
        ListParam param = new ListParam("bucket", "prefix", 1000, null, null, false);
        assertFalse(param.isRecursive());
        assertEquals("/", param.getDelimiter());
    }

    @Test
    public void testListParamContinuationToken() {
        ListParam param = new ListParam("bucket", "prefix", 1000, null, null, false);
        assertNull(param.getContinuationToken());
        param.setContinuationToken("token123");
        assertEquals("token123", param.getContinuationToken());
    }

    @Test
    public void testListParamMarker() {
        ListParam param = new ListParam("bucket", "prefix", 1000, "marker1", null, false);
        assertEquals("marker1", param.getMarker());
    }

    @Test
    public void testListParamToString() {
        ListParam param = new ListParam("bucket", "prefix", 1000, null, null, false);
        String str = param.toString();
        assertNotNull(str);
        assertTrue(str.contains("prefix"));
    }

    // ==================== ObjectSummaryParam Tests ====================

    @Test
    public void testObjectSummaryParam() {
        ObjectSummaryParam summary = new ObjectSummaryParam();
        summary.setBucketName("bucket");
        summary.setKey("key");
        summary.setETag("etag");
        summary.setSize(100);
        Date now = new Date();
        summary.setLastModified(now);

        assertEquals("bucket", summary.getBucketName());
        assertEquals("key", summary.getKey());
        assertEquals("etag", summary.getETag());
        assertEquals(100, summary.getSize());
        assertEquals(now, summary.getLastModified());
    }

    // ==================== AliyunOSSStatusProbeEnum Tests ====================

    @Test
    public void testStatusProbeEnumAll() {
        assertEquals(3, AliyunOSSStatusProbeEnum.ALL.size());
        assertTrue(AliyunOSSStatusProbeEnum.ALL.contains(AliyunOSSStatusProbeEnum.Head));
        assertTrue(AliyunOSSStatusProbeEnum.ALL.contains(AliyunOSSStatusProbeEnum.DirMarker));
        assertTrue(AliyunOSSStatusProbeEnum.ALL.contains(AliyunOSSStatusProbeEnum.List));
    }

    @Test
    public void testStatusProbeEnumHeadOnly() {
        assertEquals(1, AliyunOSSStatusProbeEnum.HEAD_ONLY.size());
        assertTrue(AliyunOSSStatusProbeEnum.HEAD_ONLY.contains(AliyunOSSStatusProbeEnum.Head));
    }

    @Test
    public void testStatusProbeEnumFile() {
        assertEquals(AliyunOSSStatusProbeEnum.HEAD_ONLY, AliyunOSSStatusProbeEnum.FILE);
    }

    @Test
    public void testStatusProbeEnumDirectories() {
        assertEquals(EnumSet.of(AliyunOSSStatusProbeEnum.DirMarker, AliyunOSSStatusProbeEnum.List),
                AliyunOSSStatusProbeEnum.DIRECTORIES);
    }

    @Test
    public void testStatusProbeEnumListOnly() {
        assertEquals(EnumSet.of(AliyunOSSStatusProbeEnum.List),
                AliyunOSSStatusProbeEnum.LIST_ONLY);
    }

    // ==================== OSSErrorCode Tests ====================

    @Test
    public void testOSSErrorCodeConstants() {
        assertEquals("AccessDenied", OSSErrorCode.ACCESS_DENIED);
        assertEquals("NoSuchKey", OSSErrorCode.NO_SUCH_KEY);
        assertEquals("NoSuchBucket", OSSErrorCode.NO_SUCH_BUCKET);
        assertEquals("InvalidArgument", OSSErrorCode.INVALID_ARGUMENT);
        assertEquals("ObjectAlreadyExists", OSSErrorCode.OBJECT_ALREADY_EXISTS);
        assertEquals("InternalError", OSSErrorCode.INTERNAL_ERROR);
        assertEquals("SignatureDoesNotMatch", OSSErrorCode.SIGNATURE_DOES_NOT_MATCH);
    }
}
