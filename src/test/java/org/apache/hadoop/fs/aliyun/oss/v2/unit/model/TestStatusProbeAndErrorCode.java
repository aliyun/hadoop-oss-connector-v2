/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.hadoop.fs.aliyun.oss.v2.unit.model;

import org.apache.hadoop.fs.aliyun.oss.v2.model.AliyunOSSStatusProbeEnum;
import org.apache.hadoop.fs.aliyun.oss.v2.model.OSSErrorCode;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AliyunOSSStatusProbeEnum} and {@link OSSErrorCode}.
 */
class TestStatusProbeAndErrorCode {

    // ============== AliyunOSSStatusProbeEnum ==============

    @Test
    void testStatusProbeEnumValues() {
        AliyunOSSStatusProbeEnum[] values = AliyunOSSStatusProbeEnum.values();
        assertEquals(3, values.length);
    }

    @Test
    void testStatusProbeEnumHead() {
        assertEquals(AliyunOSSStatusProbeEnum.Head,
                AliyunOSSStatusProbeEnum.valueOf("Head"));
    }

    @Test
    void testStatusProbeEnumDirMarker() {
        assertEquals(AliyunOSSStatusProbeEnum.DirMarker,
                AliyunOSSStatusProbeEnum.valueOf("DirMarker"));
    }

    @Test
    void testStatusProbeEnumList() {
        assertEquals(AliyunOSSStatusProbeEnum.List,
                AliyunOSSStatusProbeEnum.valueOf("List"));
    }

    @Test
    void testAllSet() {
        Set<AliyunOSSStatusProbeEnum> all = AliyunOSSStatusProbeEnum.ALL;
        assertEquals(3, all.size());
        assertTrue(all.contains(AliyunOSSStatusProbeEnum.Head));
        assertTrue(all.contains(AliyunOSSStatusProbeEnum.DirMarker));
        assertTrue(all.contains(AliyunOSSStatusProbeEnum.List));
    }

    @Test
    void testHeadOnlySet() {
        Set<AliyunOSSStatusProbeEnum> headOnly = AliyunOSSStatusProbeEnum.HEAD_ONLY;
        assertEquals(1, headOnly.size());
        assertTrue(headOnly.contains(AliyunOSSStatusProbeEnum.Head));
    }

    @Test
    void testListOnlySet() {
        Set<AliyunOSSStatusProbeEnum> listOnly = AliyunOSSStatusProbeEnum.LIST_ONLY;
        assertEquals(1, listOnly.size());
        assertTrue(listOnly.contains(AliyunOSSStatusProbeEnum.List));
    }

    @Test
    void testFileSetEqualsHeadOnly() {
        assertSame(AliyunOSSStatusProbeEnum.HEAD_ONLY,
                AliyunOSSStatusProbeEnum.FILE);
    }

    @Test
    void testDirectoriesSet() {
        Set<AliyunOSSStatusProbeEnum> dirs = AliyunOSSStatusProbeEnum.DIRECTORIES;
        assertEquals(2, dirs.size());
        assertTrue(dirs.contains(AliyunOSSStatusProbeEnum.DirMarker));
        assertTrue(dirs.contains(AliyunOSSStatusProbeEnum.List));
        assertFalse(dirs.contains(AliyunOSSStatusProbeEnum.Head));
    }

    // ============== OSSErrorCode Constants ==============

    @Test
    void testAccessDenied() {
        assertEquals("AccessDenied", OSSErrorCode.ACCESS_DENIED);
    }

    @Test
    void testAccessForbidden() {
        assertEquals("AccessForbidden", OSSErrorCode.ACCESS_FORBIDDEN);
    }

    @Test
    void testBucketAlreadyExists() {
        assertEquals("BucketAlreadyExists", OSSErrorCode.BUCKET_ALREADY_EXISTS);
    }

    @Test
    void testBucketNotEmpty() {
        assertEquals("BucketNotEmpty", OSSErrorCode.BUCKET_NOT_EMPTY);
    }

    @Test
    void testInvalidArgument() {
        assertEquals("InvalidArgument", OSSErrorCode.INVALID_ARGUMENT);
    }

    @Test
    void testInvalidAccessKeyId() {
        assertEquals("InvalidAccessKeyId", OSSErrorCode.INVALID_ACCESS_KEY_ID);
    }

    @Test
    void testInternalError() {
        assertEquals("InternalError", OSSErrorCode.INTERNAL_ERROR);
    }

    @Test
    void testNoSuchBucket() {
        assertEquals("NoSuchBucket", OSSErrorCode.NO_SUCH_BUCKET);
    }

    @Test
    void testNoSuchKey() {
        assertEquals("NoSuchKey", OSSErrorCode.NO_SUCH_KEY);
    }

    @Test
    void testNoSuchUpload() {
        assertEquals("NoSuchUpload", OSSErrorCode.NO_SUCH_UPLOAD);
    }

    @Test
    void testPreconditionFailed() {
        assertEquals("PreconditionFailed", OSSErrorCode.PRECONDITION_FAILED);
    }

    @Test
    void testSignatureDoesNotMatch() {
        assertEquals("SignatureDoesNotMatch", OSSErrorCode.SIGNATURE_DOES_NOT_MATCH);
    }

    @Test
    void testTooManyBuckets() {
        assertEquals("TooManyBuckets", OSSErrorCode.TOO_MANY_BUCKETS);
    }

    @Test
    void testEntityTooSmall() {
        assertEquals("EntityTooSmall", OSSErrorCode.ENTITY_TOO_SMALL);
    }

    @Test
    void testEntityTooLarge() {
        assertEquals("EntityTooLarge", OSSErrorCode.ENTITY_TOO_LARGE);
    }

    @Test
    void testObjectAlreadyExists() {
        assertEquals("ObjectAlreadyExists", OSSErrorCode.OBJECT_ALREADY_EXISTS);
    }

    @Test
    void testRequestTimeout() {
        assertEquals("RequestTimeout", OSSErrorCode.REQUEST_TIMEOUT);
    }

    @Test
    void testFileGroupTooLarge() {
        assertEquals("FileGroupTooLarge", OSSErrorCode.FILE_GROUP_TOO_LARGE);
    }

    @Test
    void testFilePartStale() {
        assertEquals("FilePartStale", OSSErrorCode.FILE_PART_STALE);
    }

    @Test
    void testInvalidBucketName() {
        assertEquals("InvalidBucketName", OSSErrorCode.INVALID_BUCKET_NAME);
    }

    @Test
    void testInvalidObjectName() {
        assertEquals("InvalidObjectName", OSSErrorCode.INVALID_OBJECT_NAME);
    }

    @Test
    void testInvalidPart() {
        assertEquals("InvalidPart", OSSErrorCode.INVALID_PART);
    }

    @Test
    void testInvalidPartOrder() {
        assertEquals("InvalidPartOrder", OSSErrorCode.INVALID_PART_ORDER);
    }

    @Test
    void testMissingContentLength() {
        assertEquals("MissingContentLength", OSSErrorCode.MISSING_CONTENT_LENGTH);
    }

    @Test
    void testMissingArgument() {
        assertEquals("MissingArgument", OSSErrorCode.MISSING_ARGUMENT);
    }

    @Test
    void testNotImplemented() {
        assertEquals("NotImplemented", OSSErrorCode.NOT_IMPLEMENTED);
    }

    @Test
    void testNotModified() {
        assertEquals("NotModified", OSSErrorCode.NOT_MODIFIED);
    }

    @Test
    void testMalformedXml() {
        assertEquals("MalformedXML", OSSErrorCode.MALFORMED_XML);
    }

    @Test
    void testInvalidDigest() {
        assertEquals("InvalidDigest", OSSErrorCode.INVALID_DIGEST);
    }

    @Test
    void testInvalidRange() {
        assertEquals("InvalidRange", OSSErrorCode.INVALID_RANGE);
    }

    @Test
    void testObjectNotAppendable() {
        assertEquals("ObjectNotAppendable", OSSErrorCode.OBJECT_NOT_APPENDALBE);
    }

    @Test
    void testCallbackFailed() {
        assertEquals("CallbackFailed", OSSErrorCode.CALLBACK_FAILED);
    }

    @Test
    void testInvalidObjectState() {
        assertEquals("InvalidObjectState", OSSErrorCode.INVALID_OBJECT_STATE);
    }

    @Test
    void testNoSuchVersion() {
        assertEquals("NoSuchVersion", OSSErrorCode.NO_SUCH_VERSION);
    }

    @Test
    void testSecurityTokenNotSupported() {
        assertEquals("SecurityTokenNotSupported", OSSErrorCode.SECURITY_TOKEN_NOT_SUPPORTED);
    }

    @Test
    void testMetaQueryConstants() {
        assertEquals("MetaQueryAlreadyExist", OSSErrorCode.META_QUERY_ALREADY_EXIST);
        assertEquals("MetaQueryNotExist", OSSErrorCode.META_QUERY_NOT_EXIST);
        assertEquals("MetaQueryNotReady", OSSErrorCode.META_QUERY_NOT_READY);
    }

    @Test
    void testWormConstants() {
        assertEquals("WORMConfigurationLocked", OSSErrorCode.WORM_CONFIGURATION_LOCKED);
        assertEquals("InvalidWORMConfiguration", OSSErrorCode.INVALID_WORM_CONFIGURATION);
    }

    @Test
    void testFileImmutable() {
        assertEquals("FileImmutable", OSSErrorCode.FILE_IMMUTABLE);
    }

    @Test
    void testPartNotSequential() {
        assertEquals("PartNotSequential", OSSErrorCode.PART_NOT_SEQUENTIAL);
    }

    @Test
    void testRequestTimeTooSkewed() {
        assertEquals("RequestTimeTooSkewed", OSSErrorCode.REQUEST_TIME_TOO_SKEWED);
    }
}
