package org.apache.hadoop.fs.aliyun.oss.v2.unit;

import org.apache.hadoop.fs.aliyun.oss.v2.Constants;
import org.apache.hadoop.fs.aliyun.oss.v2.OssActionEnum;
import org.apache.hadoop.fs.aliyun.oss.v2.OSSManagerLogLevel;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OssStatisticTypeEnum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Constants, OssActionEnum, OSSManagerLogLevel,
 * and OssStatisticTypeEnum.
 */
public class TestConstantsAndEnums {

    // ==================== Constants Tests ====================

    @Test
    public void testConstantsValues() {
        assertEquals("oss", Constants.FS_OSS);
        assertEquals("fs.oss.", Constants.FS_OSS_PREFIX);
        assertEquals("fs.oss.bucket.", Constants.FS_OSS_BUCKET_PREFIX);
        assertEquals("fs.oss.endpoint", Constants.ENDPOINT_KEY);
        assertEquals("fs.oss.accessKeyId", Constants.ACCESS_KEY_ID);
        assertEquals("fs.oss.accessKeySecret", Constants.ACCESS_KEY_SECRET);
        assertEquals("fs.oss.credentials.provider", Constants.CREDENTIALS_PROVIDER_KEY);
    }

    @Test
    public void testConstantsDefaults() {
        assertEquals(32, Constants.MAXIMUM_CONNECTIONS_DEFAULT);
        assertEquals(true, Constants.SECURE_CONNECTIONS_DEFAULT);
        assertEquals(10, Constants.MAX_ERROR_RETRIES_DEFAULT);
        assertEquals(50000, Constants.ESTABLISH_TIMEOUT_DEFAULT);
        assertEquals(200000, Constants.SOCKET_TIMEOUT_DEFAULT);
        assertEquals(1000, Constants.MAX_PAGING_KEYS_DEFAULT);
        assertEquals(104857600, Constants.MULTIPART_UPLOAD_PART_SIZE_DEFAULT);
        assertEquals(100 * 1024, Constants.MULTIPART_MIN_SIZE);
        assertEquals(10000, Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);
    }

    @Test
    public void testBufferConstants() {
        assertEquals("disk", Constants.FAST_UPLOAD_BUFFER_DISK);
        assertEquals("array", Constants.FAST_UPLOAD_BUFFER_ARRAY);
        assertEquals("bytebuffer", Constants.FAST_UPLOAD_BYTEBUFFER);
        assertEquals("disk", Constants.DEFAULT_FAST_UPLOAD_BUFFER);
    }

    @Test
    public void testPrefetchConstants() {
        assertEquals("v2", Constants.PREFETCH_VERSION_DEFAULT);
        assertEquals(128 * 1024, Constants.PREFETCH_BLOCK_DEFAULT_SIZE);
        assertEquals(4, Constants.DEFAULT_PREFETCH_MAX_DISK_BLOCKS_COUNT);
        assertEquals(4, Constants.PREFETCH_BLOCK_DEFAULT_COUNT);
    }

    @Test
    public void testChangeDetectionConstants() {
        assertEquals("server", Constants.CHANGE_DETECT_MODE_DEFAULT);
        assertEquals("etag", Constants.CHANGE_DETECT_SOURCE_ETAG);
    }

    // ==================== OssActionEnum Tests ====================

    @Test
    public void testOssActionEnumValues() {
        OssActionEnum[] values = OssActionEnum.values();
        assertTrue(values.length > 0);
        for (OssActionEnum action : values) {
            assertNotNull(action.name());
            assertNotNull(action.getOperationName());
        }
    }

    @Test
    public void testOssActionEnumValueOf() {
        for (OssActionEnum action : OssActionEnum.values()) {
            assertEquals(action, OssActionEnum.valueOf(action.name()));
        }
    }

    @Test
    public void testOssActionEnumOperationNames() {
        assertEquals("getObject", OssActionEnum.GET_OBJECT.getOperationName());
        assertEquals("putObject", OssActionEnum.PUT_OBJECT.getOperationName());
        assertEquals("listObjectsV2", OssActionEnum.LIST_V2.getOperationName());
        assertEquals("deleteObject", OssActionEnum.DELETE.getOperationName());
    }

    // ==================== OSSManagerLogLevel Tests ====================

    @Test
    public void testOSSManagerLogLevelValues() {
        OSSManagerLogLevel[] values = OSSManagerLogLevel.values();
        assertEquals(3, values.length);
    }

    @Test
    public void testOSSManagerLogLevelValueOf() {
        for (OSSManagerLogLevel level : OSSManagerLogLevel.values()) {
            assertEquals(level, OSSManagerLogLevel.valueOf(level.name()));
        }
    }

    @Test
    public void testOSSManagerLogLevelGetters() {
        assertEquals(0, OSSManagerLogLevel.NONE.getValue());
        assertEquals("none", OSSManagerLogLevel.NONE.getName());
        assertEquals(1, OSSManagerLogLevel.STATISTIC.getValue());
        assertEquals("statistic", OSSManagerLogLevel.STATISTIC.getName());
        assertEquals(2, OSSManagerLogLevel.DETAIL.getValue());
        assertEquals("detail", OSSManagerLogLevel.DETAIL.getName());
    }

    @Test
    public void testOSSManagerLogLevelIsAtLeast() {
        assertTrue(OSSManagerLogLevel.DETAIL.isAtLeast(OSSManagerLogLevel.STATISTIC));
        assertTrue(OSSManagerLogLevel.DETAIL.isAtLeast(OSSManagerLogLevel.NONE));
        assertTrue(OSSManagerLogLevel.DETAIL.isAtLeast(OSSManagerLogLevel.DETAIL));
        assertFalse(OSSManagerLogLevel.NONE.isAtLeast(OSSManagerLogLevel.STATISTIC));
    }

    @Test
    public void testOSSManagerLogLevelFromValue() {
        assertEquals(OSSManagerLogLevel.NONE, OSSManagerLogLevel.fromValue(0));
        assertEquals(OSSManagerLogLevel.STATISTIC, OSSManagerLogLevel.fromValue(1));
        assertEquals(OSSManagerLogLevel.DETAIL, OSSManagerLogLevel.fromValue(2));
    }

    @Test
    public void testOSSManagerLogLevelFromValueInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> OSSManagerLogLevel.fromValue(99));
    }

    @Test
    public void testOSSManagerLogLevelFromName() {
        assertEquals(OSSManagerLogLevel.NONE, OSSManagerLogLevel.fromName("none"));
        assertEquals(OSSManagerLogLevel.STATISTIC, OSSManagerLogLevel.fromName("statistic"));
        assertEquals(OSSManagerLogLevel.DETAIL, OSSManagerLogLevel.fromName("detail"));
    }

    @Test
    public void testOSSManagerLogLevelFromNameCaseInsensitive() {
        assertEquals(OSSManagerLogLevel.NONE, OSSManagerLogLevel.fromName("NONE"));
        assertEquals(OSSManagerLogLevel.STATISTIC, OSSManagerLogLevel.fromName("Statistic"));
    }

    @Test
    public void testOSSManagerLogLevelFromNameInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> OSSManagerLogLevel.fromName("invalid"));
    }

    // ==================== OssStatisticTypeEnum Tests ====================

    @Test
    public void testStatisticTypeEnumValues() {
        OssStatisticTypeEnum[] values = OssStatisticTypeEnum.values();
        assertEquals(4, values.length);
    }

    @Test
    public void testStatisticTypeEnumCounter() {
        assertEquals(OssStatisticTypeEnum.TYPE_COUNTER,
                OssStatisticTypeEnum.valueOf("TYPE_COUNTER"));
    }

    @Test
    public void testStatisticTypeEnumDuration() {
        assertEquals(OssStatisticTypeEnum.TYPE_DURATION,
                OssStatisticTypeEnum.valueOf("TYPE_DURATION"));
    }

    @Test
    public void testStatisticTypeEnumGauge() {
        assertEquals(OssStatisticTypeEnum.TYPE_GAUGE,
                OssStatisticTypeEnum.valueOf("TYPE_GAUGE"));
    }

    @Test
    public void testStatisticTypeEnumQuantile() {
        assertEquals(OssStatisticTypeEnum.TYPE_QUANTILE,
                OssStatisticTypeEnum.valueOf("TYPE_QUANTILE"));
    }
}
