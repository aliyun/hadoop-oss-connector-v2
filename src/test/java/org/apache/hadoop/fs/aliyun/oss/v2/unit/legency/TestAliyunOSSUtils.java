package org.apache.hadoop.fs.aliyun.oss.v2.unit.legency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.Constants;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.FileStatusAcceptor;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ObjectSummaryParam;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AliyunOSSUtils and FileStatusAcceptor.
 */
public class TestAliyunOSSUtils {

    // ==================== maybeAddTrailingSlash ====================

    @Test
    public void testMaybeAddTrailingSlashNull() {
        assertNull(AliyunOSSUtils.maybeAddTrailingSlash(null));
    }

    @Test
    public void testMaybeAddTrailingSlashEmpty() {
        assertEquals("", AliyunOSSUtils.maybeAddTrailingSlash(""));
    }

    @Test
    public void testMaybeAddTrailingSlashAlreadyHas() {
        assertEquals("path/", AliyunOSSUtils.maybeAddTrailingSlash("path/"));
    }

    @Test
    public void testMaybeAddTrailingSlashAdds() {
        assertEquals("path/", AliyunOSSUtils.maybeAddTrailingSlash("path"));
    }

    // ==================== objectRepresentsDirectory ====================

    @Test
    public void testObjectRepresentsDirectory() {
        assertTrue(AliyunOSSUtils.objectRepresentsDirectory("dir/", 0));
        assertFalse(AliyunOSSUtils.objectRepresentsDirectory("dir/", 10));
        assertFalse(AliyunOSSUtils.objectRepresentsDirectory("file", 0));
        assertFalse(AliyunOSSUtils.objectRepresentsDirectory("", 0));
        assertFalse(AliyunOSSUtils.objectRepresentsDirectory(null, 0));
    }

    // ==================== calculatePartSize ====================

    @Test
    public void testCalculatePartSizeSmallFile() {
        long partSize = AliyunOSSUtils.calculatePartSize(1024, 100 * 1024);
        assertEquals(100 * 1024, partSize);
    }

    @Test
    public void testCalculatePartSizeLargeFile() {
        // When contentLength / 10000 + 1 > minPartSize, use computed size
        long contentLength = 10001L * 100 * 1024;
        long minPartSize = 100 * 1024;
        long partSize = AliyunOSSUtils.calculatePartSize(contentLength, minPartSize);
        assertTrue(partSize >= minPartSize);
        assertTrue(contentLength / partSize < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);
    }

    @Test
    public void testCalculatePartSizeExceedsLimit() {
        long partSize = AliyunOSSUtils.calculatePartSize(
                10001 * 100 * 1024L, 100 * 1024);
        assertTrue(10001 * 100 * 1024L / partSize
                < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);
    }

    // ==================== intPositiveOption ====================

    @Test
    public void testIntPositiveOptionPositive() {
        Configuration conf = new Configuration();
        conf.setInt("test.key", 10);
        assertEquals(10, AliyunOSSUtils.intPositiveOption(conf, "test.key", 5));
    }

    @Test
    public void testIntPositiveOptionZero() {
        Configuration conf = new Configuration();
        conf.setInt("test.key", 0);
        // zero falls back to default
        assertEquals(5, AliyunOSSUtils.intPositiveOption(conf, "test.key", 5));
    }

    @Test
    public void testIntPositiveOptionNegative() {
        Configuration conf = new Configuration();
        conf.setInt("test.key", -1);
        assertEquals(5, AliyunOSSUtils.intPositiveOption(conf, "test.key", 5));
    }

    @Test
    public void testIntPositiveOptionNotSet() {
        Configuration conf = new Configuration();
        assertEquals(5, AliyunOSSUtils.intPositiveOption(conf, "test.key", 5));
    }

    // ==================== intOption ====================

    @Test
    public void testIntOptionAboveMin() {
        Configuration conf = new Configuration();
        conf.setInt("test.key", 10);
        assertEquals(10, AliyunOSSUtils.intOption(conf, "test.key", 5, 1));
    }

    @Test
    public void testIntOptionAtMin() {
        Configuration conf = new Configuration();
        conf.setInt("test.key", 5);
        assertEquals(5, AliyunOSSUtils.intOption(conf, "test.key", 5, 5));
    }

    @Test
    public void testIntOptionBelowMin() {
        Configuration conf = new Configuration();
        conf.setInt("test.key", 3);
        assertThrows(IllegalArgumentException.class,
                () -> AliyunOSSUtils.intOption(conf, "test.key", 5, 5));
    }

    // ==================== longOption ====================

    @Test
    public void testLongOptionAboveMin() {
        Configuration conf = new Configuration();
        conf.setLong("test.key", 100L);
        assertEquals(100L, AliyunOSSUtils.longOption(conf, "test.key", 50L, 10L));
    }

    @Test
    public void testLongOptionBelowMin() {
        Configuration conf = new Configuration();
        conf.setLong("test.key", 5L);
        assertThrows(IllegalArgumentException.class,
                () -> AliyunOSSUtils.longOption(conf, "test.key", 50L, 10L));
    }

    // ==================== getMultipartSizeProperty ====================

    @Test
    public void testGetMultipartSizePropertyNormal() {
        Configuration conf = new Configuration();
        conf.setLong("test.multipart.size", 200 * 1024);
        assertEquals(200 * 1024,
                AliyunOSSUtils.getMultipartSizeProperty(conf, "test.multipart.size", 100 * 1024));
    }

    @Test
    public void testGetMultipartSizePropertyTooSmall() {
        Configuration conf = new Configuration();
        conf.setLong("test.multipart.size", 10);
        // Too small, rounded up to MULTIPART_MIN_SIZE
        assertEquals(Constants.MULTIPART_MIN_SIZE,
                AliyunOSSUtils.getMultipartSizeProperty(conf, "test.multipart.size", 100 * 1024));
    }

    @Test
    public void testGetMultipartSizePropertyTooLarge() {
        Configuration conf = new Configuration();
        conf.setLong("test.multipart.size", Long.MAX_VALUE);
        // Capped at Integer.MAX_VALUE
        assertEquals(Integer.MAX_VALUE,
                AliyunOSSUtils.getMultipartSizeProperty(conf, "test.multipart.size", 100 * 1024));
    }

    // ==================== formatInstant ====================

    @Test
    public void testFormatInstant() {
        java.time.Instant instant = java.time.Instant.parse("2025-01-15T10:30:00Z");
        String result = AliyunOSSUtils.formatInstant(instant);
        assertEquals("2025-01-15T10:30:00Z", result);
    }

    // ==================== setBucketOption / getBucketOption / clearBucketOption ====================

    @Test
    public void testSetAndGetBucketOption() {
        Configuration conf = new Configuration();
        AliyunOSSUtils.setBucketOption(conf, "my-bucket", "fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        String value = AliyunOSSUtils.getBucketOption(conf, "my-bucket", "fs.oss.endpoint");
        assertEquals("oss-cn-hangzhou.aliyuncs.com", value);
    }

    @Test
    public void testSetBucketOptionWithoutPrefix() {
        Configuration conf = new Configuration();
        AliyunOSSUtils.setBucketOption(conf, "my-bucket", "endpoint", "value");
        String value = AliyunOSSUtils.getBucketOption(conf, "my-bucket", "endpoint");
        assertEquals("value", value);
    }

    @Test
    public void testClearBucketOption() {
        Configuration conf = new Configuration();
        AliyunOSSUtils.setBucketOption(conf, "my-bucket", "fs.oss.endpoint", "value");
        assertNotNull(AliyunOSSUtils.getBucketOption(conf, "my-bucket", "fs.oss.endpoint"));
        AliyunOSSUtils.clearBucketOption(conf, "my-bucket", "fs.oss.endpoint");
        assertNull(AliyunOSSUtils.getBucketOption(conf, "my-bucket", "fs.oss.endpoint"));
    }

    @Test
    public void testGetBucketOptionNotSet() {
        Configuration conf = new Configuration();
        assertNull(AliyunOSSUtils.getBucketOption(conf, "my-bucket", "nonexistent.key"));
    }

    // ==================== propagateBucketOptions ====================

    @Test
    public void testPropagateBucketOptions() {
        Configuration source = new Configuration();
        source.set("fs.oss.bucket.testbucket.endpoint", "oss-cn-beijing.aliyuncs.com");
        source.set("fs.oss.bucket.testbucket.accessKeyId", "ak");

        Configuration dest = AliyunOSSUtils.propagateBucketOptions(source, "testbucket");
        assertEquals("oss-cn-beijing.aliyuncs.com", dest.get("fs.oss.endpoint"));
        assertEquals("ak", dest.get("fs.oss.accessKeyId"));
    }

    @Test
    public void testPropagateBucketOptionsIgnoresImpl() {
        Configuration source = new Configuration();
        source.set("fs.oss.bucket.testbucket.impl", "SomeClass");
        source.set("fs.oss.bucket.testbucket.endpoint", "ep");

        Configuration dest = AliyunOSSUtils.propagateBucketOptions(source, "testbucket");
        assertEquals("ep", dest.get("fs.oss.endpoint"));
        // impl should not be propagated
        assertNull(dest.get("fs.oss.impl"));
    }

    @Test
    public void testPropagateBucketOptionsIgnoresBucketPrefix() {
        Configuration source = new Configuration();
        source.set("fs.oss.bucket.testbucket.bucket.something", "value");

        Configuration dest = AliyunOSSUtils.propagateBucketOptions(source, "testbucket");
        // keys starting with "bucket." after stripping should be ignored
        assertNull(dest.get("fs.oss.bucket.something"));
    }

    @Test
    public void testPropagateBucketOptionsEmptyBucket() {
        Configuration source = new Configuration();
        assertThrows(IllegalArgumentException.class,
                () -> AliyunOSSUtils.propagateBucketOptions(source, ""));
    }

    @Test
    public void testPropagateBucketOptionsNullBucket() {
        Configuration source = new Configuration();
        assertThrows(IllegalArgumentException.class,
                () -> AliyunOSSUtils.propagateBucketOptions(source, null));
    }

    // ==================== FileStatusAcceptor.AcceptFilesOnly ====================

    @Test
    public void testAcceptFilesOnlyAcceptsNonBase() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptFilesOnly acceptor =
                new FileStatusAcceptor.AcceptFilesOnly(basePath);

        ObjectSummaryParam summary = new ObjectSummaryParam();
        summary.setKey("file.txt");
        summary.setSize(100);

        Path filePath = new Path("oss://bucket/file.txt");
        assertTrue(acceptor.accept(filePath, summary));
    }

    @Test
    public void testAcceptFilesOnlyRejectsBase() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptFilesOnly acceptor =
                new FileStatusAcceptor.AcceptFilesOnly(basePath);

        ObjectSummaryParam summary = new ObjectSummaryParam();
        summary.setKey("base/");
        summary.setSize(0);

        assertFalse(acceptor.accept(basePath, summary));
    }

    @Test
    public void testAcceptFilesOnlyRejectsDirectories() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptFilesOnly acceptor =
                new FileStatusAcceptor.AcceptFilesOnly(basePath);

        ObjectSummaryParam summary = new ObjectSummaryParam();
        summary.setKey("subdir/");
        summary.setSize(0);

        Path dirPath = new Path("oss://bucket/subdir/");
        assertFalse(acceptor.accept(dirPath, summary));
    }

    @Test
    public void testAcceptFilesOnlyRejectsPrefixes() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptFilesOnly acceptor =
                new FileStatusAcceptor.AcceptFilesOnly(basePath);

        assertFalse(acceptor.accept(new Path("oss://bucket/prefix"), "prefix"));
    }

    // ==================== FileStatusAcceptor.AcceptAllButSelf ====================

    @Test
    public void testAcceptAllButSelfAcceptsOther() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptAllButSelf acceptor =
                new FileStatusAcceptor.AcceptAllButSelf(basePath);

        ObjectSummaryParam summary = new ObjectSummaryParam();
        summary.setKey("other.txt");
        summary.setSize(100);

        Path otherPath = new Path("oss://bucket/other.txt");
        assertTrue(acceptor.accept(otherPath, summary));
    }

    @Test
    public void testAcceptAllButSelfRejectsSelf() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptAllButSelf acceptor =
                new FileStatusAcceptor.AcceptAllButSelf(basePath);

        ObjectSummaryParam summary = new ObjectSummaryParam();
        summary.setKey("base/");
        summary.setSize(0);

        assertFalse(acceptor.accept(basePath, summary));
    }

    @Test
    public void testAcceptAllButSelfAcceptsPrefixesOtherThanSelf() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptAllButSelf acceptor =
                new FileStatusAcceptor.AcceptAllButSelf(basePath);

        Path otherPath = new Path("oss://bucket/other/");
        assertTrue(acceptor.accept(otherPath, "other/"));
    }

    @Test
    public void testAcceptAllButSelfRejectsSelfPrefix() {
        Path basePath = new Path("oss://bucket/base/");
        FileStatusAcceptor.AcceptAllButSelf acceptor =
                new FileStatusAcceptor.AcceptAllButSelf(basePath);

        assertFalse(acceptor.accept(basePath, "base/"));
    }
}
