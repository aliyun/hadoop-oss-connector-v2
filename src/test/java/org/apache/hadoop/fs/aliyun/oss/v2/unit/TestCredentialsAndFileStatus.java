package org.apache.hadoop.fs.aliyun.oss.v2.unit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.OSSFileStatus;
import org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider;
import org.apache.hadoop.fs.aliyun.oss.v2.model.AliyunOSSDirEmptyFlag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OSSFileStatus and SimpleCredentialsProvider.
 */
public class TestCredentialsAndFileStatus {

    // ==================== OSSFileStatus ====================

    @Test
    public void testOSSFileStatusBasicConstructor() {
        Path path = new Path("oss://bucket/file.txt");
        OSSFileStatus status = new OSSFileStatus(
                1024, false, 1, 128 * 1024, System.currentTimeMillis(), path, "owner");

        assertEquals(1024, status.getLen());
        assertFalse(status.isDirectory());
        assertEquals(path, status.getPath());
        assertEquals("owner", status.getOwner());
        assertEquals("owner", status.getGroup());
        assertEquals(AliyunOSSDirEmptyFlag.UNKNOWN, status.getEmptyFlag());
        assertNull(status.geteTag());
        assertNull(status.getVersionId());
    }

    @Test
    public void testOSSFileStatusWithETagAndVersionId() {
        Path path = new Path("oss://bucket/file.txt");
        OSSFileStatus status = new OSSFileStatus(
                2048, false, 1, 128 * 1024, System.currentTimeMillis(),
                path, "user", "etag-abc", "v1");

        assertEquals("etag-abc", status.geteTag());
        assertEquals("v1", status.getVersionId());
    }

    @Test
    public void testOSSFileStatusDirectory() {
        Path path = new Path("oss://bucket/dir/");
        OSSFileStatus status = new OSSFileStatus(
                AliyunOSSDirEmptyFlag.EMPTY, 0, 1, 0,
                System.currentTimeMillis(), path, "user");

        assertTrue(status.isDirectory());
        assertEquals(AliyunOSSDirEmptyFlag.EMPTY, status.getEmptyFlag());
    }

    @Test
    public void testOSSFileStatusDirectoryNotEmpty() {
        Path path = new Path("oss://bucket/dir/");
        OSSFileStatus status = new OSSFileStatus(
                AliyunOSSDirEmptyFlag.NOT_EMPTY, 0, 1, 0,
                System.currentTimeMillis(), path, "user");

        assertEquals(AliyunOSSDirEmptyFlag.NOT_EMPTY, status.getEmptyFlag());
    }

    @Test
    public void testOSSFileStatusDirectoryWithETag() {
        Path path = new Path("oss://bucket/dir/");
        OSSFileStatus status = new OSSFileStatus(
                AliyunOSSDirEmptyFlag.UNKNOWN, 0, 1, 0,
                System.currentTimeMillis(), path, "user", "etag", "ver");

        assertEquals("etag", status.geteTag());
        assertEquals("ver", status.getVersionId());
    }

    @Test
    public void testOSSFileStatusSetEmptyFlag() {
        Path path = new Path("oss://bucket/dir/");
        OSSFileStatus status = new OSSFileStatus(
                0, true, 1, 0, System.currentTimeMillis(), path, "user");

        assertEquals(AliyunOSSDirEmptyFlag.UNKNOWN, status.getEmptyFlag());
        status.setEmptyFlag(AliyunOSSDirEmptyFlag.EMPTY);
        assertEquals(AliyunOSSDirEmptyFlag.EMPTY, status.getEmptyFlag());
    }

    @Test
    public void testOSSFileStatusEquality() {
        Path path = new Path("oss://bucket/file.txt");
        long modTime = System.currentTimeMillis();
        OSSFileStatus s1 = new OSSFileStatus(100, false, 1, 128, modTime, path, "user");
        OSSFileStatus s2 = new OSSFileStatus(100, false, 1, 128, modTime, path, "user");
        assertEquals(s1, s2);
    }

    // ==================== SimpleCredentialsProvider ====================

    @Test
    public void testSimpleCredentialsProviderMissingAKThrows() {
        Configuration conf = new Configuration(false);
        // No AK/SK set
        assertThrows(Exception.class, () -> new SimpleCredentialsProvider(conf));
    }

    @Test
    public void testSimpleCredentialsProviderMissingSKThrows() {
        Configuration conf = new Configuration(false);
        conf.set("fs.oss.accessKeyId", "myAK");
        // No SK
        assertThrows(Exception.class, () -> new SimpleCredentialsProvider(conf));
    }

    @Test
    public void testSimpleCredentialsProviderEmptyAKThrows() {
        Configuration conf = new Configuration(false);
        conf.set("fs.oss.accessKeyId", "");
        conf.set("fs.oss.accessKeySecret", "mySK");
        assertThrows(Exception.class, () -> new SimpleCredentialsProvider(conf));
    }

    @Test
    public void testSimpleCredentialsProviderValid() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.oss.accessKeyId", "testAK");
        conf.set("fs.oss.accessKeySecret", "testSK");
        SimpleCredentialsProvider provider = new SimpleCredentialsProvider(conf);
        assertNotNull(provider);
        assertNotNull(provider.getCredentials());
    }

    @Test
    public void testSimpleCredentialsProviderWithSTS() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.oss.accessKeyId", "testAK");
        conf.set("fs.oss.accessKeySecret", "testSK");
        conf.set("fs.oss.securityToken", "testToken");
        SimpleCredentialsProvider provider = new SimpleCredentialsProvider(conf);
        assertNotNull(provider);
        assertNotNull(provider.getCredentials());
    }
}
