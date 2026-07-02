package org.apache.hadoop.fs.aliyun.oss.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils;
import org.apache.hadoop.security.ProviderUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Per-Bucket configuration support via
 * {@link AliyunOSSUtils#propagateBucketOptions(Configuration, String)}.
 */
public class TestPerBucketConfig {

    private Configuration conf;

    @BeforeEach
    void setUp() {
        conf = new Configuration(false);
    }

    @Test
    void testPropagateBucketOptions_overridesGlobalEndpoint() {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou-internal.aliyuncs.com");
        conf.set("fs.oss.bucket.data-shanghai.endpoint",
                "oss-cn-shanghai-internal.aliyuncs.com");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "data-shanghai");

        assertEquals("oss-cn-shanghai-internal.aliyuncs.com",
                result.get(ENDPOINT_KEY));
    }

    @Test
    void testPropagateBucketOptions_overridesMultipleKeys() {
        conf.set(ACCESS_KEY_ID, "DEFAULT_AK");
        conf.set(ACCESS_KEY_SECRET, "DEFAULT_SK");
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou.aliyuncs.com");

        conf.set("fs.oss.bucket.my-bucket." + ACCESS_KEY_ID.substring("fs.oss.".length()),
                "BUCKET_AK");
        conf.set("fs.oss.bucket.my-bucket." + ACCESS_KEY_SECRET.substring("fs.oss.".length()),
                "BUCKET_SK");
        conf.set("fs.oss.bucket.my-bucket." + ENDPOINT_KEY.substring("fs.oss.".length()),
                "oss-cn-beijing.aliyuncs.com");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "my-bucket");

        assertEquals("BUCKET_AK", result.get(ACCESS_KEY_ID));
        assertEquals("BUCKET_SK", result.get(ACCESS_KEY_SECRET));
        assertEquals("oss-cn-beijing.aliyuncs.com", result.get(ENDPOINT_KEY));
    }

    @Test
    void testPropagateBucketOptions_noOverrideFallsBackToGlobal() {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou-internal.aliyuncs.com");
        conf.set(ACCESS_KEY_ID, "GLOBAL_AK");
        // No per-bucket config for "other-bucket"

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "other-bucket");

        assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", result.get(ENDPOINT_KEY));
        assertEquals("GLOBAL_AK", result.get(ACCESS_KEY_ID));
    }

    @Test
    void testPropagateBucketOptions_doesNotAffectOtherBuckets() {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou-internal.aliyuncs.com");
        conf.set("fs.oss.bucket.bucket-a.endpoint", "oss-cn-shanghai.aliyuncs.com");
        conf.set("fs.oss.bucket.bucket-b.endpoint", "oss-cn-beijing.aliyuncs.com");

        // Propagate for bucket-a only (method returns a clone, original conf is untouched)
        Configuration resultA = AliyunOSSUtils.propagateBucketOptions(conf, "bucket-a");
        assertEquals("oss-cn-shanghai.aliyuncs.com", resultA.get(ENDPOINT_KEY));

        // Propagate for bucket-b only
        Configuration resultB = AliyunOSSUtils.propagateBucketOptions(conf, "bucket-b");
        assertEquals("oss-cn-beijing.aliyuncs.com", resultB.get(ENDPOINT_KEY));
    }

    @Test
    void testPropagateBucketOptions_emptyBucketName() {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou.aliyuncs.com");

        // S3A behavior: empty bucket name throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () ->
                AliyunOSSUtils.propagateBucketOptions(conf, ""));
    }

    @Test
    void testPropagateBucketOptions_nullBucketName() {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou.aliyuncs.com");

        // S3A behavior: null bucket name throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () ->
                AliyunOSSUtils.propagateBucketOptions(conf, null));
    }

    @Test
    void testPropagateBucketOptions_overridesCredentialsProvider() {
        conf.set(CREDENTIALS_PROVIDER_KEY, "");
        conf.set("fs.oss.bucket.log-bucket." + CREDENTIALS_PROVIDER_KEY.substring("fs.oss.".length()),
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "log-bucket");

        assertEquals(
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider",
                result.get(CREDENTIALS_PROVIDER_KEY));
    }

    @Test
    void testPropagateBucketOptions_overridesPerformanceParams() {
        conf.set(MULTIPART_UPLOAD_PART_SIZE_KEY, "104857600"); // 100MB
        conf.set("fs.oss.bucket.big-data." + MULTIPART_UPLOAD_PART_SIZE_KEY.substring("fs.oss.".length()),
                "209715200"); // 200MB

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "big-data");

        assertEquals("209715200", result.get(MULTIPART_UPLOAD_PART_SIZE_KEY));
    }

    @Test
    void testPropagateBucketOptions_preservesBucketSpecificKeys() {
        conf.set(ENDPOINT_KEY, "global-endpoint");
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");
        conf.set("fs.oss.bucket.other-bucket.endpoint", "other-endpoint");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "my-bucket");

        // my-bucket's override should take effect
        assertEquals("bucket-endpoint", result.get(ENDPOINT_KEY));
        // other-bucket's config should still be present but not applied
        assertEquals("other-endpoint", result.get("fs.oss.bucket.other-bucket.endpoint"));
    }

    // ===== dev.md L244-L248: per-bucket ECSRAMRoleCredentialsProvider scenario =====

    @Test
    void testPropagateBucketOptions_overridesWithECSRAMRoleProvider() {
        // dev.md L244-L248: log-bucket uses ECS instance RAM role
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set("fs.oss.bucket.log-bucket." + CREDENTIALS_PROVIDER_KEY.substring("fs.oss.".length()),
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "log-bucket");

        assertEquals(
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider",
                result.get(CREDENTIALS_PROVIDER_KEY));
    }

    @Test
    void testPropagateBucketOptions_devMdFullScenario() {
        // Full dev.md L218-L248 scenario: default + two bucket-specific overrides
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou-internal.aliyuncs.com");
        conf.set(ACCESS_KEY_ID, "DEFAULT_AK");
        conf.set(ACCESS_KEY_SECRET, "DEFAULT_SK");

        // bucket "data-shanghai": different endpoint + different AK/SK
        conf.set("fs.oss.bucket.data-shanghai.endpoint", "oss-cn-shanghai-internal.aliyuncs.com");
        conf.set("fs.oss.bucket.data-shanghai.accessKeyId", "SHANGHAI_AK");
        conf.set("fs.oss.bucket.data-shanghai.accessKeySecret", "SHANGHAI_SK");

        // bucket "log-bucket": ECS RAM role auth
        conf.set("fs.oss.bucket.log-bucket.credentials.provider",
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider");

        // Verify data-shanghai overrides (method returns clone, no need to wrap)
        Configuration shanghaiConf = AliyunOSSUtils.propagateBucketOptions(
                conf, "data-shanghai");
        assertEquals("oss-cn-shanghai-internal.aliyuncs.com", shanghaiConf.get(ENDPOINT_KEY));
        assertEquals("SHANGHAI_AK", shanghaiConf.get(ACCESS_KEY_ID));
        assertEquals("SHANGHAI_SK", shanghaiConf.get(ACCESS_KEY_SECRET));

        // Verify log-bucket overrides
        Configuration logConf = AliyunOSSUtils.propagateBucketOptions(
                conf, "log-bucket");
        assertEquals(
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider",
                logConf.get(CREDENTIALS_PROVIDER_KEY));
        // log-bucket still has global AK/SK (not overridden)
        assertEquals("DEFAULT_AK", logConf.get(ACCESS_KEY_ID));

        // Verify a third bucket with no overrides uses global
        Configuration otherConf = AliyunOSSUtils.propagateBucketOptions(
                conf, "other-bucket");
        assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", otherConf.get(ENDPOINT_KEY));
        assertEquals("DEFAULT_AK", otherConf.get(ACCESS_KEY_ID));
    }
    // ===== S3A-style filtering tests =====

    @Test
    void testPropagateBucketOptions_filtersImplKey() {
        // S3A: fs.s3a.bucket.<bucket>.impl should be ignored to prevent
        // overriding the filesystem implementation class
        conf.set(ENDPOINT_KEY, "global-endpoint");
        conf.set("fs.oss.bucket.my-bucket.impl", "com.example.FakeFileSystem");
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "my-bucket");

        // endpoint should be overridden
        assertEquals("bucket-endpoint", result.get(ENDPOINT_KEY));
        // impl should NOT be overridden (filtered out)
        assertNull(result.get("fs.oss.impl"),
                "impl key should be filtered and not propagated");
    }

    @Test
    void testPropagateBucketOptions_filtersNestedBucketPrefix() {
        // S3A: keys starting with "bucket." after stripping the bucket prefix
        // should be ignored to prevent nested bucket overrides
        conf.set(ENDPOINT_KEY, "global-endpoint");
        conf.set("fs.oss.bucket.my-bucket.bucket.nested.key", "nested-value");
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "my-bucket");

        // endpoint should be overridden
        assertEquals("bucket-endpoint", result.get(ENDPOINT_KEY));
        // nested bucket. prefix key should NOT be propagated
        assertNull(result.get("fs.oss.bucket.nested.key"),
                "bucket. prefix keys should be filtered and not propagated");
    }

    @Test
    void testPropagateBucketOptions_returnsClone() {
        // S3A: method returns a new Configuration clone,
        // original should not be modified
        conf.set(ENDPOINT_KEY, "global-endpoint");
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");

        Configuration result = AliyunOSSUtils.propagateBucketOptions(conf, "my-bucket");

        // result should have the override
        assertEquals("bucket-endpoint", result.get(ENDPOINT_KEY));
        // original conf should remain unchanged
        assertEquals("global-endpoint", conf.get(ENDPOINT_KEY));
    }

    // ===== setBucketOption / clearBucketOption / getBucketOption tests =====

    @Test
    void testSetBucketOption_withFullKey() {
        // setBucketOption should strip "fs.oss." prefix if present
        AliyunOSSUtils.setBucketOption(conf, "my-bucket",
                "fs.oss.endpoint", "bucket-endpoint");

        assertEquals("bucket-endpoint",
                conf.get("fs.oss.bucket.my-bucket.endpoint"));
    }

    @Test
    void testSetBucketOption_withBaseKey() {
        // setBucketOption should work with base key (no prefix)
        AliyunOSSUtils.setBucketOption(conf, "my-bucket",
                "endpoint", "bucket-endpoint");

        assertEquals("bucket-endpoint",
                conf.get("fs.oss.bucket.my-bucket.endpoint"));
    }

    @Test
    void testClearBucketOption() {
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");

        AliyunOSSUtils.clearBucketOption(conf, "my-bucket", "fs.oss.endpoint");

        assertNull(conf.get("fs.oss.bucket.my-bucket.endpoint"));
    }

    @Test
    void testClearBucketOption_withBaseKey() {
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");

        AliyunOSSUtils.clearBucketOption(conf, "my-bucket", "endpoint");

        assertNull(conf.get("fs.oss.bucket.my-bucket.endpoint"));
    }

    @Test
    void testGetBucketOption_withFullKey() {
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");

        String value = AliyunOSSUtils.getBucketOption(conf, "my-bucket",
                "fs.oss.endpoint");

        assertEquals("bucket-endpoint", value);
    }

    @Test
    void testGetBucketOption_withBaseKey() {
        conf.set("fs.oss.bucket.my-bucket.endpoint", "bucket-endpoint");

        String value = AliyunOSSUtils.getBucketOption(conf, "my-bucket",
                "endpoint");

        assertEquals("bucket-endpoint", value);
    }

    @Test
    void testGetBucketOption_returnsNullWhenNotSet() {
        String value = AliyunOSSUtils.getBucketOption(conf, "my-bucket",
                "endpoint");

        assertNull(value);
    }

    // ===== Integration tests: propagateBucketOptions + excludeIncompatibleCredentialProviders =====
    // Simulates the actual initialize() flow in AliyunOSSPerformanceFileSystem

    /**
     * Simulate the exact two-step flow in
     * {@link AliyunOSSPerformanceFileSystem#initialize}:
     * <pre>
     *   conf = propagateBucketOptions(conf, bucket);
     *   conf = ProviderUtils.excludeIncompatibleCredentialProviders(conf, ...);
     * </pre>
     * Verify per-bucket overrides are NOT lost after the second assignment.
     */
    @Test
    void testInitializeFlow_perBucketOverridesSurviveExcludeProviders()
            throws IOException {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou.aliyuncs.com");
        conf.set(ACCESS_KEY_ID, "GLOBAL_AK");
        conf.set(ACCESS_KEY_SECRET, "GLOBAL_SK");
        conf.set(MULTIPART_UPLOAD_PART_SIZE_KEY, "104857600");

        // Per-bucket overrides for "data-shanghai"
        conf.set("fs.oss.bucket.data-shanghai.endpoint", "oss-cn-shanghai.aliyuncs.com");
        conf.set("fs.oss.bucket.data-shanghai.accessKeyId", "SHANGHAI_AK");
        conf.set("fs.oss.bucket.data-shanghai.accessKeySecret", "SHANGHAI_SK");
        conf.set("fs.oss.bucket.data-shanghai.multipart.upload.size", "209715200");

        // Step 1: propagateBucketOptions
        Configuration patchedConf = AliyunOSSUtils.propagateBucketOptions(
                conf, "data-shanghai");
        // Step 2: excludeIncompatibleCredentialProviders
        Configuration finalConf = ProviderUtils
                .excludeIncompatibleCredentialProviders(
                        patchedConf, AliyunOSSPerformanceFileSystem.class);

        // Verify: per-bucket overrides must still be present after both steps
        assertEquals("oss-cn-shanghai.aliyuncs.com",
                finalConf.get(ENDPOINT_KEY),
                "endpoint override lost");
        assertEquals("SHANGHAI_AK",
                finalConf.get(ACCESS_KEY_ID),
                "accessKeyId override lost");
        assertEquals("SHANGHAI_SK",
                finalConf.get(ACCESS_KEY_SECRET),
                "accessKeySecret override lost");
        assertEquals("209715200",
                finalConf.get(MULTIPART_UPLOAD_PART_SIZE_KEY),
                "multipart size override lost");
    }

    @Test
    void testInitializeFlow_noPerBucketConfig_globalPreserved()
            throws IOException {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou.aliyuncs.com");
        conf.set(ACCESS_KEY_ID, "GLOBAL_AK");
        conf.set(ACCESS_KEY_SECRET, "GLOBAL_SK");

        Configuration patchedConf = AliyunOSSUtils.propagateBucketOptions(
                conf, "some-bucket");
        Configuration finalConf = ProviderUtils
                .excludeIncompatibleCredentialProviders(
                        patchedConf, AliyunOSSPerformanceFileSystem.class);

        assertEquals("oss-cn-hangzhou.aliyuncs.com",
                finalConf.get(ENDPOINT_KEY));
        assertEquals("GLOBAL_AK", finalConf.get(ACCESS_KEY_ID));
        assertEquals("GLOBAL_SK", finalConf.get(ACCESS_KEY_SECRET));
    }

    @Test
    void testInitializeFlow_perBucketCredentialsProvider_survives()
            throws IOException {
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set("fs.oss.bucket.log-bucket.credentials.provider",
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider");

        Configuration patchedConf = AliyunOSSUtils.propagateBucketOptions(
                conf, "log-bucket");
        Configuration finalConf = ProviderUtils
                .excludeIncompatibleCredentialProviders(
                        patchedConf, AliyunOSSPerformanceFileSystem.class);

        assertEquals(
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider",
                finalConf.get(CREDENTIALS_PROVIDER_KEY),
                "credentials.provider override lost");
    }

    @Test
    void testInitializeFlow_originalConfUnmodified()
            throws IOException {
        conf.set(ENDPOINT_KEY, "oss-cn-hangzhou.aliyuncs.com");
        conf.set(ACCESS_KEY_ID, "GLOBAL_AK");
        conf.set("fs.oss.bucket.my-bucket.endpoint",
                "oss-cn-shanghai.aliyuncs.com");
        conf.set("fs.oss.bucket.my-bucket.accessKeyId", "BUCKET_AK");

        Configuration patchedConf = AliyunOSSUtils.propagateBucketOptions(
                conf, "my-bucket");
        ProviderUtils.excludeIncompatibleCredentialProviders(
                patchedConf, AliyunOSSPerformanceFileSystem.class);

        // Original conf must remain unchanged
        assertEquals("oss-cn-hangzhou.aliyuncs.com",
                conf.get(ENDPOINT_KEY),
                "Original conf endpoint was mutated");
        assertEquals("GLOBAL_AK",
                conf.get(ACCESS_KEY_ID),
                "Original conf accessKeyId was mutated");
    }
}
