package org.apache.hadoop.fs.aliyun.oss.v2;

import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.CredentialsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for credential provider chain support in
 * {@link AliyunOSSUtils#getCredentialsProvider(Configuration)}.
 */
public class TestCredentialsProvider {

    private Configuration conf;

    @BeforeEach
    void setUp() {
        conf = new Configuration(false);
    }

    @Test
    void testGetCredentialsProvider_withStaticAKSK() throws Exception {
        conf.set(ACCESS_KEY_ID, "test-ak-id");
        conf.set(ACCESS_KEY_SECRET, "test-ak-secret");
        // No provider configured → falls back to DefaultCredentialProviderChain,
        // which fails → then falls back to static credentials
        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertNotNull(provider.getCredentials());
        assertEquals("test-ak-id", provider.getCredentials().accessKeyId());
        assertEquals("test-ak-secret", provider.getCredentials().accessKeySecret());
    }

    @Test
    void testGetCredentialsProvider_withStaticAKSKAndSTS() throws Exception {
        conf.set(ACCESS_KEY_ID, "test-ak-id");
        conf.set(ACCESS_KEY_SECRET, "test-ak-secret");
        conf.set(SECURITY_TOKEN, "test-sts-token");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertNotNull(provider.getCredentials());
        assertEquals("test-ak-id", provider.getCredentials().accessKeyId());
        assertEquals("test-sts-token", provider.getCredentials().securityToken());
    }

    @Test
    void testGetCredentialsProvider_withSimpleProvider() throws Exception {
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "simple-ak-id");
        conf.set(ACCESS_KEY_SECRET, "simple-ak-secret");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertNotNull(provider.getCredentials());
        assertEquals("simple-ak-id", provider.getCredentials().accessKeyId());
    }

    @Test
    void testGetCredentialsProvider_withInvalidClassName() {
        conf.set(CREDENTIALS_PROVIDER_KEY, "com.example.NonExistentProvider");
        conf.set(ACCESS_KEY_ID, "ak");
        conf.set(ACCESS_KEY_SECRET, "sk");

        // Unified loop: all providers fail → CredentialsException
        assertThrows(CredentialsException.class, () -> {
            AliyunOSSUtils.getCredentialsProvider(conf);
        });
    }

    @Test
    void testGetCredentialsProvider_withMultipleProviders_firstSucceeds() throws Exception {
        // First provider: SimpleCredentialsProvider with valid AK/SK → succeeds
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider,"
                        + "com.example.NonExistentProvider");
        conf.set(ACCESS_KEY_ID, "chain-ak-id");
        conf.set(ACCESS_KEY_SECRET, "chain-ak-secret");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("chain-ak-id", provider.getCredentials().accessKeyId());
    }

    @Test
    void testGetCredentialsProvider_withMultipleProviders_firstFails() throws Exception {
        // First provider: NonExistent → fails
        // Second provider: SimpleCredentialsProvider with valid AK/SK → succeeds
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "com.example.NonExistentProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "fallback-ak-id");
        conf.set(ACCESS_KEY_SECRET, "fallback-ak-secret");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("fallback-ak-id", provider.getCredentials().accessKeyId());
    }

    @Test
    void testGetCredentialsProvider_noAKSK_noProvider_throwsException() {
        // No AK/SK, no provider → DefaultCredentialProviderChain may fail,
        // then fallback to static which also fails
        assertThrows(Exception.class, () -> {
            AliyunOSSUtils.getCredentialsProvider(conf);
        });
    }

    @Test
    void testGetCredentialsProvider_allProvidersFail_throwsException() {
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "com.example.FakeProvider1,com.example.FakeProvider2");

        assertThrows(Exception.class, () -> {
            AliyunOSSUtils.getCredentialsProvider(conf);
        });
    }

    @Test
    void testGetCredentialsProvider_emptyProviderWithWhitespace() throws Exception {
        // Provider string with just whitespace → treated as empty → default chain
        conf.set(CREDENTIALS_PROVIDER_KEY, "  ");
        conf.set(ACCESS_KEY_ID, "ws-ak");
        conf.set(ACCESS_KEY_SECRET, "ws-sk");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("ws-ak", provider.getCredentials().accessKeyId());
    }

    // ===== Tests for dev.md L149-L153: SimpleCredentialsProvider + ECSRAMRoleCredentialsProvider chain =====

    @Test
    void testGetCredentialsProvider_simpleThenECSRAM_firstSucceeds() throws Exception {
        // dev.md L149-L153 scenario:
        // SimpleCredentialsProvider listed first with valid AK/SK → succeeds immediately
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "chain-ak-id");
        conf.set(ACCESS_KEY_SECRET, "chain-ak-secret");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("chain-ak-id", provider.getCredentials().accessKeyId());
    }

    @Test
    void testGetCredentialsProvider_ECSRAMThenSimple_firstFailsFallback() throws Exception {
        // ECSRAMRoleCredentialsProvider listed first → fails (not on ECS instance)
        // SimpleCredentialsProvider listed second → succeeds with AK/SK
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "fallback-ak-id");
        conf.set(ACCESS_KEY_SECRET, "fallback-ak-secret");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("fallback-ak-id", provider.getCredentials().accessKeyId());
    }

    @Test
    void testGetCredentialsProvider_ECSRAMOnly_noECS_fails() {
        // ECSRAMRoleCredentialsProvider alone without ECS instance → should fail
        // (credentials-java SDK may create the client but getCredential() will fail
        //  when not on an ECS instance with a RAM role)
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider");

        // The provider may be created successfully but getCredentials() will fail,
        // OR the constructor itself may throw if credentials-java is not on classpath
        assertThrows(Exception.class, () -> {
            CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
            // If provider was created, calling getCredentials should fail
            provider.getCredentials();
        });
    }

    @Test
    void testGetCredentialsProvider_ECSRAMWithRoleName() throws Exception {
        // Test that ECSRAMRoleCredentialsProvider can be configured with a role name
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ECS_RAM_ROLE_NAME_KEY, "MyTestRole");
        conf.set(ACCESS_KEY_ID, "ecs-fallback-ak");
        conf.set(ACCESS_KEY_SECRET, "ecs-fallback-sk");

        // ECS role provider will fail (not on ECS), fallback to Simple
        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("ecs-fallback-ak", provider.getCredentials().accessKeyId());
    }

    // ===== Tests for RAMRoleARNCredentialsProvider =====

    @Test
    void testGetCredentialsProvider_RAMRoleARN_missingRoleArn_throwsException() {
        // RAMRoleARNCredentialsProvider without role ARN → creation fails
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.RAMRoleARNCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "test-ak");
        conf.set(ACCESS_KEY_SECRET, "test-sk");
        // No ASSUMED_ROLE_ARN set

        assertThrows(Exception.class, () -> {
            AliyunOSSUtils.getCredentialsProvider(conf);
        });
    }

    @Test
    void testGetCredentialsProvider_RAMRoleARN_chainFallback() throws Exception {
        // RAMRoleARN fails (no real STS), falls back to Simple
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.RAMRoleARNCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "ram-fallback-ak");
        conf.set(ACCESS_KEY_SECRET, "ram-fallback-sk");
        conf.set(ASSUMED_ROLE_ARN, "acs:ram::123456:role/test-role");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("ram-fallback-ak", provider.getCredentials().accessKeyId());
    }

    // ===== Tests for OIDCRoleARNCredentialsProvider =====

    @Test
    void testGetCredentialsProvider_OIDC_missingConfig_throwsException() {
        // OIDCRoleARNCredentialsProvider without required config → creation fails
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.OIDCRoleARNCredentialsProvider");
        // No OIDC config set

        assertThrows(Exception.class, () -> {
            AliyunOSSUtils.getCredentialsProvider(conf);
        });
    }

    @Test
    void testGetCredentialsProvider_OIDC_chainFallback() throws Exception {
        // OIDC fails (no real K8s environment), falls back to Simple
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.OIDCRoleARNCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "oidc-fallback-ak");
        conf.set(ACCESS_KEY_SECRET, "oidc-fallback-sk");
        conf.set(OIDC_ROLE_ARN, "acs:ram::123456:role/oidc-role");
        conf.set(OIDC_PROVIDER_ARN, "acs:ram::123456:oidc-provider/test");
        conf.set(OIDC_TOKEN_FILE, "/tmp/nonexistent-token");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("oidc-fallback-ak", provider.getCredentials().accessKeyId());
    }

    // ===== Test for non-existent provider class (e.g. "OIDCProvider") =====

    @Test
    void testGetCredentialsProvider_nonExistentClass_skippedInChain() throws Exception {
        // A class name that doesn't exist (like "OIDCProvider") should be skipped
        // gracefully, and the chain should fall through to the next working provider
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "com.example.NonExistentOIDCProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "survived-ak");
        conf.set(ACCESS_KEY_SECRET, "survived-sk");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("survived-ak", provider.getCredentials().accessKeyId());
    }

    @Test
    void testGetCredentialsProvider_fullChain_fourProviders() throws Exception {
        // Full chain: ECSRAM → RAMRoleARN → OIDC → Simple
        // Only Simple should succeed (others lack their runtime environment)
        conf.set(CREDENTIALS_PROVIDER_KEY,
                "org.apache.hadoop.fs.aliyun.oss.v2.credentials.ECSRAMRoleCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.RAMRoleARNCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.OIDCRoleARNCredentialsProvider,"
                        + "org.apache.hadoop.fs.aliyun.oss.v2.credentials.SimpleCredentialsProvider");
        conf.set(ACCESS_KEY_ID, "full-chain-ak");
        conf.set(ACCESS_KEY_SECRET, "full-chain-sk");
        conf.set(ASSUMED_ROLE_ARN, "acs:ram::123456:role/test-role");
        conf.set(OIDC_ROLE_ARN, "acs:ram::123456:role/oidc-role");
        conf.set(OIDC_PROVIDER_ARN, "acs:ram::123456:oidc-provider/test");
        conf.set(OIDC_TOKEN_FILE, "/tmp/nonexistent-token");

        CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(conf);
        assertNotNull(provider);
        assertEquals("full-chain-ak", provider.getCredentials().accessKeyId());
    }
}
