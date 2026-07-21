package org.apache.hadoop.fs.aliyun.oss.v2.credentials;

import com.aliyun.sdk.service.oss2.credentials.Credentials;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.CredentialsException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * A {@link CredentialsProvider} implementation that uses the Alibaba Cloud
 * {@code credentials-java} SDK's
 * <a href="https://github.com/aliyun/credentials-java">default credential
 * provider chain</a> to resolve credentials.
 * <p>
 * The default chain tries credentials in the following order:
 * <ol>
 *   <li>Environment variables
 *       ({@code ALIBABA_CLOUD_ACCESS_KEY_ID} /
 *        {@code ALIBABA_CLOUD_ACCESS_KEY_SECRET})</li>
 *   <li>Credential file ({@code ~/.alibabacloud/credentials})</li>
 *   <li>ECS instance RAM role (via Instance Metadata Service)</li>
 *   <li>OIDC token (Kubernetes RRSA scenario)</li>
 * </ol>
 * <p>
 * This provider uses reflection to avoid a compile-time dependency on
 * {@code credentials-java}. If the SDK is not on the classpath, the
 * provider will fail gracefully.
 * <p>
 * This provider is used as the fallback when no explicit
 * {@code fs.oss.credentials.provider} is configured.
 */
public class DefaultCredentialProviderChain implements CredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultCredentialProviderChain.class);

    private static final String CLIENT_CLASS = "com.aliyun.credentials.Client";
    private static final String CONFIG_CLASS = "com.aliyun.credentials.models.Config";

    private final Object credentialsClient;

    /**
     * Constructor required for reflective instantiation by
     * {@code AliyunOSSUtils.createSingleProvider()}.
     *
     * @param conf Hadoop configuration (currently unused, but required by
     *             the reflection contract)
     * @throws Exception if the credentials client cannot be created
     */
    public DefaultCredentialProviderChain(Configuration conf) throws Exception {
        // Use reflection to create the credentials-java Client
        Class<?> configClass = Class.forName(CONFIG_CLASS);
        Object config = configClass.getDeclaredConstructor().newInstance();
        Method setType = configClass.getMethod("setType", String.class);
        setType.invoke(config, "default");

        Class<?> clientClass = Class.forName(CLIENT_CLASS);
        this.credentialsClient = clientClass
                .getDeclaredConstructor(configClass)
                .newInstance(config);

        LOG.debug("Initialized DefaultCredentialProviderChain with Alibaba Cloud "
                + "default credential chain");
    }

    @Override
    public Credentials getCredentials() {
        try {
            // Call credentialsClient.getCredential() via reflection
            Method getCredential = credentialsClient.getClass().getMethod("getCredential");
            Object credential = getCredential.invoke(credentialsClient);

            if (credential == null) {
                throw new CredentialsException(
                        "Default credential chain returned null credential. "
                                + "Please ensure that credentials are configured properly.");
            }

            // Get fields via reflection
            String accessKeyId = (String) credential.getClass()
                    .getMethod("getAccessKeyId").invoke(credential);
            String accessKeySecret = (String) credential.getClass()
                    .getMethod("getAccessKeySecret").invoke(credential);
            String securityToken = (String) credential.getClass()
                    .getMethod("getSecurityToken").invoke(credential);

            if (accessKeyId == null || accessKeyId.isEmpty()
                    || accessKeySecret == null || accessKeySecret.isEmpty()) {
                throw new CredentialsException(
                        "Default credential chain returned empty accessKeyId or accessKeySecret");
            }

            String type = null;
            try {
                type = (String) credential.getClass().getMethod("getType").invoke(credential);
            } catch (Exception e) {
                // ignore
            }
            LOG.debug("Successfully obtained credentials from default chain (type={})", type);

            if (securityToken != null && !securityToken.isEmpty()) {
                return new Credentials(accessKeyId, accessKeySecret, securityToken);
            } else {
                return new Credentials(accessKeyId, accessKeySecret);
            }
        } catch (CredentialsException e) {
            throw e;
        } catch (Exception e) {
            throw new CredentialsException(
                    "Failed to obtain credentials from default credential chain: " + e.getMessage(), e);
        }
    }
}
