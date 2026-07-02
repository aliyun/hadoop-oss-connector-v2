package org.apache.hadoop.fs.aliyun.oss.v2.credentials;

import com.aliyun.sdk.service.oss2.credentials.Credentials;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.CredentialsException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * A {@link CredentialsProvider} that obtains credentials via OIDC
 * (OpenID Connect) Role ARN, commonly used in Kubernetes RRSA
 * (RAM Roles for Service Accounts) scenarios.
 * <p>
 * Internally uses the Alibaba Cloud {@code credentials-java} SDK with
 * {@code Config.setType("oidc_role_arn")}. The provider reads an OIDC
 * token from a file (typically a Kubernetes projected service account
 * token), exchanges it for temporary STS credentials by assuming a
 * RAM role via the OIDC identity provider.
 * <p>
 * Required configuration:
 * <ul>
 *   <li>{@code fs.oss.oidc.role.arn} — the ARN of the RAM role to assume</li>
 *   <li>{@code fs.oss.oidc.provider.arn} — the ARN of the OIDC identity
 *       provider</li>
 *   <li>{@code fs.oss.oidc.token.file} — path to the OIDC token file
 *       (e.g., {@code /var/run/secrets/tokens/oidc-token})</li>
 * </ul>
 * <p>
 * Optional configuration:
 * <ul>
 *   <li>{@code fs.oss.oidc.session.name} — session name
 *       (default: "hadoop-oss-oidc-session")</li>
 *   <li>{@code fs.oss.oidc.policy} — a policy to scope down the temporary
 *       credentials (JSON string)</li>
 *   <li>{@code fs.oss.oidc.expiration} — session duration in seconds
 *       (default: 3600)</li>
 * </ul>
 * <p>
 * This provider uses reflection to avoid a compile-time dependency on
 * {@code credentials-java}.
 */
public class OIDCRoleARNCredentialsProvider implements CredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(OIDCRoleARNCredentialsProvider.class);

    private static final String CLIENT_CLASS = "com.aliyun.credentials.Client";
    private static final String CONFIG_CLASS = "com.aliyun.credentials.models.Config";

    private static final String DEFAULT_SESSION_NAME = "hadoop-oss-oidc-session";
    private static final int DEFAULT_EXPIRATION = 3600;

    private final Object credentialsClient;

    /**
     * Constructor for reflective instantiation.
     *
     * @param conf Hadoop configuration
     * @throws Exception if the credentials client cannot be created
     */
    public OIDCRoleARNCredentialsProvider(Configuration conf) throws Exception {
        String roleArn = conf.getTrimmed(OIDC_ROLE_ARN, "");
        String providerArn = conf.getTrimmed(OIDC_PROVIDER_ARN, "");
        String tokenFile = conf.getTrimmed(OIDC_TOKEN_FILE, "");

        if (roleArn.isEmpty()) {
            throw new CredentialsException(
                    "OIDCRoleARNCredentialsProvider requires " + OIDC_ROLE_ARN);
        }
        if (providerArn.isEmpty()) {
            throw new CredentialsException(
                    "OIDCRoleARNCredentialsProvider requires " + OIDC_PROVIDER_ARN);
        }
        if (tokenFile.isEmpty()) {
            throw new CredentialsException(
                    "OIDCRoleARNCredentialsProvider requires " + OIDC_TOKEN_FILE);
        }

        Class<?> configClass = Class.forName(CONFIG_CLASS);
        Object config = configClass.getDeclaredConstructor().newInstance();

        // Set type to "oidc_role_arn"
        Method setType = configClass.getMethod("setType", String.class);
        setType.invoke(config, "oidc_role_arn");

        // Set role ARN
        Method setRoleArn = configClass.getMethod("setRoleArn", String.class);
        setRoleArn.invoke(config, roleArn);

        // Set OIDC provider ARN
        Method setOidcProviderArn = configClass.getMethod("setOidcProviderArn", String.class);
        setOidcProviderArn.invoke(config, providerArn);

        // Set OIDC token file path
        Method setOidcTokenFile = configClass.getMethod("setOidcTokenFilePath", String.class);
        setOidcTokenFile.invoke(config, tokenFile);

        // Set session name
        String sessionName = conf.getTrimmed(OIDC_SESSION_NAME, DEFAULT_SESSION_NAME);
        Method setSessionName = configClass.getMethod("setRoleSessionName", String.class);
        setSessionName.invoke(config, sessionName);

        // Set optional policy
        String policy = conf.getTrimmed(OIDC_POLICY, "");
        if (!policy.isEmpty()) {
            Method setPolicy = configClass.getMethod("setPolicy", String.class);
            setPolicy.invoke(config, policy);
        }

        // Set optional expiration
        int expiration = conf.getInt(OIDC_EXPIRATION, DEFAULT_EXPIRATION);
        Method setExpiration = configClass.getMethod("setRoleSessionExpiration", Integer.class);
        setExpiration.invoke(config, expiration);

        // Create the Client
        Class<?> clientClass = Class.forName(CLIENT_CLASS);
        this.credentialsClient = clientClass
                .getDeclaredConstructor(configClass)
                .newInstance(config);

        LOG.info("Initialized OIDCRoleARNCredentialsProvider with roleArn={}, "
                        + "providerArn={}, tokenFile={}",
                roleArn, providerArn, tokenFile);
    }

    @Override
    public Credentials getCredentials() {
        try {
            Method getCredential = credentialsClient.getClass().getMethod("getCredential");
            Object credential = getCredential.invoke(credentialsClient);

            if (credential == null) {
                throw new CredentialsException(
                        "OIDC Role ARN credential chain returned null credential");
            }

            String accessKeyId = (String) credential.getClass()
                    .getMethod("getAccessKeyId").invoke(credential);
            String accessKeySecret = (String) credential.getClass()
                    .getMethod("getAccessKeySecret").invoke(credential);
            String securityToken = (String) credential.getClass()
                    .getMethod("getSecurityToken").invoke(credential);

            if (accessKeyId == null || accessKeyId.isEmpty()
                    || accessKeySecret == null || accessKeySecret.isEmpty()) {
                throw new CredentialsException(
                        "OIDC Role ARN returned empty accessKeyId or accessKeySecret");
            }

            LOG.debug("Successfully obtained credentials via OIDC Role ARN (type={})",
                    credential.getClass().getMethod("getType").invoke(credential));

            if (securityToken != null && !securityToken.isEmpty()) {
                return new Credentials(accessKeyId, accessKeySecret, securityToken);
            } else {
                return new Credentials(accessKeyId, accessKeySecret);
            }
        } catch (CredentialsException e) {
            throw e;
        } catch (Exception e) {
            throw new CredentialsException(
                    "Failed to obtain credentials via OIDC Role ARN: " + e.getMessage(), e);
        }
    }
}
