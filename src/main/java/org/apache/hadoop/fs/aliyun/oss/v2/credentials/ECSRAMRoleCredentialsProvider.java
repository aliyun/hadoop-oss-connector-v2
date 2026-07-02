package org.apache.hadoop.fs.aliyun.oss.v2.credentials;

import com.aliyun.sdk.service.oss2.credentials.Credentials;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.CredentialsException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.ECS_RAM_ROLE_NAME_KEY;

/**
 * A {@link CredentialsProvider} that obtains credentials via the ECS
 * instance RAM role (Instance Metadata Service).
 * <p>
 * Internally uses the Alibaba Cloud {@code credentials-java} SDK with
 * {@code Config.setType("ecs_ram_role")}. When running on an ECS instance
 * that has a RAM role attached, credentials are automatically retrieved
 * from the instance metadata service.
 * <p>
 * Configuration:
 * <ul>
 *   <li>{@code fs.oss.ecs.role.name} — (optional) the name of the ECS RAM
 *       role. If not set, the SDK will auto-detect the role name.</li>
 * </ul>
 * <p>
 * This provider uses reflection to avoid a compile-time dependency on
 * {@code credentials-java}.
 */
public class ECSRAMRoleCredentialsProvider implements CredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(ECSRAMRoleCredentialsProvider.class);

    private static final String CLIENT_CLASS = "com.aliyun.credentials.Client";
    private static final String CONFIG_CLASS = "com.aliyun.credentials.models.Config";

    /**
     * Configuration key for specifying the ECS RAM role name.
     * @deprecated Use {@link org.apache.hadoop.fs.aliyun.oss.v2.Constants#ECS_RAM_ROLE_NAME_KEY} instead.
     */
    @Deprecated
    public static final String ECS_ROLE_NAME_KEY = ECS_RAM_ROLE_NAME_KEY;

    private final Object credentialsClient;

    /**
     * Constructor for reflective instantiation.
     *
     * @param conf Hadoop configuration
     * @throws Exception if the credentials client cannot be created
     */
    public ECSRAMRoleCredentialsProvider(Configuration conf) throws Exception {
        Class<?> configClass = Class.forName(CONFIG_CLASS);
        Object config = configClass.getDeclaredConstructor().newInstance();

        // Set type to "ecs_ram_role"
        Method setType = configClass.getMethod("setType", String.class);
        setType.invoke(config, "ecs_ram_role");

        // Optionally set role name
        String roleName = conf.getTrimmed(ECS_RAM_ROLE_NAME_KEY, "");
        if (!roleName.isEmpty()) {
            Method setRoleName = configClass.getMethod("setRoleName", String.class);
            setRoleName.invoke(config, roleName);
            LOG.debug("ECSRAMRoleCredentialsProvider: using role name '{}'", roleName);
        } else {
            LOG.debug("ECSRAMRoleCredentialsProvider: role name not set, will auto-detect");
        }

        Class<?> clientClass = Class.forName(CLIENT_CLASS);
        this.credentialsClient = clientClass
                .getDeclaredConstructor(configClass)
                .newInstance(config);

        LOG.info("Initialized ECSRAMRoleCredentialsProvider");
    }

    @Override
    public Credentials getCredentials() {
        try {
            Method getCredential = credentialsClient.getClass().getMethod("getCredential");
            Object credential = getCredential.invoke(credentialsClient);

            if (credential == null) {
                throw new CredentialsException(
                        "ECS RAM role credential chain returned null credential. "
                                + "Please ensure the ECS instance has a RAM role attached.");
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
                        "ECS RAM role returned empty accessKeyId or accessKeySecret");
            }

            LOG.debug("Successfully obtained credentials from ECS RAM role (type={})",
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
                    "Failed to obtain credentials from ECS RAM role: " + e.getMessage(), e);
        }
    }
}
