package org.apache.hadoop.fs.aliyun.oss.v2.credentials;

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
public class ECSRAMRoleCredentialsProvider extends AbstractReflectiveCredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(ECSRAMRoleCredentialsProvider.class);

    /**
     * Configuration key for specifying the ECS RAM role name.
     * @deprecated Use {@link org.apache.hadoop.fs.aliyun.oss.v2.Constants#ECS_RAM_ROLE_NAME_KEY} instead.
     */
    @Deprecated
    public static final String ECS_ROLE_NAME_KEY = ECS_RAM_ROLE_NAME_KEY;

    /**
     * Constructor for reflective instantiation.
     *
     * @param conf Hadoop configuration
     * @throws Exception if the credentials client cannot be created
     */
    public ECSRAMRoleCredentialsProvider(Configuration conf) throws Exception {
        Object config = createConfig();
        Class<?> configClass = config.getClass();

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

        createClient(config);

        LOG.info("Initialized ECSRAMRoleCredentialsProvider");
    }

    @Override
    protected String getProviderName() {
        return "ECS RAM role";
    }
}
