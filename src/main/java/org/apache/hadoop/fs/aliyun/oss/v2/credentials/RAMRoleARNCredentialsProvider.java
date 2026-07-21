package org.apache.hadoop.fs.aliyun.oss.v2.credentials;

import com.aliyun.sdk.service.oss2.exceptions.CredentialsException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * A {@link CredentialsProvider} that obtains credentials by assuming a
 * RAM role via STS (Security Token Service).
 * <p>
 * Internally uses the Alibaba Cloud {@code credentials-java} SDK with
 * {@code Config.setType("ram_role_arn")}. The provider uses the configured
 * AK/SK to call STS AssumeRole and obtains temporary credentials
 * (accessKeyId, accessKeySecret, securityToken) that are automatically
 * refreshed before expiration.
 * <p>
 * Required configuration:
 * <ul>
 *   <li>{@code fs.oss.accessKeyId} — the AK used to call AssumeRole</li>
 *   <li>{@code fs.oss.accessKeySecret} — the SK used to call AssumeRole</li>
 *   <li>{@code fs.oss.assumed.role.arn} — the ARN of the RAM role to assume</li>
 * </ul>
 * <p>
 * Optional configuration:
 * <ul>
 *   <li>{@code fs.oss.assumed.role.session.name} — session name
 *       (default: "hadoop-oss-session")</li>
 *   <li>{@code fs.oss.assumed.role.policy} — a policy to scope down the
 *       temporary credentials (JSON string)</li>
 *   <li>{@code fs.oss.assumed.role.expiration} — session duration in seconds
 *       (default: 3600)</li>
 * </ul>
 * <p>
 * This provider uses reflection to avoid a compile-time dependency on
 * {@code credentials-java}.
 */
public class RAMRoleARNCredentialsProvider extends AbstractReflectiveCredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(RAMRoleARNCredentialsProvider.class);

    private static final String DEFAULT_SESSION_NAME = "hadoop-oss-session";
    private static final int DEFAULT_EXPIRATION = 3600;

    /**
     * Constructor for reflective instantiation.
     *
     * @param conf Hadoop configuration
     * @throws Exception if the credentials client cannot be created
     */
    public RAMRoleARNCredentialsProvider(Configuration conf) throws Exception {
        String accessKeyId = conf.getTrimmed(ACCESS_KEY_ID, "");
        String accessKeySecret = conf.getTrimmed(ACCESS_KEY_SECRET, "");
        String roleArn = conf.getTrimmed(ASSUMED_ROLE_ARN, "");

        if (accessKeyId.isEmpty() || accessKeySecret.isEmpty()) {
            throw new CredentialsException(
                    "RAMRoleARNCredentialsProvider requires " + ACCESS_KEY_ID
                            + " and " + ACCESS_KEY_SECRET);
        }
        if (roleArn.isEmpty()) {
            throw new CredentialsException(
                    "RAMRoleARNCredentialsProvider requires " + ASSUMED_ROLE_ARN);
        }

        Object config = createConfig();
        Class<?> configClass = config.getClass();

        // Set type to "ram_role_arn"
        Method setType = configClass.getMethod("setType", String.class);
        setType.invoke(config, "ram_role_arn");

        // Set base AK/SK
        Method setAkId = configClass.getMethod("setAccessKeyId", String.class);
        setAkId.invoke(config, accessKeyId);
        Method setAkSecret = configClass.getMethod("setAccessKeySecret", String.class);
        setAkSecret.invoke(config, accessKeySecret);

        // Set role ARN
        Method setRoleArn = configClass.getMethod("setRoleArn", String.class);
        setRoleArn.invoke(config, roleArn);

        // Set session name
        String sessionName = conf.getTrimmed(ASSUMED_ROLE_SESSION_NAME, DEFAULT_SESSION_NAME);
        Method setSessionName = configClass.getMethod("setRoleSessionName", String.class);
        setSessionName.invoke(config, sessionName);

        // Set optional policy
        String policy = conf.getTrimmed(ASSUMED_ROLE_POLICY, "");
        if (!policy.isEmpty()) {
            Method setPolicy = configClass.getMethod("setPolicy", String.class);
            setPolicy.invoke(config, policy);
        }

        // Set optional expiration
        int expiration = conf.getInt(ASSUMED_ROLE_EXPIRATION, DEFAULT_EXPIRATION);
        Method setExpiration = configClass.getMethod("setRoleSessionExpiration", Integer.class);
        setExpiration.invoke(config, expiration);

        createClient(config);

        LOG.info("Initialized RAMRoleARNCredentialsProvider with roleArn={}, sessionName={}",
                roleArn, sessionName);
    }

    @Override
    protected String getProviderName() {
        return "RAM Role ARN";
    }
}
