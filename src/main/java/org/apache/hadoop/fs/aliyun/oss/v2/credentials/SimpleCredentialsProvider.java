package org.apache.hadoop.fs.aliyun.oss.v2.credentials;

import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.credentials.StaticCredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.CredentialsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * A {@link CredentialsProvider} that reads AK/SK/STS Token directly from
 * the Hadoop {@link Configuration}.
 * <p>
 * Reads the following configuration keys:
 * <ul>
 *   <li>{@code fs.oss.accessKeyId}</li>
 *   <li>{@code fs.oss.accessKeySecret}</li>
 *   <li>{@code fs.oss.securityToken} (optional)</li>
 * </ul>
 * <p>
 * This is the simplest credential provider, suitable for scenarios where
 * AK/SK is explicitly configured.
 */
public class SimpleCredentialsProvider implements CredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(SimpleCredentialsProvider.class);

    private final CredentialsProvider delegate;

    /**
     * Constructor required for reflective instantiation.
     *
     * @param conf Hadoop configuration containing AK/SK
     * @throws Exception if AK/SK cannot be read or are empty
     */
    public SimpleCredentialsProvider(Configuration conf) throws Exception {
        String accessKeyId = getPassword(conf, ACCESS_KEY_ID);
        String accessKeySecret = getPassword(conf, ACCESS_KEY_SECRET);

        if (StringUtils.isEmpty(accessKeyId) || StringUtils.isEmpty(accessKeySecret)) {
            throw new CredentialsException(
                    "SimpleCredentialsProvider: AccessKeyId and AccessKeySecret "
                            + "should not be null or empty.");
        }

        String securityToken = getPassword(conf, SECURITY_TOKEN);

        if (StringUtils.isNotEmpty(securityToken)) {
            delegate = new StaticCredentialsProvider(accessKeyId, accessKeySecret, securityToken);
            LOG.debug("SimpleCredentialsProvider initialized with STS token");
        } else {
            delegate = new StaticCredentialsProvider(accessKeyId, accessKeySecret);
            LOG.debug("SimpleCredentialsProvider initialized without STS token");
        }
    }

    @Override
    public com.aliyun.sdk.service.oss2.credentials.Credentials getCredentials() {
        return delegate.getCredentials();
    }

    private static String getPassword(Configuration conf, String key) {
        try {
            char[] pass = conf.getPassword(key);
            if (pass != null) {
                return new String(pass).trim();
            }
        } catch (Exception e) {
            LOG.debug("Failed to read password for key {}: {}", key, e.getMessage());
        }
        return conf.get(key, "");
    }
}
