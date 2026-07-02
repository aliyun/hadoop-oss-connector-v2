/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.legency;

import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.credentials.StaticCredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.CredentialsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.Constants;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * Utility methods for Aliyun OSS code.
 */
final public class AliyunOSSUtils {
    private static final Logger LOG =
            LoggerFactory.getLogger(AliyunOSSUtils.class);
    private static volatile LocalDirAllocator directoryAllocator;

    private AliyunOSSUtils() {
    }

    public static int intPositiveOption(
            Configuration conf, String key, int defVal) {
        int v = conf.getInt(key, defVal);
        if (v <= 0) {
            LOG.warn(key + " is configured to " + v
                    + ", will use default value: " + defVal);
            v = defVal;
        }

        return v;
    }

    /**
     * Used to get password from configuration.
     *
     * @param conf configuration that contains password information
     * @param key the key of the password
     * @return the value for the key
     * @throws IOException if failed to get password from configuration
     */
    public static String getValueWithKey(Configuration conf, String key)
            throws IOException {
        try {
            final char[] pass = conf.getPassword(key);
            if (pass != null) {
                return (new String(pass)).trim();
            } else {
                return "";
            }
        } catch (IOException ioe) {
            throw new IOException("Cannot find password option " + key, ioe);
        }
    }

    /**
     * Calculate a proper size of multipart piece. If <code>minPartSize</code>
     * is too small, the number of multipart pieces may exceed the limit of
     * {@link Constants#MULTIPART_UPLOAD_PART_NUM_LIMIT}.
     *
     * @param contentLength the size of file.
     * @param minPartSize the minimum size of multipart piece.
     * @return a revisional size of multipart piece.
     */
    public static long calculatePartSize(long contentLength, long minPartSize) {
        long tmpPartSize = contentLength / MULTIPART_UPLOAD_PART_NUM_LIMIT + 1;
        return Math.max(minPartSize, tmpPartSize);
    }

    /**
     * Create credential provider specified by configuration, or create default
     * credential provider if not specified.
     * <p>
     * Supports comma-separated list of provider class names. The first provider
     * that can successfully provide credentials will be used.
     * If no provider is configured, falls back to {@code DefaultCredentialProviderChain}.
     *
     * @param conf configuration
     * @return a credential provider
     * @throws IOException on any problem. Class construction issues may be
     * nested inside the IOE.
     */
    public static CredentialsProvider getCredentialsProvider(Configuration conf) throws CredentialsException, IOException {
        String providerClassNames = conf.getTrimmed(CREDENTIALS_PROVIDER_KEY, "");

        if (StringUtils.isEmpty(providerClassNames)) {
            // No provider configured, use DefaultCredentialProviderChain
            LOG.debug("No credential provider configured, using DefaultCredentialProviderChain");
            return createDefaultCredentialProviderChain(conf);
        }

        String[] classNames = providerClassNames.split(",");
        if (classNames.length == 1) {
            // Single provider
            return createSingleProvider(conf, classNames[0].trim());
        }

        // Multiple providers: build a chain, return the first successful one.
        // IMPORTANT: Not only must the provider be created successfully, but
        // getCredentials() must also succeed. For example, ECSRAMRoleCredentialsProvider
        // can be instantiated anywhere, but getCredentials() only works on an ECS instance.
        for (String className : classNames) {
            String trimmed = className.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                CredentialsProvider provider = createSingleProvider(conf, trimmed);
                // Verify the provider can actually obtain credentials
                provider.getCredentials();
                LOG.debug("Successfully created and verified credential provider: {}", trimmed);
                return provider;
            } catch (Exception e) {
                LOG.warn("Credential provider {} failed (create or getCredentials), trying next. Error: {}",
                        trimmed, e.getMessage());
            }
        }

        throw new CredentialsException(
                "All configured credential providers failed. Configured: " + providerClassNames);
    }

    /**
     * Create a single CredentialsProvider instance from a class name.
     *
     * @param conf      configuration
     * @param className fully qualified class name
     * @return a credential provider instance
     * @throws IOException if the provider cannot be created
     */
    private static CredentialsProvider createSingleProvider(Configuration conf, String className)
            throws IOException {
        try {
            Class<?> clazz = Class.forName(className);
            if (!CredentialsProvider.class.isAssignableFrom(clazz)) {
                throw new IOException("Class " + className
                        + " does not implement CredentialsProvider interface");
            }
            Constructor<?> constructor = clazz.getConstructor(Configuration.class);
            return (CredentialsProvider) constructor.newInstance(conf);
        } catch (ClassNotFoundException e) {
            throw new IOException("Credential provider class not found: " + className, e);
        } catch (NoSuchMethodException e) {
            // Try no-arg constructor
            try {
                Class<?> clazz = Class.forName(className);
                Constructor<?> constructor = clazz.getConstructor();
                return (CredentialsProvider) constructor.newInstance();
            } catch (Exception ex) {
                throw new IOException(
                        "Cannot instantiate credential provider " + className
                                + ": no suitable constructor found", ex);
            }
        } catch (Exception e) {
            throw new IOException("Failed to create credential provider: " + className, e);
        }
    }

    /**
     * Create a DefaultCredentialProviderChain that uses the Alibaba Cloud
     * credentials SDK default credential chain.
     * <p>
     * If the default chain is available and can obtain credentials, it is used.
     * Otherwise, falls back to static AK/SK credentials from the configuration.
     *
     * @param conf configuration
     * @return a credential provider (default chain or static fallback)
     * @throws IOException if neither default chain nor static credentials work
     */
    private static CredentialsProvider createDefaultCredentialProviderChain(Configuration conf)
            throws IOException {
        try {
            String chainClassName =
                    "org.apache.hadoop.fs.aliyun.oss.v2.credentials.DefaultCredentialProviderChain";
            CredentialsProvider chainProvider = createSingleProvider(conf, chainClassName);
            // Verify the default chain can actually obtain credentials
            chainProvider.getCredentials();
            LOG.debug("DefaultCredentialProviderChain created and verified successfully");
            return chainProvider;
        } catch (Exception e) {
            // DefaultCredentialProviderChain not available or cannot obtain credentials,
            // fall back to static AK/SK credentials from configuration
            LOG.warn("DefaultCredentialProviderChain not available or failed, "
                    + "falling back to static credentials: {}", e.getMessage());
            return createStaticCredentialsProvider(conf);
        }
    }

    /**
     * Create a StaticCredentialsProvider from AK/SK configuration.
     *
     * @param conf configuration
     * @return a static credentials provider
     * @throws IOException if credentials cannot be read
     */
    private static CredentialsProvider createStaticCredentialsProvider(Configuration conf)
            throws IOException {
        String accessKeyId;
        String accessKeySecret;
        String securityToken;
        try {
            accessKeyId = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_ID);
            accessKeySecret = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_SECRET);
        } catch (IOException e) {
            throw new IOException("get accessKeyId/accessKeySecret fail ", e);
        }

        try {
            securityToken = AliyunOSSUtils.getValueWithKey(conf, SECURITY_TOKEN);
        } catch (IOException e) {
            securityToken = null;
        }

        if (StringUtils.isEmpty(accessKeyId)
                || StringUtils.isEmpty(accessKeySecret)) {
            throw new CredentialsException(
                    "AccessKeyId and AccessKeySecret should not be null or empty.");
        }

        if (StringUtils.isNotEmpty(securityToken)) {
            return new StaticCredentialsProvider(accessKeyId, accessKeySecret, securityToken);
        } else {
            return new StaticCredentialsProvider(accessKeyId, accessKeySecret);
        }
    }

    /**
     * Turns a path (relative or otherwise) into an OSS key, adding a trailing
     * "/" if the path is not the root <i>and</i> does not already have a "/"
     * at the end.
     *
     * @param key OSS key or ""
     * @return the with a trailing "/", or, if it is the root key, "".
     */
    public static String maybeAddTrailingSlash(String key) {
        if (StringUtils.isNotEmpty(key) && !key.endsWith("/")) {
            return key + '/';
        } else {
            return key;
        }
    }

    /**
     * Check if OSS object represents a directory.
     *
     * @param name object key
     * @param size object content length
     * @return true if object represents a directory
     */
    public static boolean objectRepresentsDirectory(final String name,
                                                    final long size) {
        return StringUtils.isNotEmpty(name) && name.endsWith("/") && size == 0L;
    }

    /**
     * Demand create the directory allocator, then create a temporary file.
     * This does not mark the file for deletion when a process exits.
     * {@link LocalDirAllocator#createTmpFileForWrite(
     *String, long, Configuration)}.
     * @param pathStr prefix for the temporary file
     * @param size the size of the file that is going to be written
     * @param conf the Configuration object
     * @return a unique temporary file
     * @throws IOException IO problems
     */
    public static File createTmpFileForWrite(String pathStr, long size,
                                             Configuration conf) throws IOException {
        if (conf.get(BUFFER_DIR_KEY) == null) {
            conf.set(BUFFER_DIR_KEY, conf.get("hadoop.tmp.dir") + "/oss");
        }
        if (directoryAllocator == null) {
            synchronized (AliyunOSSUtils.class) {
                if (directoryAllocator == null) {
                    directoryAllocator = new LocalDirAllocator(BUFFER_DIR_KEY);
                }
            }
        }
        Path path = directoryAllocator.getLocalPathForWrite(pathStr,
                size, conf);
        File dir = new File(path.getParent().toUri().getPath());
        String prefix = path.getName();
        // create a temp file on this directory
        return File.createTempFile(prefix, null, dir);
    }

    /**
     * Get a integer option >= the minimum allowed value.
     * @param conf configuration
     * @param key key to look up
     * @param defVal default value
     * @param min minimum value
     * @return the value
     * @throws IllegalArgumentException if the value is below the minimum
     */
    public static int intOption(Configuration conf, String key, int defVal, int min) {
        int v = conf.getInt(key, defVal);
        Preconditions.checkArgument(v >= min,
                String.format("Value of %s: %d is below the minimum value %d",
                        key, v, min));
        LOG.debug("Value of {} is {}", key, v);
        return v;
    }

    /**
     * Get a long option >= the minimum allowed value.
     * @param conf configuration
     * @param key key to look up
     * @param defVal default value
     * @param min minimum value
     * @return the value
     * @throws IllegalArgumentException if the value is below the minimum
     */
    public static long longOption(Configuration conf, String key, long defVal,
                                  long min) {
        long v = conf.getLong(key, defVal);
        Preconditions.checkArgument(v >= min,
                String.format("Value of %s: %d is below the minimum value %d",
                        key, v, min));
        LOG.debug("Value of {} is {}", key, v);
        return v;
    }

    /**
     * Get a size property from the configuration: this property must
     * be at least equal to {@link Constants#MULTIPART_MIN_SIZE}.
     * If it is too small, it is rounded up to that minimum, and a warning
     * printed.
     * @param conf configuration
     * @param property property name
     * @param defVal default value
     * @return the value, guaranteed to be above the minimum size
     */
    public static long getMultipartSizeProperty(Configuration conf,
                                                String property, long defVal) {
        long partSize = conf.getLong(property, defVal);
        if (partSize < MULTIPART_MIN_SIZE) {
            LOG.warn("{} must be at least 100 KB; configured value is {}",
                    property, partSize);
            partSize = MULTIPART_MIN_SIZE;
        } else if (partSize > Integer.MAX_VALUE) {
            LOG.warn("oss: {} capped to ~2.14GB(maximum allowed size with " +
                    "current output mechanism)", MULTIPART_UPLOAD_PART_SIZE_KEY);
            partSize = Integer.MAX_VALUE;
        }
        return partSize;
    }

    /**
     * 格式化Instant时间为字符串，精确到微秒
     *
     * @param instant 时间
     * @return 格式化后的时间字符串
     */
    public static String formatInstant(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant);
    }

    /**
     * Propagates bucket-specific settings into generic OSS configuration keys.
     * This is done by propagating the values of the form
     * {@code fs.oss.bucket.${bucket}.key} to
     * {@code fs.oss.key}, for all values of "key" other than a small set
     * of unmodifiable values.
     *
     * The source of the updated property is set to the key name of the bucket
     * property, to aid in diagnostics of where things came from.
     *
     * Returns a new configuration. Why the clone?
     * You can use the same conf for different filesystems, and the original
     * values are not updated.
     *
     * The {@code fs.oss.impl} property cannot be set, nor can
     * any with the prefix {@code fs.oss.bucket}.
     *
     * Modeled after S3A's {@code S3AUtils.propagateBucketOptions()}.
     *
     * @param source Source Configuration object.
     * @param bucket bucket name. Must not be empty.
     * @return a (potentially) patched clone of the original.
     */
    public static Configuration propagateBucketOptions(Configuration source,
                                                        String bucket) {

        Preconditions.checkArgument(StringUtils.isNotEmpty(bucket),
                "bucket is null/empty");
        final String bucketPrefix = FS_OSS_BUCKET_PREFIX + bucket + '.';
        LOG.debug("Propagating entries under {}", bucketPrefix);
        final Configuration dest = new Configuration(source);
        for (Map.Entry<String, String> entry : source) {
            final String key = entry.getKey();
            // get the (unexpanded) value.
            final String value = entry.getValue();
            if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) {
                continue;
            }
            // there's a bucket prefix, so strip it
            final String stripped = key.substring(bucketPrefix.length());
            if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
                // tell user off: these keys are not allowed to be overridden
                LOG.debug("Ignoring bucket option {}", key);
            } else {
                // propagate the value, building a new origin field.
                // to track overwrites, the generic key is overwritten even if
                // already matches the new one.
                String origin = "[" + StringUtils.join(
                        source.getPropertySources(key), ", ") + "]";
                final String generic = FS_OSS_PREFIX + stripped;
                LOG.debug("Updating {} from {}", generic, origin);
                dest.set(generic, value, key + " via " + origin);
            }
        }
        return dest;
    }

    /**
     * Set a bucket-specific property to a particular value.
     * If the generic key passed in has an {@code fs.oss.} prefix,
     * that's stripped off, so that when the bucket properties are propagated
     * down to the generic values, that value gets copied down.
     *
     * @param conf       configuration to set
     * @param bucket     bucket name
     * @param genericKey key; can start with "fs.oss."
     * @param value      value to set
     */
    public static void setBucketOption(Configuration conf, String bucket,
                                        String genericKey, String value) {
        final String baseKey = genericKey.startsWith(FS_OSS_PREFIX)
                ? genericKey.substring(FS_OSS_PREFIX.length())
                : genericKey;
        conf.set(FS_OSS_BUCKET_PREFIX + bucket + '.' + baseKey, value,
                "AliyunOSSUtils");
    }

    /**
     * Clear a bucket-specific property.
     * If the generic key passed in has an {@code fs.oss.} prefix,
     * that's stripped off.
     *
     * @param conf       configuration to modify
     * @param bucket     bucket name
     * @param genericKey key; can start with "fs.oss."
     */
    public static void clearBucketOption(Configuration conf, String bucket,
                                          String genericKey) {
        final String baseKey = genericKey.startsWith(FS_OSS_PREFIX)
                ? genericKey.substring(FS_OSS_PREFIX.length())
                : genericKey;
        String k = FS_OSS_BUCKET_PREFIX + bucket + '.' + baseKey;
        LOG.debug("Unset {}", k);
        conf.unset(k);
    }

    /**
     * Get a bucket-specific property.
     * If the generic key passed in has an {@code fs.oss.} prefix,
     * that's stripped off.
     *
     * @param conf       configuration to query
     * @param bucket     bucket name
     * @param genericKey key; can start with "fs.oss."
     * @return the bucket option, null if there is none
     */
    public static String getBucketOption(Configuration conf, String bucket,
                                          String genericKey) {
        final String baseKey = genericKey.startsWith(FS_OSS_PREFIX)
                ? genericKey.substring(FS_OSS_PREFIX.length())
                : genericKey;
        return conf.get(FS_OSS_BUCKET_PREFIX + bucket + '.' + baseKey);
    }

}
