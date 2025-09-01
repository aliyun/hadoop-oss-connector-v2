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
import java.time.Instant;
import java.time.format.DateTimeFormatter;

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
     *
     * @param uri uri passed by caller
     * @param conf configuration
     * @return a credential provider
     * @throws IOException on any problem. Class construction issues may be
     * nested inside the IOE.
     */
    public static CredentialsProvider getCredentialsProvider(Configuration conf) throws CredentialsException, IOException {
        CredentialsProvider credentials;

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
            credentials = new StaticCredentialsProvider(accessKeyId, accessKeySecret,
                    securityToken);
        } else {
            credentials = new StaticCredentialsProvider(accessKeyId, accessKeySecret);
        }

        return credentials;
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

}
