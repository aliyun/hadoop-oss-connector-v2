/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.net.URI;

/**
 * Utility class for Aliyun OSS Tests.
 */
public final class AliyunOSSTestUtils {
    public static final String TEST_FS_OSS_NAME = "test.fs.oss.name";
    public static final String FS_OSS_IMPL_DISABLE_CACHE
            = "fs.oss.impl.disable.cache";

    private AliyunOSSTestUtils() {
    }


    public static AliyunOSSPerformanceFileSystem createTestFileSystem(Configuration conf)
            throws IOException {
        AliyunOSSPerformanceFileSystem ossfs = new AliyunOSSPerformanceFileSystem();

        ossfs.initialize(getURI(conf), conf);
        return ossfs;
    }

    public static URI getURI(Configuration conf) {
        String fsname = conf.getTrimmed(
                TEST_FS_OSS_NAME, "");

        boolean liveTest = !StringUtils.isEmpty(fsname);
        URI testURI = null;
        if (liveTest) {
            testURI = URI.create(fsname);
            liveTest = testURI.getScheme().equals(Constants.FS_OSS);
        }

        if (!liveTest) {
            Assumptions.assumeTrue(false, "No test filesystem in " + TEST_FS_OSS_NAME);
        }
        return testURI;
    }

    /**
     * Generate unique test path for multiple user tests.
     *
     * @return root test path
     */
    public static String generateUniqueTestPath() {
        String testUniqueForkId = System.getProperty("test.unique.fork.id");
        return testUniqueForkId == null ? "/test" :
                "/" + testUniqueForkId + "/test";
    }

    /**
     * Turn off FS Caching: use if a filesystem with different options from
     * the default is required.
     * @param conf configuration to patch
     */
    public static void disableFilesystemCaching(Configuration conf) {
        conf.setBoolean(FS_OSS_IMPL_DISABLE_CACHE,
                true);
    }
}