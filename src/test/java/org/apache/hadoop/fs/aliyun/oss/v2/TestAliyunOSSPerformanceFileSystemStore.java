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

package org.apache.hadoop.fs.aliyun.oss.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Test the bridging logic between Hadoop's abstract filesystem and
 * Aliyun OSS.
 */
public class TestAliyunOSSPerformanceFileSystemStore {
    private Configuration conf;
    private AliyunOSSFileSystemStore store;
    private AliyunOSSPerformanceFileSystem fs;

    @BeforeEach
    public void setUp() throws Exception {
        conf = new Configuration();
        fs = new AliyunOSSPerformanceFileSystem();
        fs.initialize(URI.create(conf.get("test.fs.oss.name")), conf);
        store = fs.getStore();
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            store.purge("test");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @BeforeAll
    public static void checkSettings() throws Exception {
        Configuration conf = new Configuration();
        assertNotNull(conf.get(Constants.ACCESS_KEY_ID));
        assertNotNull(conf.get(Constants.ACCESS_KEY_SECRET));
        assertNotNull(conf.get("test.fs.oss.name"));
    }

    protected void writeRenameReadCompare(Path path, long len)
            throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        OutputStream out = new BufferedOutputStream(
                new DigestOutputStream(fs.create(path, false), digest));
        for (long i = 0; i < len; i++) {
            out.write('Q');
        }
        out.flush();
        out.close();

        assertTrue(fs.exists(path));

        Path copyPath = path.suffix(".copy");
        long start = System.currentTimeMillis();
        fs.rename(path, copyPath);

        assertTrue(fs.exists(copyPath), "Copy exists");
        // should less than 1 second
        assertTrue(System.currentTimeMillis() - start < 1000);
        // Download file from Aliyun OSS and compare the digest against the original
        MessageDigest digest2 = MessageDigest.getInstance("MD5");
        InputStream in = new BufferedInputStream(
                new DigestInputStream(fs.open(copyPath), digest2));
        long copyLen = 0;
        while (in.read() != -1) {
            copyLen++;
        }
        in.close();

        assertEquals(len, copyLen, "Copy length matches original");
        assertArrayEquals(digest.digest(), digest2.digest(), "Digests match");
    }

    @Test
    public void testSmallUpload() throws IOException, NoSuchAlgorithmException {
        // Regular upload, regular copy
        writeRenameReadCompare(new Path("/test/small"), 16384);
    }

    @Test
    public void testLargeUpload()
            throws IOException, NoSuchAlgorithmException {
        // Multipart upload, shallow copy
        writeRenameReadCompare(new Path("/test/xlarge"), 2147483648L); // 2GB
    }
}
