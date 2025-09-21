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

import com.aliyun.sdk.service.oss2.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSTestUtils.getURI;
import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for AliyunOSSFileSystem basic operations including
 * open, write, and read interfaces.
 */
public class TestPrefetchStream {

    private AliyunOSSPerformanceFileSystem fs;
    private Configuration conf;
    private Path testRootPath;

    @BeforeEach
    public void setUp() throws Exception {

        conf = new Configuration();
        // Add test resources
        conf.addResource("core-site.xml");
        String testDir = "test-" + UUID.randomUUID().toString();
        testRootPath = new Path("/root-path/" + testDir);

    }

    private void initFs(String ossClientImpl, String FileSystemImpl, int readAhead, int prefethBlockSize, int prefetchThreshold, int bigIOThreshold, int ossMergeSize) throws IOException {
        if (StringUtils.isEmpty(ossClientImpl)) {
            throw new IllegalArgumentException("ossClientImpl cannot be null");
        }

        if (StringUtils.isEmpty(FileSystemImpl)) {
            throw new IllegalArgumentException("FileSystemImpl cannot be null");
        }

        System.out.println("--**test initFs**-- ： with ossClientImpl = " + ossClientImpl);
        System.out.println("--**test initFs**-- ： with FileSystemImpl = " + FileSystemImpl);


        conf.set("fs.oss.client.factory.impl", ossClientImpl);
        conf.set("fs.oss.impl", FileSystemImpl);
        conf.set(PREFETCH_VERSION_KEY, "v2");
        conf.setInt(PREFETCH_BLOCK_SIZE_KEY, prefethBlockSize);
        conf.set(PREFETCH_BLOCK_COUNT_KEY, "1");
        conf.setInt(READAHEAD_RANGE, readAhead);
        conf.setInt(PREFETCH_THRESHOLD_KEY, prefetchThreshold);
        conf.setInt(BIG_IO_THRESHOLD_KEY, bigIOThreshold);
        conf.setInt(OSS_MERGE_SIZE_KEY, ossMergeSize);


        // For testing purposes, we'll use a local test path
        // In a real test, you would configure actual OSS credentials

        // Initialize the file system
        fs = (AliyunOSSPerformanceFileSystem) FileSystem.get(getURI(conf), conf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (fs != null) {
            // Clean up test files
            fs.close();
        }
    }


    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testRead16MBReadAhead(String ossClientImpl, String FileSystemImpl) throws IOException {
        int readAhead = 16 * 1024;
        int prefethBlockSize = 30 * 1024;
        int prefetchThreshold = 32 * 1024;
        int bigIOThreshold = 128 * 1024;
        int ossMergeSize = 250 * 1024;

        initFs(ossClientImpl, FileSystemImpl, readAhead, prefethBlockSize, prefetchThreshold, bigIOThreshold, ossMergeSize);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-size_16MB.txt");
//        testFile= new Path("root-path/test-e32a92fa-664b-43ba-aa93-520bd8e24648/test-size_16MB_01.txt");
        // 生成一个16MB的string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1 * 1024 * 102; i++) {
            sb.append("0123456789");
        }
        String content = sb.toString();

        // Write test data
//        byte[] size_16MB = new byte[16 * 1024 * 1024];
        byte[] size_16MB = content.getBytes();
        // Fill the buffer with random data
        Random random = new Random();
        random.nextBytes(size_16MB);

        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(size_16MB);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");


        // Test read operation with different buffer sizes
        try (FSDataInputStream in = fs.open(testFile, 512)) {
            byte[] buffer = new byte[20 * 1024];
            StringBuilder readDataBuilder = new StringBuilder();
            int bytesRead;
            int totalBytesRead = 0;

            while ((bytesRead = in.read(buffer)) != -1) {
                readDataBuilder.append(new String(buffer, 0, bytesRead));
                totalBytesRead += bytesRead;
            }

            String readData = readDataBuilder.toString();
            System.out.println("Read " + totalBytesRead + " bytes.");
            assertEquals(size_16MB.length, totalBytesRead, "Should read the same data with 512-byte buffer");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testReadPfetchforBigIOwithSmallBlock(String ossClientImpl, String FileSystemImpl) throws IOException {
        int readAhead = 16 * 1024;
        int prefethBlockSize = 30 * 1024;
        int prefetchThreshold = 32 * 1024;
        int bigIOThreshold = 128 * 1024;
        int ossMergeSize = 250 * 1024;

        initFs(ossClientImpl, FileSystemImpl, readAhead, prefethBlockSize, prefetchThreshold, bigIOThreshold, ossMergeSize);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-size_16MB.txt");
//        testFile= new Path("root-path/test-e32a92fa-664b-43ba-aa93-520bd8e24648/test-size_16MB_01.txt");
        // 生成一个16MB的string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1 * 1024 * 102; i++) {
            sb.append("0123456789");
        }
        String content = sb.toString();

        // Write test data
//        byte[] size_16MB = new byte[16 * 1024 * 1024];
        byte[] size_16MB = content.getBytes();
        // Fill the buffer with random data
        Random random = new Random();
        random.nextBytes(size_16MB);

        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(size_16MB);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");


        // Test read operation with different buffer sizes
        try (FSDataInputStream in = fs.open(testFile, 512)) {
            byte[] buffer = new byte[128 * 1024];
            StringBuilder readDataBuilder = new StringBuilder();
            int bytesRead;
            int totalBytesRead = 0;

            while ((bytesRead = in.read(buffer)) != -1) {
                readDataBuilder.append(new String(buffer, 0, bytesRead));
                totalBytesRead += bytesRead;
            }

            String readData = readDataBuilder.toString();
            System.out.println("Read " + totalBytesRead + " bytes.");
            assertEquals(size_16MB.length, totalBytesRead, "Should read the same data with 512-byte buffer");
        }
    }


    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testReadPfetchforBigIOWithBigBlock(String ossClientImpl, String FileSystemImpl) throws IOException {
        int readAhead = 16 * 1024;
        int prefethBlockSize = 32 * 1024;
        int prefetchThreshold = 32 * 1024;
        int bigIOThreshold = 128 * 1024;
        int ossMergeSize = 250 * 1024;

        initFs(ossClientImpl, FileSystemImpl, readAhead, prefethBlockSize, prefetchThreshold, bigIOThreshold, ossMergeSize);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-size_16MB.txt");
//        testFile= new Path("root-path/test-e32a92fa-664b-43ba-aa93-520bd8e24648/test-size_16MB_01.txt");
        // 生成一个16MB的string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1 * 1024 * 102; i++) {
            sb.append("0123456789");
        }
        String content = sb.toString();

        // Write test data
//        byte[] size_16MB = new byte[16 * 1024 * 1024];
        byte[] size_16MB = content.getBytes();
        // Fill the buffer with random data
        Random random = new Random();
        random.nextBytes(size_16MB);

        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(size_16MB);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");


        // Test read operation with different buffer sizes
        try (FSDataInputStream in = fs.open(testFile, 512)) {
            byte[] buffer = new byte[128 * 1024];
            StringBuilder readDataBuilder = new StringBuilder();
            int bytesRead;
            int totalBytesRead = 0;

            while ((bytesRead = in.read(buffer)) != -1) {
                readDataBuilder.append(new String(buffer, 0, bytesRead));
                totalBytesRead += bytesRead;
            }

            String readData = readDataBuilder.toString();
            System.out.println("Read " + totalBytesRead + " bytes.");
            assertEquals(size_16MB.length, totalBytesRead, "Should read the same data with 512-byte buffer");
        }
    }

}