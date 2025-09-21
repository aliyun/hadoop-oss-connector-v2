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
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for AliyunOSSFileSystem basic operations including
 * open, write, and read interfaces.
 */
public class TestReadAheadInputStream {

    private FileSystem fs;
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

    private void initFs(String ossClientImpl, String FileSystemImpl) throws IOException {
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
        conf.set(PREFETCH_VERSION_KEY, "v3");
        conf.set(PREFETCH_BLOCK_SIZE_KEY, "5");
        conf.set(READAHEAD_RANGE, "3");

        // For testing purposes, we'll use a local test path
        // In a real test, you would configure actual OSS credentials

        // Initialize the file system
        fs = FileSystem.get(getURI(conf), conf);
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
    public void testWriteAndRead(String ossClientImpl, String FileSystemImpl) throws IOException {
        initFs(ossClientImpl, FileSystemImpl);
        System.out.println("--**test testWriteAndRead**-- ：ossClientImpl= {} " + ossClientImpl + "    FileSystemImpl={}" + FileSystemImpl);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-file.txt");

        // Test write operation
        String testData = "01234567890123456789";
        byte[] testDataBytes = testData.getBytes();

        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testDataBytes);
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test read operation
//        try (FSDataInputStream in = fs.open(testFile)) {
//            byte[] buffer = new byte[testDataBytes.length];
//            int bytesRead = in.read(buffer);
//
//            assertEquals(testDataBytes.length, bytesRead, "Should read the same number of bytes");
//            assertArrayEquals(testDataBytes, buffer, "Should read the same data");
//        }

        // Test read operation
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1];
            int bytesRead = in.read(buffer);
            assertEquals(1, bytesRead, "Should read the same number of bytes");
            assertEquals(48, buffer[0], "Should read the same data");


            in.seek(3);
            byte[] buffer2 = new byte[2];
            int bytesRead2 = in.read(buffer2);


            assertEquals(2, bytesRead2, "Should read the same number of bytes");
            assertEquals(51, buffer2[0], "Should read the same data");
            assertEquals(52, buffer2[1], "Should read the same data");

        }
    }

    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testWriteReadMultipleTimes(String ossClientImpl, String FileSystemImpl) throws IOException {
        initFs(ossClientImpl, FileSystemImpl);
        System.out.println("--**test testWriteReadMultipleTimes**-- ：ossClientImpl= {} " + ossClientImpl + "    FileSystemImpl={}" + FileSystemImpl);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-multiple.txt");

        // Test write operation with multiple writes
        String testData1 = "First line of data\n";
        String testData2 = "Second line of data\n";
        String testData3 = "Third line of data\n";

        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testData1.getBytes());
            out.write(testData2.getBytes());
            out.write(testData3.getBytes());
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test read operation
        try (FSDataInputStream in = fs.open(testFile)) {
            // Read entire content
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String readData = new String(buffer, 0, bytesRead);

            String expectedData = testData1 + testData2 + testData3;
            assertEquals(expectedData, readData, "Should read the same data");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testAppendAndRead(String ossClientImpl, String FileSystemImpl) throws IOException {
        initFs(ossClientImpl, FileSystemImpl);
        // Create a test file path
        Path testFile = new Path(testRootPath, "test-append.txt");

        // Write initial data
        String initialData = "Initial data\n";
        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(initialData.getBytes());
        }

        // Append data (if supported)
        String appendedData = "Appended data\n";
        try (FSDataOutputStream out = fs.append(testFile)) {
            out.write(appendedData.getBytes());
        } catch (IOException e) {
            // If append is not supported, that's OK for this test
            // Just verify the initial data can be read
            try (FSDataInputStream in = fs.open(testFile)) {
                byte[] buffer = new byte[1024];
                int bytesRead = in.read(buffer);
                String readData = new String(buffer, 0, bytesRead);

                assertEquals(initialData, readData, "Should read initial data");
                return;
            }
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test read operation
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String readData = new String(buffer, 0, bytesRead);

            String expectedData = initialData + appendedData;
            assertEquals(expectedData, readData, "Should read the combined data");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testReadWithBuffer(String ossClientImpl, String FileSystemImpl) throws IOException {
        initFs(ossClientImpl, FileSystemImpl);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-buffer.txt");

        // Write test data
        StringBuilder testDataBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            testDataBuilder.append("Line___").append(i).append("\n");
        }
        String testData = testDataBuilder.toString();
        byte[] testDataBytes = testData.getBytes();

        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testDataBytes);
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");


        // Test read operation with different buffer sizes
        try (FSDataInputStream in = fs.open(testFile, 512)) {
            byte[] buffer = new byte[512];
            StringBuilder readDataBuilder = new StringBuilder();
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {
                readDataBuilder.append(new String(buffer, 0, bytesRead));
                System.out.println("Read " + bytesRead + " bytes.");
                System.out.println("Read data: " + readDataBuilder.toString());
            }

            String readData = readDataBuilder.toString();
            assertEquals(testData, readData, "Should read the same data with 512-byte buffer");
        }

//        // Test with a larger buffer
//        try (FSDataInputStream in = fs.open(testFile, 2048)) {
//            byte[] buffer = new byte[2048];
//            StringBuilder readDataBuilder = new StringBuilder();
//            int bytesRead;
//
//            while ((bytesRead = in.read(buffer)) != -1) {
//                readDataBuilder.append(new String(buffer, 0, bytesRead));
//            }
//
//            String readData = readDataBuilder.toString();
//            assertEquals(testData, readData, "Should read the same data with 2048-byte buffer");
//        }
    }


    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testRead128MB(String ossClientImpl, String FileSystemImpl) throws IOException {
        initFs(ossClientImpl, FileSystemImpl);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-size_16MB.txt");
        testFile = new Path("root-path/test-e32a92fa-664b-43ba-aa93-520bd8e24648/test-size_16MB_01.txt");
        // 生成一个16MB的string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 16 * 1024 * 102; i++) {
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
            byte[] buffer = new byte[512 * 1024];
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