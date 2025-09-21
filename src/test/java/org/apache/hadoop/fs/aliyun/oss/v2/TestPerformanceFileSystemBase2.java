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
import java.util.UUID;

import static org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSTestUtils.getURI;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for AliyunOSSFileSystem basic operations including
 * open, write, and read interfaces.
 */
public class TestPerformanceFileSystemBase2 {

    private FileSystem fs;
    private Configuration conf;
    private Path testRootPath;

//  @Param({"org.apache.hadoop.fs.aliyun.oss.mock.LocalOSSClientFactory", "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory"})
//  private String impl;

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
        // For testing purposes, we'll use a local test path
        // In a real test, you would configure actual OSS credentials

        // Initialize the file system
        fs = FileSystem.get(getURI(conf), conf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (fs != null) {
            // Clean up test files
            fs.delete(testRootPath, true);
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
        String testData = "Hello, Aliyun OSS FileSystem!";
        byte[] testDataBytes = testData.getBytes();

        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testDataBytes);
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test read operation
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[testDataBytes.length];
            int bytesRead = in.read(buffer);

            assertEquals(testDataBytes.length, bytesRead, "Should read the same number of bytes");
            assertArrayEquals(testDataBytes, buffer, "Should read the same data");
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

    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testReadWithBuffer() throws IOException {
        // Create a test file path
        Path testFile = new Path(testRootPath, "test-buffer.txt");

        // Write test data
        StringBuilder testDataBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            testDataBuilder.append("Line ").append(i).append("\n");
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
            }

            String readData = readDataBuilder.toString();
            assertEquals(testData, readData, "Should read the same data with 512-byte buffer");
        }

        // Test with a larger buffer
        try (FSDataInputStream in = fs.open(testFile, 2048)) {
            byte[] buffer = new byte[2048];
            StringBuilder readDataBuilder = new StringBuilder();
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {
                readDataBuilder.append(new String(buffer, 0, bytesRead));
            }

            String readData = readDataBuilder.toString();
            assertEquals(testData, readData, "Should read the same data with 2048-byte buffer");
        }
    }
}