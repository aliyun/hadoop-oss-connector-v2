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
import org.apache.hadoop.fs.*;
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
public class TestListFiles {

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
        // For testing purposes, we'll use a local test path
        // In a real test, you would configure actual OSS credentials

        // Initialize the file system
        fs = FileSystem.get(getURI(conf), conf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (fs != null) {
            // Clean up test files
//            fs.delete(testRootPath, true);
            fs.close();
        }
    }


    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testLisAFile(String ossClientImpl, String FileSystemImpl) throws IOException {
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

        FileStatus[] res = fs.listStatus(testFile);
        //print res size
        System.out.println("-------- res size:" + res.length);
        for (FileStatus fileStatus : res) {
            System.out.println("--------" + fileStatus.toString());
        }

        assertTrue(res.length > 0, "Test file should have a status");
        assertTrue(res[0].isFile(), "Test file should be a file");
        assertEquals(conf.get("test.fs.oss.name") + testFile.toString(), res[0].getPath().toString(), "Test file should have the right path");
        assertEquals(testDataBytes.length, res[0].getLen(), "Test file should have the right length");
    }


    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory, org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testLisADir(String ossClientImpl, String FileSystemImpl) throws IOException {
        initFs(ossClientImpl, FileSystemImpl);
        System.out.println("--**test testWriteAndRead**-- ：ossClientImpl= {} " + ossClientImpl + "    FileSystemImpl={}" + FileSystemImpl);


        Path dir1 = new Path(testRootPath, "dir1");
        fs.mkdirs(dir1);

        // Create a test file path
        Path testFile = new Path(testRootPath, "test-file.txt");
        Path testFile2 = new Path(dir1, "test-file2.txt");

        // Test write operation
        String testData = "Hello, Aliyun OSS FileSystem!";
        byte[] testDataBytes = testData.getBytes();

        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testDataBytes);
        }

        try (FSDataOutputStream out = fs.create(testFile2, true)) {
            out.write(testDataBytes);
        }

        // Verify file exists
        assertTrue(fs.exists(testFile), "Test file should exist");


        FileStatus[] res = fs.listStatus(testRootPath);

        //print res size
        System.out.println("-------- res size:" + res.length);
        for (FileStatus fileStatus : res) {
            System.out.println("--------" + fileStatus.toString());
        }

        assertTrue(res.length  == 2, "Test file should have a status");
        assertTrue(res[0].isFile(), "Test file should be a file");
        assertEquals(conf.get("test.fs.oss.name") + testFile.toString(), res[0].getPath().toString(), "Test file should have the right path");
        assertEquals(testDataBytes.length, res[0].getLen(), "Test file should have the right length");

        assertTrue(res[1].isDirectory(), "Test file should be a file");
        assertEquals(conf.get("test.fs.oss.name") + dir1.toString(), res[1].getPath().toString(), "Test file should have the right path");
        assertEquals(0, res[1].getLen(), "Test file should have the right length");
    }


}