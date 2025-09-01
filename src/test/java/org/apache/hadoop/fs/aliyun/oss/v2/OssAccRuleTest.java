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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OperationStat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.Math.min;
import static org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSTestUtils.getURI;
import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for AliyunOSSFileSystem basic operations including
 * open, write, and read interfaces.
 */
public class OssAccRuleTest {

    private AliyunOSSPerformanceFileSystem fs;
    private Configuration conf;
    private Path testRootPath;

//  @Param({"org.apache.hadoop.fs.aliyun.oss.mock.LocalOSSClientFactory", "org.apache.hadoop.fs.aliyun.oss.DefaultOSSClientFactory"})
//  private String impl;

    @BeforeEach
    public void setUp() throws Exception {

        conf = new Configuration();
        // Add test resources
        conf.addResource("core-site.xml");
        String testDir = "test-" + UUID.randomUUID().toString();
        testRootPath = new Path("/root-path/" + testDir);

    }

    private void initFs(String ruleStr, int size) throws IOException {
        conf.set(PREFETCH_VERSION_KEY, "v2");
        conf.setInt(PREFETCH_BLOCK_SIZE_KEY, size);

        // For testing purposes, we'll use a local test path
        // In a real test, you would configure actual OSS credentials

//        String ruleStr = "  <rules>\n" +
//                "               <rule>\n" +
//                "                   <keyPrefixes>\n" +
//                "                       <keyPrefix>root-path/test</keyPrefix>\n" +
//                "<!--                       <keyPrefix>b/</keyPrefix>-->\n" +
//                "                   </keyPrefixes>\n" +
//                "                   <keySuffixes>\n" +
//                "                       <keySuffix>.txt</keySuffix>\n" +
//                "<!--                       <keySuffix>.png</keySuffix>-->\n" +
//                "                   </keySuffixes>\n" +
//                "<!--                   <sizeRanges>-->\n" +
//                "<!--                       <range>-->\n" +
//                "<!--                           <minSize>0</minSize>-->\n" +
//                "<!--                           <maxSize>1048576</maxSize>-->\n" +
//                "<!--                       </range>-->\n" +
//                "<!--                       <range>-->\n" +
//                "<!--                           <minSize>end-512288</minSize>-->\n" +
//                "<!--                           <maxSize>end</maxSize>-->\n" +
//                "<!--                       </range>-->\n" +
//                "<!--                   </sizeRanges>-->\n" +
//                "                   <operations>\n" +
//                "                       <operation>getObject</operation>\n" +
//                "                   </operations>\n" +
//                "               </rule>\n" +
//                "           </rules>";

        conf.set(ACC_RULES, ruleStr);

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

    @Test
    public void testTextReadAcc() throws IOException {
        String ruleStr = "  <rules>\n" +
                "               <rule>\n" +
                "                   <keySuffixes>\n" +
                "                       <keySuffix>.txt</keySuffix>\n" +
                "                       <keySuffix>.png</keySuffix>\n" +
                "                   </keySuffixes>\n" +
                "                   <operations>\n" +
                "                       <operation>getObject</operation>\n" +
                "                   </operations>\n" +
                "               </rule>\n" +
                "           </rules>";

        initFs(ruleStr, 18);

        checkGetObjectAcc(fs, new Path(testRootPath, "test-file.txt"), "01234567", true, "read text file ,should use acc", 0, 10);
        checkGetObjectAcc(fs, new Path(testRootPath, "test-file.jpg"), "01234567", false, "read jpg file ,should use acc", 0, 10);
        checkGetObjectAcc(fs, new Path(testRootPath, "test-file.png"), "01234567", true, "read png file ,should use acc", 0, 10);
    }


    @Test
    public void testFileSizeAcc() throws IOException {
        String ruleStr = "  <rules>\n" +
                "               <rule>\n" +
                "                  <sizeRanges>\n" +
                "                       <range>\n" +
                "                          <minSize>0</minSize>\n" +
                "                          <maxSize>10</maxSize>\n" +
                "                      </range>\n" +
                "                       <range>\n" +
                "                          <minSize>21</minSize>\n" +
                "                          <maxSize>30</maxSize>\n" +
                "                      </range>\n" +
                "                   </sizeRanges>\n" +
                "                   <operations>\n" +
                "                       <operation>getObject</operation>\n" +
                "                   </operations>\n" +
                "               </rule>\n" +
                "           </rules>";

        initFs(ruleStr, 18);
        // Create a test file path
        Path testFile = new Path(testRootPath, "test-file1.txt");
        // Test write operation
        checkGetObjectAcc(fs, testFile, "01234567", true, "write 8 bytes file ,should use acc", 0, 10);
        checkGetObjectAcc(fs, testFile, "012345678901234", false, "write 15 bytes file ,should not use acc", 0, 10);
        checkGetObjectAcc(fs, testFile, "012345678901234567890123", true, "write 24 bytes file ,should use acc", 0, 10);

    }

    public static String removeLeadingSlash(String str) {
        if (str != null && !str.isEmpty() && str.startsWith("/")) {
            return str.substring(1);
        }
        return str;
    }

    @Test
    public void testPrefixAcc() throws IOException {
        String ruleStr = "  <rules>\n" +
                "               <rule>\n" +
                "                   <keyPrefixes>\n" +
                "                       <keyPrefix>" + removeLeadingSlash(testRootPath.toString()) + "/a/" + "</keyPrefix>\n" +
                "                       <keyPrefix>" + removeLeadingSlash(testRootPath.toString()) + "/b/" + "</keyPrefix>\n" +
                "                   </keyPrefixes>\n" +
                "                   <operations>\n" +
                "                       <operation>getObject</operation>\n" +
                "                   </operations>\n" +
                "               </rule>\n" +
                "           </rules>";

        initFs(ruleStr, 18);
        checkGetObjectAcc(fs, new Path(testRootPath, "c/test-file1.txt"), "01234567", false, "write 8 bytes file ,should use acc", 0, 10);
        checkGetObjectAcc(fs, new Path(testRootPath, "a/test-file1.txt"), "01234567", true, "a/ flile  ,should use acc", 0, 10);
        checkGetObjectAcc(fs, new Path(testRootPath, "b/test-file1.txt"), "01234567", true, "write 8 bytes file ,should use acc", 0, 10);
    }

    @Test
    public void testOperationAcc() throws IOException {
        String ruleStr = "  <rules>\n" +
                "               <rule>\n" +
                "                   <operations>\n" +
                "                       <operation>putObject</operation>\n" +
                "                   </operations>\n" +
                "               </rule>\n" +
                "           </rules>";

        initFs(ruleStr, 18);
        checkGetObjectAcc(fs, new Path(testRootPath, "c/test-file1.txt"), "01234567", false, "write 8 bytes file ,should use acc", 0, 10);
    }


    @Test
    public void testIOSizeAcc() throws IOException {
        String ruleStr = "  <rules>\n" +
                "               <rule>\n" +
                "                   <IOSizeRanges>\n" +
//                "                       <ioSizeRange>\n" +
//                "                           <ioType>HEAD</ioType>\n" +
//                "                           <ioSize>10</ioSize>\n" +
//                "                       </ioSizeRange>\n" +
//                "                       <ioSizeRange>\n" +
//                "                           <ioType>TAIL</ioType>\n" +
//                "                           <ioSize>10</ioSize>\n" +
//                "                       </ioSizeRange>\n" +
                "                       <ioSizeRange>\n" +
                "                           <ioType>SIZE</ioType>\n" +
                "                           <minIOSize>15</minIOSize>\n" +
                "                           <maxIOSize>20</maxIOSize>\n" +
                "                       </ioSizeRange>\n" +
                "                   </IOSizeRanges>\n" +
                "               </rule>\n" +
                "           </rules>";

        initFs(ruleStr, 15);
        String testData = "0123456789012345678901234567890123456789";
        checkGetObjectAcc(fs, new Path(testRootPath, "c/test-file1.txt"), testData, true, "write 8 bytes file ,should use acc", 0, 10);
        fs.close();
        initFs(ruleStr, 20);
        checkGetObjectAcc(fs, new Path(testRootPath, "c/test-file1.txt"), testData, true, "write 8 bytes file ,should use acc", 0, 10);
        fs.close();
        initFs(ruleStr, 21);
        checkGetObjectAcc(fs, new Path(testRootPath, "c/test-file1.txt"), testData, false, "write 8 bytes file ,should use acc", 0, 10);
        fs.close();
        initFs(ruleStr, 14);
        checkGetObjectAcc(fs, new Path(testRootPath, "c/test-file1.txt"), testData, false, "write 8 bytes file ,should use acc", 0, 10);
    }


    //            No.   Operation             Suc    Acc    Resource                                                                                             Time                           Duration(us)
//            ---   ---------             -----  -----  --------                                                                                             ----                           ------------
//            1     GET_OBJECT_METADATA  true   false root-path/test-b0655aa6-63c2-4bd6-9a94-279372f0fc79/c/test-file1.txt                                 2025-08-27T12:35:31.822235Z    50725
//            2     GET_OBJECT           true   true  root-path/test-b0655aa6-63c2-4bd6-9a94-279372f0fc79/c/test-file1.txt(0-9)                            2025-08-27T12:35:31.885643Z    227526
//            3     GET_OBJECT           true   false root-path/test-b0655aa6-63c2-4bd6-9a94-279372f0fc79/c/test-file1.txt(10-19)                          2025-08-27T12:35:31.885644Z    52725
//            4     GET_OBJECT           true   false root-path/test-b0655aa6-63c2-4bd6-9a94-279372f0fc79/c/test-file1.txt(20-29)                          2025-08-27T12:35:32.114138Z    275692
//            5     GET_OBJECT           true   false root-path/test-b0655aa6-63c2-4bd6-9a94-279372f0fc79/c/test-file1.txt(30-39)                          2025-08-27T12:35:32.114217Z    50137
//            === End of Operation Statistics ===
    @Test
    public void testIOSizeAcc2() throws IOException {
        String ruleStr = "  <rules>\n" +
                "               <rule>\n" +
                "                   <IOSizeRanges>\n" +
                "                       <ioSizeRange>\n" +
                "                           <ioType>HEAD</ioType>\n" +
                "                           <ioSize>10</ioSize>\n" +
                "                       </ioSizeRange>\n" +
                "                   </IOSizeRanges>\n" +
                "               </rule>\n" +
                "           </rules>";

        initFs(ruleStr, 10);
        String testData = "0123456789012345678901234567890123456789";
        Path testFile = new Path(testRootPath, "c/test-file1.txt");
        int read_start = 0;
        int read_end = testData.length() - 1;

        System.out.println(String.format(
                "Checking get object access with %s, filepath=%s, length=%d",
                "Read Head IO size", testFile, testData.getBytes().length
        ));

        byte[] testDataBytes = testData.getBytes();
        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testDataBytes);
        }
        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test read operation
        fs.getStore().getOSSManager().getOperationStats().clear();
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[min(testDataBytes.length, read_end - read_start)];
            int bytesRead = in.read(buffer, read_start, read_end);

            assertEquals(min(testDataBytes.length, read_end - read_start), bytesRead, "Should read the same number of bytes");
            //获取testDataBytes由read_start，到read_end的数据
            byte[] expected = Arrays.copyOfRange(testDataBytes, read_start, read_end);

            assertArrayEquals(expected, buffer, "Should read the same data");
            List<OperationStat> accOperation = fs.getStore().getOSSManager().getOperationStats().stream()
                    .filter(operationStat -> {
                        return operationStat.getOperationName() == OssActionEnum.GET_OBJECT
                                && operationStat.isUseAcc();
                    }).collect(Collectors.toList());

            assertEquals(1, accOperation.size());
        }
    }


    @Test
    public void testIOSizeAcc3() throws IOException {
        String ruleStr = "  <rules>\n" +
                "               <rule>\n" +
                "                   <IOSizeRanges>\n" +
                "                       <ioSizeRange>\n" +
                "                           <ioType>TAIL</ioType>\n" +
                "                           <ioSize>10</ioSize>\n" +
                "                       </ioSizeRange>\n" +
                "                   </IOSizeRanges>\n" +
                "               </rule>\n" +
                "           </rules>";

        initFs(ruleStr, 10);
        String testData = "0123456789012345678901234567890123456789";
        Path testFile = new Path(testRootPath, "c/test-file1.txt");
        int read_start = 0;
        int read_end = testData.length() - 1;

        System.out.println(String.format(
                "Checking get object access with %s, filepath=%s, length=%d",
                "Read Head IO size", testFile, testData.getBytes().length
        ));

        byte[] testDataBytes = testData.getBytes();
        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testDataBytes);
        }
        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test read operation
        fs.getStore().getOSSManager().getOperationStats().clear();
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[min(testDataBytes.length, read_end - read_start)];
            int bytesRead = in.read(buffer, read_start, read_end);

            assertEquals(min(testDataBytes.length, read_end - read_start), bytesRead, "Should read the same number of bytes");
            //获取testDataBytes由read_start，到read_end的数据
            byte[] expected = Arrays.copyOfRange(testDataBytes, read_start, read_end);

            assertArrayEquals(expected, buffer, "Should read the same data");
            List<OperationStat> accOperation = fs.getStore().getOSSManager().getOperationStats().stream()
                    .filter(operationStat -> {
                        return operationStat.getOperationName() == OssActionEnum.GET_OBJECT
                                && operationStat.isUseAcc();
                    }).collect(Collectors.toList());

            assertEquals(1, accOperation.size());
        }
    }


    boolean checkGetObjectAcc(AliyunOSSPerformanceFileSystem fs, Path testFile, String testData,
                              boolean expectedAcc, String scene, int read_start, int read_end) throws IOException {
        System.out.println(String.format(
                "Checking get object access with %s, filepath=%s, length=%d, expectedAcc=%b",
                scene, testFile, testData.getBytes().length, expectedAcc
        ));

        byte[] testDataBytes = testData.getBytes();
        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(testDataBytes);
        }
        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test read operation
        fs.getStore().getOSSManager().getOperationStats().clear();
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[min(testDataBytes.length, read_end - read_start)];
            int bytesRead = in.read(buffer, read_start, read_end);

            assertEquals(min(testDataBytes.length, read_end - read_start), bytesRead, "Should read the same number of bytes");
            //获取testDataBytes由read_start，到read_end的数据
            byte[] expected = Arrays.copyOfRange(testDataBytes, read_start, read_end);

            assertArrayEquals(expected, buffer, "Should read the same data");
            OperationStat operation = fs.getStore().getOSSManager().getOperationStats().stream()
                    .filter(operationStat -> {
                        return operationStat.getOperationName() == OssActionEnum.GET_OBJECT;
                    }).findFirst().get();
            assertEquals(expectedAcc, operation.isUseAcc());
        }
        return true;
    }
}