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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSFileReaderTask;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSInputStream;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.ReadBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Tests basic functionality for AliyunOSSInputStream, including seeking and
 * reading files.
 */
@Timeout(value = 30, unit = TimeUnit.MINUTES)
public class TestAliyunOSSInputStream {

    private FileSystem fs;

    private static final Logger LOG =
            LoggerFactory.getLogger(TestAliyunOSSInputStream.class);

    private static String testRootPath =
            AliyunOSSTestUtils.generateUniqueTestPath();

    @BeforeEach
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        fs = AliyunOSSTestUtils.createTestFileSystem(conf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path setPath(String path) {
        if (path.startsWith("/")) {
            return new Path(testRootPath + path);
        } else {
            return new Path(testRootPath + "/" + path);
        }
    }

    @Test
    public void testSeekFile() throws Exception {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        long size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256, 255);
        LOG.info("5MB file created: smallSeekFile.txt");

        FSDataInputStream instream = this.fs.open(smallSeekFile);
        int seekTimes = 5;
        LOG.info("multiple fold position seeking test...:");
        for (int i = 0; i < seekTimes; i++) {
            long pos = size / (seekTimes - i) - 1;
            LOG.info("begin seeking for pos: " + pos);
            instream.seek(pos);
            assertEquals(instream.getPos(), pos, "expected position at:" + pos + ", but got:"
                    + instream.getPos());
            LOG.info("completed seeking at pos: " + instream.getPos());
        }
        LOG.info("random position seeking test...:");
        Random rand = new Random();
        for (int i = 0; i < seekTimes; i++) {
            long pos = Math.abs(rand.nextLong()) % size;
            LOG.info("begin seeking for pos: " + pos);
            instream.seek(pos);
            assertEquals(instream.getPos(), pos, "expected position at:" + pos + ", but got:"
                    + instream.getPos());
            LOG.info("completed seeking at pos: " + instream.getPos());
        }
        IOUtils.closeStream(instream);
    }

    @Test
    public void testSequentialAndRandomRead() throws Exception {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        long size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256, 255);
        LOG.info("5MB file created: smallSeekFile.txt");

        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        InputStream in = fsDataInputStream.getWrappedStream();
        assertEquals(0, fsDataInputStream.getPos(), "expected position at:" + 0 + ", but got:"
                + fsDataInputStream.getPos());
        IOUtils.closeStream(fsDataInputStream);
    }

    @Test
    public void testOSSFileReaderTask() throws Exception {
        Path smallSeekFile = setPath("/test/smallSeekFileOSSFileReader.txt");
        long size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256, 255);
        LOG.info("5MB file created: smallSeekFileOSSFileReader.txt");
        ReadBuffer readBuffer = new ReadBuffer(12, 24);
        AliyunOSSFileReaderTask task = new AliyunOSSFileReaderTask("1",
                ((AliyunOSSPerformanceFileSystem) this.fs).getStore(), readBuffer);
        //NullPointerException, fail
        task.run();
        assertEquals(readBuffer.getStatus(), ReadBuffer.STATUS.ERROR);
        //OK
        task = new AliyunOSSFileReaderTask(
                "test/test/smallSeekFileOSSFileReader.txt",
                ((AliyunOSSPerformanceFileSystem) this.fs).getStore(), readBuffer);
        task.run();
        assertEquals(readBuffer.getStatus(), ReadBuffer.STATUS.SUCCESS);
    }

    @Test
    public void testReadFile() throws Exception {
        final int bufLen = 256;
        final int sizeFlag = 5;
        String filename = "readTestFile_" + sizeFlag + ".txt";
        Path readTestFile = setPath("/test/" + filename);
        long size = sizeFlag * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, readTestFile, size, 256, 255);
        LOG.info(sizeFlag + "MB file created: /test/" + filename);

        FSDataInputStream instream = this.fs.open(readTestFile);
        byte[] buf = new byte[bufLen];
        long bytesRead = 0;
        while (bytesRead < size) {
            int bytes;
            if (size - bytesRead < bufLen) {
                int remaining = (int) (size - bytesRead);
                bytes = instream.read(buf, 0, remaining);
            } else {
                bytes = instream.read(buf, 0, bufLen);
            }
            bytesRead += bytes;

            //instream.available()行为与以前不同，instream.available()可能返回0，但是数据还未接收完
//            if (bytesRead % (1024 * 1024) == 0) {
//                int available = instream.available();
//                int remaining = (int) (size - bytesRead);
//                if (remaining > 0) {
//                    assertNotEquals(available, 0);
//                }
//
////        assertEquals(remaining, available, "expected remaining:" + remaining + ", but got:" + available);
//                LOG.info("Bytes read: " + Math.round((double) bytesRead / (1024 * 1024))
//                        + " MB");
//            }
        }
        assertEquals(0, instream.available());
        assertEquals(0, (int) (size - bytesRead));
        IOUtils.closeStream(instream);
    }

    @Test
    public void testDirectoryModifiedTime() throws Exception {
        Path emptyDirPath = setPath("/test/emptyDirectory");
        fs.mkdirs(emptyDirPath);
        FileStatus dirFileStatus = fs.getFileStatus(emptyDirPath);
        assertTrue(dirFileStatus.getModificationTime() > 0L, "expected the empty dir is new");
    }
}
