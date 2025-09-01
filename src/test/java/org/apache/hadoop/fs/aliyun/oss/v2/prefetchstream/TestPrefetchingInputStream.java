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

package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import com.aliyun.sdk.service.oss2.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem;
import org.apache.hadoop.fs.aliyun.oss.v2.Constants;
import org.apache.hadoop.fs.aliyun.oss.v2.OssManager;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSTestUtils.getURI;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PrefetchingInputStream.
 */
public class TestPrefetchingInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(TestPrefetchingInputStream.class);

    private static final int BLOCK_SIZE = 4096;
    private static final int FILE_SIZE = 16 * BLOCK_SIZE; // 64KB file

    private Configuration conf;
    private ExecutorService futurePool;
    private OssManager ossManager;
    private LocalDirAllocator dirAllocator;
    private byte[] testData;
    private ObjectAttributes testAttributes;
    private OSSPerformanceStatistics statistics;
    private AliyunOSSPerformanceFileSystem fs;
    private Path testRootPath;
    private Path testFile;
    private String bucketName =                 "yacai-data-pushdown";

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
        fs = (AliyunOSSPerformanceFileSystem) FileSystem.get(getURI(conf), conf);
    }


    @BeforeEach
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.setLong(Constants.PREFETCH_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(Constants.PREFETCH_BLOCK_COUNT_KEY, 2);
        conf.set("fs.oss.buffer.dir", System.getProperty("java.io.tmpdir"));

        futurePool = Executors.newFixedThreadPool(3, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("mock-future-pool-thread-" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });

        ossManager = OssManager.init(conf);

        dirAllocator = new LocalDirAllocator("fs.oss.buffer.dir");

        // Create test data
        testData = new byte[FILE_SIZE];
        for (int i = 0; i < FILE_SIZE; i++) {
            testData[i] = (byte) (i % 255);
        }


        String testDir = "test-" + UUID.randomUUID().toString();
        testRootPath = new Path("/root-path/" + testDir);
         testFile = new Path(testRootPath, "test-key");

        testAttributes = new ObjectAttributes(
                bucketName,
                testFile,
                "test-key",
                null,
                null,
                FILE_SIZE);

        //创建测试文件
        initFs("org.apache.hadoop.fs.aliyun.oss.DefaultOSSClientFactory","org.apache.hadoop.fs.aliyun.oss.AliyunOSSPerformanceFileSystem");
        try (OutputStream out = fs.create(testFile, true)) {
            out.write(testData);
        } finally {
            fs.close();
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (futurePool != null) {
            futurePool.shutdown();
        }
        fs.delete(testFile);
    }

    private PrefetchingInputStream createInputStream() {
        ReadOpContext context = new ReadOpContext(
                testFile, null,
                new ExecutorServiceFuturePool(futurePool),
                BLOCK_SIZE,
                2);

        return new PrefetchingInputStream(context, testAttributes, ossManager, conf, dirAllocator, statistics);
    }

    @Test
    public void testRead() throws Exception {
        try (PrefetchingInputStream stream = createInputStream()) {
            assertEquals(0, stream.getPos());

            // Read one byte
            int b = stream.read();
            assertEquals(testData[0] & 0xff, b);
            assertEquals(1, stream.getPos());

            // Read multiple bytes
            byte[] buffer = new byte[100];
            int bytesRead = stream.read(buffer);
            assertEquals(100, bytesRead);
            assertEquals(101, stream.getPos());

            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i + 1], buffer[i]);
            }
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testReadWithOffset() throws Exception {
        try (PrefetchingInputStream stream = createInputStream()) {
            byte[] buffer = new byte[100];

            // Read into buffer with offset
            int bytesRead = stream.read(buffer, 10, 50);
            assertEquals(50, bytesRead);
            assertEquals(50, stream.getPos());

            for (int i = 0; i < 50; i++) {
                assertEquals(testData[i], buffer[i + 10]);
            }

            // Verify other parts of buffer are untouched
            for (int i = 0; i < 10; i++) {
                assertEquals(0, buffer[i]);
            }
            for (int i = 60; i < 100; i++) {
                assertEquals(0, buffer[i]);
            }
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testSeek() throws Exception {
        try (PrefetchingInputStream stream = createInputStream()) {
            assertEquals(0, stream.getPos());

            // Seek to position
            stream.seek(1000);
            assertEquals(1000, stream.getPos());

            // Read from new position
            int b = stream.read();
            assertEquals(testData[1000] & 0xff, b);
            assertEquals(1001, stream.getPos());
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testSeekPastEOF() {
        try (PrefetchingInputStream stream = createInputStream()) {
            assertThrows(IOException.class, () -> {
                stream.seek(FILE_SIZE + 100);
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testSeekNegative() {
        try (PrefetchingInputStream stream = createInputStream()) {
            assertThrows(IOException.class, () -> {
                stream.seek(-1);
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testAvailable() throws Exception {
        try (PrefetchingInputStream stream = createInputStream()) {
            // Available should return non-negative value
            int available = stream.available();
            assertTrue(available >= 0);
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testClose() throws Exception {
        PrefetchingInputStream stream = createInputStream();
        assertFalse(stream.isClosed());

        stream.close();
        assertTrue(stream.isClosed());

        // Reading from closed stream should throw IOException
        assertThrows(IOException.class, () -> {
            stream.read();
        });

        assertThrows(IOException.class, () -> {
            stream.read(new byte[10]);
        });

        assertThrows(IOException.class, () -> {
            stream.seek(100);
        });

        assertThrows(IOException.class, () -> {
            stream.available();
        });
    }

    @Test
    public void testHasCapability() {
        try (PrefetchingInputStream stream = createInputStream()) {
            assertTrue(stream.hasCapability("iostatistics"));
            assertTrue(stream.hasCapability("IOSTATISTICS"));
            assertTrue(stream.hasCapability("readahead"));
            assertTrue(stream.hasCapability("READAHEAD"));

            assertFalse(stream.hasCapability("unsupported"));
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetPos() throws Exception {
        try (PrefetchingInputStream stream = createInputStream()) {
            assertEquals(0, stream.getPos());

            stream.read();
            assertEquals(1, stream.getPos());

            stream.seek(100);
            assertEquals(100, stream.getPos());

            stream.read(new byte[50]);
            assertEquals(150, stream.getPos());
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testGetPosAfterClose() throws Exception {
        PrefetchingInputStream stream = createInputStream();
        stream.read();
        long pos = stream.getPos();
        stream.close();

        // getPos should still work after close and return last position
        assertEquals(pos, stream.getPos());
    }

    @Test
    public void testReadAfterEOF() throws Exception {
        try (PrefetchingInputStream stream = createInputStream()) {
            // Seek to end of file
            stream.seek(FILE_SIZE - 10);

            // Read last bytes
            byte[] buffer = new byte[20];
            int bytesRead = stream.read(buffer);
            assertEquals(10, bytesRead);

            // Next read should return -1 (EOF)
            assertEquals(-1, stream.read());
            assertEquals(-1, stream.read(buffer));
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testReadZeroLength() throws Exception {
        try (PrefetchingInputStream stream = createInputStream()) {
            byte[] buffer = new byte[0];
            int bytesRead = stream.read(buffer);
            assertEquals(0, bytesRead);
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

}