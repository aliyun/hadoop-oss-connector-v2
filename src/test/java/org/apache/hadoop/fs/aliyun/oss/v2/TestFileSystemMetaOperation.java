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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSTestUtils.TEST_FS_OSS_NAME;
import static org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSTestUtils.getURI;
import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.FS_OSS_PERFORMANCE_FLAGS_CREATE;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance comparison test between AliyunOSSPerformanceFileSystem and AliyunOSSFileSystem.
 * This test evaluates the optimization effects of AliyunOSSPerformanceFileSystem by comparing
 * the number of OSS operations for various file system operations.
 */
public class TestFileSystemMetaOperation {

    private Configuration conf;
    private Path testRootPath;

    AliyunOSSFileSystemStore store;
    OssManager ossManager;
    private AliyunOSSPerformanceFileSystem fs;

    @BeforeEach
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.addResource("core-site.xml");
        String testDir = "test-" + UUID.randomUUID().toString();
        testRootPath = new Path("/root-path/" + testDir);
    }

    private void initFs(String ossClientImpl, String fileSystemImpl) throws IOException {
        if (StringUtils.isEmpty(ossClientImpl)) {
            throw new IllegalArgumentException("ossClientImpl cannot be null");
        }

        if (StringUtils.isEmpty(fileSystemImpl)) {
            throw new IllegalArgumentException("fileSystemImpl cannot be null");
        }

        System.out.println("--**test initFs**-- ： with ossClientImpl = " + ossClientImpl);
        System.out.println("--**test initFs**-- ： with fileSystemImpl = " + fileSystemImpl);

        conf.set("fs.oss.client.factory.impl", ossClientImpl);
        conf.set("fs.oss.impl", fileSystemImpl);

        // Initialize the file system
        fs = (AliyunOSSPerformanceFileSystem) FileSystem.get(getURI(conf), conf);

        store = fs.getStore();

        ossManager = store.getOSSManager();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (fs != null) {
            // Clean up test files
            fs.delete(testRootPath, true);
            fs.close();
        }
    }


    /**
     * 测试文件创建功能的多种场景
     * 
     * 测试场景包括：
     * 1. 创建新文件：当文件不存在时，创建应成功
     * 2. 覆盖已存在文件：当文件已存在且overwrite=true时，应成功覆盖原文件内容
     * 3. 不覆盖已存在文件：当文件已存在且overwrite=false时，应抛出FileAlreadyExistsException异常
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"
    })
    public void testCreateFile(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testCreateFile**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 测试场景1: 创建新文件（文件不存在）
        Path testFile = new Path(testRootPath, "test-create-new.txt");
        assertFalse(fs.exists(testFile), "Test file should not exist initially");

        ossManager.clearTracking();
        ossManager.startTracking("create new file");
        String testData = "Test data for create file operation";
        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(testData.getBytes());
        }
        ossManager.stopTracking();
        ossManager.printTracking();
        assertTrue(fs.exists(testFile), "Test file should exist after creation");

        // 测试场景2: 覆盖已存在的文件（overwrite = true）
        ossManager.startTracking("overwrite existing file");
        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write("Updated data".getBytes());
        }
        ossManager.stopTracking();
        ossManager.printTracking();

        // 验证文件内容已更新
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals("Updated data", content, "File content should be updated");
        }

        // 测试场景3: 不覆盖已存在的文件（overwrite = false）- 应该失败
        ossManager.startTracking("create file when exists without overwrite");
        assertThrows(FileAlreadyExistsException.class, () -> {
            try (FSDataOutputStream out = fs.create(testFile, false)) {
                out.write("This should not be written".getBytes());
            }
        });
        ossManager.stopTracking();
        ossManager.printTracking();
    }

    /**
     * 测试当同名文件已存在时创建文件的场景
     * 
     * 测试场景包括：
     * 1. overwrite = false，文件已存在，应该抛出FileAlreadyExistsException异常
     * 2. overwrite = true，文件已存在，应该成功覆盖原文件内容
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testCreateFileWhenExists(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testCreateFileWhenExists**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        Path testFile = new Path(testRootPath, "test-create-exists.txt");

        // 先创建文件
        String initialData = "Initial data";
        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(initialData.getBytes());
        }

        assertTrue(fs.exists(testFile), "Test file should exist initially");

        // 场景1: overwrite = false，文件已存在，应该抛出异常
        assertThrows(FileAlreadyExistsException.class, () -> {
            try (FSDataOutputStream out = fs.create(testFile, false)) {
                out.write("This should fail".getBytes());
            }
        });


        // 验证文件内容没有改变
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals(initialData, content, "File content should remain unchanged");
        }

        // 场景2: overwrite = true，文件已存在，应该成功覆盖
        String updatedData = "Updated data";
        ossManager.startTracking();
        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(updatedData.getBytes());
        }
        ossManager.stopTracking();
        ossManager.printTracking();
        // 验证文件内容已更新
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals(updatedData, content, "File content should be updated");
        }
    }

    /**
     * 测试在开启性能优化开关情况下，当同名文件已存在时创建文件的场景
     * 
     * 测试场景包括：
     * 1. overwrite = false，文件已存在，应该抛出FileAlreadyExistsException异常
     * 2. overwrite = true，文件已存在，应该成功覆盖原文件内容
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testCreateFileWhenExistsWithPerformance(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        // 开启创建文件的性能优化开关
        fs.getConf().set(FS_OSS_PERFORMANCE_FLAGS_CREATE, "true");
        System.out.println("--**test testCreateFileWhenExists**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        Path testFile = new Path(testRootPath, "test-create-exists.txt");

        // 先创建文件
        String initialData = "Initial data";
        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(initialData.getBytes());
        }

        assertTrue(fs.exists(testFile), "Test file should exist initially");

        // 场景1: overwrite = false，文件已存在，应该抛出异常
        assertThrows(FileAlreadyExistsException.class, () -> {
            try (FSDataOutputStream out = fs.create(testFile, false)) {
                out.write("This should fail".getBytes());
            }
        });


        // 验证文件内容没有改变
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals(initialData, content, "File content should remain unchanged");
        }

        // 场景2: overwrite = true，文件已存在，应该成功覆盖
        String updatedData = "Updated data";
        ossManager.startTracking();
        try (FSDataOutputStream out = fs.create(testFile, true)) {
            out.write(updatedData.getBytes());
        }
        ossManager.stopTracking();
        ossManager.printTracking();
        // 验证文件内容已更新
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals(updatedData, content, "File content should be updated");
        }
    }


    /**
     * 测试当同名目录已存在时创建文件的场景
     * 
     * 测试场景包括：
     * 1. 创建一个目录
     * 2. 尝试在同名目录位置创建文件，应该抛出FileAlreadyExistsException异常
     * 3. 验证原目录依然存在且未被修改
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testCreateFileWhenDirectoryExists(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testCreateFileWhenDirectoryExists**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        Path testDir = new Path(testRootPath, "test-dir");
        // 先创建目录
        fs.mkdirs(testDir);

        assertTrue(fs.exists(testDir), "Test directory should exist initially");
        assertTrue(fs.getFileStatus(testDir).isDirectory(), "Test path should be a directory");

        // 尝试在同名目录位置创建文件，应该抛出异常
        ossManager.startTracking();
        Path conflictingFile = new Path(testRootPath, "test-dir"); // 同名路径
        assertThrows(FileAlreadyExistsException.class, () -> {
            try (FSDataOutputStream out = fs.create(conflictingFile, false)) {
                out.write("This should fail".getBytes());
            }
        });
        ossManager.stopTracking();
        ossManager.printTracking();


        // 验证目录依然存在且未被修改
        assertTrue(fs.exists(testDir), "Test directory should still exist");
        assertTrue(fs.getFileStatus(testDir).isDirectory(), "Test path should still be a directory");
    }


    /**
     * 测试文件打开功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 正常打开文件：文件存在，打开应成功并能读取正确内容
     * 2. 打开不存在的文件：应抛出FileNotFoundException异常
     * 3. 打开目录：应抛出FileNotFoundException异常
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testOpenFile(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testOpenFile**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        Path testFile = new Path(testRootPath, "test-open.txt");
        String testData = "Test data for open file operation";

        // Create test file first
        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(testData.getBytes());
        }

        assertTrue(fs.exists(testFile), "Test file should exist");

        // Test opening existing file
        ossManager.startTracking();
        try (FSDataInputStream in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals(testData, content, "Should read the same data");
        }
        ossManager.stopTracking();
        ossManager.printTracking();

        // Test opening non-existent file - should throw exception
        ossManager.startTracking();
        Path nonExistentFile = new Path(testRootPath, "non-existent.txt");
        assertThrows(java.io.FileNotFoundException.class, () -> {
            fs.open(nonExistentFile);
        });
        ossManager.stopTracking();
        ossManager.printTracking();


        // Test opening directory - should throw exception
        fs.mkdirs(testRootPath);
        ossManager.startTracking();
        assertThrows(java.io.FileNotFoundException.class, () -> {
            fs.open(testRootPath);
        });
        ossManager.stopTracking();
        ossManager.printTracking();
    }

    /**
     * 测试文件重命名功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 正常重命名：源文件存在，目标文件不存在，重命名应成功
     * 2. 目标文件已存在：重命名应失败
     * 3. 目标路径的父目录不存在：重命名应失败
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            

            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testRenameFile(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testRenameFile**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 场景1: 测试重命名文件（目标文件不存在）
        Path srcFile = new Path(testRootPath, "src-file.txt");
        Path dstFile = new Path(testRootPath, "dst-file.txt");

        // 创建源文件
        String testData = "Test data for rename file operation";
        try (FSDataOutputStream out = fs.create(srcFile, false)) {
            out.write(testData.getBytes());
        }

        assertTrue(fs.exists(srcFile), "Source file should exist");
        assertFalse(fs.exists(dstFile), "Destination file should not exist");

        // 执行重命名操作
        ossManager.startTracking("rename normal");
        assertTrue(fs.rename(srcFile, dstFile), "Rename operation should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();

        assertFalse(fs.exists(srcFile), "Source file should not exist after rename");
        assertTrue(fs.exists(dstFile), "Destination file should exist after rename");

        // 验证文件内容正确性
        try (FSDataInputStream in = fs.open(dstFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals(testData, content, "Should read the same data");
        }

        // 场景2: 测试重命名文件（目标文件已存在）
        Path srcFile2 = new Path(testRootPath, "src-file2.txt");
        try (FSDataOutputStream out = fs.create(srcFile2, false)) {
            out.write("Source file 2 data".getBytes());
        }

        // 确保目标文件已存在
        assertTrue(fs.exists(dstFile), "Destination file should exist");
        assertTrue(fs.exists(srcFile2), "Source file 2 should exist");

        // 重命名应失败，因为目标文件已存在
        ossManager.startTracking("rename target file exist");
        assertThrows(FileAlreadyExistsException.class, () -> fs.rename(srcFile2, dstFile));
        ossManager.stopTracking();
        ossManager.printTracking();

        // 场景3: 测试重命名文件到不存在的父目录
        Path srcFile3 = new Path(testRootPath, "src-file3.txt");
        Path dstFileWithNonExistentParent = new Path(testRootPath, "non-existent-dir/dst-file3.txt");
        try (FSDataOutputStream out = fs.create(srcFile3, false)) {
            out.write("Source file 3 data".getBytes());
        }

        // 父目录不存在，重命名应抛出异常
        ossManager.startTracking("rename:target parent not exist ");
        assertThrows(IOException.class, () -> fs.rename(srcFile3, dstFileWithNonExistentParent));
        ossManager.stopTracking();
        ossManager.printTracking();
    }

    /**
     * 测试目录重命名功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 正常重命名目录：源目录存在且包含文件和子目录，目标目录不存在，重命名应成功
     * 2. 目标目录已存在：重命名应失败
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testRenameDirectory(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testRenameDirectory**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 场景1: 测试重命名目录（目标目录不存在）
        // 创建源目录及其中的文件和子目录
        Path srcDir = new Path(testRootPath, "src-dir");
        Path srcSubDir = new Path(srcDir, "sub-dir");
        fs.mkdirs(srcSubDir);

        Path srcFile1 = new Path(srcDir, "file1.txt");
        Path srcFile2 = new Path(srcSubDir, "file2.txt");

        try (FSDataOutputStream out = fs.create(srcFile1, false)) {
            out.write("File 1 data".getBytes());
        }

        try (FSDataOutputStream out = fs.create(srcFile2, false)) {
            out.write("File 2 data".getBytes());
        }

        assertTrue(fs.exists(srcDir), "Source directory should exist");
        assertTrue(fs.exists(srcSubDir), "Source sub directory should exist");
        assertTrue(fs.exists(srcFile1), "Source file 1 should exist");
        assertTrue(fs.exists(srcFile2), "Source file 2 should exist");

        // 执行重命名操作：将src-dir重命名为dst-dir
        Path dstDir = new Path(testRootPath, "dst-dir");

        assertFalse(fs.exists(dstDir), "Destination directory should not exist");

        ossManager.startTracking("rename normal dir，Destination directory does not exist");
        assertTrue(fs.rename(srcDir, dstDir), "Rename directory operation should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();
        assertFalse(fs.exists(srcDir), "Source directory should not exist after rename");
        assertTrue(fs.exists(dstDir), "Destination directory should exist after rename");

        // 验证重命名后的目录结构和文件内容
        Path dstSubDir = new Path(dstDir, "sub-dir");
        Path dstFile1 = new Path(dstDir, "file1.txt");
        Path dstFile2 = new Path(dstSubDir, "file2.txt");

        assertTrue(fs.exists(dstSubDir), "Destination sub directory should exist");
        assertTrue(fs.exists(dstFile1), "Destination file 1 should exist");
        assertTrue(fs.exists(dstFile2), "Destination file 2 should exist");

        // 验证文件内容正确性
        try (FSDataInputStream in = fs.open(dstFile1)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals("File 1 data", content, "Should read the same data for file 1");
        }

        try (FSDataInputStream in = fs.open(dstFile2)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals("File 2 data", content, "Should read the same data for file 2");
        }

        // 场景2: 测试重命名目录（目标目录已存在）
        // 创建另一个源目录和已存在的目标目录
        Path srcDir2 = new Path(testRootPath, "src-dir2");
        fs.mkdirs(srcDir2);

        // 在源目录中创建文件
        Path srcDir2File = new Path(srcDir2, "src-dir2-file.txt");
        try (FSDataOutputStream out = fs.create(srcDir2File, false)) {
            out.write("Source dir 2 file data".getBytes());
        }

        Path dstDir2 = new Path(testRootPath, "dst-dir2");
        fs.mkdirs(dstDir2);

        // 在目标目录中创建一个原有文件
        Path dstDir2ExistingFile = new Path(dstDir2, "existing-file.txt");
        try (FSDataOutputStream out = fs.create(dstDir2ExistingFile, false)) {
            out.write("Existing file data".getBytes());
        }

        assertTrue(fs.exists(srcDir2), "Source directory 2 should exist");
        assertTrue(fs.exists(dstDir2), "Destination directory 2 should exist");
        assertTrue(fs.exists(srcDir2File), "Source directory 2 file should exist");
        assertTrue(fs.exists(dstDir2ExistingFile), "Destination directory 2 existing file should exist");

        // 目标目录已存在,重命名不会失败，而是会把源目录下的内容移动到目标目录下
        ossManager.startTracking("rename target dir exist");
        assertTrue(fs.rename(srcDir2, dstDir2), "Rename directory operation should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();

        // 验证源目录已不存在
        assertFalse(fs.exists(srcDir2), "Source directory 2 should not exist after rename");

        // 验证目标目录仍然存在
        assertTrue(fs.exists(dstDir2), "Destination directory 2 should still exist after rename");

        //递归列举打印目标目录
        listDirectoryContents(fs, dstDir2, "");


        // 验证源目录中的文件已移动到目标目录
        Path movedFile = new Path(dstDir2, srcDir2.getName() + "/src-dir2-file.txt");
        assertTrue(fs.exists(movedFile), "Source directory 2 file should be moved to destination directory");

        // 验证目标目录中原来的文件仍然存在
        assertTrue(fs.exists(dstDir2ExistingFile), "Destination directory 2 existing file should still exist");

        // 验证移动后的文件内容正确性
        try (FSDataInputStream in = fs.open(movedFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals("Source dir 2 file data", content, "Should read the same data for moved file");
        }

        // 验证原有文件内容正确性
        try (FSDataInputStream in = fs.open(dstDir2ExistingFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals("Existing file data", content, "Should read the same data for existing file");
        }
    }


    private void listDirectoryContents(AliyunOSSPerformanceFileSystem fs, Path path, String space) {
        StringBuilder sb = new StringBuilder();
        try {
            FileStatus[] fileStatuses = fs.listStatus(path);
            for (FileStatus status : fileStatuses) {
                sb.append(space + status.getPath().toString() + " (" + (status.isDirectory() ? "directory" : "file") + ")\n");
                if (status.isDirectory()) {
                    // 递归列出子目录内容
                    listDirectoryContents(fs, status.getPath(), space + "  ");
                }
            }
        } catch (IOException e) {
            System.err.println("Error listing directory contents: " + e.getMessage());
        }
        // 打印目录内容
        System.out.println("Listing directory: " + path);
        System.out.println(sb.toString());
    }

    /**
     * 测试文件删除功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 删除存在的文件：文件存在，删除应成功
     * 2. 删除不存在的文件：文件不存在，删除应返回false
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testDeleteFile(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testDeleteFile**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 场景1: 删除存在的文件
        Path testFile = new Path(testRootPath, "test-delete.txt");
        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write("Test data for delete operation".getBytes());
        }

        assertTrue(fs.exists(testFile), "Test file should exist");

        // 执行删除操作
        ossManager.startTracking("delete existing file");
        assertTrue(fs.delete(testFile, false), "Delete file should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();
        assertFalse(fs.exists(testFile), "Test file should not exist after deletion");

        // 场景2: 删除不存在的文件
        Path nonExistentFile = new Path(testRootPath, "non-existent.txt");
        ossManager.startTracking("delete non-existent file");
        assertFalse(fs.delete(nonExistentFile, false), "Delete non-existent file should return false");
        ossManager.stopTracking();
        ossManager.printTracking();
    }

    /**
     * 测试目录删除功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 删除非空目录（不使用递归标志）：应抛出IOException异常
     * 2. 递归删除目录：应成功删除目录及其所有内容
     * 3. 删除空目录（不使用递归标志）：应成功删除
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testDeleteDirectory(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testDeleteDirectory**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 创建测试目录结构：test-delete-dir/sub-dir/，并在其中创建file1.txt和file2.txt
        Path testDir = new Path(testRootPath, "test-delete-dir");
        Path subDir = new Path(testDir, "sub-dir");
        fs.mkdirs(subDir);

        Path file1 = new Path(testDir, "file1.txt");
        Path file2 = new Path(subDir, "file2.txt");

        try (FSDataOutputStream out = fs.create(file1, false)) {
            out.write("File 1 data".getBytes());
        }

        try (FSDataOutputStream out = fs.create(file2, false)) {
            out.write("File 2 data".getBytes());
        }

        // 验证所有创建的文件和目录都存在
        assertTrue(fs.exists(testDir), "Test directory should exist");
        assertTrue(fs.exists(subDir), "Sub directory should exist");
        assertTrue(fs.exists(file1), "File 1 should exist");
        assertTrue(fs.exists(file2), "File 2 should exist");

        // 场景1: 删除非空目录（不使用递归标志）- 应该失败并抛出IOException
        ossManager.startTracking("delete non-empty directory");
        assertThrows(IOException.class, () -> fs.delete(testDir, false));
        ossManager.stopTracking();
        ossManager.printTracking();

        // 场景2: 使用递归标志删除目录 - 应该成功删除目录及其所有内容
        ossManager.startTracking("delete directory recursively");
        assertTrue(fs.delete(testDir, true), "Delete directory recursively should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();
        assertFalse(fs.exists(testDir), "Test directory should not exist after deletion");
        assertFalse(fs.exists(subDir), "Sub directory should not exist after deletion");
        assertFalse(fs.exists(file1), "File 1 should not exist after deletion");
        assertFalse(fs.exists(file2), "File 2 should not exist after deletion");

        // 场景3: 删除空目录（不使用递归标志）- 应该成功
        Path emptyDir = new Path(testRootPath, "empty-dir");
        fs.mkdirs(emptyDir);
        assertTrue(fs.exists(emptyDir), "Empty directory should exist");
        ossManager.startTracking("delete empty directory");
        assertTrue(fs.delete(emptyDir, false), "Delete empty directory should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();
        assertFalse(fs.exists(emptyDir), "Empty directory should not exist after deletion");
    }

    /**
     * 测试文件系统列表状态功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 列出目录内容：目录存在且包含文件和子目录，应成功返回所有项目
     * 2. 列出单个文件：应返回包含该文件的数组
     * 3. 列出不存在的路径：应抛出FileNotFoundException异常
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testListStatus(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testListStatus**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 场景1: 创建测试目录结构用于列出目录内容
        // 创建目录 test-list-dir
        Path testDir = new Path(testRootPath, "test-list-dir");
        fs.mkdirs(testDir);

        // 在目录中创建两个文件和一个子目录
        Path file1 = new Path(testDir, "file1.txt");
        Path file2 = new Path(testDir, "file2.txt");
        Path subDir = new Path(testDir, "sub-dir");
        Path subFile = new Path(subDir, "sub-file.txt");

        try (FSDataOutputStream out = fs.create(file1, false)) {
            out.write("File 1 data".getBytes());
        }

        try (FSDataOutputStream out = fs.create(file2, false)) {
            out.write("File 2 data".getBytes());
        }

        fs.mkdirs(subDir);
        try (FSDataOutputStream out = fs.create(subFile, false)) {
            out.write("Sub file data".getBytes());
        }

        // 执行列出目录内容操作
        ossManager.startTracking("list directory content");
        FileStatus[] statuses = fs.listStatus(testDir);
        ossManager.stopTracking();
        ossManager.printTracking();
        assertEquals(3, statuses.length, "Should list 3 items (2 files + 1 directory)");

        List<String> names = new ArrayList<>();
        for (FileStatus status : statuses) {
            names.add(status.getPath().getName());
        }

        assertTrue(names.contains("file1.txt"), "Should contain file1.txt");
        assertTrue(names.contains("file2.txt"), "Should contain file2.txt");
        assertTrue(names.contains("sub-dir"), "Should contain sub-dir");

        // 场景2: 列出单个文件（应该返回1个项目）
        ossManager.startTracking("list single file");
        FileStatus[] fileStatuses = fs.listStatus(file1);
        ossManager.stopTracking();
        ossManager.printTracking();
        assertEquals(1, fileStatuses.length, "Should list 1 item for file");

        //路径为oss://bucket/path
        assertEquals(fs.getConf().get(TEST_FS_OSS_NAME)+file1, fileStatuses[0].getPath().toString(), "Should return the file itself");

        // 场景3: 列出不存在的路径（应该抛出FileNotFoundException）
        Path nonExistent = new Path(testRootPath, "non-existent");
        ossManager.startTracking("listStatus not exist");
        assertThrows(java.io.FileNotFoundException.class, () -> fs.listStatus(nonExistent));
        ossManager.stopTracking();
        ossManager.printTracking();
    }

    /**
     * 测试目录创建功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 创建目录：父目录存在，创建应成功
     * 2. 创建嵌套目录：多级目录不存在，应成功创建所有层级
     * 3. 重复创建目录：目录已存在，再次创建应成功
     * 4. 目录名冲突：同名文件已存在，创建目录应失败
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testMkdirs(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testMkdirs**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 场景1: 父目录存在时创建目录
        Path dir1 = new Path(testRootPath, "dir1");
        ossManager.startTracking("testMkdirs");
        assertTrue(fs.mkdirs(dir1), "Creating directory should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();
        assertTrue(fs.exists(dir1), "Directory should exist");
        assertTrue(fs.getFileStatus(dir1).isDirectory(), "Should be a directory");

        // 场景2: 创建嵌套目录结构
        Path nestedDir = new Path(testRootPath, "parent/child/grandchild");
        ossManager.startTracking("recursive mkdir");
        assertTrue(fs.mkdirs(nestedDir), "Creating nested directories should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();
        assertTrue(fs.exists(nestedDir), "Nested directory should exist");

        // 场景3: 重复创建已存在的目录
        ossManager.startTracking("repeat mkdir");
        assertTrue(fs.mkdirs(dir1), "Creating existing directory should succeed");
        ossManager.stopTracking();
        ossManager.printTracking();

        // 场景4: 尝试创建与现有文件同名的目录
        Path file1 = new Path(testRootPath, "file-as-dir");
        try (FSDataOutputStream out = fs.create(file1, false)) {
            out.write("Some data".getBytes());
        }

        ossManager.startTracking("mkdirs when file exists");
        assertThrows(FileAlreadyExistsException.class, () -> fs.mkdirs(file1), 
            "Should throw FileAlreadyExistsException when trying to create directory with same name as existing file");
        ossManager.stopTracking();
        ossManager.printTracking();
    }

    /**
     * 测试文件状态获取功能的多种场景
     * <p>
     * 测试场景包括：
     * 1. 获取文件的状态：创建文件并验证FileStatus信息正确性
     * 2. 获取目录的状态：创建目录并验证FileStatus信息正确性
     * 3. 获取不存在路径的状态：尝试获取不存在路径的状态，应抛出FileNotFoundException异常
     *
     * @param ossClientImpl  OSS客户端实现类
     * @param fileSystemImpl 文件系统实现类
     * @throws IOException IO异常
     */
    @ParameterizedTest
    @CsvSource({
            "org.apache.hadoop.fs.aliyun.oss.v2.DefaultOSSClientFactory,org.apache.hadoop.fs.aliyun.oss.v2.AliyunOSSPerformanceFileSystem"

    })
    public void testGetFileStatus(String ossClientImpl, String fileSystemImpl) throws IOException {
        initFs(ossClientImpl, fileSystemImpl);
        System.out.println("--**test testGetFileStatus**-- ：ossClientImpl= " + ossClientImpl + "    fileSystemImpl=" + fileSystemImpl);

        // 场景1: 获取文件的状态
        // 创建一个测试文件并验证其状态信息是否正确
        Path testFile = new Path(testRootPath, "test-status.txt");
        String testData = "Test data";
        try (FSDataOutputStream out = fs.create(testFile, false)) {
            out.write(testData.getBytes());
        }

        ossManager.startTracking("get file status");
        FileStatus fileStatus = fs.getFileStatus(testFile);
        ossManager.stopTracking();
        ossManager.printTracking();
        assertEquals(fs.getConf().get(TEST_FS_OSS_NAME) + testFile, fileStatus.getPath().toString(), "File path should match");
        assertEquals(testData.length(), fileStatus.getLen(), "File length should match");
        assertFalse(fileStatus.isDirectory(), "Should not be a directory");
        assertTrue(fileStatus.isFile(), "Should be a file");

        // 场景2: 获取目录的状态
        // 创建一个测试目录并验证其状态信息是否正确
        Path testDir = new Path(testRootPath, "test-status-dir");
        fs.mkdirs(testDir);

        ossManager.startTracking("test-status-dir");
        FileStatus dirStatus = fs.getFileStatus(testDir);
        ossManager.stopTracking();
        ossManager.printTracking();
        assertEquals(fs.getConf().get(TEST_FS_OSS_NAME) + testDir, dirStatus.getPath().toString(), "Directory path should match");
        assertTrue(dirStatus.isDirectory(), "Should be a directory");
        assertFalse(dirStatus.isFile(), "Should not be a file");

        // 场景3: 获取不存在文件的状态
        // 尝试获取不存在的文件状态，应该抛出FileNotFoundException异常
        Path nonExistent = new Path(testRootPath, "non-existent");
        ossManager.startTracking("getFileStatus not exists");
        assertThrows(java.io.FileNotFoundException.class, () -> fs.getFileStatus(nonExistent));
        ossManager.stopTracking();
        ossManager.printTracking();
    }
}