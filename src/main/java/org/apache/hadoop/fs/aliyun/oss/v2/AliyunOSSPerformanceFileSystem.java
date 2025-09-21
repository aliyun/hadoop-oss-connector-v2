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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.*;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ListParam;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ListResultV2;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ObjectMetadataParam;
import org.apache.hadoop.fs.aliyun.oss.v2.model.ObjectSummaryParam;
import org.apache.hadoop.fs.aliyun.oss.v2.model.AliyunOSSDirEmptyFlag;
import org.apache.hadoop.fs.aliyun.oss.v2.model.AliyunOSSStatusProbeEnum;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.*;
import org.apache.hadoop.fs.aliyun.oss.v2.readahead.ReadAheadInputStream;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.impl.OutputStreamStatistics;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;
import static org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils.*;
import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * Implementation of {@link FileSystem} for <a href="https://oss.aliyun.com">
 * Aliyun OSS</a>, used to access OSS blob system in a filesystem style.
 */
public class AliyunOSSPerformanceFileSystem extends FileSystem {
    private static final Logger LOG =
            LoggerFactory.getLogger(AliyunOSSPerformanceFileSystem.class);

    protected URI uri;
    protected String bucket;
    protected String username;
    protected Path workingDir;
    protected OSSDataBlocks.BlockFactory blockFactory;
    protected BlockOutputStreamStatistics blockOutputStreamStatistics;
    protected OSSPerformanceStatistics ossPerformanceStatistics;
    protected int uploadPartSize;
    protected int blockOutputActiveBlocks;
    protected AliyunOSSFileSystemStore store;
    protected int maxKeys;
    protected int maxReadAheadPartNumber;
    protected int maxConcurrentCopyTasksPerDir;
    protected ExecutorService boundedThreadPool;
    protected ExecutorService boundedCopyThreadPool;
    protected boolean putIfNotExistImplementedByServer = false;
    private ExecutorServiceFuturePool futurePool;

    protected static final PathFilter DEFAULT_FILTER = new PathFilter() {
        @Override
        public boolean accept(Path file) {
            return true;
        }
    };

    private String prefetchVersion = "v2";
    private LocalDirAllocator directoryAllocator;
    private int prefetchBlockSize;
    private int prefetchBlockCount;
    private InputPolicy inputPolicy;
    private long readAhead;
    private long asyncDrainThreshold;
    private ChangeDetectionPolicy changeDetectionPolicy;
    private int smallFileThreshold;
    private int prefetchNumAfterSeek;
    private int prefetchThreshold;
    private int bigIoThreshold;
    private int ossMergeSize;
    private int bigIoPrefetchSize;
    private int amplificationFactor;


    @Override
    public FSDataOutputStream append(Path path, int bufferSize,
                                     Progressable progress) throws IOException {
        throw new IOException("Append is not supported!");
    }

    @Override
    public void close() throws IOException {
        try {
            store.close();
            boundedThreadPool.shutdown();
            boundedCopyThreadPool.shutdown();
        } finally {
            super.close();
        }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission,
                                     boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        LOG.debug("create: path={}, permission={}, overwrite={}, bufferSize={}, replication={}, blockSize={}",
                path, permission, overwrite, bufferSize, replication, blockSize);
        String key = pathToKey(path);
        FileStatus status = null;

        boolean isSkipCheck = getConf().getBoolean(FS_OSS_PERFORMANCE_FLAGS_CREATE, FS_OSS_PERFORMANCE_FLAGS_CREATE_DEFAULT) == true;
        if (overwrite == false || isSkipCheck == false) {
            try {
                // get the status or throw a FNFE
                status = innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.ALL, false);

                // Directory can not be overwrited
                if (status.isDirectory()) {
                    throw new FileAlreadyExistsException(path + " is a directory");
                }
                if (!overwrite) {
                    // path references a file and overwrite is disabled
                    throw new FileAlreadyExistsException(path + " already exists");
                }
                LOG.debug("Overwriting file {}", path);
            } catch (FileNotFoundException e) {
                // this means the file is not found
            }
        }

        return new FSDataOutputStream(
                new AliyunOSSBlockOutputStream(getConf(),
                        store,
                        key,
                        uploadPartSize,
                        blockFactory,
                        blockOutputStreamStatistics,
                        new SemaphoredDelegatingExecutor(boundedThreadPool,
                                blockOutputActiveBlocks, true)),
                statistics);
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileNotFoundException if the parent directory is not present -or
     *                               is not a directory.
     */
    @Override
    public FSDataOutputStream createNonRecursive(Path path,
                                                 FsPermission permission,
                                                 EnumSet<CreateFlag> flags,
                                                 int bufferSize,
                                                 short replication,
                                                 long blockSize,
                                                 Progressable progress) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            // expect this to raise an exception if there is no parent

            if (!getFileStatus(parent).isDirectory()) {
                throw new FileAlreadyExistsException("Not a directory: " + parent);
            }
        }
        return create(path, permission, flags.contains(CreateFlag.OVERWRITE),
                bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        try {
            OSSFileStatus ossFileStatus = innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.ALL, true);
            return innerDelete(ossFileStatus, recursive);
        } catch (FileNotFoundException e) {
            LOG.debug("Couldn't delete {} - does not exist", path);
            return false;
        }
    }

    private boolean innerDelete(OSSFileStatus ossFileStatus, boolean recursive) throws IOException {
        // get qulified path
        Path f = ossFileStatus.getPath();
        String p = f.toUri().getPath();
        FileStatus[] statuses;
        // indicating root directory "/".
        if (p.equals("/")) {
            LOG.error("OSS: Cannot delete the root directory." + " Path: {}. Recursive: {}",
                    ossFileStatus.getPath(), recursive);
            return rejectRootDirectoryDelete(ossFileStatus.getEmptyFlag() == AliyunOSSDirEmptyFlag.EMPTY,
                    recursive);
        }

        String key = pathToKey(f);
        if (ossFileStatus.isDirectory()) {
            if (!recursive) {
                // Check whether it is an empty directory or not
                if (ossFileStatus.getEmptyFlag() != AliyunOSSDirEmptyFlag.EMPTY) {
                    throw new IOException(
                            "Cannot remove directory " + f + ": It is not empty! it is not empty or unknown");
                } else {
                    // Delete empty directory without '-r'
                    key = AliyunOSSUtils.maybeAddTrailingSlash(key);
                    store.deleteObject(key);
                }
            } else {
                store.deleteDirs(key);
            }
        } else {
            store.deleteObject(key);
        }

        // to avoid creating fake directory for root directory
        createFakeDirectoryIfNecessary(f.getParent());
        return true;
    }

    /**
     * Implements the specific logic to reject root directory deletion.
     * The caller must return the result of this call, rather than
     * attempt to continue with the delete operation: deleting root
     * directories is never allowed. This method simply implements
     * the policy of when to return an exit code versus raise an exception.
     *
     * @param isEmptyDir empty directory or not
     * @param recursive  recursive flag from command
     * @return a return code for the operation
     * @throws PathIOException if the operation was explicitly rejected.
     */
    private boolean rejectRootDirectoryDelete(boolean isEmptyDir,
                                              boolean recursive) throws IOException {
        LOG.info("oss delete the {} root directory of {}", bucket, recursive);
        if (isEmptyDir) {
            return true;
        }
        if (recursive) {
            return false;
        } else {
            // reject
            throw new PathIOException(bucket, "Cannot delete root path");
        }
    }

    protected void createFakeDirectoryIfNecessary(Path f) throws IOException {
        if (f == null || f.isRoot()) {
            LOG.debug("Not creating fake directory for root");
            return;
        }

        String key = pathToKey(f);
        String dirKey = AliyunOSSUtils.maybeAddTrailingSlash(key);
        if (StringUtils.isNotEmpty(dirKey)) {
            if (putIfNotExistImplementedByServer) {
                // The file or path is created only if it does not exist.
                // Having this functionality handled by the server can reduce one head QPS
                store.storeEmptyFileIfNecessary(dirKey);
                LOG.debug("Creating new fake directory if Necessary at {}", f);

            } else {
                if (!ossExists(f, AliyunOSSStatusProbeEnum.DIRECTORIES)) {
                    LOG.debug("Creating new fake directory at {}", f);
                    store.storeEmptyFile(dirKey);
                }
            }
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.ALL, false);
    }

    @Override
    public String getScheme() {
        return "oss";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public int getDefaultPort() {
        return Constants.OSS_DEFAULT_PORT;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Deprecated
    public long getDefaultBlockSize() {
        return getConf().getLong(FS_OSS_BLOCK_SIZE_KEY, FS_OSS_BLOCK_SIZE_DEFAULT);
    }

    @Override
    public String getCanonicalServiceName() {
        // Does not support Token
        return null;
    }

    /**
     * Initialize new FileSystem.
     *
     * @param name the uri of the file system, including host, port, etc.
     * @param conf configuration of the file system
     * @throws IOException IO problems
     */
    public void initialize(URI name, Configuration conf) throws IOException {
        bucket = name.getHost();
        LOG.debug("Initializing AliyunOSSFileSystem for {}", bucket);

        conf = ProviderUtils.excludeIncompatibleCredentialProviders(
                conf, AliyunOSSPerformanceFileSystem.class);
        super.initialize(name, conf);
        setConf(conf);

        uri = java.net.URI.create(name.getScheme() + "://" + name.getAuthority());
        // Username is the current user at the time the FS was instantiated.
        username = UserGroupInformation.getCurrentUser().getShortUserName();
        workingDir = new Path("/user", username).makeQualified(uri, null);
        long keepAliveTime = longOption(conf,
                KEEPALIVE_TIME_KEY, KEEPALIVE_TIME_DEFAULT, 0);
        blockOutputActiveBlocks = intOption(conf,
                UPLOAD_ACTIVE_BLOCKS_KEY, UPLOAD_ACTIVE_BLOCKS_DEFAULT, 1);

        uploadPartSize = (int) AliyunOSSUtils.getMultipartSizeProperty(conf,
                MULTIPART_UPLOAD_PART_SIZE_KEY, MULTIPART_UPLOAD_PART_SIZE_DEFAULT);
        String uploadBuffer = conf.getTrimmed(FAST_UPLOAD_BUFFER,
                DEFAULT_FAST_UPLOAD_BUFFER);

        blockOutputStreamStatistics = new OutputStreamStatistics();
        blockFactory = OSSDataBlocks.createFactory(this, uploadBuffer);
        LOG.debug("Using OSSBlockOutputStream with buffer = {}; block={};" +
                        " queue limit={}",
                uploadBuffer, uploadPartSize, blockOutputActiveBlocks);
        putIfNotExistImplementedByServer = conf.getBoolean(FS_OSS_PUT_IF_NOT_EXIST_KEY,
                FS_OSS_PUT_IF_NOT_EXIST_DEFAULT);
        store = new AliyunOSSFileSystemStore();
        store.initialize(name, conf, username, statistics);
        maxKeys = conf.getInt(MAX_PAGING_KEYS_KEY, MAX_PAGING_KEYS_DEFAULT);

        smallFileThreshold = conf.getInt(SMALL_FILE_THRESHOLD_KEY, SMALL_FILE_THRESHOLD_DEFAULT);
        prefetchNumAfterSeek
                = conf.getInt(PREFETCH_NUM_AFTER_SEEK_KEY, PREFETCH_NUM_AFTER_SEEK_DEFAULT);
        prefetchThreshold = conf.getInt(PREFETCH_THRESHOLD_KEY, PREFETCH_THRESHOLD_DEFAULT);
        bigIoThreshold= conf.getInt(BIG_IO_THRESHOLD_KEY, BIG_IO_THRESHOLD_DEFAULT);
        bigIoPrefetchSize= conf.getInt(BIG_IO_PREFETCH_SIZE_KEY, BIG_IO_PREFETCH_SIZE_DEFAULT);
        ossMergeSize = conf.getInt(OSS_MERGE_SIZE_KEY, OSS_MERGE_SIZE_DEFAULT);
        amplificationFactor = conf.getInt(AMPLIFICATION_FACTOR_KEY, AMPLIFICATION_FACTOR_DEFAULT);

        LOG.info("AliyunOss BUFFER_PREFETCH_DIR {}",conf.get(BUFFER_PREFETCH_DIR_KEY));

        maxConcurrentCopyTasksPerDir = AliyunOSSUtils.intPositiveOption(conf,
                Constants.MAX_CONCURRENT_COPY_TASKS_PER_DIR_KEY,
                Constants.MAX_CONCURRENT_COPY_TASKS_PER_DIR_DEFAULT);

        int maxCopyThreads = AliyunOSSUtils.intPositiveOption(conf,
                Constants.MAX_COPY_THREADS_NUM_KEY,
                Constants.MAX_COPY_THREADS_DEFAULT);

        int maxCopyTasks = AliyunOSSUtils.intPositiveOption(conf,
                Constants.MAX_COPY_TASKS_KEY,
                Constants.MAX_COPY_TASKS_DEFAULT);

        this.boundedCopyThreadPool = BlockingThreadPoolExecutorService.newInstance(
                maxCopyThreads, maxCopyTasks, 60L,
                TimeUnit.SECONDS, "oss-copy-unbounded");

        inputPolicy = InputPolicy.getPolicy(
                conf.getTrimmed(INPUT_FADVISE,
                        Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_DEFAULT), InputPolicy.Normal);

        readAhead = conf.getLong(READAHEAD_RANGE, DEFAULT_READAHEAD_RANGE);
        asyncDrainThreshold = conf.getLong(ASYNC_DRAIN_THRESHOLD, DEFAULT_ASYNC_DRAIN_THRESHOLD);
        LOG.debug("Input fadvise policy = {}", inputPolicy);
        changeDetectionPolicy = ChangeDetectionPolicy.getPolicy(conf);
        LOG.debug("Change detection policy = {}", changeDetectionPolicy);

        int maxThreads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
        this.prefetchBlockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
        this.prefetchBlockCount = conf.getInt(PREFETCH_BLOCK_COUNT_KEY, PREFETCH_BLOCK_DEFAULT_COUNT);
        int totalTasks = AliyunOSSUtils.intPositiveOption(conf,
                Constants.MAX_TOTAL_TASKS_KEY, Constants.MAX_TOTAL_TASKS_DEFAULT);
        maxReadAheadPartNumber = AliyunOSSUtils.intPositiveOption(conf,
                Constants.MULTIPART_DOWNLOAD_AHEAD_PART_MAX_NUM_KEY,
                Constants.MULTIPART_DOWNLOAD_AHEAD_PART_MAX_NUM_DEFAULT);
        int activeTasksForBoundedThreadPool = maxThreads;
        this.prefetchVersion = conf.get(PREFETCH_VERSION_KEY, PREFETCH_VERSION_DEFAULT);
        int numPrefetchThreads = "v2".equals(this.prefetchVersion) ? this.prefetchBlockCount : 0;
        int waitingTasksForBoundedThreadPool = maxThreads + totalTasks + numPrefetchThreads;
        this.boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
                activeTasksForBoundedThreadPool, waitingTasksForBoundedThreadPool, keepAliveTime, TimeUnit.SECONDS,
                "oss-boundedThreadPool-shared");

        ossPerformanceStatistics = new OSSPerformanceStatistics("AliyunOSSPerformanceStatistics", "Aliyun OSS performance statistics");
        futurePool = new ExecutorServiceFuturePool(
                new SemaphoredDelegatingExecutor(
                        boundedThreadPool,
                        activeTasksForBoundedThreadPool + waitingTasksForBoundedThreadPool,
                        true));
    }


    /**
     * Turn a path (relative or otherwise) into an OSS key.
     *
     * @param path the path of the file.
     * @return the key of the object that represents the file.
     */
    protected String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(workingDir, path);
        }

        return path.toUri().getPath().substring(1);
    }

    protected Path keyToPath(String key) {
        return new Path("/" + key);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        String key = pathToKey(path);
        LOG.debug("List status for path: {} key:{}", path, key);


        final List<FileStatus> result = new ArrayList<FileStatus>();
        final FileStatus fileStatus = getFileStatus(path);

        if (fileStatus.isDirectory()) {
            LOG.debug("listStatus: doing listObjects for directory {}", key);

            ListParam listParam = new ListParam(bucket, key,
                    maxKeys, null, null, false);
            ListResultV2 objects = store.listObjects(listParam);
            while (true) {
                for (ObjectSummaryParam objectSummaryParam : objects.getObjectSummaries()) {
                    LOG.debug("getObjectSummaries: {}", objectSummaryParam.getKey());

                    String objKey = objectSummaryParam.getKey();
                    if (objKey.equals(key + "/")) {
                        LOG.debug("ObjectSummaries Ignoring: {}", objKey);
                        continue;
                    } else {
                        Path keyPath = keyToPath(objectSummaryParam.getKey())
                                .makeQualified(uri, workingDir);
                        LOG.debug("ObjectSummaries Adding: fi: {}", keyPath);

                        result.add(new OSSFileStatus(objectSummaryParam.getSize(), false, 1,
                                getDefaultBlockSize(keyPath),
                                objectSummaryParam.getLastModified().getTime(), keyPath, username));
                    }
                }

                for (String prefix : objects.getCommonPrefixes()) {
                    LOG.debug("getCommonPrefixes: {}", prefix);

                    if (prefix.equals(key + "/")) {
                        LOG.debug("CommonPrefixes Ignoring: {}", prefix);

                        continue;
                    } else {
                        Path keyPath = keyToPath(prefix).makeQualified(uri, workingDir);
                        LOG.debug("CommonPrefixes Adding: rd: {}", keyPath);

                        result.add(getFileStatus(keyPath));
                    }
                }

                if (objects.isTruncated()) {
                    LOG.debug("listStatus: list truncated - getting next batch");
                    objects = store.continueListObjects(listParam, objects);
                } else {
                    break;
                }
            }
        } else {
            LOG.debug("Adding: rd (not a dir): {}", path);

            result.add(fileStatus);
        }

        return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(
            final Path f, final boolean recursive) throws IOException {
        Path qualifiedPath = f.makeQualified(uri, workingDir);
        final FileStatus status = getFileStatus(qualifiedPath);
        PathFilter filter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return status.isFile() || !path.equals(f);
            }
        };
        FileStatusAcceptor acceptor =
                new FileStatusAcceptor.AcceptFilesOnly(qualifiedPath);
        return innerList(f, status, filter, acceptor, recursive);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
            throws IOException {
        return listLocatedStatus(f, DEFAULT_FILTER);
    }

    /**
     * {@inheritDoc}
     *
     * @param f      a path
     * @param filter a path filter
     * @return an iterator that traverses statuses of the children of f
     * @throws IOException if an I/O error occurs
     */
    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
                                                               final PathFilter filter) throws IOException {
        Path qualifiedPath = f.makeQualified(uri, workingDir);
        final FileStatus status = getFileStatus(qualifiedPath);
        FileStatusAcceptor acceptor =
                new FileStatusAcceptor.AcceptAllButSelf(qualifiedPath);
        return innerList(f, status, filter, acceptor, false);
    }

    private RemoteIterator<LocatedFileStatus> innerList(final Path f,
                                                        final FileStatus status,
                                                        final PathFilter filter,
                                                        final FileStatusAcceptor acceptor,
                                                        final boolean recursive) throws IOException {
        Path qualifiedPath = f.makeQualified(uri, workingDir);
        String key = pathToKey(qualifiedPath);

        if (status.isFile()) {
            LOG.debug("{} is a File", qualifiedPath);
            final BlockLocation[] locations = getFileBlockLocations(status,
                    0, status.getLen());
            return store.singleStatusRemoteIterator(filter.accept(f) ? status : null,
                    locations);
        } else {
            return store.createLocatedFileStatusIterator(key, maxKeys, this, filter,
                    acceptor, recursive);
        }
    }

    /**
     * Used to create an empty file that represents an empty directory.
     *
     * @param key directory path
     * @return true if directory is successfully created
     * @throws IOException if there is any error
     */
    protected boolean mkdir(final String key) throws IOException {
        String dirName = key;
        if (StringUtils.isNotEmpty(key)) {
            if (!key.endsWith("/")) {
                dirName += "/";
            }
            store.storeEmptyFile(dirName);
        }
        return true;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission)
            throws IOException {
        try {
            FileStatus fileStatus = getFileStatus(path);

            if (fileStatus.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + path);
            }
        } catch (FileNotFoundException e) {
            validatePath(path);
            String key = pathToKey(path);
            return mkdir(key);
        }
    }

    /**
     * Check whether the path is a valid path.
     *
     * @param path the path to be checked.
     * @throws IOException
     */
    private void validatePath(Path path) throws IOException {
        Path fPart = path.getParent();
        do {
            try {
                FileStatus fileStatus = getFileStatus(fPart);
                if (fileStatus.isDirectory()) {
                    // If path exists and a directory, exit
                    break;
                } else {
                    throw new FileAlreadyExistsException(String.format(
                            "Can't make directory for path '%s', it is a file.", fPart));
                }
            } catch (FileNotFoundException fnfe) {
            }
            fPart = fPart.getParent();
        } while (fPart != null);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        // if f does not exist, there will throw a FileNotFoundException
        // if f is a file, fileStatus.isDirectory will be false
        // if f is a dir, throw a FileNotFoundException
        final OSSFileStatus fileStatus =
                innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.FILE, false);
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + path + " because it is a directory");
        }

        Configuration configuration = getConf();
        LOG.debug("AliyunOss BUFFER_PREFETCH_DIR 1 {}",configuration.get(BUFFER_PREFETCH_DIR_KEY));
        initLocalDirAllocatorIfNotInitialized(configuration);
        ReadOpContext readContext = new ReadOpContext(fileStatus.getPath(),
                fileStatus,
                futurePool,
                max(prefetchBlockSize,bufferSize),
                prefetchBlockCount,
                smallFileThreshold,
                prefetchNumAfterSeek,
                prefetchThreshold,
                bigIoThreshold,
                ossMergeSize,
                bigIoPrefetchSize,
                amplificationFactor);

        readContext.withInputPolicy(inputPolicy)
                .withChangeDetectionPolicy(changeDetectionPolicy)
                .withAsyncDrainThreshold(asyncDrainThreshold)
                .withReadahead(readAhead);

        if ("v2".equals(this.prefetchVersion)) {
            return new FSDataInputStream(
                    new PrefetchingInputStream(
                            readContext.build(),
                            createObjectAttributes(path, fileStatus),
                            store.getOSSManager(),
                            configuration,
                            directoryAllocator,
                            ossPerformanceStatistics));
        } else if ("v3".equals(this.prefetchVersion)) {
            return new FSDataInputStream(
                    new ReadAheadInputStream(readContext,
                            createObjectAttributes(path, fileStatus),
                            store.getOSSManager(),
                            statistics));
        }
        else {
            return new FSDataInputStream(new AliyunOSSInputStream(getConf(),
                    new SemaphoredDelegatingExecutor(boundedThreadPool, maxReadAheadPartNumber, true),
                    maxReadAheadPartNumber, store, pathToKey(path), fileStatus.getLen(), statistics));
        }
    }

    private ObjectAttributes createObjectAttributes(
            final Path path,
            final OSSFileStatus fileStatus) {
        return new ObjectAttributes(bucket,
                path,
                pathToKey(path),
                fileStatus.geteTag(),
                fileStatus.getVersionId(),
                fileStatus.getLen());

    }

    private void initLocalDirAllocatorIfNotInitialized(Configuration conf) {
        if (directoryAllocator == null) {
            synchronized (this) {
                LOG.info("AliyunOss BUFFER_PREFETCH_DIR {}",conf.get(BUFFER_PREFETCH_DIR_KEY));
                directoryAllocator = new LocalDirAllocator(BUFFER_PREFETCH_DIR_KEY);
            }
        }
    }

    @Override
    public boolean rename(Path srcPath, Path dstPath) throws IOException {
        if (srcPath.isRoot()) {
            // Cannot rename root of file system
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot rename the root of a filesystem");
            }
            return false;
        }
        //src can't be a subpath of dst
        Path parent = dstPath.getParent();
        while (parent != null && !srcPath.equals(parent)) {
            parent = parent.getParent();
        }
        if (parent != null) {
            return false;
        }
        FileStatus srcStatus = innerOssGetFileStatus(srcPath, AliyunOSSStatusProbeEnum.ALL, false);

        FileStatus dstStatus;
        try {
            dstStatus = innerOssGetFileStatus(dstPath, AliyunOSSStatusProbeEnum.ALL, false);
        } catch (FileNotFoundException fnde) {
            dstStatus = null;
        }
        if (dstStatus == null) {
            // If dst doesn't exist, check whether dst dir exists or not
            // this raise a FileNotFoundException if dst dir does not exist
            FileStatus dstParentStatus =
                    innerOssGetFileStatus(dstPath.getParent(), AliyunOSSStatusProbeEnum.ALL, false);
            if (!dstParentStatus.isDirectory()) {
                throw new IOException(
                        String.format("Failed to rename %s to %s, %s is a file", srcPath, dstPath,
                                dstPath.getParent()));
            }
        } else {
            if (srcStatus.getPath().equals(dstStatus.getPath())) {
                return !srcStatus.isDirectory();
            } else if (dstStatus.isDirectory()) {
                // If dst is a directory
                dstPath = new Path(dstPath, srcPath.getName());

                OSSFileStatus dstDirWithsrcPathStatus;
                try {
                    dstDirWithsrcPathStatus =
                            innerOssGetFileStatus(dstPath, AliyunOSSStatusProbeEnum.LIST_ONLY, true);
                } catch (FileNotFoundException fnde) {
                    dstDirWithsrcPathStatus = null;
                }

                if (dstDirWithsrcPathStatus != null
                        && dstDirWithsrcPathStatus.getEmptyFlag() != AliyunOSSDirEmptyFlag.EMPTY) {
                    // If dst exists and not a directory / not empty
                    throw new FileAlreadyExistsException(
                            String.format("Failed to rename %s to %s, file already exists or not empty!", srcPath,
                                    dstPath));
                }
            } else {
                // If dst is not a directory
                throw new FileAlreadyExistsException(
                        String.format("Failed to rename %s to %s, file already exists!", srcPath, dstPath));
            }
        }

        boolean succeed;
        if (srcStatus.isDirectory()) {
            succeed = copyDirectory(srcPath, dstPath);
        } else {
            succeed = copyFile(srcPath, srcStatus.getLen(), dstPath);
        }

        return srcPath.equals(dstPath) || (succeed && delete(srcPath, true));
    }

    /**
     * Copy file from source path to destination path.
     * (the caller should make sure srcPath is a file and dstPath is valid)
     *
     * @param srcPath source path.
     * @param srcLen  source path length if it is a file.
     * @param dstPath destination path.
     * @return true if file is successfully copied.
     */
    protected boolean copyFile(Path srcPath, long srcLen, Path dstPath) throws IOException {
        String srcKey = pathToKey(srcPath);
        String dstKey = pathToKey(dstPath);
        return store.copyFile(srcKey, srcLen, dstKey);
    }

    /**
     * Copy a directory from source path to destination path.
     * (the caller should make sure srcPath is a directory, and dstPath is valid)
     *
     * @param srcPath source path.
     * @param dstPath destination path.
     * @return true if directory is successfully copied.
     * @throws IOException if there is any exception.
     */
    protected boolean copyDirectory(Path srcPath, Path dstPath) throws IOException {
        String srcKey = AliyunOSSUtils
                .maybeAddTrailingSlash(pathToKey(srcPath));
        String dstKey = AliyunOSSUtils
                .maybeAddTrailingSlash(pathToKey(dstPath));

        if (dstKey.startsWith(srcKey)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot rename a directory to a subdirectory of self");
            }
            return false;
        }

        store.storeEmptyFile(dstKey);
        AliyunOSSCopyFileContext copyFileContext = new AliyunOSSCopyFileContext();
        ExecutorService executorService = MoreExecutors.listeningDecorator(
                new SemaphoredDelegatingExecutor(boundedCopyThreadPool,
                        maxConcurrentCopyTasksPerDir, true));
        ListParam listParam = new ListParam(bucket, srcKey,
                maxKeys, null, null, true);
        ListResultV2 objects = store.listObjects(listParam);
        // Copy files from src folder to dst
        int copiesToFinish = 0;
        while (true) {
            for (ObjectSummaryParam objectSummaryParam : objects.getObjectSummaries()) {
                String newKey =
                        dstKey.concat(objectSummaryParam.getKey().substring(srcKey.length()));

                //copy operation just copies metadata, oss will support shallow copy
                executorService.execute(new AliyunOSSCopyFileTask(
                        store, objectSummaryParam.getKey(),
                        objectSummaryParam.getSize(), newKey, copyFileContext));
                copiesToFinish++;
                // No need to call lock() here.
                // It's ok to copy one more file if the rename operation failed
                // Reduce the call of lock() can also improve our performance
                if (copyFileContext.isCopyFailure()) {
                    //some error occurs, break
                    LOG.warn("Copy file failed, break {} {}",objectSummaryParam.getKey(),copyFileContext.isCopyFailure());
                    break;
                }
            }
            if (objects.isTruncated()) {
                objects = store.continueListObjects(listParam, objects);
            } else {
                break;
            }
        }
        //wait operations in progress to finish
        copyFileContext.lock();
        try {
            copyFileContext.awaitAllFinish(copiesToFinish);
        } catch (InterruptedException e) {
            LOG.warn("interrupted when wait copies to finish");
        } finally {
            copyFileContext.unlock();
        }
        return !copyFileContext.isCopyFailure();
    }

    @Override
    public void setWorkingDirectory(Path dir) {
        this.workingDir = dir;
    }

    public AliyunOSSFileSystemStore getStore() {
        return store;
    }

    @VisibleForTesting
    OSSDataBlocks.BlockFactory getBlockFactory() {
        return blockFactory;
    }

    @VisibleForTesting
    BlockOutputStreamStatistics getBlockOutputStreamStatistics() {
        return blockOutputStreamStatistics;
    }

//  @Override
//  public boolean hasPathCapability(final Path path, final String capability)
//          throws IOException {
//    final Path p = makeQualified(path);
//    String cap = validatePathCapabilityArgs(p, capability);
//    switch (cap) {
//      // block locations are generated locally
//      case CommonPathCapabilities.VIRTUAL_BLOCK_LOCATIONS:
//        return true;
//
//      default:
//        return super.hasPathCapability(p, cap);
//    }
//  }

    private OSSFileStatus innerOssGetFileStatus(final Path path,
                                                final Set<AliyunOSSStatusProbeEnum> probes,
                                                final boolean needEmptyDirectoryFlag) throws IOException {
        LOG.debug("innerOssGetFileStatus {}", path);
        final Path qualifiedPath = path.makeQualified(uri, workingDir);
        LOG.debug("qualifiedPath {}", qualifiedPath);
        String key = pathToKey(qualifiedPath);
        LOG.debug("key {}", key);

        // Can only determine if it is empty through a list operation
        LOG.debug("checkArgument needEmptyDirectoryFlag {}  probes {};", needEmptyDirectoryFlag, probes);
        if (needEmptyDirectoryFlag && !probes.contains(AliyunOSSStatusProbeEnum.List)) {
            throw new IllegalArgumentException("needEmptyDirectoryFlag is true, but probes does not contain List");
        }

        // Root always exists
        if (key.length() == 0) {
            return new OSSFileStatus(0, true, 1, 0, 0, qualifiedPath, username);
        }

        if (!key.endsWith("/") && probes.contains(AliyunOSSStatusProbeEnum.Head)) {
            // look for the simple file
            ObjectMetadataParam meta = store.getObjectMetadata(key);
            if (meta != null) {
                // meta will not be null.if data is not exist,therer will throw exception
                LOG.debug("Found exact file: normal file {}", key);
                return new OSSFileStatus(meta.getContentLength(), false, 1, getDefaultBlockSize(path),
                        meta.getLastModified().getTime(), qualifiedPath, username);
            }
        }

        // if a DirMarker can be found through a Head operation, it can reduce one list
        // operation. Head operations are relatively lighter than list operations, so
        // doing one more Head operation can be beneficial.
        String dirKey = maybeAddTrailingSlash(key);
        LOG.debug("dirKey {}", dirKey);
        if (needEmptyDirectoryFlag == false && !dirKey.isEmpty() && probes.contains(AliyunOSSStatusProbeEnum.DirMarker)) {
            // look for the DirMarker
            ObjectMetadataParam meta = store.getObjectMetadata(dirKey);
            LOG.debug("Found exact file: DirMarker file {} meta is null:{}", dirKey, meta == null);
            if (meta != null) {
                return new OSSFileStatus(meta.getContentLength(), true, 1, getDefaultBlockSize(path),
                        meta.getLastModified().getTime(), qualifiedPath, username);
            }
        }

        // execute the list
        if (probes.contains(AliyunOSSStatusProbeEnum.List)) {
            // list size is dir marker + at least one entry
            int listSize = needEmptyDirectoryFlag ? 2 : 1;
            LOG.debug("execute the list listSize {}", listSize);

            ListParam listParam = new ListParam(bucket, dirKey,
                    listSize, null, null, false);

            ListResultV2 listResult = store.listObjects(listParam);

            boolean isTruncated = listResult.isTruncated();
            LOG.debug("execute the list getListResultNum {} isTruncated {} Result {}",
                    listResult.getListNum(), isTruncated, listResult.getListResultContent());

            while (isTruncated && (listResult.getListNum() < listSize)) {
                ListResultV2 tempListResult = store.continueListObjects(listParam, listResult);
                isTruncated = tempListResult.isTruncated();
                listResult.getObjectSummaries().addAll(tempListResult.getObjectSummaries());
                listResult.getCommonPrefixes().addAll(tempListResult.getCommonPrefixes());
                LOG.debug("continue List  getListResultNum {} isTruncated {}", listResult.getListNum(),
                        isTruncated);
            }

            if (listResult.getListNum() > 0) {
                if (needEmptyDirectoryFlag) {
                    if (listResult.representsEmptyDirectory(dirKey)) {
                        LOG.debug("Found a directory: {}", dirKey);
                        return new OSSFileStatus(AliyunOSSDirEmptyFlag.EMPTY, 0, 1, 0, 0, qualifiedPath,
                                username);
                    } else {
                        return new OSSFileStatus(AliyunOSSDirEmptyFlag.NOT_EMPTY, 0, 1, 0, 0, qualifiedPath,
                                username);
                    }
                }
                return new OSSFileStatus(0, true, 1, 0, 0, qualifiedPath, username);
            }
        }

        LOG.debug("Not Found: {}", path);
        throw new FileNotFoundException("No such file or directory: " + path);
    }


    private boolean ossExists(final Path path, final Set<AliyunOSSStatusProbeEnum> probes)
            throws IOException {
        try {
            innerOssGetFileStatus(path, probes, false);
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public boolean isDirectory(Path path) throws IOException {
        LOG.debug("isDirectory: path={}", path);

        try {
            OSSFileStatus fileStatus = innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.DIRECTORIES, false);
            return fileStatus.isDirectory();
        } catch (FileNotFoundException e) {
            LOG.debug("isDirectory: path={}. not found or it is a file", path);
            return false; // f does not exist
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public boolean isFile(Path path) throws IOException {
        LOG.debug("isFile: path={}", path);
        try {
            return innerOssGetFileStatus(path, AliyunOSSStatusProbeEnum.FILE, false).isFile();
        } catch (FileNotFoundException e) {
            // not found or it is a dir.
            LOG.debug("isFile: path={} . not found or it is a dir", path);
            return false;
        }
    }
}