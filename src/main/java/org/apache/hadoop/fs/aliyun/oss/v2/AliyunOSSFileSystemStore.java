/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2;


import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.FileStatusAcceptor;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.OSSDataBlocks;
import org.apache.hadoop.fs.aliyun.oss.v2.model.*;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ObjectAttributes;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.BlockLogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * Core implementation of Aliyun OSS Filesystem for Hadoop.
 * Provides the bridging logic between Hadoop's abstract filesystem and
 * Aliyun OSS.
 */
public class AliyunOSSFileSystemStore {
    public static final Logger LOG =
            LoggerFactory.getLogger(AliyunOSSFileSystemStore.class);
    private String username;
    private FileSystem.Statistics statistics;
    private String bucketName;
    private long uploadPartSize;
    private int maxKeys;
    private String serverSideEncryptionAlgorithm;
    private OssManager ossClient;

    public void initialize(URI uri, Configuration conf, String user,
                           FileSystem.Statistics stat) throws IOException {
        this.username = user;
        statistics = stat;

        ossClient = OssManager.init(conf);


        uploadPartSize = AliyunOSSUtils.getMultipartSizeProperty(conf,
                MULTIPART_UPLOAD_PART_SIZE_KEY, MULTIPART_UPLOAD_PART_SIZE_DEFAULT);

        serverSideEncryptionAlgorithm =
                conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM_KEY, "");

        bucketName = uri.getHost();

        maxKeys = conf.getInt(MAX_PAGING_KEYS_KEY, MAX_PAGING_KEYS_DEFAULT);
    }


    /**
     * Delete an object, and update write operation statistics.
     *
     * @param key key to blob to delete.
     */
    public void deleteObject(String key) throws IOException {
        try {
            ossClient.deleteObject(bucketName, key);
        } catch (ObjectNotFoundException e) {
            LOG.warn("delete Object " + key + " does not exist");
        }
        statistics.incrementWriteOps(1);
    }

    /**
     * Delete a list of keys, and update write operation statistics.
     *
     * @param keysToDelete collection of keys to delete.
     * @throws IOException if failed to delete objects.
     */
    public void deleteObjects(List<String> keysToDelete) throws IOException {
        if (CollectionUtils.isEmpty(keysToDelete)) {
            LOG.warn("Keys to delete is empty.");
            return;
        }

        int retry = 10;
        int tries = 0;
        while (CollectionUtils.isNotEmpty(keysToDelete)) {
            List<String> deletedObjects = ossClient.deleteObjects(bucketName, keysToDelete, false);

            statistics.incrementWriteOps(1);
            keysToDelete = keysToDelete.stream().filter(item -> !deletedObjects.contains(item))
                    .collect(Collectors.toList());
            tries++;
            if (tries == retry) {
                break;
            }
        }

        if (tries == retry && CollectionUtils.isNotEmpty(keysToDelete)) {
            // Most of time, it is impossible to try 10 times, expect the
            // Aliyun OSS service problems.
            throw new IOException("Failed to delete Aliyun OSS objects for " + tries + " times." + " keysToDelete:" + keysToDelete);
        }
    }

    /**
     * Delete a directory from Aliyun OSS.
     *
     * @param key directory key to delete.
     * @throws IOException if failed to delete directory.
     */
    public void deleteDirs(String key) throws IOException {
        ListParam listParam = new ListParam(bucketName, key, maxKeys, null, null, true);

        while (true) {
            ListResultV2 objects = listObjects(listParam);
            statistics.incrementReadOps(1);
            List<String> keysToDelete = new ArrayList<String>();
            for (ObjectSummaryParam objectSummaryParam : objects.getObjectSummaries()) {
                keysToDelete.add(objectSummaryParam.getKey());
            }
            deleteObjects(keysToDelete);
            if (objects.isTruncated()) {
                listParam.setContinuationToken(
                        objects.getNextContinuationToken());

            } else {
                break;
            }
        }
    }

    /**
     * Return metadata of a given object key.
     * In cases where the QPS is too high, OSS will return a 5xx error. Therefore,
     * only an explicit 'Not Found' response can be considered as non-existent;
     * other types of exceptions need to be thrown or retried.
     *
     * @param key object key.
     * @return return null if key does not exist.
     */
    public ObjectMetadataParam getObjectMetadata(String key) throws IOException {
        try {
            ObjectMetadataParam objectMeta = ossClient.getObjectMetadata(bucketName, key);
            statistics.incrementReadOps(1);
            return objectMeta;
        } catch (ObjectNotFoundException fe) {
            LOG.debug("getObjectMetadata: " + fe.getMessage());
            // non-existent
            return null;
        }
    }

    /**
     * Upload an empty file as an OSS object, using single upload.
     *
     * @param key object key.
     * @throws IOException if failed to upload object.
     */
    public void storeEmptyFile(String key) throws IOException {
        byte[] buffer = new byte[0];
        ByteArrayInputStream in = new ByteArrayInputStream(buffer);
        try {
            ossClient.putObject(bucketName, key, in, 0, null, false);
            statistics.incrementWriteOps(1);
        } finally {
            in.close();
        }
    }

    /**
     * Upload an empty file as an OSS object, using single upload.
     *
     * @param key object key.
     * @throws IOException if failed to upload object.
     */
    public void storeEmptyFileIfNecessary(String key) throws IOException {
//    ObjectMetadata dirMeta = new ObjectMetadata();
        byte[] buffer = new byte[0];
        ByteArrayInputStream in = new ByteArrayInputStream(buffer);
//    dirMeta.setContentLength(0);
//    dirMeta.setHeader("x-oss-forbid-overwrite", "true");
        try {
            ossClient.putObject(bucketName, key, in, 0, null, true);
            statistics.incrementWriteOps(1);
        } catch (ObjectAlreadyExistsException e) {
            statistics.incrementWriteOps(1);
            LOG.debug("Object already exists, ignore it: " + key);
        } finally {
            in.close();
        }
    }

    /**
     * Copy an object from source key to destination key.
     *
     * @param srcKey source key.
     * @param srcLen source file length.
     * @param dstKey destination key.
     * @return true if file is successfully copied.
     */
    public boolean copyFile(String srcKey, long srcLen, String dstKey) throws IOException {
        return singleCopy(srcKey, dstKey);
    }

    /**
     * Use single copy to copy an OSS object.
     * (The caller should make sure srcPath is a file and dstPath is valid)
     *
     * @param srcKey source key.
     * @param dstKey destination key.
     * @return true if object is successfully copied.
     */
    protected boolean singleCopy(String srcKey, String dstKey) throws IOException {
        ossClient.copyObject(bucketName, srcKey, bucketName, dstKey);
        statistics.incrementWriteOps(1);
        return true;
    }

    /**
     * Upload a file as an OSS object, using single upload.
     *
     * @param key  object key.
     * @param file local file to upload.
     * @throws IOException if failed to upload object.
     */
    public void uploadObject(String key, File file) throws IOException {
        File object = file.getAbsoluteFile();
        FileInputStream fis = new FileInputStream(object);
        try {
            String eTag = ossClient.putObject(bucketName, key, fis, (int) object.length(), serverSideEncryptionAlgorithm, false);
            LOG.debug(eTag);
            statistics.incrementWriteOps(1);
        } finally {
            fis.close();
        }
    }

    /**
     * Upload an input stream as an OSS object, using single upload.
     *
     * @param key  object key.
     * @param in   input stream to upload.
     * @param size size of the input stream.
     * @throws IOException if failed to upload object.
     */
    public void uploadObject(String key, InputStream in, long size)
            throws IOException {
//    ObjectMetadataParam meta = new ObjectMetadataParam();
//    meta.setContentLength(size);
//
//    if (StringUtils.isNotEmpty(serverSideEncryptionAlgorithm)) {
//      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
//    }

        String eTag = ossClient.putObject(bucketName, key, in, size, serverSideEncryptionAlgorithm, false);
        LOG.debug(eTag);
        statistics.incrementWriteOps(1);
    }

    /**
     * list objects.
     *
     * @param listRequest list request.
     * @return a list of matches.
     */
    public ListResultV2 listObjects(ListParam listParam) throws IOException {
        ListResultV2 listResult = ossClient.listObjectsV2(listParam);
        statistics.incrementReadOps(1);
        return listResult;
    }

    /**
     * continue to list objects depends on previous list result.
     *
     * @param listRequest   list request.
     * @param preListResult previous list result.
     * @return a list of matches.
     */
    public ListResultV2 continueListObjects(ListParam lastListParam,
                                            ListResultV2 lastListResultV2) throws IOException {


        lastListParam.setContinuationToken(lastListResultV2.getNextContinuationToken());
        ListResultV2 listResult = ossClient.listObjectsV2(lastListParam);

        statistics.incrementReadOps(1);
        return listResult;
    }


    /**
     * Retrieve a part of an object.
     *
     * @param key              the object name that is being retrieved from the Aliyun OSS.
     * @param objectAttributes the attributes of the object, including its size and other metadata.
     * @param byteStart        start position.
     * @param byteEnd          end position.
     * @param blockLogContext
     * @return This method returns null if the key is not found.
     * @throws Exception if there is an error during retrieval.
     */
    public InputStream retrieve(String key, ObjectAttributes objectAttributes, long byteStart, long byteEnd, BlockLogContext blockLogContext) throws Exception {
        try {
            InputStream in = ossClient.getObject(bucketName, key, objectAttributes, byteStart, byteEnd, blockLogContext);
            statistics.incrementReadOps(1);
            return in;
        } catch (ObjectNotFoundException e) {
            LOG.error("Exception thrown when store retrieves key: "
                    + key + ", exception: " + e);
            return null;
        }
    }

    /**
     * Close OSS client properly.
     */
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
    }

    /**
     * Clean up all objects matching the prefix.
     *
     * @param prefix Aliyun OSS object prefix.
     * @throws IOException if failed to clean up objects.
     */
    public void purge(String prefix) throws Exception {
        deleteDirs(prefix);
    }

    public RemoteIterator<LocatedFileStatus> singleStatusRemoteIterator(
            final FileStatus fileStatus, final BlockLocation[] locations) {
        return new RemoteIterator<LocatedFileStatus>() {
            private boolean hasNext = true;

            @Override
            public boolean hasNext() throws IOException {
                return fileStatus != null && hasNext;
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (hasNext()) {
                    LocatedFileStatus s = new LocatedFileStatus(fileStatus,
                            fileStatus.isFile() ? locations : null);
                    hasNext = false;
                    return s;
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    public RemoteIterator<LocatedFileStatus> createLocatedFileStatusIterator(
            final String prefix, final int maxListingLength, FileSystem fs,
            PathFilter filter, FileStatusAcceptor acceptor, boolean recursive) {
        return new RemoteIterator<LocatedFileStatus>() {
            private boolean firstListing = true;
            private boolean meetEnd = false;
            private ListIterator<FileStatus> batchIterator;
            private ListParam listParam = null;

            @Override
            public boolean hasNext() throws IOException {
                if (firstListing) {
                    requestNextBatch();
                    firstListing = false;
                }
                return batchIterator.hasNext() || requestNextBatch();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (hasNext()) {
                    FileStatus status = batchIterator.next();
                    BlockLocation[] locations = fs.getFileBlockLocations(status,
                            0, status.getLen());
                    return new LocatedFileStatus(
                            status, status.isFile() ? locations : null);
                } else {
                    throw new NoSuchElementException();
                }
            }

            private boolean requestNextBatch() throws IOException {
                while (!meetEnd) {
                    if (continueListStatus()) {
                        return true;
                    }
                }

                return false;
            }

            private boolean continueListStatus() throws IOException {
                if (meetEnd) {
                    return false;
                }
                if (listParam == null) {
                    listParam = new ListParam(bucketName, prefix,
                            maxListingLength, null, null, recursive);
                }
                ListResultV2 listing = listObjects(listParam);
                List<FileStatus> stats = new ArrayList<>(
                        listing.getObjectSummaries().size() +
                                listing.getCommonPrefixes().size());
                for (ObjectSummaryParam summary : listing.getObjectSummaries()) {
                    String key = summary.getKey();
                    Path path = fs.makeQualified(new Path("/" + key));
                    if (filter.accept(path) && acceptor.accept(path, summary)) {
                        FileStatus status = new OSSFileStatus(summary.getSize(),
                                key.endsWith("/"), 1, fs.getDefaultBlockSize(path),
                                summary.getLastModified().getTime(), path, username);
                        stats.add(status);
                    }
                }

                for (String commonPrefix : listing.getCommonPrefixes()) {
                    Path path = fs.makeQualified(new Path("/" + commonPrefix));
                    if (filter.accept(path) && acceptor.accept(path, commonPrefix)) {
                        FileStatus status = new OSSFileStatus(0, true, 1, 0, 0,
                                path, username);
                        stats.add(status);
                    }
                }

                batchIterator = stats.listIterator();
                if (listing.isTruncated()) {
                    listParam.setContinuationToken(listing.getNextContinuationToken());
                } else {
                    meetEnd = true;
                }
                statistics.incrementReadOps(1);
                return batchIterator.hasNext();
            }
        };
    }

    public PartETagParam uploadPart(OSSDataBlocks.BlockUploadData partData,
                                    long size, String key, String uploadId, int idx) throws IOException {
        if (partData.hasFile()) {
            return uploadPart(partData.getFile(), key, uploadId, idx);
        } else {
            return uploadPart(partData.getUploadStream(), size, key, uploadId, idx);
        }
    }

    public PartETagParam uploadPart(File file, String key, String uploadId, int idx)
            throws IOException {
        InputStream in = new FileInputStream(file);
        try {
            return uploadPart(in, file.length(), key, uploadId, idx);
        } finally {
            in.close();
        }
    }

    public PartETagParam uploadPart(InputStream in, long size, String key,
                                    String uploadId, int idx) throws IOException {
        Exception caught = null;
        int tries = 3;
        while (tries > 0) {
            try {
                PartETagParam partETagParam = ossClient.uploadPart(bucketName, key, uploadId, idx, size, in);
                statistics.incrementWriteOps(1);
                return partETagParam;
            } catch (Exception e) {
                LOG.debug("Failed to upload " + key + ", part " + idx +
                        "try again.", e);
                caught = e;
            }
            tries--;
        }

        assert (caught != null);
        throw new IOException("Failed to upload " + key + ", part " + idx +
                " for 3 times.", caught);
    }

    /**
     * Initiate multipart upload.
     *
     * @param key object key.
     * @return upload id.
     */
    public String getUploadId(String key) throws IOException {
        String uploadId =
                ossClient.initiateMultipartUpload(bucketName, key);
        return uploadId;
    }

    /**
     * Complete the specific multipart upload.
     *
     * @param key       object key.
     * @param uploadId  upload id of this multipart upload.
     * @param partETags part etags need to be completed.
     * @return CompleteMultipartUploadResult.
     */
    public void completeMultipartUpload(String key,
                                        String uploadId, List<PartETagParam> partETagParamList) throws IOException {
        Collections.sort(partETagParamList);
        ossClient.completeMultipartUpload(bucketName, key, uploadId, partETagParamList);
    }

    /**
     * Abort the specific multipart upload.
     *
     * @param key      object key.
     * @param uploadId upload id of this multipart upload.
     */
    public void abortMultipartUpload(String key, String uploadId) throws IOException {
        ossClient.abortMultipartUpload(bucketName, key, uploadId);
    }


    @VisibleForTesting
    public OssManager getOSSManager() {
        return ossClient;
    }
}
