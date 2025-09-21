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


import com.aliyun.sdk.service.oss2.OSSDualClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.*;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import com.aliyun.sdk.service.oss2.utils.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.v2.acc.OssAccRuleManager;
import org.apache.hadoop.fs.aliyun.oss.v2.model.*;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.BlockLogContext;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.Operation;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OperationStat;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ObjectAttributes;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.RemoteLogContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;
import static org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils.formatInstant;

public class OssManager {
    private static final Logger LOG = LoggerFactory.getLogger(OssManager.class);

    OSSDualClient client = null;
    OSSDualClient accClient = null;
    OssAccRuleManager accRuleManager = null;


    // 统计信息
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong totalOperationTime = new AtomicLong(0);

    @VisibleForTesting
    public List<OperationStat> getOperationStats() {
        return operationStats;
    }

    private final List<OperationStat> operationStats = new ArrayList<>();
    private boolean isTracking = false;
    private OSSManagerLogLevel logLevel = OSSManagerLogLevel.NONE;
    private String scene;
    private boolean remoteDebug = false;

    private OssManager(OSSDualClient client, OSSDualClient accClient, OssAccRuleManager accRuleManager) {
        this.client = client;
        this.accClient = accClient;
        this.accRuleManager = accRuleManager;
    }

    public static OssManager init(Configuration conf) throws IOException {
        Class<? extends OSSClientFactory> ossClientFactoryClass = conf.getClass(
                OSS_CLIENT_FACTORY_IMPL, DEFAULT_OSS_CLIENT_FACTORY_IMPL,
                OSSClientFactory.class);
        OSSClientFactory clientFactory = ReflectionUtils.newInstance(ossClientFactoryClass, conf);
        OSSDualClient ossClient = clientFactory.createOSSClient(conf);
        OSSDualClient accClient = clientFactory.createAccOSSClient(conf);

        OssAccRuleManager accRuleManager = new OssAccRuleManager(conf.get(ACC_RULES, ""));

        OssManager ossManager = new OssManager(ossClient, accClient, accRuleManager);

        boolean remoteDebug = conf.getBoolean(REMOTE_DEBUG, DEFAULT_REMOTE_DEBUG);
        ossManager.setRemoteDebug(remoteDebug);

        boolean tracking = conf.getBoolean(LOGGING_CLIENT, DEFAULT_LOGGING_CLIENT);
        if (tracking) {
            ossManager.startTracking();
        }


        OSSManagerLogLevel level = OSSManagerLogLevel.fromName(conf.getTrimmed(LOGGING_CLIENT_LEVEL, DEFAULT_LOGGING_CLIENT_LEVEL));
        ossManager.setLogLevel(level);


        return ossManager;
    }

    private void setRemoteDebug(boolean remoteDebug) {
        this.remoteDebug = remoteDebug;
    }

    private void setLogLevel(OSSManagerLogLevel level) {
        this.logLevel = level;
    }


    /**
     * 跟踪操作执行时间并记录统计信息
     *
     * @param <T>           返回值类型
     * @param operationStat 操作统计信息
     * @param operation     操作执行函数
     * @return 操作结果
     * @throws IOException IO异常
     */
    private <T> T trackOperation(OperationStat operationStat, Operation<T> operation) throws IOException {
        long startTimeNanos = 0;
        if (isTracking) {
            startTimeNanos = System.nanoTime();
        }

        try {
            //获取operation的所有入参
            T result = operation.execute();
            operationStat.setSuccess(true);

            return result;
        } catch (Exception e) {
            // 记录操作日志（包括异常）
            LOG.debug("Operation: {}, Resource: {}, Time: {}, Exception: {}",
                    operationStat.getOperationTime(), operationStat.getResource(), formatInstant(operationStat.getOperationTime()), e.getMessage());
            handleException(operationStat.getOperationName(), operationStat.getBucketName(), operationStat.getResource(), e);
        } finally {
            if (isTracking) {
                long endTimeNanos = System.nanoTime();
                long durationNanos = endTimeNanos - startTimeNanos;
                long durationMicros = durationNanos / 1000; // 转换为微秒

                if (logLevel.getValue() >= OSSManagerLogLevel.STATISTIC.getValue()) {
                    // 更新统计信息
                    totalOperations.incrementAndGet();
                    totalOperationTime.addAndGet(durationMicros / 1000); // 转换为毫秒

                    if (logLevel.getValue() >= OSSManagerLogLevel.DETAIL.getValue()) {
                        // 记录操作统计信息（包括异常）
                        operationStat.setDurationMicros(durationMicros);
                        operationStats.add(operationStat);
                    }
                }
            }

            // 记录操作日志
            LOG.debug("Operation: {}, Resource: {}, Time: {}, Duration: {}us",
                    operationStat.getOperationName(), operationStat.getResource(), formatInstant(operationStat.getOperationTime()));


        }
        LOG.error("Operation: {}, Resource: {}", operationStat.getOperationName(), operationStat.getResource());
        throw new IOException(
                "Operation: " + operationStat.getOperationName() + ", Resource: " + operationStat.getResource());
    }

    /**
     * Handle exception and convert to appropriate IOException.
     *
     * @param operationName operation name
     * @param bucketName    bucket name
     * @param objectKey     object key
     * @param e             exception
     * @throws IOException IO exception
     */
    private void handleException(OssActionEnum operationName, String bucketName, String objectKey, Exception e) throws IOException {
//        If the exception is caused by ServiceException, detailed information can be obtained in this way.
        ServiceException se = ServiceException.asCause(e);
        if (se != null) {
            //404
            if (se.statusCode() == 404) {
                if (StringUtils.equals(se.errorCode(), OSSErrorCode.NO_SUCH_KEY)) {
                    LOG.debug("operationName{} Object does not exist: {}", operationName, bucketName + "/" + objectKey);
                    throw new ObjectNotFoundException("Object does not exist: " + bucketName + "/" + objectKey, e);
                }
            }

            if (StringUtils.equals(se.errorCode(), OSSErrorCode.FILE_ALREADY_EXISTS)) {
                LOG.debug("operationName{} already exists: {}", operationName, bucketName + "/" + objectKey);
                throw new ObjectAlreadyExistsException("Object does not exist: " + bucketName + "/" + objectKey, e);

            }

            LOG.error(e.getMessage());
            LOG.error("handleException Exception occurred", e);
            throw new IOException("handleException" + operationName + ":" + bucketName + "/" + objectKey, e);

        }

        throw new IOException(
                "Operation: " + operationName + ", Resource: " + objectKey, e);
    }

    public void shutdown() {
        LOG.debug("Closing OSSManager!");

        try {
            client.close();
            if (accClient != null)
                accClient.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logTracking();
    }


    public void clearTracking() {
        operationStats.clear();
        totalOperations.set(0);
        totalOperationTime.set(0);
    }

    public void startTracking(String scene) {
        isTracking = true;
        this.scene = scene;
    }

    public void startTracking() {
        isTracking = true;
        this.scene = "";
    }

    public void stopTracking() {
        isTracking = false;
    }

    public void logTracking() {
        if (!isTracking) {
            return;
        }

        printTracking();
    }

    public void printTracking() {
        operationStats.sort(Comparator.comparing(OperationStat::getOperationTime));

        if (logLevel.isAtLeast(OSSManagerLogLevel.STATISTIC)) {

            // 打印详细的操作统计信息
            System.out.printf("=== Operation Statistics : scene *%s* ===", scene);
            System.out.printf("Total operations: %d, Total time: %dms, Average time: %.2fms%n",
                    totalOperations.get(), totalOperationTime.get(),
                    totalOperations.get() > 0 ? (double) totalOperationTime.get() / totalOperations.get() : 0);


            if (logLevel.isAtLeast(OSSManagerLogLevel.DETAIL)) {
                System.out.println("Operation Details (ordered by time):");
                System.out.printf("%-5s %-20s  %-5s  %-5s  %-100s %-30s %-10s%n", "No.", "Operation", "Suc", "Acc", "Resource", "Time", "Duration(us)");
                System.out.printf("%-5s %-20s  %-5s  %-5s  %-100s %-30s %-10s%n", "---", "---------", "-----", "-----", "--------", "----", "------------");

                int index = 1;
                for (OperationStat stat : operationStats) {
                    System.out.printf("%-5d %-20s %-5b  %-5b %-100s %-30s %-10d%n",
                            index++,
                            stat.getOperationName(),
                            stat.getSuccess(),
                            stat.isUseAcc(),
                            stat.getResource(),
                            formatInstant(stat.getOperationTime()),
                            stat.getDurationMicros());
                }
            }

            System.out.println("=== End of Operation Statistics ===");
        }
        clearTracking();
    }

    public long getTotalOperations() {
        return totalOperations.get();
    }

    public String initiateMultipartUpload(String bucketName, String key) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.INITIATE_MULTIPART_UPLOAD)
                .bucketName(bucketName)
                .resource(key)
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        return trackOperation(operationStat, () -> {
            InitiateMultipartUploadResult initiateResult = client.initiateMultipartUpload(
                    InitiateMultipartUploadRequest.newBuilder()
                            .bucket(bucketName)
                            .key(key)
                            .build());

            return initiateResult.initiateMultipartUpload().uploadId();
        });
    }


    public void completeMultipartUpload(String bucketName, String key, String uploadId, List<PartETagParam> partETagParamList) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.COMPLETE_MULTIPART_UPLOAD)
                .bucketName(bucketName)
                .resource(key)
                .operationTime(Instant.now())
                .useAcc(false)
                .uploadId(uploadId)
                .build();

        trackOperation(operationStat, () -> {
            client.completeMultipartUpload(CompleteMultipartUploadRequest.newBuilder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .completeAll("yes")
                    .build());
            return null;
        });
    }

    public PartETagParam uploadPart(String bucketName, String key, String uploadId, int idx, long size, InputStream in) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.MULTIPART_UPLOAD)
                .bucketName(bucketName)
                .resource(key)
                .operationTime(Instant.now())
                .useAcc(false)
                .uploadId(uploadId)
                .partNumber(idx)
                .build();

        return trackOperation(operationStat, () -> {

            UploadPartResult partResult = client.uploadPart(
                    UploadPartRequest.newBuilder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber((long) idx)
                            .body(BinaryData.fromStream(in))
                            .build());

            return new PartETagParam(idx, partResult.eTag());
        });

    }

    public void copyObject(String srcBucketName, String srcKey, String dstBucketName, String dstKey) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.COPY_OBJECT)
                .bucketName(srcBucketName)
                .resource(srcKey + " -> " + dstKey)
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        trackOperation(operationStat, () -> {
            return client.copyObject(
                    CopyObjectRequest.newBuilder()
                            .sourceBucket(srcBucketName)
                            .sourceKey(srcKey)
                            .bucket(dstBucketName)
                            .key(dstKey)
                            .build());
        });
    }

    public void deleteObject(String bucketName, String key) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.DELETE)
                .bucketName(bucketName)
                .resource(key)
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        trackOperation(operationStat, () -> {
            return client.deleteObject(
                    DeleteObjectRequest.newBuilder()
                            .bucket(bucketName)
                            .key(key)
                            .build());
        });
    }

    public List<String> deleteObjects(String bucketName, List<String> keysToDelete, boolean quiet) throws IOException {
        String resource = new StringBuilder()
                .append(keysToDelete.size())
                .append(keysToDelete.stream().findFirst().orElse(""))
                .toString();

        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.DELETE_OBJECTS)
                .bucketName(bucketName)
                .resource(resource)
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        return trackOperation(operationStat, () -> {
            //keysToDelete 转换为 List<DeleteObject>
            List<DeleteObject> deleteObjectList = keysToDelete.stream()
                    .map(key -> DeleteObject.newBuilder().key(key).build())
                    .collect(Collectors.toList());


            DeleteMultipleObjectsResult deleteMultipleResult = client.deleteMultipleObjects(DeleteMultipleObjectsRequest.newBuilder()
                    .bucket(bucketName)
                    .quiet(quiet)
                    .deleteObjects(deleteObjectList)
                    .build());

            List<String> deletedObjects = deleteMultipleResult.deletedObjects().stream()
                    .map(DeletedInfo::key)
                    .collect(Collectors.toList());

            LOG.debug("Deleted objects: {}", deletedObjects);
            return deletedObjects;
        });
    }

    public ListResultV2 listObjectsV2(ListParam listParam) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.LIST_V2)
                .bucketName(listParam.getBucket())
                .resource(listParam.toString())
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        return trackOperation(operationStat, () -> {

            ListObjectsV2Request.Builder builder = ListObjectsV2Request.newBuilder()
                    .bucket(listParam.getBucket())
                    .maxKeys(listParam.getMaxListingLength());

            if (listParam.getPrefix() != null && !listParam.getPrefix().isEmpty())
                builder.prefix(listParam.getPrefix());

            if (listParam.getDelimiter() != null && !listParam.getDelimiter().isEmpty())
                builder.delimiter(listParam.getDelimiter());

            if (listParam.getContinuationToken() != null && !listParam.getContinuationToken().isEmpty()) {
                builder.continuationToken(listParam.getContinuationToken());
            }
            ListObjectsV2Result listObjectsV2Result = client.listObjectsV2(builder.build());

            ListResultV2 result = getListResultV2(listParam, listObjectsV2Result);
            for (ObjectSummary objectSummary : listObjectsV2Result.contents()) {
                ObjectSummaryParam objectSummaryParam = new ObjectSummaryParam();
                objectSummaryParam.setBucketName(result.getBucketName());
                objectSummaryParam.setKey(objectSummary.key());
                objectSummaryParam.setETag(objectSummary.eTag());
                objectSummaryParam.setSize(objectSummary.size());
                objectSummaryParam.setLastModified(Date.from(objectSummary.lastModified()));
                result.addObjectSummary(objectSummaryParam);
            }


            for (CommonPrefix commonPrefix : listObjectsV2Result.commonPrefixes()) {
                result.addCommonPrefix(commonPrefix.prefix());
            }

            return result;

        });
    }

    private static ListResultV2 getListResultV2(ListParam listParam, ListObjectsV2Result listObjectsV2Result) {
        ListResultV2 result = new ListResultV2();
        result.setBucketName(listParam.getBucket());
        result.setPrefix(listObjectsV2Result.prefix());
        result.setMaxKeys(listObjectsV2Result.maxKeys());
        result.setTruncated(listObjectsV2Result.isTruncated());
        result.setDelimiter(listObjectsV2Result.delimiter());
        result.setContinuationToken(listObjectsV2Result.continuationToken());
        result.setNextContinuationToken(listObjectsV2Result.nextContinuationToken());
        result.setStartAfter(listObjectsV2Result.startAfter());
        result.setKeyCount(listObjectsV2Result.keyCount());
        return result;
    }

    public void abortMultipartUpload(String bucketName, String key, String uploadId) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.ABORT_MULTIPART_UPLOAD)
                .bucketName(bucketName)
                .resource(key)
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        trackOperation(operationStat, () -> {
            client.abortMultipartUpload(
                    AbortMultipartUploadRequest.newBuilder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .build()
            );
            return null;
        });
    }

    List<CompletableFuture<GetObjectResult> > getBatchObject(List<GetObjectReq> requestList) throws IOException {

        List<CompletableFuture<GetObjectResult> > results = new ArrayList<>();
        for (GetObjectReq req: requestList) {
            results.add(getObjectWithResultAsync(req.getBucketName(), req.getKey(), req.getObjectAttributes(), req.getByteStart(), req.getByteEnd()));
        }
        return results;
    }

    public InputStream getObject(String bucketName, String key, ObjectAttributes objectAttributes, long byteStart, long byteEnd, BlockLogContext blockLogContext) throws IOException {
        return getObjectWithResult(bucketName, key, objectAttributes, byteStart, byteEnd, blockLogContext).body();
    }


    public CompletableFuture<GetObjectResult>  getObjectWithResultAsync(String bucketName, String key, ObjectAttributes objectAttributes, long byteStart, long byteEnd) throws IOException {

        String resource = new StringBuilder().append(key).append("(").append(byteStart).append("-").append(byteEnd).append(")").toString();
        boolean useAcc = useAcc(bucketName, key, objectAttributes, byteStart, byteEnd, OssActionEnum.GET_OBJECT);
        OSSDualClient realClient = useAcc ? accClient : client;
        //将start和end转换成Range"bytes=0-9"样式
        String range = "bytes=" + byteStart + "-" + byteEnd;

        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.GET_OBJECT_ASYNC)
                .bucketName(bucketName)
                .resource(resource)
                .operationTime(Instant.now())
                .useAcc(useAcc)
                .byteStart(byteStart)
                .byteEnd(byteEnd)
                .build();


        return trackOperation(operationStat, () -> {

            CompletableFuture<GetObjectResult> resultFuture = realClient.getObjectAsync(
                    GetObjectRequest.newBuilder()
                            .bucket(bucketName)
                            .key(key)
                            .range(range)
                            .build());
            return resultFuture;
        });
    }

    public GetObjectResult getObjectWithResult(String bucketName, String key, ObjectAttributes objectAttributes, long byteStart, long byteEnd, RemoteLogContext blockLogContext) throws IOException {

        String resource = new StringBuilder().append(key).append("(").append(byteStart).append("-").append(byteEnd).append(")").toString();
        boolean useAcc = useAcc(bucketName, key, objectAttributes, byteStart, byteEnd, OssActionEnum.GET_OBJECT);
        OSSDualClient realClient = useAcc ? accClient : client;
        //将start和end转换成Range"bytes=0-9"样式
        String range = "bytes=" + byteStart + "-" + byteEnd;

        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.GET_OBJECT)
                .bucketName(bucketName)
                .resource(resource)
                .operationTime(Instant.now())
                .useAcc(useAcc)
                .byteStart(byteStart)
                .byteEnd(byteEnd)
                .build();


        return trackOperation(operationStat, () -> {
            GetObjectRequest.Builder r = GetObjectRequest.newBuilder()
                    .bucket(bucketName)
                    .key(key)
                    .range(range);
            if(remoteDebug) {
                String x = blockLogContext.toString();
                Map<String, String> params = new HashMap<>();
                params.put("u", blockLogContext.toString());
                r.parameters(params);
            }

            GetObjectResult result = realClient.getObject(
                    r.build(), OperationOptions.defaults());
            return result;
        });
    }

    private boolean useAcc(String bucketName, String key, ObjectAttributes objectAttributes, long byteStart, long byteEnd, OssActionEnum action) {
        // 如果没有配置ACC客户端，则直接使用普通客户端
        if (accClient == null) {
            return false;
        }

        // 如果没有配置规则管理器或者规则为空，则直接使用普通客户端
        if (accRuleManager == null || accRuleManager.getRules().isEmpty()) {
            return false;
        }

        if (objectAttributes == null) {
            return false;
        }

        long objectSize = objectAttributes.getLen();
        boolean useAcc = accRuleManager.getRules().stream()
                .anyMatch(rule ->
                        rule.matches(bucketName, key, objectSize, byteStart, byteEnd,
                                action.getOperationName()));

        return useAcc;
    }


    public String putObject(String bucketName, String key, InputStream in, long size, String
            serverSideEncryptionAlgorithm, boolean putIfNotExists) throws IOException {
        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.PUT_OBJECT)
                .bucketName(bucketName)
                .resource(key)
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        return trackOperation(operationStat, () -> {
            PutObjectRequest.Builder builder = PutObjectRequest.newBuilder()
                    .bucket(bucketName)
                    .key(key)
                    .forbidOverwrite(putIfNotExists)
                    .contentLength((int) size)
                    .body(BinaryData.fromStream(in));

            if (serverSideEncryptionAlgorithm != null && !serverSideEncryptionAlgorithm.isEmpty()) {
                builder.serverSideEncryption(serverSideEncryptionAlgorithm);
            }

            PutObjectResult result = client.putObject(builder.build());
            return result.eTag();
        });
    }

    public ObjectMetadataParam getObjectMetadata(String bucketName, String key) throws IOException {

        OperationStat operationStat = new OperationStat.Builder()
                .operationName(OssActionEnum.GET_OBJECT_METADATA)
                .bucketName(bucketName)
                .resource(key)
                .operationTime(Instant.now())
                .useAcc(false)
                .build();

        return trackOperation(operationStat, () -> {
            GetObjectMetaResult request = client.getObjectMeta(
                    GetObjectMetaRequest.newBuilder()
                            .bucket(bucketName)
                            .key(key)
                            .build());
            ObjectMetadataParam metadata = new ObjectMetadataParam();
            metadata.setMetadata(request.headers());
            metadata.setContentLength(request.contentLength());
            String lastModified = request.lastModified();
            if (lastModified != null)
                metadata.setLastModified(DateUtil.parseRfc822Date(lastModified));

            ;
            return metadata;
        });
    }

//    public void close() {
//        LOG.debug("Closing OSSManager!");
////        throw new RuntimeException(" Closing OSSManager!");
//        try {
//            client.close();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
}