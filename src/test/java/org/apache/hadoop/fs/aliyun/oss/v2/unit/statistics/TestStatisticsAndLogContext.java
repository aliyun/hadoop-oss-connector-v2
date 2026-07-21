package org.apache.hadoop.fs.aliyun.oss.v2.unit.statistics;

import org.apache.hadoop.fs.aliyun.oss.v2.OssActionEnum;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BufferData;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OperationStat;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OSSPerformanceStatistics;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.OssStatisticNames;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.impl.OutputStreamStatistics;
import org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for statistics classes and remotelog contexts.
 */
public class TestStatisticsAndLogContext {

    // ==================== OperationStat ====================

    @Test
    public void testOperationStatConstructor() {
        Instant now = Instant.now();
        OperationStat stat = new OperationStat(
                OssActionEnum.GET_OBJECT, "bucket", "key", now, false, true);
        assertEquals(OssActionEnum.GET_OBJECT, stat.getOperationName());
        assertEquals("bucket", stat.getBucketName());
        assertEquals("key", stat.getResource());
        assertEquals(now, stat.getOperationTime());
        assertTrue(stat.getSuccess());
        assertFalse(stat.isUseAcc());
    }

    @Test
    public void testOperationStatSetters() {
        OperationStat stat = new OperationStat(
                OssActionEnum.PUT_OBJECT, "b", "k", Instant.now(), true, false);
        stat.setSuccess(true);
        stat.setUseAcc(false);
        stat.setStartTime(1000L);
        stat.setBytesProcessed(2048);
        stat.setParentOperationName("parent");
        stat.setPartNumber(3);
        stat.setDurationMicros(500);

        assertTrue(stat.getSuccess());
        assertFalse(stat.isUseAcc());
        assertEquals(1000L, stat.getStartTime());
        assertEquals(2048, stat.getBytesProcessed());
        assertEquals("parent", stat.getParentOperationName());
        assertEquals(3, stat.getPartNumber());
        assertEquals(500, stat.getDurationMicros());
    }

    // ==================== OperationStat.Builder ====================

    @Test
    public void testOperationStatBuilder() {
        Instant now = Instant.now();
        OperationStat stat = new OperationStat.Builder()
                .operationName(OssActionEnum.LIST_V2)
                .bucketName("bucket")
                .resource("prefix/")
                .operationTime(now)
                .durationMicros(100)
                .success(true)
                .parentOperationName("parentOp")
                .startTime(2000L)
                .bytesProcessed(4096)
                .useAcc(true)
                .byteStart(0)
                .byteEnd(100)
                .uploadId("upload-123")
                .partNumber(1)
                .build();

        assertEquals(OssActionEnum.LIST_V2, stat.getOperationName());
        assertEquals("bucket", stat.getBucketName());
        assertEquals("prefix/", stat.getResource());
        assertEquals(100, stat.getDurationMicros());
        assertTrue(stat.getSuccess());
        assertEquals("parentOp", stat.getParentOperationName());
        assertEquals(2000L, stat.getStartTime());
        assertEquals(4096, stat.getBytesProcessed());
        assertTrue(stat.isUseAcc());
        assertEquals(0, stat.getByteStart());
        assertEquals(100, stat.getByteEnd());
    }

    // ==================== OutputStreamStatistics ====================

    @Test
    public void testOutputStreamStatisticsInitial() {
        OutputStreamStatistics stats = new OutputStreamStatistics();
        assertEquals(0, stats.getBlocksAllocated());
        assertEquals(0, stats.getBlocksReleased());
        assertEquals(0, stats.getDiskBlocksAllocated());
        assertEquals(0, stats.getDiskBlocksReleased());
        assertEquals(0, stats.getBytesAllocated());
        assertEquals(0, stats.getBytesReleased());
    }

    @Test
    public void testOutputStreamStatisticsBlockAllocation() {
        OutputStreamStatistics stats = new OutputStreamStatistics();
        stats.blockAllocated();
        stats.blockAllocated();
        assertEquals(2, stats.getBlocksAllocated());
        stats.blockReleased();
        assertEquals(1, stats.getBlocksReleased());
    }

    @Test
    public void testOutputStreamStatisticsDiskBlock() {
        OutputStreamStatistics stats = new OutputStreamStatistics();
        stats.diskBlockAllocated();
        assertEquals(1, stats.getDiskBlocksAllocated());
        stats.diskBlockReleased();
        assertEquals(1, stats.getDiskBlocksReleased());
    }

    @Test
    public void testOutputStreamStatisticsBytes() {
        OutputStreamStatistics stats = new OutputStreamStatistics();
        stats.bytesAllocated(1024);
        stats.bytesAllocated(2048);
        assertEquals(3072, stats.getBytesAllocated());
        stats.bytesReleased(512);
        assertEquals(512, stats.getBytesReleased());
    }

    // ==================== OSSPerformanceStatistics ====================

    @Test
    public void testOSSPerformanceStatisticsDefault() {
        OSSPerformanceStatistics stats = new OSSPerformanceStatistics("test", "test");
        assertEquals(0, stats.getBytesRead());
        assertEquals(0, stats.getBytesSkipped());
        assertEquals(0, stats.getBytesSeeked());
        assertEquals(0, stats.getBytesSeekedBack());
    }

    @Test
    public void testOSSPerformanceStatisticsNoOp() {
        OSSPerformanceStatistics stats = new OSSPerformanceStatistics("a", "b");
        // All methods are no-ops, just verify they don't throw
        assertDoesNotThrow(() -> {
            stats.bytesRead(100);
            stats.bytesSkipped(50);
            stats.bytesSeeked(200);
            stats.bytesSeekedBack(10);
            stats.blockAddedToFileCache();
            stats.blockRemovedFromFileCache();
            stats.blockEvictedFromFileCache();
            stats.prefetchOperationCompleted();
            stats.memoryAllocated(1024);
            stats.memoryFreed(512);
        });
    }

    // ==================== OssStatisticNames ====================

    @Test
    public void testOssStatisticNamesOperations() {
        assertEquals("op_open", OssStatisticNames.OP_OPEN);
        assertEquals("op_create", OssStatisticNames.OP_CREATE);
        assertEquals("op_delete", OssStatisticNames.OP_DELETE);
        assertEquals("op_rename", OssStatisticNames.OP_RENAME);
        assertEquals("op_list_status", OssStatisticNames.OP_LIST_STATUS);
        assertEquals("op_mkdirs", OssStatisticNames.OP_MKDIRS);
        assertEquals("op_get_file_status", OssStatisticNames.OP_GET_FILE_STATUS);
    }

    @Test
    public void testOssStatisticNamesSuffixes() {
        assertEquals(".min", OssStatisticNames.SUFFIX_MIN);
        assertEquals(".max", OssStatisticNames.SUFFIX_MAX);
        assertEquals(".mean", OssStatisticNames.SUFFIX_MEAN);
        assertEquals(".failures", OssStatisticNames.SUFFIX_FAILURES);
    }

    @Test
    public void testOssStatisticNamesMultipart() {
        assertEquals("multipart_instantiated", OssStatisticNames.MULTIPART_UPLOAD_INSTANTIATED);
        assertEquals("multipart_upload_completed", OssStatisticNames.MULTIPART_UPLOAD_COMPLETED);
        assertEquals("multipart_upload_aborted", OssStatisticNames.MULTIPART_UPLOAD_ABORTED);
    }

    // ==================== RemoteLogContext ====================
    // Note: RemoteLogContext() is package-private, use StreamLogContext instead.

    @Test
    public void testStreamLogContextBase() {
        StreamLogContext ctx = new StreamLogContext();
        assertNotNull(ctx.getUuid());
        assertNotNull(ctx.toString());
        ctx.setType("test");
        assertEquals("test", ctx.getType());
        ctx.setLastError("error");
        assertEquals("error", ctx.getLastError());
    }

    @Test
    public void testStreamLogContextToStringContainsType() {
        StreamLogContext ctx = new StreamLogContext();
        ctx.setType("myType");
        String s = ctx.toString();
        assertTrue(s.contains("myType"));
    }

    // ==================== StreamLogContext ====================

    @Test
    public void testStreamLogContext() {
        StreamLogContext ctx = new StreamLogContext();
        ctx.setType("inMem");
        ctx.setDataBlockSize(1024);
        ctx.setPrefetchThreshold(4);
        String s = ctx.toString();
        assertNotNull(s);
        assertTrue(s.contains("inMem"));
        assertTrue(s.contains("1024"));
    }

    // ==================== BlockLogContext ====================

    @Test
    public void testBlockLogContextDefault() {
        BlockLogContext ctx = new BlockLogContext();
        ctx.setToBlockNumber(5);
        ctx.setEndBlockNumber(10);
        ctx.setIsEndBlock(true);
        String s = ctx.toString();
        assertNotNull(s);
        assertTrue(s.contains("5"));
        assertTrue(s.contains("10"));
    }

    @Test
    public void testBlockLogContextWithParent() {
        StreamLogContext parent = new StreamLogContext();
        parent.setType("stream");
        BlockLogContext ctx = new BlockLogContext(parent);
        ctx.setToBlockNumber(0);
        String s = ctx.toString();
        assertTrue(s.contains("stream"));
    }

    // ==================== ReadLogContext ====================

    @Test
    public void testReadLogContext() {
        ReadLogContext ctx = new ReadLogContext(null);
        assertEquals("RD", ctx.getType());
        ctx.setOffset(100);
        ctx.setLen(512);
        String s = ctx.toString();
        assertTrue(s.contains("RD"));
        assertTrue(s.contains("100"));
        assertTrue(s.contains("512"));
    }

    // ==================== ReadAheadLogContext ====================

    @Test
    public void testReadAheadLogContext() {
        ReadAheadLogContext ctx = new ReadAheadLogContext(null);
        assertEquals("RA", ctx.getType());
        ctx.setToBlockNumber(5);
        ctx.setReadPos(1000);
        ctx.setPrefetchCount(3);
        ctx.setEndBlockNumber(10);
        ctx.setLen(4096);
        ctx.setLoop(2);
        ctx.setRelease(1);
        ctx.setCache(2);
        ctx.cancelPrefetches();
        ctx.setOutOfOrderRead();
        String s = ctx.toString();
        assertNotNull(s);
        assertTrue(s.contains("RA"));
        assertTrue(s.contains("1000"));
    }

    // ==================== MergeGetLogContext ====================

    @Test
    public void testMergeGetLogContext() {
        ReadAheadLogContext parent = new ReadAheadLogContext(null);
        MergeGetLogContext ctx = new MergeGetLogContext(parent);
        assertEquals("BM", ctx.getType());
        assertEquals(0, ctx.getCurrentTime());

        ctx.increaseRetryTime();
        assertEquals(1, ctx.getCurrentTime());

        ctx.setFirstState(BufferData.State.READY);
        ctx.setDataListSize(5);
        ctx.setRestryRes(true, 3, false, 10);

        String s = ctx.toString();
        assertNotNull(s);
        assertTrue(s.contains("BM"));
        assertTrue(s.contains("READY"));
    }
}
