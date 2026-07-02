package org.apache.hadoop.fs.aliyun.oss.v2.unit.legency;

import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSCopyFileContext;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.ReadBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ReadBuffer and AliyunOSSCopyFileContext.
 */
public class TestLegencyClasses {

    // ==================== ReadBuffer ====================

    @Test
    public void testReadBufferConstructor() {
        ReadBuffer rb = new ReadBuffer(0, 99);
        assertEquals(0, rb.getByteStart());
        assertEquals(99, rb.getByteEnd());
        assertEquals(100, rb.getBuffer().length);
        assertEquals(ReadBuffer.STATUS.INIT, rb.getStatus());
    }

    @Test
    public void testReadBufferSingleByte() {
        ReadBuffer rb = new ReadBuffer(50, 50);
        assertEquals(1, rb.getBuffer().length);
        assertEquals(50, rb.getByteStart());
        assertEquals(50, rb.getByteEnd());
    }

    @Test
    public void testReadBufferSetStatus() {
        ReadBuffer rb = new ReadBuffer(0, 10);
        assertEquals(ReadBuffer.STATUS.INIT, rb.getStatus());
        rb.setStatus(ReadBuffer.STATUS.SUCCESS);
        assertEquals(ReadBuffer.STATUS.SUCCESS, rb.getStatus());
        rb.setStatus(ReadBuffer.STATUS.ERROR);
        assertEquals(ReadBuffer.STATUS.ERROR, rb.getStatus());
    }

    @Test
    public void testReadBufferLockUnlock() {
        ReadBuffer rb = new ReadBuffer(0, 10);
        assertDoesNotThrow(() -> {
            rb.lock();
            rb.unlock();
        });
    }

    @Test
    public void testReadBufferSignalAll() {
        ReadBuffer rb = new ReadBuffer(0, 10);
        rb.lock();
        assertDoesNotThrow(rb::signalAll);
        rb.unlock();
    }

    @Test
    public void testReadBufferStatusEnum() {
        ReadBuffer.STATUS[] values = ReadBuffer.STATUS.values();
        assertEquals(3, values.length);
        assertEquals(ReadBuffer.STATUS.INIT, ReadBuffer.STATUS.valueOf("INIT"));
        assertEquals(ReadBuffer.STATUS.SUCCESS, ReadBuffer.STATUS.valueOf("SUCCESS"));
        assertEquals(ReadBuffer.STATUS.ERROR, ReadBuffer.STATUS.valueOf("ERROR"));
    }

    // ==================== AliyunOSSCopyFileContext ====================

    @Test
    public void testCopyFileContextConstructor() {
        AliyunOSSCopyFileContext ctx = new AliyunOSSCopyFileContext();
        assertFalse(ctx.isCopyFailure());
    }

    @Test
    public void testCopyFileContextSetFailure() {
        AliyunOSSCopyFileContext ctx = new AliyunOSSCopyFileContext();
        assertFalse(ctx.isCopyFailure());
        ctx.setCopyFailure(true);
        assertTrue(ctx.isCopyFailure());
    }

    @Test
    public void testCopyFileContextIncCopiesFinish() {
        AliyunOSSCopyFileContext ctx = new AliyunOSSCopyFileContext();
        ctx.lock();
        ctx.incCopiesFinish();
        ctx.incCopiesFinish();
        // can't directly read copiesFinish, but lock/signalAll should work
        ctx.signalAll();
        ctx.unlock();
    }

    @Test
    public void testCopyFileContextLockUnlock() {
        AliyunOSSCopyFileContext ctx = new AliyunOSSCopyFileContext();
        assertDoesNotThrow(() -> {
            ctx.lock();
            ctx.unlock();
        });
    }

    @Test
    public void testCopyFileContextAwaitAllFinishImmediate() throws InterruptedException {
        AliyunOSSCopyFileContext ctx = new AliyunOSSCopyFileContext();
        // copiesFinish=0 and copiesToFinish=0, should return immediately
        ctx.lock();
        ctx.awaitAllFinish(0);
        ctx.unlock();
    }
}
