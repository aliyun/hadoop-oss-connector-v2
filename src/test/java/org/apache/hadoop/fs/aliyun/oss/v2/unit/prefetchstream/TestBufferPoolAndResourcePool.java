package org.apache.hadoop.fs.aliyun.oss.v2.unit.prefetchstream;

import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BoundedResourcePool;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BufferPool;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BufferData;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Retryer;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.StreamUtils;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BlockManagerParameters;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BoundedResourcePool, BufferPool, Retryer, StreamUtils,
 * BlockManagerParameters, PrefetchConstants.
 */
public class TestBufferPoolAndResourcePool {

    // ==================== BoundedResourcePool ====================

    private BoundedResourcePool<String> createStringPool(int size) {
        return new BoundedResourcePool<String>(size) {
            private int counter = 0;
            @Override
            protected String createNew() {
                return "item-" + (counter++);
            }
        };
    }

    @Test
    public void testPoolBasic() {
        BoundedResourcePool<String> pool = createStringPool(2);
        String item1 = pool.tryAcquire();
        assertNotNull(item1);
        assertEquals("item-0", item1);
        assertEquals(1, pool.numCreated());
    }

    @Test
    public void testPoolTryAcquireWhenFull() {
        BoundedResourcePool<String> pool = createStringPool(1);
        String item1 = pool.tryAcquire();
        assertNotNull(item1);
        // Pool is full, tryAcquire should return null
        assertNull(pool.tryAcquire());
    }

    @Test
    public void testPoolReleaseAndReuse() {
        BoundedResourcePool<String> pool = createStringPool(1);
        String item1 = pool.tryAcquire();
        pool.release(item1);
        String item2 = pool.tryAcquire();
        assertSame(item1, item2);
        assertEquals(1, pool.numCreated());
    }

    @Test
    public void testPoolReleaseNullThrows() {
        BoundedResourcePool<String> pool = createStringPool(1);
        assertThrows(IllegalArgumentException.class, () -> pool.release(null));
    }

    @Test
    public void testPoolReleaseForeignItemThrows() {
        BoundedResourcePool<String> pool = createStringPool(2);
        assertThrows(IllegalArgumentException.class, () -> pool.release("foreign"));
    }

    @Test
    public void testPoolDoubleRelease() {
        BoundedResourcePool<String> pool = createStringPool(1);
        String item = pool.tryAcquire();
        pool.release(item);
        // Second release should be a no-op
        pool.release(item);
        assertEquals(1, pool.numCreated());
    }

    @Test
    public void testPoolNumAvailable() {
        BoundedResourcePool<String> pool = createStringPool(3);
        assertEquals(3, pool.numAvailable());
        String item1 = pool.tryAcquire();
        assertEquals(2, pool.numAvailable());
        pool.release(item1);
        assertEquals(3, pool.numAvailable());
    }

    @Test
    public void testPoolClose() {
        BoundedResourcePool<String> pool = createStringPool(2);
        pool.tryAcquire();
        // close should not throw
        assertDoesNotThrow(pool::close);
    }

    @Test
    public void testPoolZeroSizeThrows() {
        assertThrows(IllegalArgumentException.class, () -> createStringPool(0));
    }

    @Test
    public void testPoolNegativeSizeThrows() {
        assertThrows(IllegalArgumentException.class, () -> createStringPool(-1));
    }

    @Test
    public void testPoolToString() {
        BoundedResourcePool<String> pool = createStringPool(3);
        pool.tryAcquire();
        String s = pool.toString();
        assertNotNull(s);
        assertTrue(s.contains("size = 3"));
    }

    // ==================== BufferPool ====================

    @Test
    public void testBufferPoolBasic() {
        BufferPool pool = new BufferPool(2, 1024);
        BufferData data = pool.tryAcquire(0);
        assertNotNull(data);
        assertEquals(0, data.getBlockNumber());
        assertEquals(BufferData.State.BLANK, data.getState());
    }

    @Test
    public void testBufferPoolTryAcquireSameBlock() {
        BufferPool pool = new BufferPool(2, 1024);
        BufferData d1 = pool.tryAcquire(5);
        BufferData d2 = pool.tryAcquire(5);
        assertSame(d1, d2); // same block returns same data
    }

    @Test
    public void testBufferPoolTryAcquireExhausted() {
        BufferPool pool = new BufferPool(1, 1024);
        pool.tryAcquire(0);
        // Pool is full
        assertNull(pool.tryAcquire(1));
    }

    @Test
    public void testBufferPoolReleaseAndReuse() {
        BufferPool pool = new BufferPool(1, 1024);
        BufferData d1 = pool.tryAcquire(0);
        d1.setDone();
        pool.release(d1);
        BufferData d2 = pool.tryAcquire(1);
        assertNotNull(d2);
        assertEquals(1, d2.getBlockNumber());
    }

    @Test
    public void testBufferPoolNumCreated() {
        BufferPool pool = new BufferPool(3, 1024);
        assertEquals(0, pool.numCreated());
        pool.tryAcquire(0);
        assertEquals(1, pool.numCreated());
        pool.tryAcquire(1);
        assertEquals(2, pool.numCreated());
    }

    @Test
    public void testBufferPoolGetAll() {
        BufferPool pool = new BufferPool(3, 1024);
        pool.tryAcquire(0);
        pool.tryAcquire(1);
        assertEquals(2, pool.getAll().size());
    }

    @Test
    public void testBufferPoolClose() {
        BufferPool pool = new BufferPool(2, 1024);
        pool.tryAcquire(0);
        pool.close();
    }

    @Test
    public void testBufferPoolInvalidArgs() {
        assertThrows(IllegalArgumentException.class, () -> new BufferPool(0, 1024));
        assertThrows(IllegalArgumentException.class, () -> new BufferPool(2, 0));
    }

    // ==================== Retryer ====================

    @Test
    public void testRetryerContinueRetry() {
        // perRetry=10ms, max=100ms, statusUpdate=50ms
        Retryer retryer = new Retryer(10, 100, 50);
        assertTrue(retryer.continueRetry()); // delay=0 -> 10
        assertTrue(retryer.continueRetry()); // delay=10 -> 20
    }

    @Test
    public void testRetryerExceedsMaxDelay() {
        // perRetry=10, max=20, statusUpdate=50
        // continueRetry checks delay >= maxDelay BEFORE sleeping
        Retryer retryer = new Retryer(10, 20, 50);
        assertTrue(retryer.continueRetry());  // delay=0 < 20 -> sleep -> delay=10, true
        assertTrue(retryer.continueRetry());  // delay=10 < 20 -> sleep -> delay=20, true
        assertFalse(retryer.continueRetry()); // delay=20 >= 20, false
    }

    @Test
    public void testRetryerUpdateStatus() {
        Retryer retryer = new Retryer(10, 100, 20);
        assertFalse(retryer.updateStatus()); // delay=0, false
        retryer.continueRetry(); // delay -> 10
        assertFalse(retryer.updateStatus()); // delay=10, 10%20 != 0
        retryer.continueRetry(); // delay -> 20
        assertTrue(retryer.updateStatus()); // delay=20, 20%20 == 0
    }

    @Test
    public void testRetryerInvalidArgs() {
        assertThrows(IllegalArgumentException.class, () -> new Retryer(0, 100, 50));
        assertThrows(IllegalArgumentException.class, () -> new Retryer(10, 5, 50)); // max < perRetry
        assertThrows(IllegalArgumentException.class, () -> new Retryer(10, 100, 0));
    }

    // ==================== StreamUtils ====================

    @Test
    public void testCleanupWithLoggerEmptyCloseables() {
        assertDoesNotThrow(() -> StreamUtils.cleanupWithLogger(null, new Closeable[0]));
    }

    @Test
    public void testCleanupWithLoggerNullElement() {
        // varargs with single null element: array is non-null, element is null (skipped)
        assertDoesNotThrow(() -> StreamUtils.cleanupWithLogger(null, (Closeable) null));
    }

    @Test
    public void testCleanupWithLoggerThrowingCloseable() {
        Closeable throwing = () -> { throw new IOException("test"); };
        assertDoesNotThrow(() -> StreamUtils.cleanupWithLogger(null, throwing));
    }

    @Test
    public void testCleanupWithLoggerNormalCloseable() {
        AtomicBoolean closed = new AtomicBoolean(false);
        Closeable normal = () -> closed.set(true);
        StreamUtils.cleanupWithLogger(null, normal);
        assertTrue(closed.get());
    }

    // ==================== BlockManagerParameters ====================

    @Test
    public void testBlockManagerParametersBuilder() {
        BlockManagerParameters params = new BlockManagerParameters()
                .withBufferPoolSize(4)
                .withMaxDiskBlocksCount(8)
                .withStreamUuid("test-uuid");
        assertEquals(4, params.getBufferPoolSize());
        assertEquals(8, params.getMaxDiskBlocksCount());
        assertEquals("test-uuid", params.getStreamUuid());
    }

    @Test
    public void testBlockManagerParametersDefaults() {
        BlockManagerParameters params = new BlockManagerParameters();
        assertNull(params.getFuturePool());
        assertNull(params.getBlockData());
        assertNull(params.getConf());
        assertNull(params.getLocalDirAllocator());
    }
}
