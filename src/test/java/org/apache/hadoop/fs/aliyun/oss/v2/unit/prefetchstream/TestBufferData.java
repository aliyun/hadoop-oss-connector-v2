package org.apache.hadoop.fs.aliyun.oss.v2.unit.prefetchstream;

import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BufferData;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link BufferData} – state machine, CRC, lifecycle.
 */
public class TestBufferData {

    // ==================== Constructor ====================

    @Test
    public void testConstructorBlank() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        assertEquals(0, d.getBlockNumber());
        assertEquals(BufferData.State.BLANK, d.getState());
        assertEquals(0, d.getChecksum());
        assertNotNull(d.getBuffer());
    }

    @Test
    public void testConstructorNegativeBlockNumber() {
        assertThrows(IllegalArgumentException.class,
                () -> new BufferData(-1, ByteBuffer.allocate(10)));
    }

    @Test
    public void testConstructorNullBuffer() {
        assertThrows(IllegalArgumentException.class,
                () -> new BufferData(0, null));
    }

    // ==================== State: BLANK → PREFETCHING ====================

    @Test
    public void testSetPrefetch() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        Future<Void> f = CompletableFuture.completedFuture(null);
        d.setPrefetch(f);
        assertEquals(BufferData.State.PREFETCHING, d.getState());
        assertSame(f, d.getActionFuture());
    }

    @Test
    public void testSetPrefetchNullFuture() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        assertThrows(IllegalArgumentException.class, () -> d.setPrefetch(null));
    }

    @Test
    public void testSetPrefetchWrongState() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        // already PREFETCHING
        d.setPrefetch(CompletableFuture.completedFuture(null));
        assertThrows(IllegalStateException.class,
                () -> d.setPrefetch(CompletableFuture.completedFuture(null)));
    }

    // ==================== State: PREFETCHING → READY ====================

    @Test
    public void testSetReady() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.setPrefetch(CompletableFuture.completedFuture(null));

        ByteBuffer buf = d.getBuffer();
        buf.put(new byte[]{1, 2, 3});
        buf.flip();

        d.setReady(BufferData.State.PREFETCHING);
        assertEquals(BufferData.State.READY, d.getState());
        assertNotEquals(0, d.getChecksum());
        // buffer should now be read-only
        assertTrue(d.getBuffer().isReadOnly());
    }

    @Test
    public void testSetReadyTwiceThrows() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.setPrefetch(CompletableFuture.completedFuture(null));
        d.setReady(BufferData.State.PREFETCHING);
        assertThrows(IllegalStateException.class,
                () -> d.setReady(BufferData.State.PREFETCHING));
    }

    // ==================== State: → CACHING ====================

    @Test
    public void testSetCaching() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.setPrefetch(CompletableFuture.completedFuture(null));
        d.setCaching(CompletableFuture.completedFuture(null));
        assertEquals(BufferData.State.CACHING, d.getState());
    }

    @Test
    public void testSetCachingFromReady() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.setPrefetch(CompletableFuture.completedFuture(null));
        d.setReady(BufferData.State.PREFETCHING);
        d.setCaching(CompletableFuture.completedFuture(null));
        assertEquals(BufferData.State.CACHING, d.getState());
    }

    @Test
    public void testSetCachingWrongState() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        // BLANK is not PREFETCHING or READY
        assertThrows(IllegalStateException.class,
                () -> d.setCaching(CompletableFuture.completedFuture(null)));
    }

    // ==================== State: → DONE ====================

    @Test
    public void testSetDone() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.setPrefetch(CompletableFuture.completedFuture(null));
        d.setReady(BufferData.State.PREFETCHING);
        d.setDone();
        assertEquals(BufferData.State.DONE, d.getState());
        assertNull(d.getActionFuture());
    }

    @Test
    public void testSetDoneWithoutChecksum() {
        // setDone on BLANK is allowed (no checksum set)
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.setDone();
        assertEquals(BufferData.State.DONE, d.getState());
    }

    // ==================== updateState ====================

    @Test
    public void testUpdateState() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.updateState(BufferData.State.PREFETCHING, BufferData.State.BLANK);
        assertEquals(BufferData.State.PREFETCHING, d.getState());
    }

    @Test
    public void testUpdateStateUnexpectedThrows() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        assertThrows(IllegalStateException.class,
                () -> d.updateState(BufferData.State.PREFETCHING, BufferData.State.READY));
    }

    // ==================== stateEqualsOneOf ====================

    @Test
    public void testStateEqualsOneOf() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        assertTrue(d.stateEqualsOneOf(BufferData.State.BLANK));
        assertTrue(d.stateEqualsOneOf(BufferData.State.BLANK, BufferData.State.READY));
        assertFalse(d.stateEqualsOneOf(BufferData.State.READY, BufferData.State.DONE));
    }

    // ==================== throwIfStateIncorrect ====================

    @Test
    public void testThrowIfStateIncorrect() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        assertDoesNotThrow(() -> d.throwIfStateIncorrect(BufferData.State.BLANK));
    }

    @Test
    public void testThrowIfStateIncorrectThrows() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        assertThrows(IllegalStateException.class,
                () -> d.throwIfStateIncorrect(BufferData.State.READY));
    }

    // ==================== CRC / checksum ====================

    @Test
    public void testChecksumCalculation() {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.put(new byte[]{1, 2, 3, 4});
        buf.flip();
        long cs = BufferData.getChecksum(buf);
        assertTrue(cs != 0);
    }

    @Test
    public void testChecksumSameData() {
        ByteBuffer b1 = ByteBuffer.allocate(4);
        b1.put(new byte[]{1, 2, 3, 4});
        b1.flip();

        ByteBuffer b2 = ByteBuffer.allocate(4);
        b2.put(new byte[]{1, 2, 3, 4});
        b2.flip();

        assertEquals(BufferData.getChecksum(b1), BufferData.getChecksum(b2));
    }

    @Test
    public void testChecksumDifferentData() {
        ByteBuffer b1 = ByteBuffer.allocate(4);
        b1.put(new byte[]{1, 2, 3, 4});
        b1.flip();

        ByteBuffer b2 = ByteBuffer.allocate(4);
        b2.put(new byte[]{5, 6, 7, 8});
        b2.flip();

        assertNotEquals(BufferData.getChecksum(b1), BufferData.getChecksum(b2));
    }

    // ==================== toString ====================

    @Test
    public void testToString() {
        BufferData d = new BufferData(5, ByteBuffer.allocate(10));
        String s = d.toString();
        assertNotNull(s);
        assertTrue(s.contains("BLANK"));
        assertTrue(s.contains("005"));
    }

    @Test
    public void testToStringWithFuture() {
        BufferData d = new BufferData(0, ByteBuffer.allocate(10));
        d.setPrefetch(CompletableFuture.completedFuture(null));
        String s = d.toString();
        assertTrue(s.contains("PREFETCHING"));
    }

    // ==================== State enum ====================

    @Test
    public void testStateEnumValues() {
        BufferData.State[] values = BufferData.State.values();
        assertEquals(6, values.length);
    }

    @Test
    public void testStateEnumValueOf() {
        for (BufferData.State s : BufferData.State.values()) {
            assertEquals(s, BufferData.State.valueOf(s.name()));
        }
    }
}
