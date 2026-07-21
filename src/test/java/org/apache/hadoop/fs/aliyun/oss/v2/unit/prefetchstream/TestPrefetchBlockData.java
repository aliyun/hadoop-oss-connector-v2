package org.apache.hadoop.fs.aliyun.oss.v2.unit.prefetchstream;

import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.PrefetchBlockData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PrefetchBlockData}.
 */
public class TestPrefetchBlockData {

    // ==================== Constructor ====================

    @Test
    public void testBasicConstruction() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertEquals(1000, bd.getFileSize());
        assertEquals(100, bd.getBlockSize());
        assertEquals(10, bd.getNumBlocks());
    }

    @Test
    public void testFileSizeNotMultipleOfBlockSize() {
        // 1050 / 100 = 10 full blocks + 1 partial = 11 blocks
        PrefetchBlockData bd = new PrefetchBlockData(1050, 100);
        assertEquals(11, bd.getNumBlocks());
    }

    @Test
    public void testZeroFileSize() {
        PrefetchBlockData bd = new PrefetchBlockData(0, 100);
        assertEquals(0, bd.getNumBlocks());
        assertEquals(100, bd.getBlockSize());
    }

    @Test
    public void testZeroFileSizeZeroBlockSize() {
        PrefetchBlockData bd = new PrefetchBlockData(0, 0);
        assertEquals(0, bd.getNumBlocks());
    }

    @Test
    public void testNegativeFileSize() {
        assertThrows(IllegalArgumentException.class,
                () -> new PrefetchBlockData(-1, 100));
    }

    @Test
    public void testZeroBlockSizeNonZeroFile() {
        assertThrows(IllegalArgumentException.class,
                () -> new PrefetchBlockData(100, 0));
    }

    @Test
    public void testNegativeBlockSize() {
        assertThrows(IllegalArgumentException.class,
                () -> new PrefetchBlockData(100, -1));
    }

    // ==================== isLastBlock ====================

    @Test
    public void testIsLastBlock() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertFalse(bd.isLastBlock(0));
        assertFalse(bd.isLastBlock(8));
        assertTrue(bd.isLastBlock(9));
    }

    @Test
    public void testIsLastBlockZeroFile() {
        PrefetchBlockData bd = new PrefetchBlockData(0, 100);
        assertFalse(bd.isLastBlock(0));
    }

    @Test
    public void testIsLastBlockSingleBlock() {
        PrefetchBlockData bd = new PrefetchBlockData(50, 100);
        assertTrue(bd.isLastBlock(0));
    }

    @Test
    public void testIsLastBlockInvalidBlockNumber() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertThrows(IllegalArgumentException.class, () -> bd.isLastBlock(10));
        assertThrows(IllegalArgumentException.class, () -> bd.isLastBlock(-1));
    }

    // ==================== getBlockNumber (from offset) ====================

    @Test
    public void testGetBlockNumberFromOffset() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertEquals(0, bd.getBlockNumber(0));
        assertEquals(0, bd.getBlockNumber(99));
        assertEquals(1, bd.getBlockNumber(100));
        assertEquals(5, bd.getBlockNumber(500));
        assertEquals(9, bd.getBlockNumber(999));
    }

    @Test
    public void testGetBlockNumberInvalidOffset() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertThrows(IllegalArgumentException.class, () -> bd.getBlockNumber(-1));
        assertThrows(IllegalArgumentException.class, () -> bd.getBlockNumber(1000));
    }

    // ==================== getSize ====================

    @Test
    public void testGetSize() {
        PrefetchBlockData bd = new PrefetchBlockData(1050, 100);
        assertEquals(100, bd.getSize(0));
        assertEquals(100, bd.getSize(9));
        // Last block: 1050 - 10*100 = 50
        assertEquals(50, bd.getSize(10));
    }

    @Test
    public void testGetSizeZeroFile() {
        PrefetchBlockData bd = new PrefetchBlockData(0, 100);
        assertEquals(0, bd.getSize(0));
    }

    @Test
    public void testGetSizeExactMultiple() {
        PrefetchBlockData bd = new PrefetchBlockData(500, 100);
        assertEquals(100, bd.getSize(4)); // last block, exact fit
    }

    // ==================== isValidOffset ====================

    @Test
    public void testIsValidOffset() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertTrue(bd.isValidOffset(0));
        assertTrue(bd.isValidOffset(500));
        assertTrue(bd.isValidOffset(999));
        assertFalse(bd.isValidOffset(-1));
        assertFalse(bd.isValidOffset(1000));
        assertFalse(bd.isValidOffset(2000));
    }

    // ==================== getStartOffset ====================

    @Test
    public void testGetStartOffset() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertEquals(0, bd.getStartOffset(0));
        assertEquals(100, bd.getStartOffset(1));
        assertEquals(900, bd.getStartOffset(9));
    }

    @Test
    public void testGetStartOffsetInvalidBlock() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertThrows(IllegalArgumentException.class, () -> bd.getStartOffset(10));
        assertThrows(IllegalArgumentException.class, () -> bd.getStartOffset(-1));
    }

    // ==================== getRelativeOffset ====================

    @Test
    public void testGetRelativeOffset() {
        PrefetchBlockData bd = new PrefetchBlockData(1000, 100);
        assertEquals(50, bd.getRelativeOffset(1, 150));
        assertEquals(0, bd.getRelativeOffset(0, 0));
        assertEquals(99, bd.getRelativeOffset(9, 999));
    }

    // ==================== State management ====================
    // Note: PrefetchBlockData.State is package-private, so we test indirectly
    // via getStateString() and by passing null to setState() for boundary checks.

    @Test
    public void testInitialStateViaString() {
        PrefetchBlockData bd = new PrefetchBlockData(500, 100);
        String stateStr = bd.getStateString();
        // All blocks start in NOT_READY state (verified via string representation)
        assertNotNull(stateStr);
        assertTrue(stateStr.contains("NOT_READY"));
    }

    @Test
    public void testSetStateInvalidBlock() {
        PrefetchBlockData bd = new PrefetchBlockData(500, 100);
        // blockNumber validation happens before state parameter is used
        assertThrows(IllegalArgumentException.class,
                () -> bd.setState(-1, null));
        assertThrows(IllegalArgumentException.class,
                () -> bd.setState(5, null));
    }

    @Test
    public void testGetStateInvalidBlock() {
        PrefetchBlockData bd = new PrefetchBlockData(300, 100);
        assertThrows(IllegalArgumentException.class,
                () -> bd.getState(-1));
        assertThrows(IllegalArgumentException.class,
                () -> bd.getState(3));
    }

    // ==================== getStateString ====================

    @Test
    public void testGetStateString() {
        PrefetchBlockData bd = new PrefetchBlockData(300, 100);
        String s = bd.getStateString();
        assertNotNull(s);
        assertTrue(s.contains("NOT_READY"));
    }

    // ==================== Large file / edge cases ====================

    @Test
    public void testLargeFile() {
        // 10GB file, 128KB blocks
        long fileSize = 10L * 1024 * 1024 * 1024;
        int blockSize = 128 * 1024;
        // numBlocks = ceil(10*1024^3 / 128*1024) = 81920
        PrefetchBlockData bd = new PrefetchBlockData(fileSize, blockSize);
        assertEquals(81920, bd.getNumBlocks());
        assertTrue(bd.isLastBlock(81919));
        assertFalse(bd.isLastBlock(0));
    }

    @Test
    public void testSingleByteFile() {
        PrefetchBlockData bd = new PrefetchBlockData(1, 1024);
        assertEquals(1, bd.getNumBlocks());
        assertEquals(1, bd.getSize(0));
        assertTrue(bd.isLastBlock(0));
    }
}
