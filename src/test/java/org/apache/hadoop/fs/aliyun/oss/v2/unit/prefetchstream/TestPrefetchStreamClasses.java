package org.apache.hadoop.fs.aliyun.oss.v2.unit.prefetchstream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BufferData;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.FilePosition;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.InputPolicy;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ObjectAttributes;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.Validate;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for prefetchstream package: InputPolicy, Validate,
 * FilePosition, ObjectAttributes.
 */
public class TestPrefetchStreamClasses {

    // ==================== InputPolicy Tests ====================

    @Test
    public void testInputPolicyNormal() {
        assertEquals(InputPolicy.Normal,
                InputPolicy.getPolicy("normal", null));
    }

    @Test
    public void testInputPolicyRandom() {
        assertEquals(InputPolicy.Random,
                InputPolicy.getPolicy("random", null));
    }

    @Test
    public void testInputPolicySequential() {
        assertEquals(InputPolicy.Sequential,
                InputPolicy.getPolicy("sequential", null));
    }

    @Test
    public void testInputPolicyParquetIsRandom() {
        assertEquals(InputPolicy.Random,
                InputPolicy.getPolicy("parquet", null));
    }

    @Test
    public void testInputPolicyOrcIsRandom() {
        assertEquals(InputPolicy.Random,
                InputPolicy.getPolicy("orc", null));
    }

    @Test
    public void testInputPolicyColumnarIsRandom() {
        assertEquals(InputPolicy.Random,
                InputPolicy.getPolicy("columnar", null));
    }

    @Test
    public void testInputPolicyUnknownReturnsDefault() {
        assertEquals(InputPolicy.Normal,
                InputPolicy.getPolicy("unknown", InputPolicy.Normal));
    }

    @Test
    public void testInputPolicyUnknownReturnsNull() {
        assertNull(InputPolicy.getPolicy("unknown", null));
    }

    @Test
    public void testInputPolicyCaseInsensitive() {
        assertEquals(InputPolicy.Normal,
                InputPolicy.getPolicy("NORMAL", null));
    }

    @Test
    public void testInputPolicyGetFirstSupported() {
        assertEquals(InputPolicy.Random,
                InputPolicy.getFirstSupportedPolicy(
                        Arrays.asList("unknown1", "random", "sequential"),
                        InputPolicy.Normal));
    }

    @Test
    public void testInputPolicyGetFirstSupportedAllUnknown() {
        assertEquals(InputPolicy.Normal,
                InputPolicy.getFirstSupportedPolicy(
                        Arrays.asList("unknown1", "unknown2"),
                        InputPolicy.Normal));
    }

    @Test
    public void testInputPolicyProperties() {
        assertTrue(InputPolicy.Normal.isAdaptive());
        assertFalse(InputPolicy.Random.isAdaptive());
        assertFalse(InputPolicy.Sequential.isAdaptive());
    }

    @Test
    public void testInputPolicyToString() {
        assertNotNull(InputPolicy.Normal.toString());
        assertNotNull(InputPolicy.Random.toString());
        assertNotNull(InputPolicy.Sequential.toString());
    }

    // ==================== Validate Tests ====================

    @Test
    public void testCheckNotNull() {
        assertDoesNotThrow(() -> Validate.checkNotNull("value", "arg"));
    }

    @Test
    public void testCheckNotNullThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNull(null, "arg"));
    }

    @Test
    public void testCheckPositiveInteger() {
        assertDoesNotThrow(() -> Validate.checkPositiveInteger(1, "arg"));
    }

    @Test
    public void testCheckPositiveIntegerZeroThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkPositiveInteger(0, "arg"));
    }

    @Test
    public void testCheckPositiveIntegerNegativeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkPositiveInteger(-1, "arg"));
    }

    @Test
    public void testCheckNotNegative() {
        assertDoesNotThrow(() -> Validate.checkNotNegative(0, "arg"));
        assertDoesNotThrow(() -> Validate.checkNotNegative(1, "arg"));
    }

    @Test
    public void testCheckNotNegativeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNegative(-1, "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyString() {
        assertDoesNotThrow(() -> Validate.checkNotNullAndNotEmpty("value", "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyStringNull() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNullAndNotEmpty((String) null, "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyStringEmpty() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNullAndNotEmpty("", "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyByteArray() {
        assertDoesNotThrow(() -> Validate.checkNotNullAndNotEmpty(new byte[]{1}, "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyByteArrayEmpty() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNullAndNotEmpty(new byte[0], "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyIntArray() {
        assertDoesNotThrow(() -> Validate.checkNotNullAndNotEmpty(new int[]{1}, "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyIntArrayEmpty() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNullAndNotEmpty(new int[0], "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyLongArray() {
        assertDoesNotThrow(() -> Validate.checkNotNullAndNotEmpty(new long[]{1L}, "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyShortArray() {
        assertDoesNotThrow(() -> Validate.checkNotNullAndNotEmpty(new short[]{1}, "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyObjectArray() {
        assertDoesNotThrow(() -> Validate.checkNotNullAndNotEmpty(new String[]{"a"}, "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyObjectArrayEmpty() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNullAndNotEmpty(new String[0], "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyIterable() {
        assertDoesNotThrow(() ->
                Validate.checkNotNullAndNotEmpty(Collections.singletonList("a"), "arg"));
    }

    @Test
    public void testCheckNotNullAndNotEmptyIterableEmpty() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNullAndNotEmpty(Collections.emptyList(), "arg"));
    }

    @Test
    public void testCheckValuesEqual() {
        assertDoesNotThrow(() -> Validate.checkValuesEqual(10, "a", 10, "b"));
    }

    @Test
    public void testCheckValuesEqualThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkValuesEqual(10, "a", 20, "b"));
    }

    @Test
    public void testCheckIntegerMultiple() {
        assertDoesNotThrow(() -> Validate.checkIntegerMultiple(20, "a", 10, "b"));
    }

    @Test
    public void testCheckIntegerMultipleThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkIntegerMultiple(25, "a", 10, "b"));
    }

    @Test
    public void testCheckGreater() {
        assertDoesNotThrow(() -> Validate.checkGreater(20, "a", 10, "b"));
    }

    @Test
    public void testCheckGreaterThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkGreater(5, "a", 10, "b"));
    }

    @Test
    public void testCheckGreaterOrEqual() {
        assertDoesNotThrow(() -> Validate.checkGreaterOrEqual(10, "a", 10, "b"));
        assertDoesNotThrow(() -> Validate.checkGreaterOrEqual(20, "a", 10, "b"));
    }

    @Test
    public void testCheckGreaterOrEqualThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkGreaterOrEqual(5, "a", 10, "b"));
    }

    @Test
    public void testCheckLessOrEqual() {
        assertDoesNotThrow(() -> Validate.checkLessOrEqual(10, "a", 10, "b"));
        assertDoesNotThrow(() -> Validate.checkLessOrEqual(5, "a", 10, "b"));
    }

    @Test
    public void testCheckLessOrEqualThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkLessOrEqual(20, "a", 10, "b"));
    }

    @Test
    public void testCheckWithinRange() {
        assertDoesNotThrow(() -> Validate.checkWithinRange(5, "v", 1, 10));
        assertDoesNotThrow(() -> Validate.checkWithinRange(1, "v", 1, 10));
        assertDoesNotThrow(() -> Validate.checkWithinRange(10, "v", 1, 10));
    }

    @Test
    public void testCheckWithinRangeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkWithinRange(0, "v", 1, 10));
    }

    @Test
    public void testCheckWithinRangeDouble() {
        assertDoesNotThrow(() -> Validate.checkWithinRange(5.0, "v", 1.0, 10.0));
    }

    @Test
    public void testCheckWithinRangeDoubleThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkWithinRange(0.5, "v", 1.0, 10.0));
    }

    @Test
    public void testCheckRequired() {
        assertDoesNotThrow(() -> Validate.checkRequired(true, "arg"));
    }

    @Test
    public void testCheckRequiredThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkRequired(false, "arg"));
    }

    @Test
    public void testCheckValid() {
        assertDoesNotThrow(() -> Validate.checkValid(true, "arg"));
    }

    @Test
    public void testCheckValidThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkValid(false, "arg"));
    }

    @Test
    public void testCheckValidWithValidValues() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkValid(false, "arg", "val1,val2"));
    }

    @Test
    public void testCheckNotNullAndNumberOfElements() {
        assertDoesNotThrow(() ->
                Validate.checkNotNullAndNumberOfElements(
                        Arrays.asList("a", "b"), 2, "arg"));
    }

    @Test
    public void testCheckNotNullAndNumberOfElementsThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Validate.checkNotNullAndNumberOfElements(
                        Arrays.asList("a"), 2, "arg"));
    }

    @Test
    public void testCheckState() {
        assertDoesNotThrow(() -> Validate.checkState(true, "ok %s", "msg"));
    }

    @Test
    public void testCheckStateThrows() {
        assertThrows(IllegalStateException.class,
                () -> Validate.checkState(false, "failed: %s", "detail"));
    }

    // ==================== FilePosition Tests ====================

    @Test
    public void testFilePositionConstructor() {
        FilePosition fp = new FilePosition(1000, 128);
        assertFalse(fp.isValid());
    }

    @Test
    public void testFilePositionZeroFileSize() {
        FilePosition fp = new FilePosition(0, 128);
        assertFalse(fp.isValid());
    }

    @Test
    public void testFilePositionSetData() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(100);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 0, 0);
        assertTrue(fp.isValid());
    }

    @Test
    public void testFilePositionInvalidate() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(100);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 0, 0);
        assertTrue(fp.isValid());
        fp.invalidate();
        assertFalse(fp.isValid());
    }

    @Test
    public void testFilePositionAbsolute() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(100);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 10, 10);
        assertEquals(10, fp.absolute());
    }

    @Test
    public void testFilePositionBufferStartOffset() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(100);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 50, 50);
        assertEquals(50, fp.bufferStartOffset());
    }

    @Test
    public void testFilePositionSetAbsolute() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(100);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 0, 0);
        assertTrue(fp.setAbsolute(50));
        assertEquals(50, fp.absolute());
    }

    @Test
    public void testFilePositionSetAbsoluteOutOfRange() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(100);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 0, 0);
        assertFalse(fp.setAbsolute(200));
    }

    @Test
    public void testFilePositionIncrementBytesRead() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(100);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 0, 0);
        fp.incrementBytesRead(1);
        assertEquals(1, fp.numBytesRead());
        assertEquals(1, fp.numSingleByteReads());
        assertEquals(0, fp.numBufferReads());
        fp.incrementBytesRead(10);
        assertEquals(11, fp.numBytesRead());
        assertEquals(1, fp.numSingleByteReads());
        assertEquals(1, fp.numBufferReads());
    }

    @Test
    public void testFilePositionInvalidBufferThrows() {
        FilePosition fp = new FilePosition(1000, 128);
        assertThrows(IllegalStateException.class, fp::buffer);
        assertThrows(IllegalStateException.class, fp::data);
        assertThrows(IllegalStateException.class, fp::absolute);
    }

    @Test
    public void testFilePositionBlockNumber() {
        FilePosition fp = new FilePosition(1000, 128);
        ByteBuffer buf = ByteBuffer.allocate(128);
        BufferData data = new BufferData(0, buf);
        fp.setData(data, 0, 0);
        assertEquals(0, fp.blockNumber());
    }

    // ==================== ObjectAttributes Tests ====================

    @Test
    public void testObjectAttributes() {
        Path path = new Path("oss://bucket/key");
        ObjectAttributes attrs = new ObjectAttributes(
                "bucket", path, "key", "etag123", "v1", 1024);
        assertEquals("bucket", attrs.getBucket());
        assertEquals(path, attrs.getPath());
        assertEquals("key", attrs.getKey());
        assertEquals("etag123", attrs.getETag());
        assertEquals("v1", attrs.getVersionId());
        assertEquals(1024, attrs.getLen());
    }
}
