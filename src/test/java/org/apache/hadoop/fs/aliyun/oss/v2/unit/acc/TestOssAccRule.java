package org.apache.hadoop.fs.aliyun.oss.v2.unit.acc;

import org.apache.hadoop.fs.aliyun.oss.v2.acc.IOSizeRange;
import org.apache.hadoop.fs.aliyun.oss.v2.acc.ObjectRange;
import org.apache.hadoop.fs.aliyun.oss.v2.acc.OssAccRule;
import org.apache.hadoop.fs.aliyun.oss.v2.acc.OssAccRuleManager;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the acc package: OssAccRule, OssAccRuleManager,
 * IOSizeRange, ObjectRange.
 */
public class TestOssAccRule {

    // ==================== ObjectRange Tests ====================

    @Test
    public void testObjectRangeContains() {
        ObjectRange range = new ObjectRange(10, 100);
        assertTrue(range.contains(10));
        assertTrue(range.contains(50));
        assertTrue(range.contains(100));
        assertFalse(range.contains(9));
        assertFalse(range.contains(101));
    }

    @Test
    public void testObjectRangeEquals() {
        ObjectRange range1 = new ObjectRange(10, 100);
        ObjectRange range2 = new ObjectRange(10, 100);
        ObjectRange range3 = new ObjectRange(20, 100);

        assertEquals(range1, range2);
        assertNotEquals(range1, range3);
        assertEquals(range1.hashCode(), range2.hashCode());
    }

    @Test
    public void testObjectRangeGetters() {
        ObjectRange range = new ObjectRange(5, 50);
        assertEquals(5, range.getMinSize());
        assertEquals(50, range.getMaxSize());
    }

    @Test
    public void testObjectRangeToString() {
        ObjectRange range = new ObjectRange(10, 100);
        String str = range.toString();
        assertTrue(str.contains("10"));
        assertTrue(str.contains("100"));
    }

    // ==================== IOSizeRange Tests ====================

    @Test
    public void testIOSizeRangeHead() {
        IOSizeRange range = new IOSizeRange(IOSizeRange.IOType.HEAD, 10);
        // Head: ioEnd < size
        assertTrue(range.contains(100, 0, 9));
        assertFalse(range.contains(100, 0, 10));
        assertFalse(range.contains(100, 5, 15));
    }

    @Test
    public void testIOSizeRangeTail() {
        IOSizeRange range = new IOSizeRange(IOSizeRange.IOType.TAIL, 10);
        // Tail: ioStart >= fileSize - size
        assertTrue(range.contains(100, 90, 99));
        assertTrue(range.contains(100, 95, 99));
        assertFalse(range.contains(100, 89, 99));
    }

    @Test
    public void testIOSizeRangeSize() {
        IOSizeRange range = new IOSizeRange(IOSizeRange.IOType.SIZE, 10, 20);
        // SIZE: ioSize = ioEnd - ioStart + 1, check [size, maxSize]
        assertTrue(range.contains(100, 0, 9));    // ioSize=10
        assertTrue(range.contains(100, 0, 19));   // ioSize=20
        assertFalse(range.contains(100, 0, 8));   // ioSize=9
        assertFalse(range.contains(100, 0, 20));  // ioSize=21
    }

    @Test
    public void testIOSizeRangeSizeWithoutMaxSize() {
        IOSizeRange range = new IOSizeRange(IOSizeRange.IOType.SIZE, 10, 0);
        // When maxSize == 0: ioSize <= size
        assertTrue(range.contains(100, 0, 9));    // ioSize=10
        assertFalse(range.contains(100, 0, 10));  // ioSize=11
    }

    @Test
    public void testIOSizeRangeEquals() {
        IOSizeRange range1 = new IOSizeRange(IOSizeRange.IOType.HEAD, 10);
        IOSizeRange range2 = new IOSizeRange(IOSizeRange.IOType.HEAD, 10);
        IOSizeRange range3 = new IOSizeRange(IOSizeRange.IOType.TAIL, 10);

        assertEquals(range1, range2);
        assertNotEquals(range1, range3);
        assertEquals(range1.hashCode(), range2.hashCode());
    }

    @Test
    public void testIOSizeRangeGetters() {
        IOSizeRange range = new IOSizeRange(IOSizeRange.IOType.SIZE, 10, 20);
        assertEquals(IOSizeRange.IOType.SIZE, range.getType());
        assertEquals(10, range.getSize());
        assertEquals(20, range.getMaxSize());
    }

    // ==================== OssAccRule Tests ====================

    @Test
    public void testRuleMatchesBucket() {
        OssAccRule rule = new OssAccRule("*", null, null, null, null);
        assertTrue(rule.matches("any-bucket", "key", 100, 0, 99, "getObject"));
    }

    @Test
    public void testRuleMatchesSpecificBucket() {
        OssAccRule rule = new OssAccRule("my-bucket", null, null, null, null);
        assertTrue(rule.matches("my-bucket", "key", 100, 0, 99, "getObject"));
        assertFalse(rule.matches("other-bucket", "key", 100, 0, 99, "getObject"));
    }

    @Test
    public void testRuleMatchesNullBucketPattern() {
        OssAccRule rule = new OssAccRule(null, null, null, null, null);
        // null defaults to "*"
        assertEquals("*", rule.getBucketPattern());
        assertTrue(rule.matches("any-bucket", "key", 100, 0, 99, "getObject"));
    }

    @Test
    public void testRuleMatchesKeyPrefix() {
        OssAccRule rule = new OssAccRule("*",
                Arrays.asList("a/", "b/"), null, null, null);
        assertTrue(rule.matches("bucket", "a/file.txt", 100, 0, 99, "getObject"));
        assertTrue(rule.matches("bucket", "b/file.txt", 100, 0, 99, "getObject"));
        assertFalse(rule.matches("bucket", "c/file.txt", 100, 0, 99, "getObject"));
    }

    @Test
    public void testRuleMatchesKeySuffix() {
        OssAccRule rule = new OssAccRule("*", null,
                Arrays.asList(".txt", ".png"), null, null);
        assertTrue(rule.matches("bucket", "file.txt", 100, 0, 99, "getObject"));
        assertTrue(rule.matches("bucket", "file.png", 100, 0, 99, "getObject"));
        assertFalse(rule.matches("bucket", "file.jpg", 100, 0, 99, "getObject"));
    }

    @Test
    public void testRuleMatchesSize() {
        ObjectRange range = new ObjectRange(10, 100);
        OssAccRule rule = new OssAccRule("*", null, null,
                Collections.singletonList(range), null);
        assertTrue(rule.matches("bucket", "key", 50, 0, 49, "getObject"));
        assertFalse(rule.matches("bucket", "key", 5, 0, 4, "getObject"));
    }

    @Test
    public void testRuleMatchesOperation() {
        OssAccRule rule = new OssAccRule("*", null, null, null,
                Arrays.asList("getObject", "putObject"));
        assertTrue(rule.matches("bucket", "key", 100, 0, 99, "getObject"));
        assertTrue(rule.matches("bucket", "key", 100, 0, 99, "putObject"));
        assertFalse(rule.matches("bucket", "key", 100, 0, 99, "headObject"));
    }

    @Test
    public void testRuleMatchesIOSize() {
        IOSizeRange ioRange = new IOSizeRange(IOSizeRange.IOType.HEAD, 10);
        OssAccRule rule = new OssAccRule("*", null, null, null, null,
                Collections.singletonList(ioRange));
        assertTrue(rule.matches("bucket", "key", 100, 0, 9, "getObject"));
        assertFalse(rule.matches("bucket", "key", 100, 0, 10, "getObject"));
    }

    @Test
    public void testRuleNoConstraintsMatchesAll() {
        OssAccRule rule = new OssAccRule("*", null, null, null, null, null);
        assertTrue(rule.matches("bucket", "any/key.txt", 9999, 0, 9998, "anyOp"));
    }

    @Test
    public void testRuleEquals() {
        OssAccRule rule1 = new OssAccRule("*", null, null, null,
                Collections.singletonList("getObject"));
        OssAccRule rule2 = new OssAccRule("*", null, null, null,
                Collections.singletonList("getObject"));
        assertEquals(rule1, rule2);
        assertEquals(rule1.hashCode(), rule2.hashCode());
    }

    @Test
    public void testRuleToString() {
        OssAccRule rule = new OssAccRule("my-bucket", null, null, null, null);
        String str = rule.toString();
        assertTrue(str.contains("my-bucket"));
    }

    @Test
    public void testRuleGetters() {
        List<String> suffixes = Arrays.asList(".txt", ".png");
        List<String> operations = Arrays.asList("getObject");
        List<ObjectRange> sizeRanges = Collections.singletonList(new ObjectRange(0, 100));
        List<IOSizeRange> ioRanges = Collections.singletonList(
                new IOSizeRange(IOSizeRange.IOType.HEAD, 10));

        OssAccRule rule = new OssAccRule("*", null, suffixes, sizeRanges, operations, ioRanges);
        assertEquals(suffixes, rule.getKeySuffixes());
        assertEquals(operations, rule.getOperations());
        assertEquals(sizeRanges, rule.getSizeRanges());
        assertEquals(ioRanges, rule.getIoSizeRanges());
    }

    // ==================== OssAccRuleManager Tests ====================

    @Test
    public void testRuleManagerEmptyContent() {
        OssAccRuleManager manager = new OssAccRuleManager("");
        assertTrue(manager.getRules().isEmpty());
    }

    @Test
    public void testRuleManagerParseKeySuffixRule() {
        String xml = "<rules>"
                + "  <rule>"
                + "    <keySuffixes>"
                + "      <keySuffix>.txt</keySuffix>"
                + "      <keySuffix>.png</keySuffix>"
                + "    </keySuffixes>"
                + "    <operations>"
                + "      <operation>getObject</operation>"
                + "    </operations>"
                + "  </rule>"
                + "</rules>";

        OssAccRuleManager manager = new OssAccRuleManager(xml);
        assertEquals(1, manager.getRules().size());
        OssAccRule rule = manager.getRules().get(0);
        assertEquals(Arrays.asList(".txt", ".png"), rule.getKeySuffixes());
        assertEquals(Collections.singletonList("getObject"), rule.getOperations());
    }

    @Test
    public void testRuleManagerParseSizeRangeRule() {
        String xml = "<rules>"
                + "  <rule>"
                + "    <sizeRanges>"
                + "      <range>"
                + "        <minSize>0</minSize>"
                + "        <maxSize>100</maxSize>"
                + "      </range>"
                + "    </sizeRanges>"
                + "    <operations>"
                + "      <operation>getObject</operation>"
                + "    </operations>"
                + "  </rule>"
                + "</rules>";

        OssAccRuleManager manager = new OssAccRuleManager(xml);
        assertEquals(1, manager.getRules().size());
        OssAccRule rule = manager.getRules().get(0);
        assertEquals(1, rule.getSizeRanges().size());
        assertEquals(0, rule.getSizeRanges().get(0).getMinSize());
        assertEquals(100, rule.getSizeRanges().get(0).getMaxSize());
    }

    @Test
    public void testRuleManagerParseIOSizeRangeRule() {
        String xml = "<rules>"
                + "  <rule>"
                + "    <IOSizeRanges>"
                + "      <ioSizeRange>"
                + "        <ioType>SIZE</ioType>"
                + "        <minIOSize>10</minIOSize>"
                + "        <maxIOSize>20</maxIOSize>"
                + "      </ioSizeRange>"
                + "    </IOSizeRanges>"
                + "    <operations>"
                + "      <operation>getObject</operation>"
                + "    </operations>"
                + "  </rule>"
                + "</rules>";

        OssAccRuleManager manager = new OssAccRuleManager(xml);
        assertEquals(1, manager.getRules().size());
        OssAccRule rule = manager.getRules().get(0);
        assertEquals(1, rule.getIoSizeRanges().size());
        assertEquals(IOSizeRange.IOType.SIZE, rule.getIoSizeRanges().get(0).getType());
    }

    @Test
    public void testRuleManagerParseHeadIOType() {
        String xml = "<rules>"
                + "  <rule>"
                + "    <IOSizeRanges>"
                + "      <ioSizeRange>"
                + "        <ioType>HEAD</ioType>"
                + "        <ioSize>10</ioSize>"
                + "      </ioSizeRange>"
                + "    </IOSizeRanges>"
                + "    <operations>"
                + "      <operation>getObject</operation>"
                + "    </operations>"
                + "  </rule>"
                + "</rules>";

        OssAccRuleManager manager = new OssAccRuleManager(xml);
        assertEquals(1, manager.getRules().size());
        OssAccRule rule = manager.getRules().get(0);
        assertEquals(IOSizeRange.IOType.HEAD, rule.getIoSizeRanges().get(0).getType());
        assertEquals(10, rule.getIoSizeRanges().get(0).getSize());
    }

    @Test
    public void testRuleManagerParseBucketPattern() {
        String xml = "<rules>"
                + "  <rule>"
                + "    <bucketPattern>my-bucket</bucketPattern>"
                + "    <operations>"
                + "      <operation>getObject</operation>"
                + "    </operations>"
                + "  </rule>"
                + "</rules>";

        OssAccRuleManager manager = new OssAccRuleManager(xml);
        assertEquals(1, manager.getRules().size());
        assertEquals("my-bucket", manager.getRules().get(0).getBucketPattern());
    }

    @Test
    public void testRuleManagerParseKeyPrefixes() {
        String xml = "<rules>"
                + "  <rule>"
                + "    <keyPrefixes>"
                + "      <keyPrefix>a/</keyPrefix>"
                + "      <keyPrefix>b/</keyPrefix>"
                + "    </keyPrefixes>"
                + "    <operations>"
                + "      <operation>getObject</operation>"
                + "    </operations>"
                + "  </rule>"
                + "</rules>";

        OssAccRuleManager manager = new OssAccRuleManager(xml);
        assertEquals(1, manager.getRules().size());
    }

    @Test
    public void testRuleManagerInvalidXml() {
        assertThrows(RuntimeException.class, () ->
                new OssAccRuleManager("<invalid>xml"));
    }

    @Test
    public void testRuleManagerEndSizeParsing() {
        String xml = "<rules>"
                + "  <rule>"
                + "    <sizeRanges>"
                + "      <range>"
                + "        <minSize>end-1024</minSize>"
                + "        <maxSize>end</maxSize>"
                + "      </range>"
                + "    </sizeRanges>"
                + "    <operations>"
                + "      <operation>getObject</operation>"
                + "    </operations>"
                + "  </rule>"
                + "</rules>";

        OssAccRuleManager manager = new OssAccRuleManager(xml);
        ObjectRange range = manager.getRules().get(0).getSizeRanges().get(0);
        assertEquals(Long.MAX_VALUE - 1024, range.getMinSize());
        assertEquals(Long.MAX_VALUE, range.getMaxSize());
    }
}
