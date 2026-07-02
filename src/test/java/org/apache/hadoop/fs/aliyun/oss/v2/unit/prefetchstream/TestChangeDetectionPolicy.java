package org.apache.hadoop.fs.aliyun.oss.v2.unit.prefetchstream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ChangeDetectionPolicy;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.RemoteFileChangedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ChangeDetectionPolicy}.
 */
public class TestChangeDetectionPolicy {

    // ==================== Mode enum ====================

    @Test
    public void testModeValues() {
        ChangeDetectionPolicy.Mode[] values = ChangeDetectionPolicy.Mode.values();
        assertEquals(4, values.length);
    }

    @Test
    public void testModeValueOf() {
        for (ChangeDetectionPolicy.Mode m : ChangeDetectionPolicy.Mode.values()) {
            assertEquals(m, ChangeDetectionPolicy.Mode.valueOf(m.name()));
        }
    }

    // ==================== Source enum ====================

    @Test
    public void testSourceValues() {
        ChangeDetectionPolicy.Source[] values = ChangeDetectionPolicy.Source.values();
        assertEquals(2, values.length);
    }

    @Test
    public void testSourceValueOf() {
        for (ChangeDetectionPolicy.Source s : ChangeDetectionPolicy.Source.values()) {
            assertEquals(s, ChangeDetectionPolicy.Source.valueOf(s.name()));
        }
    }

    // ==================== createPolicy ====================

    @Test
    public void testCreatePolicyETag() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.Client,
                ChangeDetectionPolicy.Source.ETag,
                false);
        assertNotNull(policy);
        assertEquals(ChangeDetectionPolicy.Mode.Client, policy.getMode());
        assertEquals(ChangeDetectionPolicy.Source.ETag, policy.getSource());
        assertFalse(policy.isRequireVersion());
    }

    @Test
    public void testCreatePolicyNone() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.None,
                ChangeDetectionPolicy.Source.None,
                false);
        assertNotNull(policy);
        assertEquals(ChangeDetectionPolicy.Source.None, policy.getSource());
    }

    @Test
    public void testCreatePolicyWithRequireVersion() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.Server,
                ChangeDetectionPolicy.Source.ETag,
                true);
        assertTrue(policy.isRequireVersion());
    }

    // ==================== getPolicy ====================

    @Test
    public void testGetPolicy() {
        Configuration conf = new Configuration();
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.getPolicy(conf);
        assertNotNull(policy);
        assertEquals(ChangeDetectionPolicy.Source.ETag, policy.getSource());
    }

    // ==================== onChangeDetected ====================

    @Test
    public void testOnChangeDetectedNoneMode() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.None,
                ChangeDetectionPolicy.Source.ETag,
                false);
        ImmutablePair<Boolean, RemoteFileChangedException> result =
                policy.onChangeDetected("v1", "v2", "uri", 0, "read", 0);
        assertFalse(result.getLeft());
        assertNull(result.getRight());
    }

    @Test
    public void testOnChangeDetectedWarnMode() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.Warn,
                ChangeDetectionPolicy.Source.ETag,
                false);
        ImmutablePair<Boolean, RemoteFileChangedException> result =
                policy.onChangeDetected("v1", "v2", "uri", 0, "read", 0);
        assertTrue(result.getLeft());
        assertNull(result.getRight());
    }

    @Test
    public void testOnChangeDetectedWarnModeAlreadyDetected() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.Warn,
                ChangeDetectionPolicy.Source.ETag,
                false);
        // Already detected once
        ImmutablePair<Boolean, RemoteFileChangedException> result =
                policy.onChangeDetected("v1", "v2", "uri", 0, "read", 1);
        assertFalse(result.getLeft());
        assertNull(result.getRight());
    }

    @Test
    public void testOnChangeDetectedClientMode() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.Client,
                ChangeDetectionPolicy.Source.ETag,
                false);
        ImmutablePair<Boolean, RemoteFileChangedException> result =
                policy.onChangeDetected("v1", "v2", "uri", 100, "read", 0);
        assertTrue(result.getLeft());
        assertNotNull(result.getRight());
        assertTrue(result.getRight().getMessage().contains("change detected"));
    }

    @Test
    public void testOnChangeDetectedServerMode() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.Server,
                ChangeDetectionPolicy.Source.ETag,
                false);
        ImmutablePair<Boolean, RemoteFileChangedException> result =
                policy.onChangeDetected("v1", "v2", "uri", -1, "read", 0);
        assertTrue(result.getLeft());
        assertNotNull(result.getRight());
    }

    // ==================== toString ====================

    @Test
    public void testToStringETag() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.Client,
                ChangeDetectionPolicy.Source.ETag,
                false);
        String s = policy.toString();
        assertNotNull(s);
        assertTrue(s.contains("ETag"));
    }

    @Test
    public void testToStringNoChange() {
        ChangeDetectionPolicy policy = ChangeDetectionPolicy.createPolicy(
                ChangeDetectionPolicy.Mode.None,
                ChangeDetectionPolicy.Source.None,
                false);
        assertEquals("NoChangeDetection", policy.toString());
    }

    // ==================== RemoteFileChangedException ====================

    @Test
    public void testRemoteFileChangedException() {
        RemoteFileChangedException e = new RemoteFileChangedException(
                "oss://bucket/key", "read", "test message");
        // PathIOException prepends path to message: "read `oss://bucket/key': test message"
        assertTrue(e.getMessage().contains("test message"));
        assertNotNull(e.getPath());
    }

    @Test
    public void testRemoteFileChangedExceptionWithCause() {
        Exception cause = new RuntimeException("cause");
        RemoteFileChangedException e = new RemoteFileChangedException(
                "oss://bucket/key", "open", "msg", cause);
        assertSame(cause, e.getCause());
    }

    @Test
    public void testRemoteFileChangedExceptionConstants() {
        assertNotNull(RemoteFileChangedException.PRECONDITIONS_FAILED);
        assertNotNull(RemoteFileChangedException.FILE_NOT_FOUND_SINGLE_ATTEMPT);
    }
}
