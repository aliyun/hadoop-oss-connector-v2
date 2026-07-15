/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.hadoop.fs.aliyun.oss.v2.unit.prefetchstream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.InputPolicy;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.ReadOpContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ReadOpContext} and {@link ExecutorServiceFuturePool}.
 */
class TestReadOpContextAndFuturePool {

    private static final Logger LOG =
            LoggerFactory.getLogger(TestReadOpContextAndFuturePool.class);

    // ============== ReadOpContext ==============

    @Test
    void testReadOpContextConstruction() {
        Path path = new Path("oss://bucket/key");
        ReadOpContext ctx = new ReadOpContext(
                path, null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2);

        assertEquals(path, ctx.getPath());
        assertEquals(1024, ctx.getPrefetchBlockSize());
        assertEquals(4, ctx.getPrefetchBlockCount());
        assertEquals(100, ctx.getSmallFileThreshold());
        assertEquals(2, ctx.getPrefetchNumAfterSeek());
        assertEquals(200, ctx.getPrefetchThreshold());
        assertEquals(500, ctx.getBigIoThreshold());
        assertEquals(64, ctx.getOssMergeSize());
        assertEquals(256, ctx.getBigIoPrefetchSize());
        assertEquals(2, ctx.getAmplificationFactor());
    }

    @Test
    void testReadOpContextBuilderMethods() {
        Path path = new Path("oss://bucket/file");
        ReadOpContext ctx = new ReadOpContext(
                path, null, null,
                512, 2, 50, 1, 100, 300, 32, 128, 1);

        InputPolicy policy = InputPolicy.Normal;
        ReadOpContext returned = ctx.withInputPolicy(policy);
        assertSame(ctx, returned);
        assertEquals(policy, ctx.getInputPolicy());

        returned = ctx.withReadahead(8192L);
        assertSame(ctx, returned);
        assertEquals(8192L, ctx.getReadahead());

        returned = ctx.withAsyncDrainThreshold(4096L);
        assertSame(ctx, returned);
        assertEquals(4096L, ctx.getAsyncDrainThreshold());
    }

    @Test
    void testReadOpContextBuildSuccess() {
        Path path = new Path("oss://bucket/key");
        ReadOpContext ctx = new ReadOpContext(
                path, null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2)
                .withInputPolicy(InputPolicy.Normal)
                .withReadahead(1024L)
                .withAsyncDrainThreshold(0L);

        ReadOpContext built = ctx.build();
        assertSame(ctx, built);
    }

    @Test
    void testReadOpContextBuildMissingInputPolicy() {
        Path path = new Path("oss://bucket/key");
        ReadOpContext ctx = new ReadOpContext(
                path, null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2)
                .withReadahead(0L)
                .withAsyncDrainThreshold(0L);

        assertThrows(NullPointerException.class, ctx::build);
    }

    @Test
    void testReadOpContextBuildNegativeReadahead() {
        Path path = new Path("oss://bucket/key");
        ReadOpContext ctx = new ReadOpContext(
                path, null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2)
                .withInputPolicy(InputPolicy.Normal)
                .withReadahead(-1L)
                .withAsyncDrainThreshold(0L);

        assertThrows(IllegalArgumentException.class, ctx::build);
    }

    @Test
    void testReadOpContextBuildNegativeDrainThreshold() {
        Path path = new Path("oss://bucket/key");
        ReadOpContext ctx = new ReadOpContext(
                path, null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2)
                .withInputPolicy(InputPolicy.Normal)
                .withReadahead(0L)
                .withAsyncDrainThreshold(-1L);

        assertThrows(IllegalArgumentException.class, ctx::build);
    }

    @Test
    void testReadOpContextNullPath() {
        assertThrows(NullPointerException.class, () ->
                new ReadOpContext(null, null, null,
                        1024, 4, 100, 2, 200, 500, 64, 256, 2));
    }

    @Test
    void testReadOpContextInvalidPrefetchBlockSize() {
        assertThrows(IllegalArgumentException.class, () ->
                new ReadOpContext(new Path("oss://b/k"), null, null,
                        0, 4, 100, 2, 200, 500, 64, 256, 2));
    }

    @Test
    void testReadOpContextNegativePrefetchBlockSize() {
        assertThrows(IllegalArgumentException.class, () ->
                new ReadOpContext(new Path("oss://b/k"), null, null,
                        -1, 4, 100, 2, 200, 500, 64, 256, 2));
    }

    @Test
    void testReadOpContextNegativePrefetchBlockCount() {
        assertThrows(IllegalArgumentException.class, () ->
                new ReadOpContext(new Path("oss://b/k"), null, null,
                        1024, -1, 100, 2, 200, 500, 64, 256, 2));
    }

    @Test
    void testReadOpContextZeroPrefetchBlockCount() {
        ReadOpContext ctx = new ReadOpContext(
                new Path("oss://b/k"), null, null,
                1024, 0, 100, 2, 200, 500, 64, 256, 2);
        assertEquals(0, ctx.getPrefetchBlockCount());
    }

    @Test
    void testReadOpContextNullFuturePool() {
        ReadOpContext ctx = new ReadOpContext(
                new Path("oss://b/k"), null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2);
        assertNull(ctx.getFuturePool());
    }

    @Test
    void testReadOpContextToStringBeforeBuild() {
        ReadOpContext ctx = new ReadOpContext(
                new Path("oss://bucket/key"), null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2);
        String s = ctx.toString();
        assertTrue(s.contains("ReadOpContext"));
        assertTrue(s.contains("oss://bucket/key"));
    }

    @Test
    void testReadOpContextToStringAfterBuild() {
        ReadOpContext ctx = new ReadOpContext(
                new Path("oss://bucket/file"), null, null,
                1024, 4, 100, 2, 200, 500, 64, 256, 2)
                .withInputPolicy(InputPolicy.Normal)
                .withReadahead(0L)
                .withAsyncDrainThreshold(0L)
                .build();
        String s = ctx.toString();
        // InputPolicy.toString() returns the policy string, not enum name
        assertTrue(s.contains(InputPolicy.Normal.toString()));
    }

    // ============== ExecutorServiceFuturePool ==============

    @Test
    void testExecutorServiceFuturePoolExecuteFunction() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();
        ExecutorServiceFuturePool pool = new ExecutorServiceFuturePool(es);

        AtomicBoolean executed = new AtomicBoolean(false);
        Future<Void> future = pool.executeFunction(() -> {
            executed.set(true);
            return null;
        });

        future.get(5, TimeUnit.SECONDS);
        assertTrue(executed.get());

        pool.shutdown(LOG, 5, TimeUnit.SECONDS);
    }

    @Test
    void testExecutorServiceFuturePoolExecuteRunnable() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();
        ExecutorServiceFuturePool pool = new ExecutorServiceFuturePool(es);

        AtomicInteger counter = new AtomicInteger(0);
        Future<Void> future = pool.executeRunnable(counter::incrementAndGet);

        future.get(5, TimeUnit.SECONDS);
        assertEquals(1, counter.get());

        pool.shutdown(LOG, 5, TimeUnit.SECONDS);
    }

    @Test
    void testExecutorServiceFuturePoolMultipleTasks() throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(2);
        ExecutorServiceFuturePool pool = new ExecutorServiceFuturePool(es);

        AtomicInteger counter = new AtomicInteger(0);
        Future<Void> f1 = pool.executeRunnable(() -> counter.incrementAndGet());
        Future<Void> f2 = pool.executeRunnable(() -> counter.incrementAndGet());
        Future<Void> f3 = pool.executeFunction(() -> {
            counter.incrementAndGet();
            return null;
        });

        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);
        f3.get(5, TimeUnit.SECONDS);
        assertEquals(3, counter.get());

        pool.shutdown(LOG, 5, TimeUnit.SECONDS);
    }

    @Test
    void testExecutorServiceFuturePoolToString() {
        ExecutorService es = Executors.newSingleThreadExecutor();
        ExecutorServiceFuturePool pool = new ExecutorServiceFuturePool(es);

        String s = pool.toString();
        assertTrue(s.contains("ExecutorServiceFuturePool"));
        assertTrue(s.contains("executor="));

        pool.shutdown(LOG, 1, TimeUnit.SECONDS);
    }

    @Test
    void testExecutorServiceFuturePoolShutdown() {
        ExecutorService es = Executors.newSingleThreadExecutor();
        ExecutorServiceFuturePool pool = new ExecutorServiceFuturePool(es);
        // shutdown should not throw
        assertDoesNotThrow(() -> pool.shutdown(LOG, 1, TimeUnit.SECONDS));
    }
}
