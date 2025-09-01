/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;

import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ExecutorServiceFuturePool {

  private final ExecutorService executor;

  public ExecutorServiceFuturePool(ExecutorService executor) {
    this.executor = executor;
  }

  public Future<Void> executeFunction(final Supplier<Void> f) {
    return executor.submit(f::get);
  }

  @SuppressWarnings("unchecked")
  public Future<Void> executeRunnable(final Runnable r) {
    return (Future<Void>) executor.submit(r::run);
  }

  public void shutdown(Logger logger, long timeout, TimeUnit unit) {
    HadoopExecutors.shutdown(executor, logger, timeout, unit);
  }

  public String toString() {
    return String.format(Locale.ROOT, "ExecutorServiceFuturePool(executor=%s)", executor);
  }
}
