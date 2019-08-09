// This file is part of OpenTSDB.
// Copyright (C) 2019 The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.threadpools;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;

/**
 * Thin Wrapper layer around {@link ThreadPoolExecutor}.
 * 
 * @since 3.0
 *
 */
public class FixedThreadPoolExecutor implements TSDBThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(FixedThreadPoolExecutor.class);

  public static final String TYPE = "FixedThreadPoolExecutor";

  public String id;

  private ExecutorService threadPool;

  public FixedThreadPoolExecutor() {}

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {

    this.id = id;
    if (this.threadPool != null) {
      LOG.error("FixedThreadPoolExecutor is already initialized!!");
      return Deferred
          .fromError(new IllegalStateException("FixedThreadPoolExecutor is already initialized"));
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(QUEUE_MAX_SIZE))) {
      tsdb.getConfig().register(getConfigKey(QUEUE_MAX_SIZE), 100000, false,
          "The ID of an object pool factory plugin to use for this pool. "
              + "Can be null to use the default.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(CORE_THREAD_POOL_SIZE))) {
      tsdb.getConfig().register(getConfigKey(CORE_THREAD_POOL_SIZE),
          Runtime.getRuntime().availableProcessors() * 2, false,
          "Core thread pool size. " + "Can be null to use the default.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(MAX_THREAD_POOL_SIZE))) {
      tsdb.getConfig().register(getConfigKey(MAX_THREAD_POOL_SIZE),
          Runtime.getRuntime().availableProcessors() * 2, false,
          "Max thread pool size. " + "Can be null to use the default.");
    }

    int maxSize = tsdb.getConfig().getInt(getConfigKey(QUEUE_MAX_SIZE));
    int corePoolSize = tsdb.getConfig().getInt(getConfigKey(CORE_THREAD_POOL_SIZE));
    int maxTPoolSize = tsdb.getConfig().getInt(getConfigKey(MAX_THREAD_POOL_SIZE));
    LOG.info("Initializing new FixedThreadPoolExecutor with queue max capacity of {}", maxSize);

    this.threadPool = new ThreadPoolExecutor(corePoolSize, maxTPoolSize, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(maxSize));

    LOG.info(
        "Initializing new FixedThreadPoolExecutor with max queue size of {} with core threads {} and a "
            + "maximum pool size of {}.",
        maxSize, corePoolSize, maxTPoolSize);

    return Deferred.fromResult(null);
  }

  String getConfigKey(final String key) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
  }

  @Override
  public Deferred<Object> shutdown() {
    if (!threadPool.isShutdown()) {
      threadPool.shutdown();
      LOG.info("Shutting down FixedThreadPoolExecutor, no more threads will be executed!");
    }
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public String tpType() {

    return TYPE;
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return this.threadPool.submit(task);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task, QueryContext qctx) {
    return this.threadPool.submit(task);
  }
  
  @Override
  public Future<?> submit(Runnable task) {
    return this.threadPool.submit(task);
  }

  @Override
  public Future<?> submit(Runnable task, QueryContext qctx) {
    return this.threadPool.submit(task);
  }

  @Override
  public Future<?> submit(Runnable task, QueryContext qctx, TSDTask tsdTask) {
    return this.threadPool.submit(task);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task, QueryContext qctx, TSDTask tsdTask) {
    return this.threadPool.submit(task);
  }

}
