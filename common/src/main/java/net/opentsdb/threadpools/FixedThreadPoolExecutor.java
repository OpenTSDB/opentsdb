// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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

public class FixedThreadPoolExecutor implements TSDBThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(UserAwareThreadPoolExecutor.class);

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

    Integer maxSize = tsdb.getConfig().getInt(getConfigKey(QUEUE_MAX_SIZE));
    LOG.info("Initializing new FixedThreadPoolExecutor with queue max capacity of {}", maxSize);

    int nThreads = Runtime.getRuntime().availableProcessors() * 2;
    this.threadPool =

        // TODO: change this to new implementation.
        new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(maxSize));

    LOG.info("Initialized new FixedThreadPoolExecutor with {} threads", nThreads);
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
  public Future<?> submit(Runnable task) {
    return this.threadPool.submit(task);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task, QueryContext qctx) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Future<?> submit(Runnable task, QueryContext qctx) {
    // TODO Auto-generated method stub
    return null;
  }

}
