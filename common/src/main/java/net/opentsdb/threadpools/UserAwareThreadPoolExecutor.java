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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;

public class UserAwareThreadPoolExecutor implements TSDBThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(UserAwareThreadPoolExecutor.class);

  public static final String TYPE = "UserAwareThreadPoolExecutor";

  /** Percentage of active threads to consider before applying the reschedule */
  protected static final String THREAD_THRESHOLD_PCT = "thread.threshold.pct";

  /**
   * Percentage of active threads per user to consider before applying the reschedule when the total
   * percentage of active thread in the executor falls below this threshold value
   */
  protected static final String MAX_THREAD_PER_USER_PCT = "max.thread.per.user.pct";

  /** Stores the User thread count that's being current executed */
  private static final ConcurrentHashMap<String, AtomicInteger> CURRENT_EXECUTIONS =
      new ConcurrentHashMap<>();

  /** Queue to store the tasks */
  private LinkedBlockingQueue<Runnable> workQueue;

  /** Threadpool to execute the query tasks */
  private ExecutorService threadPool;

  public String id;

  /** The number of threads to keep in the pool, even if they are idle */
  private int corePoolSize;

  /** MaximumPoolSize the maximum number of threads to allow in the pool */
  private int maxTPoolSize;

  /** Absolute value calculated based the percentage defined by THREAD_THRESHOLD_PCT */
  private int threadThresholdCnt;

  /** Absolute value calculated based the percentage defined by MAX_THREAD_PER_USER_PCT */
  private int threadThresholdPerUserCnt;

  private static final int PURGE_CNT = 10000;

  /** Attempts to purge the CURRENT_EXECUTIONS state after every 10000 executions */
  private static final AtomicInteger COUNT_TO_PURGE = new AtomicInteger(PURGE_CNT);

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
      LOG.error("UserAwareThreadPoolExecutor is already initialized!!");
      return Deferred.fromError(
          new IllegalStateException("UserAwareThreadPoolExecutor is already initialized"));
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(QUEUE_MAX_SIZE))) {
      tsdb.getConfig().register(getConfigKey(QUEUE_MAX_SIZE), 100000, false,
          "The max size allowed for the blocking queue. " + "Can be null to use the default.");
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

    if (!tsdb.getConfig().hasProperty(getConfigKey(THREAD_THRESHOLD_PCT))) {
      tsdb.getConfig().register(getConfigKey(THREAD_THRESHOLD_PCT), 85, false,
          "Percentage of active threads to consider before applying the reschedule. "
              + "Can be null to use the default.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(MAX_THREAD_PER_USER_PCT))) {
      tsdb.getConfig().register(getConfigKey(MAX_THREAD_PER_USER_PCT), 50, false,
          "Percentage of active threads per user to consider before applying the reschedule when the total "
              + "percentage of active thread in the executor falls below the "
              + THREAD_THRESHOLD_PCT + " threshold. " + "Can be null to use the default.");
    }

    int threadThresholdPct = tsdb.getConfig().getInt(getConfigKey(THREAD_THRESHOLD_PCT));
    if (threadThresholdPct > 100) {
      return Deferred.fromError(
          new IllegalStateException(THREAD_THRESHOLD_PCT + " value should be less than 100"));
    }
    int threadThresholdPerUserPct = tsdb.getConfig().getInt(getConfigKey(THREAD_THRESHOLD_PCT));
    if (threadThresholdPerUserPct > 100) {
      return Deferred.fromError(
          new IllegalStateException(MAX_THREAD_PER_USER_PCT + " value should be less than 100"));
    }

    Integer maxSize = tsdb.getConfig().getInt(getConfigKey(QUEUE_MAX_SIZE));
    corePoolSize = tsdb.getConfig().getInt(getConfigKey(CORE_THREAD_POOL_SIZE));
    maxTPoolSize = tsdb.getConfig().getInt(getConfigKey(MAX_THREAD_POOL_SIZE));
    threadThresholdCnt = maxTPoolSize * (threadThresholdPct / 100);
    threadThresholdPerUserCnt = maxTPoolSize * (threadThresholdPerUserPct / 100);

    LOG.info(
        "Initializing new UserAwareThreadPoolExecutor with max queue size of {} with core threads {} and a "
            + "maximum pool size of {} with threshold Thread count of {}.",
        maxSize, corePoolSize, maxTPoolSize, threadThresholdPct);

    workQueue = new LinkedBlockingQueue<Runnable>(maxSize);

    // TODO: better alternative?
    // If the worker queue is full, we run the task in the thread that calls execute(task)
    RejectedExecutionHandler inPlaceRunTaskHandler = new RejectedExecutionHandler() {

      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (!executor.isShutdown()) {
          r.run();
        }

      }
    };
    this.threadPool = new ThreadPoolExecutor(corePoolSize, maxTPoolSize, 1L, TimeUnit.SECONDS,
        workQueue, inPlaceRunTaskHandler);

    LOG.info("Initialized new UserAwareThreadPoolExecutor with {} threads", corePoolSize);
    return Deferred.fromResult(null);
  }

  class QCRunnableWrapper implements Runnable {
    final Runnable r;
    final QueryContext qctx;

    public QCRunnableWrapper(Runnable r, QueryContext qctx) {
      this.r = r;
      this.qctx = qctx;
    }

    @Override
    public void run() {

      if (qctx == null) {
        r.run();
        return;
      }

      final String user = this.qctx.authState().getUser();

      AtomicInteger ai = CURRENT_EXECUTIONS.get(user);
      if (ai == null) {
        ai = CURRENT_EXECUTIONS.computeIfAbsent(user, v -> new AtomicInteger(0));
      }

      // Check if the task needs to be requeued based on the thresholds.
      if (((ThreadPoolExecutor) threadPool).getActiveCount() > threadThresholdCnt) {
        if (ai.get() > threadThresholdPerUserCnt) {

          // Requeue the task.
          // Use execute() instead of submit() to use the same Future object;
          ((ThreadPoolExecutor) threadPool).execute(r);
          return;
        }

      }

      // Increment the counter per user..
      ai.incrementAndGet();

      try {
        // Safe to schedule the task
        r.run();
      } finally {
        // Decrement the counter per user..
        ai.decrementAndGet();

        // Remove the entry from the state to make sure we don't hold the state for all the users in
        // the lifetime of the process
        if (COUNT_TO_PURGE.decrementAndGet() <= 0) {
          CURRENT_EXECUTIONS.entrySet().removeIf(m -> m.getValue().get() == 0);
          COUNT_TO_PURGE.set(PURGE_CNT);
        }
      }

    }
  }

  class QCFutureWrapper<V> extends FutureTask<V> {

    final QueryContext qctx;

    public QCFutureWrapper(Callable<V> task, QueryContext qctx) {
      super(task);
      this.qctx = qctx;
    }

    @Override
    public void run() {

      if (qctx == null) {
        super.run();
        return;
      }

      final String user = this.qctx.authState().getUser();

      AtomicInteger ai = CURRENT_EXECUTIONS.get(user);
      if (ai == null) {
        ai = CURRENT_EXECUTIONS.computeIfAbsent(user, v -> new AtomicInteger(0));
      }

      // Check if the task needs to be requeued based on the thresholds.
      if (((ThreadPoolExecutor) threadPool).getActiveCount() > threadThresholdCnt) {
        if (ai.get() > threadThresholdPerUserCnt) {

          // Requeue the task.
          ((ThreadPoolExecutor) threadPool).execute(this);
          return;
        }

      }

      // Increment the counter per user..
      ai.incrementAndGet();

      try {
        // Safe to schedule the task
        super.run();
      } finally {
        // Decrement the counter per user..
        ai.decrementAndGet();

        // Remove the entry from the state to make sure we don't hold the state for all the users in
        // the lifetime of the process
        if (COUNT_TO_PURGE.decrementAndGet() <= 0) {
          CURRENT_EXECUTIONS.entrySet().removeIf(m -> m.getValue().get() == 0);
          COUNT_TO_PURGE.set(PURGE_CNT);
        }
      }
    }
  }

  @Override
  public Deferred<Object> shutdown() {
    if (!threadPool.isShutdown()) {
      threadPool.shutdown();
      LOG.info("Shutting down UserAwareThreadPoolExecutor, no more threads will be executed!");
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

  String getConfigKey(final String key) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
  }

  private <T> FutureTask<T> newTaskFor(Callable<T> task, QueryContext qctx) {
    return new QCFutureWrapper<T>(task, qctx);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return this.threadPool.submit(task);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return this.threadPool.submit(task);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task, QueryContext qctx) {

    if (task == null) {
      throw new NullPointerException("Callable task is null! Query context is " + qctx.query());
    }
    RunnableFuture<T> ftask = newTaskFor(task, qctx);
    ((ThreadPoolExecutor) threadPool).execute(ftask);
    return ftask;

  }

  @Override
  public Future<?> submit(Runnable task, QueryContext qctx) {
    return this.threadPool.submit(new QCRunnableWrapper(task, qctx));
  }

  @VisibleForTesting
  static ConcurrentHashMap<String, AtomicInteger> getCurrentExecutions() {
    return CURRENT_EXECUTIONS;
  }

  @VisibleForTesting
  static AtomicInteger getCountToPurge() {
    return COUNT_TO_PURGE;
  }

}
