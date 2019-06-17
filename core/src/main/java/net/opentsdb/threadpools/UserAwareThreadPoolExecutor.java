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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;

/**
 * A ThreadPoolExecutor that keeps track of tasks by {@link net.opentsdb.auth.AuthState}. This
 * executor throttles the tasks of a User (of type {@link net.opentsdb.auth.AuthState}) when a
 * percentage of currently executed tasks for the User is above a certain limit (defined by
 * {@link MAX_THREAD_PER_USER_PCT}) and fewer threads in the threadpool are available for executing
 * tasks.
 * 
 * This would make sure all the Users get a fair share of the ThreadPool executor to run their tasks
 * by throttling the tasks of the heavy User.
 *
 * @since 3.0
 */
public class UserAwareThreadPoolExecutor implements TSDBThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(UserAwareThreadPoolExecutor.class);

  public static final String TYPE = "UserAwareThreadPoolExecutor";

  /** Percentage of active threads to consider before applying the reschedule */
  private static final String THREAD_THRESHOLD_PCT = "thread.threshold.pct";

  /**
   * Percentage of active threads per user to consider before applying the reschedule when the total
   * percentage of active thread in the executor falls below this threshold value
   */
  private static final String MAX_THREAD_PER_USER_PCT = "max.thread.per.user.pct";

  /**
   * Will disable the user level scheduling and works just like {@link FixedThreadPoolExecutor} and
   * logs when it should have rescheduled
   */
  private static final String DISABLE_SCHEDULE = "disable";

  /**
   * Override values for a given user in the config
   */
  private static final String MAX_THREAD_PER_USER_OVERRIDE_PCT = "max.thread.per.user.override.pct";

  /** Stores the User thread count that's being current executed */
  private final ConcurrentHashMap<String, AtomicInteger> CURRENT_EXECUTIONS =
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

  /** Absolute value calculated based on the percentage defined by THREAD_THRESHOLD_PCT */
  private int threadThresholdCnt;

  /** Absolute value calculated based on the percentage defined by MAX_THREAD_PER_USER_PCT */
  private int threadThresholdPerUserCnt;

  /** Disable schedule flag */
  private volatile boolean disableScheduling = false;

  /**
   * Stores the limit of {@link threadThresholdPerUserCnt} per user defined by
   * MAX_THREAD_PER_USER_OVERRIDE_PCT
   */
  private volatile Map<String, Integer> perUserLimitOverride = new HashMap<>();

  private final int PURGE_CNT = 10000;

  /** Attempts to purge the CURRENT_EXECUTIONS state after every 10000 executions */
  private final AtomicInteger COUNT_TO_PURGE = new AtomicInteger(PURGE_CNT);

  private static final TypeReference<List<Map<String, String>>> TYPE_REF =
      new TypeReference<List<Map<String, String>>>() {};

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String id() {
    return id;
  }

  /**
   * A callback for the per user limit that updates perUserLimitOverride and enable/disable the User
   * level scheduling
   */
  class SettingsCallback implements ConfigurationCallback<Object> {

    @SuppressWarnings("unchecked")
    @Override
    public void update(final String key, final Object value) {

      if (key.equals(getConfigKey(MAX_THREAD_PER_USER_OVERRIDE_PCT))) {
        if (value != null) {
          synchronized (UserAwareThreadPoolExecutor.this) {

            Map<String, Integer> update = new HashMap<>();
            // Squash them together
            ((List<Map<String, Integer>>) value).stream().forEach(map -> {
              update.putAll(map.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            });
            perUserLimitOverride = update;
          }
        }
      } else if (key.equals(getConfigKey(DISABLE_SCHEDULE))) {
        if (value != null) {
          setDisableScheduling(Boolean.valueOf(value.toString()));
        }
      }
    }

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
          "The max size allowed for the blocking queue. Can be null to use the default.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(CORE_THREAD_POOL_SIZE))) {
      tsdb.getConfig().register(getConfigKey(CORE_THREAD_POOL_SIZE),
          Runtime.getRuntime().availableProcessors() * 2, false,
          "Core thread pool size. Can be null to use the default.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(MAX_THREAD_POOL_SIZE))) {
      tsdb.getConfig().register(getConfigKey(MAX_THREAD_POOL_SIZE),
          Runtime.getRuntime().availableProcessors() * 2, false,
          "Max thread pool size. Can be null to use the default.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(THREAD_THRESHOLD_PCT))) {
      tsdb.getConfig().register(getConfigKey(THREAD_THRESHOLD_PCT), 85, false,
          "Percentage of active threads to consider before applying the reschedule. "
              + "Can be null to use the default.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(DISABLE_SCHEDULE))) {
      tsdb.getConfig().register(getConfigKey(DISABLE_SCHEDULE), false, false,
          "Per User schedule is enabled by default. Can be null to use the default.");
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

    disableScheduling = tsdb.getConfig().getBoolean(getConfigKey(DISABLE_SCHEDULE));

    threadThresholdCnt = (int) ((double) maxTPoolSize * ((double) threadThresholdPct) / 100);

    threadThresholdPerUserCnt =
        (int) ((double) maxTPoolSize * ((double) threadThresholdPerUserPct) / 100);

    if (!tsdb.getConfig().hasProperty(getConfigKey(MAX_THREAD_PER_USER_OVERRIDE_PCT))) {
      tsdb.getConfig()
          .register(ConfigurationEntrySchema
              .newBuilder()
              .setKey(getConfigKey(MAX_THREAD_PER_USER_OVERRIDE_PCT))
              .setType(TYPE_REF)
              .setDescription("The list of hosts including protocol, host and port.")
              .isDynamic()
              .isNullable()
              .setSource(getClass().getName())
              .build());
    }

    tsdb.getConfig().bind(getConfigKey(MAX_THREAD_PER_USER_OVERRIDE_PCT), new SettingsCallback());

    tsdb.getConfig().bind(getConfigKey(DISABLE_SCHEDULE), new SettingsCallback());

    LOG.info(
        "Initializing new UserAwareThreadPoolExecutor with max queue size of {} with core threads {} and a "
            + "maximum pool size of {} with threshold Thread count of {}.",
        maxSize, corePoolSize, maxTPoolSize, threadThresholdCnt);

    LOG.info("Overriding User level thresholds {}", perUserLimitOverride);

    workQueue = new LinkedBlockingQueue<Runnable>(maxSize);

    // TODO: better alternative?
    // If the worker queue is full, we run the task in the thread that calls execute(task)
    RejectedExecutionHandler inPlaceRunTaskHandler = new RejectedExecutionHandler() {

      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (!executor.isShutdown()) {
          r.run();
          LOG.warn("Executor queue is full {}, running the task anyway. Need tuning!",
              workQueue.size());
        } else {
          LOG.warn("Executor was shutdown and a task was assigned, dropping the task");
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

      if (qctx == null || qctx.authState() == null) {
        r.run();
        return;
      }

      final String user = getUser(qctx);

      int thresholdPerUser = threadThresholdPerUserCnt;
      Integer limit = perUserLimitOverride.get(user);
      if (limit != null) {
        thresholdPerUser = limit;
      }

      AtomicInteger ai = CURRENT_EXECUTIONS.get(user);
      if (ai == null) {
        ai = CURRENT_EXECUTIONS.computeIfAbsent(user, v -> new AtomicInteger(0));
      }

      // Check if the task needs to be requeued based on the thresholds.
      int activeCount = ((ThreadPoolExecutor) threadPool).getActiveCount();
      if (activeCount > threadThresholdCnt) {
        if (ai.get() > thresholdPerUser) {

          if (disableScheduling) {
            LOG.info("User ({}) task would have been rescheduled since the currently executed user "
                + "tasks are {} out of available {} threads", user, ai.get(), activeCount);
          } else {
            // Requeue the task.
            // Use execute() instead of submit() to use the same Future object;
            ((ThreadPoolExecutor) threadPool).execute(r);
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "User ({}) task is rescheduled since the currently executed user tasks are {} out of available {} threads",
                  user, ai.get(), activeCount);
            }
            return;
          }
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

        purgeStaleState();
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

      if (qctx == null || qctx.authState() == null) {
        super.run();
        return;
      }

      final String user = getUser(qctx);
      int thresholdPerUser = threadThresholdPerUserCnt;
      Integer limit = perUserLimitOverride.get(user);
      if (limit != null) {
        thresholdPerUser = limit;
      }

      AtomicInteger ai = CURRENT_EXECUTIONS.get(user);
      if (ai == null) {
        ai = CURRENT_EXECUTIONS.computeIfAbsent(user, v -> new AtomicInteger(0));
      }

      // Check if the task needs to be requeued based on the thresholds.
      int activeCount = ((ThreadPoolExecutor) threadPool).getActiveCount();
      if (activeCount > threadThresholdCnt) {
        if (ai.get() > thresholdPerUser) {

          if (disableScheduling) {
            LOG.info("User ({}) task would have been rescheduled since the currently executed user "
                + "tasks are {} out of available {} threads", user, ai.get(), activeCount);
          } else {
            // Requeue the task.
            ((ThreadPoolExecutor) threadPool).execute(this);
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "User ({}) task is rescheduled since the currently executed user tasks are {} out of available {} threads",
                  user, ai.get(), activeCount);
            }
            return;
          }
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

        purgeStaleState();
      }
    }

  }

  private void purgeStaleState() {
    // Remove the entry from the state to make sure we don't hold the state for all the users in
    // the lifetime of the process
    if (COUNT_TO_PURGE.decrementAndGet() <= 0) {
      CURRENT_EXECUTIONS.entrySet().removeIf(m -> m.getValue().get() == 0);
      COUNT_TO_PURGE.set(PURGE_CNT);
    }
  }

  private String getUser(QueryContext qctx) {
    final String user = qctx.authState().getUser() == null ? "Unknown" : qctx.authState().getUser();
    return user;
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
  ConcurrentHashMap<String, AtomicInteger> getCurrentExecutions() {
    return CURRENT_EXECUTIONS;
  }

  @VisibleForTesting
  AtomicInteger getCountToPurge() {
    return COUNT_TO_PURGE;
  }

  @VisibleForTesting
  void setDisableScheduling(boolean disableScheduling) {
    this.disableScheduling = disableScheduling;
  }

}
