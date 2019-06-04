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

public class FixedThreadPoolExecutor implements TSDBThreadPoolExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(FixedThreadPoolExecutor.class);

  public static final String TYPE = "FixedThreadPoolExecutor";

  public String id;
  ExecutorService fixedThreadPool;

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
    if (this.fixedThreadPool != null) {
      LOG.error("FixedThreadPoolExecutor is already initialized!!");
      return Deferred
          .fromError(new IllegalStateException("FixedThreadPoolExecutor is already initialized"));
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(MAX_SIZE))) {
      tsdb.getConfig().register(getConfigKey(MAX_SIZE), 100000, false,
          "The ID of an object pool factory plugin to use for this pool. "
              + "Can be null to use the default.");
    }

    Integer maxSize = tsdb.getConfig().getInt(getConfigKey(MAX_SIZE));
    LOG.info("Initializing new FixedThreadPoolExecutor with queue max capacity of {}", maxSize);
    int nThreads = Runtime.getRuntime().availableProcessors() * 2;
    this.fixedThreadPool =

        new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(maxSize));

    LOG.info("Initialized new FixedThreadPoolExecutor with {} threads", nThreads);
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    if (!fixedThreadPool.isShutdown()) {
      fixedThreadPool.shutdown();
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

  String getConfigKey(final String key) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return this.fixedThreadPool.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return this.fixedThreadPool.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return this.fixedThreadPool.submit(task);
  }

}
