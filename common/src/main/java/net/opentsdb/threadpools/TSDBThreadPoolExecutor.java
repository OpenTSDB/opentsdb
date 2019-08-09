// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.threadpools;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.QueryContext;

/**
 * An interface for ThreadPoolExecutor plugin
 * 
 * This executor is used across the query lifetime by most of the processing pipeline nodes
 * 
 * @since 3.0
 *
 */
public interface TSDBThreadPoolExecutor extends TSDBPlugin {
  
  /** Prefix for the config keys for the ThreadPoolExecutor */
  public static final String KEY_PREFIX = "threadpool.";

  /** Defines the blocking queue size in the ThreadPoolExecutor */
  public static final String QUEUE_MAX_SIZE = "queue.max.size";
  
  /** The number of threads to keep in the pool, even if they are idle */
  public static final String CORE_THREAD_POOL_SIZE = "core.thread.pool.size";

  /** MaximumPoolSize the maximum number of threads to allow in the pool */
  public static final String MAX_THREAD_POOL_SIZE = "max.thread.pool.size";

  /**
   * Returns the type of the ThreadPoolExecutor.
   * 
   * @return 
   */
  public String tpType();

  /**
   * Submits a value-returning task for execution and returns a
   * Future representing the pending results of the task. The
   * Future's {@code get} method will return the task's result upon
   * successful completion.
   *
   * <p>
   * If you would like to immediately block waiting
   * for a task, you can use constructions of the form
   * {@code result = exec.submit(aCallable).get();}
   *
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  <T> Future<T> submit(Callable<T> task);

  /**
   * Submits a value-returning task for execution and returns a
   * Future representing the pending results of the task. The
   * Future's {@code get} method will return the task's result upon
   * successful completion. Uses the QueryContext to in the scheduling logic
   *
   * <p>
   * If you would like to immediately block waiting
   * for a task, you can use constructions of the form
   * {@code result = exec.submit(aCallable).get();}
   *
   * @param task the task to submit
   * @param qctx the QueryContext for advanced scheduling
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  <T> Future<T> submit(Callable<T> task, QueryContext qctx);

  /**
   * Submits a value-returning task for execution and returns a
   * Future representing the pending results of the task. The
   * Future's {@code get} method will return the task's result upon
   * successful completion. Uses the {@code QueryContext} and 
   * {@code TSDTask} in the scheduling logic
   *
   * <p>
   * If you would like to immediately block waiting
   * for a task, you can use constructions of the form
   * {@code result = exec.submit(aCallable).get();}
   *
   * @param task the task to submit
   * @param qctx the QueryContext for advanced scheduling
   * @param tsdTask the {@code TSDTask} for task scheduling
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  <T> Future<T> submit(Callable<T> task, QueryContext qctx, TSDTask tsdTask);

  /**
   * Submits a Runnable task for execution and returns a Future
   * representing that task. The Future's {@code get} method will
   * return {@code null} upon <em>successful</em> completion.
   *
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  Future<?> submit(Runnable task);
 
  /**
   * Submits a Runnable task for execution and returns a Future
   * representing that task. The Future's {@code get} method will
   * return {@code null} upon <em>successful</em> completion.
   * Uses the {@code QueryContext} in the scheduling logic
   *
   * @param task the task to submit
   * @param qctx the QueryContext for advanced scheduling
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  Future<?> submit(Runnable task, QueryContext qctx);

  /**
   * Submits a Runnable task for execution and returns a Future
   * representing that task. The Future's {@code get} method will
   * return {@code null} upon <em>successful</em> completion. 
   * Uses the {@code QueryContext} and {@code TSDTask} in the scheduling logic
   *
   * @param task the task to submit
   * @param qctx the QueryContext for advanced scheduling
   * @param tsdTask the {@code TSDTask} for task scheduling
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  Future<?> submit(Runnable task, QueryContext qctx, TSDTask tsdTask);

}
