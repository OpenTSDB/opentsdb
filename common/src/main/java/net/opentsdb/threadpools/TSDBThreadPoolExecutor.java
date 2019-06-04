package net.opentsdb.threadpools;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import net.opentsdb.core.TSDBPlugin;

public interface TSDBThreadPoolExecutor extends TSDBPlugin {
  public static final String KEY_PREFIX = "threadpool.";

  public static final String MAX_SIZE = "max.size";

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
   * @param <T> the type of the task's result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  <T> Future<T> submit(Callable<T> task);

  /**
   * Submits a Runnable task for execution and returns a Future
   * representing that task. The Future's {@code get} method will
   * return the given result upon successful completion.
   *
   * @param task the task to submit
   * @param result the result to return
   * @param <T> the type of the result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  <T> Future<T> submit(Runnable task, T result);

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


}
