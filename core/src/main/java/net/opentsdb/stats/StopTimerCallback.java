package net.opentsdb.stats;

import static com.google.common.base.Preconditions.checkNotNull;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

/**
 * A {@link FutureCallback} implementation for use with {@link ListenableFuture}s. This callback
 * will stop the provided timer when called.
 */
public class StopTimerCallback<T> implements FutureCallback<T> {
  private final Timer.Context timerContext;

  /**
   * Create a new instance that will stop the provided timer when called.
   *
   * @param timerContext The timer to call stop on
   */
  public StopTimerCallback(final Timer.Context timerContext) {
    this.timerContext = checkNotNull(timerContext);
  }

  /**
   * Add a callback on the provided {@link ListenableFuture} that will stop the provided {@link
   * com.codahale.metrics.Timer.Context} once called.
   *
   * @param timerContext The timer to stop
   * @param future The future to wait on
   * @param <T> The type of result returned by the future
   */
  public static <T> void stopOn(final Timer.Context timerContext,
                                final ListenableFuture<T> future) {
    Futures.addCallback(future, new StopTimerCallback<T>(timerContext));
  }

  @Override
  public void onSuccess(@Nullable final T result) {
    timerContext.stop();
  }

  @Override
  public void onFailure(final Throwable t) {
    timerContext.stop();
  }
}
