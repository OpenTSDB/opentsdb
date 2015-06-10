package net.opentsdb.stats;

import static com.google.common.base.Preconditions.checkNotNull;

import com.codahale.metrics.Timer;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * A {@link com.stumbleupon.async.Callback} implementation for use with {@link
 * com.stumbleupon.async.Deferred}s. This callback will stop the provided timer when called. It will
 * not make any modifications to the result of the callback.
 */
public class StopTimerCallback<T> implements Callback<T, T> {
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
   * Add both an errback and a callback on the provided {@link com.stumbleupon.async.Deferred} that
   * will stop the provided {@link com.codahale.metrics.Timer.Context} once called.
   *
   * @param timerContext The timer to stop
   * @param deferred The deferred to wait on
   * @param <T> The type of result returned by the deferred
   * @return The deferred with an updated callback chain
   */
  public static <T> Deferred<T> stopOn(final Timer.Context timerContext,
                                       final Deferred<T> deferred) {
    return deferred.addBoth(new StopTimerCallback<T>(timerContext));
  }

  @Override
  public T call(final T arg) {
    timerContext.stop();
    return arg;
  }
}
