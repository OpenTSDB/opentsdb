package net.opentsdb.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.Timer;
import com.stumbleupon.async.Deferred;
import org.junit.Test;

public class StopTimerCallbackTest {
  @Test
  public void testStopOnCallsStopOnTimer() throws Exception {
    Deferred<Object> d = Deferred.fromResult(null);
    Timer.Context timerContext = mock(Timer.Context.class);
    StopTimerCallback.stopOn(timerContext, d);
    verify(timerContext, times(1)).stop();
  }
}
