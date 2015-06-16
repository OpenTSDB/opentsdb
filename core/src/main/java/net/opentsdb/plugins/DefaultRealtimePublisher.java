package net.opentsdb.plugins;

import net.opentsdb.BuildData;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.TimeseriesId;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

/**
 * A default realtime publisher to use when no other real-time publisher has been configured. This
 * real-time publisher will just discard all data given to it.
 */
public class DefaultRealtimePublisher extends RTPublisher {
  @Override
  public void close() {
  }

  @Override
  public String version() {
    return BuildData.version();
  }

  @Override
  public ListenableFuture<Void> publishDataPoint(final String metric,
                                                 final long timestamp,
                                                 final long value,
                                                 final Map<String, String> tags,
                                                 final TimeseriesId timeSeriesId) {
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> publishDataPoint(final String metric,
                                                 final long timestamp,
                                                 final double value,
                                                 final Map<String, String> tags,
                                                 final TimeseriesId timeSeriesId) {
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> publishAnnotation(final Annotation annotation) {
    return Futures.immediateFuture(null);
  }
}
