package net.opentsdb.core;

import java.util.Map;

import net.opentsdb.BuildData;
import net.opentsdb.meta.Annotation;

import com.stumbleupon.async.Deferred;

/**
 * A default realtime publisher to use when no other realtime publisher has
 * been configured. This realtime publisher will just discard all data given
 * to it.
 * @see net.opentsdb.core.TSDB
 */
public class DefaultRealtimePublisher extends RTPublisher {
  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return BuildData.version();
  }

  @Override
  public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final long value, final Map<String, String> tags, final byte[] tsuid) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value, final Map<String, String> tags, final byte[] tsuid) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> publishAnnotation(final Annotation annotation) {
    return Deferred.fromResult(null);
  }
}
