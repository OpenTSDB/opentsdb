package net.opentsdb.core;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import com.stumbleupon.async.Deferred;

import java.util.Map;

/**
 * Base plugin interface for all plugin types used by TSDB.
 */
public abstract class Plugin {
  /**
   * Called to gracefully shutdown the plugin. Implementations should close
   * any IO they have open
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> shutdown();

  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. 2.0.1. The MAJOR version should match the major
   * version of OpenTSDB the plugin is meant to work with.
   * @return A version string used to log the loaded version
   */
  public abstract String version();

  /**
   * May be called to retrieve references to the metrics that this plugin
   * exposes. There are no guarantees that this will ever be called.
   * @return A map of {@link Metric}s mapped to their names as defined by
   * metrics-core
   */
  public MetricSet metrics() {
    return new MetricSet() {
      @Override
      public Map<String, Metric> getMetrics() {
        return ImmutableMap.of();
      }
    };
  }
}
