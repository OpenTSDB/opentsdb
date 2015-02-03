package net.opentsdb.core;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import com.stumbleupon.async.Deferred;

import java.util.Map;

public abstract class Plugin {
  /**
   * Called by TSDB to initialize the plugin
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the
   * problem. Please use IllegalArgumentException for configuration issues.
   * @param tsdb The parent TSDB object
   * @throws IllegalArgumentException if required configuration parameters are
   * missing
   * @throws Exception if something else goes wrong
   */
  public abstract void initialize(final TSDB tsdb) throws Exception;

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
