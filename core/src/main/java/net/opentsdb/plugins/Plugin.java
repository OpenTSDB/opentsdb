package net.opentsdb.plugins;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;

import java.io.Closeable;
import java.util.Map;

/**
 * Base plugin interface for all plugin types used by TSDB.
 */
public abstract class Plugin implements Closeable {
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
