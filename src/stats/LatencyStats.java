// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.stats;

import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages latency stats plugins for given measurement points. Supports differing plugin by measurement point as well as
 * specifying a global default via config. If no config exists then this will return an {@link Histogram}. Ensures each 
 * returned instance is a singleton.
 */
public class LatencyStats {

  /** Logging */
  private static final Logger LOG = LoggerFactory.getLogger(LatencyStats.class);

  /** Created instances */
  private static ConcurrentHashMap<String, LatencyStatsPlugin> instances = new ConcurrentHashMap<String, LatencyStatsPlugin>();

  /**
   * Get the latency stats plugin for a given measurement point.
   * @param config The configuration for this TSDB instance
   * @param instanceName The named measurement point
   * @param metricName The name of the metric to emit aggregations to
   */
  public static LatencyStatsPlugin getInstance(Config config, String instanceName, String metricName) {
    return getInstance(config, instanceName, metricName, null);
  }

  /**
   * Get the latency stats plugin for a given measurement point.
   * @param config The configuration for this TSDB instance
   * @param instanceName The named measurement point
   * @param metricName The name of the metric to emit aggregations to
   * @param xtratag    Extra tags to use when emitting aggregations
   */
  public static LatencyStatsPlugin getInstance(Config config, String instanceName, String metricName, String xtratag) {
    // simple existence check
    if (instances.containsKey(instanceName)) {
      return instances.get(instanceName);
    }
    
    // need to create one..
    final String configKey = "tsd.latency_stats.plugin";
    
    LatencyStatsPlugin ret;
    if (config.hasProperty(configKey)) {
      ret = PluginLoader.loadSpecificPlugin(
              config.getString(configKey), LatencyStatsPlugin.class);
      if (ret == null) {
        throw new IllegalArgumentException(
                "Unable to locate latency stats plugin: " + config.getString(configKey));
      }
      try {
        ret.initialize(config, metricName, xtratag);
      } catch (Exception e) {
        throw new RuntimeException(
                "Failed to initialize latency stats plugin", e);
      }
      LOG.info("Successfully initialized latency stats plugin - instance for " + instanceName + " [" +
              ret.getClass().getCanonicalName() + "] version: "
              + ret.version());
    } else {
      ret = new Histogram(16000, (short) 2, 100);
    }
    
    if (instances.putIfAbsent(instanceName, ret) == null) {
      ret.start();
    };
    return instances.get(instanceName);
  }

  public static List<Deferred<Object>> shutdownAll() {
    List<Deferred<Object>> multi = new ArrayList<Deferred<Object>>(instances.size());
    for (LatencyStatsPlugin p : instances.values()) {
      multi.add(p.shutdown());
    }
    return multi;
  }


  static void clear() {
    instances.clear();
  }
}
