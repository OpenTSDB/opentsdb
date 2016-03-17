/*
 * Copyright 2015, Simon Matić Langford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.stats;

import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.Config;

/**
 *
 */
public class DummyLatencyStatsPlugin extends LatencyStatsPlugin {
  
  private static LatencyStatsPlugin mock;

  public static void setMock(LatencyStatsPlugin mock) {
    DummyLatencyStatsPlugin.mock = mock;
  }

  @Override
  public void initialize(Config config, String metricName, String xtratag) {
    if (mock != null) {
      mock.initialize(config, metricName, xtratag);
    }
  }

  @Override
  public Deferred<Object> shutdown() {
    if (mock != null) {
      return mock.shutdown();
    }
    return Deferred.fromResult(null);
  }

  @Override
  public void start() {
    if (mock != null) {
      mock.start();
    }
  }

  @Override
  public String version() {
    if (mock != null) {
      return mock.version();
    }
    return null;
  }

  @Override
  public void collectStats(StatsCollector collector) {
    if (mock != null) {
      mock.collectStats(collector);
    }
  }

  @Override
  public void add(int value) {
    if (mock != null) {
      mock.add(value);
    }
  }
}
