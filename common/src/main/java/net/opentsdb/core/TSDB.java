// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.core;

import io.netty.util.Timer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.stats.StatsCollector;

/**
 * The core interface for an OpenTSDB client.
 * 
 * @since 3.0
 */
public interface TSDB {

  /** @return The non-null OpenTSDB config repository. */
  public Configuration getConfig();
  
  /** @return The non-null registry for components. */
  public Registry getRegistry();
  
  /** @return The non-null metric and stats collector/reporter. */
  public StatsCollector getStatsCollector();

  /** @return A timer used for scheduling non-critical maintenance tasks
   * like metrics collection, expirations, etc. */
  public Timer getMaintenanceTimer();
  
}
