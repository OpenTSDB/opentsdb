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

import java.util.concurrent.ExecutorService;

import io.netty.util.Timer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.query.QueryContext;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.threadpools.TSDBThreadPoolExecutor;

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
  
  /**
   * A thread pool for use by query nodes when operations need to be 
   * handled in a separate thread.
   * @return A non-null executor service.
   */
  public TSDBThreadPoolExecutor getQueryThreadPool();
  
  /**
   * An executor service used to run tasks with short life span, typically around <10 ms. For long
   * running tasks, prefer using {@code getQueryThreadPool()}
   * 
   * @return
   */
  public ExecutorService quickWorkPool();
  
  /**
   * A timer for use by queries.
   * <b>WARNING:</b> Make sure to use {@link #getQueryThreadPool()} as 
   * soon as the timer task is executed. Do not perform heavy work in
   * the timer thread itself.
   * @return A non-null timer.
   */
  public Timer getQueryTimer();
  
  /**
   * Adds a running query to the tracking map so we can clean up resources.
   * @param hash The hash of the query.
   * @param context The non-null context.
   * @return True if added successfully, false if there was a collision.
   */
  public boolean registerRunningQuery(final long hash, 
                                      final QueryContext context);
  
  /**
   * Calls {@code close()} on the running query and removes it from the 
   * tracking map, returning true if found, false if not.
   * @param hash The hash of the query.
   * @return True if the query was found and closed, false if not.
   */
  public boolean completeRunningQuery(final long hash);
  
}
