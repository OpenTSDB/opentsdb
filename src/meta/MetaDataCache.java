// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.meta;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * This is a first stab at a meta data cache. Initially it only handles
 * incrementing TSUID counters in a local database. The class will then 
 * periodically sync the local counter cache with HBase and generate TSMeta
 * objects if necessary. This keeps us from having to maintain thousands or
 * millions of callback objects in memory while we wait for individual atomic
 * increments per data point. 
 * @since 2.3
 */
public abstract class MetaDataCache {
  
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
   * @throws RuntimeException if something else goes wrong
   */
  public abstract void initialize(final TSDB tsdb);

  /**
   * Called when the TSD is shutting down to gracefully flush any buffers or
   * close open connections.
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
   * Called by the TSD when a request for statistics collection has come in. The
   * implementation may provide one or more statistics. If no statistics are
   * available for the implementation, simply stub the method.
   * @param collector The collector used for emitting statistics
   */
  public abstract void collectStats(final StatsCollector collector);

  /**
   * Increments the given TSUID in the cache by 1
   * @param tsuid The tsuid to increment 
   */
  public abstract void increment(final byte[] tsuid);

}
