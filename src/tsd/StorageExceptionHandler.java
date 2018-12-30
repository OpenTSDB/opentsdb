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
package net.opentsdb.tsd;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * This is a plugin for handling data points that fail the write to the 
 * underlying data store for various reasons. For example, HBase may lose a
 * region server and a very busy TSD may queue up RPCs in a region, hit the
 * high watermark, and start rejecting any data points that would hit the
 * region that's offline. In the error callback for each data point, we can
 * call into this object and have it queued to disk, send it to an external
 * queue or even push it to another TSD.
 * @since 2.2
 */
public abstract class StorageExceptionHandler {

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
   * Called by the TSD when a request for statistics collection has come in. The
   * implementation may provide one or more statistics. If no statistics are
   * available for the implementation, simply stub the method.
   * @param collector The collector used for emitting statistics
   */
  public abstract void collectStats(final StatsCollector collector);
  
  /**
   * Receives a data point from the storage attempt along with the exception
   * that was associated with the failure.
   * @param dp The data point to store
   * @param exception The exception associated with the data point
   */
  public abstract void handleError(final IncomingDataPoint dp, 
      final Exception exception);
}
