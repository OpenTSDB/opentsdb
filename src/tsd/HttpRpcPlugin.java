// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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

import java.io.IOException;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * @since 2.1
 */
public abstract class HttpRpcPlugin {
  /**
   * Called by TSDB to initialize the plugin.
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the 
   * problem. Please use IllegalArgumentException for configuration issues.
   * @param tsdb The parent TSDB object
   * @throws IllegalArgumentException if required configuration parameters are 
   * missing
   * @throws Exception
   */
  public abstract void initialize(TSDB tsdb);
  
  /**
   * Called to gracefully shutdown the plugin.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> shutdown();
  
  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. "2.0.1". The MAJOR version should match the major
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
  public abstract void collectStats(StatsCollector collector);
  
  /**
   * @return
   */
  public abstract String getPath();

  /**
   * @param tsdb
   * @param query
   * @throws IOException
   */
  public abstract void execute(TSDB tsdb, HttpRpcPluginQuery query) throws IOException;

}
