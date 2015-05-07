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
 * A plugin that runs along side TSD's built-in HTTP endpoints (like the 
 * <code>/api</code> endpoints).  There can be multiple implementations of
 * such plugins per TSD.  These plugins run on the same Netty server as 
 * built-in HTTP endpoints and thus are available on the same port as those
 * endpoints.  However, these plugins are mounted beneath a special base 
 * path called <code>/plugin</code>.
 * 
 * <p>Notes on multi-threaded behavior:
 * <ul>
 *  <li>Plugins are created and initialized <strong>once</strong> per instance
 *    of the TSD.  Therefore, these plugins are effectively singletons.
 *  <li>Plugins will be executed from multiple threads so the {@link #execute}
 *    and {@link collectStats} methods <strong>must be thread safe</strong> 
 *    with respect to the plugin's internal state and external resources.
 * </ul>
 * @since 2.2
 */
public abstract class HttpRpcPlugin {
  /**
   * Called by TSDB to initialize the plugin. This is called <strong>once</strong>
   * (and from a single thread) at the time the plugin in loaded.
   * 
   * <p><b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the 
   * problem. Please use IllegalArgumentException for configuration issues.
   * 
   * @param tsdb The parent TSDB object
   * @throws IllegalArgumentException if required configuration parameters are 
   * missing
   * @throws Exception
   */
  public abstract void initialize(TSDB tsdb);
  
  /**
   * Called to gracefully shutdown the plugin. This is called <strong>once</strong>
   * (and from a single thread) at the time the owning TSD is shutting down.
   * 
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
   * 
   * <p><strong>Note:</strong> Must be thread-safe.
   * 
   * @param collector The collector used for emitting statistics
   */
  public abstract void collectStats(StatsCollector collector);
  
  /**
   * The (web) path this plugin should be available at.  This value 
   * <strong>should</strong> start with a <code>/</code>. However, it 
   * <strong>must not</strong> contain the system's plugin base path or the
   * plugin will fail to load. 
   * 
   * <p>Here are some examples where 
   * <code>path --(is available at)--> server path</code>
   * <ul>
   *  <li><code>/myAwesomePlugin --> /plugin/myAwesomePlugin</code>
   *  <li><code>/myOtherPlugin/operation --> /plugin/myOtherPlugin/operation</code>
   * </ul>
   * 
   * @return a slash separated path
   */
  public abstract String getPath();

  /**
   * Executes the plugin for the given query received on the path derived from
   * {@link #getPath()}.  This method will be called by multiple threads
   * simultaneously and <strong>must be</strong> thread-safe.
   * 
   * @param tsdb the owning TSDB instance.
   * @param query the parsed query
   * @throws IOException
   */
  public abstract void execute(TSDB tsdb, HttpRpcPluginQuery query) throws IOException;

}
