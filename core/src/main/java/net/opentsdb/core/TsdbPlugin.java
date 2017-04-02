// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.core;

import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.StatsCollector;

/**
 * The base class used for TSDB plugins of all types.
 * 
 * @since 3.0
 */
public abstract class TsdbPlugin {

  /** The TSDB to which this plugin belongs. */
  protected TSDB tsdb;
  
  /**
   * Ctor without any arguments is required for instantiating plugins.
   */
  protected TsdbPlugin() { }
  
  /**
   * An ID for the plugin in case multiple plugins of the same class are 
   * loaded.
   * 
   * @return A non-null string ID.
   */
  public abstract String id();
  
  /**
   * Called by TSDB to initialize the plugin asynchronously. Almost every 
   * implementation will have to override this to load settings from the TSDB's
   * config file.
   * 
   * <b>WARNING:</b> Initialization order is not guaranteed. If you depend on
   * another plugin to be loaded by the TSDB, load on the first call to a method.
   * TODO - ^^ that sucks.
   * 
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * 
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the
   * problem. Please use IllegalArgumentException for configuration issues.
   * If it can't startup for another reason, return an Exception in the deferred.
   * 
   * @param tsdb The parent TSDB object.
   * @return A non-null deferred for the TSDB to wait on to confirm 
   * initialization. The deferred should resolve to a {@code null} on successful 
   * init or an exception on failure.
   * 
   * @throws IllegalArgumentException if required configuration parameters are
   * missing.
   */
  public Deferred<Object> initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }
  
  /**
   * Called to gracefully shutdown the plugin. Implementations should close
   * any IO they have open and release resources.
   * 
   * @return A non-null deferred for the TSDB to wait on to confirm shutdown.
   * The deferred should resolve to a {@code null} on successful shutdown or an 
   * exception on failure.
   */
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
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
   * available for the implementation just ignore this.
   * @param collector The collector used for emitting statistics.
   */
  public void collectStats(final StatsCollector collector) {
    if (collector == null) {
      return;
    }
  }
}
