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
public abstract class BaseTSDBPlugin implements TSDBPlugin {

  /** The TSDB to which this plugin belongs. */
  protected TSDB tsdb;
  
  /**
   * Ctor without any arguments is required for instantiating plugins.
   */
  protected BaseTSDBPlugin() { }
  
  @Override
  public abstract String id();
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  @Override
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
