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
