// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * This is a non-pooling pool that is used if no default implementation is found.
 * It will allocate an object and wrapper for each call. Boo!
 * 
 * @since 3.0
 */
public class DummyObjectPool implements ObjectPool {

  /** The config. */
  private final ObjectPoolConfig config;
  
  /** Stats. */
  private final StatsCollector stats;
  
  /**
   * The default ctor.
   * @param tsdb The TSDB we'll use for stats.
   * @param config The non-null config we'll pull the allocator from.
   */
  public DummyObjectPool(final TSDB tsdb, final ObjectPoolConfig config) {
    stats = tsdb.getStatsCollector();
    this.config = config;
  }
  
  @Override
  public PooledObject claim() {
    stats.incrementCounter("objectpool.claim.miss", "pool", config.id());
    final Object obj = config.allocator().allocate();;
    return new PooledObject() {

      @Override
      public Object object() {
        return obj;
      }

      @Override
      public void release() { }
      
    };
  }

  @Override
  public PooledObject claim(long time, ChronoUnit unit) {
    stats.incrementCounter("objectpool.claim.miss", "pool", config.id());
    final Object obj = config.allocator().allocate();;
    return new PooledObject() {

      @Override
      public Object object() {
        return obj;
      }

      @Override
      public void release() { }
      
    };
  }

  @Override
  public String id() {
    return config.id();
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

}
