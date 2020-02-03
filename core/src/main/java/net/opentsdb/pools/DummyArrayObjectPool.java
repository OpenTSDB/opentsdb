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

/**
 * This is a non-pooling pool that is used if no default implementation is found.
 * It will allocate an object and wrapper for each call. Boo!
 * 
 * @since 3.0
 */
public class DummyArrayObjectPool implements ArrayObjectPool {

  /** The config. */
  private final ObjectPoolConfig config;
  
  /** For stats. */
  private final TSDB tsdb;
  
  /**
   * The default ctor.
   * @param tsdb The TSDB we'll use for stats.
   * @param config The non-null config we'll pull the allocator from.
   */
  public DummyArrayObjectPool(final TSDB tsdb, final ObjectPoolConfig config) {
    this.tsdb = tsdb;
    this.config = config;
    if (!(config.allocator() instanceof ArrayObjectPoolAllocator)) {
      throw new IllegalArgumentException("The allocator must be of the type "
          + "'ArrayObjectPoolAllocator'");
    }
  }
  
  @Override
  public PooledObject claim() {
    tsdb.getStatsCollector().incrementCounter("objectpool.claim.miss", 
        "pool", config.id());
    final Object obj = config.allocator().allocate();
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
  public PooledObject claim(final long time, final ChronoUnit unit) {
    tsdb.getStatsCollector().incrementCounter("objectpool.claim.miss", 
        "pool", config.id());
    final Object obj = config.allocator().allocate();
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
  public PooledObject claim(final int length) {
    final Object obj = ((ArrayObjectPoolAllocator) config.allocator())
        .allocate(length);
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
  public PooledObject claim(final int length, 
                            final long time, 
                            final ChronoUnit unit) {
    return claim(length);
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