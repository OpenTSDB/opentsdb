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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.core.TSDB;

/**
 * This is a super simple {@link BlockingQueue} based pool if no other 
 * plugin is found for arrays of primitives or objects.
 * 
 * @since 3.0
 */
public class BlockingQueueArrayObjectPool implements ArrayObjectPool, TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      BlockingQueueArrayObjectPool.class);
  
  /** Reference to the TSDB. */
  private final TSDB tsdb;
  
  /** The config. */
  private final ObjectPoolConfig config;
  
  /** The queue we'll circulate objects through. */
  private final BlockingQueue<PooledObject> pool;
  
  /**
   * The default ctor.
   * @param tsdb The TSDB we'll use for stats.
   * @param config The non-null config we'll pull the allocator from.
   */
  public BlockingQueueArrayObjectPool(final TSDB tsdb, final ObjectPoolConfig config) {
    this.tsdb = tsdb;
    this.config = config;
    int count = config.initialCount() > 0 ? config.initialCount() : 
      config.allocator().initialCount();
    if (count <= 0) {
      count = 1024;
    }
    if (!(config.allocator() instanceof ArrayObjectPoolAllocator)) {
      throw new IllegalArgumentException("The allocator must be of the type "
          + "'ArrayObjectPoolAllocator'");
    }
    pool = new ArrayBlockingQueue<PooledObject>(count);
    for (int i = 0; i < count; i++) {
      final Object obj = config.arrayLength() > 0 ? 
          ((ArrayObjectPoolAllocator) config.allocator()).allocate(config.arrayLength())
          : config.allocator().allocate();
      pool.offer(new LocalPooled(obj, true));
    }
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Instantiated object pool with " + count + " entries for " 
          + config.id() + " with an array size of " + 
          (config.arrayLength() > 0 ? config.arrayLength() : 
            ((ArrayObjectPoolAllocator) config.allocator()).pooledLength()));
    }
  }
  
  @Override
  public PooledObject claim() {
    PooledObject obj = pool.poll();
    if (obj == null) {
      tsdb.getStatsCollector().incrementCounter("objectpool.claim.miss", 
          "pool", config.id());
      return new LocalPooled(config.allocator().allocate(), false);
    } else {
      tsdb.getStatsCollector().incrementCounter("objectpool.claim.success", 
          "pool", config.id());
      return obj;
    }
  }

  @Override
  public PooledObject claim(final long time, final ChronoUnit unit) {
    // TODO - time it and retry
    return claim();
  }

  @Override
  public PooledObject claim(final int length) {
    if (length > ((ArrayObjectPoolAllocator) config.allocator()).pooledLength()) {
      tsdb.getStatsCollector().incrementCounter("objectpool.claim.largeArray", 
          "pool", config.id());
      return new LocalPooled(((ArrayObjectPoolAllocator) 
          config.allocator()).allocate(length), false);
    }
    return claim();
  }

  @Override
  public PooledObject claim(final int length, 
                            final long time, 
                            final ChronoUnit unit) {
    // TODO - time it and retry
    return claim(length);
  };
  
  @Override
  public String id() {
    return config.id();
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  public void run(final Timeout timeout) {
    try {
      tsdb.getStatsCollector().setGauge("objectpool.available", pool.size(),
          "pool", config.id());
    } catch (Throwable t) {
      LOG.error("Failed to run the metric timer task for " + config.id(), t);
    } finally {
      tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
    }
  }
  
  /**
   * Super simple wrapper.
   */
  private class LocalPooled implements PooledObject {
    final Object obj;
    final boolean was_pooled;
    
    protected LocalPooled(final Object obj, final boolean was_pooled) {
      this.obj = obj;
      if (obj instanceof CloseablePooledObject) {
        ((CloseablePooledObject) obj).setPooledObject(this);
      }
      this.was_pooled = was_pooled;
    }
    
    @Override
    public Object object() {
      return obj;
    }

    @Override
    public void release() {
      if (was_pooled) {
        if (!pool.offer(this)) {
          LOG.warn("Failed to return a pooled object to the pool for " + config.id());
          tsdb.getStatsCollector().incrementCounter("objectpool.offer.failure", 
              "pool", config.id());
        }
      }
    }
    
  }

  
}
