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
import java.util.concurrent.TimeUnit;

import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.core.TSDB;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolException;
import stormpot.BlazePool;
import stormpot.MetricsRecorder;
import stormpot.PoolException;
import stormpot.Slot;

/**
 * An object pool implementation using StormPot as the backend store. 
 * 
 * @since 3.0
 */
public class StormPotPool implements ObjectPool, TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(StormPotPool.class);
  
  private static final int SIZE = (int) (
      ClassLayout.parseClass(SPPoolable.class).instanceSize() + 8 /* ref in pool */);
  private static final stormpot.Timeout DEFAULT_TIMEOUT = 
      new stormpot.Timeout(1, TimeUnit.NANOSECONDS);
  
  protected final TSDB tsdb;
  
  /** The pool. */
  protected final stormpot.BlazePool<SPPoolable> stormpot;
  
  /** The original config reference. */
  protected final ObjectPoolConfig config;
  
  /** Counters to track metrics. */
  protected long last_allocations;
  protected long last_allocations_failed;
  protected long last_leaks;
  
  /**
   * Ctor called by the factory.
   * @param tsdb
   * @param config
   */
  protected StormPotPool(final TSDB tsdb, final ObjectPoolConfig config) {
    this.tsdb = tsdb;
    this.config = config;
    stormpot.Config<SPPoolable> storm_pot_config = 
        new stormpot.Config<SPPoolable>()
        .setAllocator(new SPAllocator())
        .setSize(config.initialCount())
        .setMetricsRecorder(new SPMetricsRecorder());
    try {
      stormpot = new BlazePool<SPPoolable>(storm_pot_config);
      // TODO - config
      tsdb.getMaintenanceTimer().newTimeout(this, 0, TimeUnit.SECONDS);
      LOG.info("Instantiated StormPot pool with ID: " + config.id());
    } catch (Exception e) {
      LOG.error("Failed to instantiated StormPot pool with ID: " + config.id(), e);
      throw new ObjectPoolException(e);
    }
  }
  
  @Override
  public PooledObject claim() {
    try {
      final PooledObject poolable = stormpot.claim(DEFAULT_TIMEOUT);
      if (poolable != null) {
        tsdb.getStatsCollector().incrementCounter("objectpool.claim.success", 
            "pool", config.id());
        return poolable;
      }
    } catch (PoolException e) {
      LOG.error("Unexpected pool exception for: " + config.id(), e);
    } catch (InterruptedException e) {
      throw new ObjectPoolException(e);
    }
    // arg, missed a claim so we're allocating a new object.
    tsdb.getStatsCollector().incrementCounter("objectpool.claim.miss", 
        "pool", config.id());
    return new SPPoolable(config.allocator().allocate(), null);
  }

  @Override
  public PooledObject claim(final long time, final ChronoUnit unit) {
    try {
      // arg, need jdk9+
      TimeUnit tu = null;
      switch (unit) {
      case NANOS:
        tu = TimeUnit.NANOSECONDS;
        break;
      case MICROS:
        tu = TimeUnit.MICROSECONDS;
        break;
      case MILLIS:
        tu = TimeUnit.MILLISECONDS;
        break;
      case SECONDS:
        tu = TimeUnit.SECONDS;
        break;
      default:
        throw new ObjectPoolException("Must choose something <= seconds. "
            + "Otherwise why are you waiting so long?");
      }
      final PooledObject poolable = stormpot.claim(new stormpot.Timeout(time, tu));
      if (poolable != null) {
        tsdb.getStatsCollector().incrementCounter("objectpool.claim.success", 
            "pool", config.id());
        return poolable;
      }
    } catch (PoolException e) {
      LOG.error("Unexpected pool exception for: " + config.id(), e);
    } catch (InterruptedException e) {
      throw new ObjectPoolException(e);
    }
    // arg, missed a claim so we're allocating a new object.
    tsdb.getStatsCollector().incrementCounter("objectpool.claim.miss", 
        "pool", config.id());
    return new SPPoolable(config.allocator().allocate(), null);
  }
  
  @Override
  public Deferred<Object> shutdown() {
    // TODO - WTF? Why doesn't his completion return a Void or null at least?
    try {
      stormpot.shutdown();
    } catch (Throwable t) {
      LOG.error("WTF?", t);
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public String id() {
    return config.id();
  }
  
  @Override
  public void run(final Timeout timeout) throws Exception {
    try {
      long temp = stormpot.getAllocationCount();
      tsdb.getStatsCollector().incrementCounter("objectpool.allocation.success", 
          temp - last_allocations, "pool", config.id());
      last_allocations = temp;
      
      temp = stormpot.getFailedAllocationCount();
      tsdb.getStatsCollector().incrementCounter("objectpool.allocation.failed", 
          temp - last_allocations_failed, "pool", config.id());
      last_allocations_failed = temp;
      
      temp = stormpot.getLeakedObjectsCount();
      tsdb.getStatsCollector().incrementCounter("objectpool.leaks", 
          temp - last_leaks, "pool", config.id());
      last_leaks = temp;
      
    } catch (Throwable t) {
      LOG.error("Failed to record metrics for pool: " + config.id(), t);
    }
    // TODO - config
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
  }
  
  /**
   * Poolable wrapper that can set the poolable on closeable pooled objects.
   */
  protected class SPPoolable implements stormpot.Poolable, PooledObject {
    final Object object;
    final Slot slot;
    
    SPPoolable(final Object object, final Slot slot) {
      this.object = object;
      this.slot = slot;
      if (object instanceof CloseablePooledObject) {
        ((CloseablePooledObject) object).setPooledObject(this);
      }
    }
    
    @Override
    public Object object() {
      return object;
    }
    
    @Override
    public void release() {
      if (slot != null) {
        try {
          slot.release(this);
          tsdb.getStatsCollector().incrementCounter("objectpool.release.success", 
              "pool", config.id());
        } catch (Throwable t) {
          LOG.error("Unexpected exception releasing object to pool", t);
        }
      }
    }
    
  }

  /**
   * Allocator to set the slot on a poolable. And uses the ObjectPoolAllocator.
   */
  protected class SPAllocator implements stormpot.Allocator<SPPoolable> {

    @Override
    public SPPoolable allocate(final Slot slot) throws Exception {
      return new SPPoolable(config.allocator().allocate(), slot);
    }

    @Override
    public void deallocate(SPPoolable poolable) throws Exception {
      poolable.release();
      config.allocator().deallocate(poolable.object);
    }
    
  }
  
  /**
   * Recorder to route metrics from the storm pot pool to the stats recorder.
   */
  protected class SPMetricsRecorder implements MetricsRecorder {

    @Override
    public void recordAllocationLatencySampleMillis(final long milliseconds) {
      tsdb.getStatsCollector().addTime("objectpool.allocation.success.latency", 
          milliseconds, ChronoUnit.MILLIS, "pool", config.id());
    }

    @Override
    public void recordAllocationFailureLatencySampleMillis(final long milliseconds) {
      tsdb.getStatsCollector().addTime("objectpool.allocation.failure.latency", 
          milliseconds, ChronoUnit.MILLIS, "pool", config.id());
    }

    @Override
    public void recordDeallocationLatencySampleMillis(final long milliseconds) {
      tsdb.getStatsCollector().addTime("objectpool.deallocation.latency", 
          milliseconds, ChronoUnit.MILLIS, "pool", config.id());
    }

    @Override
    public void recordReallocationLatencySampleMillis(final long milliseconds) {
      tsdb.getStatsCollector().addTime("objectpool.reallocation.success.latency", 
          milliseconds, ChronoUnit.MILLIS, "pool", config.id());
    }

    @Override
    public void recordReallocationFailureLatencySampleMillis(
        final long milliseconds) {
      tsdb.getStatsCollector().addTime("objectpool.reallocation.failure.latency", 
          milliseconds, ChronoUnit.MILLIS, "pool", config.id());
    }

    @Override
    public void recordObjectLifetimeSampleMillis(final long milliseconds) {
      tsdb.getStatsCollector().addTime("objectpool.object.lifetime", milliseconds, 
          ChronoUnit.MILLIS, "pool", config.id());
    }

    @Override
    public double getAllocationLatencyPercentile(final double percentile) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getAllocationFailureLatencyPercentile(final double percentile) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getDeallocationLatencyPercentile(final double percentile) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getReallocationLatencyPercentile(final double percentile) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getReallocationFailurePercentile(final double percentile) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getObjectLifetimePercentile(final double percentile) {
      // TODO Auto-generated method stub
      return 0;
    }
    
  }

    
}