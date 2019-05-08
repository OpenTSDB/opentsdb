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
package net.opentsdb.storage.schemas.tsdb1x;

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.NoDataPartialTimeSeriesPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryNode;
import net.opentsdb.rollup.RollupUtils.RollupUsage;

/**
 * A set for Tsdb1x schema implementations.
 * <p>
 * The way this one works (after calling {@code reset()}) is that a scanner calls 
 * {@link #increment(Tsdb1xPartialTimeSeries, boolean)} when it finds a matching
 * time series. The series is buffered and not sent upstream immediately as we 
 * may have more data from other salt scanners and we want to send a real value
 * upstream when {@code complete} is set. The final call to increment with 
 * complete set will send both the buffered and current series upstream.
 * <p>
 * Alternatively some scanners may not find data so they should call 
 * {@link #setCompleteAndEmpty(boolean)} so when all scanners call in an empty
 * data sentinel is sent upstream.
 * 
 * @since 3.0
 */
public class Tsdb1xPartialTimeSeriesSet implements PartialTimeSeriesSet, 
    CloseablePooledObject {

  /** The pooled object reference. */
  protected PooledObject pooled_object;
  
  /** A ref to the TSDB. */
  protected final TSDB tsdb;

  /** A ref to the runnable pool. */
  protected final ObjectPool runnable_pool;
  
  /** Poole used when we need to send an empty data sentinel. */
  protected final ObjectPool no_data_pool;
  
  /** The current node. */
  protected QueryNode node;
  
  /** The start time of the set. */
  protected TimeStamp start;
  
  /** The end time of the set. */
  protected TimeStamp end;
  
  /** The rollup usage flag. Used to determine if the nodes should fall back. */
  protected RollupUsage rollup_usage;
  
  /** The number of time series sent for this set. */
  protected int series;
  
  /** A latch used for salt buckets. */
  protected int latch;
  
  /** Whether or not the set is finished. */
  protected boolean complete;
  
  /** The total number of sets. */
  protected int total_sets;
  
  /** The reference to a map of time series IDs. */
  protected TLongObjectMap<TimeSeriesId> ids;
  
  /** A reference to the last partial time series discovered. */
  protected Tsdb1xPartialTimeSeries pts;
  
  /**
   * Ctor for the allocator.
   * @param tsdb The non-null TSDB reference.
   */
  protected Tsdb1xPartialTimeSeriesSet(final TSDB tsdb) {
    this.tsdb = tsdb;
    runnable_pool = tsdb.getRegistry().getObjectPool(
        PooledPartialTimeSeriesRunnablePool.TYPE);
    no_data_pool = tsdb.getRegistry().getObjectPool(
        NoDataPartialTimeSeriesPool.TYPE);
    pts = null;
    series = 0;
    start = new SecondTimeStamp(0);
    end = new SecondTimeStamp(0);
  }
  
  /**
   * Resets the state for re-use.
   * @param node The non-null query node this set came from.
   * @param start The start time to set.
   * @param end The end time to set.
   * @param rollup_usage The rollup usage.
   * @param salts The number of salts. At least 1.
   * @param total_sets The total number of sets.
   * @param ids The ref to IDs.
   */
  public void reset(final QueryNode node, 
                    final TimeStamp start, 
                    final TimeStamp end, 
                    final RollupUsage rollup_usage,
                    final int salts, 
                    final int total_sets, 
                    final TLongObjectMap<TimeSeriesId> ids) {
    this.node = node;
    this.start.update(start);
    this.end.update(end);
    this.rollup_usage = rollup_usage;
    series = 0;
    complete = false;
    latch = salts;
    this.total_sets = total_sets;
    this.ids = ids;
  }
  
  @Override
  public void close() throws Exception {
    ids = null;
    node = null;
    pts = null;
    release();
  }

  @Override
  public int totalSets() {
    return total_sets;
  }

  @Override
  public synchronized boolean complete() {
    return complete;
  }

  @Override
  public QueryNode node() {
    return node;
  }

  @Override
  public String dataSource() {
    return node.config().getId();
  }

  @Override
  public TimeStamp start() {
    return start;
  }

  @Override
  public TimeStamp end() {
    return end;
  }

  @Override
  public TimeSeriesId id(final long hash) {
    synchronized (ids) { // have to since it's a TLong map
      return ids.get(hash);
    }
  }

  @Override
  public int timeSeriesCount() {
    return series;
  }

  @Override
  public TimeSpecification timeSpecification() {
    // for now always null from the schema. Change with pushdowns.
    return null;
  }

  /**
   * Called when no data was found for the salt scanner.
   * @param is_final Whether or not we're at the end of fallbacks meaning we
   * should send up an empty sentinel.
   */
  public void setCompleteAndEmpty(final boolean is_final) {
    // yeah it's synced cause we have to latch and complete in the same call to
    // avoid races.
    Tsdb1xPartialTimeSeries extant = pts;
    boolean complete = false; // shadow
    synchronized (this) {
      if (--latch == 0) {
        complete = this.complete = true;
        pts = null; // for others
        // TODO - when we make a base/interface for the Tsdb1xQueryNode
        //node.sentData();
      }
    }
    
    if (complete) {
      if (extant != null) {
        final PooledPartialTimeSeriesRunnable runnable = 
            (PooledPartialTimeSeriesRunnable) runnable_pool.claim().object();
        runnable.reset(extant, node);
        tsdb.getQueryThreadPool().submit(runnable);
      } else if (rollup_usage == RollupUsage.ROLLUP_NOFALLBACK || is_final) {
        // send up sentinel
        final NoDataPartialTimeSeries pts = 
            (NoDataPartialTimeSeries) no_data_pool.claim().object();
        pts.reset(this);
        final PooledPartialTimeSeriesRunnable runnable = 
            (PooledPartialTimeSeriesRunnable) runnable_pool.claim().object();
        runnable.reset(pts, node);
        tsdb.getQueryThreadPool().submit(runnable);
      }
    }
  }
  
  /**
   * Called when good data was found for the set.
   * @param pts The non-null series.
   * @param complete Whether or not this is the last series for the set.
   * @throws IllegalArgumentException if the PTS was null.
   */
  public void increment(final Tsdb1xPartialTimeSeries pts, 
                        final boolean complete) {
    if (pts == null) {
      throw new IllegalArgumentException("Time series cannot be null.");
    }
    boolean all_done = false;
    Tsdb1xPartialTimeSeries extant;
    synchronized (this) {
      series++;
      if (complete) {
        if (--latch == 0) {
          this.complete = true;
          all_done = true;
        }
      }
      extant = this.pts;
      if (all_done) {
        this.pts = null;
      } else {
        this.pts = pts;
      }
      // TODO - when we make a base/interface for the Tsdb1xQueryNode
      //node.sentData();
    }
    
    if (extant != null) {
      final PooledPartialTimeSeriesRunnable runnable = 
          (PooledPartialTimeSeriesRunnable) runnable_pool.claim().object();
      runnable.reset(extant, node);
      tsdb.getQueryThreadPool().submit(runnable);
    }
    
    if (all_done) {
      final PooledPartialTimeSeriesRunnable runnable = 
          (PooledPartialTimeSeriesRunnable) runnable_pool.claim().object();
      runnable.reset(pts, node);
      tsdb.getQueryThreadPool().submit(runnable);
    }
  }

  /**
   * A shortcut used to send an empty result upstream, marking the set as complete
   * without any timeseries.
   */
  public void sendEmpty() {
    synchronized (this) {
      if (series > 0) {
        throw new IllegalStateException("Don't call this if you've already "
            + "called increment or setCompleteAndEmpty!");
      }
      latch = 0;
      complete = true;
    }
    final NoDataPartialTimeSeries pts = 
        (NoDataPartialTimeSeries) no_data_pool.claim().object();
    pts.reset(this);
    final PooledPartialTimeSeriesRunnable runnable = 
        (PooledPartialTimeSeriesRunnable) runnable_pool.claim().object();
    runnable.reset(pts, node);
    tsdb.getQueryThreadPool().submit(runnable);
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (pooled_object != null) {
      pooled_object.release();
    }
  }

  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }
  
}