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

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.rollup.RollupInterval;

/**
 * The base class for a Tsdb1x Partial Time Series to be populated by 1x style
 * schemas.
 * 
 * @param <T> The type of {@link TimeSeriesDataType}.
 * 
 * @since 3.0
 */
public abstract class Tsdb1xPartialTimeSeries<T extends TimeSeriesDataType> 
    implements PartialTimeSeries<T>, CloseablePooledObject {
  /** Reference to the Object pool for this instance. */
  protected PooledObject pooled_object;
  
  /** A hash for the time series ID. */
  protected long id_hash;
  
  /** The set we currently belong to. */
  protected PartialTimeSeriesSet set;
  
  /** A reference counter for the array to determine when we can return it to
   * the pool. */
  protected AtomicInteger reference_counter;
  
  /** An array to store the data in. */
  protected PooledObject pooled_array;
  
  /** The current write index for array stores. */
  protected int write_idx;

  /** The base row timestamp. */
  protected TimeStamp base_timestamp;
  
  /** The pool we use for fetching arrays. */
  protected ObjectPool array_pool;
  
  /** The optional rollup interval. */
  protected RollupInterval interval;
  
  /**
   * Local ctor.
   */
  protected Tsdb1xPartialTimeSeries() {
    reference_counter = new AtomicInteger();
    base_timestamp = new SecondTimeStamp(0);
  }
  
  /**
   * Sorts, de-duplicates and optionally reverses the data in this series. Call
   * it only after adding all of the data.
   * @param keep_earliest Whether or not to keep the earliest duplicates in the
   * array or the latest. 
   * @param reverse Whether or not to reverse the data.
   */
  public abstract void dedupe(final boolean keep_earliest, final boolean reverse);
 
  /**
   * Called on the first time this is fetched from a pool to setup the base 
   * information for the series.
   * @param base_timestamp A non-null base timestamp.
   * @param id_hash The hash ID for the series.
   * @param array_pool An optional array pool.
   * @param set The non-null set.
   * @param interval An optional interval.
   * @throws IllegalArgumentException if the base timestamp, array pool or set
   * were null.
   */
  public void reset(final TimeStamp base_timestamp, 
                    final long id_hash, 
                    final ObjectPool array_pool,
                    final PartialTimeSeriesSet set,
                    final RollupInterval interval) {
    if (base_timestamp == null) {
      throw new IllegalArgumentException("Base timestamp cannot be null.");
    }
    if (set == null) {
      throw new IllegalArgumentException("Set cannot be null.");
    }

    this.id_hash = id_hash;
    this.set = set;
    this.base_timestamp.update(base_timestamp);
    this.array_pool = array_pool;
    this.interval = interval;
    reference_counter.set(0);
    write_idx = 0;
    if (pooled_array != null) {
      pooled_array.release();
      pooled_array = null;
    }
  }
  
  /**
   * Adds a column to the series.
   * @param prefix The schema prefix so we know what kind of data we're dealing
   * with.
   * @param qualifier The non-null qualifier.
   * @param value The non-null value.
   */
  public abstract void addColumn(final byte prefix,
                                 final byte[] qualifier, 
                                 final byte[] value);

  /**
   * Determines if the hash is the same as that given.
   * @param hash The hash to compare.
   * @return True if the hashes are the same, false if not.
   */
  public boolean sameHash(final long hash) {
    return id_hash == hash;
  }
  
  @Override
  public Object object() {
    return this;
  }
  
  @Override
  public void close() throws Exception {
    final int ref = reference_counter.decrementAndGet();
    if (ref == 0) {
      release();
    }
  }
  
  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }

  @Override
  public long idHash() {
    return id_hash;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_BYTE_ID;
  }
  
  @Override
  public PartialTimeSeriesSet set() {
    return set;
  }
  
}
