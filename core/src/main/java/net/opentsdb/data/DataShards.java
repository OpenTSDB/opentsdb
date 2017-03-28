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
package net.opentsdb.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.opentsdb.data.TimeStamp.TimeStampComparator;

/**
 * A collection of zero or more {@link DataShard}s with different data types
 * for the same time series ID. For example, if a query is asking for numeric
 * data, annotations and quality information for a specific time series, the
 * underlying data store can populate this object with a shard for each type
 * of data. The shards will then pass through the proper processors for their
 * type.
 * <p>
 * <b>Invariants:</b>
 * <ul>
 * <li>Each shard must share the same {@link TimeSeriesId} as other shards in 
 * the set.</li>
 * <li>Each shard must have the same {@link #baseTime()} as the other shards
 * and share the same timespan.</li>
 * <li>Only one shard per unique data type is allowed in the set. When calling
 * {@link #addShard(DataShard)}, if data with the given type is present, an
 * exception is thrown.</li>
 * </ul>
 * 
 * @since 3.0
 */
public abstract class DataShards {
  /** Returned for empty results. */
  protected static final List<DataShard<?>> EMPTY_SHARDS = 
      Collections.unmodifiableList(new ArrayList<DataShard<?>>(0));
  
  /** The ID shared by all time series in the shard list. */
  protected TimeSeriesId id;
  
  /** The list of shards. May be null. */
  protected List<DataShard<?>> shards;
  
  /** The base time shared by all shards. */
  protected TimeStamp base_time;
  
  /** An order if shard is part of a slice config. */
  protected int order;
  
  /**
   * Default ctor that sets the ID.
   * @param id A non-null ID.
   * @throws IllegalArgumentException if the ID was null.
   */
  public DataShards(final TimeSeriesId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.id = id;
    order = -1;
  }
  
  /** @return The ID shared with all shards in the set. */
  public TimeSeriesId id() {
    return id;
  }
  
  /** @return The base timestamp of the shards. May be null. */
  public TimeStamp baseTime() {
    return base_time;
  }
  
  /** @return An unmodifiable list of data shards in this collection. May be empty. */
  public List<DataShard<?>> data() {
    return shards == null ? EMPTY_SHARDS : Collections.unmodifiableList(shards);
  }
  
  /** @return An optional order within a slice config. -1 by default. */
  public int order() {
    return order;
  }
  
  /**
   * Adds a shard to the collection.
   * @param shard A non-null data shard.
   * @throws IllegalArgumentException if the shard was null, it's ID was different
   * from the collection ID, it's base time was different from the collection's
   * base time, it had a different order from other shards or a shard of the 
   * given type is already present.
   */
  public void addShard(final DataShard<?> shard) {
    if (shard == null) {
      throw new IllegalArgumentException("Shard cannot be null.");
    }
    if (!shard.id().equals(id)) {
      throw new IllegalArgumentException("Shard ID [" + shard.id() + "] differs "
          + "from the collection id: " + id);
    }
    if (shards == null) {
      shards = new ArrayList<DataShard<?>>(1);
      shards.add(shard);
      base_time = shard.baseTime();
      order = shard.order();
    } else {
      if (base_time.compare(TimeStampComparator.NE, shard.baseTime())) {
        throw new IllegalArgumentException("Shard base time " + shard.baseTime() 
          + " was different from the collection's time: " + base_time);
      }
      if (shard.order() != order) {
        throw new IllegalArgumentException("Shard order " + shard.order() 
          + " was different from the collection's order: " + order);
      }
      for (final DataShard<?> existing : shards) {
        if (existing.type().equals(shard.type())) {
          throw new IllegalArgumentException("Shards already have data for type " 
              + shard.type());
        }
      }
      shards.add(shard);
    }
  }
  
  /** @return TODO */
  public boolean cached() {
    return false;
  }
}
