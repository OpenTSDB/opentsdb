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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import net.opentsdb.data.TimeStamp.TimeStampComparator;

/**
 * A collection of zero or more data shards belonging to the same 
 * {@link TimeSeriesGroupId}.
 * 
 * @since 3.0
 */
public abstract class DataShardsGroup {

  /** The group ID shared with all shards. */
  protected TimeSeriesGroupId id;
  
  /** The list of data shards. May be null. */
  protected List<DataShards> data;
  
  /** The base time shared by all shards. */
  protected TimeStamp base_time;
  
  /** An order if shard is part of a slice config. */
  protected int order;
  
  /**
   * Default ctor.
   * @param id A non-null ID.
   * @throws IllegalArgumentException if the ID was null.
   */
  public DataShardsGroup(final TimeSeriesGroupId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.id = id;
    order = -1;
  }
  
  /** @return The time series group Id */
  public TimeSeriesGroupId id() {
    return id;
  }
  
  /** @return The list of data shards. May be empty. */
  public List<DataShards> data() {
    return data == null ? Collections.<DataShards>emptyList() : data; 
  }
  
  /** @return The base time of shards in this collection. */
  public TimeStamp baseTime() {
    return base_time;
  }
  
  /** @return An optional order within a slice config. -1 by default. */
  public int order() {
    return order;
  }
  
  /**
   * Adds a shard collection to the list.
   * @param shards A non-null shards collection.
   * @throws IllegalArgumentException if the shards were null, the shard's 
   * base time differed from the collection's base time or it's order was 
   * different.
   */
  public void addShards(final DataShards shards) {
    if (shards == null) {
      throw new IllegalArgumentException("Shards cannot be null.");
    }
    if (base_time == null) {
      base_time = shards.baseTime();
      order = shards.order();
    } else {
      if (base_time.compare(TimeStampComparator.NE, shards.baseTime())) {
        throw new IllegalArgumentException("Shards base time " + shards.baseTime() 
          + " was different from the collection's time: " + base_time);
      }
      if (order != shards.order()) {
        throw new IllegalArgumentException("Shard order " + shards.order() 
          + " was different from the collection's order: " + order);
      }
    }
    if (data == null) {
      data = Lists.newArrayList(shards);
    } else {
      data.add(shards);
    }
  }
  
  /** @return TODO */
  public boolean cached() {
    return false;
  }
}
