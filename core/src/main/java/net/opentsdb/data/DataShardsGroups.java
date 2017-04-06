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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeStamp.TimeStampComparator;

/**
 * A collection of zero or more data shard groups resulting from a query.
 * 
 * @since 3.0
 */
public abstract class DataShardsGroups {

  /** The data type reference to pass around. */
  public static final TypeToken<DataShardsGroups> TYPE = 
      TypeToken.of(DataShardsGroups.class);
  
  /** The list of data shards. May be null. */
  protected List<DataShardsGroup> data;
  
  /** The base time shared by all shards. */
  protected TimeStamp base_time;
  
  /** An order if shard is part of a slice config. */
  protected int order;
  
  /**
   * Default ctor.
   */
  public DataShardsGroups() {
    order = -1;
  }
  
  /** @return The list of data shard groups. May be empty. */
  public List<DataShardsGroup> data() {
    return data == null ? Collections.<DataShardsGroup>emptyList() : data;
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
   * Adds a group to the collection.
   * <b>Note:</b> This method does not check for duplicate groups.
   * @param group A non-null group.
   * @throws IllegalArgumentException if the group was null, the group's 
   * base time differed from the collection's base time or it's order was 
   * different.
   */
  public void addGroup(final DataShardsGroup group) {
    if (group == null) {
      throw new IllegalArgumentException("Group cannot be null.");
    }
    if (base_time == null) {
      base_time = group.baseTime();
      order = group.order();
    } else {
      if (base_time.compare(TimeStampComparator.NE, group.baseTime())) {
        throw new IllegalArgumentException("Shards base time " + group.baseTime() 
          + " was different from the collection's time: " + base_time);
      }
      if (order != group.order()) {
        throw new IllegalArgumentException("Shard order " + group.order() 
          + " was different from the collection's order: " + order + " " 
          + group.id());
      }
    }
    if (data == null) {
      data = Lists.newArrayList(group);
    } else {
      data.add(group);
    }
  }
  
  /**
   * Add a collection of groups to this group set.
   * @param groups A non-null collection of groups.
   * @throws IllegalArgumentException if the groups was null, any group was null,
   * a group's base time differed from the collection's base time or a group 
   * order was different.
   */
  public void addGroups(final Collection<DataShardsGroup> groups) {
    if (groups == null) {
      throw new IllegalArgumentException("Groups cannot be null.");
    }
    for (final DataShardsGroup group : groups) {
      addGroup(group);
    }
  }
}
