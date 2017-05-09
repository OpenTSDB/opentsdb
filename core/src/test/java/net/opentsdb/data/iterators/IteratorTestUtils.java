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
package net.opentsdb.data.iterators;

import org.junit.Ignore;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;

/**
 * A helper class with static utilities for working with iterators in unit
 * tests.
 */
@Ignore
public class IteratorTestUtils {

  public static  TimeSeriesId ID_A = SimpleStringTimeSeriesId.newBuilder()
      .addMetric("system.cpu.user")
      .build();
  public static TimeSeriesId ID_B = SimpleStringTimeSeriesId.newBuilder()
      .addMetric("system.cpu.idle")
      .build();
  
  public static TimeSeriesGroupId GROUP_A = new SimpleStringGroupId("a");
  public static TimeSeriesGroupId GROUP_B = new SimpleStringGroupId("b");
  
  /**
   * Generates an {@link IteratorGroups} with two groups with two time series
   * each including real values from the start to end timestamp.
   * @param start A start timestamp in milliseconds.
   * @param end An end timestamp in milliseconds.
   * @param order The order of the results.
   * @param interval The interval between values in milliseconds.
   * @return An IteratorGroups object.
   */
  public static IteratorGroups generateData(final long start, 
                                            final long end, 
                                            final int order, 
                                            final long interval) {
    final IteratorGroups groups = new DefaultIteratorGroups();
    
    NumericMillisecondShard shard = new NumericMillisecondShard(ID_A, 
        new MillisecondTimeStamp(start), new MillisecondTimeStamp(end), order);
    for (long ts = start; ts <= end; ts += interval) {
      shard.add(ts, ts, 1);
    }
    
    groups.addIterator(GROUP_A, shard);
    groups.addIterator(GROUP_B, shard.getShallowCopy(null));
    
    shard = new NumericMillisecondShard(ID_B, 
        new MillisecondTimeStamp(start), new MillisecondTimeStamp(end), order);
    for (long ts = start; ts <= end; ts += interval) {
      shard.add(ts, ts, 1);
    }
    
    groups.addIterator(GROUP_A, shard);
    groups.addIterator(GROUP_B, shard.getShallowCopy(null));
    
    return groups;
  }
}
