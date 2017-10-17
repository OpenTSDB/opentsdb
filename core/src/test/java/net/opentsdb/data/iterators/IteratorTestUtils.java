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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Ignore;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * A helper class with static utilities for working with iterators in unit
 * tests.
 */
@Ignore
public class IteratorTestUtils {

  public static  TimeSeriesId ID_A = BaseTimeSeriesId.newBuilder()
      .setMetric("system.cpu.user")
      .build();
  public static TimeSeriesId ID_B = BaseTimeSeriesId.newBuilder()
      .setMetric("system.cpu.idle")
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
      shard.add(ts, ts);
    }
    
//    groups.addIterator(GROUP_A, shard);
//    groups.addIterator(GROUP_B, shard.getShallowCopy(null));
//    
//    shard = new NumericMillisecondShard(ID_B, 
//        new MillisecondTimeStamp(start), new MillisecondTimeStamp(end), order);
//    for (long ts = start; ts <= end; ts += interval) {
//      shard.add(ts, ts);
//    }
//    
//    groups.addIterator(GROUP_A, shard);
//    groups.addIterator(GROUP_B, shard.getShallowCopy(null));
    
    return groups;
  }

  @SuppressWarnings("unchecked")
  public static void validateData(final IteratorGroups results, 
                                  final long start, final long end,  
                                  final int order, 
                                  final long interval) {
    assertEquals(4, results.flattenedIterators().size());
    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
    assertEquals(2, group.flattenedIterators().size());
    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
    
    TimeSeriesIterator<NumericType> iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    long ts = start;
    int count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    ts = start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
    
    group = results.group(IteratorTestUtils.GROUP_B);
    assertEquals(2, group.flattenedIterators().size());
    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    ts = start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    ts = start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
  }
}