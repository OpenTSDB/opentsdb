// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.IteratorTestUtils;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;

public class TestIteratorGroupsSlicePlanner {
  private long ts_start;
  private long ts_end;
  private TimeSeriesQuery query;
  private IteratorGroupsSlicePlanner planner;
  
  @Before
  public void beforeLocal() throws Exception {
    ts_start = 1493942400000L;
    ts_end = 1493956800000L;
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1493942400000")
            .setEnd("1493956800000"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user"))
        .build();
    planner = new IteratorGroupsSlicePlanner(query);
  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void mergeSlicedResultsOneResult() throws Exception {
//    final IteratorGroups result = IteratorTestUtils
//        .generateData(ts_start, ts_end, 0, 300000);
//    
//    final IteratorGroups merged = planner.mergeSlicedResults(
//        Lists.<IteratorGroups>newArrayList(result));
//    assertNotSame(result, merged);
//    assertEquals(4, merged.flattenedIterators().size());
//    TimeSeriesIterator<NumericType> it = 
//        (TimeSeriesIterator<NumericType>) merged.flattenedIterators().get(0);
//    assertEquals(ts_start, it.next().timestamp().msEpoch());
//    long ts = 0;
//    int dps = 0;
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      ts = it.next().timestamp().msEpoch();
//      ++dps;
//    }
//    assertEquals(ts_end, ts);
//    assertEquals(48, dps);
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void mergeSlicedResultsOneResultNullPadding() throws Exception {
//    final IteratorGroups result = IteratorTestUtils
//        .generateData(ts_start, ts_end, 0, 300000);
//    
//    final IteratorGroups merged = planner.mergeSlicedResults(
//        Lists.<IteratorGroups>newArrayList(null, null, result, null, null));
//    assertNotSame(result, merged);
//    assertEquals(4, merged.flattenedIterators().size());
//    TimeSeriesIterator<NumericType> it = 
//        (TimeSeriesIterator<NumericType>) merged.flattenedIterators().get(0);
//    assertEquals(ts_start, it.next().timestamp().msEpoch());
//    long ts = 0;
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      ts = it.next().timestamp().msEpoch();
//    }
//    assertEquals(ts_end, ts);
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void mergeSlicedResultsMultipleResults() throws Exception {
//    final List<IteratorGroups> results = Lists.newArrayListWithCapacity(4);
//    results.add(IteratorTestUtils
//        .generateData(ts_start, 1493946000000L, 0, 300000));
//    results.add(IteratorTestUtils
//        .generateData(1493946000000L, 1493949600000L, 1, 300000));
//    results.add(IteratorTestUtils
//        .generateData(1493949600000L, 1493953200000L, 2, 300000));
//    results.add(IteratorTestUtils
//        .generateData(1493953200000L, ts_end, 3, 300000));
//    
//    final IteratorGroups merged = planner.mergeSlicedResults(
//        Lists.<IteratorGroups>newArrayList(results));
//    assertEquals(4, merged.flattenedIterators().size());
//    TimeSeriesIterator<NumericType> it = 
//        (TimeSeriesIterator<NumericType>) merged.flattenedIterators().get(0);
//    assertEquals(ts_start, it.next().timestamp().msEpoch());
//    long ts = 0;
//    int dps = 0;
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      ts = it.next().timestamp().msEpoch();
//      ++dps;
//    }
//    assertEquals(ts_end, ts);
//    assertEquals(48, dps);
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void mergeSlicedResultsMultipleResultsGapInMiddle() throws Exception {
//    final List<IteratorGroups> results = Lists.newArrayListWithCapacity(4);
//    results.add(IteratorTestUtils
//        .generateData(ts_start, 1493946000000L, 0, 300000));
//    results.add(IteratorTestUtils
//        .generateData(1493946000000L, 1493949600000L, 1, 300000));
//    //results.add(IteratorTestUtils
//    //    .generateData(1493949600000L, 1493953200000L, 2, 300000));
//    results.add(null);
//    results.add(IteratorTestUtils
//        .generateData(1493953200000L, ts_end, 3, 300000));
//    
//    final IteratorGroups merged = planner.mergeSlicedResults(
//        Lists.<IteratorGroups>newArrayList(results));
//    assertEquals(4, merged.flattenedIterators().size());
//    TimeSeriesIterator<NumericType> it = 
//        (TimeSeriesIterator<NumericType>) merged.flattenedIterators().get(0);
//    assertEquals(ts_start, it.next().timestamp().msEpoch());
//    long ts = 0;
//    int dps = 0;
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      ts = it.next().timestamp().msEpoch();
//      ++dps;
//    }
//    assertEquals(ts_end, ts);
//    assertEquals(37, dps);
//  }
//  
//  @Test
//  public void mergeSlicedResultsMultipleResultsOrderException() throws Exception {
//    final List<IteratorGroups> results = Lists.newArrayListWithCapacity(4);
//    results.add(IteratorTestUtils
//        .generateData(ts_start, 1493946000000L, 0, 300000));
//    results.add(IteratorTestUtils
//        .generateData(1493946000000L, 1493949600000L, 1, 300000));
//    results.add(IteratorTestUtils
//        .generateData(1493946000000L, 1493949600000L, 1, 300000));
//    results.add(IteratorTestUtils
//        .generateData(1493953200000L, ts_end, 3, 300000));
//    try {
//      planner.mergeSlicedResults(Lists.<IteratorGroups>newArrayList(results));
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//  
//  @Test
//  public void mergeSlicedResultsEmpty() throws Exception {
//    final IteratorGroups results = 
//        planner.mergeSlicedResults(Lists.<IteratorGroups>newArrayList());
//    assertTrue(results.flattenedIterators().isEmpty());
//  }
//  
//  @Test
//  public void mergeSlicedResultsAllNulls() throws Exception {
//    IteratorGroups results = planner.mergeSlicedResults(
//        Lists.<IteratorGroups>newArrayList(null, null, null));
//    assertTrue(results.flattenedIterators().isEmpty());
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void mergeSlicedResultsIllegalArgument() throws Exception {
//    planner.mergeSlicedResults(null);
//  }
//
//  @SuppressWarnings("unchecked")
//  @Test
//  public void sliceResult() throws Exception {
//    final IteratorGroups result = IteratorTestUtils
//        .generateData(ts_start, ts_end, 0, 300000);
//    
//    final List<IteratorGroups> slices = planner.sliceResult(result, 0, 2);
//    assertEquals(3, slices.size());
//    assertEquals(-1, slices.get(0).order());
//    assertEquals(-1, slices.get(1).order());
//    assertEquals(-1, slices.get(2).order());
//    
//    assertEquals(4, slices.get(0).flattenedIterators().size());
//    TimeSeriesIterator<NumericType> it = 
//        (TimeSeriesIterator<NumericType>) slices.get(0).flattenedIterators().get(0);
//    assertEquals(ts_start, it.startTime().msEpoch());
//    assertEquals(1493946000000L, it.endTime().msEpoch());
//    
//    int dps = 0;
//    long last_ts = ts_start;
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      TimeSeriesValue<NumericType> v = it.next();
//      assertEquals(last_ts, v.timestamp().msEpoch());
//      assertEquals(last_ts, v.value().longValue());
//      ++dps;
//      last_ts += 300000;
//    }
//    assertEquals(13, dps);
//    
//    TimeSeriesId last_id = it.id();
//    it = (TimeSeriesIterator<NumericType>) slices.get(1).flattenedIterators().get(0);
//    assertSame(last_id, it.id());
//    dps = 0;
//    last_ts -= 300000; 
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      TimeSeriesValue<NumericType> v = it.next();
//      assertEquals(last_ts, v.timestamp().msEpoch());
//      assertEquals(last_ts, v.value().longValue());
//      ++dps;
//      last_ts += 300000;
//    }
//    assertEquals(13, dps);
//    
//    it = (TimeSeriesIterator<NumericType>) slices.get(2).flattenedIterators().get(0);
//    assertSame(last_id, it.id());
//    dps = 0;
//    last_ts -= 300000; 
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      TimeSeriesValue<NumericType> v = it.next();
//      assertEquals(last_ts, v.timestamp().msEpoch());
//      assertEquals(last_ts, v.value().longValue());
//      ++dps;
//      last_ts += 300000;
//    }
//    assertEquals(13, dps);
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void sliceResultEmptySource() throws Exception {
//    final IteratorGroups result = new DefaultIteratorGroups();
//    result.addIterator(IteratorTestUtils.GROUP_A, 
//        new NumericMillisecondShard(IteratorTestUtils.ID_A, 
//            new MillisecondTimeStamp(ts_start), 
//            new MillisecondTimeStamp(ts_end)));
//    result.addIterator(IteratorTestUtils.GROUP_A, 
//        new NumericMillisecondShard(IteratorTestUtils.ID_B, 
//            new MillisecondTimeStamp(ts_start), 
//            new MillisecondTimeStamp(ts_end)));
//    result.addIterator(IteratorTestUtils.GROUP_B, 
//        new NumericMillisecondShard(IteratorTestUtils.ID_A, 
//            new MillisecondTimeStamp(ts_start), 
//            new MillisecondTimeStamp(ts_end)));
//    result.addIterator(IteratorTestUtils.GROUP_B, 
//        new NumericMillisecondShard(IteratorTestUtils.ID_B, 
//            new MillisecondTimeStamp(ts_start), 
//            new MillisecondTimeStamp(ts_end)));
//    
//    final List<IteratorGroups> slices = planner.sliceResult(result, 0, 2);
//    assertEquals(3, slices.size());
//    assertEquals(-1, slices.get(0).order());
//    assertEquals(-1, slices.get(1).order());
//    assertEquals(-1, slices.get(2).order());
//    
//    assertEquals(4, slices.get(0).flattenedIterators().size());
//    TimeSeriesIterator<NumericType> it = 
//        (TimeSeriesIterator<NumericType>) slices.get(0).flattenedIterators().get(0);
//    assertEquals(ts_start, it.startTime().msEpoch());
//    assertEquals(1493946000000L, it.endTime().msEpoch());
//    assertEquals(IteratorStatus.END_OF_DATA, it.status());
//    
//    TimeSeriesId last_id = it.id();
//    it = (TimeSeriesIterator<NumericType>) slices.get(1).flattenedIterators().get(0);
//    assertSame(last_id, it.id());
//    assertEquals(1493949600000L, it.endTime().msEpoch());
//    assertEquals(IteratorStatus.END_OF_DATA, it.status());
//    
//    it = (TimeSeriesIterator<NumericType>) slices.get(2).flattenedIterators().get(0);
//    assertSame(last_id, it.id());
//    assertEquals(1493953200000L, it.endTime().msEpoch());
//    assertEquals(IteratorStatus.END_OF_DATA, it.status());
//  }
//  
//  @Test
//  public void sliceResultIllegalArguments() throws Exception {
//    final IteratorGroups result = IteratorTestUtils
//        .generateData(ts_start, ts_end, 0, 300000);
//    
//    try {
//      planner.sliceResult(null, 0, 2);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      planner.sliceResult(result, -1, 2);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      planner.sliceResult(result, 0, 5);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      planner.sliceResult(result, 2, 1);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      planner.sliceResult(result, 0, -1);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
}
