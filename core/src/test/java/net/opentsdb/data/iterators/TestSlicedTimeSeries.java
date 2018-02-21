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
package net.opentsdb.data.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;

public class TestSlicedTimeSeries {

  private TimeSeriesStringId id;
  
  @Before
  public void before() throws Exception {
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("Drogo")
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    assertNull(ts.id());
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    ts.addSource(shard);
    
    assertSame(id, ts.id());
    assertEquals(1, ts.types().size());
    assertEquals(NumericType.TYPE, ts.types().iterator().next());
  }
  
  @Test
  public void oneIterator() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void twoIteratorsNoOverlap() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045901000L, 4);
    shard.add(1486045971000L, 5);
    
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045901000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045971000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }

  // TODO - check for overlap
//  @Test
//  public void twoIteratorsOverlap() throws Exception {
//    SlicedTimeSeries ts = new SlicedTimeSeries();
//    
//    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
//    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
//    
//    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
//    shard.add(1486045801000L, 1);
//    shard.add(1486045871000L, 2);
//    shard.add(1486045900000L, 3);
//    ts.addSource(shard);
//    
//    start = new MillisecondTimeStamp(1486045900000L);
//    end = new MillisecondTimeStamp(1486046000000L);
//    shard = new NumericMillisecondShard(id, start, end, 1);
//    shard.add(1486045900000L, 100); // <-- ignored
//    shard.add(1486045901000L, 4);
//    shard.add(1486045971000L, 5);
//    ts.addSource(shard);
//    
//    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
//    assertTrue(iterator.hasNext());
//    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045801000L, v.timestamp().msEpoch());
//    assertEquals(1, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045871000L, v.timestamp().msEpoch());
//    assertEquals(2, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045900000L, v.timestamp().msEpoch());
//    assertEquals(3, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045901000L, v.timestamp().msEpoch());
//    assertEquals(4, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045971000L, v.timestamp().msEpoch());
//    assertEquals(5, v.value().longValue());
//    
//    assertFalse(iterator.hasNext());
//  }

  @Test
  public void twoIteratorsSkipMiddle() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    ts.addSource(shard);
    
    // nothing to see here.
    //start = new MillisecondTimeStamp(1486045900000L);
    //end = new MillisecondTimeStamp(1486046000000L);
    //shard = new NumericMillisecondShard(id, start, end, 1);
    //shard.add(1486045900000L, 100);
    //ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void threeIteratorsEmptyMiddle() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
//  @Test
//  public void threeIteratorsMiddleHasDupe() throws Exception {
//    SlicedTimeSeries ts = new SlicedTimeSeries();
//    
//    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
//    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
//    
//    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
//    shard.add(1486045801000L, 1);
//    shard.add(1486045871000L, 2);
//    shard.add(1486045900000L, 3);
//    ts.addSource(shard);
//    
//    start = new MillisecondTimeStamp(1486045900000L);
//    end = new MillisecondTimeStamp(1486046000000L);
//    shard = new NumericMillisecondShard(id, start, end, 1);
//    shard.add(1486045900000L, 100);
//    ts.addSource(shard);
//    
//    start = new MillisecondTimeStamp(1486046000000L);
//    end = new MillisecondTimeStamp(1486046100000L);
//    shard = new NumericMillisecondShard(id, start, end, 2);
//    shard.add(1486046010000L, 4);
//    shard.add(1486046071000L, 5);
//    ts.addSource(shard);
//    
//    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
//    assertTrue(iterator.hasNext());
//    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045801000L, v.timestamp().msEpoch());
//    assertEquals(1, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045871000L, v.timestamp().msEpoch());
//    assertEquals(2, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045900000L, v.timestamp().msEpoch());
//    assertEquals(3, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486046010000L, v.timestamp().msEpoch());
//    assertEquals(4, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486046071000L, v.timestamp().msEpoch());
//    assertEquals(5, v.value().longValue());
//    
//    assertFalse(iterator.hasNext());
//  }
  
//  @Test
//  public void threeIteratorsMiddleHasDupes() throws Exception {
//    SlicedTimeSeries ts = new SlicedTimeSeries();
//    
//    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
//    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
//    
//    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
//    shard.add(1486045801000L, 1);
//    shard.add(1486045871000L, 2);
//    shard.add(1486045900000L, 3);
//    ts.addSource(shard);
//    
//    start = new MillisecondTimeStamp(1486045900000L);
//    end = new MillisecondTimeStamp(1486046000000L);
//    shard = new NumericMillisecondShard(id, start, end, 1);
//    shard.add(1486045900000L, 100);
//    shard.add(1486046000000L, 4);
//    ts.addSource(shard);
//    
//    start = new MillisecondTimeStamp(1486046000000L);
//    end = new MillisecondTimeStamp(1486046100000L);
//    shard = new NumericMillisecondShard(id, start, end, 2);
//    shard.add(1486046000000L, 200);
//    shard.add(1486046010000L, 5);
//    shard.add(1486046071000L, 6);
//    ts.addSource(shard);
//    
//    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
//    assertTrue(iterator.hasNext());
//    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045801000L, v.timestamp().msEpoch());
//    assertEquals(1, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045871000L, v.timestamp().msEpoch());
//    assertEquals(2, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045900000L, v.timestamp().msEpoch());
//    assertEquals(3, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486046000000L, v.timestamp().msEpoch());
//    assertEquals(4, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486046010000L, v.timestamp().msEpoch());
//    assertEquals(5, v.value().longValue());
//    
//    assertTrue(iterator.hasNext());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486046071000L, v.timestamp().msEpoch());
//    assertEquals(6, v.value().longValue());
//    
//    assertFalse(iterator.hasNext());
//  }
  
  @Test
  public void threeIteratorsEmptyStart() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 1);
    shard.add(1486045901000L, 2);
    shard.add(1486045907000L, 3);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045901000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045907000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void threeIteratorsEmptyTail() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045800000L, 1);
    shard.add(1486045801000L, 2);
    shard.add(1486045807000L, 3);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 4);
    shard.add(1486045907000L, 5);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045800000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045807000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045907000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }

  @Test
  public void fourIteratorsEmptyStart() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    shard.add(1486046110000L, 4);
    shard.add(1486046171000L, 5);
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046110000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046171000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fourIteratorsEmptyMiddle() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    shard.add(1486046110000L, 4);
    shard.add(1486046171000L, 5);
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046110000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486046171000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fourIteratorsEmptyTail() throws Exception {
    SlicedTimeSeries ts = new SlicedTimeSeries();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    ts.addSource(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    ts.addSource(shard);
    
    Iterator<TimeSeriesValue<?>> iterator = ts.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }

}
