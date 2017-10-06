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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.processor.DefaultTimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessor;

public class TestSlicedTimeSeriesIterator {

  private TSDB tsdb;
  private ExecutionGraph execution_graph;
  private TimeSeriesId id; 
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    execution_graph = mock(ExecutionGraph.class);
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
  }
  
  @Test
  public void addIterator() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    assertNull(iterator.id());
    assertNull(iterator.type());
    assertNull(iterator.startTime());
    assertNull(iterator.endTime());
    assertNull(iterator.peek());
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    iterator.addIterator(shard);
    assertSame(id, iterator.id());
    assertSame(shard.type(), iterator.type());
    assertEquals(start, iterator.startTime());
    assertEquals(end, iterator.endTime());
    
    // same can't be added.
    try {
      iterator.addIterator(shard);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // overlapping is evil.
    start = new MillisecondTimeStamp(1486045850000L);
    end = new MillisecondTimeStamp(1486045950000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    try {
      iterator.addIterator(shard);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // skipping is ok...
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    iterator.addIterator(shard);
    assertEquals(end, iterator.endTime());
    
    // ...but not going back in time
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    try {
      iterator.addIterator(shard);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // null
    try {
      iterator.addIterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void peek() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    assertNull(iterator.id());
    assertNull(iterator.type());
    assertNull(iterator.startTime());
    assertNull(iterator.endTime());
    assertNull(iterator.peek());
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    iterator.addIterator(shard);
    assertSame(id, iterator.id());
    assertSame(shard.type(), iterator.type());
    assertEquals(start, iterator.startTime());
    assertEquals(end, iterator.endTime());
    
    TimeSeriesValue<NumericType> v = iterator.peek();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
  }
  
  @Test
  public void getCopy() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end, 0);
    shard_a.add(1486045801000L, 1);
    shard_a.add(1486045871000L, 2);
    shard_a.add(1486045900000L, 3);
    iterator.addIterator(shard_a);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end, 1);
    shard_b.add(1486045901000L, 4);
    shard_b.add(1486045971000L, 5);
    iterator.addIterator(shard_b);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    TimeSeriesIterator<NumericType> clone = iterator.getShallowCopy(null);
    assertNotSame(clone, iterator);
    
    assertEquals(IteratorStatus.HAS_DATA, clone.status());
    v = clone.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
  }
  
  @Test
  public void empty() throws Exception {
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void initialize() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    assertNull(iterator.initialize().join());
    
    TimeSeriesIterator<NumericType> mock_a = mock(TimeSeriesIterator.class);
    when(mock_a.startTime()).thenReturn(new MillisecondTimeStamp(0L));
    when(mock_a.endTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(mock_a.order()).thenReturn(0);
    when(mock_a.initialize()).thenReturn(Deferred.fromResult(null));
    
    TimeSeriesIterator<NumericType> mock_b = mock(TimeSeriesIterator.class);
    when(mock_b.startTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(mock_b.endTime()).thenReturn(new MillisecondTimeStamp(2000L));
    when(mock_b.order()).thenReturn(1);
    when(mock_b.initialize()).thenReturn(Deferred.fromResult(null));
    InOrder order = Mockito.inOrder(mock_a, mock_b);

    iterator.addIterator(mock_a);
    iterator.addIterator(mock_b);
    
    assertNull(iterator.initialize().join());
    order.verify(mock_b).initialize();
    order.verify(mock_a).initialize();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void initializeException() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeSeriesIterator<NumericType> mock_a = mock(TimeSeriesIterator.class);
    when(mock_a.startTime()).thenReturn(new MillisecondTimeStamp(0L));
    when(mock_a.endTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(mock_a.order()).thenReturn(0);
    when(mock_a.initialize()).thenReturn(Deferred.fromResult(null));
    
    TimeSeriesIterator<NumericType> mock_b = mock(TimeSeriesIterator.class);
    when(mock_b.startTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(mock_b.endTime()).thenReturn(new MillisecondTimeStamp(2000L));
    when(mock_b.order()).thenReturn(1);
    when(mock_b.initialize()).thenReturn(
        Deferred.fromError(new IllegalStateException("Boo!")));

    iterator.addIterator(mock_a);
    iterator.addIterator(mock_b);
    try {
      iterator.initialize().join();
      fail("Expected IllegalStateException"); 
    } catch (IllegalStateException e) { }
    verify(mock_b, times(1)).initialize();
    verify(mock_a, never()).initialize();
  }
  
  @Test
  public void oneIterator() throws Exception {
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void twoIteratorsNoOverlap() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045901000L, 4);
    shard.add(1486045971000L, 5);
    
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045901000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045971000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }

  @Test
  public void twoIteratorsOverlap() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 100); // <-- ignored
    shard.add(1486045901000L, 4);
    shard.add(1486045971000L, 5);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045901000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045971000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }

  @Test
  public void twoIteratorsSkipMiddle() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    iterator.addIterator(shard);
    
    // nothing to see here.
    //start = new MillisecondTimeStamp(1486045900000L);
    //end = new MillisecondTimeStamp(1486046000000L);
    //shard = new NumericMillisecondShard(id, start, end, 1);
    //shard.add(1486045900000L, 100);
    //iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void threeIteratorsEmptyMiddle() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void threeIteratorsMiddleHasDupe() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 100);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void threeIteratorsMiddleHasDupes() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 100);
    shard.add(1486046000000L, 4);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046000000L, 200);
    shard.add(1486046010000L, 5);
    shard.add(1486046071000L, 6);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046000000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void threeIteratorsEmptyStart() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 1);
    shard.add(1486045901000L, 2);
    shard.add(1486045907000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045901000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045907000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void threeIteratorsEmptyTail() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045800000L, 1);
    shard.add(1486045801000L, 2);
    shard.add(1486045807000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 4);
    shard.add(1486045907000L, 5);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045800000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045807000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045907000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }

  @Test
  public void fourIteratorsEmptyStart() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    shard.add(1486046110000L, 4);
    shard.add(1486046171000L, 5);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486046110000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046171000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void fourIteratorsEmptyMiddle() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    shard.add(1486046110000L, 4);
    shard.add(1486046171000L, 5);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046110000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486046171000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void fourIteratorsEmptyTail() throws Exception {
    SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    iterator.addIterator(shard);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }

  @Test
  public void singleMockIterator() throws Exception {
    final List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(3);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(7000), 7));
    data.add(set);
    
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    final MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    iterator.addIterator(it);
    
    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (iterator.status() != IteratorStatus.END_OF_DATA) {
      while (iterator.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (iterator.status() == IteratorStatus.END_OF_CHUNK) {
        assertNull(iterator.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(7, iterations);
    assertEquals(2, fetched);
  }
  
  @Test
  public void twoMockIterators() throws Exception {
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    iterator.addIterator(it);

    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (iterator.status() != IteratorStatus.END_OF_DATA) {
      while (iterator.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (iterator.status() == IteratorStatus.END_OF_CHUNK) {
        assertNull(iterator.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(6, iterations);
    assertEquals(0, fetched);
  }
  
  @Test
  public void twoMockIteratorsWithOverlap() throws Exception {
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(4);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 100));
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    iterator.addIterator(it);

    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (iterator.status() != IteratorStatus.END_OF_DATA) {
      while (iterator.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (iterator.status() == IteratorStatus.END_OF_CHUNK) {
        assertNull(iterator.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(6, iterations);
    assertEquals(0, fetched);
  }

  @Test
  public void twoMockIteratorsFetchBeforeOverlap() throws Exception {
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(4);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 100));
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    iterator.addIterator(it);

    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (iterator.status() != IteratorStatus.END_OF_DATA) {
      while (iterator.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (iterator.status() == IteratorStatus.END_OF_CHUNK) {
        assertNull(iterator.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(6, iterations);
    assertEquals(1, fetched);
  }
  
  @Test
  public void twoMockIteratorsExceptionAfterFetch() throws Exception {
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(4);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 100));
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    iterator.addIterator(it);

    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (iterator.status() != IteratorStatus.END_OF_DATA) {
      while (iterator.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (iterator.status() == IteratorStatus.END_OF_CHUNK) {
        assertNull(iterator.fetchNext().join());
        it.ex = new IllegalStateException("Boo!");
        ++fetched;
      } else {
        break;
      }
    }
    // stop after the first 3.
    assertEquals(IteratorStatus.EXCEPTION, iterator.status());
    assertEquals(3, iterations);
    assertEquals(1, fetched);
  }
  
  @Test
  public void twoMockIteratorsExceptionDuringIteration() throws Exception {
    final SlicedTimeSeriesIterator<NumericType> iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    iterator.addIterator(it);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    it.ex = new IllegalStateException("Boo!");
    
    assertEquals(IteratorStatus.EXCEPTION, iterator.status());
    try {
      iterator.next();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }

  @Test
  public void twoIteratorsWithContextAndOverlap() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 100); // <-- ignored
    shard.add(1486045901000L, 4);
    shard.add(1486045971000L, 5);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(1486045801000L, context.nextTimestamp().msEpoch());
    
    // fill as if another series had an earlier dp.
    context.updateContext(IteratorStatus.HAS_DATA, 
        new MillisecondTimeStamp(1486045800000L));
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(1486045800000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045800000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    // real value
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045800000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045801000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1, v.value().longValue());
    
    // fill in middle
    context.updateContext(IteratorStatus.HAS_DATA, 
        new MillisecondTimeStamp(1486045802000L));
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045801000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045802000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045802000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    // real value
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045802000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045871000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(2, v.value().longValue());
    
    // real value
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045871000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045900000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(3, v.value().longValue());
    
    // real value
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045900000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045901000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045901000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(4, v.value().longValue());
    
    // fill in middle
    context.updateContext(IteratorStatus.HAS_DATA, 
        new MillisecondTimeStamp(1486045902000L));
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045901000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045902000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045902000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    // real value
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045902000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045971000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045971000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    // end of data.
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(1486045971000L, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void twoIteratorsWithContextSkipMiddle() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    slice_iterator.addIterator(shard);
    
    // nothing to see here.
    //start = new MillisecondTimeStamp(1486045900000L);
    //end = new MillisecondTimeStamp(1486046000000L);
    //shard = new NumericMillisecondShard(id, start, end, 1);
    //shard.add(1486045900000L, 100);
    //iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void threeIteratorsWithContextEmptyMiddle() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void threeIteratorsWithContextMiddleHasDupe() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 100);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void threeIteratorsWithContextMiddleHasDupes() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 100);
    shard.add(1486046000000L, 4);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046000000L, 200);
    shard.add(1486046010000L, 5);
    shard.add(1486046071000L, 6);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046000000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void threeIteratorsWithContextEmptyStart() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 1);
    shard.add(1486045901000L, 2);
    shard.add(1486045907000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    shard.add(1486046010000L, 4);
    shard.add(1486046071000L, 5);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045901000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045907000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046010000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046071000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void threeIteratorsWithContextEmptyTail() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045800000L, 1);
    shard.add(1486045801000L, 2);
    shard.add(1486045807000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    shard.add(1486045900000L, 4);
    shard.add(1486045907000L, 5);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045800000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045807000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045907000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void fourIteratorsWithContextEmptyStart() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    shard.add(1486046110000L, 4);
    shard.add(1486046171000L, 5);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486046110000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046171000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void fourIteratorsWithContextEmptyMiddle() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    shard.add(1486046110000L, 4);
    shard.add(1486046171000L, 5);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046110000L, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486046171000L, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void fourIteratorsWithContextEmptyTail() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045900000L);
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end, 0);
    shard.add(1486045801000L, 1);
    shard.add(1486045871000L, 2);
    shard.add(1486045900000L, 3);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486045900000L);
    end = new MillisecondTimeStamp(1486046000000L);
    shard = new NumericMillisecondShard(id, start, end, 1);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046000000L);
    end = new MillisecondTimeStamp(1486046100000L);
    shard = new NumericMillisecondShard(id, start, end, 2);
    slice_iterator.addIterator(shard);
    
    start = new MillisecondTimeStamp(1486046100000L);
    end = new MillisecondTimeStamp(1486046200000L);
    shard = new NumericMillisecondShard(id, start, end, 3);
    slice_iterator.addIterator(shard);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = iterator.next();
    assertEquals(1486045900000L, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void singleMockIteratorWithContext() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    final List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(3);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(7000), 7));
    data.add(set);
    
    final SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    final MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    slice_iterator.addIterator(it);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
      while (context.advance() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
        assertNull(context.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(7, iterations);
    assertEquals(2, fetched);
  }

  @Test
  public void twoMockIteratorsWithContext() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    final SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    slice_iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    slice_iterator.addIterator(it);

    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
      while (context.advance() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
        assertNull(context.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(6, iterations);
    assertEquals(0, fetched);
  }

  @Test
  public void twoMockIteratorsWithContextWithOverlap() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    final SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    slice_iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(4);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 100));
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    slice_iterator.addIterator(it);

    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
      while (context.advance() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
        assertNull(context.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(6, iterations);
    assertEquals(0, fetched);
  }
  
  @Test
  public void twoMockIteratorsWithContextFetchBeforeOverlap() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    final SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    slice_iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(4);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 100));
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    slice_iterator.addIterator(it);

    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
      while (context.advance() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
        assertNull(context.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(6, iterations);
    assertEquals(1, fetched);
  }

  @Test
  public void twoMockIteratorsWithContextExceptionAfterFetch() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    final SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    slice_iterator.addIterator(it);
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(4);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 100));
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data.add(set);
    
    it = new MockNumericIterator(id, 1);
    it.data = data;
    slice_iterator.addIterator(it);

    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
      while (context.advance() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
        assertNull(context.fetchNext().join());
        it.ex = new IllegalStateException("Boo!");
        ++fetched;
      } else {
        break;
      }
    }
    // stop after the first 3.
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(3, iterations);
    assertEquals(1, fetched);
  }

  @Test
  public void twoMockIteratorsWithContextExceptionDuringIteration() throws Exception {
    QueryContext context = new DefaultQueryContext(tsdb, execution_graph);
    final SlicedTimeSeriesIterator<NumericType> slice_iterator = 
        new SlicedTimeSeriesIterator<NumericType>();
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    
    MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    slice_iterator.addIterator(it);
    
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(new SimpleStringGroupId("a"), slice_iterator);
    slice_iterator.setContext(context);
    assertNull(context.initialize().join());
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
        group.iterators().flattenedIterators().get(0);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    it.ex = new IllegalStateException("Boo!");
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    try {
      iterator.next();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
}
