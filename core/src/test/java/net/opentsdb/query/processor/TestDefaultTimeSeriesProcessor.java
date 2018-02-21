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
package net.opentsdb.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;

public class TestDefaultTimeSeriesProcessor {
  private DefaultTSDB tsdb;
  private TimeSeriesGroupId group_id;
  private TimeSeriesStringId id_a;
  private TimeSeriesStringId id_b;
  
  private List<List<MutableNumericType>> data_a;
  private List<List<MutableNumericType>> data_b;
  
  private MockNumericIterator it_a;
  private MockNumericIterator it_b;
  
  private QueryContext context;
  private TimeSeriesProcessor processor;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    group_id = new SimpleStringGroupId("Dothraki");
    id_a = BaseTimeSeriesId.newBuilder()
        .setAlias("Khaleesi")
        .setMetric("Khalasar")
        .build();
    id_b = BaseTimeSeriesId.newBuilder()
        .setAlias("Drogo")
        .setMetric("Khalasar")
        .build();
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_a.add(set);

    data_b = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_b.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_b.add(set);
    
    it_a = spy(new MockNumericIterator(id_a));
    it_a.data = data_a;
    
    it_b = spy(new MockNumericIterator(id_b));
    it_b.data = data_b;
    
    context = new DefaultQueryContext(tsdb, mock(ExecutionGraph.class));
    processor = new DefaultTimeSeriesProcessor(context);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void standardOperation() throws Exception {
    assertNull(it_a.processor);
    assertNull(it_b.processor);
       
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(1000, context.nextTimestamp().msEpoch());
    
    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(2, iterators.size());
    // ordering maintained.
    assertSame(id_a, iterators.get(0).id());
    assertSame(id_b, iterators.get(1).id());
    
    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
    assertNull(context.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, times(1)).initialize();
    verify(it_b, times(1)).initialize();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void chunkEndedEarlyBothRecover() throws Exception {
    assertNull(it_a.processor);
    assertNull(it_b.processor);
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_a.add(set);
    it_a.data = data_a;

    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());
    
    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(2, iterators.size());
    
    // ordering maintained.
    assertSame(id_a, iterators.get(0).id());
    assertSame(id_b, iterators.get(1).id());

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA,context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
    assertNull(context.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, times(1)).initialize();
    verify(it_b, times(1)).initialize();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void chunkEndedEarlySameRecoversOtherUnaligned() throws Exception {
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_a.add(set);
    it_a.data = data_a;
    
    data_b = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_b.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_b.add(set);
    it_b.data = data_b;
    
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(2, iterators.size());
    // ordering maintained.
    assertSame(id_a, iterators.get(0).id());
    assertSame(id_b, iterators.get(1).id());

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>)iterators.get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
    assertNull(context.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, times(1)).initialize();
    verify(it_b, times(1)).initialize();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void chunkEndedEarlyOtherRecoversSameUnaligned() throws Exception {
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_a.add(set);
    it_a.data = data_a;

    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(2, iterators.size());
    // ordering maintained.
    assertSame(id_a, iterators.get(0).id());
    assertSame(id_b, iterators.get(1).id());

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
    assertNull(context.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, times(1)).initialize();
    verify(it_b, times(1)).initialize();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state1() throws Exception {
    ProcessorTestsHelpers.setState1(it_a, it_b);
    
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();

    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state2() throws Exception {
    ProcessorTestsHelpers.setState2(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state3() throws Exception {
    ProcessorTestsHelpers.setState3(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state4() throws Exception {
    ProcessorTestsHelpers.setState4(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state5() throws Exception {
    ProcessorTestsHelpers.setState5(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state6() throws Exception {
    ProcessorTestsHelpers.setState6(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state7() throws Exception {
    ProcessorTestsHelpers.setState7(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state8() throws Exception {
    ProcessorTestsHelpers.setState8(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state9() throws Exception {
    ProcessorTestsHelpers.setState9(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());

    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));

    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state10() throws Exception {
    ProcessorTestsHelpers.setState10(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));

    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state11() throws Exception {
    ProcessorTestsHelpers.setState11(it_a, it_b);
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void exceptionStatusOnNext() throws Exception {
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    // inject an exception
    it_b.ex = new RuntimeException("Boo!");

    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertNull(iterators.get(1).next());
    assertEquals(IteratorStatus.EXCEPTION, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void exceptionThrowOnNext() throws Exception {
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());

    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    // inject an exception
    it_b.ex = new RuntimeException("Boo!");
    it_b.throw_ex = true;

    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) iterators.get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    try {
      iterators.get(1).next();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
    assertEquals(IteratorStatus.EXCEPTION, context.advance());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getCopy() throws Exception {
    // add series
    processor.addSeries(group_id, it_a);
    processor.addSeries(group_id, it_b);
    
    assertNull(context.initialize().join());
    
    final List<TimeSeriesIterator<?>> iterators = 
        processor.iterators().flattenedIterators();
    assertEquals(2, iterators.size());
    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterators.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    
    // copy!
    final QueryContext ctx2 = new DefaultQueryContext(tsdb, 
        mock(ExecutionGraph.class));
    final TimeSeriesProcessor copy = processor.getClone(ctx2);
    final List<TimeSeriesIterator<?>> its_copy = 
        copy.iterators().flattenedIterators();
    assertNull(ctx2.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, ctx2.advance());
    v = (TimeSeriesValue<NumericType>) its_copy.get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) its_copy.get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(IteratorStatus.HAS_DATA, ctx2.advance());
    
    assertNotSame(iterators.get(0), its_copy.get(0));
    assertNotSame(iterators.get(1), its_copy.get(1));
  }
  
}
