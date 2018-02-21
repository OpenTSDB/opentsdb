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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

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
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.processor.DefaultTimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessor;

/**
 * Yes we are testing a mock. Gotta make sure it's happy.
 */
public class TestMockNumericIterator {
  private DefaultTSDB tsdb;
  private ExecutionGraph execution_graph;
  private QueryContext context;
  private TimeSeriesProcessor processor;
  private TimeSeriesGroupId group;
  private TimeSeriesStringId id;
  private List<List<MutableNumericType>> data;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    execution_graph = mock(ExecutionGraph.class);
    id = BaseTimeSeriesId.newBuilder()
        .setAlias("Khalisi")
        .setMetric("Khalasar")
        .build();
    
    data = Lists.newArrayListWithCapacity(3);
    
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
    
    context = new DefaultQueryContext(tsdb, execution_graph);
    processor = new DefaultTimeSeriesProcessor(context);
    group = new SimpleStringGroupId("Freys");
  }
  
  @Test
  public void iterator() throws Exception {
    MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    processor.addSeries(group, it);
    
    assertNull(context.initialize().join());
    assertSame(id, it.id());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(1000, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(1000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // next
    TimeSeriesValue<NumericType> v = it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1, v.value().longValue());
    
    // post advance
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1000, context.syncTimestamp().msEpoch());
    assertEquals(2000, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(2000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // next
    v = it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(2, v.value().longValue());
    
    // post advance
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(2000, context.syncTimestamp().msEpoch());
    assertEquals(3000, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(3000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // next
    v = it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(3, v.value().longValue());
    
    // post advance
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.nextStatus());
    assertEquals(3000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // confirm another call to advance leaves us as-is.
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // fetch
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(4000, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(4000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // next
    v = it.next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(4, v.value().longValue());
    
    // post advance
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(4000, context.syncTimestamp().msEpoch());
    assertEquals(5000, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(5000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // next
    v = it.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    // post advance
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(5000, context.syncTimestamp().msEpoch());
    assertEquals(6000, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(6000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // next
    v = it.next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(6, v.value().longValue());
    
    // post advance
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_CHUNK, context.nextStatus());
    assertEquals(6000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // fetch
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(6000, context.syncTimestamp().msEpoch());
    assertEquals(7000, context.nextTimestamp().msEpoch());
    
    // advance
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(7000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // next
    v = it.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(7, v.value().longValue());
    
    // post advance
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(7000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // confirm calls to advance just return EOD
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    v = it.next();
    assertEquals(Long.MAX_VALUE, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
  }

  @Test
  public void iteratorWithoutContext() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id, 0);
    it.data = data;
    
    int fetched = 0;
    int iterations = 0;
    long timestamp = 1000L;
    int value = 1;
    
    while (it.status() != IteratorStatus.END_OF_DATA) {
      while (it.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = it.next();
        assertEquals(timestamp, v.timestamp().msEpoch());
        assertEquals(value++, v.value().longValue());
        timestamp += 1000;
        ++iterations;
      }
      if (it.status() == IteratorStatus.END_OF_CHUNK) {
        assertNull(it.fetchNext().join());
        ++fetched;
      } else {
        break;
      }
    }
    assertEquals(7, iterations);
    assertEquals(2, fetched);
  }
  
  @Test
  public void initializeException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    try {
      it.initialize().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
  @Test
  public void nextException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    it.throw_ex = true;
    try {
      it.next();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
  @Test
  public void fetchNextException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    try {
      it.fetchNext().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
  @Test
  public void getCopy() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    processor.addSeries(group, it);
    
    assertNull(context.initialize().join());
    assertSame(id, it.id());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.nextStatus());
    // left the parent in an END_OF_CHUNK state to verify the copy starts over.
    
    final QueryContext ctx2 = new DefaultQueryContext(tsdb, execution_graph);
    final TimeSeriesProcessor processor2 = new DefaultTimeSeriesProcessor(ctx2);
    
    final MockNumericIterator copy = (MockNumericIterator) it.getShallowCopy(ctx2);
    processor2.addSeries(group, copy);
    
    assertNull(ctx2.initialize().join());
    assertNotSame(copy, it);
    
    assertEquals(IteratorStatus.HAS_DATA, ctx2.advance());
    v = copy.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
  }
  
  @Test
  public void closeException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    try {
      it.close().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
}
