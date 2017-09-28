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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.TimeoutException;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.IteratorTestUtils;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.StorageQueryExecutor.Config;
import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.utils.JSON;

public class TestStorageQueryExecutor extends BaseExecutorTest {
  
  private MockDownstream<IteratorGroups> execution;
  private TimeSeriesDataStore store;
  private TimeSeriesQuery query;
  private QueryContext context;
  private Config config;
  private long ts_start;
  private long ts_end;
  
  @Before
  public void beforeLocal() {
    node = mock(ExecutionGraphNode.class);
    store = mock(TimeSeriesDataStore.class);
    context = mock(QueryContext.class);
    config = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor")
        .build();
    
    when(node.getDefaultConfig()).thenReturn(config);
    when(node.graph()).thenReturn(graph);
    when(registry.getPlugin(eq(TimeSeriesDataStore.class), anyString()))
      .thenReturn(store);
    
    ts_start = 1483228800000L;
    ts_end = 1483236000000L;
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(ts_start))
            .setEnd(Long.toString(ts_end)))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            .addFilter(TagVFilter.newBuilder()
                .setFilter("PHX")
                .setType("literal_or")
                .setTagk("dc")
                )
            )
        .build();
    execution = new MockDownstream<IteratorGroups>(query);
    when(store.runTimeSeriesQuery(context, query, span)).thenReturn(execution);
  }
  
  @Test
  public void ctor() throws Exception {
    try {
      new StorageQueryExecutor<IteratorGroups>(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no default config
    node = mock(ExecutionGraphNode.class);
    try {
      new StorageQueryExecutor<IteratorGroups>(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no such store
    node = mock(ExecutionGraphNode.class);
    when(node.getDefaultConfig()).thenReturn(config);
    when(registry.getPlugin(eq(TimeSeriesDataStore.class), anyString()))
      .thenReturn(null);
    try {
      new StorageQueryExecutor<IteratorGroups>(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void executeGoodQuery() throws Exception {
    final StorageQueryExecutor<IteratorGroups> executor = 
        new StorageQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(1, executor.outstandingRequests().size());
    
    final IteratorGroups data = 
        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
    execution.callback(data);
    
    final IteratorGroups results = exec.deferred().join(1);
    assertEquals(4, results.flattenedIterators().size());
    
    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
    assertEquals(2, group.flattenedIterators().size());
    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
    
    TimeSeriesIterator<NumericType> iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
    assertEquals(ts_start, iterator.startTime().msEpoch());
    assertEquals(ts_end, iterator.endTime().msEpoch());
    long ts = ts_start;
    int count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += 300000;
      ++count;
    }
    assertEquals(25, count);
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
    assertEquals(ts_start, iterator.startTime().msEpoch());
    assertEquals(ts_end, iterator.endTime().msEpoch());
    ts = ts_start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += 300000;
      ++count;
    }
    assertEquals(25, count);
    
    group = results.group(IteratorTestUtils.GROUP_B);
    assertEquals(2, group.flattenedIterators().size());
    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
    assertEquals(ts_start, iterator.startTime().msEpoch());
    assertEquals(ts_end, iterator.endTime().msEpoch());
    ts = ts_start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += 300000;
      ++count;
    }
    assertEquals(25, count);
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
    assertEquals(ts_start, iterator.startTime().msEpoch());
    assertEquals(ts_end, iterator.endTime().msEpoch());
    ts = ts_start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += 300000;
      ++count;
    }
    assertEquals(25, count);
    
    verify(store, times(1)).runTimeSeriesQuery(context, query, span);
    assertFalse(execution.cancelled);
    assertEquals(0, executor.outstandingRequests().size());
  }
  
  @Test
  public void executeException() throws Exception {
    final StorageQueryExecutor<IteratorGroups> executor = 
        new StorageQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(1, executor.outstandingRequests().size());
    
    execution.callback(new IllegalStateException("Boo!"));
    
    try {
      exec.deferred().join(1);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
        
    verify(store, times(1)).runTimeSeriesQuery(context, query, span);
    assertFalse(execution.cancelled);
    assertEquals(0, executor.outstandingRequests().size());
  }
  
  @Test
  public void executeCancel() throws Exception {
    final StorageQueryExecutor<IteratorGroups> executor = 
        new StorageQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(1, executor.outstandingRequests().size());
    
    execution.cancel();
    
    try {
      exec.deferred().join(1);
      fail("Expected QueryExecutionCanceled");
    } catch (QueryExecutionCanceled e) { }
        
    verify(store, times(1)).runTimeSeriesQuery(context, query, span);
    assertTrue(execution.cancelled);
    assertEquals(0, executor.outstandingRequests().size());
  }
  
  @Test
  public void executeClose() throws Exception {
    final StorageQueryExecutor<IteratorGroups> executor = 
        new StorageQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(1, executor.outstandingRequests().size());
    
    executor.close().join();
    

    verify(store, times(1)).runTimeSeriesQuery(context, query, span);
    assertTrue(execution.cancelled);
    assertEquals(0, executor.outstandingRequests().size());
  }
  
  @Test
  public void builder() throws Exception {
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"executorType\":\"StorageQueryExecutor\""));
    assertTrue(json.contains("\"storageId\":\"MockStore\""));
    assertTrue(json.contains("\"executorId\":\"LocalStore\""));
    
    json = "{\"executorType\":\"StorageQueryExecutor\",\"storageId\":"
        + "\"MockStore\",\"executorId\":\"LocalStore\"}";
    config = JSON.parseToObject(json, Config.class);
    assertEquals("StorageQueryExecutor", config.executorType());
    assertEquals("MockStore", config.getStorageId());
    assertEquals("LocalStore", config.getExecutorId());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Config c1 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor")
        .build();
    
    Config c2 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setStorageId("RemoteStore") // <-- Diff
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("DiffStore") // <-- Diff
        .setExecutorType("StorageQueryExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor2") // <-- Diff
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
}
