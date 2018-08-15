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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TagVFilter;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.stats.MockStats;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.storage.MockDataStore;
import net.opentsdb.storage.MockDataStoreFactory;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDBV2QueryContextBuilder.class })
public class TestTSDBV2QueryContextBuilder {
  private static MockDataStoreFactory STORE_FACTORY;
  private static QueryDataSourceFactory SOURCE_FACTORY;
  private static MockTSDB TSDB;
  
  private TimeSeriesQuery query;
  private QuerySink sink;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    TSDB.registry = mock(DefaultRegistry.class);
    STORE_FACTORY = new MockDataStoreFactory();
    TSDB.config.register("MockDataStore.timestamp", 1483228800000L, false, "UT");
    
    SingleQueryNodeFactory factory = mock(SingleQueryNodeFactory.class);
    when(factory.newNode(any(QueryPipelineContext.class), anyString()))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          return new PassThrough(factory, null, 
              (String) invocation.getArguments()[1]);
        }
      });
    when(factory.newNode(any(QueryPipelineContext.class), anyString(), any(QueryNodeConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          return new PassThrough(factory, 
              (QueryPipelineContext) invocation.getArguments()[0], 
              (String) invocation.getArguments()[1]);
        }
      });
    when(TSDB.registry.getQueryNodeFactory(anyString()))
      .thenAnswer(new Answer<QueryNodeFactory>() {
        @Override
        public QueryNodeFactory answer(InvocationOnMock invocation)
            throws Throwable {
          String id = (String) invocation.getArguments()[0];
          if (id.toLowerCase().equals("datasource")) {
            return SOURCE_FACTORY;
          }
          
          return factory;
        }
      });
    when(((DefaultRegistry) TSDB.registry).getDefaultStore())
      .thenReturn(new MockDataStore(TSDB, null));
    SOURCE_FACTORY = new QueryDataSourceFactory();
    SOURCE_FACTORY.initialize(TSDB).join();
  }
  
  @Before
  public void before() throws Exception {
    sink = mock(QuerySink.class);
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000L;
    
    query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
  }
  
  @Test
  public void buildWithoutStats() throws Exception {
    final QueryContext context = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
    
    assertEquals(QueryMode.SINGLE, context.mode());
    assertSame(sink, context.sinks().iterator().next());
    assertNull(context.stats());
  }
  
  @Test
  public void buildWithStats() throws Exception {
    final MockTrace tracer = new MockTrace();
    final QueryStats stats = new MockStats(tracer, tracer.newSpan("mock").start());
    
    final QueryContext context = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .setStats(stats)
        .build();
    
    assertEquals(QueryMode.SINGLE, context.mode());
    assertSame(sink, context.sinks().iterator().next());
    assertSame(stats, context.stats());
    assertEquals(3, tracer.spans.size());
    context.close();
    assertEquals(5, tracer.spans.size());
  }
  
  @Test
  public void buildErrors() throws Exception {
    try {
      TSDBV2QueryContextBuilder.newBuilder(null)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TSDBV2QueryContextBuilder.newBuilder(TSDB)
        //.setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        //.setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        //.addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void querySingleOneMetric() throws Exception {
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          
          assertEquals("sys.cpu.user", ((TimeSeriesStringId) ts.id()).metric());
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals((end_ts - start_ts) / MockDataStore.INTERVAL, values);
        }
        next.close();
        on_next++;
        if (on_next == 1) {
          call_limit.callback(null);
        }
      }

      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Long.toString(start_ts))
        .setEnd(Long.toString(end_ts))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("DataSource")
                .setConfig(QuerySourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.cpu.user")
                        .build())
                    .setFilterId("f1")
                    .build())
                .build())
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("GroupBy")
                .addSource("DataSource")
                .setConfig(GroupByConfig.newBuilder()
                    .setAggregator("sum")
                    .addTagKey("host")
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                      .setDataType(NumericType.TYPE.toString())
                      .build())
                    .build()))
            .build())
        .addFilter(DefaultNamedFilter.newBuilder()
            .setId("f1")
            .setFilter(TagValueLiteralOrFilter.newBuilder()
                .setTagKey("host")
                .setFilter("web01")
                .build())
            .build())
        .addSink(listener)
        .build();
    
    QueryContext ctx = SemanticQueryContext.newBuilder()
        .setTSDB(TSDB)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .build();
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
  }
  
  @Test
  public void querySingleOneMetricNoMatch() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("metric.does.not.exist")
            .setFilter("f1")
            .setId("m1"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        assertEquals(0, next.timeSeries().size());
        next.close();
        on_next++;
        if (on_next == 1) {
          call_limit.callback(null);
        }
      }

      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void querySingleTwoMetrics() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        assertEquals(8, next.timeSeries().size());
        int[] metrics = new int[2];
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          // order is indeterminate
          if (((TimeSeriesStringId) ts.id()).metric().equals("web.requests")) {
            metrics[0]++;
          } else {
            metrics[1]++;
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals((end_ts - start_ts) / MockDataStore.INTERVAL, values);
        }
        assertEquals(4, metrics[0]);
        assertEquals(4, metrics[1]);
        next.close();
        on_next++;
        if (on_next == 1) {
          call_limit.callback(null);
        }
      }

      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void querySingleTwoMetricsNoMatch() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("no.such.metric")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("also.no.metric")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        assertEquals(0, next.timeSeries().size());
        next.close();
        on_next++;
        if (on_next == 1) {
          call_limit.callback(null);
        }
      }

      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void queryBoundedClientStream() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }
  
      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        int[] metrics = new int[2];
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          // order is indeterminate
          if (((TimeSeriesStringId) ts.id()).metric().equals("web.requests")) {
            metrics[0]++;
          } else {
            metrics[1]++;
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals(MockDataStore.ROW_WIDTH / MockDataStore.INTERVAL, values);
        }
        assertTrue((metrics[0] == 4 && metrics[1] == 0) || 
                   (metrics[0] == 0 && metrics[1] == 4));
        next.close();
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        ctx.fetchNext(null);
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_CLIENT_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }

  @Test
  public void queryBoundedClientStreamNoMatch() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("no.such.metric")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("also.no.metric")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }
  
      @Override
      public void onNext(QueryResult next) {
        assertEquals(0, next.timeSeries().size());
        next.close();
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        ctx.fetchNext(null);
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_CLIENT_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }

  @Test
  public void queryContinousClientStream() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }
  
      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        int[] metrics = new int[2];
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          // order is indeterminate
          if (((TimeSeriesStringId) ts.id()).metric().equals("web.requests")) {
            metrics[0]++;
          } else {
            metrics[1]++;
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals(MockDataStore.ROW_WIDTH / MockDataStore.INTERVAL, values);
        }
        assertTrue((metrics[0] == 4 && metrics[1] == 0) || 
                   (metrics[0] == 0 && metrics[1] == 4));
        next.close();
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        ctx.fetchNext(null);
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.CONTINOUS_CLIENT_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext(null);
    
    listener.call_limit.join(1000);
    try {
      listener.completed.join(10);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }

  @Test
  public void queryBoundedServerSyncStream() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }
  
      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        int[] metrics = new int[2];
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          // order is indeterminate
          if (((TimeSeriesStringId) ts.id()).metric().equals("web.requests")) {
            metrics[0]++;
          } else {
            metrics[1]++;
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals(MockDataStore.ROW_WIDTH / MockDataStore.INTERVAL, values);
        }
        assertTrue((metrics[0] == 4 && metrics[1] == 0) || 
                   (metrics[0] == 0 && metrics[1] == 4));
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        next.close(); // triggers the next response
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_SERVER_SYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void queryContinousServerSyncStream() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }
  
      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        int[] metrics = new int[2];
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          // order is indeterminate
          if (((TimeSeriesStringId) ts.id()).metric().equals("web.requests")) {
            metrics[0]++;
          } else {
            metrics[1]++;
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals(MockDataStore.ROW_WIDTH / MockDataStore.INTERVAL, values);
        }
        assertTrue((metrics[0] == 4 && metrics[1] == 0) || 
                   (metrics[0] == 0 && metrics[1] == 4));
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        next.close(); // triggers the next response
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.CONTINOUS_SERVER_SYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext(null);
    
    listener.call_limit.join(1000);
    try {
      listener.completed.join(10);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }

  @Test
  public void queryBoundedServerAsyncStream() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }
  
      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        int[] metrics = new int[2];
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          // order is indeterminate
          if (((TimeSeriesStringId) ts.id()).metric().equals("web.requests")) {
            metrics[0]++;
          } else {
            metrics[1]++;
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals(MockDataStore.ROW_WIDTH / MockDataStore.INTERVAL, values);
        }
        assertTrue((metrics[0] == 4 && metrics[1] == 0) || 
                   (metrics[0] == 0 && metrics[1] == 4));
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        next.close(); // triggers the next response
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_SERVER_ASYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext(null);
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void queryContinousServerAsyncStream() throws Exception {
    MockDataStore mds = new MockDataStore(TSDB, "Mock");
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts))
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")))
        .build().convert().build();
    
    class TestListener implements QuerySink {
      int on_next = 0;
      int on_error = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      Deferred<Object> call_limit = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }
  
      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        int[] metrics = new int[2];
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          // order is indeterminate
          if (((TimeSeriesStringId) ts.id()).metric().equals("web.requests")) {
            metrics[0]++;
          } else {
            metrics[1]++;
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals(MockDataStore.ROW_WIDTH / MockDataStore.INTERVAL, values);
        }
        on_next++;
        assertTrue((metrics[0] == 4 && metrics[1] == 0) || 
                   (metrics[0] == 0 && metrics[1] == 4));
        if (on_next == 4) {
          call_limit.callback(null);
        }
        next.close(); // triggers the next response
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = TSDBV2QueryContextBuilder.newBuilder(TSDB)
        .setQuery(query)
        .setMode(QueryMode.CONTINOUS_SERVER_ASYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext(null);
    
    listener.call_limit.join(1000);
    try {
      listener.completed.join(10);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  static class PassThrough extends AbstractQueryNode {

    public PassThrough(final QueryNodeFactory factory, 
                       final QueryPipelineContext context,
                       final String id) {
      super(factory, context, id);
    }

    @Override
    public QueryNodeConfig config() { return null; }

    @Override
    public void close() { }

    @Override
    public void onComplete(QueryNode downstream, long final_sequence,
        long total_sequences) {
      completeUpstream(final_sequence, total_sequences);
    }

    @Override
    public void onNext(QueryResult next) {
      sendUpstream(next);
    }

    @Override
    public void onError(Throwable t) {
      sendUpstream(t);
    }
    
  }
}
