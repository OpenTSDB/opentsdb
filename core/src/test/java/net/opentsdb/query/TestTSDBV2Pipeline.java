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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.storage.MockDataStore;
import net.opentsdb.utils.Config;

public class TestTSDBV2Pipeline {

  private DefaultTSDB tsdb;
  private Config config;
  private DefaultRegistry registry;
  private MockDataStore mds;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    config = new Config(false);
    registry = mock(DefaultRegistry.class);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    
    config.overrideConfig("MockDataStore.timestamp", "1483228800000");
    //config.overrideConfig("MockDataStore.threadpool.enable", "true");
    mds = new MockDataStore();
    mds.initialize(tsdb).join();
    when(registry.getDefaultPlugin(any(Class.class))).thenReturn(mds);
  }
  
  @Test
  public void foo() throws Exception {
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setAggregator("mult")
            .setFilter("f1")
            .setId("m1"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("*")
                .setType("wildcard")
                .setTagk("dc")
                .setGroupBy(true)
                )
            )
        .build();
    
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
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          System.out.println(ts.id());
//          assertEquals("sys.cpu.user", ts.id().metric());
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            System.out.println("  " + v.timestamp().msEpoch() + " " + v.value().toDouble());
//            assertEquals(timestamp, v.timestamp().msEpoch());
//            timestamp += MockDataStore.INTERVAL;
//            values++;
          }
//          assertEquals((end_ts - start_ts) / MockDataStore.INTERVAL, values);
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void querySingleOneMetric() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
            )
        .build();
    
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
          
          assertEquals("sys.cpu.user", ts.id().metric());
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }

  @Test
  public void querySingleOneMetricNoMatch() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
        .addMetric(Metric.newBuilder()
            .setMetric("metric.does.not.exist")
            .setFilter("f1")
            .setId("m1"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            )
        .build();
    
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void querySingleTwoMetrics() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        int idx = 0;
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          if (idx++ > 3) {
            assertEquals("sys.cpu.user", ts.id().metric());
          } else {
            assertEquals("web.requests", ts.id().metric());
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void querySingleTwoMetricsNoMatch() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(listener)
        .build();
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void queryBoundedClientStream() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = end_ts - ((next.sequenceId() + 1) * MockDataStore.ROW_WIDTH);
          int values = 0;
          
          if (on_next % 2 == 0) {
            assertEquals("web.requests", ts.id().metric());
          } else {
            assertEquals("sys.cpu.user", ts.id().metric());
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
        next.close();
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        ctx.fetchNext();
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_CLIENT_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }

  @Test
  public void queryBoundedClientStreamNoMatch() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        ctx.fetchNext();
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_CLIENT_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }

  @Test
  public void queryContinousClientStream() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = end_ts - ((next.sequenceId() + 1) * MockDataStore.ROW_WIDTH);
          int values = 0;
          
          if (on_next % 2 == 0) {
            assertEquals("web.requests", ts.id().metric());
          } else {
            assertEquals("sys.cpu.user", ts.id().metric());
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
        next.close();
        on_next++;
        if (on_next == 4) {
          call_limit.callback(null);
        }
        ctx.fetchNext();
      }
  
      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.CONTINOUS_CLIENT_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
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
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = end_ts - ((next.sequenceId() + 1) * MockDataStore.ROW_WIDTH);
          int values = 0;
          
          if (on_next < 2) {
            assertEquals("web.requests", ts.id().metric());
          } else {
            assertEquals("sys.cpu.user", ts.id().metric());
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_SERVER_SYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void queryContinousServerSyncStream() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = end_ts - ((next.sequenceId() + 1) * MockDataStore.ROW_WIDTH);
          int values = 0;
          
          if (on_next < 2) {
            assertEquals("web.requests", ts.id().metric());
          } else {
            assertEquals("sys.cpu.user", ts.id().metric());
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.CONTINOUS_SERVER_SYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
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
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = end_ts - ((next.sequenceId() + 1) * MockDataStore.ROW_WIDTH);
          int values = 0;
          
          if (on_next < 2) {
            assertEquals("web.requests", ts.id().metric());
          } else {
            assertEquals("sys.cpu.user", ts.id().metric());
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.BOUNDED_SERVER_ASYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
    listener.completed.join(1000);
    listener.call_limit.join(1000);
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void queryContinousServerAsyncStream() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
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
                .setTagk("host")
                )
            )
        .build();
    
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
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = end_ts - ((next.sequenceId() + 1) * MockDataStore.ROW_WIDTH);
          int values = 0;
          
          if (on_next < 2) {
            assertEquals("web.requests", ts.id().metric());
          } else {
            assertEquals("sys.cpu.user", ts.id().metric());
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
    QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.CONTINOUS_SERVER_ASYNC_STREAM)
        .addQuerySink(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
    listener.call_limit.join(1000);
    try {
      listener.completed.join(10);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
}
