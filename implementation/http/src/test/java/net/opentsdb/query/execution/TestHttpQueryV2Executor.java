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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.common.Const;
import net.opentsdb.data.DataShards;
import net.opentsdb.data.DataShardsGroup;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.HttpContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.context.RemoteContext;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.JSON;

public class TestHttpQueryV2Executor {
  private TimeSeriesGroupId group_id;
  private QueryContext context;
  private HttpContext http_context;
  private String endpoint;
  private FutureCallback<HttpResponse> callback;
  private Future<HttpResponse> future;
  private HttpPost post_request;
  private Map<String, String> headers;
  private String response_content;
  private HttpResponse response;
  private HttpEntity entity;
  private StatusLine status;
  private TimeSeriesQuery query;
  
  @Before
  public void before() throws Exception {
    context = spy(new DefaultQueryContext());
    http_context = mock(HttpContext.class);
    endpoint = "http://my.tsd:4242";
    group_id = new SimpleStringGroupId("a");
    headers = Maps.newHashMap();
    headers.put("X-MyHeader", "Winter Is Coming!");
    
    when(context.getRemoteContext()).thenReturn((RemoteContext) http_context);
    when(http_context.getHeaders()).thenReturn(headers);
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1490122900000")
            .setEnd("1490123050000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
        .build();
    query.validate();
    query.groupId(group_id);
    
    response_content = "[{"
        + " \"metric\": \"some.fun.metric\","
        + " \"tags\": {"
        + "   \"hostgroup\": \"group_b\","
        + "   \"_aggregate\": \"SUM\""
        + " },"
        + " \"aggregateTags\": [],"
        + " \"dps\": {"
        + "   \"1490122920000\": 23789.095703125,"
        + "   \"1490122980000\": 28918,"
        + "   \"1490123040000\": 23737.69921875"
        + " },"
        + " \"stats\":{}"
        + "}, {"
        + " \"metric\": \"some.fun.metric\","
        + " \"tags\": {"
        + "   \"hostgroup\": \"group_a\","
        + "   \"_aggregate\": \"SUM\""
        + " },"
        + " \"aggregateTags\": [\"host\"],"
        + " \"dps\": {"
        + "   \"1490122920000\": 1301.65673828125,"
        + "   \"1490122980000\": 1285,"
        + "   \"1490123040000\": 1498.576171875"
        + " },"
        + " \"stats\":{}"
        + "}]";
    
    response = mock(HttpResponse.class);
    entity = new StringEntity(response_content);
    status = mock(StatusLine.class);
    
    when(response.getEntity()).thenReturn(entity);
    when(response.getStatusLine()).thenReturn(status);
    final Header[] response_headers = new Header[1];
    response_headers[0] = new BasicHeader("X-MyHeader", "Winter Is Coming!");
    when(response.getAllHeaders()).thenReturn(response_headers);
    when(status.getStatusCode()).thenReturn(200);
  }

  @Test
  public void ctor() throws Exception {
    new HttpQueryV2Executor(context, endpoint);
    
    try {
      new HttpQueryV2Executor(null, endpoint);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new HttpQueryV2Executor(context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new HttpQueryV2Executor(context, "");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(context.getRemoteContext()).thenReturn(null);
    try {
      new HttpQueryV2Executor(context, endpoint);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(context.getRemoteContext()).thenReturn(mock(RemoteContext.class));
    try {
      new HttpQueryV2Executor(context, endpoint);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void close() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    assertNull(executor.close().join());
    
    Future<HttpResponse> f1 = mock(Future.class);
    Future<HttpResponse> f2 = mock(Future.class);
    
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    futures.add(f1);
    futures.add(f2);
    
    assertNull(executor.close().join());
    verify(f1, times(1)).cancel(true);
    verify(f2, times(1)).cancel(true);
    
    futures.clear();
    f1 = mock(Future.class);
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(f1.cancel(true)).thenThrow(ex);
    futures.add(f1);
    
    try {
      executor.close().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void executeQuery() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNotNull(callback);
    assertTrue(futures.contains(future));
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    callback.completed(response);
    
    assertEquals("Winter Is Coming!", 
        post_request.getFirstHeader("X-MyHeader").getValue());
    final DataShardsGroup data = exec.deferred().join();
    assertEquals(2, data.data().size());
    TimeSeriesIterator<NumericType> it_a = (TimeSeriesIterator<NumericType>) 
        data.data().get(0).data().get(0);
    assertArrayEquals("some.fun.metric".getBytes(Const.UTF8_CHARSET), 
        it_a.id().metrics().get(0));
    assertArrayEquals("group_b".getBytes(Const.UTF8_CHARSET), 
        it_a.id().tags().get("hostgroup".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("SUM".getBytes(Const.UTF8_CHARSET), 
        it_a.id().tags().get("_aggregate".getBytes(Const.UTF8_CHARSET)));
    assertTrue(it_a.id().aggregatedTags().isEmpty());
    
    TimeSeriesIterator<NumericType> it_b = (TimeSeriesIterator<NumericType>) 
        data.data().get(1).data().get(0);
    assertArrayEquals("some.fun.metric".getBytes(Const.UTF8_CHARSET), 
        it_b.id().metrics().get(0));
    assertArrayEquals("group_a".getBytes(Const.UTF8_CHARSET), 
        it_b.id().tags().get("hostgroup".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("SUM".getBytes(Const.UTF8_CHARSET), 
        it_b.id().tags().get("_aggregate".getBytes(Const.UTF8_CHARSET)));
    assertEquals(1, it_b.id().aggregatedTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        it_b.id().aggregatedTags().get(0));
    
    TimeSeriesValue<NumericType> v = it_a.next();
    assertEquals(1490122920000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(23789.095703125, v.value().doubleValue(), 0.000000001);
    
    v = it_b.next();
    assertEquals(1490122920000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(1301.65673828125, v.value().doubleValue(), 0.000000001);
    
    v = it_a.next();
    assertEquals(1490122980000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(28918, v.value().longValue());
    
    v = it_b.next();
    assertEquals(1490122980000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1285, v.value().longValue());
    
    v = it_a.next();
    assertEquals(1490123040000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(23737.69921875, v.value().doubleValue(), 0.000000001);
    
    v = it_b.next();
    assertEquals(1490123040000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(1498.576171875, v.value().doubleValue(), 0.000000001);
    
    // future was removed
    assertTrue(futures.isEmpty());
  }
  
  @Test
  public void executeQueryGroupIdMissing() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    setupQuery();
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1490122900000")
            .setEnd("1490123050000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
        .build();
    query.validate();
    try {
      executor.executeQuery(query);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void executeQueryAlreadyCancelled() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    
    final AtomicBoolean cancelled = 
        Whitebox.getInternalState(executor, "completed");
    cancelled.set(true);
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNull(callback);
    assertFalse(futures.contains(future));
    try {
      exec.deferred().join();
      fail("Expected RejectedExecutionException");
    } catch (RejectedExecutionException e) { }
    verify(http_context, never()).getClient();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeQueryNullQuery() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    executor.executeQuery(null);
  }
  
  @Test
  public void executeQueryFailToConvert() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486015200")
            .setEnd("1486018800")
            .setAggregator("sum"))
        //.addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
        .build();
    query.groupId(group_id);
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNull(callback);
    assertFalse(futures.contains(future));
    try {
      exec.deferred().join();
      fail("Expected RejectedExecutionException");
    } catch (RejectedExecutionException e) { }
    verify(http_context, never()).getClient();
  }
  
  @Test
  public void executeQueryFutureCancelled() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNotNull(callback);
    assertTrue(futures.contains(future));
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    callback.cancelled();
    
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    assertTrue(futures.isEmpty());
  }
  
  @Test
  public void executeQueryUpstreamCancelled() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNotNull(callback);
    assertTrue(futures.contains(future));
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    exec.cancel();
    
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    assertTrue(futures.isEmpty());
    verify(future, times(1)).cancel(true);
    
  }
  
  @Test
  public void executeQueryException() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNotNull(callback);
    assertTrue(futures.contains(future));
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    callback.cancelled();
    
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    assertTrue(futures.isEmpty());
  }
  
  @Test
  public void executeQueryParsingException() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    
    response_content = "[{"
        + " \"metric\": \"some.fun.metric\","
        + " \"tags\": {"
        + "   \"hostgroup\": \"group_b\","
        + "   \"_aggregate\": \"SUM\""
        + " },"
        + " \"aggregateTags\": [],"
        + " \"dps\": {"
        + "   \"1490122920";
    entity = new StringEntity(response_content);
    when(response.getEntity()).thenReturn(entity);
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNotNull(callback);
    assertTrue(futures.contains(future));
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    callback.cancelled();
    
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    assertTrue(futures.isEmpty());
  }
  
  @Test
  public void executeQueryRemoteError() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final Set<Future<HttpResponse>> futures = 
        Whitebox.getInternalState(executor, "futures");
    setupQuery();
    when(status.getStatusCode()).thenReturn(404);
    
    QueryExecution<DataShardsGroup> exec = executor.executeQuery(query);
    assertNotNull(callback);
    assertTrue(futures.contains(future));
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    callback.cancelled();
    
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    assertTrue(futures.isEmpty());
  }
  
  @Test
  public void convertQuery() throws Exception {
    // first one super simple
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    query.validate();
    
    TSQuery converted = HttpQueryV2Executor.convertQuery(query);
    converted.validateAndSetQuery();
    assertEquals("1410742740000", converted.getStart());
    assertNull(converted.getEnd());
    assertEquals(1, converted.getQueries().size());
    assertEquals("sum", converted.getQueries().get(0).getAggregator());
    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
    assertFalse(converted.getQueries().get(0).getRate());
    assertNull(converted.getQueries().get(0).getRateOptions());
    assertNull(converted.getQueries().get(0).getDownsample());
    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .addMetric(Metric.newBuilder()
            .setId("m2")
            .setMetric("sys.cpu.idle"))
        .addExpression(Expression.newBuilder()
            .setId("e1")
            .setExpression("m1 + m2"))
        .build();
    query.validate();
    
    converted = HttpQueryV2Executor.convertQuery(query);
    converted.validateAndSetQuery();
    assertEquals("1410742740000", converted.getStart());
    assertNull(converted.getEnd());
    assertEquals(2, converted.getQueries().size());
    assertEquals("sum", converted.getQueries().get(0).getAggregator());
    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
    assertFalse(converted.getQueries().get(0).getRate());
    assertNull(converted.getQueries().get(0).getRateOptions());
    assertNull(converted.getQueries().get(0).getDownsample());
    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
    
    assertEquals("sum", converted.getQueries().get(1).getAggregator());
    assertEquals("sys.cpu.idle", converted.getQueries().get(1).getMetric());
    assertFalse(converted.getQueries().get(1).getRate());
    assertNull(converted.getQueries().get(1).getRateOptions());
    assertNull(converted.getQueries().get(1).getDownsample());
    assertTrue(converted.getQueries().get(1).getFilters().isEmpty());
    
    // test out TimeSpan options
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setEnd("1s-ago")
            .setAggregator("sum")
            .setDownsampler(Downsampler.newBuilder()
                .setAggregator("max")
                .setInterval("1m"))
            .setTimezone("UTC")
            .setRate(true)
            .setRateOptions(RateOptions.newBuilder()
                .setCounter(true)
                .setCounterMax(1024)
                .setResetValue(-1)))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    query.validate();
    
    converted = HttpQueryV2Executor.convertQuery(query);
    converted.validateAndSetQuery();
    assertEquals("1410742740000", converted.getStart());
    assertEquals("1s-ago", converted.getEnd());
    assertEquals("UTC", converted.getTimezone());
    assertEquals(1, converted.getQueries().size());
    assertEquals("sum", converted.getQueries().get(0).getAggregator());
    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
    assertTrue(converted.getQueries().get(0).getRate());
    assertTrue(converted.getQueries().get(0).getRateOptions().isCounter());
    assertEquals(1024, converted.getQueries().get(0).getRateOptions().getCounterMax());
    assertEquals(-1, converted.getQueries().get(0).getRateOptions().getResetValue());
    assertEquals("1m-max", converted.getQueries().get(0).getDownsample());
    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
    
    // test out per-metric overrides
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setAggregator("sum")
            .setDownsampler(Downsampler.newBuilder()
                .setAggregator("max")
                .setInterval("1m")))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user")
            .setAggregator("avg")
            .setDownsampler(Downsampler.newBuilder()
                .setAggregator("min")
                .setInterval("30m")))
        .build();
    query.validate();
    
    converted = HttpQueryV2Executor.convertQuery(query);
    converted.validateAndSetQuery();
    assertEquals("1410742740000", converted.getStart());
    assertEquals(1, converted.getQueries().size());
    assertEquals("avg", converted.getQueries().get(0).getAggregator());
    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
    assertFalse(converted.getQueries().get(0).getRate());
    assertNull(converted.getQueries().get(0).getRateOptions());
    assertEquals("30m-min", converted.getQueries().get(0).getDownsample());
    
    // filters
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setAggregator("sum"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("*")
                .setGroupBy(true)
                .setTagk("host")
                .setType("literal_or"))
            .addFilter(TagVFilter.newBuilder()
                .setFilter("lga")
                .setGroupBy(false)
                .setTagk("dc")
                .setType("literal_or")))
        .addFilter(Filter.newBuilder()
            .setId("f2")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("*")
                .setGroupBy(true)
                .setTagk("host")
                .setType("literal_or"))
            .addFilter(TagVFilter.newBuilder()
                .setFilter("nyc")
                .setGroupBy(false)
                .setTagk("dc")
                .setType("literal_or")))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user")
            .setFilter("f2"))
        .build();
    query.validate();
    
    converted = HttpQueryV2Executor.convertQuery(query);
    converted.validateAndSetQuery();
    assertEquals("1410742740000", converted.getStart());
    assertEquals(1, converted.getQueries().size());
    assertEquals("sum", converted.getQueries().get(0).getAggregator());
    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
    assertFalse(converted.getQueries().get(0).getRate());
    assertNull(converted.getQueries().get(0).getRateOptions());
    assertNull(converted.getQueries().get(0).getDownsample());
    assertEquals(2, converted.getQueries().get(0).getFilters().size());
    
    assertEquals("host", converted.getQueries().get(0).getFilters().get(0).getTagk());
    assertEquals("*", converted.getQueries().get(0).getFilters().get(0).getFilter());
    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(0).getType());
    assertTrue(converted.getQueries().get(0).getFilters().get(0).isGroupBy());
    
    assertEquals("dc", converted.getQueries().get(0).getFilters().get(1).getTagk());
    assertEquals("nyc", converted.getQueries().get(0).getFilters().get(1).getFilter());
    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(1).getType());
    assertFalse(converted.getQueries().get(0).getFilters().get(1).isGroupBy());
    
    // filter routing between multiple metrics.
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setAggregator("sum"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setGroupBy(true)
                .setTagk("host")
                .setType("literal_or"))
            .addFilter(TagVFilter.newBuilder()
                .setFilter("lga")
                .setGroupBy(false)
                .setTagk("dc")
                .setType("literal_or")))
        .addFilter(Filter.newBuilder()
            .setId("f2")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("*")
                .setGroupBy(true)
                .setTagk("host")
                .setType("literal_or"))
            .addFilter(TagVFilter.newBuilder()
                .setFilter("nyc")
                .setGroupBy(false)
                .setTagk("dc")
                .setType("literal_or")))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user")
            .setFilter("f2"))
        .addMetric(Metric.newBuilder()
            .setId("m2")
            .setMetric("sys.cpu.idle")
            .setFilter("f1"))
        .build();
    query.validate();
    
    converted = HttpQueryV2Executor.convertQuery(query);
    converted.validateAndSetQuery();
    assertEquals("1410742740000", converted.getStart());
    assertEquals(2, converted.getQueries().size());
    assertEquals("sum", converted.getQueries().get(0).getAggregator());
    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
    assertFalse(converted.getQueries().get(0).getRate());
    assertNull(converted.getQueries().get(0).getRateOptions());
    assertNull(converted.getQueries().get(0).getDownsample());
    assertEquals(2, converted.getQueries().get(0).getFilters().size());
    
    assertEquals("host", converted.getQueries().get(0).getFilters().get(0).getTagk());
    assertEquals("*", converted.getQueries().get(0).getFilters().get(0).getFilter());
    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(0).getType());
    assertTrue(converted.getQueries().get(0).getFilters().get(0).isGroupBy());
    
    assertEquals("dc", converted.getQueries().get(0).getFilters().get(1).getTagk());
    assertEquals("nyc", converted.getQueries().get(0).getFilters().get(1).getFilter());
    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(1).getType());
    assertFalse(converted.getQueries().get(0).getFilters().get(1).isGroupBy());
    
    assertEquals("sum", converted.getQueries().get(1).getAggregator());
    assertEquals("sys.cpu.idle", converted.getQueries().get(1).getMetric());
    assertFalse(converted.getQueries().get(1).getRate());
    assertNull(converted.getQueries().get(1).getRateOptions());
    assertNull(converted.getQueries().get(1).getDownsample());
    assertEquals(2, converted.getQueries().get(1).getFilters().size());
    
    assertEquals("host", converted.getQueries().get(1).getFilters().get(0).getTagk());
    assertEquals("web01", converted.getQueries().get(1).getFilters().get(0).getFilter());
    assertEquals("literal_or", converted.getQueries().get(1).getFilters().get(0).getType());
    assertTrue(converted.getQueries().get(1).getFilters().get(0).isGroupBy());
    
    assertEquals("dc", converted.getQueries().get(1).getFilters().get(1).getTagk());
    assertEquals("lga", converted.getQueries().get(1).getFilters().get(1).getFilter());
    assertEquals("literal_or", converted.getQueries().get(1).getFilters().get(1).getType());
    assertFalse(converted.getQueries().get(1).getFilters().get(1).isGroupBy());
    
    // filter routing between multiple metrics.
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user")
            .setFilter("f1"))
        .build();
    // query.validate(); // should throw an exception
    try {
      converted = HttpQueryV2Executor.convertQuery(query);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // null query
    try {
      converted = HttpQueryV2Executor.convertQuery(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no metric
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1410742740000")
            .setAggregator("sum"))
        .build();
    //query.validate();
    try {
      converted = HttpQueryV2Executor.convertQuery(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void parseResponse() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    
    entity = new StringEntity("Hello!");
    when(response.getEntity()).thenReturn(entity);
    
    // raw
    assertEquals("Hello!", executor.parseResponse(response, 0));
    
    // TODO - figure out how to test the compressed entities. Looks like it's a 
    // bit of a pain.
    
    // non-200
    when(status.getStatusCode()).thenReturn(400);
    try {
      executor.parseResponse(response, 0);
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { 
      assertEquals("Hello!", e.getMessage());
    }
    
    // null entity
    when(status.getStatusCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(null);
    try {
      executor.parseResponse(response, 0);
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void parseTSQuery() throws Exception {
    final HttpQueryV2Executor executor = 
        new HttpQueryV2Executor(context, endpoint);
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1490122900000")
            .setEnd("1490123050000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
        .build();
    query.validate();
    JsonNode root = JSON.getMapper().readTree(response_content);
    
    DataShards shards = executor.parseTSQuery(query, root.get(0));
    assertArrayEquals("some.fun.metric".getBytes(Const.UTF8_CHARSET), 
        shards.id().metrics().get(0));
    assertArrayEquals("group_b".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("hostgroup".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("SUM".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("_aggregate".getBytes(Const.UTF8_CHARSET)));
    assertTrue(shards.id().aggregatedTags().isEmpty());
    assertNull(shards.id().alias());
    assertTrue(shards.id().namespaces().isEmpty());
    assertTrue(shards.id().uniqueIds().isEmpty());
    
    assertEquals(1, shards.data().size());
    TimeSeriesIterator<NumericType> it = 
        (TimeSeriesIterator<NumericType>) shards.data().get(0);
    TimeSeriesValue<NumericType> v = it.next();
    assertEquals(1490122920000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(23789.095703125, v.value().doubleValue(), 0.000000001);
    
    v = it.next();
    assertEquals(1490122980000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(28918, v.value().longValue());
    
    v = it.next();
    assertEquals(1490123040000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(23737.69921875, v.value().doubleValue(), 0.000000001);
    
    shards = executor.parseTSQuery(query, root.get(1));
    assertArrayEquals("some.fun.metric".getBytes(Const.UTF8_CHARSET), 
        shards.id().metrics().get(0));
    assertArrayEquals("group_a".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("hostgroup".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("SUM".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("_aggregate".getBytes(Const.UTF8_CHARSET)));
    assertEquals(1, shards.id().aggregatedTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        shards.id().aggregatedTags().get(0));
    assertNull(shards.id().alias());
    assertTrue(shards.id().namespaces().isEmpty());
    assertTrue(shards.id().uniqueIds().isEmpty());
    
    assertEquals(1, shards.data().size());
    it = (TimeSeriesIterator<NumericType>) shards.data().get(0);
    v = it.next();
    assertEquals(1490122920000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(1301.65673828125, v.value().doubleValue(), 0.000000001);
    
    v = it.next();
    assertEquals(1490122980000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1285, v.value().longValue());
    
    v = it.next();
    assertEquals(1490123040000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(1498.576171875, v.value().doubleValue(), 0.000000001);
    
    // no data points
    response_content = "[{"
        + " \"metric\": \"some.fun.metric\","
        + " \"tags\": {"
        + "   \"hostgroup\": \"group_b\","
        + "   \"_aggregate\": \"SUM\""
        + " },"
        + " \"aggregateTags\": [],"
        + " \"stats\":{}"
        + "}, {"
        + " \"metric\": \"some.fun.metric\","
        + " \"tags\": {"
        + "   \"hostgroup\": \"group_a\","
        + "   \"_aggregate\": \"SUM\""
        + " },"
        + " \"aggregateTags\": [\"host\"],"
        + " \"stats\":{}"
        + "}]";
    root = JSON.getMapper().readTree(response_content);
    
    shards = executor.parseTSQuery(query, root.get(0));
    assertArrayEquals("some.fun.metric".getBytes(Const.UTF8_CHARSET), 
        shards.id().metrics().get(0));
    assertArrayEquals("group_b".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("hostgroup".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("SUM".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("_aggregate".getBytes(Const.UTF8_CHARSET)));
    assertTrue(shards.id().aggregatedTags().isEmpty());
    assertNull(shards.id().alias());
    assertTrue(shards.id().namespaces().isEmpty());
    assertTrue(shards.id().uniqueIds().isEmpty());
    
    assertEquals(1, shards.data().size());
    it = (TimeSeriesIterator<NumericType>) shards.data().get(0);
    try {
      it.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    shards = executor.parseTSQuery(query, root.get(1));
    assertArrayEquals("some.fun.metric".getBytes(Const.UTF8_CHARSET), 
        shards.id().metrics().get(0));
    assertArrayEquals("group_a".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("hostgroup".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("SUM".getBytes(Const.UTF8_CHARSET), 
        shards.id().tags().get("_aggregate".getBytes(Const.UTF8_CHARSET)));
    assertEquals(1, shards.id().aggregatedTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        shards.id().aggregatedTags().get(0));
    assertNull(shards.id().alias());
    assertTrue(shards.id().namespaces().isEmpty());
    assertTrue(shards.id().uniqueIds().isEmpty());
    
    assertEquals(1, shards.data().size());
    it = (TimeSeriesIterator<NumericType>) shards.data().get(0);
    try {
      it.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @SuppressWarnings("unchecked")
  private void setupQuery() {
    final CloseableHttpAsyncClient client = mock(CloseableHttpAsyncClient.class);
    
    future = mock(Future.class);
    when(http_context.getClient()).thenReturn(client);
    when(client.execute(any(HttpPost.class), any(FutureCallback.class)))
      .thenAnswer(new Answer<Future<HttpResponse>>() {
        @Override
        public Future<HttpResponse> answer(final InvocationOnMock invocation)
            throws Throwable {
          post_request = (HttpPost) invocation.getArguments()[0];
          callback = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
          return future;
        }
      });
  }
}
