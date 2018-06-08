//// This file is part of OpenTSDB.
//// Copyright (C) 2017  The OpenTSDB Authors.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package net.opentsdb.query.execution;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.mockito.Matchers.any;
//import static org.mockito.Matchers.anyInt;
//import static org.mockito.Mockito.doAnswer;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import java.util.Map;
//import java.util.NoSuchElementException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Future;
//import java.util.concurrent.RejectedExecutionException;
//
//import org.apache.http.Header;
//import org.apache.http.HttpEntity;
//import org.apache.http.HttpResponse;
//import org.apache.http.StatusLine;
//import org.apache.http.client.methods.HttpPost;
//import org.apache.http.concurrent.FutureCallback;
//import org.apache.http.entity.StringEntity;
//import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
//import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
//import org.apache.http.impl.nio.client.HttpAsyncClients;
//import org.apache.http.impl.nio.reactor.IOReactorConfig;
//import org.apache.http.message.BasicHeader;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.invocation.InvocationOnMock;
//import org.mockito.stubbing.Answer;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.google.common.collect.Maps;
//import com.stumbleupon.async.TimeoutException;
//
//import io.opentracing.Span;
//import net.opentsdb.data.SimpleStringGroupId;
//import net.opentsdb.data.TimeSeriesValue;
//import net.opentsdb.data.iterators.DefaultIteratorGroup;
//import net.opentsdb.data.iterators.IteratorGroups;
//import net.opentsdb.data.iterators.IteratorGroup;
//import net.opentsdb.data.iterators.TimeSeriesIterator;
//import net.opentsdb.data.types.numeric.NumericType;
//import net.opentsdb.exceptions.QueryExecutionException;
//import net.opentsdb.exceptions.RemoteQueryExecutionException;
//import net.opentsdb.query.TSQuery;
//import net.opentsdb.query.execution.HttpQueryV2Executor.Config;
//import net.opentsdb.query.execution.graph.ExecutionGraphNode;
//import net.opentsdb.query.filter.TagVFilter;
//import net.opentsdb.query.pojo.Downsampler;
//import net.opentsdb.query.pojo.Expression;
//import net.opentsdb.query.pojo.FillPolicy;
//import net.opentsdb.query.pojo.Filter;
//import net.opentsdb.query.pojo.Metric;
//import net.opentsdb.query.pojo.NumericFillPolicy;
//import net.opentsdb.query.pojo.TimeSeriesQuery;
//import net.opentsdb.query.pojo.RateOptions;
//import net.opentsdb.query.pojo.Timespan;
//import net.opentsdb.utils.JSON;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({ HttpQueryV2Executor.class, HttpAsyncClients.class, 
//  HttpAsyncClientBuilder.class }) 
//public class TestHttpQueryV2Executor extends BaseExecutorTest {
//  private ExecutorService cleanup_pool;
//  private String endpoint;
//  private FutureCallback<HttpResponse> callback;
//  private Future<HttpResponse> future;
//  private HttpPost post_request;
//  private Map<String, String> headers;
//  private String response_content;
//  private HttpResponse response;
//  private HttpEntity entity;
//  private StatusLine status;
//  private TimeSeriesQuery query;
//  private CloseableHttpAsyncClient client;
//  private Span span;
//  private Config config;
//  
//  @Before
//  public void beforeLocal() throws Exception {
//    node = mock(ExecutionGraphNode.class);
//    cleanup_pool = mock(ExecutorService.class);
//    endpoint = "http://my.tsd:4242";
//    headers = Maps.newHashMap();
//    headers.put("X-MyHeader", "Winter Is Coming!");
//    config = (Config) Config.newBuilder()
//        .setEndpoint(endpoint)
//        .setExecutorId("Http")
//        .setExecutorType("HttpQueryV2Executor")
//        .build();
//    
//    when(registry.cleanupPool()).thenReturn(cleanup_pool);
//    doAnswer(new Answer<Void>() {
//      @Override
//      public Void answer(InvocationOnMock invocation) throws Throwable {
//        ((Runnable) invocation.getArguments()[0]).run();
//        return null;
//      }
//    }).when(cleanup_pool).execute(any(Runnable.class));
//    when(node.getConfig()).thenReturn(config);
//    when(node.graph()).thenReturn(graph);
//    when(context.getSessionObject(HttpQueryV2Executor.SESSION_HEADERS_KEY))
//      .thenReturn(headers);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1490122900000")
//            .setEnd("1490123050000")
//            .setAggregator("sum"))
//        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
//        .build();
//    query.validate();
//    
//    response_content = "[{"
//        + " \"metric\": \"sys.cpu.user\","
//        + " \"tags\": {"
//        + "   \"hostgroup\": \"group_b\","
//        + "   \"_aggregate\": \"SUM\""
//        + " },"
//        + " \"aggregateTags\": [],"
//        + " \"dps\": {"
//        + "   \"1490122920000\": 23789.095703125,"
//        + "   \"1490122980000\": 28918,"
//        + "   \"1490123040000\": 23737.69921875"
//        + " },"
//        + " \"stats\":{}"
//        + "}, {"
//        + " \"metric\": \"sys.cpu.user\","
//        + " \"tags\": {"
//        + "   \"hostgroup\": \"group_a\","
//        + "   \"_aggregate\": \"SUM\""
//        + " },"
//        + " \"aggregateTags\": [\"host\"],"
//        + " \"dps\": {"
//        + "   \"1490122920000\": 1301.65673828125,"
//        + "   \"1490122980000\": \"NaN\","
//        + "   \"1490123040000\": 1498.576171875"
//        + " },"
//        + " \"stats\":{}"
//        + "}]";
//    
//    response = mock(HttpResponse.class);
//    entity = new StringEntity(response_content);
//    status = mock(StatusLine.class);
//    
//    when(response.getEntity()).thenReturn(entity);
//    when(response.getStatusLine()).thenReturn(status);
//    final Header[] response_headers = new Header[1];
//    response_headers[0] = new BasicHeader("X-MyHeader", "Winter Is Coming!");
//    when(response.getAllHeaders()).thenReturn(response_headers);
//    when(status.getStatusCode()).thenReturn(200);
//  }
//
//  @Test
//  public void ctor() throws Exception {
//    new HttpQueryV2Executor(node);
//    
//    try {
//      new HttpQueryV2Executor(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    when(node.getConfig()).thenReturn(null);
//    try {
//      new HttpQueryV2Executor(node);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    config = (Config) Config.newBuilder()
//        //.setEndpoint(endpoint)
//        .setExecutorId("Http")
//        .setExecutorType("HttpQueryV2Executor")
//        .build();
//    when(node.getConfig()).thenReturn(config);
//    new HttpQueryV2Executor(node);
//  }
//
//  @SuppressWarnings("unchecked")
//  @Test
//  public void close() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    assertNull(executor.close().join());
//    
//    QueryExecution<IteratorGroups> e1 = mock(QueryExecution.class);
//    QueryExecution<IteratorGroups> e2 = mock(QueryExecution.class);
//    executor.outstandingRequests().add(e1);
//    executor.outstandingRequests().add(e2);
//    
//    assertNull(executor.close().join());
//    verify(e1, times(1)).cancel();
//    verify(e2, times(1)).cancel();
//  }
//
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeQuery() throws Exception {
////    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
////    setupQuery();
////    
////    QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    assertNotNull(callback);
////    assertTrue(executor.outstandingRequests().contains(exec));
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    callback.completed(response);
////    
////    assertEquals("Winter Is Coming!", 
////        post_request.getFirstHeader("X-MyHeader").getValue());
////    final IteratorGroups groups = (IteratorGroups) exec.deferred().join();
////    System.out.println("Result: " + groups);
////    assertEquals(1, groups.groups().size());
////    IteratorGroup data = groups.groups().iterator().next();
////    TimeSeriesIterator<NumericType> it_a = (TimeSeriesIterator<NumericType>) 
////        data.flattenedIterators().get(0);
////    assertEquals("sys.cpu.user", it_a.id().metric());
////    assertEquals("group_b", it_a.id().tags().get("hostgroup"));
////    assertEquals("SUM", it_a.id().tags().get("_aggregate"));
////    assertTrue(it_a.id().aggregatedTags().isEmpty());
////    
////    TimeSeriesIterator<NumericType> it_b = (TimeSeriesIterator<NumericType>) 
////        data.iterators().get(1).iterators().get(0);
////    assertEquals("sys.cpu.user", it_b.id().metric());
////    assertEquals("group_a", it_b.id().tags().get("hostgroup"));
////    assertEquals("SUM", it_b.id().tags().get("_aggregate"));
////    assertEquals(1, it_b.id().aggregatedTags().size());
////    assertEquals("host",it_b.id().aggregatedTags().get(0));
////    
////    TimeSeriesValue<NumericType> v = it_a.next();
////    assertEquals(1490122920000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(23789.095703125, v.value().doubleValue(), 0.000000001);
////    
////    v = it_b.next();
////    assertEquals(1490122920000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(1301.65673828125, v.value().doubleValue(), 0.000000001);
////    
////    v = it_a.next();
////    assertEquals(1490122980000L, v.timestamp().msEpoch());
////    assertTrue(v.value().isInteger());
////    assertEquals(28918, v.value().longValue());
////    
////    v = it_b.next();
////    assertEquals(1490122980000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertTrue(Double.isNaN(v.value().doubleValue()));
////    
////    v = it_a.next();
////    assertEquals(1490123040000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(23737.69921875, v.value().doubleValue(), 0.000000001);
////    
////    v = it_b.next();
////    assertEquals(1490123040000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(1498.576171875, v.value().doubleValue(), 0.000000001);
////    
////    // future was removed
////    assertTrue(executor.outstandingRequests().isEmpty());
////    verify(client, times(1)).close();
////  }
//
//  @Test (expected = IllegalStateException.class)
//  public void executeQueryNonMapSessionHeaders() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//
//    final Object notMap = new Object();
//    when(context.getSessionObject(HttpQueryV2Executor.SESSION_HEADERS_KEY))
//      .thenReturn(notMap);
//
//    QueryExecution<IteratorGroups> exec =
//      executor.executeQuery(context, query, span);
//    exec.deferred().join();
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void executeQueryNullQuery() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    executor.executeQuery(context, null, span);
//  }
//  
//  @Test
//  public void executeQueryFailToConvert() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1486015200")
//            .setEnd("1486018800")
//            .setAggregator("sum"))
//        //.addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
//        .build();
//    
//    QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    assertNull(callback);
//    assertFalse(executor.outstandingRequests().contains(future));
//    try {
//      exec.deferred().join();
//      fail("Expected RejectedExecutionException");
//    } catch (RejectedExecutionException e) { }
//    verify(client, times(1)).close();
//  }
//  
//  @Test
//  public void executeQueryFutureCancelled() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//    
//    QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    assertNotNull(callback);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    callback.cancelled();
//    
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) { 
//      assertEquals(400, e.getStatusCode());
//    }
//    assertTrue(executor.outstandingRequests().isEmpty());
//    verify(client, times(1)).close();
//  }
//  
//  @Test
//  public void executeQueryUpstreamCancelled() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//    
//    QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    assertNotNull(callback);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    exec.cancel();
//    
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) { 
//      assertEquals(400, e.getStatusCode());
//    }
//    assertTrue(executor.outstandingRequests().isEmpty());
//    verify(future, times(1)).cancel(true);
//    verify(client, times(1)).close();
//  }
//  
//  @Test
//  public void executeQueryException() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//    
//    QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    assertNotNull(callback);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    callback.failed(new IllegalArgumentException("Boo!"));
//    
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) { 
//      assertEquals(500, e.getStatusCode());
//    }
//    assertTrue(executor.outstandingRequests().isEmpty());
//    verify(client, times(1)).close();
//  }
//  
//  @Test
//  public void executeQueryParsingException() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//    
//    response_content = "[{"
//        + " \"metric\": \"some.fun.metric\","
//        + " \"tags\": {"
//        + "   \"hostgroup\": \"group_b\","
//        + "   \"_aggregate\": \"SUM\""
//        + " },"
//        + " \"aggregateTags\": [],"
//        + " \"dps\": {"
//        + "   \"1490122920";
//    entity = new StringEntity(response_content);
//    when(response.getEntity()).thenReturn(entity);
//    
//    QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    assertNotNull(callback);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    callback.completed(response);
//    
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) {
//      assertEquals(500, e.getStatusCode());
//    }
//    assertTrue(executor.outstandingRequests().isEmpty());
//    verify(client, times(1)).close();
//  }
//  
//  @Test
//  public void executeQueryRemoteError() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//    when(status.getStatusCode()).thenReturn(404);
//    
//    QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    assertNotNull(callback);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    callback.completed(response);
//    
//    try {
//      exec.deferred().join();
//      fail("Expected RemoteQueryExecutionException");
//    } catch (RemoteQueryExecutionException e) { 
//      assertEquals(404, e.getStatusCode());
//    }
//    assertTrue(executor.outstandingRequests().isEmpty());
//    verify(client, times(1)).close();
//  }
//  
//  @Test
//  public void executeQueryCancelled() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    setupQuery();
//    
//    QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    assertNotNull(callback);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    exec.cancel();
//    
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) { 
//      assertEquals(400, e.getStatusCode());
//    }
//    assertTrue(executor.outstandingRequests().isEmpty());
//    verify(future, times(1)).cancel(true);
//    verify(client, times(1)).close();
//  }
//  
//  @Test
//  public void convertQuery() throws Exception {
//    // first one super simple
//    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setAggregator("sum"))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user"))
//        .build();
//    query.validate();
//    
//    TSQuery converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertNull(converted.getEnd());
//    assertEquals(1, converted.getQueries().size());
//    assertEquals("sum", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertFalse(converted.getQueries().get(0).getRate());
//    assertNull(converted.getQueries().get(0).getRateOptions());
//    assertNull(converted.getQueries().get(0).getDownsample());
//    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setAggregator("sum"))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user"))
//        .addMetric(Metric.newBuilder()
//            .setId("m2")
//            .setMetric("sys.cpu.idle"))
//        .addExpression(Expression.newBuilder()
//            .setId("e1")
//            .setExpression("m1 + m2"))
//        .build();
//    query.validate();
//    
//    converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertNull(converted.getEnd());
//    assertEquals(2, converted.getQueries().size());
//    assertEquals("sum", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertFalse(converted.getQueries().get(0).getRate());
//    assertNull(converted.getQueries().get(0).getRateOptions());
//    assertNull(converted.getQueries().get(0).getDownsample());
//    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
//    
//    assertEquals("sum", converted.getQueries().get(1).getAggregator());
//    assertEquals("sys.cpu.idle", converted.getQueries().get(1).getMetric());
//    assertFalse(converted.getQueries().get(1).getRate());
//    assertNull(converted.getQueries().get(1).getRateOptions());
//    assertNull(converted.getQueries().get(1).getDownsample());
//    assertTrue(converted.getQueries().get(1).getFilters().isEmpty());
//    
//    // test out TimeSpan options
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setEnd("1s-ago")
//            .setAggregator("sum")
//            .setDownsampler(Downsampler.newBuilder()
//                .setAggregator("max")
//                .setInterval("1m"))
//            .setTimezone("UTC")
//            .setRate(true)
//            .setRateOptions(RateOptions.newBuilder()
//                .setCounter(true)
//                .setCounterMax(1024)
//                .setResetValue(-1)))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user"))
//        .build();
//    query.validate();
//    
//    converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertEquals("1s-ago", converted.getEnd());
//    assertEquals("UTC", converted.getTimezone());
//    assertEquals(1, converted.getQueries().size());
//    assertEquals("sum", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertTrue(converted.getQueries().get(0).getRate());
//    assertTrue(converted.getQueries().get(0).getRateOptions().isCounter());
//    assertEquals(1024, converted.getQueries().get(0).getRateOptions().getCounterMax());
//    assertEquals(-1, converted.getQueries().get(0).getRateOptions().getResetValue());
//    assertEquals("1m-max", converted.getQueries().get(0).getDownsample());
//    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
//    
//    // metric rate overrides
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setEnd("1s-ago")
//            .setAggregator("sum")
//            .setTimezone("UTC")
//            .setRate(true)
//            .setRateOptions(RateOptions.newBuilder()
//                .setCounter(true)
//                .setCounterMax(1024)
//                .setResetValue(-1)))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user")
//            .setIsRate(false)
//            .setRateOptions(RateOptions.newBuilder()
//                .setCounter(true)
//                .setCounterMax(128)
//                .setResetValue(16)))
//        .build();
//    query.validate();
//    
//    converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertEquals("1s-ago", converted.getEnd());
//    assertEquals("UTC", converted.getTimezone());
//    assertEquals(1, converted.getQueries().size());
//    assertEquals("sum", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertTrue(converted.getQueries().get(0).getRate());
//    assertTrue(converted.getQueries().get(0).getRateOptions().isCounter());
//    assertEquals(128, converted.getQueries().get(0).getRateOptions().getCounterMax());
//    assertEquals(16, converted.getQueries().get(0).getRateOptions().getResetValue());
//    assertNull(converted.getQueries().get(0).getDownsample());
//    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
//    
//    // Downsample fills
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setEnd("1s-ago")
//            .setAggregator("sum")
//            .setDownsampler(Downsampler.newBuilder()
//                .setAggregator("max")
//                .setInterval("1m")
//                .setFillPolicy(NumericFillPolicy.newBuilder()
//                    .setPolicy(FillPolicy.NOT_A_NUMBER)))
//            .setTimezone("UTC")
//            .setRate(true)
//            .setRateOptions(RateOptions.newBuilder()
//                .setCounter(true)
//                .setCounterMax(1024)
//                .setResetValue(-1)))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user"))
//        .addMetric(Metric.newBuilder()
//            .setId("m2")
//            .setMetric("sys.cpu.idle")
//            .setDownsampler(Downsampler.newBuilder()
//                .setAggregator("min")
//                .setInterval("30s")
//                .setFillPolicy(NumericFillPolicy.newBuilder()
//                    .setPolicy(FillPolicy.ZERO))))
//        .build();
//    query.validate();
//    
//    converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertEquals("1s-ago", converted.getEnd());
//    assertEquals("UTC", converted.getTimezone());
//    assertEquals(2, converted.getQueries().size());
//    assertEquals("sum", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertTrue(converted.getQueries().get(0).getRate());
//    assertTrue(converted.getQueries().get(0).getRateOptions().isCounter());
//    assertEquals(1024, converted.getQueries().get(0).getRateOptions().getCounterMax());
//    assertEquals(-1, converted.getQueries().get(0).getRateOptions().getResetValue());
//    assertEquals("1m-max-nan", converted.getQueries().get(0).getDownsample());
//    assertTrue(converted.getQueries().get(0).getFilters().isEmpty());
//    
//    assertEquals("sum", converted.getQueries().get(1).getAggregator());
//    assertEquals("sys.cpu.idle", converted.getQueries().get(1).getMetric());
//    assertTrue(converted.getQueries().get(1).getRate());
//    assertTrue(converted.getQueries().get(1).getRateOptions().isCounter());
//    assertEquals(1024, converted.getQueries().get(1).getRateOptions().getCounterMax());
//    assertEquals(-1, converted.getQueries().get(1).getRateOptions().getResetValue());
//    assertEquals("30s-min-zero", converted.getQueries().get(1).getDownsample());
//    assertTrue(converted.getQueries().get(1).getFilters().isEmpty());
//    
//    // test out per-metric overrides
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setAggregator("sum")
//            .setDownsampler(Downsampler.newBuilder()
//                .setAggregator("max")
//                .setInterval("1m")))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user")
//            .setAggregator("avg")
//            .setDownsampler(Downsampler.newBuilder()
//                .setAggregator("min")
//                .setInterval("30m")))
//        .build();
//    query.validate();
//    
//    converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertEquals(1, converted.getQueries().size());
//    assertEquals("avg", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertFalse(converted.getQueries().get(0).getRate());
//    assertNull(converted.getQueries().get(0).getRateOptions());
//    assertEquals("30m-min", converted.getQueries().get(0).getDownsample());
//    
//    // filters
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setAggregator("sum"))
//        .addFilter(Filter.newBuilder()
//            .setId("f1")
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("*")
//                .setGroupBy(true)
//                .setTagk("host")
//                .setType("literal_or"))
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("lga")
//                .setGroupBy(false)
//                .setTagk("dc")
//                .setType("literal_or")))
//        .addFilter(Filter.newBuilder()
//            .setId("f2")
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("*")
//                .setGroupBy(true)
//                .setTagk("host")
//                .setType("literal_or"))
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("nyc")
//                .setGroupBy(false)
//                .setTagk("dc")
//                .setType("literal_or")))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user")
//            .setFilter("f2"))
//        .build();
//    query.validate();
//    
//    converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertEquals(1, converted.getQueries().size());
//    assertEquals("sum", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertFalse(converted.getQueries().get(0).getRate());
//    assertNull(converted.getQueries().get(0).getRateOptions());
//    assertNull(converted.getQueries().get(0).getDownsample());
//    assertEquals(2, converted.getQueries().get(0).getFilters().size());
//    
//    assertEquals("host", converted.getQueries().get(0).getFilters().get(0).getTagk());
//    assertEquals("*", converted.getQueries().get(0).getFilters().get(0).getFilter());
//    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(0).getType());
//    assertTrue(converted.getQueries().get(0).getFilters().get(0).isGroupBy());
//    
//    assertEquals("dc", converted.getQueries().get(0).getFilters().get(1).getTagk());
//    assertEquals("nyc", converted.getQueries().get(0).getFilters().get(1).getFilter());
//    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(1).getType());
//    assertFalse(converted.getQueries().get(0).getFilters().get(1).isGroupBy());
//    
//    // filter routing between multiple metrics.
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setAggregator("sum"))
//        .addFilter(Filter.newBuilder()
//            .setId("f1")
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("web01")
//                .setGroupBy(true)
//                .setTagk("host")
//                .setType("literal_or"))
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("lga")
//                .setGroupBy(false)
//                .setTagk("dc")
//                .setType("literal_or")))
//        .addFilter(Filter.newBuilder()
//            .setId("f2")
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("*")
//                .setGroupBy(true)
//                .setTagk("host")
//                .setType("literal_or"))
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("nyc")
//                .setGroupBy(false)
//                .setTagk("dc")
//                .setType("literal_or")))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user")
//            .setFilter("f2"))
//        .addMetric(Metric.newBuilder()
//            .setId("m2")
//            .setMetric("sys.cpu.idle")
//            .setFilter("f1"))
//        .build();
//    query.validate();
//    
//    converted = HttpQueryV2Executor.convertQuery(query);
//    converted.validateAndSetQuery();
//    assertEquals("1410742740000", converted.getStart());
//    assertEquals(2, converted.getQueries().size());
//    assertEquals("sum", converted.getQueries().get(0).getAggregator());
//    assertEquals("sys.cpu.user", converted.getQueries().get(0).getMetric());
//    assertFalse(converted.getQueries().get(0).getRate());
//    assertNull(converted.getQueries().get(0).getRateOptions());
//    assertNull(converted.getQueries().get(0).getDownsample());
//    assertEquals(2, converted.getQueries().get(0).getFilters().size());
//    
//    assertEquals("host", converted.getQueries().get(0).getFilters().get(0).getTagk());
//    assertEquals("*", converted.getQueries().get(0).getFilters().get(0).getFilter());
//    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(0).getType());
//    assertTrue(converted.getQueries().get(0).getFilters().get(0).isGroupBy());
//    
//    assertEquals("dc", converted.getQueries().get(0).getFilters().get(1).getTagk());
//    assertEquals("nyc", converted.getQueries().get(0).getFilters().get(1).getFilter());
//    assertEquals("literal_or", converted.getQueries().get(0).getFilters().get(1).getType());
//    assertFalse(converted.getQueries().get(0).getFilters().get(1).isGroupBy());
//    
//    assertEquals("sum", converted.getQueries().get(1).getAggregator());
//    assertEquals("sys.cpu.idle", converted.getQueries().get(1).getMetric());
//    assertFalse(converted.getQueries().get(1).getRate());
//    assertNull(converted.getQueries().get(1).getRateOptions());
//    assertNull(converted.getQueries().get(1).getDownsample());
//    assertEquals(2, converted.getQueries().get(1).getFilters().size());
//    
//    assertEquals("host", converted.getQueries().get(1).getFilters().get(0).getTagk());
//    assertEquals("web01", converted.getQueries().get(1).getFilters().get(0).getFilter());
//    assertEquals("literal_or", converted.getQueries().get(1).getFilters().get(0).getType());
//    assertTrue(converted.getQueries().get(1).getFilters().get(0).isGroupBy());
//    
//    assertEquals("dc", converted.getQueries().get(1).getFilters().get(1).getTagk());
//    assertEquals("lga", converted.getQueries().get(1).getFilters().get(1).getFilter());
//    assertEquals("literal_or", converted.getQueries().get(1).getFilters().get(1).getType());
//    assertFalse(converted.getQueries().get(1).getFilters().get(1).isGroupBy());
//    
//    // filter routing between multiple metrics.
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setAggregator("sum"))
//        .addMetric(Metric.newBuilder()
//            .setId("m1")
//            .setMetric("sys.cpu.user")
//            .setFilter("f1"))
//        .build();
//    // query.validate(); // should throw an exception
//    try {
//      converted = HttpQueryV2Executor.convertQuery(query);
//      fail("expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    // null query
//    try {
//      converted = HttpQueryV2Executor.convertQuery(null);
//      fail("expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    // no metric
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1410742740000")
//            .setAggregator("sum"))
//        .build();
//    //query.validate();
//    try {
//      converted = HttpQueryV2Executor.convertQuery(null);
//      fail("expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//
//  @Test
//  public void parseResponse() throws Exception {
//    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
//    
//    entity = new StringEntity("Hello!");
//    when(response.getEntity()).thenReturn(entity);
//    
//    // raw
//    assertEquals("Hello!", executor.parseResponse(response, 0, "unknown"));
//    
//    // TODO - figure out how to test the compressed entities. Looks like it's a 
//    // bit of a pain.
//    
//    // non-200
//    when(status.getStatusCode()).thenReturn(400);
//    try {
//      executor.parseResponse(response, 0, "unknown");
//      fail("Expected RemoteQueryExecutionException");
//    } catch (RemoteQueryExecutionException e) { 
//      assertEquals("Hello!", e.getMessage());
//    }
//    
//    // null entity
//    when(status.getStatusCode()).thenReturn(200);
//    when(response.getEntity()).thenReturn(null);
//    try {
//      executor.parseResponse(response, 0, "unknown");
//      fail("Expected RemoteQueryExecutionException");
//    } catch (RemoteQueryExecutionException e) { }
//  }
//
////  @SuppressWarnings("unchecked")
////  @Test
////  public void parseTSQuery() throws Exception {
////    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
////    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
////        .setTime(Timespan.newBuilder()
////            .setStart("1490122900000")
////            .setEnd("1490123050000")
////            .setAggregator("sum"))
////        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
////        .build();
////    query.validate();
////    JsonNode root = JSON.getMapper().readTree(response_content);
////    
////    final Map<String, IteratorGroup> groups = Maps.newHashMapWithExpectedSize(1);
////    groups.put("m1", new DefaultIteratorGroup(new SimpleStringGroupId("m1")));
////    executor.parseTSQuery(query, root.get(0), span, groups);
////    TimeSeriesIterator<NumericType> it = (TimeSeriesIterator<NumericType>) 
////        groups.get("m1").iterators().get(0).iterators().get(0);
////    assertEquals("sys.cpu.user", it.id().metric());
////    assertEquals("group_b", it.id().tags().get("hostgroup"));
////    assertEquals("SUM", it.id().tags().get("_aggregate"));
////    assertTrue(it.id().aggregatedTags().isEmpty());
////    assertNull(it.id().alias());
////    assertTrue(it.id().namespace() == null);
////    assertTrue(it.id().uniqueIds().isEmpty());
////    
////    assertEquals(1, groups.get("m1").iterators().get(0).iterators().size());
////    TimeSeriesValue<NumericType> v = it.next();
////    assertEquals(1490122920000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(23789.095703125, v.value().doubleValue(), 0.000000001);
////    
////    v = it.next();
////    assertEquals(1490122980000L, v.timestamp().msEpoch());
////    assertTrue(v.value().isInteger());
////    assertEquals(28918, v.value().longValue());
////    
////    v = it.next();
////    assertEquals(1490123040000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(23737.69921875, v.value().doubleValue(), 0.000000001);
////    
////    executor.parseTSQuery(query, root.get(1), span, groups);
////    it = (TimeSeriesIterator<NumericType>) 
////        groups.get("m1").iterators().get(1).iterators().get(0);
////    assertEquals("sys.cpu.user", it.id().metric());
////    assertEquals("group_a", it.id().tags().get("hostgroup"));
////    assertEquals("SUM", it.id().tags().get("_aggregate"));
////    assertEquals(1, it.id().aggregatedTags().size());
////    assertEquals("host",it.id().aggregatedTags().get(0));
////    assertNull(it.id().alias());
////    assertTrue(it.id().namespace() == null);
////    assertTrue(it.id().uniqueIds().isEmpty());
////    
////    assertEquals(1, groups.get("m1").iterators().get(1).iterators().size());
////    v = it.next();
////    assertEquals(1490122920000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(1301.65673828125, v.value().doubleValue(), 0.000000001);
////    
////    v = it.next();
////    assertEquals(1490122980000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertTrue(Double.isNaN(v.value().doubleValue()));
////    
////    v = it.next();
////    assertEquals(1490123040000L, v.timestamp().msEpoch());
////    assertFalse(v.value().isInteger());
////    assertEquals(1498.576171875, v.value().doubleValue(), 0.000000001);
////  }
////
////  @SuppressWarnings("unchecked")
////  @Test
////  public void parseTSQueryNoResponse() throws Exception {
////    final HttpQueryV2Executor executor = new HttpQueryV2Executor(node);
////    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
////        .setTime(Timespan.newBuilder()
////            .setStart("1490122900000")
////            .setEnd("1490123050000")
////            .setAggregator("sum"))
////        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
////        .build();
////    query.validate();
////    JsonNode root = JSON.getMapper().readTree(response_content);
////    
////    final Map<String, IteratorGroup> groups = Maps.newHashMapWithExpectedSize(1);
////    
////    // no data points
////    response_content = "[{"
////        + " \"metric\": \"sys.cpu.user\","
////        + " \"tags\": {"
////        + "   \"hostgroup\": \"group_b\","
////        + "   \"_aggregate\": \"SUM\""
////        + " },"
////        + " \"aggregateTags\": [],"
////        + " \"stats\":{}"
////        + "}, {"
////        + " \"metric\": \"sys.cpu.user\","
////        + " \"tags\": {"
////        + "   \"hostgroup\": \"group_a\","
////        + "   \"_aggregate\": \"SUM\""
////        + " },"
////        + " \"aggregateTags\": [\"host\"],"
////        + " \"stats\":{}"
////        + "}]";
////    root = JSON.getMapper().readTree(response_content);
////    groups.clear();
////    groups.put("m1", new DefaultIteratorGroup(new SimpleStringGroupId("m1")));
////    
////    executor.parseTSQuery(query, root.get(0), span, groups);
////    
////    TimeSeriesIterator<NumericType> it = (TimeSeriesIterator<NumericType>) 
////        groups.get("m1").iterators().get(0).iterators().get(0);
////    assertEquals("sys.cpu.user", it.id().metric());
////    assertEquals("group_b", it.id().tags().get("hostgroup"));
////    assertEquals("SUM", it.id().tags().get("_aggregate"));
////    assertTrue(it.id().aggregatedTags().isEmpty());
////    assertNull(it.id().alias());
////    assertTrue(it.id().namespace() == null);
////    assertTrue(it.id().uniqueIds().isEmpty());
////    
////    assertEquals(1, groups.get("m1").iterators().get(0).iterators().size());
////    try {
////      it.next();
////      fail("Expected NoSuchElementException");
////    } catch (NoSuchElementException e) { }
////    
////    executor.parseTSQuery(query, root.get(1), span, groups);
////    it = (TimeSeriesIterator<NumericType>) 
////        groups.get("m1").iterators().get(1).iterators().get(0);
////    assertEquals("sys.cpu.user", it.id().metric());
////    assertEquals("group_a", it.id().tags().get("hostgroup"));
////    assertEquals("SUM", it.id().tags().get("_aggregate"));
////    assertEquals(1, it.id().aggregatedTags().size());
////    assertEquals("host",it.id().aggregatedTags().get(0));
////    assertNull(it.id().alias());
////    assertTrue(it.id().namespace() == null);
////    assertTrue(it.id().uniqueIds().isEmpty());
////    
////    assertEquals(1, groups.get("m1").iterators().get(1).iterators().size());
////    try {
////      it.next();
////      fail("Expected NoSuchElementException");
////    } catch (NoSuchElementException e) { }
////  }
////  
//  @Test
//  public void builder() throws Exception {
//    String json = JSON.serializeToString(config);
//    assertTrue(json.contains("\"executorType\":\"HttpQueryV2Executor\""));
//    assertTrue(json.contains("\"endpoint\":\"http://my.tsd:4242\""));
//    assertTrue(json.contains("\"executorId\":\"Http\""));
//    
//    json = "{\"executorType\":\"HttpQueryV2Executor\",\"endpoint\":"
//        + "\"http://my.tsd:4242\",\"executorId\":\"Http\"}";
//    config = JSON.parseToObject(json, Config.class);
//    assertEquals("HttpQueryV2Executor", config.executorType());
//    assertEquals("Http", config.getExecutorId());
//    assertEquals("http://my.tsd:4242", config.getEndpoint());
//  }
//  
//  @Test
//  public void hashCodeEqualsCompareTo() throws Exception {
//    final Config c1 = (Config) Config.newBuilder()
//        .setEndpoint(endpoint)
//        .setExecutorId("Http")
//        .setExecutorType("HttpQueryV2Executor")
//        .build();
//    
//    Config c2 = (Config) Config.newBuilder()
//        .setEndpoint(endpoint)
//        .setExecutorId("Http")
//        .setExecutorType("HttpQueryV2Executor")
//        .build();
//    assertEquals(c1.hashCode(), c2.hashCode());
//    assertEquals(c1, c2);
//    assertEquals(0, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setEndpoint("http://localhost:12345")  // <-- Diff
//        .setExecutorId("Http")
//        .setExecutorType("HttpQueryV2Executor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        //.setEndpoint(endpoint)  // <-- Diff
//        .setExecutorId("Http")
//        .setExecutorType("HttpQueryV2Executor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setEndpoint(endpoint)
//        .setExecutorId("Http2")  // <-- Diff
//        .setExecutorType("HttpQueryV2Executor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setEndpoint(endpoint)
//        .setExecutorId("Http")
//        .setExecutorType("HttpQueryV2Executor2")  // <-- Diff
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//  }
//  
//  @SuppressWarnings("unchecked")
//  private void setupQuery() {
//    client = mock(CloseableHttpAsyncClient.class);
//    future = mock(Future.class);
//    
//    final HttpAsyncClientBuilder builder = 
//        PowerMockito.mock(HttpAsyncClientBuilder.class);
//    PowerMockito.when(builder
//        .setDefaultIOReactorConfig(any(IOReactorConfig.class)))
//          .thenReturn(builder);
//    when(builder.setMaxConnTotal(anyInt())).thenReturn(builder);
//    when(builder.setMaxConnPerRoute(anyInt())).thenReturn(builder);
//    when(builder.build()).thenReturn(client);
//    
//    PowerMockito.mockStatic(HttpAsyncClients.class);
//    when(HttpAsyncClients.custom()).thenReturn(builder);
//    
//    when(client.execute(any(HttpPost.class), any(FutureCallback.class)))
//      .thenAnswer(new Answer<Future<HttpResponse>>() {
//        @Override
//        public Future<HttpResponse> answer(final InvocationOnMock invocation)
//            throws Throwable {
//          post_request = (HttpPost) invocation.getArguments()[0];
//          callback = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
//          return future;
//        }
//      });
//  }
//}
