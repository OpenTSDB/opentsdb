// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.lang.Thread.State;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.storage.MockDataPoints;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.cache.CacheBuilder;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

/**
 * Unit tests for the JSON serializer.
 * <b>Note:</b> Tests for the default error handlers are in the TestHttpQuery
 * class
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ HttpJsonSerializer.class, TSDB.class, Config.class, 
  HttpQuery.class, TSQuery.class, TSSubQuery.class, QueryStats.class,
  DateTime.class })
public final class TestHttpJsonSerializer {
  private TSDB tsdb = null;
  private final List<Long> timestamp = new ArrayList<Long>(1);
  private static String remote = "192.168.1.1:4242";
  private static Field running_queries; 
  static {
      try {
        running_queries = QueryStats.class.getDeclaredField("running_queries");
        running_queries.setAccessible(true);
      } catch (Exception e) {
        throw new RuntimeException("Failed in static initializer", e);
      }
  }
  private static Field completed_queries;
  static {
    try {
      completed_queries = QueryStats.class.getDeclaredField("completed_queries");
      completed_queries.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }

  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
  }
  
  @Test
  public void constructorDefault() {
    assertNotNull(new HttpJsonSerializer());
  }
  
  @Test
  public void constructorQuery() {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    assertNotNull(new HttpJsonSerializer(query));
  }
  
  @Test
  public void shutdown() {
    assertNotNull(new HttpJsonSerializer().shutdown());
  }
  
  @Test
  public void version() {
    assertEquals("2.0.0", new HttpJsonSerializer().version());
  }
  
  @Test
  public void shortName() {
    assertEquals("json", new HttpJsonSerializer().shortName());
  }
  
  @Test
  public void requestContentType() {
    HttpJsonSerializer serdes = new HttpJsonSerializer();
    assertEquals("application/json", serdes.requestContentType());
  }
  
  @Test
  public void responseContentType() {
    HttpJsonSerializer serdes = new HttpJsonSerializer();
    assertEquals("application/json; charset=UTF-8", serdes.responseContentType());
  }
  
  @Test
  public void parseSuggestV1() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        "{\"type\":\"metrics\",\"q\":\"\"}", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    HashMap<String, String> map = serdes.parseSuggestV1();
    assertNotNull(map);
    assertEquals("metrics", map.get("type"));
  }
  
  @Test (expected = BadRequestException.class)
  public void parseSuggestV1NoContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        null, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseSuggestV1();
  }
  
  @Test (expected = BadRequestException.class)
  public void parseSuggestV1EmptyContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        "", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseSuggestV1();
  }
  
  @Test (expected = BadRequestException.class)
  public void parseSuggestV1NotJSON() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        "This is unparsable", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseSuggestV1();
  }
  
  @Test
  public void parseUidRenameV1() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "",
        "{\"metric\":\"sys.cpu.1\",\"name\":\"sys.cpu.2\"}", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    HashMap<String, String> map = serdes.parseUidRenameV1();
    assertNotNull(map);
    assertEquals("sys.cpu.1", map.get("metric"));
  }

  @Test (expected = BadRequestException.class)
  public void parseUidRenameV1NoContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", null, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseUidRenameV1();
  }

  @Test (expected = BadRequestException.class)
  public void parseUidRenameV1EmptyContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", "", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseUidRenameV1();
  }

  @Test (expected = BadRequestException.class)
  public void parseUidRenameV1NotJSON() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", "NOT JSON", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseUidRenameV1();
  }

  @Test
  public void formatSuggestV1() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final List<String> metrics = new ArrayList<String>();
    metrics.add("sys.cpu.0.system"); 
    ChannelBuffer cb = serdes.formatSuggestV1(metrics);
    assertNotNull(cb);
    assertEquals("[\"sys.cpu.0.system\"]", 
        cb.toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void formatSuggestV1JSONP() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "?jsonp=func");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final List<String> metrics = new ArrayList<String>();
    metrics.add("sys.cpu.0.system"); 
    ChannelBuffer cb = serdes.formatSuggestV1(metrics);
    assertNotNull(cb);
    assertEquals("func([\"sys.cpu.0.system\"])", 
        cb.toString(Charset.forName("UTF-8")));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void formatSuggestV1Null() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.formatSuggestV1(null);
  }
  
  @Test
  public void formatUidRenameV1Success() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final HashMap<String, String> map = new HashMap<String, String>(2);
    map.put("result", "true");
    ChannelBuffer cb = serdes.formatUidRenameV1(map);
    assertNotNull(cb);
    assertEquals("{\"result\":\"true\"}",
        cb.toString(Charset.forName("UTF-8")));
  }

  @Test
  public void formatUidRenameV1Failed() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final HashMap<String, String> map = new HashMap<String, String>(2);
    map.put("result", "false");
    map.put("error", "known");
    ChannelBuffer cb = serdes.formatUidRenameV1(map);
    assertNotNull(cb);
    assertEquals("{\"error\":\"known\",\"result\":\"false\"}",
        cb.toString(Charset.forName("UTF-8")));
  }

  @Test (expected = IllegalArgumentException.class)
  public void formatUidRenameV1Null() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.formatUidRenameV1(null);
  }

  @Test
  public void formatSerializersV1() throws Exception {
    HttpQuery.initializeSerializerMaps(tsdb);
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    assertEquals("[{\"formatters\":",
        serdes.formatSerializersV1().toString(Charset.forName("UTF-8"))
        .substring(0, 15));
  }

  @Test
  public void formatQueryAsyncV1() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    validateTestQuery(data_query);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { new MockDataPoints().getMock() });

    ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\","));
    assertTrue(json.contains("\"1356998700\":1,"));
    assertTrue(json.contains("\"1357058700\":201"));
    assertFalse(json.contains("\"timeTotal\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"query\":"));
  }

  @Test
  public void formatQueryAsyncV1wQuery() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    data_query.setShowQuery(true);
    validateTestQuery(data_query);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { new MockDataPoints().getMock() });

    ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\","));
    assertTrue(json.contains("\"1356998700\":1,"));
    assertTrue(json.contains("\"1357058700\":201"));
    assertFalse(json.contains("\"timeTotal\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"query\":"));
  }
  
  @Test
  public void formatQueryAsyncV1wStatsSummary() throws Exception {
    setupFormatQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, "");
    final HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(true, true);
    validateTestQuery(data_query);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { new MockDataPoints().getMock() });

    final ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\","));
    assertTrue(json.contains("\"1356998700\":1,"));
    assertTrue(json.contains("\"1357058700\":201"));
    //assert stats
    assertTrue(json.contains("\"stats\":{"));
    assertTrue(json.contains("\"emittedDPs\":401"));
    System.out.println(json);
    //assert stats summary
    assertTrue(json.contains("{\"statsSummary\":{"));
    assertTrue(json.contains("\"serializationTime\":"));
    assertTrue(json.contains("\"processingPreWriteTime\":"));
    assertTrue(json.contains("\"queryIdx_00\":"));
  }
  
  @Test
  public void formatQueryAsyncV1wStatsWoSummary() throws Exception {
    setupFormatQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, "");
    final HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(true, false);
    validateTestQuery(data_query);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { new MockDataPoints().getMock() });

    final ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\","));
    assertTrue(json.contains("\"stats\":{"));
    assertTrue(json.contains("\"1356998700\":1,"));
    assertTrue(json.contains("\"1357058700\":201"));
    
    //assert stats
    assertTrue(json.contains("\"stats\":{"));
    assertTrue(json.contains("\"emittedDPs\":401"));
    
    //assert stats summary
    assertFalse(json.contains("{\"statsSummary\":{"));
  }
  
  @Test
  public void formatQueryAsyncV1woStatsWSummary() throws Exception {
    setupFormatQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, "");
    final HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false, true);
    validateTestQuery(data_query);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { new MockDataPoints().getMock() });

    final ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\","));
    assertTrue(json.contains("\"1356998700\":1,"));
    assertTrue(json.contains("\"1357058700\":201"));
    
    //assert stats
    assertFalse(json.contains("\"stats\":{"));
    
    //assert stats summary
    assertTrue(json.contains("{\"statsSummary\":{"));
    assertTrue(json.contains("\"serializationTime\":"));
    assertTrue(json.contains("\"processingPreWriteTime\":"));
    assertTrue(json.contains("\"emittedDPs\":401"));
    assertTrue(json.contains("\"queryIdx_00\":"));
  }
  
  @Test
  public void formatQueryAsyncV1woStatsWoSummary() throws Exception {
    setupFormatQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, "");
    final HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false, false);
    validateTestQuery(data_query);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { new MockDataPoints().getMock() });

    final ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\","));
    assertTrue(json.contains("\"1356998700\":1,"));
    assertTrue(json.contains("\"1357058700\":201"));
    
    //assert stats
    assertFalse(json.contains("\"stats\":{"));
    
    //assert stats summary
    assertFalse(json.contains("{\"statsSummary\":{"));
  }
  
  @Test
  public void formatQueryAsyncTimeFilterV1() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false, false);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { new MockDataPoints().getMock() });
    
    data_query.setEnd("1357000500");
    validateTestQuery(data_query);

    ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\","));
    assertTrue(json.contains("\"1356998700\":1,"));
    assertTrue(json.contains("\"1357000500\":7"));
  }

  @Test
  public void formatQueryAsyncV1EmptyDPs() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    validateTestQuery(data_query);
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);

    ChannelBuffer cb = serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertEquals("[]", json);
  }
  
  @Test (expected = DeferredGroupException.class)
  public void formatQueryAsyncV1NoSuchMetricId() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    validateTestQuery(data_query);
    
    final DataPoints dps = new MockDataPoints().getMock();
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { dps });

    when(dps.metricNameAsync())
      .thenReturn(Deferred.<String>fromError(
          new NoSuchUniqueId("No such metric", new byte[] { 0, 0, 1 })));
    
    serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
  }
  
  @Test (expected = DeferredGroupException.class)
  public void formatQueryAsyncV1NoSuchTagId() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    validateTestQuery(data_query);
    
    final DataPoints dps = new MockDataPoints().getMock();
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { dps });

    when(dps.getTagsAsync())
      .thenReturn(Deferred.<Map<String, String>>fromError(
          new NoSuchUniqueId("No such tagv", new byte[] { 0, 0, 1 })));
    
    serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
  }
  
  @Test (expected = DeferredGroupException.class)
  public void formatQueryAsyncV1NoSuchAggTagId() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    validateTestQuery(data_query);
    
    final DataPoints dps = new MockDataPoints().getMock();
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { dps });

    when(dps.getAggregatedTagsAsync())
      .thenReturn(Deferred.<List<String>>fromError(
          new NoSuchUniqueId("No such tagk", new byte[] { 0, 0, 1 })));
    
    serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
  }
  
  @Test (expected = NullPointerException.class)
  public void formatQueryAsyncV1NullIterator() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    validateTestQuery(data_query);
    
    final DataPoints dps = new MockDataPoints().getMock();
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { dps });

    when(dps.iterator()).thenReturn(null);
    
    serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
  }
  
  @Test (expected = RuntimeException.class)
  public void formatQueryAsyncV1UnexpectedAggException() throws Exception {
    setupFormatQuery();
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final TSQuery data_query = getTestQuery(false);
    validateTestQuery(data_query);
    
    final MockDataPoints mdps = new MockDataPoints();
    final List<DataPoints[]> results = new ArrayList<DataPoints[]>(1);
    results.add(new DataPoints[] { mdps.getMock() });

    when(mdps.getMockDP().timestamp()).thenThrow(
        new RuntimeException("Unexpected error"));
    
    serdes.formatQueryAsyncV1(data_query, results, 
        Collections.<Annotation> emptyList()).joinUninterruptibly();
  }
  
  @Test
  public void formatThreadStats() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    
    final List<Map<String, Object>> output = 
        new ArrayList<Map<String, Object>>(1);
    Map<String, Object> status = new HashMap<String, Object>();
    status.put("threadID", 1);
    status.put("name", "Test Thread 1");
    status.put("state", State.RUNNABLE);
    status.put("interrupted", false);
    status.put("priority", 1);

    List<String> stack = new ArrayList<String>(2);
    stack.add("net.opentsdb.tsd(TestHttpJsonSerializer.java:0)");
    stack.add("java.lang.Thread.run(Thread.java:695)");
    status.put("stack", stack);
    output.add(status);

    ChannelBuffer cb = serdes.formatThreadStatsV1(output);
    assertNotNull(cb);
    final String json = cb.toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"threadID\":1"));
    assertTrue(json.contains("\"name\":\"Test Thread 1\""));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void formatThreadStatsNull() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.formatThreadStatsV1(null);
  }
  
  /**
   * Helper to reset the query stats and mock the time calls before each
   * data point query. 
   */
  private void setupFormatQuery() throws Exception {
    mockTime();
    running_queries.set(null, new ConcurrentHashMap<Integer, QueryStats>());
    completed_queries.set(null, CacheBuilder.newBuilder().maximumSize(2).build());
  }
  
  /** @return Returns a test TSQuery object to pass on to the serializer */
  private TSQuery getTestQuery(final boolean show_stats) {
    return getTestQuery(show_stats, false);
  }

  /** @return Returns a test TSQuery object to pass on to the serializer */
  private TSQuery getTestQuery(final boolean show_stats, final boolean show_summary) {
    final TSQuery data_query = new TSQuery();
    data_query.setStart("1356998400");
    data_query.setEnd("1388534400");
    data_query.setShowStats(show_stats);
    data_query.setShowSummary(show_summary);
    
    final TSSubQuery sub_query = new TSSubQuery();
    sub_query.setMetric("sys.cpu.user");
    sub_query.setAggregator("sum");
    final ArrayList<TSSubQuery> sub_queries = new ArrayList<TSSubQuery>(1);
    sub_queries.add(sub_query);
    data_query.setQueries(sub_queries);    
    
    return data_query;
  }
  
  /**
   * Helper to validate (set) the time series query
   * @param data_query The query to validate
   */
  private void validateTestQuery(final TSQuery data_query) {
    data_query.validateAndSetQuery();
    data_query.setQueryStats(new QueryStats(remote, data_query, null));
  }
  
  /**
   * Mocks out the DateTime class and increments the timestamp by 500ms every
   * time it's called.
   */
  private void mockTime() {
    timestamp.add(1388534400000L);
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenCallRealMethod();
    PowerMockito.when(DateTime.currentTimeMillis())
      .thenAnswer(new Answer<Long> () {
      public Long answer(InvocationOnMock invocation) throws Throwable {
        long ts = timestamp.get(0);
        timestamp.set(0, ts + 500);
        return ts;
      }
    });
    
    PowerMockito.when(DateTime.nanoTime())
    .thenAnswer(new Answer<Long> () {
    public Long answer(InvocationOnMock invocation) throws Throwable {
      long ts = timestamp.get(0);
      timestamp.set(0, ts + 500);
      return ts * 1000000;
    }
  });
  }

}
