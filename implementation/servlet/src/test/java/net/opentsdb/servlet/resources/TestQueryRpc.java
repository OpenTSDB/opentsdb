// This file is part of OpenTSDB.
// Copyright (C) 2013-2017 The OpenTSDB Authors.
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
package net.opentsdb.servlet.resources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.pojo.TagVLiteralOrFilter;
import net.opentsdb.query.pojo.TagVRegexFilter;
import net.opentsdb.query.pojo.TagVWildcardFilter;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.uid.NoSuchUniqueName;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

/**
 * Unit tests for the Query RPC class that handles parsing user queries for
 * timeseries data and returning that data
 * <b>Note:</b> Testing query validation and such should be done in the 
 * core.TestTSQuery and TestTSSubQuery classes
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DefaultTSDB.class, Config.class, 
  Deferred.class, TSQuery.class, DateTime.class, DeferredGroupException.class })
public final class TestQueryRpc {
  private DefaultTSDB tsdb;
  private Configuration config;
  private QueryRpc rpc;
  private ServletConfig servlet_config;
  private ServletContext context;
  private HttpServletRequest request;
  private AsyncContext async;
  private Map<String, String> headers;
//  private Query empty_query = mock(Query.class);
//  private Query query_result;
//  private List<ExpressionTree> expressions;
//  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(DefaultTSDB.class);
    config = UnitTestConfiguration.getConfiguration();
//    empty_query = mock(Query.class);
//    query_result = mock(Query.class);
    rpc = new QueryRpc();
//    expressions = null;
    servlet_config = mock(ServletConfig.class);
    context = mock(ServletContext.class);
    request = mock(HttpServletRequest.class);
    async = mock(AsyncContext.class);
    headers = Maps.newHashMap();
    
    when(tsdb.getConfig()).thenReturn(config);
    when(servlet_config.getServletContext()).thenReturn(context);
    
//    when(tsdb.newQuery()).thenReturn(query_result);
//    when(empty_query.run()).thenReturn(new DataPoints[0]);
//    when(query_result.configureFromQuery((TSQuery)any(), anyInt()))
//      .thenReturn(Deferred.fromResult(null));
//    when(query_result.runAsync())
//      .thenReturn(Deferred.fromResult(new DataPoints[0]));
  }
  
//  @Test
//  public void parseQueryMType() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertEquals("1h-ago", tsq.getStart());
//    assertNotNull(tsq.getQueries());
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub);
//    assertEquals("sum", sub.getAggregator());
//    assertEquals("sys.cpu.0", sub.getMetric());
//  }
//  
//  @Test
//  public void parseQueryMTypeWEnd() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&end=5m-ago&m=sum:sys.cpu.0");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertEquals("5m-ago", tsq.getEnd());
//  }
//  
//  @Test
//  public void parseQuery2MType() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0&m=avg:sys.cpu.1");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq.getQueries());
//    assertEquals(2, tsq.getQueries().size());
//    TSSubQuery sub1 = tsq.getQueries().get(0);
//    assertNotNull(sub1);
//    assertEquals("sum", sub1.getAggregator());
//    assertEquals("sys.cpu.0", sub1.getMetric());
//    TSSubQuery sub2 = tsq.getQueries().get(1);
//    assertNotNull(sub2);
//    assertEquals("avg", sub2.getAggregator());
//    assertEquals("sys.cpu.1", sub2.getMetric());
//  }
//  
//  @Test
//  public void parseQueryMTypeWRate() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:rate:sys.cpu.0");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertTrue(sub.getRate());
//  }
//  
//  @Test
//  public void parseQueryMTypeWDS() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:1h-avg:sys.cpu.0");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertEquals("1h-avg", sub.getDownsample());
//  }
//  
//  @Test
//  public void parseQueryMTypeWDSAndFill() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:1h-avg-lerp:sys.cpu.0");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertEquals("1h-avg-lerp", sub.getDownsample());
//  }
//
//  @Test
//  public void parseQueryMTypeWRateAndDS() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:1h-avg:rate:sys.cpu.0");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertTrue(sub.getRate());
//    assertEquals("1h-avg", sub.getDownsample());
//  }
//  
//  @Test
//  public void parseQueryMTypeWTag() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=web01}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub.getTags());
//    assertEquals("literal_or(web01)", sub.getTags().get("host"));
//  }
//  
//  @Test
//  public void parseQueryMTypeWGroupByRegex() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=" + 
//          TagVRegexFilter.FILTER_NAME + "(something(foo|bar))}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertEquals(1, sub.getFilters().size());
//    assertTrue(sub.getFilters().get(0) instanceof TagVRegexFilter);
//  }
//  
//  @Test
//  public void parseQueryMTypeWGroupByWildcardExplicit() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=" + 
//          TagVWildcardFilter.FILTER_NAME + "(*quirm)}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertEquals(1, sub.getFilters().size());
//    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
//  }
//  
//  @Test
//  public void parseQueryMTypeWGroupByWildcardImplicit() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=*quirm}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertEquals(1, sub.getFilters().size());
//    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
//  }
//  
//  @Test
//  public void parseQueryMTypeWWildcardFilterExplicit() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{}{host=wildcard(*quirm)}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertEquals(1, sub.getFilters().size());
//    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
//  }
//  
//  @Test
//  public void parseQueryMTypeWWildcardFilterImplicit() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{}{host=*quirm}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertEquals(1, sub.getFilters().size());
//    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
//  }
//  
//  @Test
//  public void parseQueryMTypeWGroupByAndWildcardFilterExplicit() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{colo=lga}{host=wildcard(*quirm)}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
//    assertTrue(sub.getFilters().get(1) instanceof TagVLiteralOrFilter);
//  }
//  
//  @Test
//  public void parseQueryMTypeWGroupByAndWildcardFilterSameTagK() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=quirm|tsort}"
//      + "{host=wildcard(*quirm)}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
//    assertTrue(sub.getFilters().get(1) instanceof TagVLiteralOrFilter);
//  }
//  
//  @Test
//  public void parseQueryMTypeWGroupByFilterAndWildcardFilterSameTagK() 
//      throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=wildcard(*tsort)}"
//      + "{host=wildcard(*quirm)}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertEquals(2, sub.getFilters().size());
//    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
//    assertTrue(sub.getFilters().get(1) instanceof TagVWildcardFilter);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void parseQueryMTypeWGroupByFilterMissingClose() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=wildcard(*tsort)}"
//      + "{host=wildcard(*quirm)");
//    parseQuery.invoke(rpc, tsdb, query, expressions);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void parseQueryMTypeWGroupByFilterMissingEquals() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=wildcard(*tsort)}"
//      + "{hostwildcard(*quirm)}");
//    parseQuery.invoke(rpc, tsdb, query, expressions);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void parseQueryMTypeWGroupByNoSuchFilter() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=nosuchfilter(*tsort)}"
//      + "{host=dummyfilter(*quirm)}");
//    parseQuery.invoke(rpc, tsdb, query, expressions);
//  }
//  
//  @Test
//  public void parseQueryMTypeWEmptyFilterBrackets() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0{}{}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    sub.validateAndSetQuery();
//    assertEquals(0, sub.getFilters().size());
//  }
//  
//  @Test
//  public void parseQueryMTypeWExplicit() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:explicit_tags:sys.cpu.0{host=web01}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub.getTags());
//    assertEquals("literal_or(web01)", sub.getTags().get("host"));
//    assertTrue(sub.getExplicitTags());
//  }
//  
//  @Test
//  public void parseQueryMTypeWExplicitAndRate() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:explicit_tags:rate:sys.cpu.0{host=web01}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub.getTags());
//    assertEquals("literal_or(web01)", sub.getTags().get("host"));
//    assertTrue(sub.getRate());
//    assertTrue(sub.getExplicitTags());
//  }
//  
//  @Test
//  public void parseQueryMTypeWExplicitAndRateAndDS() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:explicit_tags:rate:1m-sum:sys.cpu.0{host=web01}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub.getTags());
//    assertEquals("literal_or(web01)", sub.getTags().get("host"));
//    assertTrue(sub.getRate());
//    assertTrue(sub.getExplicitTags());
//    assertEquals("1m-sum", sub.getDownsample());
//  }
//  
//  @Test
//  public void parseQueryMTypeWExplicitAndDSAndRate() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:explicit_tags:1m-sum:rate:sys.cpu.0{host=web01}");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub.getTags());
//    assertEquals("literal_or(web01)", sub.getTags().get("host"));
//    assertTrue(sub.getRate());
//    assertTrue(sub.getExplicitTags());
//    assertEquals("1m-sum", sub.getDownsample());
//  }
//  
//  @Test
//  public void parseQueryTSUIDType() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&tsuid=sum:010101");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertEquals("1h-ago", tsq.getStart());
//    assertNotNull(tsq.getQueries());
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub);
//    assertEquals("sum", sub.getAggregator());
//    assertEquals(1, sub.getTsuids().size());
//    assertEquals("010101", sub.getTsuids().get(0));
//  }
//  
//  @Test
//  public void parseQueryTSUIDTypeMulti() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&tsuid=sum:010101,020202");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertEquals("1h-ago", tsq.getStart());
//    assertNotNull(tsq.getQueries());
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub);
//    assertEquals("sum", sub.getAggregator());
//    assertEquals(2, sub.getTsuids().size());
//    assertEquals("010101", sub.getTsuids().get(0));
//    assertEquals("020202", sub.getTsuids().get(1));
//  }
//  
//  @Test
//  public void parseQuery2TSUIDType() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&tsuid=sum:010101&tsuid=avg:020202");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertEquals("1h-ago", tsq.getStart());
//    assertNotNull(tsq.getQueries());
//    assertEquals(2, tsq.getQueries().size());
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub);
//    assertEquals("sum", sub.getAggregator());
//    assertEquals(1, sub.getTsuids().size());
//    assertEquals("010101", sub.getTsuids().get(0));
//    sub = tsq.getQueries().get(1);
//    assertNotNull(sub);
//    assertEquals("avg", sub.getAggregator());
//    assertEquals(1, sub.getTsuids().size());
//    assertEquals("020202", sub.getTsuids().get(0));
//  }
//  
//  @Test
//  public void parseQueryTSUIDTypeWRate() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&tsuid=sum:rate:010101");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertEquals("1h-ago", tsq.getStart());
//    assertNotNull(tsq.getQueries());
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub);
//    assertEquals("sum", sub.getAggregator());
//    assertEquals(1, sub.getTsuids().size());
//    assertEquals("010101", sub.getTsuids().get(0));
//    assertTrue(sub.getRate());
//  }
//  
//  @Test
//  public void parseQueryTSUIDTypeWDS() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&tsuid=sum:1m-sum:010101");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertEquals("1h-ago", tsq.getStart());
//    assertNotNull(tsq.getQueries());
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub);
//    assertEquals("sum", sub.getAggregator());
//    assertEquals(1, sub.getTsuids().size());
//    assertEquals("010101", sub.getTsuids().get(0));
//    assertEquals("1m-sum", sub.getDownsample());
//  }
//  
//  @Test
//  public void parseQueryTSUIDTypeWRateAndDS() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&tsuid=sum:1m-sum:rate:010101");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertEquals("1h-ago", tsq.getStart());
//    assertNotNull(tsq.getQueries());
//    TSSubQuery sub = tsq.getQueries().get(0);
//    assertNotNull(sub);
//    assertEquals("sum", sub.getAggregator());
//    assertEquals(1, sub.getTsuids().size());
//    assertEquals("010101", sub.getTsuids().get(0));
//    assertEquals("1m-sum", sub.getDownsample());
//    assertTrue(sub.getRate());
//  }
//  
//  @Test
//  public void parseQueryWPadding() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago&m=sum:sys.cpu.0&padding");
//    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query, expressions);
//    assertNotNull(tsq);
//    assertTrue(tsq.getPadding());
//  }
//  
//  @Test (expected = WebApplicationException.class)
//  public void parseQueryStartMissing() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?end=1h-ago&m=sum:sys.cpu.0");
//    parseQuery.invoke(rpc, tsdb, query, expressions);
//  }
//  
//  @Test (expected = WebApplicationException.class)
//  public void parseQueryNoSubQuery() throws Exception {
//    HttpQuery query = NettyMocks.getQuery(tsdb, 
//      "/api/query?start=1h-ago");
//    parseQuery.invoke(rpc, tsdb, query, expressions);
//  }
//  
//  @Test
//  public void postQuerySimplePass() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//    
//    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query",
//        "{\"start\":1425440315306,\"queries\":" +
//          "[{\"metric\":\"somemetric\",\"aggregator\":\"sum\",\"rate\":true," +
//          "\"rateOptions\":{\"counter\":false}}]}");
//    NettyMocks.mockChannelFuture(query);
//    rpc.execute(tsdb, query);
//    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
//  }
//
//  @Test
//  public void postQueryNoMetricBadRequest() throws Exception {
//    final DeferredGroupException dge = mock(DeferredGroupException.class);
//    when(dge.getCause()).thenReturn(new NoSuchUniqueName("foo", "metrics"));
//
//    when(query_result.configureFromQuery((TSQuery)any(), anyInt()))
//      .thenReturn(Deferred.fromError(dge));
//
//    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query",
//        "{\"start\":1425440315306,\"queries\":" +
//          "[{\"metric\":\"nonexistent\",\"aggregator\":\"sum\",\"rate\":true," +
//          "\"rateOptions\":{\"counter\":false}}]}");
//    rpc.execute(tsdb, query);
//    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("No such name for 'foo': 'metrics'"));
//  }
//
//  @Test
//  public void executeEmpty() throws Exception {
//    final HttpQuery query = NettyMocks.getQuery(tsdb, 
//        "/api/query?start=1h-ago&m=sum:sys.cpu.user");
//    NettyMocks.mockChannelFuture(query);
//    rpc.execute(tsdb, query);
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertEquals("[]", json);
//  }
//  
//  @Test
//  public void executeURI() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//    
//    final HttpQuery query = NettyMocks.getQuery(tsdb, 
//        "/api/query?start=1h-ago&m=sum:sys.cpu.user");
//    NettyMocks.mockChannelFuture(query);
//    rpc.execute(tsdb, query);
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
//  }
//  
//  @Test
//  public void executeURIDuplicates() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//    
//    final HttpQuery query = NettyMocks.getQuery(tsdb, 
//        "/api/query?start=1h-ago&m=sum:sys.cpu.user&m=sum:sys.cpu.user"
//        + "&m=sum:sys.cpu.user");
//    NettyMocks.mockChannelFuture(query);
//    rpc.execute(tsdb, query);
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
//  }
//  
//  @Test
//  public void executeNSU() throws Exception {
//    final DeferredGroupException dge = mock(DeferredGroupException.class);
//    when(dge.getCause()).thenReturn(new NoSuchUniqueName("foo", "metrics"));
//
//    when(query_result.configureFromQuery((TSQuery)any(), anyInt()))
//      .thenReturn(Deferred.fromError(dge));
//    
//    final HttpQuery query = NettyMocks.getQuery(tsdb, 
//        "/api/query?start=1h-ago&m=sum:sys.cpu.user");
//    rpc.execute(tsdb, query);
//    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("No such name for 'foo': 'metrics'"));
//  }
//  
//  @Test
//  public void executeWithBadDSFill() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//
//    try {    
//      final HttpQuery query = NettyMocks.getQuery(tsdb, 
//          "/api/query?start=1h-ago&m=sum:10m-avg-badbadbad:sys.cpu.user");
//      rpc.execute(tsdb, query);
//      fail("expected WebApplicationException");
//    } catch (final WebApplicationException exn) {
//      System.out.println(exn.getMessage());
//      assertTrue(exn.getMessage().startsWith(
//          "Unrecognized fill policy: badbadbad"));
//    }
//  }
//  
//  @Test
//  public void executePOST() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//    
//    final HttpQuery query = NettyMocks.postQuery(tsdb,"/api/query",
//        "{\"start\":\"1h-ago\",\"queries\":" +
//            "[{\"metric\":\"sys.cpu.user\",\"aggregator\":\"sum\"}]}");
//    NettyMocks.mockChannelFuture(query);
//    rpc.execute(tsdb, query);
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
//  }
//  
//  @Test
//  public void executePOSTDuplicates() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//    
//    final HttpQuery query = NettyMocks.postQuery(tsdb,"/api/query",
//        "{\"start\":\"1h-ago\",\"queries\":" +
//            "[{\"metric\":\"sys.cpu.user\",\"aggregator\":\"sum\"},"
//            + "{\"metric\":\"sys.cpu.user\",\"aggregator\":\"sum\"}]}");
//    NettyMocks.mockChannelFuture(query);
//    rpc.execute(tsdb, query);
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
//  }
//  
//  @Test (expected = WebApplicationException.class)
//  public void deleteDatapointsBadRequest() throws Exception {
//    HttpQuery query = NettyMocks.deleteQuery(tsdb,
//      "/api/query?start=1356998400&m=sum:sys.cpu.user", "");
//    rpc.execute(tsdb, query);
//    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
//    final String json =
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("Deleting data is not enabled"));
//  }
//  
//  @Test
//  public void gexp() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//    
//    final HttpQuery query = NettyMocks.getQuery(tsdb, 
//        "/api/query/gexp?start=1h-ago&exp=scale(sum:sys.cpu.user,1)");
//    NettyMocks.mockChannelFuture(query);
//    rpc.execute(tsdb, query);
//    assertEquals(query.response().getStatus(), HttpResponseStatus.OK);
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
//  }
//  
//  @Test
//  public void gexpBadExpression() throws Exception {
//    final DataPoints[] datapoints = new DataPoints[1];
//    datapoints[0] = new MockDataPoints().getMock();
//    when(query_result.runAsync()).thenReturn(
//        Deferred.fromResult(datapoints));
//    
//    final HttpQuery query = NettyMocks.getQuery(tsdb, 
//        "/api/query/gexp?start=1h-ago&exp=scale(sum:sys.cpu.user,notanumber)");
//    rpc.execute(tsdb, query);
//    assertEquals(query.response().getStatus(), HttpResponseStatus.BAD_REQUEST);
//    final String json = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(json.contains("factor"));
//  }
//  
  //TODO(cl) add unit tests for the rate options parsing
}
