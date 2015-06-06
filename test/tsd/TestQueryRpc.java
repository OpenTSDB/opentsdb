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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Method;
import java.nio.charset.Charset;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.filter.TagVRegexFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;
import net.opentsdb.storage.MockDataPoints;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.uid.NoSuchUniqueName;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

/**
 * Unit tests for the Query RPC class that handles parsing user queries for
 * timeseries data and returning that data
 * <b>Note:</b> Testing query validation and such should be done in the 
 * core.TestTSQuery and TestTSSubQuery classes
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HttpQuery.class, Query.class, 
  Deferred.class, TSQuery.class, DateTime.class, DeferredGroupException.class })
public final class TestQueryRpc {
  private TSDB tsdb = null;
  private QueryRpc rpc;
  private Query empty_query = mock(Query.class);
  private Query query_result;
  
  private static final Method parseQuery;
  static {
    try {
      parseQuery = QueryRpc.class.getDeclaredMethod("parseQuery", 
          TSDB.class, HttpQuery.class);
      parseQuery.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
    empty_query = mock(Query.class);
    query_result = mock(Query.class);
    rpc = new QueryRpc();
    
    when(tsdb.newQuery()).thenReturn(query_result);
    when(empty_query.run()).thenReturn(new DataPoints[0]);
    when(query_result.configureFromQuery((TSQuery)any(), anyInt()))
      .thenReturn(Deferred.fromResult(null));
    when(query_result.runAsync())
      .thenReturn(Deferred.fromResult(new DataPoints[0]));
  }
  
  @Test
  public void parseQueryMType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals("sys.cpu.0", sub.getMetric());
  }
  
  @Test
  public void parseQueryMTypeWEnd() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&end=5m-ago&m=sum:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertEquals("5m-ago", tsq.getEnd());
  }
  
  @Test
  public void parseQuery2MType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0&m=avg:sys.cpu.1");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq.getQueries());
    assertEquals(2, tsq.getQueries().size());
    TSSubQuery sub1 = tsq.getQueries().get(0);
    assertNotNull(sub1);
    assertEquals("sum", sub1.getAggregator());
    assertEquals("sys.cpu.0", sub1.getMetric());
    TSSubQuery sub2 = tsq.getQueries().get(1);
    assertNotNull(sub2);
    assertEquals("avg", sub2.getAggregator());
    assertEquals("sys.cpu.1", sub2.getMetric());
  }
  
  @Test
  public void parseQueryMTypeWRate() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:rate:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertTrue(sub.getRate());
  }
  
  @Test
  public void parseQueryMTypeWDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:1h-avg:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertEquals("1h-avg", sub.getDownsample());
  }
  
  @Test
  public void parseQueryMTypeWDSAndFill() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:1h-avg-lerp:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertEquals("1h-avg-lerp", sub.getDownsample());
  }

  @Test
  public void parseQueryMTypeWRateAndDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:1h-avg:rate:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertTrue(sub.getRate());
    assertEquals("1h-avg", sub.getDownsample());
  }
  
  @Test
  public void parseQueryMTypeWTag() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=web01}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub.getTags());
    assertEquals("literal_or(web01)", sub.getTags().get("host"));
  }
  
  @Test
  public void parseQueryMTypeWGroupByRegex() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=" + 
          TagVRegexFilter.FILTER_NAME + "(something(foo|bar))}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVRegexFilter);
  }
  
  @Test
  public void parseQueryMTypeWGroupByWildcardExplicit() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=" + 
          TagVWildcardFilter.FILTER_NAME + "(*quirm)}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
  }
  
  @Test
  public void parseQueryMTypeWGroupByWildcardImplicit() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=*quirm}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
  }
  
  @Test
  public void parseQueryMTypeWWildcardFilterExplicit() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{}{host=wildcard(*quirm)}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
  }
  
  @Test
  public void parseQueryMTypeWWildcardFilterImplicit() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{}{host=*quirm}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
  }
  
  @Test
  public void parseQueryMTypeWGroupByAndWildcardFilterExplicit() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{colo=lga}{host=wildcard(*quirm)}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
    assertTrue(sub.getFilters().get(1) instanceof TagVLiteralOrFilter);
  }
  
  @Test
  public void parseQueryMTypeWGroupByAndWildcardFilterSameTagK() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=quirm|tsort}"
      + "{host=wildcard(*quirm)}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
    assertTrue(sub.getFilters().get(1) instanceof TagVLiteralOrFilter);
  }
  
  @Test
  public void parseQueryMTypeWGroupByFilterAndWildcardFilterSameTagK() 
      throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=wildcard(*tsort)}"
      + "{host=wildcard(*quirm)}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertEquals(2, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
    assertTrue(sub.getFilters().get(1) instanceof TagVWildcardFilter);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseQueryMTypeWGroupByFilterMissingClose() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=wildcard(*tsort)}"
      + "{host=wildcard(*quirm)");
    parseQuery.invoke(rpc, tsdb, query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseQueryMTypeWGroupByFilterMissingEquals() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=wildcard(*tsort)}"
      + "{hostwildcard(*quirm)}");
    parseQuery.invoke(rpc, tsdb, query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseQueryMTypeWGroupByNoSuchFilter() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=nosuchfilter(*tsort)}"
      + "{host=dummyfilter(*quirm)}");
    parseQuery.invoke(rpc, tsdb, query);
  }
  
  @Test
  public void parseQueryMTypeWEmptyFilterBrackets() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{}{}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    sub.validateAndSetQuery();
    assertEquals(0, sub.getFilters().size());
  }
  
  @Test
  public void parseQueryTSUIDType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
  }
  
  @Test
  public void parseQueryTSUIDTypeMulti() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:010101,020202");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(2, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertEquals("020202", sub.getTsuids().get(1));
  }
  
  @Test
  public void parseQuery2TSUIDType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:010101&tsuid=avg:020202");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    assertEquals(2, tsq.getQueries().size());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    sub = tsq.getQueries().get(1);
    assertNotNull(sub);
    assertEquals("avg", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("020202", sub.getTsuids().get(0));
  }
  
  @Test
  public void parseQueryTSUIDTypeWRate() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:rate:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertTrue(sub.getRate());
  }
  
  @Test
  public void parseQueryTSUIDTypeWDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:1m-sum:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertEquals("1m-sum", sub.getDownsample());
  }
  
  @Test
  public void parseQueryTSUIDTypeWRateAndDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:1m-sum:rate:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertEquals("1m-sum", sub.getDownsample());
    assertTrue(sub.getRate());
  }
  
  @Test
  public void parseQueryWPadding() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0&padding");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertTrue(tsq.getPadding());
  }
  
  @Test (expected = BadRequestException.class)
  public void parseQueryStartMissing() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?end=1h-ago&m=sum:sys.cpu.0");
    parseQuery.invoke(rpc, tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void parseQueryNoSubQuery() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago");
    parseQuery.invoke(rpc, tsdb, query);
  }
  
  @Test
  public void postQuerySimplePass() throws Exception {
    final DataPoints[] datapoints = new DataPoints[1];
    datapoints[0] = new MockDataPoints().getMock();
    when(query_result.runAsync()).thenReturn(
        Deferred.fromResult(datapoints));
    
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query",
        "{\"start\":1425440315306,\"queries\":" +
          "[{\"metric\":\"somemetric\",\"aggregator\":\"sum\",\"rate\":true," +
          "\"rateOptions\":{\"counter\":false}}]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }

  @Test
  public void postQueryNoMetricBadRequest() throws Exception {
    final DeferredGroupException dge = mock(DeferredGroupException.class);
    when(dge.getCause()).thenReturn(new NoSuchUniqueName("foo", "metrics"));

    when(query_result.configureFromQuery((TSQuery)any(), anyInt()))
      .thenReturn(Deferred.fromError(dge));

    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query",
        "{\"start\":1425440315306,\"queries\":" +
          "[{\"metric\":\"nonexistent\",\"aggregator\":\"sum\",\"rate\":true," +
          "\"rateOptions\":{\"counter\":false}}]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("No such name for 'foo': 'metrics'"));
  }

  @Test
  public void executeEmpty() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query?start=1h-ago&m=sum:sys.cpu.user");
    rpc.execute(tsdb, query);
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertEquals("[]", json);
  }
  
  @Test
  public void execute() throws Exception {
    final DataPoints[] datapoints = new DataPoints[1];
    datapoints[0] = new MockDataPoints().getMock();
    when(query_result.runAsync()).thenReturn(
        Deferred.fromResult(datapoints));
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query?start=1h-ago&m=sum:sys.cpu.user");
    rpc.execute(tsdb, query);
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
  }
  
  @Test
  public void executeNSU() throws Exception {
    final DeferredGroupException dge = mock(DeferredGroupException.class);
    when(dge.getCause()).thenReturn(new NoSuchUniqueName("foo", "metrics"));

    when(query_result.configureFromQuery((TSQuery)any(), anyInt()))
      .thenReturn(Deferred.fromError(dge));
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query?start=1h-ago&m=sum:sys.cpu.user");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("No such name for 'foo': 'metrics'"));
  }
  
  @Test
  public void executeWithBadDSFill() throws Exception {
    final DataPoints[] datapoints = new DataPoints[1];
    datapoints[0] = new MockDataPoints().getMock();
    when(query_result.runAsync()).thenReturn(
        Deferred.fromResult(datapoints));

    try {    
      final HttpQuery query = NettyMocks.getQuery(tsdb, 
          "/api/query?start=1h-ago&m=sum:10m-avg-badbadbad:sys.cpu.user");
      rpc.execute(tsdb, query);
      fail("expected BadRequestException");
    } catch (final BadRequestException exn) {
      assertTrue(exn.getMessage().startsWith(
          "No such fill policy: 'badbadbad': must be one of:"));
    }
  }
  
  //TODO(cl) add unit tests for the rate options parsing
}